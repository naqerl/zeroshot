//! Resolution of an eval's execution configuration.
//!
//! An eval is authored once and run many times, so everything `eval run` needs is either in the
//! document or supplied on the command line. This module owns that meeting point: which named
//! target receives the samples, and which built-in template judges them.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use openengine_cluster_protocol::{
    GraphSpec, NodeRuntimeBinding, RuntimePlan, SourceBranchId, SourceRevisionId,
};

use crate::native_v2_cli::{BuiltinGraphTemplate, TemplateDelivery};

use super::{EvalArm, EvalDocument, EvalError, EvalStore};

/// The checkout an eval store belongs to.
///
/// `EvalStore` owns `<workspace>/.zeroshot/evals`, so a named run's source selection reads the
/// workspace the eval document was stored in rather than the folder the command happens to run in.
pub(crate) fn workspace_root(store: &EvalStore) -> Result<PathBuf, EvalError> {
    let root = store.root();
    let workspace = root.parent().and_then(Path::parent).ok_or_else(|| {
        EvalError::Invalid(format!(
            "eval store {} is not a .zeroshot/evals directory inside a workspace",
            root.display()
        ))
    })?;
    Ok(workspace.to_path_buf())
}

/// Resolve the named target that receives this eval's sample runs.
///
/// `requested` is the command line's `--target`; it wins over the document so one stored eval can
/// be compared on more than one target. An eval that names neither is refused rather than run
/// locally, because a sample executed in the caller's own checkout is not a comparable measurement.
pub(crate) fn resolve_target(
    document: &EvalDocument,
    requested: Option<&str>,
) -> Result<String, EvalError> {
    let target = requested
        .map(str::trim)
        .filter(|target| !target.is_empty())
        .or_else(|| {
            document
                .target
                .as_deref()
                .map(str::trim)
                .filter(|target| !target.is_empty())
        });
    target.map(str::to_owned).ok_or_else(|| {
        EvalError::Invalid(format!(
            "eval {:?} requires a nonempty target: pass --target NAME or set target in the eval document",
            document.slug
        ))
    })
}

/// Expand the eval's judging template into the graph and runtime one sample is judged by.
///
/// A document without a `judge` object keeps the sample runs' outputs as the result. A document
/// that names a template the binary does not ship is a named error: the document came from another
/// version or from a typo, and guessing a fallback would judge with a graph the author never chose.
pub(crate) fn judge_plan(document: &EvalDocument) -> Result<Option<JudgePlan>, EvalError> {
    let Some(judge) = &document.judge else {
        return Ok(None);
    };
    let template = BuiltinGraphTemplate::parse(&judge.template).ok_or_else(|| {
        EvalError::Invalid(format!(
            "eval {:?} names unknown judge template {:?}; available templates: {}",
            document.slug,
            judge.template,
            available_templates()
        ))
    })?;
    let graph = template
        .materialize(TemplateDelivery::None)
        .map_err(|error| {
            EvalError::Invalid(format!(
                "eval {:?} cannot expand judge template {:?}: {error}",
                document.slug, judge.template
            ))
        })?;
    let runtime = judge_runtime(document, template, &graph)?;
    Ok(Some(JudgePlan { graph, runtime }))
}

/// The judging graph an eval selected, with the runtime it runs on.
#[derive(Debug)]
pub(crate) struct JudgePlan {
    /// The template expanded into a runnable graph.
    pub(crate) graph: GraphSpec,
    /// The runtime that binds every agent of [`Self::graph`].
    pub(crate) runtime: RuntimePlan,
}

fn available_templates() -> String {
    BuiltinGraphTemplate::all()
        .iter()
        .map(|template| template.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The source selection one arm's samples are submitted with.
#[derive(Debug)]
pub(crate) struct ArmSource {
    pub(crate) branch: Option<SourceBranchId>,
    pub(crate) revision: Option<SourceRevisionId>,
}

/// Read one arm's `head` as the source the target should check out.
///
/// An eval may pin an arm to either a branch name or an exact commit, and the two travel in
/// different fields, so the shape of `head` decides where it goes: a full Git object name is an
/// exact revision, anything else names a branch the target resolves itself. Sending a branch name
/// as a revision would ask the target for a commit by that name and fail, and sending a revision
/// as a branch would let the branch move under the comparison.
pub(crate) fn arm_source(document: &EvalDocument, arm: &EvalArm) -> Result<ArmSource, EvalError> {
    let head = arm.head.trim();
    if head.is_empty() {
        return Err(EvalError::Invalid(format!(
            "eval {:?} arm {:?} requires a nonempty head",
            document.slug, arm.id
        )));
    }
    let unusable = || {
        EvalError::Invalid(format!(
            "eval {:?} arm {:?} has an unusable head {head:?}",
            document.slug, arm.id
        ))
    };
    if is_object_id(head) {
        return Ok(ArmSource {
            branch: None,
            revision: Some(SourceRevisionId::new(head).map_err(|_| unusable())?),
        });
    }
    Ok(ArmSource {
        branch: Some(SourceBranchId::new(head).map_err(|_| unusable())?),
        revision: None,
    })
}

fn is_object_id(head: &str) -> bool {
    matches!(head.len(), 40 | 64) && head.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Build the runtime that judges one sample.
///
/// An eval authors a runtime per arm, never one for the judge, so the judge borrows the lane of
/// the first arm that binds an agent: the eval already decided who runs its samples, and judging
/// them on that same lane keeps the comparison reproducible. Every agent the judge graph declares
/// is bound to that lane; a template that also needs a non-agent worker is refused instead of
/// guessed at.
pub(crate) fn judge_runtime(
    document: &EvalDocument,
    template: BuiltinGraphTemplate,
    graph: &GraphSpec,
) -> Result<RuntimePlan, EvalError> {
    let (source, binding) = document
        .arms
        .iter()
        .find_map(|arm| {
            arm.runtime
                .nodes
                .values()
                .find(|binding| matches!(binding, NodeRuntimeBinding::Agent { .. }))
                .map(|binding| (&arm.runtime, binding))
        })
        .ok_or_else(|| {
            EvalError::Invalid(format!(
                "eval {:?} cannot derive a judge runtime: no arm binds an agent node",
                document.slug
            ))
        })?;
    let NodeRuntimeBinding::Agent {
        lane,
        model,
        effort,
        harness_config,
        context_window,
        max_tokens,
        session_scope,
        connections,
    } = binding
    else {
        unreachable!("the selected binding is an agent binding")
    };

    let mut nodes = BTreeMap::new();
    for (name, delivery) in crate::native_v2_admission::executable_runtime_roles(&graph.root) {
        if delivery {
            return Err(EvalError::Invalid(format!(
                "eval {:?} cannot judge with template {:?}: the template needs a delivery worker",
                document.slug,
                template.name()
            )));
        }
        nodes.insert(
            name,
            NodeRuntimeBinding::Agent {
                lane: lane.clone(),
                model: model.clone(),
                effort: *effort,
                harness_config: harness_config.clone(),
                context_window: *context_window,
                max_tokens: *max_tokens,
                session_scope: *session_scope,
                connections: connections.clone(),
            },
        );
    }

    Ok(RuntimePlan {
        harness: source.harness.clone(),
        provider: source.provider.clone(),
        size: source.size.clone(),
        max_concurrency: source.max_concurrency,
        workspace_checkpoints: source.workspace_checkpoints,
        active_memory: source.active_memory.clone(),
        global_agents: source.global_agents.clone(),
        nodes,
    })
}
