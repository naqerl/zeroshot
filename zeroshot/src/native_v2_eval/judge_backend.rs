//! Provider-backed judge graph execution through the native-v2 local runner.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use openengine_cluster_protocol::{
    IdempotencyKey, ModelId, NodeRuntimeBinding, RunId, RunTitle, RuntimePlan,
};
use serde_json::Value;

use crate::native_v2_cli::{NativeV2CliBackend, PreparedRunRequest, TargetRunIntent};
use super::{BackendSampleRequest, LocalRunner, run_sample};

use super::{EvalError, JudgeGraphExecutor, JudgeTemplate};

/// Load an exact secret-free runtime plan for judge graph nodes.
pub fn load_judge_runtime(path: &Path) -> Result<RuntimePlan, EvalError> {
    let bytes = std::fs::read(path).map_err(|error| EvalError::io(path, error))?;
    serde_json::from_slice(&bytes).map_err(|error| {
        EvalError::Command(format!(
            "judge runtime plan {} is not valid JSON: {error}",
            path.display()
        ))
    })
}

/// Apply the effective per-sample model to every agent binding in the authored runtime.
pub fn runtime_with_model(runtime: &RuntimePlan, model: &str) -> Result<RuntimePlan, EvalError> {
    let model = ModelId::new(model)
        .map_err(|error| EvalError::Command(format!("judge model is invalid: {error}")))?;
    let mut updated = runtime.clone();
    let nodes = match &mut updated {
        RuntimePlan::Copilot { nodes, .. }
        | RuntimePlan::Codex { nodes, .. }
        | RuntimePlan::Claude { nodes, .. }
        | RuntimePlan::Pi { nodes, .. } => nodes,
    };
    let mut agents = 0_usize;
    for binding in nodes.values_mut() {
        if let NodeRuntimeBinding::Agent {
            model: node_model, ..
        } = binding
        {
            *node_model = model.clone();
            agents += 1;
        }
    }
    if agents == 0 {
        return Err(EvalError::Command(
            "judge runtime plan has no agent bindings".to_owned(),
        ));
    }
    Ok(updated)
}

/// Executes a judge graph as a real local native-v2 run. The selected model is applied to every
/// agent node immediately before submission, so per-arm and per-run overrides cannot be lost.
pub struct LocalJudgeGraphExecutor<B> {
    backend: Arc<B>,
    runtime: RuntimePlan,
    workspace: PathBuf,
}

impl<B> LocalJudgeGraphExecutor<B> {
    #[must_use]
    pub fn new(backend: Arc<B>, runtime: RuntimePlan, workspace: impl Into<PathBuf>) -> Self {
        Self {
            backend,
            runtime,
            workspace: workspace.into(),
        }
    }
}

#[async_trait]
impl<B: NativeV2CliBackend> JudgeGraphExecutor for LocalJudgeGraphExecutor<B> {
    async fn execute(
        &self,
        template: &JudgeTemplate,
        model: &str,
        input: Value,
    ) -> Result<Value, EvalError> {
        let runtime = runtime_with_model(&self.runtime, model)?;
        let identity = uuid::Uuid::now_v7().simple().to_string();
        let run_id = RunId::new(format!("eval-judge-{identity}"));
        let title = RunTitle::new("Eval judge")
            .map_err(|error| EvalError::Command(format!("judge title is invalid: {error}")))?;
        let submission_key = IdempotencyKey::new(format!("eval-judge-{identity}"))
            .map_err(|error| EvalError::Command(format!("judge request is invalid: {error}")))?;
        let directory = tempfile::Builder::new()
            .prefix("zeroshot-judge-")
            .tempdir()
            .map_err(|error| EvalError::io(std::env::temp_dir(), error))?;
        let intent = TargetRunIntent {
            title,
            graph: template.graph.clone(),
            initial_input: input,
            runtime,
            environment: None,
            branch: None,
            submission_key,
        };
        let requirements = intent.connection_requirements();
        if !requirements.is_empty() {
            let keys = requirements
                .keys()
                .map(|key| key.as_str())
                .collect::<Vec<_>>();
            return Err(EvalError::Command(format!(
                "judge runtime plan declares connection requirements {keys:?}; eval judge does not resolve connection values"
            )));
        }
        let request = PreparedRunRequest {
            run_id: run_id.clone(),
            intent,
            connections: BTreeMap::new(),
            github_token: None,
            git_credentials: None,
            cwd: Some(directory.path().to_path_buf()),
            source_connection: None,
            source: None,
            profile: None,
        };
        let scratch = std::fs::canonicalize(directory.path())
            .map_err(|error| EvalError::io(directory.path(), error))?;
        let live_workspace = std::fs::canonicalize(&self.workspace)
            .map_err(|error| EvalError::io(&self.workspace, error))?;
        if scratch.starts_with(&live_workspace) {
            return Err(EvalError::Command(format!(
                "judge scratch {} is inside the live workspace {}",
                scratch.display(),
                live_workspace.display()
            )));
        }
        let (resolved, _) = crate::native_v2_local::prepare_local_source(
            &request,
            directory.path(),
            Path::new("git"),
        )
        .map_err(|error| {
            EvalError::Command(format!("judge scratch workspace is invalid: {error}"))
        })?;
        if resolved != scratch {
            return Err(EvalError::Command(format!(
                "judge workspace resolved to {} instead of private scratch {}",
                resolved.display(),
                scratch.display()
            )));
        }
        let runner = LocalRunner::new(Arc::clone(&self.backend));
        let artifact = run_sample(
            &runner,
            BackendSampleRequest {
                sample_id: format!("eval-judge-{identity}"),
                concurrency: 1,
                request,
            },
            directory.path(),
            |_, _| None,
        )
        .await
        .map_err(|error| EvalError::Runner(error.to_string()))?;
        if let Some(error) = artifact.capture_error.as_deref() {
            return Err(EvalError::Judge(format!(
                "judge graph capture failed: {error}"
            )));
        }
        if let Some(output) = artifact.outcome.output {
            return Ok(output);
        }
        let status = format!("{:?}", artifact.outcome.status);
        let reason = artifact
            .outcome
            .failure_reason
            .as_deref()
            .unwrap_or("no terminal output was recorded");
        Err(EvalError::Judge(format!(
            "judge graph run finished as {status}: {reason}"
        )))
    }
}
