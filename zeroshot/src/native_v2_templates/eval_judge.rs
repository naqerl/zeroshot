//! Independent per-trait judging of one delivered eval sample.
//!
//! `eval run` expands this template into the judge graph it names, so the authored contract lives
//! here rather than in the eval document: a sample's task, acceptance traits, and delivered patch
//! go in, and one verdict per trait comes out. The judge works in the sample's own checkout, so it
//! can see the code around the patch instead of trusting the diff alone.
use openengine_cluster_protocol::{GraphNode, SucceedNode};

use super::*;

const JUDGE: &str = "judge";
const DONE: &str = "done";
const TASK: &str = "task";
const TRAITS: &str = "traits";
const PATCH: &str = "patch";
const VERDICTS: &str = "verdicts";

const JUDGE_INSTRUCTIONS: &str = "You are an independent evaluator of a code change. The input has \
     `task` (what the change was asked to do), `traits` (a JSON array of acceptance traits, each \
     {\"id\", \"description\"}), and `patch` (the unified diff of the delivered change). Your \
     working directory is the delivered checkout for this sample: inspect the surrounding code, \
     call sites, and tests there in addition to the patch, because the patch alone can hide \
     regressions outside its changed files. Treat the checkout as read-only. Do not modify any \
     files, do not run delivery, and do not repair the patch. Judge the patch and checkout against \
     every trait. Return `verdicts` as an array with exactly one entry per input trait, in input \
     order: `id` is the trait id copied exactly, `pass` is true only when the change clearly \
     satisfies the trait and false otherwise (including when the evidence is missing), and \
     `reason` is one or two sentences that cite concrete files or lines in the patch or checkout. \
     Do not add, rename, or skip traits.";

/// One authored acceptance trait and the judge's verdict for it.
fn verdict_type() -> Result<PayloadType, BuiltinTemplateError> {
    record_type(vec![
        ("id", PayloadType::String, true),
        ("pass", PayloadType::Boolean, true),
        ("reason", PayloadType::String, true),
    ])
}

fn verdicts_type() -> Result<PayloadType, BuiltinTemplateError> {
    Ok(array(verdict_type()?))
}

/// The sample a judge receives: everything a judge needs to decide the traits, and nothing else.
fn sample_type() -> Result<PayloadType, BuiltinTemplateError> {
    record_type(vec![
        (TASK, PayloadType::String, true),
        (TRAITS, PayloadType::String, true),
        (PATCH, PayloadType::String, true),
    ])
}

/// Sample fields carried through the run plus the verdicts the judge writes.
///
/// `verdicts` is not part of the initial input: an array has a deterministic empty value, so a
/// caller supplies only the sample and the reducer materializes the collection the judge fills.
fn judge_state() -> Result<PayloadType, BuiltinTemplateError> {
    let mut fields = vec![
        (TASK, PayloadType::String, true),
        (TRAITS, PayloadType::String, true),
        (PATCH, PayloadType::String, true),
    ];
    fields.push((VERDICTS, verdicts_type()?, true));
    record_type(fields)
}

fn judge_step() -> Result<GraphNode, BuiltinTemplateError> {
    Ok(GraphNode::Step(StepNode {
        name: node_name(JUDGE)?,
        worker: worker_ref(crate::native_v2_eval::EVAL_JUDGE_WORKER_REF)?,
        instructions: Some(instructions(JUDGE_INSTRUCTIONS)?),
        input: sample_type()?,
        output: record_type(vec![(VERDICTS, verdicts_type()?, true)])?,
        input_bindings: vec![
            state_input(TASK, TASK)?,
            state_input(TRAITS, TRAITS)?,
            state_input(PATCH, PATCH)?,
        ],
        write_bindings: vec![output_write(JUDGE, VERDICTS, VERDICTS)?],
        timeout_ms: None,
        attempts: positive(crate::native_v2_eval::EVAL_JUDGE_ATTEMPTS)?,
    }))
}

fn done() -> Result<GraphNode, BuiltinTemplateError> {
    Ok(GraphNode::Succeed(SucceedNode {
        name: node_name(DONE)?,
        output: record_type(vec![(VERDICTS, verdicts_type()?, true)])?,
        bindings: vec![state_input(VERDICTS, VERDICTS)?],
    }))
}

pub(super) fn eval_judge_graph() -> Result<GraphSpec, BuiltinTemplateError> {
    let state = judge_state()?;
    let route = choice(
        "judge_result",
        state.clone(),
        vec![ChoiceBranch {
            when: executable_error_guard(JUDGE)?,
            node: fail("judge_failed", "judge_failed")?,
        }],
        Some(done()?),
    )?;
    graph(
        sample_type()?,
        sequence("run", state, vec![judge_step()?, route], Vec::new())?,
    )
}
