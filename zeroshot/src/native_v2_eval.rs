//! Standalone eval runner for Zeroshot graphs.
//!
//! An eval is described by one strict manifest. The runner persists every sample record and then
//! derives the report from those records, so `show`, `report`, and `score` never depend on state
//! carried by the process that executed the eval. This module owns the manifest/store/report/scoring
//! core, the capacity/capture/resume support, and one `Submit`/`Wait`/`Stop` runner seam implemented
//! by the platform, local, and synthetic runners.

mod capacity;
mod capture;
mod cli;
mod error;
mod fake;
mod judge;
mod judge_backend;
mod judge_runner;
mod judge_template;
mod manifest;
mod model;
mod report;
mod resume;
mod runner;
mod scorer;
mod store;

pub use capacity::{
    Capacity, CapacityError, CapacitySource, CapacitySourceKind, StaticCapacity, resolve_capacity,
};
pub use capture::{
    CaptureCredentials, CaptureError, CaptureRequest, CapturedSample, DeliveryReceipt,
    JudgeCheckout, capture_patch, checkout_patch,
};
pub use cli::{
    BackendRunRequest, EvalAction, EvalCommand, EvalJudgeCapturesCommand, EvalJudgeCommand,
    EvalRunCommand, ManifestRunRequest, RunnerSelection, execute, run_manifest_with_backend,
    run_manifest_with_runner, select_runner,
};
pub use error::EvalError;
pub use fake::{FakeFixtureRequest, FakeRunner, FakeSampleRequest};
pub use judge::{
    JUDGE_PASS_SCORE, JUDGE_SCORE_MAX, JUDGE_SCORE_MIN, JUDGE_TEMPLATE_NAME, Judge,
    JudgeParseError, JudgeRequest, JudgeResult, Judgement, parse_judgement,
};
pub use judge_backend::{LocalJudgeGraphExecutor, load_judge_runtime, runtime_with_model};
pub use judge_runner::{
    CapturedSampleInput, JudgeOptions, JudgeRunReport, JudgedCaptures, JudgedSample,
    JudgedSampleSummary, captured_sample_from_directory, judge_captured_samples, judge_eval,
    judge_model_for,
};
pub use judge_template::{
    FixtureJudgeGraphExecutor, JUDGE_TEMPLATE_JSON, JudgeGraphExecutor, JudgeTemplate,
    TemplateJudge, judge_input, judge_input_for_template, resolve_judge_template,
};
pub use manifest::{EvalArm, EvalCriterion, EvalManifest, EvalOptions, FakeOptions, MANIFEST_SCHEMA};
pub use model::{
    CriterionResult, EvalData, EvalId, EvalSample, PatchArtifact, SampleCounts, SampleStatus,
    SampleTiming, TokenUsage,
};
pub use report::{
    ArmCostTime, ArmTraitSummary, CostTimeComparison, EvalReport, QualityCostFit, TraitCell,
    TraitColumn, TraitMatrix, TraitMatrixRow, TraitSummary, UnmatchedReasoning, XyComparison,
    XyOutlier, XyTrait, REPORT_SCHEMA, render_markdown,
};
pub use resume::{
    CapacityWarning, RESUME_FILE, ResumeAction, ResumeError, ResumeJournal, ResumeSample,
    SampleResumeState,
};
pub use runner::{
    BackendSampleRequest, EvalRunError, EvalRunReport, EvalRunRequest, EvalSamplePlan, LocalRunner,
    PlatformRunner, ResumeCredentials, SAMPLE_FILE, SampleArtifact, SampleExecution,
    SampleExecutionError, SampleFailure, SampleOutcome, SamplePatch, SampleRequest, SampleRun,
    SampleRunner, SampleRunnerError, SampleTimings, run_eval, run_sample,
};
pub use scorer::{NoneScorer, QualityVsCostScorer, Scorer, ScorerInput, ScorerOutput, ScorerRegistry};
pub use store::{EvalStore, EvalSummary};

/// Runs a manifest end to end through the synthetic runner.
pub async fn run_fixture(
    manifest_path: impl Into<std::path::PathBuf>,
    workspace: impl Into<std::path::PathBuf>,
    id: Option<String>,
) -> Result<EvalData, EvalError> {
    cli::run_manifest_through_fake(cli::EvalRunCommand {
        manifest: manifest_path.into(),
        directory: workspace.into(),
        id,
        scorer: None,
        judge_template: None,
        judge_model: None,
    })
    .await
}

#[cfg(test)]
#[path = "native_v2_eval/tests.rs"]
mod tests;

/// Current wall-clock time in milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> u64 {
    model::now_ms()
}

/// Write JSON through a temporary file in the same directory so a crash never leaves a half file.
fn write_json_atomic(path: &std::path::Path, value: &impl serde::Serialize) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?;
    write_bytes_atomic(path, &bytes)
}

fn write_bytes_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    if !bytes.ends_with(b"\n") {
        temporary.write_all(b"\n")?;
    }
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}
