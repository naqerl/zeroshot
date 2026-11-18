//! Standalone eval runner for Zeroshot graphs.
//!
//! An eval is described by one strict manifest. The runner persists every sample record and then
//! derives the report from those records, so `show`, `report`, and `score` never depend on state
//! carried by the process that executed the eval. Slices 2 and 3 fill in real sample runners and a
//! judge; this module owns the manifest/store/report/scoring core plus the synthetic runner.

mod capacity;
mod cli;
mod error;
mod fake;
mod judge;
mod manifest;
mod model;
mod report;
mod runner;
mod scorer;
mod store;

pub use capacity::CapacitySource;
pub use cli::{execute, EvalAction, EvalCommand, EvalRunCommand};
pub use error::EvalError;
pub use fake::FakeRunner;
pub use judge::{Judge, JudgeRequest, JudgeResult};
pub use manifest::{EvalArm, EvalCriterion, EvalManifest, EvalOptions, FakeOptions, MANIFEST_SCHEMA};
pub use model::{
    CriterionResult, EvalData, EvalId, EvalSample, PatchArtifact, SampleCounts, SampleStatus,
    SampleTiming, TokenUsage,
};
pub use report::{
    ArmCostTime, ArmTraitSummary, CostTimeComparison, EvalReport, QualityCostFit, TraitCell,
    TraitColumn, TraitMatrix, TraitMatrixRow, TraitSummary, UnmatchedReasoning, REPORT_SCHEMA,
    render_markdown,
};
pub use runner::{SampleHandle, SampleOutcome, SamplePatch, SampleRequest, SampleRunner};
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
    })
    .await
}

#[cfg(test)]
#[path = "native_v2_eval/tests.rs"]
mod tests;
