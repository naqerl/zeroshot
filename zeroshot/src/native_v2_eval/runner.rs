use std::collections::BTreeMap;

use async_trait::async_trait;

use super::{
    CriterionResult, EvalArm, EvalCriterion, EvalError, EvalId, SampleStatus, SampleTiming,
    TokenUsage,
};

/// One arm/run pair submitted to a sample runner.
#[derive(Clone, Debug, PartialEq)]
pub struct SampleRequest {
    pub eval_id: EvalId,
    pub arm: EvalArm,
    pub run_index: u32,
    pub criteria: Vec<EvalCriterion>,
}

/// Opaque runner handle. The synthetic runner and later platform/local runners only need this.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SampleHandle {
    pub run_id: String,
    pub arm_id: String,
    pub run_index: u32,
}

/// Settled sample outcome before the store assigns storage paths.
#[derive(Clone, Debug, PartialEq)]
pub struct SampleOutcome {
    pub status: SampleStatus,
    pub timing: SampleTiming,
    pub tokens: TokenUsage,
    pub delivered_revision: Option<String>,
    pub failure_reason: Option<String>,
    pub patch: Option<SamplePatch>,
    pub criteria: BTreeMap<String, CriterionResult>,
    pub reasoning: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SamplePatch {
    pub content: String,
}

/// Execution seam filled by slice 2. Slice 1 ships only [`super::FakeRunner`].
#[async_trait]
pub trait SampleRunner: Send + Sync {
    async fn submit(&self, request: SampleRequest) -> Result<SampleHandle, EvalError>;
    async fn wait(&self, handle: SampleHandle) -> Result<SampleOutcome, EvalError>;
    async fn stop(&self, handle: SampleHandle) -> Result<(), EvalError>;
}
