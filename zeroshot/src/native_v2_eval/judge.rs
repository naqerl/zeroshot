use std::collections::BTreeMap;

use async_trait::async_trait;

use super::{CriterionResult, EvalError, EvalSample};

/// Slice 3 supplies a judge implementation. Slice 1 declares only the hook.
#[async_trait]
pub trait Judge: Send + Sync {
    async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct JudgeRequest {
    pub sample: EvalSample,
    pub criteria: Vec<super::EvalCriterion>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JudgeResult {
    pub criteria: BTreeMap<String, CriterionResult>,
    /// Reasoning keyed by criterion. Keys outside the manifest remain visible in the report.
    pub reasoning: BTreeMap<String, String>,
}
