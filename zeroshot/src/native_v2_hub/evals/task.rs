//! A stored evals-v2 task and the request that creates it.

use serde::{Deserialize, Serialize};

use super::{request_hash, task_id};

/// A cost kept bit-for-bit comparable so hub state can derive `Eq`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialOrd)]
#[serde(transparent)]
pub(crate) struct Cost(pub f64);

impl PartialEq for Cost {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for Cost {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct BaselineMetrics {
    pub cost_usd: Cost,
    pub duration_ms: u64,
}

/// `POST /v1/hub/evals/tasks`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CreateTask {
    pub title: String,
    pub prompt: String,
    #[serde(default)]
    pub baseline_commits: Vec<String>,
    pub pinned_revision: String,
    pub baseline: BaselineMetrics,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct EvalTask {
    pub task_id: String,
    pub task_hash: String,
    pub title: String,
    pub prompt: String,
    #[serde(default)]
    pub baseline_commits: Vec<String>,
    pub pinned_revision: String,
    pub baseline: BaselineMetrics,
    pub created_at_ms: u64,
}

impl CreateTask {
    pub(crate) fn hash(&self) -> String {
        request_hash(self)
    }
}

impl EvalTask {
    pub(crate) fn from_request(request: &CreateTask, created_at_ms: u64) -> Self {
        let task_hash = request.hash();
        Self {
            task_id: task_id(&task_hash),
            task_hash,
            title: request.title.clone(),
            prompt: request.prompt.clone(),
            baseline_commits: request.baseline_commits.clone(),
            pinned_revision: request.pinned_revision.clone(),
            baseline: request.baseline.clone(),
            created_at_ms,
        }
    }

    pub(crate) fn view(&self) -> serde_json::Value {
        serde_json::json!({
            "taskId": self.task_id,
            "taskHash": self.task_hash,
            "title": self.title,
            "prompt": self.prompt,
            "baselineCommits": self.baseline_commits,
            "pinnedRevision": self.pinned_revision,
            "baseline": self.baseline,
            "createdAtMs": self.created_at_ms,
        })
    }
}
