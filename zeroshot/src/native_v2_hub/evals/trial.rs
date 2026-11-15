//! An evals-v2 run and its trials. A trial is one submitted hub job.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::super::usage::JobUsage;

/// `POST /v1/hub/evals/runs`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CreateEvalRun {
    pub tasks: Vec<String>,
    pub arms: Vec<String>,
    #[serde(default = "one")]
    pub repeat: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<Value>,
}

const fn one() -> u32 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct EvalV2Run {
    pub run_id: String,
    pub task_hashes: Vec<String>,
    pub arm_hashes: Vec<String>,
    pub repeat: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<Value>,
    pub created_at_ms: u64,
    #[serde(default)]
    pub trial_ids: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TrialState {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Trial {
    pub trial_id: String,
    pub run_id: String,
    pub task_hash: String,
    pub arm_hash: String,
    pub task_id: String,
    pub arm_id: String,
    pub seed: u32,
    pub state: TrialState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<JobUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_path: Option<PathBuf>,
    /// The bundle has been written at least once. A later missing file is reported as failed
    /// instead of being rebuilt from current run output.
    #[serde(default)]
    pub bundle_written: bool,
}

impl EvalV2Run {
    pub(crate) fn view(&self) -> Value {
        serde_json::json!({
            "runId": self.run_id,
            "taskHashes": self.task_hashes,
            "armHashes": self.arm_hashes,
            "repeat": self.repeat,
            "judge": self.judge,
            "createdAtMs": self.created_at_ms,
            "trialIds": self.trial_ids,
        })
    }
}

impl Trial {
    pub(crate) fn view(&self) -> Value {
        serde_json::json!({
            "trialId": self.trial_id,
            "runId": self.run_id,
            "taskHash": self.task_hash,
            "armHash": self.arm_hash,
            "taskId": self.task_id,
            "armId": self.arm_id,
            "seed": self.seed,
            "state": self.state,
            "jobId": self.job_id,
            "failureKind": self.failure_kind,
            "tokenUsage": self.token_usage,
            "durationMs": self.duration_ms,
            "queueWaitMs": self.queue_wait_ms,
            "bundlePath": self.bundle_path,
            "bundleWritten": self.bundle_written,
        })
    }
}
