//! Trial bundles: the durable evidence written before a trial's workspace is discarded.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::super::error::HubError;
use super::super::service::{create_private_directory, replace_private};
use super::super::usage::JobUsage;
use super::trial::Trial;

/// `bundle.json` of one trial.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Bundle {
    pub trial_id: String,
    pub run_id: String,
    pub task_hash: String,
    pub arm_hash: String,
    pub task_id: String,
    pub arm_id: String,
    pub seed: u32,
    pub pinned_revision: String,
    pub model_ids: Vec<String>,
    /// The result diff the run reported; empty when the run reported none.
    pub diff: String,
    /// Whatever the acceptance node reported as its checks.
    pub checks: Value,
    pub failure_kind: Option<String>,
    pub token_usage: Option<JobUsage>,
    pub duration_ms: Option<u64>,
    pub queue_wait_ms: Option<u64>,
    /// Paths of the logs kept with this bundle.
    pub session_log_paths: Vec<String>,
}

/// `<hub dir>/evals/<runId>/<taskId>/<armId>/<trialId>/bundle.json`.
pub(crate) fn path(hub_directory: &Path, trial: &Trial) -> PathBuf {
    hub_directory
        .join("evals")
        .join(&trial.run_id)
        .join(&trial.task_id)
        .join(&trial.arm_id)
        .join(&trial.trial_id)
        .join("bundle.json")
}

impl Bundle {
    /// Write the bundle atomically and return its path. The parent folder is private.
    pub(crate) fn write(&self, path: &Path) -> Result<(), HubError> {
        let folder = path.parent().ok_or_else(|| {
            HubError::InvalidState(format!("bundle path {} has no folder", path.display()))
        })?;
        create_private_directory(folder)?;
        let mut document = serde_json::to_value(self)
            .map_err(|error| HubError::storage("trial bundle could not be encoded", error))?;
        if let Some(object) = document.as_object_mut() {
            // The same paths under a second, descriptive name so readers of either shape agree.
            object.insert("sessionLogs".to_owned(), json!(self.session_log_paths));
        }
        let mut bytes = serde_json::to_vec_pretty(&document)
            .map_err(|error| HubError::storage("trial bundle could not be encoded", error))?;
        bytes.push(b'\n');
        replace_private(path, &bytes)
    }
}
