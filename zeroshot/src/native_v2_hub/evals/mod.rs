//! `zeroshot hub evals` slice 1: tasks, arms, trials, and bundles.
//!
//! This is deliberately a separate subsystem from [`super::eval`]. The older eval module keeps
//! owning suites, cases, runs, and reports; this module only adds the pieces slice 1 needs and
//! reuses the old judge/check host-command implementation when a bundle needs grading evidence.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub(crate) mod arm;
pub(crate) mod bundle;
pub(crate) mod service;
pub(crate) mod task;
pub(crate) mod trial;

pub(crate) use arm::{Arm, CreateArm};
pub(crate) use task::{CreateTask, EvalTask};
pub(crate) use trial::{CreateEvalRun, EvalV2Run, Trial};

/// Stored evals-v2 state. It is a sibling of the old `evals` field so both subsystems can live in
/// one `state.json` without sharing a representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct EvalsState {
    #[serde(default)]
    pub tasks: Vec<EvalTask>,
    #[serde(default)]
    pub arms: Vec<Arm>,
    #[serde(default)]
    pub runs: Vec<EvalV2Run>,
    #[serde(default)]
    pub trials: Vec<Trial>,
}

impl EvalsState {
    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
            && self.arms.is_empty()
            && self.runs.is_empty()
            && self.trials.is_empty()
    }

    pub(crate) fn task(&self, id_or_hash: &str) -> Option<&EvalTask> {
        self.tasks
            .iter()
            .find(|task| task.task_id == id_or_hash || task.task_hash == id_or_hash)
    }

    pub(crate) fn arm(&self, id_or_hash: &str) -> Option<&Arm> {
        self.arms
            .iter()
            .find(|arm| arm.arm_id == id_or_hash || arm.arm_hash == id_or_hash)
    }

    pub(crate) fn run(&self, run_id: &str) -> Option<&EvalV2Run> {
        self.runs.iter().find(|run| run.run_id == run_id)
    }

    pub(crate) fn trial(&self, trial_id: &str) -> Option<&Trial> {
        self.trials.iter().find(|trial| trial.trial_id == trial_id)
    }
}

/// JSON with object keys sorted, so equal documents hash equally.
pub(crate) fn canonical(value: &Value) -> String {
    fn sort(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted = map
                    .iter()
                    .map(|(key, value)| (key.clone(), sort(value)))
                    .collect::<BTreeMap<_, _>>();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    sort(value).to_string()
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The 12 hex digits every evals-v2 public identifier uses.
pub(crate) fn short_hash(text: &str) -> String {
    sha256_hex(text.as_bytes())[..12].to_owned()
}

/// Hash a request value after canonicalization.
pub(crate) fn request_hash<T: Serialize>(request: &T) -> String {
    let value = serde_json::to_value(request).unwrap_or(Value::Null);
    sha256_hex(canonical(&value).as_bytes())
}

/// `ev-<12 hex>` from a task hash.
pub(crate) fn task_id(task_hash: &str) -> String {
    format!("ev-{}", &task_hash[..12.min(task_hash.len())])
}

/// `ea-<12 hex>` from an arm hash.
pub(crate) fn arm_id(arm_hash: &str) -> String {
    format!("ea-{}", &arm_hash[..12.min(arm_hash.len())])
}

/// `et-<12 hex>` from run, task, arm, and seed.
pub(crate) fn trial_id(run_id: &str, task_hash: &str, arm_hash: &str, seed: u32) -> String {
    format!(
        "et-{}",
        short_hash(&format!("{run_id}\n{task_hash}\n{arm_hash}\n{seed}"))
    )
}

/// Deterministic hub job ID of one trial.
pub(crate) fn trial_job_id(trial_id: &str) -> String {
    format!(
        "hj_{}",
        &sha256_hex(format!("zeroshot.evals.v2\n{trial_id}").as_bytes())[..32]
    )
}
