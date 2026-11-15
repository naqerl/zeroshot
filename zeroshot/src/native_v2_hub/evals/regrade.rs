//! Human regrades of a trial's blind quality.
//!
//! A regrade is an append-only record. It never rewrites the bundle that holds the measured
//! evidence, so the automated quality stays readable beside every human override.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// `POST /v1/hub/evals/trials/{trialId}/regrade`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CreateRegrade {
    pub quality: f64,
    pub reviewer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// One appended human quality for a trial.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Regrade {
    pub regrade_id: String,
    pub trial_id: String,
    pub quality: f64,
    pub reviewer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub created_at_ms: u64,
}

impl PartialEq for Regrade {
    fn eq(&self, other: &Self) -> bool {
        self.regrade_id == other.regrade_id
            && self.trial_id == other.trial_id
            && self.quality.to_bits() == other.quality.to_bits()
            && self.reviewer == other.reviewer
            && self.note == other.note
            && self.created_at_ms == other.created_at_ms
    }
}

impl Eq for Regrade {}

impl Regrade {
    pub(crate) fn view(&self) -> Value {
        json!({
            "regradeId": self.regrade_id,
            "trialId": self.trial_id,
            "quality": self.quality,
            "reviewer": self.reviewer,
            "note": self.note,
            "createdAtMs": self.created_at_ms,
        })
    }
}
