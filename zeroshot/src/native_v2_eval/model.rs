use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{EvalError, EvalManifest, EvalReport};

/// Stable directory identity for one eval.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct EvalId(String);

impl EvalId {
    pub fn new(value: impl Into<String>) -> Result<Self, EvalError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            && value
                .as_bytes()
                .first()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && value != "."
            && value != "..";
        if valid {
            Ok(Self(value))
        } else {
            Err(EvalError::InvalidId(value))
        }
    }

    /// Generate a time-ordered id from the eval name without creating a filesystem path.
    pub fn generate(name: &str) -> Result<Self, EvalError> {
        let mut slug = name
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() {
                    character.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>();
        while slug.contains("--") {
            slug = slug.replace("--", "-");
        }
        let slug = slug.trim_matches('-');
        let slug = if slug.is_empty() {
            "eval".to_owned()
        } else {
            slug.chars().take(48).collect()
        };
        let id = format!("{slug}-{}", uuid::Uuid::now_v7().simple());
        Self::new(id)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EvalId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Everything needed to inspect or rescore an eval, reconstructed from the store.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalData {
    pub id: EvalId,
    pub created_at_ms: u64,
    pub manifest: EvalManifest,
    pub samples: Vec<EvalSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<EvalReport>,
}

impl EvalData {
    #[must_use]
    pub fn sample_counts(&self) -> SampleCounts {
        let mut counts = SampleCounts::default();
        for sample in &self.samples {
            match sample.status {
                SampleStatus::Succeeded => counts.succeeded += 1,
                SampleStatus::Failed => counts.failed += 1,
                SampleStatus::Skipped => counts.skipped += 1,
            }
        }
        counts
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SampleCounts {
    pub succeeded: u64,
    pub failed: u64,
    pub skipped: u64,
}

/// One durable sample record. A failed execution is a first-class record, never an omission.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalSample {
    pub id: String,
    pub run_id: String,
    pub arm_id: String,
    pub run_index: u32,
    pub status: SampleStatus,
    pub timing: SampleTiming,
    pub tokens: TokenUsage,
    /// Implementation cost in USD when the runner or capture recorded it directly. When absent,
    /// the report prices `tokens` with the manifest's `pricing` table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch: Option<PatchArtifact>,
    #[serde(default)]
    pub criteria: BTreeMap<String, CriterionResult>,
    #[serde(default)]
    pub reasoning: BTreeMap<String, String>,
    /// Judge graph template used for this sample, when it was judged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_template: Option<String>,
    /// Read-only delivered tree captured by slice 2, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_checkout: Option<String>,
    /// Executor that produced the judgement: `fixture`, `graph`, or a caller-defined label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_executor: Option<String>,
    /// Facts the source did not expose, such as a missing cache split or queue time, recorded by
    /// `eval import-baseline` instead of being guessed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleStatus {
    Succeeded,
    Failed,
    Skipped,
}

impl SampleStatus {
    #[must_use]
    pub const fn is_attempted(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SampleTiming {
    pub started_at_ms: u64,
    pub wall_clock_ms: u64,
    pub queue_wait_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
    /// All cache tokens (`cacheRead + cacheWrite` when the split is known).
    pub cache: u64,
    /// Cache-read input tokens, when the provider reported the split.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_read: u64,
    /// Cache-creation input tokens, when the provider reported the split.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_write: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl TokenUsage {
    #[must_use]
    pub fn total(self) -> u64 {
        self.input
            .saturating_add(self.output)
            .saturating_add(self.cache)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CriterionResult {
    pub passed: bool,
    /// Judge verdict as a number: 1 for pass, 0 for fail. Older captures may carry other scores.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    /// Concrete defects reported by the judge. Never provider or serialization noise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PatchArtifact {
    /// Path relative to the eval directory.
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
