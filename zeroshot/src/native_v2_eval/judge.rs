//! Structured judge contract, strict parser, and the extension seam for judge graphs.
//!
//! A judge returns one pass/fail verdict with a short reason for every acceptance trait of one
//! captured sample. The built-in `eval-judge` graph template produces exactly this shape:
//!
//! ```json
//! {"traits": [{"id": "tests", "pass": true, "reason": "adds src/list_test.rs covering csv"}]}
//! ```
//!
//! The judge's provider and model come only from the runtime configuration the judge graph runs
//! with; nothing here selects or records one.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{CriterionResult, EvalCriterion, EvalError, EvalSample};

/// Name of the built-in judge graph template.
pub const JUDGE_TEMPLATE_NAME: &str = "eval-judge";

const MAX_REASON_CHARS: usize = 4_096;

/// One verdict for one acceptance trait.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Judgement {
    pub pass: bool,
    pub reason: String,
}

impl Judgement {
    /// Validate a judgement constructed in-process, using the same rules as the parser.
    pub fn new(pass: bool, reason: impl Into<String>) -> Result<Self, JudgeParseError> {
        let judgement = Self {
            pass,
            reason: reason.into(),
        };
        judgement.validate("judgement")?;
        Ok(judgement)
    }

    #[must_use]
    pub const fn passed(&self) -> bool {
        self.pass
    }

    /// The stored per-criterion result. `score` is the pass indicator (1 pass, 0 fail), so report
    /// means over it are pass rates.
    #[must_use]
    pub fn as_criterion_result(&self) -> CriterionResult {
        CriterionResult {
            passed: self.pass,
            score: Some(if self.pass { 1.0 } else { 0.0 }),
            reasoning: Some(self.reason.clone()),
            errors: Vec::new(),
        }
    }

    pub(crate) fn validate(&self, path: &str) -> Result<(), JudgeParseError> {
        validate_reason(&self.reason, &format!("{path}.reason"))
    }
}

/// Parsed judge response keyed by criterion id.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JudgeResult {
    pub judgements: BTreeMap<String, Judgement>,
}

impl JudgeResult {
    #[must_use]
    pub fn criteria(&self) -> BTreeMap<String, CriterionResult> {
        self.judgements
            .iter()
            .map(|(criterion, judgement)| (criterion.clone(), judgement.as_criterion_result()))
            .collect()
    }

    #[must_use]
    pub fn reasoning(&self) -> BTreeMap<String, String> {
        self.judgements
            .iter()
            .map(|(criterion, judgement)| (criterion.clone(), judgement.reason.clone()))
            .collect()
    }
}

/// A field-named parse or validation failure. Serde details never escape this boundary.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("judge response field `{field}` is invalid: {message}")]
pub struct JudgeParseError {
    pub field: String,
    pub message: String,
}

impl JudgeParseError {
    #[must_use]
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

/// Everything a judge implementation needs for one captured sample.
#[derive(Clone, Debug, PartialEq)]
pub struct JudgeRequest {
    pub sample: EvalSample,
    pub criteria: Vec<EvalCriterion>,
    /// Captured `patch.diff` content. It is never re-run to obtain this value.
    pub patch: String,
    /// Capture-side `sample.json` content when available, otherwise the stored sample projection.
    pub sample_json: Option<Value>,
    /// The task prompt the sample implemented.
    pub task: String,
    /// Judge graph template selected for this sample.
    pub template: Option<String>,
    /// Read-only delivered checkout path captured by slice 2, when available.
    pub checkout: Option<String>,
}

impl JudgeRequest {
    #[must_use]
    pub fn new(sample: EvalSample, criteria: Vec<EvalCriterion>, patch: impl Into<String>) -> Self {
        Self {
            sample,
            criteria,
            patch: patch.into(),
            sample_json: None,
            task: String::new(),
            template: None,
            checkout: None,
        }
    }

    #[must_use]
    pub fn with_sample_json(mut self, sample_json: Value) -> Self {
        self.sample_json = Some(sample_json);
        self
    }

    #[must_use]
    pub fn with_task(mut self, task: impl Into<String>) -> Self {
        self.task = task.into();
        self
    }

    #[must_use]
    pub fn with_template(mut self, template: impl Into<String>) -> Self {
        self.template = Some(template.into());
        self
    }

    #[must_use]
    pub fn with_checkout(mut self, checkout: impl Into<String>) -> Self {
        self.checkout = Some(checkout.into());
        self
    }
}

#[async_trait]
pub trait Judge: Send + Sync {
    async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError>;
}

/// Parse and validate a judge response held in a JSON string. See [`parse_judgement_value`].
pub fn parse_judgement(
    source: &str,
    criteria: &[EvalCriterion],
) -> Result<JudgeResult, JudgeParseError> {
    let value: Value = serde_json::from_str(source)
        .map_err(|error| JudgeParseError::new("<json>", error.to_string()))?;
    parse_judgement_value(&value, criteria)
}

/// Parse and validate a judge graph output against the requested traits.
///
/// The output is `{"traits": [{"id", "pass", "reason"}]}`. Unknown fields are rejected, and every
/// expected trait must appear exactly once: an unknown, duplicate, or missing trait id is an error
/// that names the offending field.
pub fn parse_judgement_value(
    value: &Value,
    criteria: &[EvalCriterion],
) -> Result<JudgeResult, JudgeParseError> {
    let object = value
        .as_object()
        .ok_or_else(|| JudgeParseError::new("<root>", "must be a JSON object"))?;
    if let Some(key) = object.keys().find(|key| key.as_str() != "traits") {
        return Err(JudgeParseError::new(
            key,
            "unknown top-level field; expected `traits`",
        ));
    }
    let entries = object
        .get("traits")
        .ok_or_else(|| JudgeParseError::new("traits", "is required"))?
        .as_array()
        .ok_or_else(|| JudgeParseError::new("traits", "must be an array"))?;
    let expected = criteria
        .iter()
        .map(|criterion| criterion.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut judgements = BTreeMap::new();
    for (index, entry) in entries.iter().enumerate() {
        let path = format!("traits[{index}]");
        let entry = entry
            .as_object()
            .ok_or_else(|| JudgeParseError::new(&path, "must be an object"))?;
        if let Some(key) = entry
            .keys()
            .find(|key| !matches!(key.as_str(), "id" | "pass" | "reason"))
        {
            return Err(JudgeParseError::new(
                format!("{path}.{key}"),
                "unknown field; expected `id`, `pass`, and `reason`",
            ));
        }
        let id = entry
            .get("id")
            .ok_or_else(|| JudgeParseError::new(format!("{path}.id"), "is required"))?
            .as_str()
            .ok_or_else(|| JudgeParseError::new(format!("{path}.id"), "must be a string"))?;
        if !expected.contains(id) {
            return Err(JudgeParseError::new(
                format!("{path}.id"),
                format!(
                    "unknown trait {id:?}; expected one of {}",
                    expected.iter().copied().collect::<Vec<_>>().join(", ")
                ),
            ));
        }
        if judgements.contains_key(id) {
            return Err(JudgeParseError::new(
                format!("{path}.id"),
                format!("duplicate verdict for trait {id:?}"),
            ));
        }
        let pass = entry
            .get("pass")
            .ok_or_else(|| JudgeParseError::new(format!("{path}.pass"), "is required"))?
            .as_bool()
            .ok_or_else(|| JudgeParseError::new(format!("{path}.pass"), "must be a boolean"))?;
        let reason = entry
            .get("reason")
            .ok_or_else(|| JudgeParseError::new(format!("{path}.reason"), "is required"))?
            .as_str()
            .ok_or_else(|| JudgeParseError::new(format!("{path}.reason"), "must be a string"))?;
        validate_reason(reason, &format!("{path}.reason"))?;
        judgements.insert(
            id.to_owned(),
            Judgement {
                pass,
                reason: reason.to_owned(),
            },
        );
    }
    if let Some(missing) = expected
        .iter()
        .find(|criterion| !judgements.contains_key(**criterion))
    {
        return Err(JudgeParseError::new(
            "traits",
            format!("missing verdict for trait {missing:?}"),
        ));
    }
    Ok(JudgeResult { judgements })
}

fn validate_reason(reason: &str, path: &str) -> Result<(), JudgeParseError> {
    if reason.trim().is_empty() {
        return Err(JudgeParseError::new(path, "must not be empty"));
    }
    if reason.chars().count() > MAX_REASON_CHARS {
        return Err(JudgeParseError::new(
            path,
            format!("must contain at most {MAX_REASON_CHARS} characters"),
        ));
    }
    if reason
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(JudgeParseError::new(
            path,
            "must not contain control characters other than whitespace",
        ));
    }
    Ok(())
}
