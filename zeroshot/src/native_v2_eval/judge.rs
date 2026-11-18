//! Structured judge contract, strict parser, and the extension seam for model judges.
//!
//! A judge returns one judgement per manifest criterion for one captured sample. The score uses
//! the documented inclusive 1..=5 scale, where 3 is the pass threshold. Reasoning is short prose
//! that refers to the captured patch, and errors lists concrete defects rather than parser noise.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{CriterionResult, EvalCriterion, EvalError, EvalSample};

/// Lowest accepted judge score.
pub const JUDGE_SCORE_MIN: f64 = 1.0;
/// Highest accepted judge score.
pub const JUDGE_SCORE_MAX: f64 = 5.0;
/// Score at or above which a criterion passes.
pub const JUDGE_PASS_SCORE: f64 = 3.0;
/// Name of the built-in judge graph template.
pub const JUDGE_TEMPLATE_NAME: &str = "eval-judge";

const MAX_REASONING_CHARS: usize = 4_096;
const MAX_ERRORS: usize = 64;
const MAX_ERROR_CHARS: usize = 1_024;

/// One model judgement for one criterion.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Judgement {
    pub score: f64,
    pub reasoning: String,
    #[serde(default)]
    pub errors: Vec<String>,
}

impl Judgement {
    /// Validate a judgement constructed in-process, using the same rules as the parser.
    pub fn new(
        score: f64,
        reasoning: impl Into<String>,
        errors: Vec<String>,
    ) -> Result<Self, JudgeParseError> {
        let judgement = Self {
            score,
            reasoning: reasoning.into(),
            errors,
        };
        judgement.validate("judgement")?;
        Ok(judgement)
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.score >= JUDGE_PASS_SCORE
    }

    #[must_use]
    pub fn as_criterion_result(&self) -> CriterionResult {
        CriterionResult {
            passed: self.passed(),
            score: Some(self.score),
            reasoning: Some(self.reasoning.clone()),
            errors: self.errors.clone(),
        }
    }

    pub(crate) fn validate(&self, path: &str) -> Result<(), JudgeParseError> {
        validate_score(self.score, &format!("{path}.score"))?;
        validate_reasoning(&self.reasoning, &format!("{path}.reasoning"))?;
        validate_errors(&self.errors, &format!("{path}.errors"))
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
            .map(|(criterion, judgement)| (criterion.clone(), judgement.reasoning.clone()))
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
    /// Provider-owned model identifier selected for this sample.
    pub model: String,
    /// Judge graph template selected for this sample.
    pub template: Option<String>,
    /// Read-only delivered checkout path captured by slice 2, when available.
    pub checkout: Option<String>,
}

impl JudgeRequest {
    #[must_use]
    pub fn new(
        sample: EvalSample,
        criteria: Vec<EvalCriterion>,
        patch: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            sample,
            criteria,
            patch: patch.into(),
            sample_json: None,
            model: model.into(),
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

/// Parse and validate a judge model response against the requested criteria.
///
/// The top-level object carries a `judgements` array (the plural `criteria` alias and the
/// judgement-key aliases `criterion`/`id` are accepted for interoperability). Unknown top-level
/// or per-judgement fields are rejected, and every expected criterion must appear exactly once.
pub fn parse_judgement(
    source: &str,
    criteria: &[EvalCriterion],
) -> Result<JudgeResult, JudgeParseError> {
    let value: Value = serde_json::from_str(source)
        .map_err(|error| JudgeParseError::new("<json>", error.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| JudgeParseError::new("<root>", "must be a JSON object"))?;
    for key in object.keys() {
        if key != "judgements" && key != "criteria" {
            return Err(JudgeParseError::new(
                key,
                "unknown top-level field; expected `judgements`",
            ));
        }
    }
    if object.contains_key("judgements") && object.contains_key("criteria") {
        return Err(JudgeParseError::new(
            "judgements",
            "declare either `judgements` or `criteria`, not both",
        ));
    }
    let entries = object
        .get("judgements")
        .or_else(|| object.get("criteria"))
        .ok_or_else(|| JudgeParseError::new("judgements", "is required"))?
        .as_array()
        .ok_or_else(|| JudgeParseError::new("judgements", "must be an array"))?;
    let expected = criteria
        .iter()
        .map(|criterion| criterion.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut judgements = BTreeMap::new();
    for (index, entry) in entries.iter().enumerate() {
        let path = format!("judgements[{index}]");
        let entry = entry
            .as_object()
            .ok_or_else(|| JudgeParseError::new(&path, "must be an object"))?;
        for key in entry.keys() {
            if !matches!(
                key.as_str(),
                "criterion" | "id" | "score" | "reasoning" | "errors"
            ) {
                return Err(JudgeParseError::new(
                    format!("{path}.{key}"),
                    "unknown field",
                ));
            }
        }
        if entry.contains_key("criterion") && entry.contains_key("id") {
            return Err(JudgeParseError::new(
                format!("{path}.criterion"),
                "declare either `criterion` or `id`, not both",
            ));
        }
        let criterion = entry
            .get("criterion")
            .or_else(|| entry.get("id"))
            .ok_or_else(|| JudgeParseError::new(format!("{path}.criterion"), "is required"))?
            .as_str()
            .ok_or_else(|| JudgeParseError::new(format!("{path}.criterion"), "must be a string"))?;
        if criterion.trim().is_empty() {
            return Err(JudgeParseError::new(
                format!("{path}.criterion"),
                "must not be empty",
            ));
        }
        if !expected.contains(criterion) {
            return Err(JudgeParseError::new(
                format!("{path}.criterion"),
                format!(
                    "unknown criterion {criterion:?}; expected one of {}",
                    expected.iter().copied().collect::<Vec<_>>().join(", ")
                ),
            ));
        }
        if judgements.contains_key(criterion) {
            return Err(JudgeParseError::new(
                format!("{path}.criterion"),
                format!("duplicate judgement for criterion {criterion:?}"),
            ));
        }
        let score = entry
            .get("score")
            .ok_or_else(|| JudgeParseError::new(format!("{path}.score"), "is required"))?
            .as_f64()
            .ok_or_else(|| JudgeParseError::new(format!("{path}.score"), "must be a number"))?;
        validate_score(score, &format!("{path}.score"))?;
        let reasoning = entry
            .get("reasoning")
            .ok_or_else(|| JudgeParseError::new(format!("{path}.reasoning"), "is required"))?
            .as_str()
            .ok_or_else(|| JudgeParseError::new(format!("{path}.reasoning"), "must be a string"))?;
        validate_reasoning(reasoning, &format!("{path}.reasoning"))?;
        let errors = parse_errors(
            entry
                .get("errors")
                .ok_or_else(|| JudgeParseError::new(format!("{path}.errors"), "is required"))?,
            &format!("{path}.errors"),
        )?;
        judgements.insert(
            criterion.to_owned(),
            Judgement {
                score,
                reasoning: reasoning.to_owned(),
                errors,
            },
        );
    }
    if let Some(missing) = expected
        .iter()
        .find(|criterion| !judgements.contains_key(**criterion))
    {
        return Err(JudgeParseError::new(
            "judgements",
            format!("missing judgement for criterion {missing:?}"),
        ));
    }
    Ok(JudgeResult { judgements })
}

fn parse_errors(value: &Value, path: &str) -> Result<Vec<String>, JudgeParseError> {
    let entries = value
        .as_array()
        .ok_or_else(|| JudgeParseError::new(path, "must be an array of strings"))?;
    if entries.len() > MAX_ERRORS {
        return Err(JudgeParseError::new(
            path,
            format!("must contain at most {MAX_ERRORS} entries"),
        ));
    }
    let mut errors = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let error = entry
            .as_str()
            .ok_or_else(|| JudgeParseError::new(format!("{path}[{index}]"), "must be a string"))?;
        if error.trim().is_empty() {
            return Err(JudgeParseError::new(
                format!("{path}[{index}]"),
                "must not be empty",
            ));
        }
        if error.chars().count() > MAX_ERROR_CHARS {
            return Err(JudgeParseError::new(
                format!("{path}[{index}]"),
                format!("must contain at most {MAX_ERROR_CHARS} characters"),
            ));
        }
        if error
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(JudgeParseError::new(
                format!("{path}[{index}]"),
                "must not contain control characters other than whitespace",
            ));
        }
        errors.push(error.to_owned());
    }
    Ok(errors)
}

fn validate_score(score: f64, path: &str) -> Result<(), JudgeParseError> {
    if !score.is_finite() || !(JUDGE_SCORE_MIN..=JUDGE_SCORE_MAX).contains(&score) {
        return Err(JudgeParseError::new(
            path,
            format!("must be between {JUDGE_SCORE_MIN} and {JUDGE_SCORE_MAX} inclusive"),
        ));
    }
    Ok(())
}

fn validate_reasoning(reasoning: &str, path: &str) -> Result<(), JudgeParseError> {
    if reasoning.trim().is_empty() {
        return Err(JudgeParseError::new(path, "must not be empty"));
    }
    if reasoning.chars().count() > MAX_REASONING_CHARS {
        return Err(JudgeParseError::new(
            path,
            format!("must contain at most {MAX_REASONING_CHARS} characters"),
        ));
    }
    if reasoning
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

fn validate_errors(errors: &[String], path: &str) -> Result<(), JudgeParseError> {
    if errors.len() > MAX_ERRORS {
        return Err(JudgeParseError::new(
            path,
            format!("must contain at most {MAX_ERRORS} entries"),
        ));
    }
    for (index, error) in errors.iter().enumerate() {
        if error.trim().is_empty() {
            return Err(JudgeParseError::new(
                format!("{path}[{index}]"),
                "must not be empty",
            ));
        }
        if error.chars().count() > MAX_ERROR_CHARS {
            return Err(JudgeParseError::new(
                format!("{path}[{index}]"),
                format!("must contain at most {MAX_ERROR_CHARS} characters"),
            ));
        }
        if error
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(JudgeParseError::new(
                format!("{path}[{index}]"),
                "must not contain control characters other than whitespace",
            ));
        }
    }
    Ok(())
}
