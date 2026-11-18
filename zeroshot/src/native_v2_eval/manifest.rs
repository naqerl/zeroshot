use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::EvalError;

pub const MANIFEST_SCHEMA: &str = "zeroshot.eval/v1";

/// Strict TOML manifest for one eval.
///
/// ```toml
/// schema = "zeroshot.eval/v1"
/// name = "synthetic"
/// runsPerArm = 2
///
/// [options]
/// scorer = "quality-vs-cost"
/// runner = "fake"
///
/// [[arms]]
/// id = "baseline"
/// template = "single-worker"
/// prompt = "Do the task."
/// input = { task = "Do the task." }
///
/// [[criteria]]
/// id = "correctness"
/// description = "Passes the repository checks."
/// weight = 2.0
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalManifest {
    #[serde(default = "default_schema")]
    pub schema: String,
    pub name: String,
    pub runs_per_arm: u32,
    pub arms: Vec<EvalArm>,
    pub criteria: Vec<EvalCriterion>,
    #[serde(default)]
    pub options: EvalOptions,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalArm {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(default)]
    pub prompt: String,
    #[serde(default = "empty_input")]
    pub input: Value,
    #[serde(default)]
    pub knobs: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalCriterion {
    pub id: String,
    pub description: String,
    pub weight: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalOptions {
    #[serde(default = "default_scorer")]
    pub scorer: String,
    #[serde(default = "default_runner")]
    pub runner: String,
    #[serde(default)]
    pub seed: u64,
    #[serde(default)]
    pub fake: FakeOptions,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FakeOptions {
    #[serde(default = "default_queue_wait_ms")]
    pub queue_wait_ms: u64,
    #[serde(default = "default_wall_clock_ms")]
    pub wall_clock_ms: u64,
    #[serde(default = "default_input_tokens")]
    pub input_tokens: u64,
    #[serde(default = "default_output_tokens")]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_tokens: u64,
    #[serde(default)]
    pub fail_every: Option<u32>,
    #[serde(default)]
    pub skip_every: Option<u32>,
    #[serde(default = "default_pass_rate")]
    pub pass_rate: f64,
}

impl Default for EvalOptions {
    fn default() -> Self {
        Self {
            scorer: default_scorer(),
            runner: default_runner(),
            seed: 0,
            fake: FakeOptions::default(),
        }
    }
}

impl Default for FakeOptions {
    fn default() -> Self {
        Self {
            queue_wait_ms: default_queue_wait_ms(),
            wall_clock_ms: default_wall_clock_ms(),
            input_tokens: default_input_tokens(),
            output_tokens: default_output_tokens(),
            cache_tokens: 0,
            fail_every: None,
            skip_every: None,
            pass_rate: default_pass_rate(),
        }
    }
}

impl EvalManifest {
    pub fn parse(source: &str) -> Result<Self, EvalError> {
        let manifest: Self = toml::from_str(source)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn from_file(path: &Path) -> Result<(Self, String), EvalError> {
        let source = std::fs::read_to_string(path).map_err(|error| EvalError::io(path, error))?;
        let manifest = Self::parse(&source)?;
        Ok((manifest, source))
    }

    pub fn to_toml(&self) -> Result<String, EvalError> {
        toml::to_string_pretty(self).map_err(|error| {
            EvalError::Command(format!(
                "could not serialize eval manifest as TOML: {error}"
            ))
        })
    }

    pub fn validate(&self) -> Result<(), EvalError> {
        if self.schema != MANIFEST_SCHEMA {
            return Err(invalid(format!(
                "schema must be {MANIFEST_SCHEMA:?}, found {:?}",
                self.schema
            )));
        }
        validate_label("eval name", &self.name, 128)?;
        if self.runs_per_arm == 0 {
            return Err(invalid("runsPerArm must be greater than zero"));
        }
        if self.runs_per_arm > 1_000 {
            return Err(invalid("runsPerArm must not exceed 1000"));
        }
        if self.arms.is_empty() {
            return Err(invalid("at least one arm is required"));
        }
        if self.arms.len() > 64 {
            return Err(invalid("at most 64 arms are supported"));
        }
        let mut arm_ids = BTreeSet::new();
        for arm in &self.arms {
            arm.validate()?;
            if !arm_ids.insert(arm.id.as_str()) {
                return Err(invalid(format!("duplicate arm id {:?}", arm.id)));
            }
        }
        if self.criteria.is_empty() {
            return Err(invalid("at least one criterion is required"));
        }
        if self.criteria.len() > 64 {
            return Err(invalid("at most 64 criteria are supported"));
        }
        let mut criterion_ids = BTreeSet::new();
        for criterion in &self.criteria {
            criterion.validate()?;
            if !criterion_ids.insert(criterion.id.as_str()) {
                return Err(invalid(format!(
                    "duplicate criterion id {:?}",
                    criterion.id
                )));
            }
        }
        self.options.validate()
    }

    pub fn criterion_ids(&self) -> impl Iterator<Item = &str> {
        self.criteria.iter().map(|criterion| criterion.id.as_str())
    }

    pub fn arm(&self, id: &str) -> Option<&EvalArm> {
        self.arms.iter().find(|arm| arm.id == id)
    }
}

impl EvalArm {
    fn validate(&self) -> Result<(), EvalError> {
        validate_identifier("arm id", &self.id)?;
        match (&self.graph, &self.template) {
            (Some(_), Some(_)) => {
                return Err(invalid(format!(
                    "arm {:?} must declare exactly one of graph or template",
                    self.id
                )));
            }
            (None, None) => {
                return Err(invalid(format!(
                    "arm {:?} must declare exactly one of graph or template",
                    self.id
                )));
            }
            (Some(reference), None) | (None, Some(reference)) => {
                if reference.trim().is_empty() {
                    return Err(invalid(format!(
                        "arm {:?} has an empty graph or template reference",
                        self.id
                    )));
                }
                if reference.len() > 512 {
                    return Err(invalid(format!(
                        "arm {:?} graph or template reference is too long",
                        self.id
                    )));
                }
            }
        }
        if self.prompt.trim().is_empty() && self.input_absent() {
            return Err(invalid(format!(
                "arm {:?} requires a prompt or input payload",
                self.id
            )));
        }
        Ok(())
    }

    fn input_absent(&self) -> bool {
        self.input.is_null()
            || self
                .input
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
    }

    pub fn reference(&self) -> &str {
        self.graph
            .as_deref()
            .or(self.template.as_deref())
            .unwrap_or_default()
    }
}

impl EvalCriterion {
    fn validate(&self) -> Result<(), EvalError> {
        validate_identifier("criterion id", &self.id)?;
        validate_label("criterion description", &self.description, 2_048)?;
        if !self.weight.is_finite() || self.weight <= 0.0 {
            return Err(invalid(format!(
                "criterion {:?} weight must be a finite positive number",
                self.id
            )));
        }
        if self.weight > 1_000_000.0 {
            return Err(invalid(format!(
                "criterion {:?} weight is too large",
                self.id
            )));
        }
        Ok(())
    }
}

impl EvalOptions {
    fn validate(&self) -> Result<(), EvalError> {
        validate_scorer_name(&self.scorer)?;
        if self.runner != "fake" {
            return Err(invalid(format!(
                "runner {:?} is not available in this build; expected \"fake\"",
                self.runner
            )));
        }
        if self
            .fake
            .fail_every
            .is_some_and(|every| !(1..=1_000_000).contains(&every))
        {
            return Err(invalid(
                "options.fake.failEvery must be between 1 and 1000000",
            ));
        }
        if self
            .fake
            .skip_every
            .is_some_and(|every| !(1..=1_000_000).contains(&every))
        {
            return Err(invalid(
                "options.fake.skipEvery must be between 1 and 1000000",
            ));
        }
        if !self.fake.pass_rate.is_finite() || !(0.0..=1.0).contains(&self.fake.pass_rate) {
            return Err(invalid(
                "options.fake.passRate must be a finite number between 0 and 1",
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_scorer_name(value: &str) -> Result<(), EvalError> {
    validate_identifier("scorer name", value)
}

fn validate_identifier(kind: &str, value: &str) -> Result<(), EvalError> {
    if value.is_empty() || value.len() > 128 {
        return Err(invalid(format!("{kind} must contain 1 to 128 characters")));
    }
    let valid = value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    let first_valid = value
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_alphanumeric());
    if !valid || !first_valid || value == "." || value == ".." {
        return Err(invalid(format!(
            "{kind} {value:?} must start with a letter or digit and contain only letters, digits, '.', '_' or '-'"
        )));
    }
    Ok(())
}

fn validate_label(kind: &str, value: &str, max: usize) -> Result<(), EvalError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > max {
        return Err(invalid(format!(
            "{kind} must contain 1 to {max} non-whitespace characters"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(format!(
            "{kind} must not contain control characters"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> EvalError {
    EvalError::Manifest(message.into())
}

fn empty_input() -> Value {
    Value::Object(serde_json::Map::new())
}

fn default_schema() -> String {
    MANIFEST_SCHEMA.to_owned()
}

fn default_scorer() -> String {
    "quality-vs-cost".to_owned()
}

fn default_runner() -> String {
    "fake".to_owned()
}

fn default_queue_wait_ms() -> u64 {
    10
}

fn default_wall_clock_ms() -> u64 {
    100
}

fn default_input_tokens() -> u64 {
    1_000
}

fn default_output_tokens() -> u64 {
    200
}

fn default_pass_rate() -> f64 {
    0.8
}
