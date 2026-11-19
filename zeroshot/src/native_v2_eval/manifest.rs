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
    /// Token prices for this arm. Overrides `options.pricing` when the arm uses other models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing: Option<Pricing>,
    /// Exact runtime plan JSON (`zeroshot run --runtime-config`) for the `local` runner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_config: Option<String>,
    /// Uniform runtime JSON (`zeroshot run --uniform-runtime-config`) for the `local` runner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uniform_runtime_config: Option<String>,
    /// Template delivery mode for the `local` runner (`push`, `pull_request`, `merge`, `none`).
    /// Defaults to `push` so every run leaves a capturable branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
    /// Extra `zeroshot run` arguments for the `local` runner, such as `["--environment", "env.json"]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub run_args: Vec<String>,
}

/// USD price per one million tokens for each token kind. There is no default price: cost `C` is
/// computed only when every token kind a sample used has a price, otherwise the sample stays
/// unpriced and the report says which price is missing.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Pricing {
    /// Uncached input (read) tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<f64>,
    /// Output (write) tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<f64>,
    /// Cache-read input tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read: Option<f64>,
    /// Cache-creation (cache write) input tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<f64>,
}

/// Weights and arm roles for `S = Q / (C^a * T^b)`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ScoreOptions {
    /// Cost exponent `a` in `[0, 1]`.
    #[serde(default = "default_exponent")]
    pub a: f64,
    /// Time exponent `b` in `[0, 1]`.
    #[serde(default = "default_exponent")]
    pub b: f64,
    /// Baseline arm id. Defaults to the first arm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<String>,
    /// Candidate arm id. Defaults to the second arm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<String>,
}

impl Default for ScoreOptions {
    fn default() -> Self {
        Self {
            a: default_exponent(),
            b: default_exponent(),
            baseline: None,
            candidate: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalCriterion {
    pub id: String,
    pub description: String,
    /// Relative weight in `Q`. Equal weights (the default) make `Q = passed / total`.
    #[serde(default = "default_weight")]
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
    /// Judge graph: `eval-judge` (the built-in template, also `zeroshot template show eval-judge`)
    /// or a graph JSON path with the same input/output contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_template: Option<String>,
    /// How the judge runs: `synthetic` answers from recorded fake evidence without a model;
    /// `local` submits the judge graph as a real `zeroshot run`. Defaults to `synthetic` for the
    /// fake runner and `local` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_runner: Option<String>,
    /// Exact runtime plan for the judge run (`zeroshot run --runtime-config`). The judge's
    /// provider and model are whatever this plan (or the profile in `judgeRunArgs`) configures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_runtime_config: Option<String>,
    /// One runtime for every judge node (`zeroshot run --uniform-runtime-config`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_uniform_runtime_config: Option<String>,
    /// Extra `zeroshot run` arguments for the judge run, such as `["--profile", "judge"]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub judge_run_args: Vec<String>,
    /// Default token prices for cost `C` in USD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing: Option<Pricing>,
    /// Baseline/candidate score weights.
    #[serde(default)]
    pub score: ScoreOptions,
    /// Git remote that holds pushed `local` runner branches. Defaults to the workspace's
    /// `origin` remote URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_remote: Option<String>,
    /// Per-sample deadline for the `local` runner, in minutes. A run past it is stopped and failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_minutes: Option<u64>,
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
    /// Deterministic per-run spread of wall time and tokens, as a fraction (`0.1` = +/-10%).
    #[serde(default)]
    pub jitter: f64,
}

impl Default for EvalOptions {
    fn default() -> Self {
        Self {
            scorer: default_scorer(),
            runner: default_runner(),
            seed: 0,
            fake: FakeOptions::default(),
            judge_template: None,
            judge_runner: None,
            judge_runtime_config: None,
            judge_uniform_runtime_config: None,
            judge_run_args: Vec::new(),
            pricing: None,
            score: ScoreOptions::default(),
            capture_remote: None,
            deadline_minutes: None,
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
            jitter: 0.0,
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
        self.options.validate()?;
        for (role, arm) in [
            ("baseline", &self.options.score.baseline),
            ("candidate", &self.options.score.candidate),
        ] {
            if let Some(arm) = arm {
                if !arm_ids.contains(arm.as_str()) {
                    return Err(invalid(format!(
                        "options.score.{role} names unknown arm {arm:?}"
                    )));
                }
            }
        }
        if let (Some(baseline), Some(candidate)) =
            (&self.options.score.baseline, &self.options.score.candidate)
        {
            if baseline == candidate {
                return Err(invalid(
                    "options.score.baseline and options.score.candidate must name different arms",
                ));
            }
        }
        if let Some(runner) = &self.options.judge_runner {
            validate_judge_runner(runner)?;
        }
        if self.options.judge_runtime_config.is_some()
            && self.options.judge_uniform_runtime_config.is_some()
        {
            return Err(invalid(
                "options must declare at most one of judgeRuntimeConfig or judgeUniformRuntimeConfig",
            ));
        }
        if let Some(runtime) = &self.options.judge_uniform_runtime_config {
            validate_label("options.judgeUniformRuntimeConfig", runtime, 4_096)?;
        }
        for argument in &self.options.judge_run_args {
            validate_label("options.judgeRunArgs entry", argument, 4_096)?;
        }
        if let Some(template) = &self.options.judge_template {
            validate_label("options.judgeTemplate", template, 512)?;
        }
        if let Some(runtime) = &self.options.judge_runtime_config {
            validate_label("options.judgeRuntimeConfig", runtime, 4_096)?;
        }
        Ok(())
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
        if let Some(pricing) = &self.pricing {
            pricing.validate(&format!("arms.{}.pricing", self.id))?;
        }
        if self.runtime_config.is_some() && self.uniform_runtime_config.is_some() {
            return Err(invalid(format!(
                "arm {:?} must declare at most one of runtimeConfig or uniformRuntimeConfig",
                self.id
            )));
        }
        for path in [&self.runtime_config, &self.uniform_runtime_config]
            .into_iter()
            .flatten()
        {
            validate_label("arm runtime config path", path, 4_096)?;
        }
        if let Some(delivery) = &self.delivery {
            if !matches!(
                delivery.as_str(),
                "none" | "push" | "pull_request" | "merge" | "comment"
            ) {
                return Err(invalid(format!(
                    "arm {:?} delivery {delivery:?} must be none, push, pull_request, merge, or comment",
                    self.id
                )));
            }
        }
        if self.run_args.len() > 64
            || self
                .run_args
                .iter()
                .any(|argument| argument.is_empty() || argument.chars().any(char::is_control))
        {
            return Err(invalid(format!(
                "arm {:?} runArgs must hold at most 64 nonempty arguments without control characters",
                self.id
            )));
        }
        Ok(())
    }

    /// Initial graph input: `input` when present, otherwise `{ "task": prompt }`.
    #[must_use]
    pub fn initial_input(&self) -> Value {
        if self.input_absent() {
            serde_json::json!({ "task": self.prompt })
        } else {
            self.input.clone()
        }
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
        if !matches!(self.runner.as_str(), "fake" | "platform" | "local") {
            return Err(invalid(format!(
                "runner {:?} is not available; expected \"fake\", \"platform\", or \"local\"",
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
        if !(0.0..=1.0).contains(&self.fake.jitter) {
            return Err(invalid(
                "options.fake.jitter must be a finite number between 0 and 1",
            ));
        }
        if let Some(pricing) = &self.pricing {
            pricing.validate("options.pricing")?;
        }
        for (name, value) in [("a", self.score.a), ("b", self.score.b)] {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(invalid(format!(
                    "options.score.{name} must be a finite number between 0 and 1"
                )));
            }
        }
        if let Some(remote) = &self.capture_remote {
            validate_label("options.captureRemote", remote, 4_096)?;
        }
        if self
            .deadline_minutes
            .is_some_and(|minutes| !(1..=10_080).contains(&minutes))
        {
            return Err(invalid(
                "options.deadlineMinutes must be between 1 and 10080",
            ));
        }
        Ok(())
    }
}

impl Pricing {
    fn validate(&self, field: &str) -> Result<(), EvalError> {
        for (name, value) in self.fields() {
            if let Some(value) = value {
                if !value.is_finite() || !(0.0..=1_000_000.0).contains(&value) {
                    return Err(invalid(format!(
                        "{field}.{name} must be a finite nonnegative USD price per million tokens"
                    )));
                }
            }
        }
        Ok(())
    }

    const fn fields(&self) -> [(&'static str, Option<f64>); 4] {
        [
            ("input", self.input),
            ("output", self.output),
            ("cacheRead", self.cache_read),
            ("cacheWrite", self.cache_write),
        ]
    }

    /// Prices from `self`, with each unset field taken from `fallback`.
    #[must_use]
    pub fn or(self, fallback: Self) -> Self {
        Self {
            input: self.input.or(fallback.input),
            output: self.output.or(fallback.output),
            cache_read: self.cache_read.or(fallback.cache_read),
            cache_write: self.cache_write.or(fallback.cache_write),
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.input.is_none()
            && self.output.is_none()
            && self.cache_read.is_none()
            && self.cache_write.is_none()
    }

    /// Price `tokens` in USD: the sum of each token kind's count times its price.
    ///
    /// Returns the names of the prices that are missing for token kinds with a nonzero count.
    /// Cache tokens without a read/write split are priced only when `cacheRead` and `cacheWrite`
    /// are equal, because the split decides the cost otherwise.
    pub fn cost_usd(&self, tokens: &super::TokenUsage) -> Result<f64, Vec<&'static str>> {
        let split = tokens.cache_read.saturating_add(tokens.cache_write);
        let unsplit = tokens.cache.saturating_sub(split);
        let mut missing = Vec::new();
        let mut total = 0.0;
        for (name, count, price) in [
            ("input", tokens.input, self.input),
            ("output", tokens.output, self.output),
            ("cacheRead", tokens.cache_read, self.cache_read),
            ("cacheWrite", tokens.cache_write, self.cache_write),
        ] {
            if count == 0 {
                continue;
            }
            match price {
                Some(price) => total += count as f64 * price,
                None => missing.push(name),
            }
        }
        if unsplit > 0 {
            match (self.cache_read, self.cache_write) {
                (Some(read), Some(write)) if read == write => total += unsplit as f64 * read,
                _ => missing.push("cacheRead = cacheWrite (cache tokens have no read/write split)"),
            }
        }
        if missing.is_empty() {
            Ok(total / 1_000_000.0)
        } else {
            Err(missing)
        }
    }
}

/// Judge runner names accepted by `options.judgeRunner` and `--judge-runner`.
pub const JUDGE_RUNNERS: [&str; 2] = ["synthetic", "local"];

pub(crate) fn validate_judge_runner(runner: &str) -> Result<(), EvalError> {
    if JUDGE_RUNNERS.contains(&runner) {
        Ok(())
    } else {
        Err(invalid(format!(
            "judge runner {runner:?} must be one of {}",
            JUDGE_RUNNERS.join(", ")
        )))
    }
}

impl EvalManifest {
    /// The effective judge runner: the override, then `options.judgeRunner`, then `synthetic`
    /// for the fake runner and `local` for every runner that executes real arms.
    pub fn judge_runner(&self, run_override: Option<&str>) -> Result<&'static str, EvalError> {
        let selected = run_override
            .or(self.options.judge_runner.as_deref())
            .unwrap_or(if self.options.runner == "fake" {
                "synthetic"
            } else {
                "local"
            });
        validate_judge_runner(selected)?;
        Ok(if selected == "local" {
            "local"
        } else {
            "synthetic"
        })
    }
}

pub(crate) fn validate_scorer_name(value: &str) -> Result<(), EvalError> {
    validate_identifier("scorer name", value)
}

pub(crate) fn validate_identifier(kind: &str, value: &str) -> Result<(), EvalError> {
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

fn default_exponent() -> f64 {
    0.5
}

fn default_weight() -> f64 {
    1.0
}
