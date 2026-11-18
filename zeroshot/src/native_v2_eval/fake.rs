use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;
use openengine_cluster_protocol::{
    MAX_SAFE_GENERATION, RunId, TokenCount, TokenUsage as ProtocolTokenUsage,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::EvalError;
use super::capture::DeliveryReceipt;
use super::manifest::{EvalArm, EvalCriterion};
use super::model::{CriterionResult, EvalId};
use super::runner::{
    ResumeCredentials, SampleOutcome, SamplePatch, SampleRequest, SampleRun, SampleRunner,
    SampleRunnerError, SampleStatus, SampleTimings,
};
use super::{FakeOptions, model::now_ms};

/// Inputs the manifest fixture passes to the synthetic runner.
#[derive(Clone, Debug)]
pub struct FakeFixtureRequest {
    pub eval_id: EvalId,
    pub arm: EvalArm,
    pub run_index: u32,
    pub criteria: Vec<EvalCriterion>,
}

/// One request for [`FakeRunner`].
///
/// A `fixture` request derives its result from `[options.fake]` and per-arm knobs. A request
/// without one is a scripted result, which keeps the platform runner tests independent of the
/// manifest fixture vocabulary.
#[derive(Clone, Debug)]
pub struct FakeSampleRequest {
    pub sample_id: String,
    pub run_id: Option<RunId>,
    pub concurrency: usize,
    pub queue_wait_ms: u64,
    pub wall_clock_ms: u64,
    pub status: SampleStatus,
    pub token_usage: Option<ProtocolTokenUsage>,
    pub delivery: Option<DeliveryReceipt>,
    pub failure_reason: Option<String>,
    pub fixture: Option<FakeFixtureRequest>,
}

impl FakeSampleRequest {
    #[must_use]
    pub fn succeeded(sample_id: impl Into<String>) -> Self {
        Self {
            sample_id: sample_id.into(),
            run_id: None,
            concurrency: 1,
            queue_wait_ms: 0,
            wall_clock_ms: 1,
            status: SampleStatus::Succeeded,
            token_usage: None,
            delivery: None,
            failure_reason: None,
            fixture: None,
        }
    }

    #[must_use]
    pub fn failed(sample_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            status: SampleStatus::Failed,
            failure_reason: Some(reason.into()),
            ..Self::succeeded(sample_id)
        }
    }

    #[must_use]
    pub fn fixture(
        eval_id: EvalId,
        arm: EvalArm,
        run_index: u32,
        criteria: Vec<EvalCriterion>,
    ) -> Self {
        let sample_id = format!("{}-{run_index:04}", arm.id);
        let run_id = RunId::new(format!("fake-{eval_id}-{sample_id}"));
        Self {
            sample_id,
            run_id: Some(run_id),
            fixture: Some(FakeFixtureRequest {
                eval_id,
                arm,
                run_index,
                criteria,
            }),
            ..Self::succeeded(String::new())
        }
    }
}

impl SampleRequest for FakeSampleRequest {
    fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

#[derive(Default)]
struct FakeState {
    submissions: BTreeMap<RunId, FakeSampleRequest>,
    order: Vec<SampleRun>,
    stopped: Vec<RunId>,
}

/// Deterministic in-process runner used by fixtures and tests. It never touches the network.
pub struct FakeRunner {
    options: FakeOptions,
    seed: u64,
    state: Mutex<FakeState>,
}

impl FakeRunner {
    #[must_use]
    pub fn new() -> Self {
        Self::from_fixture(FakeOptions::default(), 0)
    }

    /// Build a runner seeded by the manifest's `[options.fake]` block.
    #[must_use]
    pub fn from_fixture(options: FakeOptions, seed: u64) -> Self {
        Self {
            options,
            seed,
            state: Mutex::new(FakeState::default()),
        }
    }

    pub async fn submitted(&self) -> Vec<SampleRun> {
        self.state
            .lock()
            .map(|state| state.order.clone())
            .unwrap_or_default()
    }

    pub async fn stopped(&self) -> Vec<RunId> {
        self.state
            .lock()
            .map(|state| state.stopped.clone())
            .unwrap_or_default()
    }

    fn outcome(
        &self,
        request: &FakeSampleRequest,
        run: &SampleRun,
    ) -> Result<SampleOutcome, SampleRunnerError> {
        match &request.fixture {
            Some(fixture) => self.fixture_outcome(run, fixture),
            None => Ok(scripted_outcome(request, run)),
        }
    }

    fn fixture_outcome(
        &self,
        run: &SampleRun,
        fixture: &FakeFixtureRequest,
    ) -> Result<SampleOutcome, SampleRunnerError> {
        let settings = FakeArmSettings::from_request(&self.options, fixture)
            .map_err(|error| SampleRunnerError::InvalidRequest(error.to_string()))?;
        let ordinal = fixture.run_index.saturating_add(1);
        let skip = settings.skip
            || self
                .options
                .skip_every
                .is_some_and(|every| ordinal % every == 0);
        let fail = !skip
            && (settings.fail
                || self
                    .options
                    .fail_every
                    .is_some_and(|every| ordinal % every == 0));
        let mut criteria = BTreeMap::new();
        let mut reasoning = BTreeMap::new();
        if !skip && !fail {
            for criterion in &fixture.criteria {
                let passed = settings
                    .criteria
                    .get(&criterion.id)
                    .copied()
                    .unwrap_or_else(|| {
                        unit_hash(self.seed, &fixture.arm.id, &criterion.id, fixture.run_index)
                            < settings.pass_rate
                    });
                let explanation = format!(
                    "fake judgement: {}{} for arm {} run {}",
                    criterion.id,
                    if passed { " passed" } else { " failed" },
                    fixture.arm.id,
                    fixture.run_index
                );
                reasoning.insert(criterion.id.clone(), explanation.clone());
                criteria.insert(
                    criterion.id.clone(),
                    CriterionResult {
                        passed,
                        score: Some(if passed { 5.0 } else { 1.0 }),
                        reasoning: Some(explanation),
                        errors: Vec::new(),
                    },
                );
            }
        }
        let status = if skip {
            SampleStatus::Skipped
        } else if fail {
            SampleStatus::Failed
        } else {
            SampleStatus::Succeeded
        };
        let failure_reason = match status {
            SampleStatus::Failed => Some(format!(
                "fake runner failed arm {} run {}",
                fixture.arm.id, fixture.run_index
            )),
            SampleStatus::Skipped => Some(format!(
                "fake runner skipped arm {} run {}",
                fixture.arm.id, fixture.run_index
            )),
            SampleStatus::Succeeded => None,
        };
        let started_at_ms = run.submitted_at_ms.saturating_add(settings.queue_wait_ms);
        let finished_at_ms = started_at_ms.saturating_add(settings.wall_clock_ms);
        Ok(SampleOutcome {
            run_id: run.run_id.clone(),
            status,
            timings: SampleTimings {
                submitted_at_ms: run.submitted_at_ms,
                started_at_ms: Some(started_at_ms),
                finished_at_ms,
                queue_wait_ms: settings.queue_wait_ms,
                wall_clock_ms: settings.wall_clock_ms,
                concurrency: run.concurrency,
            },
            token_usage: Some(protocol_tokens(settings.tokens)),
            delivered_revision: (status == SampleStatus::Succeeded)
                .then(|| revision(self.seed, &fixture.arm.id, fixture.run_index)),
            delivered_branch: None,
            delivery: None,
            failure_reason,
            output: None,
            criteria,
            reasoning,
            patch: (status == SampleStatus::Succeeded).then(|| SamplePatch {
                content: format!(
                    "fake patch for eval {} arm {} run {}\n",
                    fixture.eval_id, fixture.arm.id, fixture.run_index
                ),
            }),
        })
    }
}

impl Default for FakeRunner {
    fn default() -> Self {
        Self::new()
    }
}

fn scripted_outcome(request: &FakeSampleRequest, run: &SampleRun) -> SampleOutcome {
    let started_at_ms = run.submitted_at_ms.saturating_add(request.queue_wait_ms);
    let finished_at_ms = started_at_ms.saturating_add(request.wall_clock_ms);
    let delivery = request.delivery.clone();
    SampleOutcome {
        run_id: run.run_id.clone(),
        status: request.status,
        timings: SampleTimings {
            submitted_at_ms: run.submitted_at_ms,
            started_at_ms: Some(started_at_ms),
            finished_at_ms,
            queue_wait_ms: request.queue_wait_ms,
            wall_clock_ms: request.wall_clock_ms,
            concurrency: run.concurrency,
        },
        token_usage: request.token_usage.clone(),
        delivered_revision: delivery
            .as_ref()
            .and_then(DeliveryReceipt::delivered_revision),
        delivered_branch: delivery
            .as_ref()
            .and_then(|value| value.head_branch.clone()),
        delivery,
        failure_reason: request.failure_reason.clone(),
        output: None,
        criteria: BTreeMap::new(),
        reasoning: BTreeMap::new(),
        patch: None,
    }
}

fn protocol_tokens(tokens: super::model::TokenUsage) -> ProtocolTokenUsage {
    ProtocolTokenUsage {
        input_tokens: token_count(tokens.input),
        output_tokens: token_count(tokens.output),
        cache_read_input_tokens: (tokens.cache > 0).then(|| token_count(tokens.cache)),
        cache_creation_input_tokens: None,
        complete: true,
    }
}

fn token_count(value: u64) -> TokenCount {
    TokenCount::new(value.min(MAX_SAFE_GENERATION)).unwrap_or_default()
}

#[async_trait]
impl SampleRunner for FakeRunner {
    type Request = FakeSampleRequest;

    async fn submit(&self, request: Self::Request) -> Result<SampleRun, SampleRunnerError> {
        if request.sample_id.trim().is_empty() {
            return Err(SampleRunnerError::InvalidRequest(
                "sample id is required".to_owned(),
            ));
        }
        let run_id = request
            .run_id
            .clone()
            .unwrap_or_else(|| RunId::new(uuid::Uuid::now_v7().to_string()));
        let run = SampleRun {
            sample_id: request.sample_id.clone(),
            run_id: run_id.clone(),
            target: None,
            submitted_at_ms: now_ms(),
            concurrency: request.concurrency.max(1),
        };
        // Validate fixture knobs before the sample enters the journal.
        self.outcome(&request, &run)?;
        let mut state = self.state.lock().map_err(|_| {
            SampleRunnerError::InvalidRequest("fake runner state is poisoned".to_owned())
        })?;
        state.order.push(run.clone());
        state.submissions.insert(run_id, request);
        Ok(run)
    }

    async fn wait(&self, run: &SampleRun) -> Result<SampleOutcome, SampleRunnerError> {
        let (request, stopped) = {
            let state = self.state.lock().map_err(|_| {
                SampleRunnerError::InvalidRequest("fake runner state is poisoned".to_owned())
            })?;
            let request = state.submissions.get(&run.run_id).cloned().ok_or_else(|| {
                SampleRunnerError::InvalidRequest("run was not submitted".to_owned())
            })?;
            (request, state.stopped.contains(&run.run_id))
        };
        if stopped {
            return Err(SampleRunnerError::Stopped);
        }
        self.outcome(&request, run)
    }

    async fn stop(&self, run: &SampleRun) -> Result<(), SampleRunnerError> {
        self.state
            .lock()
            .map_err(|_| {
                SampleRunnerError::InvalidRequest("fake runner state is poisoned".to_owned())
            })?
            .stopped
            .push(run.run_id.clone());
        Ok(())
    }

    async fn resume(
        &self,
        run: SampleRun,
        _credentials: ResumeCredentials,
    ) -> Result<SampleRun, SampleRunnerError> {
        let mut state = self.state.lock().map_err(|_| {
            SampleRunnerError::InvalidRequest("fake runner state is poisoned".to_owned())
        })?;
        let submission =
            state.submissions.get(&run.run_id).cloned().ok_or_else(|| {
                SampleRunnerError::InvalidRequest("run was not submitted".to_owned())
            })?;
        let successor = RunId::new(uuid::Uuid::now_v7().to_string());
        let resumed = SampleRun {
            run_id: successor.clone(),
            submitted_at_ms: now_ms(),
            ..run
        };
        state.order.push(resumed.clone());
        state.submissions.insert(successor, submission);
        Ok(resumed)
    }
}

struct FakeArmSettings {
    queue_wait_ms: u64,
    wall_clock_ms: u64,
    tokens: super::model::TokenUsage,
    pass_rate: f64,
    fail: bool,
    skip: bool,
    criteria: BTreeMap<String, bool>,
}

impl FakeArmSettings {
    fn from_request(
        options: &FakeOptions,
        request: &FakeFixtureRequest,
    ) -> Result<Self, EvalError> {
        let knobs = &request.arm.knobs;
        let criteria = criteria_overrides(knobs, &request.arm.id)?;
        Ok(Self {
            queue_wait_ms: u64_knob(knobs, "queueWaitMs", options.queue_wait_ms, &request.arm.id)?,
            wall_clock_ms: u64_knob(knobs, "wallClockMs", options.wall_clock_ms, &request.arm.id)?,
            tokens: super::model::TokenUsage {
                input: u64_knob(knobs, "inputTokens", options.input_tokens, &request.arm.id)?,
                output: u64_knob(
                    knobs,
                    "outputTokens",
                    options.output_tokens,
                    &request.arm.id,
                )?,
                cache: u64_knob(knobs, "cacheTokens", options.cache_tokens, &request.arm.id)?,
            },
            pass_rate: f64_knob(knobs, "passRate", options.pass_rate, &request.arm.id)?,
            fail: bool_knob(knobs, "fail", false, &request.arm.id)?,
            skip: bool_knob(knobs, "skip", false, &request.arm.id)?,
            criteria,
        })
    }
}

fn criteria_overrides(
    knobs: &Map<String, Value>,
    arm: &str,
) -> Result<BTreeMap<String, bool>, EvalError> {
    let mut overrides = BTreeMap::new();
    let Some(value) = knobs.get("criteria") else {
        return Ok(overrides);
    };
    let Some(object) = value.as_object() else {
        return Err(knob_error(arm, "criteria", "an object of booleans"));
    };
    let mut seen = BTreeSet::new();
    for (criterion, value) in object {
        if !seen.insert(criterion.as_str()) {
            return Err(knob_error(arm, "criteria", "an object of unique booleans"));
        }
        let Some(passed) = value.as_bool() else {
            return Err(knob_error(arm, "criteria", "an object of booleans"));
        };
        overrides.insert(criterion.clone(), passed);
    }
    Ok(overrides)
}

fn u64_knob(
    knobs: &Map<String, Value>,
    key: &str,
    default: u64,
    arm: &str,
) -> Result<u64, EvalError> {
    match knobs.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| knob_error(arm, key, "a nonnegative integer")),
    }
}

fn f64_knob(
    knobs: &Map<String, Value>,
    key: &str,
    default: f64,
    arm: &str,
) -> Result<f64, EvalError> {
    match knobs.get(key) {
        None => Ok(default),
        Some(value) => {
            let Some(number) = value.as_f64() else {
                return Err(knob_error(arm, key, "a number between 0 and 1"));
            };
            if !number.is_finite() || !(0.0..=1.0).contains(&number) {
                return Err(knob_error(arm, key, "a number between 0 and 1"));
            }
            Ok(number)
        }
    }
}

fn bool_knob(
    knobs: &Map<String, Value>,
    key: &str,
    default: bool,
    arm: &str,
) -> Result<bool, EvalError> {
    match knobs.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_bool()
            .ok_or_else(|| knob_error(arm, key, "a boolean")),
    }
}

fn knob_error(arm: &str, key: &str, expected: &str) -> EvalError {
    EvalError::Runner(format!(
        "fake runner arm {arm:?} knob {key:?} must be {expected}"
    ))
}

fn unit_hash(seed: u64, arm: &str, criterion: &str, run_index: u32) -> f64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
    for byte in arm
        .bytes()
        .chain([0])
        .chain(criterion.bytes())
        .chain([0])
        .chain(run_index.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as f64) / (u64::MAX as f64)
}

fn revision(seed: u64, arm: &str, run_index: u32) -> String {
    let mut digest = Sha256::new();
    digest.update(seed.to_le_bytes());
    digest.update(arm.as_bytes());
    digest.update(run_index.to_le_bytes());
    digest
        .finalize()
        .iter()
        .take(20)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
