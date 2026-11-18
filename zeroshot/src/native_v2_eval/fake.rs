use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::{
    CriterionResult, EvalError, FakeOptions, SampleHandle, SampleOutcome, SamplePatch,
    SampleRequest, SampleRunner, SampleStatus, SampleTiming, TokenUsage, model::now_ms,
};

/// Deterministic in-process runner used by fixtures and tests. It never touches the network.
pub struct FakeRunner {
    options: FakeOptions,
    seed: u64,
    pending: Mutex<BTreeMap<String, SampleOutcome>>,
}

impl FakeRunner {
    #[must_use]
    pub fn new(options: FakeOptions, seed: u64) -> Self {
        Self {
            options,
            seed,
            pending: Mutex::new(BTreeMap::new()),
        }
    }

    fn outcome(&self, request: &SampleRequest) -> Result<SampleOutcome, EvalError> {
        let settings = FakeArmSettings::from_request(&self.options, request)?;
        let ordinal = request.run_index.saturating_add(1);
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
            for criterion in &request.criteria {
                let passed = settings
                    .criteria
                    .get(&criterion.id)
                    .copied()
                    .unwrap_or_else(|| {
                        unit_hash(self.seed, &request.arm.id, &criterion.id, request.run_index)
                            < settings.pass_rate
                    });
                let explanation = format!(
                    "fake judgement: {}{} for arm {} run {}",
                    criterion.id,
                    if passed { " passed" } else { " failed" },
                    request.arm.id,
                    request.run_index
                );
                reasoning.insert(criterion.id.clone(), explanation.clone());
                criteria.insert(
                    criterion.id.clone(),
                    CriterionResult {
                        passed,
                        reasoning: Some(explanation),
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
                request.arm.id, request.run_index
            )),
            SampleStatus::Skipped => Some(format!(
                "fake runner skipped arm {} run {}",
                request.arm.id, request.run_index
            )),
            SampleStatus::Succeeded => None,
        };
        Ok(SampleOutcome {
            status,
            timing: SampleTiming {
                started_at_ms: now_ms(),
                wall_clock_ms: settings.wall_clock_ms,
                queue_wait_ms: settings.queue_wait_ms,
            },
            tokens: settings.tokens,
            delivered_revision: (status == SampleStatus::Succeeded)
                .then(|| revision(self.seed, &request.arm.id, request.run_index)),
            failure_reason,
            patch: (status == SampleStatus::Succeeded).then(|| SamplePatch {
                content: format!(
                    "fake patch for eval {} arm {} run {}\n",
                    request.eval_id, request.arm.id, request.run_index
                ),
            }),
            criteria,
            reasoning,
        })
    }
}

#[async_trait]
impl SampleRunner for FakeRunner {
    async fn submit(&self, request: SampleRequest) -> Result<SampleHandle, EvalError> {
        let run_id = format!(
            "fake-{}-{}-{:04}",
            request.eval_id, request.arm.id, request.run_index
        );
        let outcome = self.outcome(&request)?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| EvalError::Runner("fake runner state is poisoned".to_owned()))?;
        pending.insert(run_id.clone(), outcome);
        Ok(SampleHandle {
            run_id,
            arm_id: request.arm.id,
            run_index: request.run_index,
        })
    }

    async fn wait(&self, handle: SampleHandle) -> Result<SampleOutcome, EvalError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| EvalError::Runner("fake runner state is poisoned".to_owned()))?;
        pending.remove(&handle.run_id).ok_or_else(|| {
            EvalError::Runner(format!(
                "fake runner has no pending sample {}",
                handle.run_id
            ))
        })
    }

    async fn stop(&self, handle: SampleHandle) -> Result<(), EvalError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| EvalError::Runner("fake runner state is poisoned".to_owned()))?;
        pending.remove(&handle.run_id);
        Ok(())
    }
}

struct FakeArmSettings {
    queue_wait_ms: u64,
    wall_clock_ms: u64,
    tokens: TokenUsage,
    pass_rate: f64,
    fail: bool,
    skip: bool,
    criteria: BTreeMap<String, bool>,
}

impl FakeArmSettings {
    fn from_request(options: &FakeOptions, request: &SampleRequest) -> Result<Self, EvalError> {
        let knobs = &request.arm.knobs;
        let criteria = criteria_overrides(knobs, &request.arm.id)?;
        Ok(Self {
            queue_wait_ms: u64_knob(knobs, "queueWaitMs", options.queue_wait_ms, &request.arm.id)?,
            wall_clock_ms: u64_knob(knobs, "wallClockMs", options.wall_clock_ms, &request.arm.id)?,
            tokens: TokenUsage {
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
