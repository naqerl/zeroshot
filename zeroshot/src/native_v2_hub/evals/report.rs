//! Evals-v2 measurement: a loss computed only at report time, and a paired per-task report.
//!
//! Nothing here is stored. A `report` call reads the run's trials and their bundles, turns each
//! graded trial into a task-relative measurement, and derives the loss
//! `quality / (costRatio^a * timeRatio^b)` from the caller's `a`,`b`. The same bundles therefore
//! answer differently at different `(a,b)`, and `state.json` plus the bundles never carry a loss.
//!
//! Ratios are always against the trial task's own baseline, so a cheap task cannot dominate a
//! costly one. A trial is measurable only when its bundle has a quality in `[0,1]`, its checks
//! report a positive `costUsd` (a top-level `costUsd` is also accepted), it has a positive
//! duration, and its task has a positive baseline cost and duration. Every
//! other trial is listed under the task's `excluded[]` with the reason instead of being scored
//! as `0` or `1`.
//!
//! Deltas are paired: a non-baseline trial is compared with the `baseline` arm's trial of the
//! same task and seed. An arm delta is the median of those pairs, and a task delta is the best
//! non-baseline arm delta. The summary bootstraps a 95 % confidence interval over the task
//! deltas with a generator seeded from `sha256(runId)`, so two reports over the same data are
//! byte-identical, and it never publishes a pooled average of loss across tasks.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::Path;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value, json};

use super::super::error::HubError;
use super::super::model::HubState;
use super::arm::Arm;
use super::bundle::{self, Bundle};
use super::regrade::Regrade;
use super::sha256_hex;
use super::task::EvalTask;
use super::trial::{EvalV2Run, Trial};

/// Default number of bootstrap resamples.
pub(crate) const DEFAULT_RESAMPLES: usize = 2_000;
/// Upper bound on the caller's `resamples`.
pub(crate) const MAX_RESAMPLES: usize = 100_000;

/// `loss: {a, b}` of `POST /v1/hub/evals/runs/{id}/report`.
#[derive(Clone, Copy, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LossParams {
    pub a: f64,
    pub b: f64,
}

impl<'de> Deserialize<'de> for LossParams {
    /// Read the two parameters by name so every malformed request names the field it got wrong.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let object: Map<String, Value> = value
            .as_object()
            .cloned()
            .ok_or_else(|| D::Error::custom("loss must be an object with a and b"))?;
        if let Some(unknown) = object.keys().find(|key| *key != "a" && *key != "b") {
            return Err(D::Error::custom(format!(
                "loss.{unknown} is not a loss parameter; use a and b"
            )));
        }
        let a = read_parameter::<D::Error>(&object, "a")?;
        let b = read_parameter::<D::Error>(&object, "b")?;
        Ok(Self { a, b })
    }
}

fn read_parameter<E: serde::de::Error>(object: &Map<String, Value>, field: &str) -> Result<f64, E> {
    let value = object
        .get(field)
        .ok_or_else(|| E::custom(format!("loss.{field} is required")))?;
    let number = value
        .as_f64()
        .ok_or_else(|| E::custom(format!("loss.{field} must be a number in 0.0..=2.0")))?;
    if !number.is_finite() || !(0.0..=2.0).contains(&number) {
        return Err(E::custom(format!(
            "loss.{field} must be a number in 0.0..=2.0"
        )));
    }
    Ok(number)
}

/// The report request. `loss` is required; `resamples` defaults to [`DEFAULT_RESAMPLES`].
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct EvalsReportRequest {
    pub loss: LossParams,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resamples: Option<usize>,
}

impl EvalsReportRequest {
    /// Validate both loss parameters and the resample count, naming the offending field.
    pub(crate) fn validate(&self) -> Result<usize, HubError> {
        validate_parameter("loss.a", self.loss.a)?;
        validate_parameter("loss.b", self.loss.b)?;
        let resamples = self.resamples.unwrap_or(DEFAULT_RESAMPLES);
        if resamples == 0 || resamples > MAX_RESAMPLES {
            return Err(HubError::Usage(format!(
                "resamples must be 1..={MAX_RESAMPLES}"
            )));
        }
        Ok(resamples)
    }
}

fn validate_parameter(field: &str, value: f64) -> Result<(), HubError> {
    if !value.is_finite() || !(0.0..=2.0).contains(&value) {
        return Err(HubError::Usage(format!(
            "{field} must be a number in 0.0..=2.0"
        )));
    }
    Ok(())
}

fn round(value: f64) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let rounded = (value * 1_000_000.0).round() / 1_000_000.0;
    if rounded.is_finite() { rounded } else { value }
}

fn finite(value: f64) -> Option<f64> {
    value.is_finite().then(|| round(value))
}

/// A task-relative measurement of one graded trial.
#[derive(Clone, Copy, Debug)]
struct Measurement {
    seed: u32,
    quality: f64,
    cost_ratio: f64,
    time_ratio: f64,
    loss: f64,
}

/// One trial that could not be measured, and why.
#[derive(Clone, Debug)]
struct Exclusion {
    trial_id: String,
    arm_id: String,
    seed: u32,
    quality: Option<f64>,
    quality_source: &'static str,
    regrade_id: Option<String>,
    reason: &'static str,
}

/// A per-trial report entry. Unlike an arm aggregate, this names the trial whose quality was
/// read and says whether that quality was human, automated, or absent.
#[derive(Clone, Debug)]
struct TrialEntry {
    trial_id: String,
    arm_id: String,
    seed: u32,
    quality: Option<f64>,
    quality_source: &'static str,
    regrade_id: Option<String>,
    cost_ratio: Option<f64>,
    time_ratio: Option<f64>,
    loss: Option<f64>,
    reason: Option<&'static str>,
    measurement: Option<Measurement>,
}

impl TrialEntry {
    fn view(&self) -> Value {
        json!({
            "trialId": self.trial_id,
            "armId": self.arm_id,
            "seed": self.seed,
            "quality": self.quality.map(round),
            "qualitySource": self.quality_source,
            "regradeId": self.regrade_id,
            "costRatio": self.cost_ratio.and_then(finite),
            "timeRatio": self.time_ratio.and_then(finite),
            "loss": self.loss,
            "reason": self.reason,
        })
    }
}

fn latest_regrade<'a>(regrades: &'a [Regrade], trial_id: &str) -> Option<&'a Regrade> {
    regrades
        .iter()
        .filter(|regrade| regrade.trial_id == trial_id)
        .max_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.regrade_id.cmp(&right.regrade_id))
        })
}

/// The object maps of a bundle's `checks`, whether it is one object or a list of checks.
fn check_objects(checks: &Value) -> Vec<&Map<String, Value>> {
    match checks {
        Value::Object(map) => vec![map],
        Value::Array(items) => items.iter().filter_map(Value::as_object).collect(),
        _ => Vec::new(),
    }
}

fn number(map: &Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| map.get(*key).and_then(Value::as_f64))
}

fn named<'a>(objects: &[&'a Map<String, Value>], names: &[&str]) -> Option<&'a Map<String, Value>> {
    objects.iter().copied().find(|map| {
        map.get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| names.iter().any(|wanted| name.eq_ignore_ascii_case(wanted)))
    })
}

/// The blind quality of a graded trial: a `quality` in `[0,1]` in the bundle's checks.
fn quality(objects: &[&Map<String, Value>]) -> Result<f64, &'static str> {
    let value = objects
        .iter()
        .find_map(|map| number(map, &["quality"]))
        .or_else(|| {
            named(objects, &["quality"]).and_then(|map| number(map, &["quality", "score", "value"]))
        })
        .ok_or("no_quality")?;
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err("invalid_quality");
    }
    Ok(value)
}

/// The measured USD cost of a trial, when its checks reported one.
fn cost_usd(objects: &[&Map<String, Value>]) -> Option<f64> {
    objects
        .iter()
        .find_map(|map| number(map, &["costUsd"]))
        .or_else(|| {
            named(objects, &["cost", "costUsd"])
                .and_then(|map| number(map, &["costUsd", "value", "score"]))
        })
        .filter(|value| value.is_finite())
}

/// The task's positive baseline cost and duration.
fn baseline(task: &EvalTask) -> Result<(f64, f64), &'static str> {
    let cost = task.baseline.cost_usd.0;
    if !cost.is_finite() || cost <= 0.0 {
        return Err("no_baseline_cost");
    }
    let duration = task.baseline.duration_ms as f64;
    if duration <= 0.0 {
        return Err("no_baseline_duration");
    }
    Ok((cost, duration))
}

/// `quality / (costRatio^a * timeRatio^b)`, or `None` when it is not a finite number.
fn loss(quality: f64, cost_ratio: f64, time_ratio: f64, params: &LossParams) -> Option<f64> {
    if !cost_ratio.is_finite() || !time_ratio.is_finite() || cost_ratio <= 0.0 || time_ratio <= 0.0
    {
        return None;
    }
    let denominator = cost_ratio.powf(params.a) * time_ratio.powf(params.b);
    if !denominator.is_finite() || denominator <= 0.0 {
        return None;
    }
    finite(quality / denominator)
}

/// Inputs of one trial's report entry.
struct TrialEntryInput<'a> {
    task: &'a EvalTask,
    trial: &'a Trial,
    hub_directory: &'a Path,
    params: &'a LossParams,
    regrades: &'a [Regrade],
}

/// Read one trial's bundle and turn it into a report entry and, when measurable, a measurement.
/// A human regrade replaces only the quality; it never rewrites the bundle evidence.
fn trial_entry(input: TrialEntryInput<'_>) -> TrialEntry {
    let TrialEntryInput {
        task,
        trial,
        hub_directory,
        params,
        regrades,
    } = input;
    let regrade = latest_regrade(regrades, &trial.trial_id);
    let mut effective_quality = regrade.map(|regrade| regrade.quality);
    let mut quality_source = if regrade.is_some() {
        "human"
    } else {
        "missing"
    };
    let mut reason = None;
    let mut cost_ratio = None;
    let mut time_ratio = None;
    let mut loss_value = None;
    let path = trial
        .bundle_path
        .clone()
        .unwrap_or_else(|| bundle::path(hub_directory, trial));
    match std::fs::read(&path) {
        Err(_) => reason = Some("missing_bundle"),
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Err(_) => reason = Some("invalid_bundle"),
            Ok(document) => match serde_json::from_value::<Bundle>(document.clone()) {
                Err(_) => reason = Some("invalid_bundle"),
                Ok(bundle) => {
                    let objects = check_objects(&bundle.checks);
                    if effective_quality.is_none() {
                        match quality(&objects) {
                            Ok(auto) => {
                                effective_quality = Some(auto);
                                quality_source = "auto";
                            }
                            Err(why) => reason = Some(why),
                        }
                    }
                    if reason.is_none()
                        && let Some(quality_value) = effective_quality
                    {
                        let measured =
                            baseline(task).and_then(|(baseline_cost, baseline_duration)| {
                                let cost = cost_usd(&objects)
                                    .or_else(|| document.get("costUsd").and_then(Value::as_f64))
                                    .filter(|value| value.is_finite())
                                    .ok_or("no_cost")?;
                                if cost <= 0.0 {
                                    return Err("no_cost");
                                }
                                let duration = trial
                                    .duration_ms
                                    .or(bundle.duration_ms)
                                    .map(|duration| duration as f64)
                                    .filter(|duration| *duration > 0.0)
                                    .ok_or("no_duration")?;
                                Ok((cost / baseline_cost, duration / baseline_duration))
                            });
                        match measured {
                            Err(why) => reason = Some(why),
                            Ok((cost, time)) => match loss(quality_value, cost, time, params) {
                                Some(value) => {
                                    cost_ratio = Some(cost);
                                    time_ratio = Some(time);
                                    loss_value = Some(value);
                                }
                                None => reason = Some("undefined_loss"),
                            },
                        }
                    }
                }
            },
        },
    }
    let measurement = match (
        reason,
        effective_quality,
        cost_ratio,
        time_ratio,
        loss_value,
    ) {
        (None, Some(quality), Some(cost_ratio), Some(time_ratio), Some(loss)) => {
            Some(Measurement {
                seed: trial.seed,
                quality,
                cost_ratio,
                time_ratio,
                loss,
            })
        }
        _ => None,
    };
    TrialEntry {
        trial_id: trial.trial_id.clone(),
        arm_id: trial.arm_id.clone(),
        seed: trial.seed,
        quality: effective_quality,
        quality_source,
        regrade_id: regrade.map(|regrade| regrade.regrade_id.clone()),
        cost_ratio,
        time_ratio,
        loss: loss_value,
        reason,
        measurement,
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let total = values.iter().sum::<f64>();
    finite(total / values.len() as f64)
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        // Halve the gap so the midpoint of two huge values cannot overflow.
        let (left, right) = (sorted[middle - 1], sorted[middle]);
        finite(left + (right - left) / 2.0)
    } else {
        finite(sorted[middle])
    }
}

/// Deterministic SplitMix64; no platform randomness can reach a report.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// The bootstrap generator seed: the first eight bytes of `sha256(runId)`.
fn bootstrap_seed(run_id: &str) -> u64 {
    let digest = sha256_hex(run_id.as_bytes());
    u64::from_str_radix(&digest[..16], 16).unwrap_or(0)
}

/// Type-7 quantile of an ascending slice.
fn quantile(sorted: &[f64], q: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let position = (sorted.len() - 1) as f64 * q;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let value = if lower == upper {
        sorted[lower]
    } else {
        sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower as f64)
    };
    finite(value)
}

/// The median of `deltas` and the 95 % bootstrap interval of that median.
fn bootstrap_median(
    deltas: &[f64],
    resamples: usize,
    seed: u64,
) -> (Option<f64>, Option<(f64, f64)>) {
    let observed = median(deltas);
    if deltas.is_empty() || resamples == 0 {
        return (observed, None);
    }
    let mut rng = Rng(seed);
    let mut sample = Vec::with_capacity(deltas.len());
    let mut medians = Vec::with_capacity(resamples);
    for _ in 0..resamples {
        sample.clear();
        for _ in 0..deltas.len() {
            let index = (rng.next_u64() % deltas.len() as u64) as usize;
            sample.push(deltas[index]);
        }
        if let Some(median) = median(&sample) {
            medians.push(median);
        }
    }
    medians.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let low = quantile(&medians, 0.025);
    let high = quantile(&medians, 0.975);
    match (low, high) {
        (Some(low), Some(high)) if low <= high => (observed, Some((round(low), round(high)))),
        _ => (observed, None),
    }
}

/// One arm's measurements inside one task.
struct ArmMeasurement {
    arm_id: String,
    measurements: Vec<Measurement>,
    excluded: Vec<Exclusion>,
}

/// An aggregate arm entry of one task.
struct ArmReport {
    arm_id: String,
    trials: usize,
    quality: Option<f64>,
    loss: Option<f64>,
    cost_ratio: Option<f64>,
    time_ratio: Option<f64>,
    delta: Option<f64>,
}

impl ArmReport {
    fn view(&self) -> Value {
        json!({
            "armId": self.arm_id,
            "trials": self.trials,
            "quality": self.quality,
            "loss": self.loss,
            "costRatio": self.cost_ratio,
            "timeRatio": self.time_ratio,
            "deltaVsBaseline": self.delta,
        })
    }
}

/// Aggregate one arm: means of the trial ratios and quality, then the loss of those means.
fn arm_report(
    arm: &ArmMeasurement,
    baseline_losses: Option<&BTreeMap<u32, f64>>,
    is_baseline: bool,
    params: &LossParams,
) -> ArmReport {
    let qualities = arm
        .measurements
        .iter()
        .map(|measurement| measurement.quality)
        .collect::<Vec<_>>();
    let cost_ratios = arm
        .measurements
        .iter()
        .map(|measurement| measurement.cost_ratio)
        .collect::<Vec<_>>();
    let time_ratios = arm
        .measurements
        .iter()
        .map(|measurement| measurement.time_ratio)
        .collect::<Vec<_>>();
    let (quality, cost_ratio, time_ratio) =
        (mean(&qualities), mean(&cost_ratios), mean(&time_ratios));
    let aggregate_loss = match (quality, cost_ratio, time_ratio) {
        (Some(quality), Some(cost_ratio), Some(time_ratio)) => {
            loss(quality, cost_ratio, time_ratio, params)
        }
        _ => None,
    };
    let delta = if is_baseline {
        Some(0.0)
    } else {
        let paired = baseline_losses.map(|baseline_losses| {
            arm.measurements
                .iter()
                .filter_map(|measurement| {
                    baseline_losses
                        .get(&measurement.seed)
                        .map(|baseline| round(measurement.loss - baseline))
                })
                .collect::<Vec<_>>()
        });
        paired.and_then(|paired| median(&paired))
    };
    ArmReport {
        arm_id: arm.arm_id.clone(),
        trials: arm.measurements.len(),
        quality,
        loss: aggregate_loss,
        cost_ratio,
        time_ratio,
        delta,
    }
}

/// One task of the report.
struct TaskReport {
    task_id: String,
    task_hash: String,
    verdict: &'static str,
    arms: Vec<ArmReport>,
    trials: Vec<TrialEntry>,
    excluded: Vec<Exclusion>,
    regraded_trials: usize,
    delta: Option<f64>,
}

/// Inputs of one task's report.
struct TaskInput<'a> {
    task: &'a EvalTask,
    arms: &'a [Arm],
    trials: &'a [&'a Trial],
    regrades: &'a [Regrade],
    hub_directory: &'a Path,
    params: &'a LossParams,
}

/// Build one task's report from the run's trials.
fn task_report(input: TaskInput<'_>) -> TaskReport {
    let TaskInput {
        task,
        arms,
        trials,
        regrades,
        hub_directory,
        params,
    } = input;
    let baseline_arm = arms
        .iter()
        .find(|arm| arm.name.eq_ignore_ascii_case("baseline"));
    let mut arm_measurements = Vec::new();
    let mut entries = Vec::new();
    let mut excluded = Vec::new();
    for arm in arms {
        let mut measurements = Vec::new();
        let mut arm_excluded = Vec::new();
        for trial in trials.iter().filter(|trial| trial.arm_hash == arm.arm_hash) {
            let entry = trial_entry(TrialEntryInput {
                task,
                trial,
                hub_directory,
                params,
                regrades,
            });
            if let Some(measurement) = entry.measurement {
                measurements.push(measurement);
            } else {
                arm_excluded.push(Exclusion {
                    trial_id: trial.trial_id.clone(),
                    arm_id: arm.arm_id.clone(),
                    seed: trial.seed,
                    quality: entry.quality,
                    quality_source: entry.quality_source,
                    regrade_id: entry.regrade_id.clone(),
                    reason: entry.reason.unwrap_or("missing_quality"),
                });
            }
            entries.push(entry);
        }
        arm_measurements.push(ArmMeasurement {
            arm_id: arm.arm_id.clone(),
            measurements,
            excluded: arm_excluded,
        });
    }
    // Pair each non-baseline arm with the baseline arm's trial of the same seed.
    let baseline_losses = baseline_arm.and_then(|baseline_arm| {
        arm_measurements
            .iter()
            .find(|arm| arm.arm_id == baseline_arm.arm_id)
            .map(|arm| {
                arm.measurements
                    .iter()
                    .map(|measurement| (measurement.seed, measurement.loss))
                    .collect::<BTreeMap<_, _>>()
            })
    });
    let mut reports = Vec::new();
    let mut insufficient = false;
    for arm in &arm_measurements {
        let is_baseline = baseline_arm.is_some_and(|baseline| baseline.arm_id == arm.arm_id);
        if arm.measurements.len() < 2 {
            insufficient = true;
        }
        reports.push(arm_report(
            arm,
            baseline_losses.as_ref(),
            is_baseline,
            params,
        ));
        excluded.extend(arm.excluded.iter().cloned());
    }
    let (verdict, delta) = match baseline_arm {
        None => ("no_baseline", None),
        Some(_) if insufficient => ("insufficient_data", None),
        Some(baseline) => {
            let delta = reports
                .iter()
                .filter(|report| report.arm_id != baseline.arm_id)
                .filter_map(|report| report.delta)
                .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
            ("measured", delta)
        }
    };
    TaskReport {
        task_id: task.task_id.clone(),
        task_hash: task.task_hash.clone(),
        verdict,
        arms: reports,
        regraded_trials: entries
            .iter()
            .filter(|entry| entry.quality_source == "human")
            .count(),
        trials: entries,
        excluded,
        delta,
    }
}

impl Exclusion {
    fn view(&self, task_id: &str) -> Value {
        json!({
            "taskId": task_id,
            "trialId": self.trial_id,
            "armId": self.arm_id,
            "seed": self.seed,
            "quality": self.quality.map(round),
            "qualitySource": self.quality_source,
            "regradeId": self.regrade_id,
            "reason": self.reason,
        })
    }
}

impl TaskReport {
    fn view(&self) -> Value {
        json!({
            "taskId": self.task_id,
            "taskHash": self.task_hash,
            "verdict": self.verdict,
            "delta": self.delta,
            "regradedTrials": self.regraded_trials,
            "arms": self.arms.iter().map(ArmReport::view).collect::<Vec<_>>(),
            "trials": self.trials.iter().map(TrialEntry::view).collect::<Vec<_>>(),
            "excluded": self
                .excluded
                .iter()
                .map(|exclusion| exclusion.view(&self.task_id))
                .collect::<Vec<_>>(),
        })
    }
}

/// Pooled point of one arm for the Pareto frontier.
struct Point {
    arm_id: String,
    cost_ratio: f64,
    time_ratio: f64,
    quality: f64,
}

fn pareto(tasks: &[TaskReport]) -> Vec<Point> {
    let mut sums: BTreeMap<&str, (f64, f64, f64, f64)> = BTreeMap::new();
    // A task without a baseline arm contributes nothing to the summary, so its arms are not
    // frontier points either.
    for task in tasks.iter().filter(|task| task.verdict != "no_baseline") {
        for arm in &task.arms {
            let (Some(cost_ratio), Some(time_ratio), Some(quality), trials) =
                (arm.cost_ratio, arm.time_ratio, arm.quality, arm.trials)
            else {
                continue;
            };
            if trials == 0 {
                continue;
            }
            let entry = sums.entry(&arm.arm_id).or_insert((0.0, 0.0, 0.0, 0.0));
            entry.0 += cost_ratio;
            entry.1 += time_ratio;
            entry.2 += quality;
            entry.3 += 1.0;
        }
    }
    let points = sums
        .into_iter()
        .map(|(arm_id, (cost, time, quality, count))| Point {
            arm_id: arm_id.to_owned(),
            cost_ratio: round(cost / count),
            time_ratio: round(time / count),
            quality: round(quality / count),
        })
        .collect::<Vec<_>>();
    let dominated = |candidate: &Point, other: &Point| {
        other.cost_ratio <= candidate.cost_ratio
            && other.time_ratio <= candidate.time_ratio
            && other.quality >= candidate.quality
            && (other.cost_ratio < candidate.cost_ratio
                || other.time_ratio < candidate.time_ratio
                || other.quality > candidate.quality)
    };
    points
        .iter()
        .filter(|candidate| {
            !points
                .iter()
                .any(|other| other.arm_id != candidate.arm_id && dominated(candidate, other))
        })
        .map(|point| Point {
            arm_id: point.arm_id.clone(),
            cost_ratio: point.cost_ratio,
            time_ratio: point.time_ratio,
            quality: point.quality,
        })
        .collect()
}

/// Inputs of the summary document.
struct SummaryInput<'a> {
    tasks: &'a [TaskReport],
    deltas: &'a [f64],
    any_baseline: bool,
    insufficient: bool,
    regraded_trials: usize,
    resamples: usize,
    run_id: &'a str,
}

fn summary_view(input: SummaryInput<'_>) -> Value {
    let SummaryInput {
        tasks,
        deltas,
        any_baseline,
        insufficient,
        regraded_trials,
        resamples,
        run_id,
    } = input;
    let verdict = if !any_baseline {
        "no_baseline"
    } else if insufficient {
        "insufficient_data"
    } else {
        "measured"
    };
    let seed = bootstrap_seed(run_id);
    let (median_delta, ci) = bootstrap_median(deltas, resamples, seed);
    let epsilon = 1e-9;
    let wins = deltas.iter().filter(|delta| **delta < -epsilon).count();
    let losses = deltas.iter().filter(|delta| **delta > epsilon).count();
    let ties = deltas.len() - wins - losses;
    let pareto = pareto(tasks)
        .into_iter()
        .map(|point| {
            json!({
                "armId": point.arm_id,
                "costRatio": point.cost_ratio,
                "timeRatio": point.time_ratio,
                "quality": point.quality,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "verdict": verdict,
        "tasks": deltas.len(),
        "medianDelta": median_delta,
        "ci95": ci.map(|(low, high)| json!([low, high])),
        "resamples": resamples,
        "seed": format!("{seed:016x}"),
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "regradedTrials": regraded_trials,
        "pareto": pareto,
    })
}

/// Inputs of a whole report.
pub(crate) struct ReportInput<'a> {
    pub run: &'a EvalV2Run,
    pub state: &'a HubState,
    pub hub_directory: &'a Path,
    pub params: &'a LossParams,
    pub resamples: usize,
}

/// The report document of `run`.
pub(crate) fn report(input: ReportInput<'_>) -> Result<Value, HubError> {
    let ReportInput {
        run,
        state,
        hub_directory,
        params,
        resamples,
    } = input;
    let mut tasks = Vec::new();
    let mut task_deltas = Vec::new();
    let mut any_baseline = false;
    let mut insufficient = false;
    for task_hash in &run.task_hashes {
        let task = state.evals_v2.task(task_hash).ok_or_else(|| {
            HubError::EvalNotFound(format!("evals task {task_hash} does not exist"))
        })?;
        let arms = run
            .arm_hashes
            .iter()
            .filter_map(|arm_hash| state.evals_v2.arm(arm_hash).cloned())
            .collect::<Vec<_>>();
        let trials = state
            .evals_v2
            .trials
            .iter()
            .filter(|trial| trial.run_id == run.run_id && &trial.task_hash == task_hash)
            .collect::<Vec<_>>();
        let task_report = task_report(TaskInput {
            task,
            arms: &arms,
            trials: &trials,
            regrades: &state.evals_v2.regrades,
            hub_directory,
            params,
        });
        any_baseline |= task_report.verdict != "no_baseline";
        insufficient |= task_report.verdict == "insufficient_data";
        task_deltas.extend(task_report.delta);
        tasks.push(task_report);
    }
    let regraded_trials = tasks.iter().map(|task| task.regraded_trials).sum();
    let summary = summary_view(SummaryInput {
        tasks: &tasks,
        deltas: &task_deltas,
        any_baseline,
        insufficient,
        regraded_trials,
        resamples,
        run_id: &run.run_id,
    });
    // The same per-task exclusions are also listed at the report root, so a caller does not have
    // to descend into every task to know what was unmeasurable. Every trial entry is likewise
    // visible at the root, naming its task, so a quality source is never hidden behind an arm.
    let excluded = tasks
        .iter()
        .flat_map(|task| {
            task.excluded
                .iter()
                .map(|exclusion| exclusion.view(&task.task_id))
        })
        .collect::<Vec<_>>();
    let trials = tasks
        .iter()
        .flat_map(|task| {
            task.trials.iter().map(|entry| {
                let mut view = entry.view();
                view["taskId"] = json!(task.task_id);
                view
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema": "zeroshot.hub.evals.report/v1",
        "runId": run.run_id,
        "loss": { "a": params.a, "b": params.b },
        "tasks": tasks.iter().map(TaskReport::view).collect::<Vec<_>>(),
        "trials": trials,
        "excluded": excluded,
        "regradedTrials": regraded_trials,
        "summary": summary,
    }))
}
