//! Eval reports: per-case and aggregate metrics of every variant, deltas against a baseline,
//! and improvement/regression flags. Pure functions over stored samples.
//!
//! Metrics per sample: pass (job succeeded, every check passed, and an absolute judge did not
//! say `fail`), judge score, tokens (input + output) and estimated cost (from a price table) of
//! complete usage only (see `usage.rs`), and
//! run duration. Each aggregate is a mean with a 95% normal-approximation confidence interval
//! (mean ± 1.96·s/√n, none below two samples); nothing more is claimed about significance.
//! Pass rate and judge score deltas are absolute; tokens, cost, and duration deltas are
//! relative to the baseline mean. A delta beyond its threshold flags `improved` or `regressed`.
//! Next to those per-metric deltas a report carries one composite [`loss`]: the weighted mean of
//! the five candidate/baseline ratios, where `1.0` is "no change", above it the candidate is
//! worse, and below it better.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use super::super::error::HubError;
use super::super::stats::Prices;
use super::super::usage::{JobUsage, Tokens, cost};
use super::{EvalRun, JudgeMode, Sample};

pub(crate) const SCHEMA: &str = "zeroshot.hub.eval.report/v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Better {
    Higher,
    Lower,
}

/// `(metric, default threshold, better direction)`; pass rate and judge score compare
/// absolutely, the others relatively.
const METRICS: [(&str, f64, Better); 5] = [
    ("passRate", 0.05, Better::Higher),
    ("judgeScore", 0.05, Better::Higher),
    ("tokens", 0.10, Better::Lower),
    ("costUsd", 0.10, Better::Lower),
    ("durationMs", 0.25, Better::Lower),
];

/// Weights of the composite [`Loss`], keyed by the metric names of [`METRICS`].
///
/// Pass rate and judge score dominate because they answer whether the change worked at all: a
/// candidate that fails cases or reads worse to the judge is worse however few tokens it spent.
/// Tokens, cost, and duration only separate candidates that already work, so they share the
/// remaining 0.30.
const LOSS_WEIGHTS: [(&str, f64); 5] = [
    ("passRate", 0.35),
    ("judgeScore", 0.35),
    ("tokens", 0.10),
    ("costUsd", 0.10),
    ("durationMs", 0.10),
];

/// Bounds of one component ratio. Without them a near-zero metric mean would contribute a
/// meaningless 500x (or 0x) and hide the remaining metrics.
const LOSS_RATIO_BOUNDS: (f64, f64) = (0.25, 4.0);

pub(crate) fn metric_names() -> Vec<&'static str> {
    METRICS.iter().map(|(name, _, _)| *name).collect()
}

/// Thresholds by metric; `None` disables flags for that metric.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Thresholds(BTreeMap<&'static str, Option<f64>>);

impl Default for Thresholds {
    fn default() -> Self {
        Self(
            METRICS
                .iter()
                .map(|(name, threshold, _)| (*name, Some(*threshold)))
                .collect(),
        )
    }
}

impl Thresholds {
    /// Apply `METRIC=VALUE` overrides; `off` disables a metric.
    pub(crate) fn parse(pairs: &[(String, String)]) -> Result<Self, HubError> {
        let mut thresholds = Self::default();
        for (metric, value) in pairs {
            let Some((name, _, _)) = METRICS.iter().find(|(name, _, _)| name == metric) else {
                return Err(HubError::Usage(format!(
                    "unknown --threshold metric {metric:?}; use one of {}",
                    metric_names().join(", ")
                )));
            };
            let parsed = if value == "off" {
                None
            } else {
                Some(
                    value
                        .parse::<f64>()
                        .ok()
                        .filter(|value| value.is_finite() && *value >= 0.0)
                        .ok_or_else(|| {
                            HubError::Usage(format!(
                                "--threshold {metric}={value} needs a non-negative number or off"
                            ))
                        })?,
                )
            };
            thresholds.0.insert(name, parsed);
        }
        Ok(thresholds)
    }

    fn view(&self) -> Value {
        json!(self.0)
    }
}

/// Mean, 95% confidence interval, and count of one metric.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Stat {
    pub mean: Option<f64>,
    pub ci95: Option<(f64, f64)>,
    pub n: usize,
}

#[allow(clippy::cast_precision_loss)]
pub(crate) fn stat(values: &[f64]) -> Stat {
    let n = values.len();
    if n == 0 {
        return Stat::default();
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let ci95 = (n >= 2).then(|| {
        let variance = values
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (n - 1) as f64;
        let half = 1.96 * variance.sqrt() / (n as f64).sqrt();
        (mean - half, mean + half)
    });
    Stat {
        mean: Some(mean),
        ci95,
        n,
    }
}

fn round(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

impl Stat {
    fn view(&self) -> Value {
        json!({
            "mean": self.mean.map(round),
            "ci95": self.ci95.map(|(low, high)| [round(low), round(high)]),
            "n": self.n,
        })
    }
}

/// The per-sample metric values, `None` where a sample has no such measurement.
#[allow(clippy::cast_precision_loss)]
fn values(sample: &Sample, mode: Option<JudgeMode>, prices: Option<&Prices>) -> [Option<f64>; 5] {
    let outcome = sample.outcome.as_ref();
    let usage = outcome
        .and_then(|outcome| outcome.token_usage.as_ref())
        .and_then(JobUsage::complete);
    let cost = usage.and_then(|usage| cost(usage, prices?.get(&outcome?.model)?));
    [
        sample
            .passed(mode)
            .map(|passed| if passed { 1.0 } else { 0.0 }),
        sample
            .judge
            .as_ref()
            .and_then(|verdict| verdict.score)
            .map(|score| score.0),
        usage
            .and_then(|usage| Tokens::of(usage).input_and_output())
            .map(|tokens| tokens as f64),
        cost,
        outcome
            .and_then(|outcome| outcome.duration_ms)
            .map(|duration| duration as f64),
    ]
}

/// Metric stats of a group of samples.
fn aggregate(samples: &[&Sample], mode: Option<JudgeMode>, prices: Option<&Prices>) -> [Stat; 5] {
    let mut columns: [Vec<f64>; 5] = Default::default();
    for sample in samples {
        for (column, value) in columns.iter_mut().zip(values(sample, mode, prices)) {
            column.extend(value);
        }
    }
    columns.map(|column| stat(&column))
}

fn aggregate_view(samples: &[&Sample], stats: &[Stat; 5], judged: bool) -> Value {
    let mut view = json!({
        "samples": samples.len(),
        "settled": samples.iter().filter(|sample| sample.is_settled(judged)).count(),
    });
    for ((name, _, _), stat) in METRICS.iter().zip(stats) {
        view[*name] = stat.view();
    }
    view
}

/// Deltas, flags, and verdict of `candidate` against `baseline`. `relative_scores` means the
/// candidate's judge scores are already relative to the baseline (pairwise judging).
fn compare(
    candidate: &[Stat; 5],
    baseline: &[Stat; 5],
    relative_scores: bool,
    thresholds: &Thresholds,
) -> (Value, Vec<Value>, &'static str) {
    let mut deltas = serde_json::Map::new();
    let mut flags = Vec::new();
    for (index, (name, _, better)) in METRICS.iter().enumerate() {
        let (candidate, baseline) = (candidate[index].mean, baseline[index].mean);
        let delta = if *name == "judgeScore" && relative_scores {
            candidate.map(|mean| (mean, None))
        } else {
            match (candidate, baseline) {
                (Some(candidate), Some(baseline)) => Some((
                    candidate - baseline,
                    (baseline.abs() > f64::EPSILON).then(|| candidate / baseline - 1.0),
                )),
                _ => None,
            }
        };
        let Some((absolute, relative)) = delta else {
            deltas.insert((*name).to_owned(), Value::Null);
            continue;
        };
        deltas.insert(
            (*name).to_owned(),
            json!({ "absolute": round(absolute), "relative": relative.map(round) }),
        );
        let compared = match better {
            Better::Higher => Some(absolute),
            Better::Lower => relative.map(|relative| -relative),
        };
        let (Some(gain), Some(Some(threshold))) = (compared, thresholds.0.get(name)) else {
            continue;
        };
        let direction = if gain > *threshold {
            "improved"
        } else if gain < -*threshold {
            "regressed"
        } else {
            continue;
        };
        flags.push(json!({
            "metric": name,
            "direction": direction,
            "delta": round(if *better == Better::Higher { absolute } else { relative.unwrap_or(0.0) }),
            "threshold": threshold,
        }));
    }
    let improved = flags.iter().any(|flag| flag["direction"] == "improved");
    let regressed = flags.iter().any(|flag| flag["direction"] == "regressed");
    let verdict = match (improved, regressed) {
        (true, true) => "mixed",
        (true, false) => "improved",
        (false, true) => "regressed",
        (false, false) if deltas.values().all(Value::is_null) => "insufficient",
        (false, false) => "unchanged",
    };
    (Value::Object(deltas), flags, verdict)
}

/// One metric's contribution to a [`Loss`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LossComponent {
    /// The ratio, rounded to six decimals: `baseline / candidate` for a `Better::Higher` metric
    /// and `candidate / baseline` for a `Better::Lower` one, so above `1.0` the candidate is
    /// worse on this metric. The value is clamped to [`LOSS_RATIO_BOUNDS`].
    pub ratio: f64,
    /// The component's share of the total: its [`LOSS_WEIGHTS`] weight, renormalised over the
    /// metrics that survived.
    pub weight: f64,
}

/// One scalar for "is `candidate` better than `baseline`, and by how much".
///
/// `value` is the weighted mean of the per-metric [`LossComponent::ratio`]s. Equal means give
/// exactly `1.0` for every component and for the total, so a caller reads `value > 1.0` as "the
/// candidate is worse" and `value < 1.0` as "better". A component whose candidate or baseline
/// mean is missing, or whose baseline mean is zero (nothing to compare against), is dropped and
/// the surviving weights are renormalised; `renormalised` says so. Fewer than two surviving
/// components is not a loss at all, and [`loss`] returns `None`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Loss {
    pub value: f64,
    pub weights: BTreeMap<&'static str, f64>,
    pub components: BTreeMap<&'static str, LossComponent>,
    pub renormalised: bool,
}

impl Loss {
    fn view(&self) -> Value {
        let components = self
            .components
            .iter()
            .map(|(metric, component)| {
                (
                    (*metric).to_owned(),
                    json!({ "ratio": component.ratio, "weight": component.weight }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let weights = self
            .weights
            .iter()
            .map(|(metric, weight)| ((*metric).to_owned(), json!(weight)))
            .collect::<serde_json::Map<_, _>>();
        json!({
            "value": self.value,
            "renormalised": self.renormalised,
            "components": components,
            "weights": weights,
        })
    }
}

/// The composite loss of `candidate` against `baseline`, or `None` when fewer than two metrics
/// can be compared.
pub(crate) fn loss(candidate: &[Stat; 5], baseline: &[Stat; 5]) -> Option<Loss> {
    let mut ratios = BTreeMap::<&'static str, (f64, f64)>::new();
    let mut surviving = 0.0;
    for ((name, _, better), (candidate, baseline)) in
        METRICS.iter().zip(candidate.iter().zip(baseline))
    {
        let (Some(candidate), Some(baseline)) = (candidate.mean, baseline.mean) else {
            continue;
        };
        // A zero baseline cannot say whether the candidate improved or regressed.
        if baseline == 0.0 {
            continue;
        }
        let Some((_, weight)) = LOSS_WEIGHTS.iter().find(|(metric, _)| metric == name) else {
            continue;
        };
        let ratio = match better {
            Better::Higher => baseline / candidate,
            Better::Lower => candidate / baseline,
        };
        let ratio = ratio.clamp(LOSS_RATIO_BOUNDS.0, LOSS_RATIO_BOUNDS.1);
        // Only an undefined ratio (0/0) is unusable; clamping already folded the infinities in.
        if !ratio.is_finite() {
            continue;
        }
        ratios.insert(name, (ratio, *weight));
        surviving += weight;
    }
    if ratios.len() < 2 || surviving <= 0.0 {
        return None;
    }
    let renormalised = ratios.len() != METRICS.len();
    let mut weights = BTreeMap::new();
    let mut components = BTreeMap::new();
    let mut value = 0.0;
    for (name, (ratio, weight)) in ratios {
        let weight = if renormalised {
            weight / surviving
        } else {
            weight
        };
        value += weight * ratio;
        weights.insert(name, round(weight));
        components.insert(
            name,
            LossComponent {
                ratio: round(ratio),
                weight: round(weight),
            },
        );
    }
    Some(Loss {
        value: round(value),
        weights,
        components,
        renormalised,
    })
}

/// What the variants are compared with.
pub(crate) struct Against<'a> {
    /// `{runId, variant}` of the baseline, echoed in the report.
    pub label: Value,
    pub samples: Vec<&'a Sample>,
    /// The run's own pairwise baseline: judge scores are already relative to it.
    pub pairwise_reference: bool,
}

/// The report document of `run`.
pub(crate) fn report(
    run: &EvalRun,
    against: Option<&Against<'_>>,
    thresholds: &Thresholds,
    prices: Option<&Prices>,
) -> Value {
    let mode = run.judge_mode();
    let judged = run.judge.is_some();
    let baseline_name = against
        .and_then(|against| against.label.get("variant"))
        .and_then(Value::as_str)
        .filter(|_| {
            against.is_some_and(|against| against.label.get("runId") == Some(&json!(run.run_id)))
        });
    let mut variant_names = Vec::<&str>::new();
    for sample in &run.samples {
        if !variant_names.contains(&sample.variant.as_str()) {
            variant_names.push(&sample.variant);
        }
    }
    let baseline_cases = against.map(|against| {
        against
            .samples
            .iter()
            .map(|sample| sample.case_id.as_str())
            .collect::<BTreeSet<_>>()
    });
    let mut regressions = 0;
    let mut improvements = 0;
    let variants = variant_names
        .iter()
        .map(|name| {
            let samples = run
                .samples
                .iter()
                .filter(|sample| sample.variant == *name)
                .collect::<Vec<_>>();
            let stats = aggregate(&samples, mode, prices);
            let mut view =
                json!({ "variant": name, "aggregate": aggregate_view(&samples, &stats, judged) });
            if Some(*name) == baseline_name {
                view["role"] = json!("baseline");
                view["loss"] = Value::Null;
                return view;
            }
            if let (Some(against), Some(cases)) = (against, &baseline_cases) {
                let matched = samples
                    .iter()
                    .filter(|sample| cases.contains(sample.case_id.as_str()))
                    .copied()
                    .collect::<Vec<_>>();
                let candidate = aggregate(&matched, mode, prices);
                let baseline = aggregate(&against.samples, mode, prices);
                let (delta, flags, verdict) = compare(
                    &candidate,
                    &baseline,
                    against.pairwise_reference,
                    thresholds,
                );
                match verdict {
                    "regressed" | "mixed" => regressions += 1,
                    "improved" => improvements += 1,
                    _ => {}
                }
                view["delta"] = delta;
                view["flags"] = json!(flags);
                view["verdict"] = json!(verdict);
                view["loss"] = match loss(&candidate, &baseline) {
                    Some(loss) => loss.view(),
                    None => Value::Null,
                };
            } else {
                view["loss"] = Value::Null;
            }
            view
        })
        .collect::<Vec<_>>();
    let mut case_ids = Vec::<&str>::new();
    for sample in &run.samples {
        if !case_ids.contains(&sample.case_id.as_str()) {
            case_ids.push(&sample.case_id);
        }
    }
    let cases = case_ids
        .iter()
        .map(|case_id| {
            let baseline = against.map(|against| {
                let samples = against
                    .samples
                    .iter()
                    .filter(|sample| sample.case_id == *case_id)
                    .copied()
                    .collect::<Vec<_>>();
                aggregate(&samples, mode, prices)
            });
            let rows = variant_names
                .iter()
                .map(|name| {
                    let samples = run
                        .samples
                        .iter()
                        .filter(|sample| sample.case_id == *case_id && sample.variant == *name)
                        .collect::<Vec<_>>();
                    let stats = aggregate(&samples, mode, prices);
                    let mut view = json!({ "variant": name, "aggregate": aggregate_view(&samples, &stats, judged) });
                    if Some(*name) == baseline_name {
                        view["role"] = json!("baseline");
                    } else if let (Some(baseline), Some(against)) = (&baseline, against) {
                        let (delta, flags, verdict) =
                            compare(&stats, baseline, against.pairwise_reference, thresholds);
                        view["delta"] = delta;
                        view["flags"] = json!(flags);
                        view["verdict"] = json!(verdict);
                    }
                    view
                })
                .collect::<Vec<_>>();
            json!({ "caseId": case_id, "variants": rows })
        })
        .collect::<Vec<_>>();
    let count = |predicate: fn(&Sample) -> bool| {
        run.samples
            .iter()
            .filter(|sample| predicate(sample))
            .count()
    };
    json!({
        "schema": SCHEMA,
        "runId": run.run_id,
        "suite": run.suite,
        "complete": run.complete(),
        "judge": run.judge.as_ref().map(|judge| json!({ "kind": judge.kind, "mode": judge.mode })),
        "samples": {
            "total": run.samples.len(),
            "pending": count(|sample| sample.state == super::SampleState::Pending),
            "measured": count(|sample| sample.state == super::SampleState::Measured),
            "judged": count(|sample| sample.state == super::SampleState::Judged),
            "failed": count(|sample| sample.state == super::SampleState::Failed),
        },
        "baseline": against.map(|against| against.label.clone()),
        "thresholds": thresholds.view(),
        "variants": variants,
        "cases": cases,
        "improvements": improvements,
        "regressions": regressions,
        "regressed": regressions > 0,
        "priced": prices.is_some(),
    })
}

/// A fixed-width summary of a report document for people.
pub(crate) fn table(report: &Value) -> String {
    let number = |value: &Value, digits: usize| {
        value
            .as_f64()
            .map_or_else(|| "-".to_owned(), |value| format!("{value:.digits$}"))
    };
    // A star marks a loss whose weights were renormalised because a metric was unavailable.
    let loss_cell = |row: &Value| match row["loss"]["value"].as_f64() {
        None => "-".to_owned(),
        Some(value) if row["loss"]["renormalised"] == json!(true) => format!("{value:.3}*"),
        Some(value) => format!("{value:.3}"),
    };
    let cell = |row: &Value, metric: &str, digits: usize| {
        let mean = number(&row["aggregate"][metric]["mean"], digits);
        match &row["delta"][metric] {
            Value::Null => mean,
            delta if matches!(metric, "passRate" | "judgeScore") => {
                format!("{mean} ({:+.3})", delta["absolute"].as_f64().unwrap_or(0.0))
            }
            delta => match delta["relative"].as_f64() {
                Some(relative) => format!("{mean} ({:+.1}%)", relative * 100.0),
                None => mean,
            },
        }
    };
    let mut lines = vec![format!(
        "eval run {}  complete={}  baseline={}",
        report["runId"].as_str().unwrap_or("-"),
        report["complete"],
        report["baseline"]
            .get("variant")
            .and_then(Value::as_str)
            .map_or_else(
                || "-".to_owned(),
                |variant| {
                    format!(
                        "{}:{variant}",
                        report["baseline"]["runId"].as_str().unwrap_or("-")
                    )
                }
            ),
    )];
    let header = format!(
        "{:<22} {:<16} {:>4} {:<18} {:<18} {:<20} {:<20} {:<20} {:<7} {}",
        "CASE",
        "VARIANT",
        "N",
        "PASS",
        "JUDGE",
        "TOKENS",
        "COST_USD",
        "DURATION_MS",
        "LOSS",
        "VERDICT"
    );
    lines.push(header);
    let mut push = |case: &str, row: &Value| {
        lines.push(format!(
            "{:<22} {:<16} {:>4} {:<18} {:<18} {:<20} {:<20} {:<20} {:<7} {}",
            case,
            row["variant"].as_str().unwrap_or("-"),
            row["aggregate"]["samples"],
            cell(row, "passRate", 2),
            cell(row, "judgeScore", 2),
            cell(row, "tokens", 0),
            cell(row, "costUsd", 4),
            cell(row, "durationMs", 0),
            loss_cell(row),
            row["verdict"]
                .as_str()
                .or_else(|| row["role"].as_str())
                .unwrap_or("-"),
        ));
    };
    for row in report["variants"].as_array().into_iter().flatten() {
        push("ALL", row);
    }
    for case in report["cases"].as_array().into_iter().flatten() {
        for row in case["variants"].as_array().into_iter().flatten() {
            push(case["caseId"].as_str().unwrap_or("-"), row);
        }
    }
    lines.push(format!(
        "improvements={} regressions={}",
        report["improvements"], report["regressions"]
    ));
    lines.push("loss: <1.0 better, >1.0 worse, 1.0 unchanged; * = renormalised".to_owned());
    lines.join("\n") + "\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One mean per metric in [`METRICS`] order; `None` means the metric was not measured.
    fn stats(means: [Option<f64>; 5]) -> [Stat; 5] {
        means.map(|mean| Stat {
            mean,
            ci95: None,
            n: usize::from(mean.is_some()),
        })
    }

    /// `(passRate, judgeScore, tokens, costUsd, durationMs)`.
    fn baseline() -> [Stat; 5] {
        stats([Some(0.8), Some(0.6), Some(1000.0), Some(0.5), Some(5000.0)])
    }

    fn worse() -> [Stat; 5] {
        stats([Some(0.4), Some(0.3), Some(2000.0), Some(1.0), Some(10000.0)])
    }

    fn better() -> [Stat; 5] {
        stats([Some(1.0), Some(0.9), Some(500.0), Some(0.25), Some(2500.0)])
    }

    fn sum_weights(loss: &Loss) -> f64 {
        loss.weights.values().sum()
    }

    #[test]
    fn every_metric_has_exactly_one_weight() {
        assert_eq!(
            LOSS_WEIGHTS
                .iter()
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            metric_names()
        );
        let total = LOSS_WEIGHTS.iter().map(|(_, weight)| weight).sum::<f64>();
        assert!((total - 1.0).abs() < 1e-9, "{total}");
    }

    #[test]
    fn identical_inputs_are_a_loss_of_exactly_one() {
        let baseline = baseline();
        let loss = loss(&baseline, &baseline).expect("loss");
        assert_eq!(loss.value, 1.0);
        assert!(!loss.renormalised);
        assert_eq!(loss.components.len(), METRICS.len());
        for (name, component) in &loss.components {
            assert_eq!(component.ratio, 1.0, "{name}");
        }
        // Nothing was dropped, so the authored weights survive unchanged.
        for (name, weight) in LOSS_WEIGHTS {
            assert_eq!(loss.weights[name], weight, "{name}");
            assert_eq!(loss.components[name].weight, weight, "{name}");
        }
        assert_eq!(sum_weights(&loss), 1.0);
    }

    #[test]
    fn worse_is_above_one_and_better_is_below_one() {
        let baseline = baseline();
        let worse = loss(&worse(), &baseline).expect("loss");
        assert!(worse.value > 1.0, "{}", worse.value);
        assert!(
            worse
                .components
                .values()
                .all(|component| component.ratio > 1.0)
        );
        let better = loss(&better(), &baseline).expect("loss");
        assert!(better.value < 1.0, "{}", better.value);
        assert!(
            better
                .components
                .values()
                .all(|component| component.ratio < 1.0)
        );
        assert_eq!(sum_weights(&worse), 1.0);
        assert!(!worse.renormalised && !better.renormalised);
    }

    #[test]
    fn a_missing_metric_drops_its_weight_and_renormalises() {
        let mut without_cost = baseline();
        without_cost[3].mean = None;
        let mut candidate = worse();
        candidate[3].mean = None;
        let loss = loss(&candidate, &without_cost).expect("loss");
        assert!(loss.renormalised);
        assert!(!loss.components.contains_key("costUsd"));
        assert!(!loss.weights.contains_key("costUsd"));
        assert_eq!(loss.components.len(), 4);
        assert!(
            (sum_weights(&loss) - 1.0).abs() < 1e-9,
            "{:?}",
            loss.weights
        );
        // The remaining four keep their relative weights (0.35/0.9, 0.35/0.9, 0.1/0.9, 0.1/0.9).
        assert_eq!(loss.components["passRate"].weight, 0.388889);
        assert_eq!(loss.components["tokens"].weight, 0.111111);
    }

    #[test]
    fn fewer_than_two_components_is_not_a_loss() {
        // Only pass rate survives.
        let baseline = stats([Some(0.8), None, None, None, None]);
        let candidate = stats([Some(0.4), None, None, None, None]);
        assert_eq!(loss(&candidate, &baseline), None);
        // A zero baseline cannot be compared against, so it drops like a missing mean.
        let baseline = stats([Some(0.8), Some(0.0), None, None, None]);
        let candidate = stats([Some(0.4), Some(0.5), None, None, None]);
        assert_eq!(loss(&candidate, &baseline), None);
        // Nothing measured at all.
        assert_eq!(loss(&stats([None; 5]), &stats([None; 5])), None);
    }

    #[test]
    fn a_ratio_is_clamped_so_one_metric_cannot_dominate() {
        let baseline = stats([Some(1.0), Some(1.0), Some(1000.0), Some(1.0), Some(1000.0)]);
        let extreme = stats([
            Some(1.0),
            Some(1.0),
            Some(100_000.0),
            Some(1.0),
            Some(1000.0),
        ]);
        let beyond = stats([
            Some(1.0),
            Some(1.0),
            Some(1_000_000.0),
            Some(1.0),
            Some(1000.0),
        ]);
        let first = loss(&extreme, &baseline).expect("loss");
        assert_eq!(first.components["tokens"].ratio, 4.0);
        // 0.9 * 1.0 + 0.1 * 4.0, and the total stops reacting past the clamp.
        assert_eq!(first.value, 1.3);
        let second = loss(&beyond, &baseline).expect("loss");
        assert_eq!(second.value, first.value);
        assert_eq!(second.components["tokens"].ratio, 4.0);
        assert_eq!(second.view(), first.view());
    }

    #[test]
    fn loss_is_deterministic() {
        let (candidate, baseline) = (worse(), baseline());
        let first = loss(&candidate, &baseline).expect("loss").view();
        let second = loss(&candidate, &baseline).expect("loss").view();
        assert_eq!(first, second);
        assert_eq!(
            serde_json::to_string(&first).expect("json"),
            serde_json::to_string(&second).expect("json")
        );
    }
}
