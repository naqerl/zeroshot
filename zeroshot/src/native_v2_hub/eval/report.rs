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
        "{:<22} {:<16} {:>4} {:<18} {:<18} {:<20} {:<20} {:<20} {}",
        "CASE", "VARIANT", "N", "PASS", "JUDGE", "TOKENS", "COST_USD", "DURATION_MS", "VERDICT"
    );
    lines.push(header);
    let mut push = |case: &str, row: &Value| {
        lines.push(format!(
            "{:<22} {:<16} {:>4} {:<18} {:<18} {:<20} {:<20} {:<20} {}",
            case,
            row["variant"].as_str().unwrap_or("-"),
            row["aggregate"]["samples"],
            cell(row, "passRate", 2),
            cell(row, "judgeScore", 2),
            cell(row, "tokens", 0),
            cell(row, "costUsd", 4),
            cell(row, "durationMs", 0),
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
    lines.join("\n") + "\n"
}
