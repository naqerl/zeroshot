//! Baseline vs candidate scoring: `S = Q / (C^a * T^b)` per run and from the means.
//!
//! Every attempted sample contributes wall time `T` (minutes), cost `C` (USD, or millions of
//! tokens when no price is known), and quality `Q` (weighted share of passed acceptance traits,
//! `0..=1`). A failed run keeps its measured `T`/`C` and scores `Q = 0`; a skipped run is excluded.
//! The headline number is `S(candidate means) / S(baseline means)`: above 1 the candidate wins.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use super::{EvalArm, EvalData, EvalSample, SampleStatus};

pub const SCORE_SCHEMA: &str = "zeroshot.eval-score/v1";

/// Unit of the cost axis. Scores are only comparable within one unit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CostUnit {
    /// US dollars, recorded by the runner or priced from tokens with `pricing`.
    Usd,
    /// Millions of tokens (input + output + cache). Used when any attempted sample has no price.
    Mtok,
}

impl CostUnit {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Usd => "usd",
            Self::Mtok => "mtok",
        }
    }
}

/// Where one sample's cost came from.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CostSource {
    Recorded,
    Priced,
    Tokens,
}

/// One run on one side of the comparison.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunMetrics {
    pub sample_id: String,
    pub arm_id: String,
    pub run_index: u32,
    pub status: SampleStatus,
    /// Counted in the means: succeeded, or failed after the run actually started. A sample that
    /// failed before starting (no wall time, e.g. a missing executable) or was skipped is not.
    pub measured: bool,
    /// Wall time from submission to terminal state, in minutes.
    pub time_minutes: f64,
    pub cost: f64,
    pub cost_source: CostSource,
    /// Weighted passed-trait share in `0..=1`; absent for a succeeded run that was not judged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<f64>,
    pub traits_passed: u32,
    pub traits_judged: u32,
    pub traits_total: u32,
    /// `Q / (C^a * T^b)` for this run alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_tokens: u64,
}

/// One row of the slide table: the baseline and candidate runs with the same run index.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunPair {
    pub run_index: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<RunMetrics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<RunMetrics>,
}

/// Aggregates over one arm's measured runs.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArmAggregate {
    pub arm_id: String,
    /// Attempted (succeeded or failed) runs counted in the means.
    pub runs: u32,
    pub failed: u32,
    pub skipped: u32,
    /// Failed runs that never started (zero wall time); excluded from the means.
    pub not_started: u32,
    /// Succeeded runs without judged traits; excluded from the quality mean.
    pub unjudged: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_minutes: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<f64>,
    /// `S` computed from the means above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median_time_minutes: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median_cost: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median_quality: Option<f64>,
    /// `S` computed from the medians.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median_score: Option<f64>,
}

/// The whole baseline vs candidate result: "Baseline, N runs, one score."
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BaselineComparison {
    pub schema: String,
    pub eval_id: String,
    pub eval_name: String,
    pub baseline_arm: String,
    pub candidate_arm: String,
    /// Cost exponent.
    pub a: f64,
    /// Time exponent.
    pub b: f64,
    pub cost_unit: CostUnit,
    pub runs_per_arm: u32,
    pub pairs: Vec<RunPair>,
    pub baseline: ArmAggregate,
    pub candidate: ArmAggregate,
    /// `S_candidate / S_baseline` from the means; above 1 favors the candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratio: Option<f64>,
    /// The same ratio from the medians.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub median_ratio: Option<f64>,
    #[serde(default)]
    pub notes: Vec<String>,
}

/// `S = Q / (C^a * T^b)`. Undefined for a missing quality or a nonpositive cost or time.
#[must_use]
pub fn score(
    quality: Option<f64>,
    cost: Option<f64>,
    time: Option<f64>,
    weights: (f64, f64),
) -> Option<f64> {
    let (a, b) = weights;
    let (quality, cost, time) = (quality?, cost?, time?);
    if !(cost > 0.0 && time > 0.0) || !quality.is_finite() {
        return None;
    }
    let value = quality / (cost.powf(a) * time.powf(b));
    value.is_finite().then_some(value)
}

/// Ratio of two scores. Undefined when the baseline score is zero or absent.
#[must_use]
pub fn score_ratio(candidate: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    let (candidate, baseline) = (candidate?, baseline?);
    if baseline <= 0.0 {
        return None;
    }
    let value = candidate / baseline;
    value.is_finite().then_some(value)
}

/// Build the comparison for the manifest's baseline and candidate arms, if both exist.
#[must_use]
pub fn compare(data: &EvalData) -> Option<BaselineComparison> {
    let options = &data.manifest.options.score;
    let baseline_arm = match &options.baseline {
        Some(id) => data.manifest.arm(id)?,
        None => data.manifest.arms.first()?,
    };
    let candidate_arm = match &options.candidate {
        Some(id) => data.manifest.arm(id)?,
        None => data
            .manifest
            .arms
            .iter()
            .find(|arm| arm.id != baseline_arm.id)?,
    };
    let weights = (options.a, options.b);
    let attempted = |arm: &EvalArm| {
        data.samples
            .iter()
            .filter(|sample| sample.arm_id == arm.id && is_measured(sample))
            .collect::<Vec<_>>()
    };
    let baseline_samples = attempted(baseline_arm);
    let candidate_samples = attempted(candidate_arm);
    let priced = |arm: &EvalArm, sample: &EvalSample| {
        sample.cost_usd.is_some() || arm.pricing.or(data.manifest.options.pricing).is_some()
    };
    let all_priced = baseline_samples
        .iter()
        .all(|sample| priced(baseline_arm, sample))
        && candidate_samples
            .iter()
            .all(|sample| priced(candidate_arm, sample));
    let cost_unit = if all_priced {
        CostUnit::Usd
    } else {
        CostUnit::Mtok
    };
    let mut notes = Vec::new();
    if cost_unit == CostUnit::Mtok {
        notes.push(
            "cost is millions of tokens: add [options.pricing] (USD per million tokens) to report USD"
                .to_owned(),
        );
    }
    let metrics = |arm: &EvalArm| -> Vec<RunMetrics> {
        let mut samples = data
            .samples
            .iter()
            .filter(|sample| sample.arm_id == arm.id)
            .collect::<Vec<_>>();
        samples.sort_by(|left, right| {
            left.run_index
                .cmp(&right.run_index)
                .then_with(|| left.id.cmp(&right.id))
        });
        samples
            .into_iter()
            .map(|sample| {
                run_metrics(
                    &MetricContext {
                        data,
                        unit: cost_unit,
                        weights,
                    },
                    arm,
                    sample,
                )
            })
            .collect()
    };
    let baseline_runs = metrics(baseline_arm);
    let candidate_runs = metrics(candidate_arm);
    let baseline = aggregate(&baseline_arm.id, &baseline_runs, weights);
    let candidate = aggregate(&candidate_arm.id, &candidate_runs, weights);
    let mut indices = baseline_runs
        .iter()
        .chain(&candidate_runs)
        .map(|run| run.run_index)
        .collect::<Vec<_>>();
    indices.sort_unstable();
    indices.dedup();
    let pairs = indices
        .into_iter()
        .map(|run_index| RunPair {
            run_index,
            baseline: baseline_runs
                .iter()
                .find(|run| run.run_index == run_index)
                .cloned(),
            candidate: candidate_runs
                .iter()
                .find(|run| run.run_index == run_index)
                .cloned(),
        })
        .collect::<Vec<_>>();
    for side in [&baseline, &candidate] {
        if side.unjudged > 0 {
            notes.push(format!(
                "{} succeeded run(s) of arm {} have no judged traits and are excluded from its Q mean; run `zeroshot eval judge`",
                side.unjudged, side.arm_id
            ));
        }
        if side.not_started > 0 {
            notes.push(format!(
                "{} run(s) of arm {} failed before starting and are excluded from the means; see the sample failure reasons",
                side.not_started, side.arm_id
            ));
        }
        if side.failed > 0 {
            notes.push(format!(
                "{} failed run(s) of arm {} count with Q = 0 and their measured T and C",
                side.failed, side.arm_id
            ));
        }
    }
    let ratio = score_ratio(candidate.score, baseline.score);
    if ratio.is_none() {
        notes.push(
            "the score ratio is undefined: a side has no measured runs, no judged quality, zero cost or time, or a zero baseline score"
                .to_owned(),
        );
    }
    Some(BaselineComparison {
        schema: SCORE_SCHEMA.to_owned(),
        eval_id: data.id.to_string(),
        eval_name: data.manifest.name.clone(),
        baseline_arm: baseline_arm.id.clone(),
        candidate_arm: candidate_arm.id.clone(),
        a: weights.0,
        b: weights.1,
        cost_unit,
        runs_per_arm: data.manifest.runs_per_arm,
        pairs,
        ratio,
        median_ratio: score_ratio(candidate.median_score, baseline.median_score),
        baseline,
        candidate,
        notes,
    })
}

#[derive(Clone, Copy)]
struct MetricContext<'a> {
    data: &'a EvalData,
    unit: CostUnit,
    weights: (f64, f64),
}

fn run_metrics(context: &MetricContext<'_>, arm: &EvalArm, sample: &EvalSample) -> RunMetrics {
    let MetricContext {
        data,
        unit,
        weights,
    } = *context;
    let pricing = arm.pricing.or(data.manifest.options.pricing);
    let (cost, cost_source) = match (unit, sample.cost_usd, pricing) {
        (CostUnit::Usd, Some(cost), _) => (cost, CostSource::Recorded),
        (CostUnit::Usd, None, Some(pricing)) => {
            (pricing.cost_usd(&sample.tokens), CostSource::Priced)
        }
        _ => (
            sample.tokens.total() as f64 / 1_000_000.0,
            CostSource::Tokens,
        ),
    };
    let time_minutes = sample.timing.wall_clock_ms as f64 / 60_000.0;
    let mut weight_judged = 0.0;
    let mut weight_passed = 0.0;
    let mut judged = 0_u32;
    let mut passed = 0_u32;
    for criterion in &data.manifest.criteria {
        if let Some(result) = sample.criteria.get(&criterion.id) {
            judged += 1;
            weight_judged += criterion.weight;
            if result.passed {
                passed += 1;
                weight_passed += criterion.weight;
            }
        }
    }
    let quality = match sample.status {
        SampleStatus::Failed => Some(0.0),
        SampleStatus::Skipped => None,
        SampleStatus::Succeeded if judged == 0 || weight_judged <= 0.0 => None,
        SampleStatus::Succeeded => Some(weight_passed / weight_judged),
    };
    let measured = is_measured(sample);
    RunMetrics {
        sample_id: sample.id.clone(),
        arm_id: sample.arm_id.clone(),
        run_index: sample.run_index,
        status: sample.status,
        measured,
        time_minutes,
        cost,
        cost_source,
        quality,
        traits_passed: passed,
        traits_judged: judged,
        traits_total: u32::try_from(data.manifest.criteria.len()).unwrap_or(u32::MAX),
        score: if measured {
            score(quality, Some(cost), Some(time_minutes), weights)
        } else {
            None
        },
        input_tokens: sample.tokens.input,
        output_tokens: sample.tokens.output,
        cache_tokens: sample.tokens.cache,
    }
}

fn aggregate(arm_id: &str, runs: &[RunMetrics], weights: (f64, f64)) -> ArmAggregate {
    let attempted = runs.iter().filter(|run| run.measured).collect::<Vec<_>>();
    let times = attempted
        .iter()
        .map(|run| run.time_minutes)
        .collect::<Vec<_>>();
    let costs = attempted.iter().map(|run| run.cost).collect::<Vec<_>>();
    let qualities = attempted
        .iter()
        .filter_map(|run| run.quality)
        .collect::<Vec<_>>();
    let count = |predicate: &dyn Fn(&RunMetrics) -> bool| {
        u32::try_from(runs.iter().filter(|run| predicate(run)).count()).unwrap_or(u32::MAX)
    };
    let time_minutes = mean(&times);
    let cost = mean(&costs);
    let quality = mean(&qualities);
    let median_time_minutes = median(&times);
    let median_cost = median(&costs);
    let median_quality = median(&qualities);
    ArmAggregate {
        arm_id: arm_id.to_owned(),
        runs: u32::try_from(attempted.len()).unwrap_or(u32::MAX),
        failed: count(&|run| run.status == SampleStatus::Failed && run.measured),
        not_started: count(&|run| run.status == SampleStatus::Failed && !run.measured),
        skipped: count(&|run| run.status == SampleStatus::Skipped),
        unjudged: count(&|run| run.status == SampleStatus::Succeeded && run.quality.is_none()),
        score: score(quality, cost, time_minutes, weights),
        median_score: score(median_quality, median_cost, median_time_minutes, weights),
        time_minutes,
        cost,
        quality,
        median_time_minutes,
        median_cost,
        median_quality,
    }
}

fn is_measured(sample: &EvalSample) -> bool {
    match sample.status {
        SampleStatus::Succeeded => true,
        SampleStatus::Failed => sample.timing.wall_clock_ms > 0,
        SampleStatus::Skipped => false,
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Some(if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

/// The slide table as CSV: one row per run pair, then `mean` and `ratio` rows.
///
/// Columns: `run,t_base_min,c_base,q_base,s_base,t_min,c,q,s`. Cost is in `cost_unit`. Empty cells
/// mean "not measured". The `ratio` row carries `S_candidate / S_baseline` in the `s` column.
#[must_use]
pub fn table_csv(comparison: &BaselineComparison) -> String {
    let mut out = String::from("run,t_base_min,c_base,q_base,s_base,t_min,c,q,s\n");
    for pair in &comparison.pairs {
        let side = |run: Option<&RunMetrics>| match run {
            Some(run) if run.measured => [
                number(Some(run.time_minutes)),
                number(Some(run.cost)),
                number(run.quality),
                number(run.score),
            ]
            .join(","),
            _ => ",,,".to_owned(),
        };
        let _ = writeln!(
            out,
            "{},{},{}",
            pair.run_index,
            side(pair.baseline.as_ref()),
            side(pair.candidate.as_ref())
        );
    }
    let mean_side = |side: &ArmAggregate| {
        [
            number(side.time_minutes),
            number(side.cost),
            number(side.quality),
            number(side.score),
        ]
        .join(",")
    };
    let _ = writeln!(
        out,
        "mean,{},{}",
        mean_side(&comparison.baseline),
        mean_side(&comparison.candidate)
    );
    let _ = writeln!(out, "ratio,,,,,,,,{}", number(comparison.ratio));
    out
}

/// Long-format CSV with one row per sample, for Pareto plots (cost vs time, cost vs quality).
#[must_use]
pub fn runs_csv(comparison: &BaselineComparison) -> String {
    let mut out = String::from(
        "eval,role,arm,run,sample,status,measured,time_min,cost,cost_unit,cost_source,quality,score,traits_passed,traits_judged,traits_total,input_tokens,output_tokens,cache_tokens\n",
    );
    for pair in &comparison.pairs {
        for (role, run) in [
            ("baseline", pair.baseline.as_ref()),
            ("candidate", pair.candidate.as_ref()),
        ] {
            let Some(run) = run else { continue };
            let _ = writeln!(
                out,
                "{},{role},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                csv_field(&comparison.eval_id),
                csv_field(&run.arm_id),
                run.run_index,
                csv_field(&run.sample_id),
                status_label(run.status),
                run.measured,
                number(Some(run.time_minutes)),
                number(Some(run.cost)),
                comparison.cost_unit.label(),
                cost_source_label(run.cost_source),
                number(run.quality),
                number(run.score),
                run.traits_passed,
                run.traits_judged,
                run.traits_total,
                run.input_tokens,
                run.output_tokens,
                run.cache_tokens
            );
        }
    }
    out
}

/// Markdown section with the slide table, mean row, and the headline ratio.
#[must_use]
pub fn render_markdown(comparison: &BaselineComparison) -> String {
    let mut out = String::new();
    let unit = match comparison.cost_unit {
        CostUnit::Usd => "USD",
        CostUnit::Mtok => "Mtok",
    };
    let _ = writeln!(out, "## Baseline vs candidate");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Baseline is `{}`, candidate is `{}`. `S = Q / (C^a * T^b)` with a = {}, b = {}; T in minutes, C in {unit}, Q is the passed-trait share.",
        comparison.baseline_arm, comparison.candidate_arm, comparison.a, comparison.b
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "| Run | T base | C base | Q base | T | C | Q |");
    let _ = writeln!(out, "| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
    for pair in &comparison.pairs {
        let side = |run: Option<&RunMetrics>| match run {
            Some(run) if run.measured => format!(
                "{} | {} | {}",
                fixed(Some(run.time_minutes), 1),
                fixed(Some(run.cost), 2),
                fixed(run.quality, 2)
            ),
            Some(run) => format!("{} | - | -", status_label(run.status)),
            None => "- | - | -".to_owned(),
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} |",
            pair.run_index,
            side(pair.baseline.as_ref()),
            side(pair.candidate.as_ref())
        );
    }
    let mean_side = |side: &ArmAggregate| {
        format!(
            "**{}** | **{}** | **{}**",
            fixed(side.time_minutes, 1),
            fixed(side.cost, 2),
            fixed(side.quality, 2)
        )
    };
    let _ = writeln!(
        out,
        "| mean | {} | {} |",
        mean_side(&comparison.baseline),
        mean_side(&comparison.candidate)
    );
    let _ = writeln!(out);
    let verdict = match comparison.ratio {
        Some(ratio) if ratio > 1.0 => "candidate better",
        Some(ratio) if ratio < 1.0 => "baseline better",
        Some(_) => "no difference",
        None => "undefined",
    };
    let _ = writeln!(
        out,
        "**S_candidate / S_baseline = {}** (from the means; {verdict}). S_base = {}, S_cand = {}. Median ratio: {}.",
        fixed(comparison.ratio, 2),
        fixed(comparison.baseline.score, 4),
        fixed(comparison.candidate.score, 4),
        fixed(comparison.median_ratio, 2)
    );
    for note in &comparison.notes {
        let _ = writeln!(out);
        let _ = write!(out, "- {note}");
    }
    if !comparison.notes.is_empty() {
        let _ = writeln!(out);
    }
    out
}

fn number(value: Option<f64>) -> String {
    match value {
        Some(value) if value.is_finite() => {
            let text = format!("{value:.6}");
            let text = text.trim_end_matches('0').trim_end_matches('.');
            if text.is_empty() || text == "-" {
                "0".to_owned()
            } else {
                text.to_owned()
            }
        }
        _ => String::new(),
    }
}

fn fixed(value: Option<f64>, digits: usize) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{value:.digits$}"))
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

const fn status_label(status: SampleStatus) -> &'static str {
    match status {
        SampleStatus::Succeeded => "succeeded",
        SampleStatus::Failed => "failed",
        SampleStatus::Skipped => "skipped",
    }
}

const fn cost_source_label(source: CostSource) -> &'static str {
    match source {
        CostSource::Recorded => "recorded",
        CostSource::Priced => "priced",
        CostSource::Tokens => "tokens",
    }
}

#[cfg(test)]
#[path = "baseline_test.rs"]
mod tests;
