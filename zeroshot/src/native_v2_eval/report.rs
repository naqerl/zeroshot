use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use super::{Capacity, EvalData, EvalId, EvalSample, SampleStatus, TokenUsage, scorer::ScorerOutput};

pub const REPORT_SCHEMA: &str = "zeroshot.eval-report/v1";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalReport {
    pub schema: String,
    pub eval_id: EvalId,
    pub eval_name: String,
    pub generated_at_ms: u64,
    pub scorer: String,
    /// Effective scheduler width and its provenance for this attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<Capacity>,
    pub aggregate_score: f64,
    /// Weighted quality (Q) over every attempted sample.
    pub quality: f64,
    pub matrix: TraitMatrix,
    pub cost_time: CostTimeComparison,
    /// Paired X vs Y comparison for the first two arms, when judge scores exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison: Option<XyComparison>,
    pub unmatched_reasoning: Vec<UnmatchedReasoning>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TraitMatrix {
    pub columns: Vec<TraitColumn>,
    pub rows: Vec<TraitMatrixRow>,
    pub traits: Vec<TraitSummary>,
    pub arms: Vec<ArmTraitSummary>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TraitColumn {
    pub id: String,
    pub description: String,
    pub weight: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TraitMatrixRow {
    pub sample_id: String,
    pub arm_id: String,
    pub run_index: u32,
    pub status: SampleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_executor: Option<String>,
    pub cells: BTreeMap<String, TraitCell>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TraitCell {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TraitSummary {
    pub id: String,
    pub description: String,
    pub weight: f64,
    pub passed: u64,
    pub attempted: u64,
    pub skipped: u64,
    pub rate: f64,
    /// Judge score mean on the 1..=5 scale. Old boolean-only results omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_score: Option<f64>,
    #[serde(default)]
    pub scored: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArmTraitSummary {
    pub arm_id: String,
    pub quality: f64,
    pub succeeded: u64,
    pub failed: u64,
    pub skipped: u64,
    pub traits: Vec<TraitSummary>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UnmatchedReasoning {
    pub sample_id: String,
    pub arm_id: String,
    pub key: String,
    pub reasoning: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct XyComparison {
    pub arm_x: String,
    pub arm_y: String,
    pub traits: Vec<XyTrait>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct XyTrait {
    pub id: String,
    pub description: String,
    pub weight: f64,
    pub scored_pairs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x_mean: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y_mean: Option<f64>,
    /// Mean of Y - X over paired samples.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paired_difference: Option<f64>,
    pub outliers: Vec<XyOutlier>,
}

/// The pairs with the largest absolute Y - X difference, including both judges' reasoning.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct XyOutlier {
    pub x_sample_id: String,
    pub y_sample_id: String,
    pub run_index: u32,
    pub x_score: f64,
    pub y_score: f64,
    /// Y - X on the 1..=5 scale.
    pub difference: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x_reasoning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y_reasoning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x_judge_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y_judge_model: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CostTimeComparison {
    pub baseline_arm: String,
    pub arms: Vec<ArmCostTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fit: Option<QualityCostFit>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArmCostTime {
    pub arm_id: String,
    pub quality: f64,
    pub wall_clock_ms: u64,
    pub queue_wait_ms: u64,
    pub tokens: TokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_ratio: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_ratio: Option<f64>,
}

/// Least-squares fit of `Q = a + b * (C / Cbase)`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QualityCostFit {
    pub a: f64,
    pub b: f64,
}

pub(crate) struct Analysis {
    pub matrix: TraitMatrix,
    pub cost_time: CostTimeComparison,
    pub comparison: Option<XyComparison>,
    pub unmatched_reasoning: Vec<UnmatchedReasoning>,
    pub quality: f64,
}

pub(crate) fn analyze(data: &EvalData) -> Analysis {
    let columns = data
        .manifest
        .criteria
        .iter()
        .map(|criterion| TraitColumn {
            id: criterion.id.clone(),
            description: criterion.description.clone(),
            weight: criterion.weight,
        })
        .collect::<Vec<_>>();
    let criterion_ids = data
        .manifest
        .criteria
        .iter()
        .map(|criterion| criterion.id.as_str())
        .collect::<BTreeSet<_>>();
    let ordered = ordered_samples(data);
    let rows = ordered
        .iter()
        .map(|sample| matrix_row(sample, &columns))
        .collect::<Vec<_>>();
    let traits = summaries(&columns, &rows);
    let arms = data
        .manifest
        .arms
        .iter()
        .map(|arm| {
            let arm_rows = rows
                .iter()
                .filter(|row| row.arm_id == arm.id)
                .cloned()
                .collect::<Vec<_>>();
            let arm_traits = summaries(&columns, &arm_rows);
            let counts = counts(&ordered, &arm.id);
            ArmTraitSummary {
                arm_id: arm.id.clone(),
                quality: weighted_quality(&arm_traits),
                succeeded: counts.succeeded,
                failed: counts.failed,
                skipped: counts.skipped,
                traits: arm_traits,
            }
        })
        .collect::<Vec<_>>();
    let quality = weighted_quality(&traits);
    let cost_time = cost_time(data, &arms);
    let comparison = comparison(data, &columns, &ordered);
    let unmatched_reasoning = unmatched_reasoning(data, &criterion_ids);
    Analysis {
        matrix: TraitMatrix {
            columns,
            rows,
            traits,
            arms,
        },
        cost_time,
        comparison,
        unmatched_reasoning,
        quality,
    }
}

pub(crate) struct ReportStamp {
    pub capacity: Option<Capacity>,
    pub generated_at_ms: u64,
}

impl EvalReport {
    pub(crate) fn from_analysis(
        data: &EvalData,
        analysis: Analysis,
        output: ScorerOutput,
        stamp: ReportStamp,
    ) -> Self {
        let mut cost_time = analysis.cost_time;
        cost_time.fit = output.fit;
        Self {
            schema: REPORT_SCHEMA.to_owned(),
            eval_id: data.id.clone(),
            eval_name: data.manifest.name.clone(),
            generated_at_ms: stamp.generated_at_ms,
            scorer: output.name,
            capacity: stamp.capacity,
            aggregate_score: output.score,
            quality: output.quality,
            matrix: analysis.matrix,
            cost_time,
            comparison: analysis.comparison,
            unmatched_reasoning: analysis.unmatched_reasoning,
            notes: output.notes,
        }
    }
}

/// Render the Markdown view strictly from the JSON report value.
#[must_use]
pub fn render_markdown(report: &EvalReport) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Eval: {}", report.eval_name);
    let _ = writeln!(out);
    let _ = writeln!(out, "- ID: `{}`", report.eval_id);
    let _ = writeln!(out, "- Generated: {}", report.generated_at_ms);
    let _ = writeln!(out, "- Scorer: `{}`", report.scorer);
    if let Some(capacity) = report.capacity {
        let _ = writeln!(
            out,
            "- Capacity: {} ({})",
            capacity.concurrency,
            capacity.capacity_source.label()
        );
    }
    let _ = writeln!(out, "- Aggregate score: {:.4}", report.aggregate_score);
    let _ = writeln!(out, "- Quality (Q): {:.4}", report.quality);
    let _ = writeln!(out);

    let _ = writeln!(out, "## Trait matrix");
    let _ = writeln!(out);
    out.push_str("| Sample | Arm | Run | Status | Judge |");
    for column in &report.matrix.columns {
        let _ = write!(out, " {} |", escape(&column.id));
    }
    out.push('\n');
    out.push_str("| --- | --- | ---: | --- | --- |");
    for _ in &report.matrix.columns {
        out.push_str(" --- |");
    }
    out.push('\n');
    for row in &report.matrix.rows {
        let _ = write!(
            out,
            "| `{}` | `{}` | {} | {} | {} |",
            escape(&row.sample_id),
            escape(&row.arm_id),
            row.run_index,
            status_label(row.status),
            judge_label(row.judge_model.as_deref(), row.judge_executor.as_deref())
        );
        for column in &report.matrix.columns {
            let cell = row.cells.get(&column.id);
            out.push(' ');
            out.push_str(&cell_label(cell));
            out.push_str(" |");
        }
        out.push('\n');
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Trait rates");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Trait | Weight | Passed | Attempted | Skipped | Rate | Mean score | Scored |"
    );
    let _ = writeln!(
        out,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for summary in &report.matrix.traits {
        let _ = writeln!(
            out,
            "| {} | {:.2} | {} | {} | {} | {:.4} | {} | {} |",
            escape(&summary.id),
            summary.weight,
            summary.passed,
            summary.attempted,
            summary.skipped,
            summary.rate,
            mean_label(summary.mean_score),
            summary.scored
        );
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Judge errors");
    let _ = writeln!(out);
    let mut any_errors = false;
    for row in &report.matrix.rows {
        for column in &report.matrix.columns {
            let Some(cell) = row.cells.get(&column.id) else {
                continue;
            };
            for error in &cell.errors {
                any_errors = true;
                let _ = writeln!(
                    out,
                    "- `{}` / `{}` / {}: {}",
                    escape(&row.sample_id),
                    escape(&row.arm_id),
                    escape(&column.id),
                    escape(error)
                );
            }
        }
    }
    if !any_errors {
        let _ = writeln!(out, "None.");
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Cost and time");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Arm | Q | T/Tbase | C/Cbase | Wall clock (ms) | Queue wait (ms) | Tokens |"
    );
    let _ = writeln!(out, "| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
    for arm in &report.cost_time.arms {
        let _ = writeln!(
            out,
            "| {} | {:.4} | {} | {} | {} | {} | {} |",
            escape(&arm.arm_id),
            arm.quality,
            ratio_label(arm.time_ratio),
            ratio_label(arm.cost_ratio),
            arm.wall_clock_ms,
            arm.queue_wait_ms,
            arm.tokens.total()
        );
    }
    let _ = writeln!(out);
    match &report.cost_time.fit {
        Some(fit) => {
            let _ = writeln!(
                out,
                "Fitted `Q = {:.6} + {:.6} * (C / Cbase)` (baseline `{}`).",
                fit.a,
                fit.b,
                escape(&report.cost_time.baseline_arm)
            );
        }
        None => {
            let _ = writeln!(out, "No quality-vs-cost fit is available.");
        }
    }
    let _ = writeln!(out);

    if let Some(comparison) = &report.comparison {
        let _ = writeln!(out, "## X vs Y");
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "X is `{}`, Y is `{}`. Difference is Y - X on the judge's 1..=5 scale; positive values favor Y.",
            escape(&comparison.arm_x),
            escape(&comparison.arm_y)
        );
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "| Trait | X mean | Y mean | Paired difference | Pairs |"
        );
        let _ = writeln!(out, "| --- | ---: | ---: | ---: | ---: |");
        for trait_summary in &comparison.traits {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} |",
                escape(&trait_summary.id),
                mean_label(trait_summary.x_mean),
                mean_label(trait_summary.y_mean),
                mean_label(trait_summary.paired_difference),
                trait_summary.scored_pairs
            );
        }
        let _ = writeln!(out);
        let _ = writeln!(out, "### Outlier reasoning");
        let _ = writeln!(out);
        let mut any = false;
        for trait_summary in &comparison.traits {
            for outlier in &trait_summary.outliers {
                any = true;
                let _ = writeln!(
                    out,
                    "- {} / run {} / `{}` -> `{}`: X {:.2}, Y {:.2}, difference {:+.2}",
                    escape(&trait_summary.id),
                    outlier.run_index,
                    escape(&outlier.x_sample_id),
                    escape(&outlier.y_sample_id),
                    outlier.x_score,
                    outlier.y_score,
                    outlier.difference
                );
                if let Some(reasoning) = &outlier.x_reasoning {
                    let _ = writeln!(
                        out,
                        "  - X{}: {}",
                        model_suffix(outlier.x_judge_model.as_deref()),
                        escape(reasoning)
                    );
                }
                if let Some(reasoning) = &outlier.y_reasoning {
                    let _ = writeln!(
                        out,
                        "  - Y{}: {}",
                        model_suffix(outlier.y_judge_model.as_deref()),
                        escape(reasoning)
                    );
                }
            }
        }
        if !any {
            let _ = writeln!(out, "No unequal paired judge scores were recorded.");
        }
    }
    let _ = writeln!(out);

    let _ = writeln!(out, "## Unmatched reasoning");
    let _ = writeln!(out);
    if report.unmatched_reasoning.is_empty() {
        let _ = writeln!(out, "None.");
    } else {
        for entry in &report.unmatched_reasoning {
            let _ = writeln!(
                out,
                "- `{}` / `{}` / `{}`: {}",
                escape(&entry.sample_id),
                escape(&entry.arm_id),
                escape(&entry.key),
                escape(&entry.reasoning)
            );
        }
    }
    if !report.notes.is_empty() {
        let _ = writeln!(out);
        let _ = writeln!(out, "## Notes");
        let _ = writeln!(out);
        for note in &report.notes {
            let _ = writeln!(out, "- {}", escape(note));
        }
    }
    out
}

fn ordered_samples(data: &EvalData) -> Vec<&EvalSample> {
    let mut ordered = Vec::with_capacity(data.samples.len());
    for arm in &data.manifest.arms {
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
        ordered.extend(samples);
    }
    let known = data
        .manifest
        .arms
        .iter()
        .map(|arm| arm.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut extra = data
        .samples
        .iter()
        .filter(|sample| !known.contains(sample.arm_id.as_str()))
        .collect::<Vec<_>>();
    extra.sort_by(|left, right| {
        left.arm_id
            .cmp(&right.arm_id)
            .then_with(|| left.run_index.cmp(&right.run_index))
            .then_with(|| left.id.cmp(&right.id))
    });
    ordered.extend(extra);
    ordered
}

fn matrix_row(sample: &EvalSample, columns: &[TraitColumn]) -> TraitMatrixRow {
    let cells = columns
        .iter()
        .map(|column| {
            let result = sample.criteria.get(&column.id);
            let cell = match sample.status {
                SampleStatus::Skipped => TraitCell {
                    passed: None,
                    score: result.and_then(|result| result.score),
                    reasoning: result.and_then(|result| result.reasoning.clone()),
                    errors: result
                        .map(|result| result.errors.clone())
                        .unwrap_or_default(),
                },
                SampleStatus::Failed => TraitCell {
                    passed: Some(result.is_some_and(|result| result.passed)),
                    score: result.and_then(|result| result.score),
                    reasoning: result
                        .and_then(|result| result.reasoning.clone())
                        .or_else(|| sample.failure_reason.clone()),
                    errors: result
                        .map(|result| result.errors.clone())
                        .unwrap_or_default(),
                },
                SampleStatus::Succeeded => TraitCell {
                    passed: result.map(|result| result.passed),
                    score: result.and_then(|result| result.score),
                    reasoning: result.and_then(|result| result.reasoning.clone()),
                    errors: result
                        .map(|result| result.errors.clone())
                        .unwrap_or_default(),
                },
            };
            (column.id.clone(), cell)
        })
        .collect();
    TraitMatrixRow {
        sample_id: sample.id.clone(),
        arm_id: sample.arm_id.clone(),
        run_index: sample.run_index,
        status: sample.status,
        judge_model: sample.judge_model.clone(),
        judge_executor: sample.judge_executor.clone(),
        cells,
    }
}

fn summaries(columns: &[TraitColumn], rows: &[TraitMatrixRow]) -> Vec<TraitSummary> {
    columns
        .iter()
        .map(|column| {
            let mut passed = 0_u64;
            let mut attempted = 0_u64;
            let mut skipped = 0_u64;
            let mut scores = Vec::new();
            for row in rows {
                let cell = row.cells.get(&column.id);
                match cell.and_then(|cell| cell.passed) {
                    Some(true) => {
                        passed += 1;
                        attempted += 1;
                    }
                    Some(false) => attempted += 1,
                    None => skipped += 1,
                }
                if let Some(score) = cell.and_then(|cell| cell.score) {
                    scores.push(score);
                }
            }
            let scored = u64::try_from(scores.len()).unwrap_or(u64::MAX);
            TraitSummary {
                id: column.id.clone(),
                description: column.description.clone(),
                weight: column.weight,
                passed,
                attempted,
                skipped,
                rate: ratio(passed, attempted).unwrap_or(0.0),
                mean_score: mean(&scores),
                scored,
            }
        })
        .collect()
}

fn weighted_quality(traits: &[TraitSummary]) -> f64 {
    let mut weight = 0.0;
    let mut total = 0.0;
    for summary in traits {
        if summary.attempted == 0 {
            continue;
        }
        weight += summary.weight;
        total += summary.weight * summary.rate;
    }
    if weight > 0.0 { total / weight } else { 0.0 }
}

fn counts(samples: &[&EvalSample], arm_id: &str) -> SampleCounts {
    let mut counts = SampleCounts::default();
    for sample in samples.iter().filter(|sample| sample.arm_id == arm_id) {
        match sample.status {
            SampleStatus::Succeeded => counts.succeeded += 1,
            SampleStatus::Failed => counts.failed += 1,
            SampleStatus::Skipped => counts.skipped += 1,
        }
    }
    counts
}

#[derive(Default)]
struct SampleCounts {
    succeeded: u64,
    failed: u64,
    skipped: u64,
}

fn cost_time(data: &EvalData, arms: &[ArmTraitSummary]) -> CostTimeComparison {
    let baseline_arm = data
        .manifest
        .arms
        .first()
        .map(|arm| arm.id.clone())
        .unwrap_or_default();
    let mut measured = Vec::with_capacity(arms.len());
    for arm in arms {
        let mut wall_clock_ms = 0_u64;
        let mut queue_wait_ms = 0_u64;
        let mut tokens = TokenUsage::default();
        for sample in data
            .samples
            .iter()
            .filter(|sample| sample.arm_id == arm.arm_id && sample.status.is_attempted())
        {
            wall_clock_ms = wall_clock_ms.saturating_add(sample.timing.wall_clock_ms);
            queue_wait_ms = queue_wait_ms.saturating_add(sample.timing.queue_wait_ms);
            tokens.input = tokens.input.saturating_add(sample.tokens.input);
            tokens.output = tokens.output.saturating_add(sample.tokens.output);
            tokens.cache = tokens.cache.saturating_add(sample.tokens.cache);
        }
        measured.push(ArmCostTime {
            arm_id: arm.arm_id.clone(),
            quality: arm.quality,
            wall_clock_ms,
            queue_wait_ms,
            tokens,
            time_ratio: None,
            cost_ratio: None,
        });
    }
    let baseline_time = measured.first().map(|arm| arm.wall_clock_ms);
    let baseline_cost = measured.first().map(|arm| arm.tokens.total());
    for arm in &mut measured {
        arm.time_ratio = baseline_time.and_then(|baseline| ratio(arm.wall_clock_ms, baseline));
        arm.cost_ratio = baseline_cost.and_then(|baseline| ratio(arm.tokens.total(), baseline));
    }
    CostTimeComparison {
        baseline_arm,
        arms: measured,
        fit: None,
    }
}

fn comparison(
    data: &EvalData,
    columns: &[TraitColumn],
    samples: &[&EvalSample],
) -> Option<XyComparison> {
    let x_arm = data.manifest.arms.first()?;
    let y_arm = data.manifest.arms.get(1)?;
    let x_samples = samples
        .iter()
        .copied()
        .filter(|sample| sample.arm_id == x_arm.id)
        .collect::<Vec<_>>();
    let y_samples = samples
        .iter()
        .copied()
        .filter(|sample| sample.arm_id == y_arm.id)
        .collect::<Vec<_>>();
    let traits = columns
        .iter()
        .map(|column| {
            let x_scores = scores_for(&x_samples, &column.id);
            let y_scores = scores_for(&y_samples, &column.id);
            let mut pairs = Vec::new();
            for y_sample in &y_samples {
                let Some(y) = sample_score(y_sample, &column.id) else {
                    continue;
                };
                let Some(x_sample) = x_samples
                    .iter()
                    .find(|sample| sample.run_index == y_sample.run_index)
                else {
                    continue;
                };
                let Some(x) = sample_score(x_sample, &column.id) else {
                    continue;
                };
                pairs.push((*x_sample, *y_sample, x, y));
            }
            let differences = pairs
                .iter()
                .map(|(_, _, x, y)| y.values.0 - x.values.0)
                .collect::<Vec<_>>();
            let paired_difference = mean(&differences);
            let mut ordered = pairs;
            ordered.sort_by(
                |(x_left, y_left, x_score_left, y_score_left),
                 (x_right, y_right, x_score_right, y_score_right)| {
                    let left = (y_score_left.values.0 - x_score_left.values.0).abs();
                    let right = (y_score_right.values.0 - x_score_right.values.0).abs();
                    right
                        .total_cmp(&left)
                        .then_with(|| x_left.id.cmp(&x_right.id))
                        .then_with(|| y_left.id.cmp(&y_right.id))
                },
            );
            let outliers = ordered
                .into_iter()
                .take(3)
                .map(|(x_sample, y_sample, x, y)| XyOutlier {
                    x_sample_id: x_sample.id.clone(),
                    y_sample_id: y_sample.id.clone(),
                    run_index: x_sample.run_index,
                    x_score: x.values.0,
                    y_score: y.values.0,
                    difference: y.values.0 - x.values.0,
                    x_reasoning: x.values.1,
                    y_reasoning: y.values.1,
                    x_judge_model: x_sample.judge_model.clone(),
                    y_judge_model: y_sample.judge_model.clone(),
                })
                .collect::<Vec<_>>();
            XyTrait {
                id: column.id.clone(),
                description: column.description.clone(),
                weight: column.weight,
                scored_pairs: u64::try_from(differences.len()).unwrap_or(u64::MAX),
                x_mean: mean(&x_scores),
                y_mean: mean(&y_scores),
                paired_difference,
                outliers,
            }
        })
        .collect::<Vec<_>>();
    if traits
        .iter()
        .all(|trait_summary| trait_summary.scored_pairs == 0)
    {
        return None;
    }
    Some(XyComparison {
        arm_x: x_arm.id.clone(),
        arm_y: y_arm.id.clone(),
        traits,
    })
}

struct CellScore {
    values: (f64, Option<String>),
}

fn scores_for(samples: &[&EvalSample], criterion: &str) -> Vec<f64> {
    samples
        .iter()
        .filter_map(|sample| sample_score(sample, criterion).map(|score| score.values.0))
        .collect()
}

fn sample_score(sample: &EvalSample, criterion: &str) -> Option<CellScore> {
    let result = sample.criteria.get(criterion)?;
    let score = result.score?;
    Some(CellScore {
        values: (score, result.reasoning.clone()),
    })
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let sum = values.iter().sum::<f64>();
    sum.is_finite().then_some(sum / values.len() as f64)
}

fn unmatched_reasoning(data: &EvalData, criterion_ids: &BTreeSet<&str>) -> Vec<UnmatchedReasoning> {
    let mut unmatched = Vec::new();
    for sample in &data.samples {
        for (key, reasoning) in &sample.reasoning {
            if criterion_ids.contains(key.as_str()) || reasoning.trim().is_empty() {
                continue;
            }
            unmatched.push(UnmatchedReasoning {
                sample_id: sample.id.clone(),
                arm_id: sample.arm_id.clone(),
                key: key.clone(),
                reasoning: reasoning.clone(),
            });
        }
        for (key, result) in &sample.criteria {
            let Some(reasoning) = result.reasoning.as_deref() else {
                continue;
            };
            if criterion_ids.contains(key.as_str()) || reasoning.trim().is_empty() {
                continue;
            }
            unmatched.push(UnmatchedReasoning {
                sample_id: sample.id.clone(),
                arm_id: sample.arm_id.clone(),
                key: key.clone(),
                reasoning: reasoning.to_owned(),
            });
        }
    }
    unmatched
}

fn ratio(value: u64, baseline: u64) -> Option<f64> {
    if baseline == 0 {
        return (value == 0).then_some(1.0);
    }
    Some(value as f64 / baseline as f64)
}

fn status_label(status: SampleStatus) -> &'static str {
    match status {
        SampleStatus::Succeeded => "succeeded",
        SampleStatus::Failed => "failed",
        SampleStatus::Skipped => "skipped",
    }
}

fn cell_label(cell: Option<&TraitCell>) -> String {
    match cell.and_then(|cell| cell.passed) {
        Some(true) => "pass".to_owned(),
        Some(false) => "fail".to_owned(),
        None => "-".to_owned(),
    }
}

fn ratio_label(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{value:.4}"))
}

fn mean_label(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{value:.2}"))
}

fn model_suffix(model: Option<&str>) -> String {
    model.map_or_else(String::new, |model| format!(" (`{}`)", escape(model)))
}

fn judge_label(model: Option<&str>, executor: Option<&str>) -> String {
    match (model, executor) {
        (Some(model), Some(executor)) => {
            format!("`{}` ({})", escape(model), escape(executor))
        }
        (Some(model), None) => format!("`{}`", escape(model)),
        (None, Some(executor)) => format!("- ({})", escape(executor)),
        (None, None) => "-".to_owned(),
    }
}

fn escape(value: &str) -> String {
    value.replace('|', "\\|").replace(['\n', '\r'], " ")
}
