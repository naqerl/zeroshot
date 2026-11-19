use std::collections::BTreeMap;

use super::*;
use crate::native_v2_eval::{
    CriterionResult, EvalCriterion, EvalId, EvalManifest, Pricing, SampleTiming, TokenUsage,
};

const MANIFEST: &str = r#"
schema = "zeroshot.eval/v1"
name = "baseline-math"
runsPerArm = 5

[options]
scorer = "baseline-ratio"

[[arms]]
id = "baseline"
template = "software-change"
prompt = "Do the task."

[[arms]]
id = "candidate"
template = "software-change"
prompt = "Do the task."

[[criteria]]
id = "tests"
description = "Adds tests."

[[criteria]]
id = "docs"
description = "Updates docs."
"#;

fn manifest() -> EvalManifest {
    EvalManifest::parse(MANIFEST).expect("manifest parses")
}

struct Run {
    arm: &'static str,
    index: u32,
    status: SampleStatus,
    minutes: f64,
    cost: Option<f64>,
    passed: &'static [bool],
}

fn sample(manifest: &EvalManifest, run: &Run) -> EvalSample {
    let criteria = manifest
        .criteria
        .iter()
        .zip(run.passed)
        .map(|(criterion, passed)| {
            (
                criterion.id.clone(),
                CriterionResult {
                    passed: *passed,
                    score: Some(if *passed { 4.0 } else { 2.0 }),
                    reasoning: Some("judged".to_owned()),
                    errors: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    EvalSample {
        id: format!("{}-{:04}", run.arm, run.index),
        run_id: format!("run-{}-{}", run.arm, run.index),
        arm_id: run.arm.to_owned(),
        run_index: run.index,
        status: run.status,
        timing: SampleTiming {
            started_at_ms: 0,
            wall_clock_ms: (run.minutes * 60_000.0).round() as u64,
            queue_wait_ms: 0,
        },
        tokens: TokenUsage {
            input: 1_000_000,
            output: 100_000,
            cache: 0,
            ..TokenUsage::default()
        },
        cost_usd: run.cost,
        delivered_revision: None,
        failure_reason: None,
        patch: None,
        criteria,
        reasoning: BTreeMap::new(),
        judge_model: None,
        judge_template: None,
        judge_checkout: None,
        judge_executor: None,
    }
}

fn data(manifest: EvalManifest, runs: &[Run]) -> EvalData {
    let samples = runs.iter().map(|run| sample(&manifest, run)).collect();
    EvalData {
        id: EvalId::new("baseline-math").expect("id"),
        created_at_ms: 0,
        manifest,
        samples,
        report: None,
    }
}

fn close(left: f64, right: f64) -> bool {
    (left - right).abs() < 1e-9
}

/// The illustrative Eval 1 rows from the talk (T minutes, C USD, Q), N = 5 per side.
const SLIDE_BASELINE: [(f64, f64, f64); 5] = [
    (21.4, 2.31, 0.85),
    (18.7, 1.94, 0.82),
    (24.9, 2.72, 0.88),
    (20.2, 2.08, 0.84),
    (23.1, 2.46, 0.87),
];
const SLIDE_CANDIDATE: [(f64, f64, f64); 5] = [
    (25.3, 1.59, 0.86),
    (23.2, 1.40, 0.80),
    (28.6, 1.80, 0.88),
    (24.4, 1.54, 0.86),
    (27.0, 1.67, 0.86),
];

#[test]
fn score_is_quality_over_weighted_cost_and_time() {
    let value = score(Some(0.85), Some(2.3), Some(21.66), (0.5, 0.5)).expect("defined");
    assert!(close(value, 0.85 / (2.3_f64.sqrt() * 21.66_f64.sqrt())));
    // a = b = 0 ignores cost and time entirely.
    assert_eq!(
        score(Some(0.7), Some(9.0), Some(3.0), (0.0, 0.0)),
        Some(0.7)
    );
    // Time-focused weights.
    let value = score(Some(1.0), Some(4.0), Some(16.0), (0.2, 0.8)).expect("defined");
    assert!(close(value, 1.0 / (4.0_f64.powf(0.2) * 16.0_f64.powf(0.8))));
    assert_eq!(score(Some(1.0), Some(0.0), Some(1.0), (0.5, 0.5)), None);
    assert_eq!(score(Some(1.0), Some(1.0), Some(0.0), (0.5, 0.5)), None);
    assert_eq!(score(None, Some(1.0), Some(1.0), (0.5, 0.5)), None);
    assert_eq!(score_ratio(Some(1.0), Some(0.0)), None);
    assert_eq!(score_ratio(Some(1.5), Some(1.0)), Some(1.5));
}

#[test]
fn slide_table_ratio_comes_from_the_means() {
    // Q is represented by a single weighted trait so arbitrary pass rates are reproducible.
    let mut manifest = manifest();
    let mean = |rows: &[(f64, f64, f64)], index: usize| {
        rows.iter()
            .map(|row| [row.0, row.1, row.2][index])
            .sum::<f64>()
            / rows.len() as f64
    };
    let expected =
        |rows: &[(f64, f64, f64)]| mean(rows, 2) / (mean(rows, 1).sqrt() * mean(rows, 0).sqrt());
    let expected_ratio = expected(&SLIDE_CANDIDATE) / expected(&SLIDE_BASELINE);
    // Rebuild quality through trait verdicts: 100 equally weighted traits, Q*100 of them passed.
    manifest.criteria = (0..100)
        .map(|index| EvalCriterion {
            id: format!("t{index:03}"),
            description: "trait".to_owned(),
            weight: 1.0,
        })
        .collect();
    let mut samples = Vec::new();
    for (arm, rows) in [
        ("baseline", &SLIDE_BASELINE),
        ("candidate", &SLIDE_CANDIDATE),
    ] {
        for (index, (minutes, cost, quality)) in rows.iter().enumerate() {
            let passed = (quality * 100.0).round() as usize;
            let verdicts: &'static [bool] = Box::leak(
                (0..100)
                    .map(|trait_index| trait_index < passed)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            );
            samples.push(Run {
                arm,
                index: u32::try_from(index).expect("index"),
                status: SampleStatus::Succeeded,
                minutes: *minutes,
                cost: Some(*cost),
                passed: verdicts,
            });
        }
    }
    let comparison = compare(&data(manifest, &samples)).expect("comparison");
    assert_eq!(comparison.cost_unit, CostUnit::Usd);
    assert_eq!(comparison.pairs.len(), 5);
    assert_eq!(comparison.baseline.runs, 5);
    let ratio = comparison.ratio.expect("ratio");
    assert!(
        (ratio - expected_ratio).abs() < 1e-6,
        "{ratio} vs {expected_ratio}"
    );
    // The talk's slide reports 1.10 for this data.
    assert_eq!(format!("{ratio:.2}"), "1.10");
    assert!(close(
        comparison.baseline.quality.expect("q"),
        mean(&SLIDE_BASELINE, 2)
    ));
    let first = comparison.pairs[0]
        .candidate
        .as_ref()
        .expect("candidate run");
    assert_eq!(first.traits_passed, 86);
    assert_eq!(first.traits_total, 100);
    assert_eq!(first.cost_source, CostSource::Recorded);
}

#[test]
fn failed_runs_count_with_zero_quality_and_skipped_runs_are_excluded() {
    let runs = [
        Run {
            arm: "baseline",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 10.0,
            cost: Some(1.0),
            passed: &[true, true],
        },
        Run {
            arm: "baseline",
            index: 1,
            status: SampleStatus::Failed,
            minutes: 20.0,
            cost: Some(3.0),
            passed: &[],
        },
        Run {
            arm: "candidate",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 10.0,
            cost: Some(1.0),
            passed: &[true, false],
        },
        Run {
            arm: "candidate",
            index: 1,
            status: SampleStatus::Skipped,
            minutes: 99.0,
            cost: Some(99.0),
            passed: &[],
        },
        Run {
            arm: "candidate",
            index: 2,
            status: SampleStatus::Succeeded,
            minutes: 10.0,
            cost: Some(1.0),
            passed: &[],
        },
    ];
    let comparison = compare(&data(manifest(), &runs)).expect("comparison");
    let baseline = &comparison.baseline;
    assert_eq!(
        (baseline.runs, baseline.failed, baseline.skipped),
        (2, 1, 0)
    );
    assert!(close(baseline.quality.expect("q"), 0.5));
    assert!(close(baseline.time_minutes.expect("t"), 15.0));
    assert!(close(baseline.cost.expect("c"), 2.0));
    let candidate = &comparison.candidate;
    assert_eq!(
        (candidate.runs, candidate.skipped, candidate.unjudged),
        (2, 1, 1)
    );
    // The unjudged run still counts for T and C, but not for Q.
    assert!(close(candidate.quality.expect("q"), 0.5));
    assert!(close(candidate.cost.expect("c"), 1.0));
    let expected = (0.5 / (1.0_f64 * 10.0).sqrt()) / (0.5 / (2.0_f64 * 15.0).sqrt());
    assert!(close(comparison.ratio.expect("ratio"), expected));
    assert!(
        comparison
            .notes
            .iter()
            .any(|note| note.contains("no judged traits"))
    );
    assert!(comparison.notes.iter().any(|note| note.contains("Q = 0")));
    let skipped = comparison.pairs[1]
        .candidate
        .as_ref()
        .expect("skipped run kept");
    assert_eq!(skipped.score, None);
    assert!(!skipped.measured);
}

#[test]
fn runs_that_failed_before_starting_are_excluded_from_the_means() {
    let runs = [
        Run {
            arm: "baseline",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 10.0,
            cost: Some(1.0),
            passed: &[true, true],
        },
        Run {
            arm: "baseline",
            index: 1,
            status: SampleStatus::Failed,
            minutes: 0.0,
            cost: Some(0.0),
            passed: &[],
        },
        Run {
            arm: "candidate",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 10.0,
            cost: Some(1.0),
            passed: &[true, true],
        },
    ];
    let comparison = compare(&data(manifest(), &runs)).expect("comparison");
    assert_eq!(comparison.baseline.runs, 1);
    assert_eq!(comparison.baseline.not_started, 1);
    assert_eq!(comparison.baseline.failed, 0);
    assert!(close(comparison.ratio.expect("ratio"), 1.0));
    assert!(
        comparison
            .notes
            .iter()
            .any(|note| note.contains("failed before starting"))
    );
    assert!(
        table_csv(&comparison)
            .lines()
            .nth(2)
            .expect("row")
            .starts_with("1,,,,,")
    );
}

#[test]
fn cost_prefers_recorded_usd_then_pricing_then_tokens() {
    let mut manifest = manifest();
    manifest.options.pricing = Some(Pricing {
        input: 3.0,
        output: 15.0,
        cache_read: 0.3,
        cache_write: 3.75,
    });
    let runs = [
        Run {
            arm: "baseline",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 1.0,
            cost: Some(0.42),
            passed: &[true, true],
        },
        Run {
            arm: "candidate",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 1.0,
            cost: None,
            passed: &[true, true],
        },
    ];
    let comparison = compare(&data(manifest.clone(), &runs)).expect("comparison");
    assert_eq!(comparison.cost_unit, CostUnit::Usd);
    let baseline = comparison.pairs[0].baseline.as_ref().expect("baseline");
    assert_eq!(baseline.cost_source, CostSource::Recorded);
    assert!(close(baseline.cost, 0.42));
    let candidate = comparison.pairs[0].candidate.as_ref().expect("candidate");
    assert_eq!(candidate.cost_source, CostSource::Priced);
    // 1M input * $3 + 100k output * $15 per million tokens.
    assert!(close(candidate.cost, 4.5));

    manifest.options.pricing = None;
    let comparison = compare(&data(manifest, &runs)).expect("comparison");
    assert_eq!(comparison.cost_unit, CostUnit::Mtok);
    let baseline = comparison.pairs[0].baseline.as_ref().expect("baseline");
    assert_eq!(baseline.cost_source, CostSource::Tokens);
    assert!(close(baseline.cost, 1.1));
    assert!(
        comparison
            .notes
            .iter()
            .any(|note| note.contains("[options.pricing]"))
    );
}

#[test]
fn pricing_splits_cache_reads_and_writes() {
    let pricing = Pricing {
        input: 1.0,
        output: 2.0,
        cache_read: 0.1,
        cache_write: 1.25,
    };
    let split = TokenUsage {
        input: 1_000_000,
        output: 1_000_000,
        cache: 3_000_000,
        cache_read: 2_000_000,
        cache_write: 1_000_000,
    };
    assert!(close(pricing.cost_usd(&split), 1.0 + 2.0 + 0.2 + 1.25));
    let unsplit = TokenUsage {
        input: 0,
        output: 0,
        cache: 1_000_000,
        ..TokenUsage::default()
    };
    assert!(close(pricing.cost_usd(&unsplit), 0.1));
}

#[test]
fn csv_exports_have_the_slide_table_and_long_rows() {
    let runs = [
        Run {
            arm: "baseline",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 20.0,
            cost: Some(2.0),
            passed: &[true, false],
        },
        Run {
            arm: "baseline",
            index: 1,
            status: SampleStatus::Succeeded,
            minutes: 30.0,
            cost: Some(3.0),
            passed: &[true, true],
        },
        Run {
            arm: "candidate",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 25.0,
            cost: Some(1.5),
            passed: &[true, true],
        },
    ];
    let comparison = compare(&data(manifest(), &runs)).expect("comparison");
    let table = table_csv(&comparison);
    let lines = table.lines().collect::<Vec<_>>();
    assert_eq!(lines[0], "run,t_base_min,c_base,q_base,s_base,t_min,c,q,s");
    assert!(lines[1].starts_with("0,20,2,0.5,"), "{}", lines[1]);
    assert!(lines[2].starts_with("1,30,3,1,"), "{}", lines[2]);
    assert!(
        lines[2].ends_with(",,,,"),
        "missing candidate run stays empty: {}",
        lines[2]
    );
    assert!(lines[3].starts_with("mean,25,2.5,0.75,"), "{}", lines[3]);
    assert!(lines[4].starts_with("ratio,,,,,,,,"));
    assert_eq!(lines.len(), 5);

    let runs_csv = runs_csv(&comparison);
    let rows = runs_csv.lines().collect::<Vec<_>>();
    assert!(
        rows[0].starts_with("eval,role,arm,run,sample,status,measured,time_min,cost,cost_unit")
    );
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().any(|row| row.starts_with(
        "baseline-math,candidate,candidate,0,candidate-0000,succeeded,true,25,1.5,usd,recorded,1,"
    )));

    let markdown = render_markdown(&comparison);
    assert!(markdown.contains("| Run | T base | C base | Q base | T | C | Q |"));
    assert!(markdown.contains("S_candidate / S_baseline ="));

    let json = serde_json::to_value(&comparison).expect("serializes");
    assert_eq!(json["schema"], SCORE_SCHEMA);
    assert_eq!(json["costUnit"], "usd");
    let round_trip: BaselineComparison = serde_json::from_value(json).expect("round trips");
    assert_eq!(round_trip.pairs.len(), comparison.pairs.len());
    assert_eq!(round_trip.cost_unit, comparison.cost_unit);
}

#[test]
fn score_options_select_arms_and_validate_weights() {
    let source = MANIFEST.replace(
        "scorer = \"baseline-ratio\"",
        "scorer = \"baseline-ratio\"\n\n[options.score]\na = 0.2\nb = 0.8\nbaseline = \"candidate\"\ncandidate = \"baseline\"",
    );
    let manifest = EvalManifest::parse(&source).expect("manifest parses");
    let runs = [
        Run {
            arm: "baseline",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 1.0,
            cost: Some(1.0),
            passed: &[true, true],
        },
        Run {
            arm: "candidate",
            index: 0,
            status: SampleStatus::Succeeded,
            minutes: 1.0,
            cost: Some(1.0),
            passed: &[true, false],
        },
    ];
    let comparison = compare(&data(manifest, &runs)).expect("comparison");
    assert_eq!(comparison.baseline_arm, "candidate");
    assert_eq!(comparison.candidate_arm, "baseline");
    assert!(close(comparison.a, 0.2));
    assert!(close(comparison.ratio.expect("ratio"), 2.0));

    for (replacement, message) in [
        ("a = 1.5", "options.score.a"),
        ("baseline = \"missing\"", "unknown arm"),
    ] {
        let source = MANIFEST.replace(
            "scorer = \"baseline-ratio\"",
            &format!("scorer = \"baseline-ratio\"\n\n[options.score]\n{replacement}"),
        );
        let error = EvalManifest::parse(&source).expect_err("invalid score options");
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn single_arm_evals_have_no_comparison() {
    let mut manifest = manifest();
    manifest.arms.truncate(1);
    assert!(compare(&data(manifest, &[])).is_none());
}
