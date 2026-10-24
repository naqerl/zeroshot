use std::collections::BTreeMap;

use openengine_cluster_protocol::MAX_SAFE_GENERATION;
use serde_json::json;

use super::super::metrics::Metrics;
use super::super::model::{HubState, Job, JobState};
use super::super::stats::{ModelPrice, StatsQuery, aggregate};
use super::super::usage::{JobUsage, Tokens, UsageTotals, cost, reported};
use super::job;

fn status(usage: serde_json::Value) -> serde_json::Value {
    json!({"status": {"phase": "finished", "metadata": {"tokenUsage": usage}}})
}

fn incomplete(input: u64) -> JobUsage {
    let JobUsage::Reported(mut usage) = reported(input, 1, None) else {
        unreachable!()
    };
    usage.complete = false;
    JobUsage::Reported(usage)
}

fn finished(id: &str, usage: Option<JobUsage>) -> Job {
    let mut finished = job(id, 1_000);
    finished.state = JobState::Succeeded;
    finished.target = Some("a".to_owned());
    finished.models = vec!["m1".to_owned()];
    finished.token_usage = usage;
    finished
}

#[test]
fn status_usage_is_reported_absent_or_explicitly_invalid() {
    assert_eq!(JobUsage::from_status(&json!({"status": {}})), None);
    assert_eq!(JobUsage::from_status(&status(json!(null))), None);
    let valid = JobUsage::from_status(&status(
        json!({"inputTokens": 3, "outputTokens": 2, "complete": true}),
    ));
    assert_eq!(valid, Some(reported(3, 2, None)));
    for malformed in [
        json!({"inputTokens": -1, "outputTokens": 2, "complete": true}),
        json!({"inputTokens": MAX_SAFE_GENERATION + 1, "outputTokens": 2, "complete": true}),
        json!({"inputTokens": 1, "outputTokens": 2}),
        json!("lots"),
    ] {
        let usage = JobUsage::from_status(&status(malformed.clone())).expect("recorded");
        assert!(
            matches!(&usage, JobUsage::Invalid { invalid } if invalid.contains("unreadable tokenUsage")),
            "{malformed} -> {usage:?}"
        );
        assert!(usage.complete().is_none());
    }
    // State written before invalid usage existed reads back unchanged, and invalid usage
    // round-trips through state.json.
    let stored: JobUsage = serde_json::from_value(
        json!({"inputTokens": 3, "outputTokens": 2, "cacheReadInputTokens": 1, "complete": true}),
    )
    .expect("stored usage");
    assert_eq!(stored, reported(3, 2, Some(1)));
    let invalid = JobUsage::Invalid {
        invalid: "unreadable".to_owned(),
    };
    let text = serde_json::to_string(&invalid).expect("encode");
    assert_eq!(
        serde_json::from_str::<JobUsage>(&text).expect("decode"),
        invalid
    );
}

#[test]
fn totals_count_complete_usage_and_report_the_rest_separately() {
    let mut totals = UsageTotals::default();
    assert_eq!(totals.tokens(), None);
    assert!(totals.add(None).is_none());
    assert!(totals.add(Some(&reported(10, 5, Some(2)))).is_some());
    assert!(totals.add(Some(&incomplete(1_000))).is_none());
    let invalid = JobUsage::Invalid {
        invalid: "unreadable".to_owned(),
    };
    assert!(totals.add(Some(&invalid)).is_none());
    assert_eq!(
        totals.tokens(),
        Some(Tokens {
            input: 10,
            output: 5,
            cache_read: Some(2),
            cache_creation: None,
        })
    );
    totals.add(Some(&reported(1, 1, None)));
    let view = totals.view();
    assert_eq!(view["inputTokens"], json!(11));
    assert_eq!(view["outputTokens"], json!(6));
    // One counted usage did not report cache reads, so no cache total is claimed.
    assert_eq!(view["cacheReadInputTokens"], json!(null));
    assert_eq!(view["jobsWithUsage"], json!(4));
    assert_eq!(view["jobsWithIncompleteUsage"], json!(1));
    assert_eq!(view["jobsWithInvalidUsage"], json!(1));
    assert_eq!(view["usageOverflowed"], json!(false));
    assert_eq!(UsageTotals::default().view()["inputTokens"], json!(0));
}

#[test]
fn an_overflowing_total_is_withheld_rather_than_wrapped() {
    let largest = reported(MAX_SAFE_GENERATION, MAX_SAFE_GENERATION, Some(1));
    let mut totals = UsageTotals::default();
    // 2^64 / 2^53 = 2048 of the largest counts fit; one more overflows.
    for _ in 0..=2048 {
        totals.add(Some(&largest));
    }
    assert_eq!(totals.tokens(), None);
    let view = totals.view();
    assert_eq!(view["usageOverflowed"], json!(true));
    assert_eq!(view["inputTokens"], json!(null));
    assert_eq!(view["outputTokens"], json!(null));
    // A later small usage does not resurrect a partial total.
    totals.add(Some(&reported(1, 1, Some(1))));
    assert_eq!(totals.tokens(), None);
    assert_eq!(
        Tokens::of(&match largest {
            JobUsage::Reported(usage) => usage,
            JobUsage::Invalid { .. } => unreachable!(),
        })
        .input_and_output(),
        Some(2 * MAX_SAFE_GENERATION)
    );
    let huge = Tokens {
        input: u64::MAX,
        output: 1,
        cache_read: None,
        cache_creation: None,
    };
    assert_eq!(huge.input_and_output(), None);
    assert_eq!(
        huge.checked_add(Tokens {
            input: 0,
            output: 0,
            cache_read: Some(1),
            cache_creation: Some(1)
        })
        .map(|tokens| tokens.cache_read),
        Some(None)
    );
}

#[test]
fn only_complete_usage_is_priced() {
    let price = ModelPrice {
        input: 2.0,
        output: 10.0,
        cache_read: 1.0,
        cache_creation: 0.0,
    };
    let JobUsage::Reported(complete) = reported(1_000_000, 100_000, Some(1_000_000)) else {
        unreachable!()
    };
    assert_eq!(cost(&complete, &price), Some(4.0));
    let JobUsage::Reported(partial) = incomplete(1_000_000) else {
        unreachable!()
    };
    assert_eq!(cost(&partial, &price), None);

    let jobs = vec![
        finished("complete", Some(reported(1_000_000, 0, None))),
        finished("incomplete", Some(incomplete(5_000_000))),
        finished(
            "invalid",
            Some(JobUsage::Invalid {
                invalid: "unreadable".to_owned(),
            }),
        ),
        finished("none", None),
    ];
    let prices = BTreeMap::from([("m1".to_owned(), price)]);
    let result = aggregate(
        &jobs,
        &StatsQuery {
            by: &[],
            since_ms: None,
            until_ms: None,
            prices: Some(&prices),
        },
    );
    let totals = &result["totals"];
    assert_eq!(totals["inputTokens"], json!(1_000_000));
    assert_eq!(totals["estimatedCostUsd"], json!(2.0));
    assert_eq!(totals["unpricedJobs"], json!(0));
    assert_eq!(totals["jobsWithUsage"], json!(3));
    assert_eq!(totals["jobsWithIncompleteUsage"], json!(1));
    assert_eq!(totals["jobsWithInvalidUsage"], json!(1));
}

#[test]
fn metrics_count_complete_usage_and_saturate_instead_of_wrapping() {
    let mut state = HubState::default();
    state
        .jobs
        .push(finished("incomplete", Some(incomplete(7_777))));
    let largest = reported(MAX_SAFE_GENERATION, 1, None);
    for index in 0..=2048 {
        state
            .jobs
            .push(finished(&format!("large-{index}"), Some(largest.clone())));
    }
    let text = Metrics::new(Vec::new()).render(&state, 2_000);
    let input = text
        .lines()
        .find(|line| {
            line.starts_with("zeroshot_hub_tokens_total{") && line.contains("kind=\"input\"")
        })
        .expect("input tokens");
    #[allow(clippy::cast_precision_loss)]
    let saturated = u64::MAX as f64;
    let value: f64 = input
        .rsplit(' ')
        .next()
        .and_then(|value| value.parse().ok())
        .expect("value");
    assert!((value - saturated).abs() < 1.0, "{input}");
    assert!(!text.contains("7777"));
}
