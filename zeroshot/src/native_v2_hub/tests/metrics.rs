use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde_json::json;

use super::super::api::{ApiState, router};
use super::super::daemon::bind_loopback_metrics;
use super::super::metrics::{Exposition, Histogram, Metrics, OTHER, VALUE_CAP, escape};
use super::super::model::{HubState, JobState, Schedule, ScheduleState};
use super::super::usage::{JobUsage, reported};
use super::super::placement::{Load, Reachability, Rejection};
use super::super::service::HubService;
use super::super::store::HubStore;
use super::{FakeDriver, job, member};
use crate::native_v2_target::FileTargetRegistry;

type Samples = Vec<(String, BTreeMap<String, String>, f64)>;

/// Checks the text exposition format: every sample belongs to a family declared with HELP and
/// TYPE, names and label syntax are valid, and histogram buckets are cumulative and end with
/// `+Inf` equal to `_count`. Returns the samples as `(name, labels, value)`.
pub(super) fn parse_exposition(text: &str) -> Samples {
    let mut families: BTreeMap<String, String> = BTreeMap::new();
    let mut samples = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            assert!(rest.split_once(' ').is_some(), "HELP has text: {line}");
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').expect("TYPE name kind");
            assert!(
                ["counter", "gauge", "histogram"].contains(&kind),
                "type {kind}"
            );
            assert!(
                families.insert(name.to_owned(), kind.to_owned()).is_none(),
                "family {name} declared once"
            );
            continue;
        }
        assert!(!line.starts_with('#'), "unexpected comment {line}");
        let (series, value) = line.rsplit_once(' ').expect("sample value");
        let value = match value {
            "+Inf" => f64::INFINITY,
            value => value.parse::<f64>().expect("numeric value"),
        };
        let (name, labels) = match series.split_once('{') {
            Some((name, labels)) => (name, labels.strip_suffix('}').expect("closing brace")),
            None => (series, ""),
        };
        assert!(
            name.chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
                && name.starts_with("zeroshot_hub_"),
            "metric name {name}"
        );
        let mut parsed = BTreeMap::new();
        let mut rest = labels;
        while !rest.is_empty() {
            let (key, after) = rest.split_once("=\"").expect("label key");
            let mut label = String::new();
            let mut chars = after.char_indices();
            let end = loop {
                let (index, character) = chars.next().expect("closing quote");
                match character {
                    '\\' => {
                        let (_, escaped) = chars.next().expect("escape");
                        label.push(if escaped == 'n' { '\n' } else { escaped });
                    }
                    '"' => break index,
                    other => label.push(other),
                }
            };
            parsed.insert(key.to_owned(), label);
            rest = after[end + 1..].trim_start_matches(',');
        }
        let family = ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|suffix| {
                name.strip_suffix(suffix)
                    .filter(|base| families.get(*base).is_some_and(|kind| kind == "histogram"))
            })
            .unwrap_or(name);
        assert!(families.contains_key(family), "{name} has a TYPE line");
        samples.push((name.to_owned(), parsed, value));
    }
    for family in families
        .iter()
        .filter(|(_, kind)| *kind == "histogram")
        .map(|(family, _)| family)
    {
        check_histogram(family, &samples);
    }
    samples
}

fn check_histogram(family: &str, samples: &Samples) {
    let bucket = format!("{family}_bucket");
    let mut series: BTreeMap<String, Vec<(f64, f64)>> = BTreeMap::new();
    for (name, labels, value) in samples {
        if *name == bucket {
            let mut key = labels.clone();
            let le = key.remove("le").expect("le label");
            let le = if le == "+Inf" {
                f64::INFINITY
            } else {
                le.parse().expect("le")
            };
            series
                .entry(format!("{key:?}"))
                .or_default()
                .push((le, *value));
        }
    }
    for (key, buckets) in series {
        assert!(
            buckets
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0 && pair[0].1 <= pair[1].1),
            "{family}{key} buckets are ordered and cumulative: {buckets:?}"
        );
        let (last_le, last) = *buckets.last().expect("buckets");
        assert!(last_le.is_infinite(), "{family}{key} ends with +Inf");
        let count = samples
            .iter()
            .find(|(name, labels, _)| {
                *name == format!("{family}_count") && format!("{labels:?}") == key
            })
            .map(|(_, _, value)| *value)
            .expect("count");
        assert!((count - last).abs() < f64::EPSILON, "+Inf equals count");
    }
}

pub(super) fn sample(samples: &Samples, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    samples
        .iter()
        .find(|(sample, sample_labels, _)| {
            sample == name
                && labels
                    .iter()
                    .all(|(key, value)| sample_labels.get(*key).map(String::as_str) == Some(*value))
        })
        .map(|(_, _, value)| *value)
}

#[test]
fn exposition_escapes_labels_and_writes_cumulative_histograms() {
    assert_eq!(escape("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    let mut histogram = Histogram::new(&[1.0, 5.0]);
    for value in [0.5, 2.0, 7.0] {
        histogram.observe(value);
    }
    let mut out = Exposition::default();
    out.family("zeroshot_hub_test_seconds", "histogram", "Test.");
    out.histogram(
        "zeroshot_hub_test_seconds",
        &[("member", "a\"1")],
        &histogram,
    );
    let text = out.finish();
    assert_eq!(
        text,
        "# HELP zeroshot_hub_test_seconds Test.\n\
         # TYPE zeroshot_hub_test_seconds histogram\n\
         zeroshot_hub_test_seconds_bucket{member=\"a\\\"1\",le=\"1\"} 1\n\
         zeroshot_hub_test_seconds_bucket{member=\"a\\\"1\",le=\"5\"} 2\n\
         zeroshot_hub_test_seconds_bucket{member=\"a\\\"1\",le=\"+Inf\"} 3\n\
         zeroshot_hub_test_seconds_sum{member=\"a\\\"1\"} 9.5\n\
         zeroshot_hub_test_seconds_count{member=\"a\\\"1\"} 3\n"
    );
    parse_exposition(&text);
}

fn usage(input: u64) -> Option<JobUsage> {
    Some(reported(input, 10, Some(5)))
}

fn state_with_history(now: u64) -> HubState {
    let mut state = HubState {
        members: vec![member("a", &["gpu"], 2, 0), member("b", &[], 1, 0)],
        ..HubState::default()
    };
    let mut queued = job("hj_queued", now - 10_000);
    queued.labels.insert("task".to_owned(), "lint".to_owned());
    let mut later = job("hj_later", now);
    later.not_before_ms = Some(now + 60_000);
    let mut running = job("hj_running", now - 20_000);
    running.state = JobState::Running;
    running.target = Some("a".to_owned());
    running.run_id = Some("run-1".to_owned());
    running.dispatched_at_ms = Some(now - 18_000);
    state.jobs.extend([queued, later, running]);
    // More distinct models than the cap: the overflow collapses into `_other`.
    for index in 0..=VALUE_CAP {
        let mut done = job(&format!("hj_done_{index}"), now - 100_000);
        done.state = JobState::Succeeded;
        done.target = Some(if index == 0 { "gone" } else { "b" }.to_owned());
        done.dispatched_at_ms = Some(now - 99_000);
        done.finished_at_ms = Some(now - 90_000);
        done.models = vec![format!("model-{index:02}")];
        done.project = Some("citydiff".to_owned());
        done.labels
            .insert("task".to_owned(), format!("t{}", index % 3));
        done.token_usage = usage(100);
        state.jobs.push(done);
    }
    state.schedules.push(Schedule {
        schedule_id: "hs_1".to_owned(),
        state: ScheduleState::Active,
        cron: "* * * * *".to_owned(),
        timezone: "UTC".to_owned(),
        template: job("hs_1", now),
        repin_from: None,
        submission_key: None,
        created_at_ms: now,
        next_fire_ms: None,
        last_fired_ms: None,
        last_job_id: None,
        fired: 4,
        skipped: 1,
        removed_at_ms: None,
        error: None,
    });
    state
}

fn record_activity(metrics: &Metrics) {
    metrics.record_pass(0.2, false);
    metrics.record_pass(3.0, true);
    metrics.record_placement("a", "placed");
    metrics.record_placement("b", "dispatch_failed");
    metrics.record_rejection(None);
    metrics.record_rejection(Some(&[]));
    metrics.record_rejection(Some(&[
        Rejection {
            member: "b".to_owned(),
            reason: "target does not advertise openengine.node-runtime-lanes/v1".to_owned(),
        },
        Rejection {
            member: "local".to_owned(),
            reason: "local runs reject --environment hooks".to_owned(),
        },
    ]));
    metrics.record_loads(&BTreeMap::from([
        (
            "a".to_owned(),
            Load {
                reachability: Reachability::Reachable,
                active: 1,
                error: None,
            },
        ),
        (
            "b".to_owned(),
            Load {
                reachability: Reachability::Unreachable,
                active: 0,
                error: Some("down".to_owned()),
            },
        ),
    ]));
    metrics.record_request(("/v1/hub/jobs/{id}", "GET"), 200, 0.003);
    metrics.record_request(("/v1/hub/jobs/{id}", "GET"), 404, 0.002);
}

#[test]
fn state_metrics_cover_jobs_queue_cron_members_and_durations() {
    let now = 1_000_000;
    let metrics = Metrics::new(vec!["task".to_owned()]);
    record_activity(&metrics);
    let text = metrics.render(&state_with_history(now), now);
    let samples = parse_exposition(&text);
    assert!(!text.contains("hj_"), "no job IDs in labels:\n{text}");
    assert!(!text.contains("hs_1"), "no schedule IDs in labels");
    let get = |name: &str, labels: &[(&str, &str)]| sample(&samples, name, labels);
    assert_eq!(get("zeroshot_hub_jobs", &[("state", "queued")]), Some(1.0));
    assert_eq!(
        get("zeroshot_hub_jobs", &[("state", "scheduled")]),
        Some(1.0)
    );
    assert_eq!(get("zeroshot_hub_jobs", &[("state", "running")]), Some(1.0));
    assert_eq!(
        get("zeroshot_hub_jobs", &[("state", "dequeued")]),
        Some(0.0)
    );
    assert_eq!(
        get("zeroshot_hub_queue_depth", &[("kind", "due")]),
        Some(1.0)
    );
    assert_eq!(get("zeroshot_hub_cron_fires_total", &[]), Some(4.0));
    assert_eq!(get("zeroshot_hub_cron_skips_total", &[]), Some(1.0));
    assert_eq!(
        get("zeroshot_hub_member_slots", &[("member", "a")]),
        Some(2.0)
    );
    assert_eq!(
        get("zeroshot_hub_member_slots_used", &[("member", "a")]),
        Some(1.0)
    );
    assert_eq!(
        get("zeroshot_hub_member_slots_free", &[("member", "a")]),
        Some(1.0)
    );
    assert_eq!(get("zeroshot_hub_member_up", &[("member", "a")]), Some(1.0));
    assert_eq!(get("zeroshot_hub_member_up", &[("member", "b")]), Some(0.0));
    assert_eq!(
        get(
            "zeroshot_hub_member_state",
            &[("member", "b"), ("state", "active")]
        ),
        Some(1.0)
    );
    // Every finished job ran 9 s; a former member is `_removed`.
    assert_eq!(
        get(
            "zeroshot_hub_job_run_duration_seconds_bucket",
            &[("target", "b"), ("outcome", "succeeded"), ("le", "5")]
        ),
        Some(0.0)
    );
    assert_eq!(
        get(
            "zeroshot_hub_job_run_duration_seconds_count",
            &[("target", "_removed")]
        ),
        Some(1.0)
    );
    assert_eq!(
        get(
            "zeroshot_hub_job_queue_wait_seconds_count",
            &[("target", "a")]
        ),
        Some(1.0)
    );
}

#[test]
fn counter_metrics_cover_passes_placements_rejections_api_and_bounded_tokens() {
    let now = 1_000_000;
    let metrics = Metrics::new(vec!["task".to_owned()]);
    record_activity(&metrics);
    let samples = parse_exposition(&metrics.render(&state_with_history(now), now));
    let get = |name: &str, labels: &[(&str, &str)]| sample(&samples, name, labels);
    assert_eq!(get("zeroshot_hub_passes_total", &[]), Some(2.0));
    assert_eq!(get("zeroshot_hub_pass_errors_total", &[]), Some(1.0));
    assert_eq!(
        get(
            "zeroshot_hub_pass_duration_seconds_bucket",
            &[("le", "0.25")]
        ),
        Some(1.0)
    );
    assert_eq!(
        get(
            "zeroshot_hub_placements_total",
            &[("member", "b"), ("outcome", "dispatch_failed")]
        ),
        Some(1.0)
    );
    for reason in [
        "no_capacity",
        "no_matching_member",
        "missing_capability",
        "local_unsupported",
    ] {
        assert_eq!(
            get(
                "zeroshot_hub_placement_rejections_total",
                &[("reason", reason)]
            ),
            Some(1.0),
            "{reason}"
        );
    }
    assert_eq!(
        get(
            "zeroshot_hub_api_requests_total",
            &[("route", "/v1/hub/jobs/{id}"), ("code", "404")]
        ),
        Some(1.0)
    );
    assert_eq!(
        get(
            "zeroshot_hub_api_request_duration_seconds_count",
            &[("route", "/v1/hub/jobs/{id}"), ("method", "GET")]
        ),
        Some(2.0)
    );
    let models = samples
        .iter()
        .filter(|(name, _, _)| name == "zeroshot_hub_tokens_total")
        .filter_map(|(_, labels, _)| labels.get("model").cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        models.len(),
        VALUE_CAP + 1,
        "{VALUE_CAP} models plus {OTHER}"
    );
    assert!(models.contains(OTHER));
    assert_eq!(
        get(
            "zeroshot_hub_tokens_total",
            &[
                ("target", "b"),
                ("model", "model-01"),
                ("project", "citydiff"),
                ("kind", "input")
            ]
        ),
        Some(100.0)
    );
    assert_eq!(
        get(
            "zeroshot_hub_label_tokens_total",
            &[("label", "task"), ("value", "t0"), ("kind", "cache_read")]
        ),
        Some(5.0 * 17.0)
    );
}

#[tokio::test]
async fn metrics_need_the_token_on_the_api_and_count_routes_by_template() {
    let directory = tempfile::tempdir().expect("tempdir");
    let service = Arc::new(
        HubService::open(
            HubStore::new(directory.path().join("hub")),
            Arc::new(FakeDriver::default()),
            FileTargetRegistry::new(directory.path().join("targets.json")),
        )
        .expect("service"),
    );
    let state = Arc::new(ApiState {
        service,
        token: "t0ken".to_owned(),
        heartbeat: Arc::new(Mutex::new(json!({}))),
        executable: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, router(state)).await });
    let http = reqwest::Client::new();
    let anonymous = http
        .get(format!("{url}/metrics"))
        .send()
        .await
        .expect("metrics");
    assert_eq!(anonymous.status().as_u16(), 401);
    let missing = http
        .get(format!("{url}/v1/hub/jobs/hj_missing"))
        .bearer_auth("t0ken")
        .send()
        .await
        .expect("status");
    assert_eq!(missing.status().as_u16(), 404);
    let scraped = http
        .get(format!("{url}/metrics"))
        .bearer_auth("t0ken")
        .send()
        .await
        .expect("metrics");
    assert_eq!(scraped.status().as_u16(), 200);
    assert!(
        scraped
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/plain; version=0.0.4"))
    );
    let text = scraped.text().await.expect("text");
    let samples = parse_exposition(&text);
    assert!(
        !text.contains("hj_missing"),
        "concrete paths never become labels"
    );
    assert_eq!(
        sample(
            &samples,
            "zeroshot_hub_api_requests_total",
            &[
                ("route", "/v1/hub/jobs/{id}"),
                ("method", "GET"),
                ("code", "404")
            ]
        ),
        Some(1.0)
    );
    assert_eq!(
        sample(
            &samples,
            "zeroshot_hub_api_requests_total",
            &[("route", "/metrics"), ("code", "401")]
        ),
        Some(1.0)
    );
}

#[tokio::test]
async fn the_unauthenticated_metrics_listener_must_be_loopback() {
    assert!(bind_loopback_metrics("127.0.0.1:0").await.is_ok());
    let refused = bind_loopback_metrics("0.0.0.0:0")
        .await
        .expect_err("wildcard refused");
    assert!(
        refused.to_string().contains("not a loopback address"),
        "{refused}"
    );
}
