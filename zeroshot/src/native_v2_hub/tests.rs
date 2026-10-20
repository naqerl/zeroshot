use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{Value, json};

use super::clock::{parse_duration_ms, parse_future_ms, parse_past_ms, rfc3339, utc_day};
use super::driver::{HubDriver, SubmitRequest};
use super::error::{EXIT_NO_CAPACITY, EXIT_NO_ELIGIBLE_TARGET, HubError};
use super::jobs::{repository_name, runtime_models};
use super::model::{
    HubState, Job, JobState, Member, MemberState, PlacementSpec, Requirements, Strategy, TokenUsage,
};
use super::placement::{Load, Placement, describe, place};
use super::schedule::{active_runs, apply_status, due_jobs, expire, tick};
use super::stats::{Dimension, ModelPrice, StatsQuery, aggregate, percentiles};

#[path = "tests/api.rs"]
mod api;
#[path = "tests/capabilities.rs"]
mod capabilities;
#[path = "tests/cron.rs"]
mod cron;
#[path = "tests/daemon.rs"]
mod daemon;
#[path = "tests/metrics.rs"]
mod metrics;

pub(super) fn member(name: &str, tags: &[&str], slots: u32, priority: i32) -> Member {
    Member {
        tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
        slots,
        priority,
        ..Member::new(name.to_owned(), 1)
    }
}

pub(super) fn job(id: &str, submitted_at_ms: u64) -> Job {
    Job {
        job_id: id.to_owned(),
        state: JobState::Queued,
        title: id.to_owned(),
        run_args: vec!["--title".to_owned(), id.to_owned()],
        cwd: std::path::PathBuf::from("/"),
        pinned_revision: None,
        placement: PlacementSpec::default(),
        requirements: Requirements::default(),
        priority: 0,
        labels: BTreeMap::new(),
        project: None,
        models: Vec::new(),
        submission_key: None,
        submitted_at_ms,
        not_before_ms: None,
        expires_at_ms: None,
        attempts: 0,
        target: None,
        run_id: None,
        dispatched_at_ms: None,
        finished_at_ms: None,
        failure_reason: None,
        token_usage: None,
        error: None,
        schedule_id: None,
    }
}

pub(super) fn spec(target: Option<&str>, tags: &[&str], strategy: Strategy) -> PlacementSpec {
    PlacementSpec {
        target: target.map(str::to_owned),
        tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
        strategy,
    }
}

pub(super) fn loads(entries: &[(&str, bool, u32)]) -> BTreeMap<String, Load> {
    entries
        .iter()
        .map(|(name, reachable, active)| {
            (
                (*name).to_owned(),
                Load {
                    reachable: *reachable,
                    active: *active,
                    error: None,
                },
            )
        })
        .collect()
}

#[test]
fn first_free_prefers_priority_then_registration_order() {
    let members = [
        member("a", &["linux"], 1, 0),
        member("b", &["linux"], 1, 5),
        member("c", &["linux"], 1, 5),
    ];
    let all_free = loads(&[("a", true, 0), ("b", true, 0), ("c", true, 0)]);
    let request = spec(None, &["linux"], Strategy::FirstFree);
    assert_eq!(
        place(&members, &request, &Requirements::default(), &all_free),
        Placement::Place("b".to_owned())
    );
    let b_busy = loads(&[("a", true, 0), ("b", true, 1), ("c", true, 0)]);
    assert_eq!(
        place(&members, &request, &Requirements::default(), &b_busy),
        Placement::Place("c".to_owned())
    );
}

#[test]
fn least_loaded_compares_active_to_slots_ratio() {
    let members = [member("a", &[], 4, 9), member("b", &[], 2, 0)];
    let request = spec(None, &[], Strategy::LeastLoaded);
    // a: 3/4 busy, b: 1/2 busy, so b is less loaded despite lower priority.
    let observed = loads(&[("a", true, 3), ("b", true, 1)]);
    assert_eq!(
        place(&members, &request, &Requirements::default(), &observed),
        Placement::Place("b".to_owned())
    );
    // Equal ratios fall back to priority.
    let equal = loads(&[("a", true, 2), ("b", true, 1)]);
    assert_eq!(
        place(&members, &request, &Requirements::default(), &equal),
        Placement::Place("a".to_owned())
    );
}

#[test]
fn placement_distinguishes_busy_from_no_eligible_member() {
    let mut drained = member("d", &["gpu"], 1, 0);
    drained.state = MemberState::Drain;
    let members = [
        member("a", &["gpu"], 1, 0),
        drained,
        member("b", &["cpu"], 1, 0),
    ];
    let gpu = spec(None, &["gpu"], Strategy::FirstFree);
    assert_eq!(
        place(
            &members,
            &gpu,
            &Requirements::default(),
            &loads(&[("a", true, 1), ("d", true, 0)])
        ),
        Placement::Busy {
            matching: vec!["a".to_owned()]
        }
    );
    assert_eq!(
        place(
            &members,
            &gpu,
            &Requirements::default(),
            &loads(&[("a", false, 0)])
        ),
        Placement::Busy {
            matching: vec!["a".to_owned()]
        },
        "an unreachable member is retryable, not ineligible"
    );
    let tpu = spec(None, &["tpu"], Strategy::FirstFree);
    assert_eq!(
        place(&members, &tpu, &Requirements::default(), &BTreeMap::new()),
        Placement::NoEligible {
            rejected: Vec::new()
        }
    );
    let pinned = spec(Some("b"), &["gpu"], Strategy::FirstFree);
    assert_eq!(
        place(
            &members,
            &pinned,
            &Requirements::default(),
            &BTreeMap::new()
        ),
        Placement::NoEligible {
            rejected: Vec::new()
        }
    );
    assert_eq!(describe(&pinned), "target b and tags [gpu]");
    assert_eq!(describe(&PlacementSpec::default()), "any member");
}

#[test]
fn due_jobs_order_by_priority_then_fifo_and_skip_scheduled() {
    let mut jobs = vec![job("j1", 10), job("j2", 20), job("j3", 30), job("j4", 5)];
    jobs[1].priority = 5;
    jobs[3].not_before_ms = Some(1_000);
    jobs[2].state = JobState::Running;
    let due = due_jobs(&jobs, 100);
    assert_eq!(
        due.iter()
            .map(|index| jobs[*index].job_id.as_str())
            .collect::<Vec<_>>(),
        ["j2", "j1"]
    );
    assert_eq!(jobs[3].display_state(100), "scheduled");
    assert_eq!(due_jobs(&jobs, 1_000).len(), 3);
}

#[test]
fn expire_only_affects_queued_jobs() {
    let mut jobs = vec![job("j1", 0), job("j2", 0), job("j3", 0)];
    jobs[0].expires_at_ms = Some(50);
    jobs[1].expires_at_ms = Some(500);
    jobs[2].expires_at_ms = Some(50);
    jobs[2].state = JobState::Running;
    assert_eq!(expire(&mut jobs, 100), ["j1"]);
    assert_eq!(jobs[0].state, JobState::Expired);
    assert_eq!(jobs[1].state, JobState::Queued);
    assert_eq!(jobs[2].state, JobState::Running);
}

#[test]
fn finished_status_records_outcome_and_tokens() {
    let mut running = job("j1", 0);
    running.state = JobState::Running;
    assert!(!apply_status(
        &mut running,
        &json!({"status": {"phase": "running", "activeExecutions": []}}),
        5
    ));
    let finished = json!({"status": {
        "phase": "finished",
        "terminalResult": {"status": "failed", "reason": "runtime_unavailable"},
        "metadata": {"tokenUsage": {"inputTokens": 10, "outputTokens": 4, "complete": true}}
    }});
    assert!(apply_status(&mut running, &finished, 9));
    assert_eq!(running.state, JobState::Failed);
    assert_eq!(
        running.failure_reason.as_deref(),
        Some("runtime_unavailable")
    );
    assert_eq!(
        running.token_usage,
        Some(TokenUsage {
            input_tokens: 10,
            output_tokens: 4,
            cache_read_input_tokens: None,
            cache_creation_input_tokens: None,
            complete: true,
        })
    );
    assert_eq!(running.finished_at_ms, Some(9));
}

#[test]
fn active_runs_count_nonterminal_phases() {
    let runs = [
        json!({"status": {"phase": "running"}}),
        json!({"status": {"phase": "admitted"}}),
        json!({"status": {"phase": "finished"}}),
        json!({"status": {}}),
    ];
    assert_eq!(active_runs(&runs), 2);
}

/// In-memory targets: each member has slots of fake runs that finish when told to.
#[derive(Default)]
pub(super) struct FakeDriver {
    pub runs: Mutex<BTreeMap<String, Vec<(String, &'static str)>>>,
    pub unreachable: Vec<String>,
    pub rejecting: Vec<String>,
    pub submissions: Mutex<Vec<(String, String)>>,
    /// Advertised discovery kinds per member; absent members fail discovery.
    pub capabilities: BTreeMap<String, Vec<String>>,
    /// Makes status reads slow, like a target that hangs.
    pub status_delay: Option<std::time::Duration>,
}

impl FakeDriver {
    fn finish_all(&self) {
        for runs in self.runs.lock().expect("fake lock").values_mut() {
            for run in runs.iter_mut() {
                run.1 = "finished";
            }
        }
    }
}

#[async_trait]
impl HubDriver for FakeDriver {
    async fn list_runs(&self, member: &str) -> Result<Vec<Value>, HubError> {
        if self.unreachable.iter().any(|name| name == member) {
            return Err(HubError::Target {
                message: "unreachable".to_owned(),
                diagnostic: None,
            });
        }
        let runs = self.runs.lock().expect("fake lock");
        Ok(runs
            .get(member)
            .map(|runs| {
                runs.iter()
                    .map(|(_, phase)| json!({"status": {"phase": phase}}))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn submit(&self, request: SubmitRequest<'_>) -> Result<String, HubError> {
        if self.rejecting.iter().any(|name| name == request.member) {
            return Err(HubError::Target {
                message: "rejected".to_owned(),
                diagnostic: None,
            });
        }
        let mut runs = self.runs.lock().expect("fake lock");
        let entry = runs.entry(request.member.to_owned()).or_default();
        let run_id = format!("{}-run-{}", request.member, entry.len());
        entry.push((run_id.clone(), "running"));
        self.submissions
            .lock()
            .expect("fake lock")
            .push((request.member.to_owned(), request.submission_key));
        Ok(run_id)
    }

    async fn status(&self, member: &str, run_id: &str) -> Result<Value, HubError> {
        if let Some(delay) = self.status_delay {
            tokio::time::sleep(delay).await;
        }
        let runs = self.runs.lock().expect("fake lock");
        let phase = runs
            .get(member)
            .and_then(|runs| runs.iter().find(|(id, _)| id == run_id))
            .map_or("running", |(_, phase)| *phase);
        Ok(if phase == "finished" {
            json!({"status": {
                "phase": "finished",
                "terminalResult": {"status": "succeeded", "output": {}},
                "metadata": {"tokenUsage": {"inputTokens": 1000, "outputTokens": 100, "complete": true}}
            }})
        } else {
            json!({"status": {"phase": "running", "activeExecutions": []}})
        })
    }

    async fn capabilities(&self, member: &str) -> Result<Vec<String>, HubError> {
        self.capabilities
            .get(member)
            .cloned()
            .ok_or_else(|| HubError::Target {
                message: format!("{member} discovery failed"),
                diagnostic: None,
            })
    }
}

#[tokio::test]
async fn tick_fills_free_slots_in_queue_order_and_keeps_the_rest_queued() {
    let driver = FakeDriver::default();
    let mut state = HubState {
        members: vec![member("a", &["linux"], 1, 0), member("b", &["linux"], 2, 0)],
        ..HubState::default()
    };
    for (index, id) in ["j1", "j2", "j3", "j4"].iter().enumerate() {
        let mut queued = job(id, index as u64);
        queued.placement = spec(None, &["linux"], Strategy::LeastLoaded);
        state.jobs.push(queued);
    }
    let report = tick(&mut state, &driver, 100).await;
    assert_eq!(
        report.dispatched.len(),
        3,
        "three slots exist across the pool"
    );
    let placed = state
        .jobs
        .iter()
        .map(|job| (job.job_id.as_str(), job.target.as_deref(), job.state))
        .collect::<Vec<_>>();
    assert_eq!(
        placed,
        [
            ("j1", Some("a"), JobState::Running),
            ("j2", Some("b"), JobState::Running),
            ("j3", Some("b"), JobState::Running),
            ("j4", None, JobState::Queued),
        ]
    );
    assert_eq!(
        driver.submissions.lock().expect("fake lock")[0],
        ("a".to_owned(), "hub-j1-1".to_owned())
    );

    driver.finish_all();
    let report = tick(&mut state, &driver, 200).await;
    assert_eq!(report.finished.len(), 3);
    assert_eq!(report.dispatched.len(), 1);
    assert_eq!(state.jobs[3].state, JobState::Running);
    assert_eq!(state.jobs[0].state, JobState::Succeeded);
    assert_eq!(
        state.jobs[0].token_usage.map(|usage| usage.input_tokens),
        Some(1000)
    );
}

#[tokio::test]
async fn tick_skips_unreachable_members_and_records_rejections() {
    let driver = FakeDriver {
        unreachable: vec!["a".to_owned()],
        rejecting: vec!["b".to_owned()],
        ..FakeDriver::default()
    };
    let mut state = HubState {
        members: vec![member("a", &[], 1, 9), member("b", &[], 1, 0)],
        jobs: vec![job("j1", 0)],
        ..HubState::default()
    };
    let report = tick(&mut state, &driver, 10).await;
    assert!(report.dispatched.is_empty());
    assert_eq!(report.failed.len(), 1);
    assert_eq!(state.jobs[0].state, JobState::Failed);
    assert_eq!(state.jobs[0].target.as_deref(), Some("b"));
    assert!(
        state.jobs[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("rejected"))
    );
}

fn finished_job(id: &str, target: &str, model: &str, usage: (u64, u64)) -> Job {
    let mut finished = job(id, 1_000);
    finished.state = JobState::Succeeded;
    finished.target = Some(target.to_owned());
    finished.models = vec![model.to_owned()];
    finished.dispatched_at_ms = Some(1_500);
    finished.finished_at_ms = Some(11_500);
    finished
        .labels
        .insert("task".to_owned(), "parser".to_owned());
    finished.token_usage = Some(TokenUsage {
        input_tokens: usage.0,
        output_tokens: usage.1,
        cache_read_input_tokens: Some(10),
        cache_creation_input_tokens: None,
        complete: true,
    });
    finished
}

#[test]
fn stats_group_by_target_and_model_with_prices() {
    let mut mixed = finished_job("j3", "b", "m1", (500_000, 0));
    mixed.models.push("m2".to_owned());
    let jobs = vec![
        finished_job("j1", "a", "m1", (1_000_000, 100_000)),
        finished_job("j2", "a", "m1", (1_000_000, 100_000)),
        mixed,
    ];
    let prices = BTreeMap::from([(
        "m1".to_owned(),
        ModelPrice {
            input: 2.0,
            output: 10.0,
            cache_read: 0.0,
            cache_creation: 0.0,
        },
    )]);
    let by = [Dimension::Target, Dimension::Model];
    let result = aggregate(
        &jobs,
        &StatsQuery {
            by: &by,
            since_ms: None,
            until_ms: None,
            prices: Some(&prices),
        },
    );
    let rows = result["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["key"], json!({"target": "a", "model": "m1"}));
    assert_eq!(rows[0]["inputTokens"], json!(2_000_000));
    assert_eq!(rows[0]["estimatedCostUsd"], json!(6.0));
    assert_eq!(rows[0]["queueWaitMs"]["p50"], json!(500));
    assert_eq!(rows[0]["runDurationMs"]["p95"], json!(10_000));
    assert_eq!(rows[1]["key"], json!({"target": "b", "model": "mixed"}));
    assert_eq!(rows[1]["unpricedJobs"], json!(1));
    assert_eq!(result["totals"]["jobs"], json!(3));
    assert_eq!(result["totals"]["cacheReadInputTokens"], json!(30));
}

#[test]
fn stats_window_and_label_dimension() {
    let mut late = finished_job("j2", "a", "m1", (1, 1));
    late.submitted_at_ms = 5_000;
    let jobs = vec![
        finished_job("j1", "a", "m1", (1, 1)),
        late,
        job("j3", 2_000),
    ];
    let by = [Dimension::parse("label:task").expect("label dimension")];
    let result = aggregate(
        &jobs,
        &StatsQuery {
            by: &by,
            since_ms: Some(1_000),
            until_ms: Some(5_000),
            prices: None,
        },
    );
    let rows = result["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2, "the queued job has no task label");
    assert_eq!(result["totals"]["jobs"], json!(2));
    assert_eq!(result["totals"]["queued"], json!(1));
    assert!(result["totals"].get("estimatedCostUsd").is_none());
    assert!(Dimension::parse("label:").is_err());
    assert!(Dimension::parse("region").is_err());
}

#[test]
fn percentiles_use_nearest_rank() {
    assert_eq!(
        percentiles(&[]),
        json!({"p50": null, "p95": null, "samples": 0})
    );
    let samples = (1..=20).collect::<Vec<u64>>();
    assert_eq!(
        percentiles(&samples),
        json!({"p50": 10, "p95": 19, "samples": 20})
    );
    assert_eq!(percentiles(&[7]), json!({"p50": 7, "p95": 7, "samples": 1}));
}

#[test]
fn durations_and_instants_parse() {
    assert_eq!(parse_duration_ms("90s").expect("seconds"), 90_000);
    assert_eq!(parse_duration_ms("1h30m").expect("combined"), 5_400_000);
    assert_eq!(parse_duration_ms("2w").expect("weeks"), 1_209_600_000);
    for invalid in ["", "10", "5x", "m", "0s"] {
        assert!(
            parse_duration_ms(invalid).is_err(),
            "{invalid:?} must be rejected"
        );
    }
    assert_eq!(parse_future_ms("+2h", 1_000).expect("relative"), 7_201_000);
    assert_eq!(parse_past_ms("1s", 5_000).expect("ago"), 4_000);
    let instant = parse_future_ms("2026-10-20T03:00:00Z", 0).expect("rfc3339");
    assert_eq!(rfc3339(instant), "2026-10-20T03:00:00Z");
    assert_eq!(utc_day(instant), "2026-10-20");
}

#[test]
fn job_attribution_helpers() {
    assert_eq!(
        repository_name("git@github.com:naqerl/zeroshot.git"),
        "naqerl/zeroshot"
    );
    assert_eq!(
        repository_name("https://github.com/naqerl/zeroshot"),
        "naqerl/zeroshot"
    );
    assert_eq!(
        repository_name("ssh://git@host:22/team/app.git"),
        "team/app"
    );
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("runtime.json");
    std::fs::write(
        &path,
        r#"{"harness":"codex","nodes":{"worker":{"model":"m1"},"reviewer":{"model":"m2"}}}"#,
    )
    .expect("runtime file");
    let mut models = runtime_models(&path);
    models.sort();
    assert_eq!(models, ["m1", "m2"]);
    assert!(runtime_models(&directory.path().join("missing.json")).is_empty());
    let mut attributed = job("j", 0);
    assert_eq!(attributed.model_key(), "unknown");
    attributed.models = vec!["m1".to_owned()];
    assert_eq!(attributed.model_key(), "m1");
}

#[test]
fn placement_errors_have_distinct_exit_codes_and_json_codes() {
    let no_capacity = HubError::NoCapacity("any member".to_owned());
    let timeout = HubError::WaitTimeout("any member".to_owned());
    let ineligible = HubError::no_eligible("tags [gpu]".to_owned(), Vec::new());
    assert_eq!(no_capacity.exit_code(), EXIT_NO_CAPACITY);
    assert_eq!(timeout.exit_code(), EXIT_NO_CAPACITY);
    assert_eq!(ineligible.exit_code(), EXIT_NO_ELIGIBLE_TARGET);
    assert_eq!(HubError::JobNotFound("j".to_owned()).exit_code(), 1);
    let diagnostic = ineligible.diagnostic();
    assert_eq!(diagnostic["schema"], json!("zeroshot.error/v1"));
    assert_eq!(diagnostic["code"], json!("hub.no_eligible_target"));
    assert_eq!(timeout.diagnostic()["code"], json!("hub.wait_timeout"));
}

#[test]
fn store_round_trips_state_under_lock() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = super::store::HubStore::new(directory.path().join("hub"));
    let lock = store.lock().expect("lock");
    assert_eq!(store.load(&lock).expect("empty state"), HubState::default());
    let state = HubState {
        members: vec![member("local", &["laptop"], 2, 0)],
        jobs: vec![job("j1", 1)],
        ..HubState::default()
    };
    store.save(&lock, &state).expect("save");
    assert_eq!(store.load(&lock).expect("reload"), state);
}
