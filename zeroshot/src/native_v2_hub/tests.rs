use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{Value, json};

use super::clock::{parse_duration_ms, parse_future_ms, parse_past_ms, rfc3339, utc_day};
use super::driver::{HubDriver, SubmitRequest};
use super::error::{EXIT_NO_CAPACITY, EXIT_NO_ELIGIBLE_TARGET, HubError};
use super::jobs::{repository_name, runtime_models};
use super::model::{
    HubState, Job, JobActivity, JobError, JobErrorKind, JobState, Member, MemberState,
    PlacementSpec, Requirements, Strategy,
};
use super::pool::{Lease, LeasePhase};
use super::usage::{JobUsage, Tokens, reported};
use super::placement::{Load, Placement, Reachability, describe, place};
use super::schedule::{UNREACHABLE_REAP_THRESHOLD, active_runs, apply_status, due_jobs, tick};
use super::stats::{Dimension, ModelPrice, StatsQuery, aggregate, percentiles};
use crate::native_v2_target::{FileTargetRegistry, TargetAccess, TargetRecord, TargetRegistry};

#[path = "tests/api.rs"]
mod api;
#[path = "tests/capabilities.rs"]
mod capabilities;
#[path = "tests/cron.rs"]
mod cron;
#[path = "tests/daemon.rs"]
mod daemon;
#[path = "tests/driver.rs"]
mod driver;
#[path = "tests/eval.rs"]
mod eval;
#[path = "tests/eval_flows.rs"]
mod eval_flows;
#[path = "tests/flows.rs"]
mod flows;
#[path = "tests/mcp.rs"]
mod mcp;
#[path = "tests/metrics.rs"]
mod metrics;
#[path = "tests/paths.rs"]
mod paths;
#[path = "tests/usage.rs"]
mod usage;

#[path = "tests/pool.rs"]
mod pool;

#[path = "tests/tunnel.rs"]
mod tunnel;

#[path = "tests/pool_capacity.rs"]
mod pool_capacity;

#[path = "tests/pool_profiles.rs"]
mod pool_profiles;

#[path = "tests/pool_data.rs"]
mod pool_data;
#[path = "tests/pool_health.rs"]
mod pool_health;

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
        max_stall_ms: None,
        submitted_at_ms,
        not_before_ms: None,
        attempts: 0,
        unconfirmed_submissions: 0,
        target: None,
        run_id: None,
        dispatched_at_ms: None,
        last_cursor: None,
        last_cursor_at_ms: None,
        last_activity_at_ms: None,
        last_activity_reported_at_ms: None,
        last_activity: None,
        target_last_ok_at_ms: None,
        reaper: None,
        finished_at_ms: None,
        failure_reason: None,
        failure_classification: None,
        token_usage: None,
        error: None,
        schedule_id: None,
        output: None,
        archive: None,
        archive_skipped: None,
        graph_hash: None,
        pool_wait: None,
        quota: None,
        quota_attempts: 0,
    }
}

fn lease_on(member: &str) -> Lease {
    Lease {
        id: format!("lease-{member}"),
        name: member.to_owned(),
        pool: "burst".to_owned(),
        phase: LeasePhase::Active,
        local_port: 0,
        transport: super::pool::Transport::Ssh,
        transport_logged: false,
        hold: false,
        quota_hold: false,
        created_at_ms: 1,
        progress_at_ms: 1,
        drain_started_ms: None,
        server_id: None,
        firewall_id: None,
        ipv4: None,
        action_id: None,
        reason: None,
        attempts: 0,
        next_attempt_ms: None,
        error: None,
        retirement_reason: None,
        spec: Default::default(),
        machine: None,
        event_phase: None,
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
                    reachability: if *reachable {
                        super::placement::Reachability::Reachable
                    } else {
                        super::placement::Reachability::Unreachable
                    },
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

#[tokio::test]
async fn a_stalled_run_is_stopped_and_failed_with_its_cursor_and_still_time() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver.clone(),
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service")
    .with_max_stall(Some(100));
    let mut running = job("hj_stalled", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-stalled".to_owned());
    running.last_cursor = Some("run-stalled-cursor".to_owned());
    running.last_cursor_at_ms = Some(0);
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");
    let (report, view) = service.pass().await.expect("pass");
    assert_eq!(report.stalled.len(), 1);
    assert_eq!(report.stalled[0]["runId"], json!("run-stalled"));
    assert_eq!(report.stalled[0]["cursor"], json!("run-stalled-cursor"));
    assert_eq!(view["stalled"], json!(report.stalled));
    let settled = service.snapshot().await.jobs.remove(0);
    assert_eq!(settled.state, JobState::Failed);
    assert_eq!(settled.failure_reason.as_deref(), Some("stalled"));
    assert_eq!(
        settled.failure_classification.as_deref(),
        Some(super::schedule::CLASSIFIED_STALLED),
        "a hub-initiated kill names its own classification"
    );
    let error = settled.error.expect("stall error");
    assert_eq!(error.kind, JobErrorKind::Stalled);
    assert!(
        error.message.contains("run-stalled-cursor"),
        "{}",
        error.message
    );
    assert!(error.message.contains("ms"), "{}", error.message);
    assert!(
        error.message.contains("no cursor movement for "),
        "the measured silence is in the diagnostic: {}",
        error.message
    );
    assert!(
        error.message.contains("limit 100ms from hub default"),
        "the hub limit and its source are in the diagnostic: {}",
        error.message
    );
    assert_eq!(report.stalled[0]["maxStallMs"], json!(100));
    assert_eq!(report.stalled[0]["maxStallSource"], json!("hub"));
    assert_eq!(
        driver.stops.lock().expect("fake lock").as_slice(),
        [("box".to_owned(), "run-stalled".to_owned())]
    );
}

#[test]
fn a_fresh_activity_heartbeat_keeps_a_long_tool_call_alive() {
    let mut running = job("hj_active", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-active".to_owned());
    running.last_cursor = Some("v2:5".to_owned());
    running.last_cursor_at_ms = Some(0);
    running.last_activity_at_ms = Some(90);
    running.last_activity = Some(JobActivity {
        node: "worker".to_owned(),
        command: "cargo check --workspace --all-targets".to_owned(),
        elapsed_ms: 90_000,
    });
    let limit = |job: &Job| super::schedule::StallLimit::for_job(job, Some(100));
    assert!(
        super::schedule::stalled_runs(std::slice::from_ref(&running), 150, limit).is_empty(),
        "a still-running tool is activity even while the cursor is unchanged"
    );
}

#[test]
fn an_older_activity_heartbeat_still_stalls_and_names_the_last_command() {
    let mut running = job("hj_stale_activity", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-stale-activity".to_owned());
    running.last_cursor = Some("v2:5".to_owned());
    running.last_cursor_at_ms = Some(0);
    running.last_activity_at_ms = Some(20);
    running.last_activity = Some(JobActivity {
        node: "worker".to_owned(),
        command: "cargo check --workspace --all-targets".to_owned(),
        elapsed_ms: 90_000,
    });
    let limit = |job: &Job| super::schedule::StallLimit::for_job(job, Some(100));
    let stalled = super::schedule::stalled_runs(std::slice::from_ref(&running), 150, limit);
    assert_eq!(stalled.len(), 1);
    assert_eq!(
        stalled[0].still_ms, 130,
        "the newest activity sets the clock"
    );
    assert_eq!(stalled[0].activity, running.last_activity);
    let mut jobs = [job("hj_stale_activity", 0)];
    jobs[0] = running;
    let failed = super::schedule::settle_stalled(
        &mut jobs,
        super::schedule::StallSettlement {
            now_ms: 150,
            stalled: &stalled,
            stop_errors: &BTreeMap::new(),
        },
    );
    assert_eq!(failed.len(), 1);
    let error = jobs[0].error.as_ref().expect("stall error");
    assert_eq!(error.kind, JobErrorKind::Stalled);
    assert!(
        error.message.contains("no activity for 130ms"),
        "{}",
        error.message
    );
    assert!(
        error.message.contains(
            "last activity node 'worker' ran 'cargo check --workspace --all-targets' for 90s"
        ),
        "{}",
        error.message
    );
}

#[test]
fn two_activity_heartbeats_five_seconds_apart_keep_only_the_newest_record() {
    let mut running = job("hj_coalesced", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-coalesced".to_owned());
    running.last_cursor = Some("v2:5".to_owned());
    running.last_cursor_at_ms = Some(0);
    let status = |timestamp: u64, elapsed_ms: u64| {
        json!({
            "atCursor": "v2:5",
            "status": {
                "phase": "running",
                "activity": {
                    "node": "worker",
                    "command": "cargo check --workspace --all-targets",
                    "elapsedMs": elapsed_ms,
                    "timestamp": timestamp,
                },
            },
        })
    };
    super::schedule::record_cursor(&mut running, &status(1000, 5000), 10_000);
    super::schedule::record_cursor(&mut running, &status(6000, 10_000), 15_000);
    assert_eq!(running.last_activity_at_ms, Some(15_000));
    assert_eq!(running.last_activity_reported_at_ms, Some(6000));
    assert_eq!(
        running
            .last_activity
            .as_ref()
            .map(|activity| activity.elapsed_ms),
        Some(10_000),
        "the newest heartbeat replaces the earlier one; there is no event list to grow"
    );
}

#[test]
fn a_job_stall_limit_overrides_the_hub_limit() {
    let mut running = job("hj_override", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-override".to_owned());
    running.last_cursor = Some("run-override-cursor".to_owned());
    running.last_cursor_at_ms = Some(0);
    running.max_stall_ms = Some(200);
    let limit = |job: &Job| super::schedule::StallLimit::for_job(job, Some(100));
    assert!(
        super::schedule::stalled_runs(std::slice::from_ref(&running), 150, limit).is_empty(),
        "the job's larger limit wins over the hub's"
    );
    let stalled = super::schedule::stalled_runs(std::slice::from_ref(&running), 250, limit);
    assert_eq!(stalled.len(), 1);
    assert_eq!(stalled[0].max_stall_ms, 200);
    assert_eq!(
        stalled[0].limit_source,
        super::schedule::StallLimitSource::Job
    );
}

#[test]
fn a_job_stall_limit_works_without_a_hub_limit() {
    let mut running = job("hj_own", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-own".to_owned());
    running.last_cursor = Some("run-own-cursor".to_owned());
    running.last_cursor_at_ms = Some(0);
    running.max_stall_ms = Some(50);
    let stalled = super::schedule::stalled_runs(&[running], 60, |job| {
        super::schedule::StallLimit::for_job(job, None)
    });
    assert_eq!(stalled.len(), 1, "a job limit needs no hub-wide limit");
    assert_eq!(stalled[0].max_stall_ms, 50);
    assert_eq!(
        stalled[0].limit_source,
        super::schedule::StallLimitSource::Job
    );
}

#[test]
fn a_stalled_run_without_a_cursor_falls_back_to_its_dispatch_time() {
    let mut running = job("hj_silent", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-silent".to_owned());
    running.dispatched_at_ms = Some(10);
    let stalled = super::schedule::stalled_runs(&[running], 100, |job| {
        super::schedule::StallLimit::for_job(job, Some(50))
    });
    assert_eq!(
        stalled.len(),
        1,
        "a status without a cursor must not disable the limit"
    );
    assert_eq!(stalled[0].job_id, "hj_silent");
    assert_eq!(stalled[0].cursor, "");
    assert_eq!(stalled[0].still_ms, 90);
}

#[tokio::test]
async fn a_job_stall_limit_keeps_a_run_past_the_hub_limit_until_its_own() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver.clone(),
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service")
    .with_max_stall(Some(100));
    let job_limit = 10_000;
    let now = super::clock::now_ms();
    let mut running = job("hj_override", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-override".to_owned());
    running.last_cursor = Some("run-override-cursor".to_owned());
    running.last_cursor_at_ms = Some(now.saturating_sub(5_000));
    running.max_stall_ms = Some(job_limit);
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");

    let status = service.status("hj_override").await.expect("status");
    assert_eq!(status["job"]["jobMaxStallMs"], json!(job_limit));
    assert_eq!(status["job"]["maxStallMs"], json!(job_limit));
    assert_eq!(status["job"]["maxStallSource"], json!("job"));

    let (report, _) = service.pass().await.expect("pass");
    assert!(
        report.stalled.is_empty(),
        "the job may use its larger budget past the hub limit"
    );
    let observed = service.snapshot().await.jobs.remove(0);
    assert_eq!(observed.state, JobState::Running);
    assert!(driver.stops.lock().expect("fake lock").is_empty());

    let now = super::clock::now_ms();
    service
        .mutate(|state| {
            state.jobs[0].last_cursor_at_ms = Some(now.saturating_sub(job_limit + 2_000));
            Ok(())
        })
        .await
        .expect("age cursor");
    let (report, _) = service.pass().await.expect("pass at own limit");
    assert_eq!(report.stalled.len(), 1);
    assert_eq!(report.stalled[0]["maxStallSource"], json!("job"));
    let settled = service.snapshot().await.jobs.remove(0);
    assert_eq!(settled.state, JobState::Failed);
    let error = settled.error.expect("stall error");
    assert!(
        error
            .message
            .contains(&format!("limit {job_limit}ms from job")),
        "the job limit and its source are in the diagnostic: {}",
        error.message
    );
    assert!(
        error.message.contains("no cursor movement for "),
        "the measured silence is in the diagnostic: {}",
        error.message
    );
}

#[tokio::test]
async fn a_stalled_run_whose_stop_fails_stays_running_until_a_later_stop_succeeds() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver {
        fail_stops: std::sync::atomic::AtomicBool::new(true),
        ..FakeDriver::default()
    });
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver.clone(),
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service")
    .with_max_stall(Some(100));
    let mut running = job("hj_stalled", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-stalled".to_owned());
    running.last_cursor = Some("run-stalled-cursor".to_owned());
    running.last_cursor_at_ms = Some(0);
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");
    let (report, _) = service.pass().await.expect("pass");
    assert!(
        report.stalled.is_empty(),
        "an unstopped run cannot stall-fail"
    );
    let observed = service.snapshot().await.jobs.remove(0);
    assert_eq!(observed.state, JobState::Running);
    assert_eq!(observed.failure_reason, None);
    let error = observed.error.expect("stop failure");
    assert_eq!(error.kind, JobErrorKind::Stalled);
    assert!(
        error.message.contains("stopping it failed"),
        "{}",
        error.message
    );
    assert!(error.message.contains("will retry"), "{}", error.message);
    assert_eq!(driver.stops.lock().expect("fake lock").len(), 1);

    driver
        .fail_stops
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let (report, _) = service.pass().await.expect("retry pass");
    assert_eq!(report.stalled.len(), 1);
    let settled = service.snapshot().await.jobs.remove(0);
    assert_eq!(settled.state, JobState::Failed);
    assert_eq!(settled.failure_reason.as_deref(), Some("stalled"));
    assert_eq!(driver.stops.lock().expect("fake lock").len(), 2);
}

#[tokio::test]
async fn an_advancing_cursor_resets_the_stall_clock_and_without_max_stall_nothing_stops() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    let store = super::store::HubStore::new(directory.path().join("hub"));
    let registry =
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json"));
    let service = super::service::HubService::open(store.clone(), driver.clone(), registry.clone())
        .expect("service")
        .with_max_stall(Some(100));
    let mut running = job("hj_cursor", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-cursor".to_owned());
    running.last_cursor = Some("old-cursor".to_owned());
    running.last_cursor_at_ms = Some(0);
    driver
        .cursors
        .lock()
        .expect("fake lock")
        .insert("run-cursor".to_owned(), "new-cursor".to_owned());
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");
    let (report, _) = service.pass().await.expect("pass");
    assert!(report.stalled.is_empty(), "a fresh cursor cannot stall");
    let observed = service.snapshot().await.jobs.remove(0);
    assert_eq!(observed.state, JobState::Running);
    assert_eq!(observed.last_cursor.as_deref(), Some("new-cursor"));
    assert!(observed.last_cursor_at_ms.is_some_and(|at| at > 0));
    assert!(driver.stops.lock().expect("fake lock").is_empty());

    let disabled = super::service::HubService::open(store, driver.clone(), registry)
        .expect("service")
        .with_max_stall(None);
    let mut running = observed;
    running.last_cursor = Some("new-cursor".to_owned());
    running.last_cursor_at_ms = Some(0);
    disabled
        .mutate(|state| {
            state.jobs = vec![running];
            Ok(())
        })
        .await
        .expect("seed");
    let (report, _) = disabled.pass().await.expect("pass");
    assert!(report.stalled.is_empty());
    assert_eq!(disabled.snapshot().await.jobs[0].state, JobState::Running);
    assert!(driver.stops.lock().expect("fake lock").is_empty());
}

#[tokio::test]
async fn a_placed_job_whose_member_disappears_is_reaped_with_target_lost() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver,
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    let mut running = job("hj_lost", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-lost".to_owned());
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");
    let (report, _) = service.pass().await.expect("healthy pass");
    assert!(report.target_lost.is_empty());
    let observed = service.snapshot().await;
    assert!(
        observed
            .member("box")
            .expect("member")
            .last_probe_ok_at_ms
            .is_some()
    );
    assert!(observed.jobs[0].target_last_ok_at_ms.is_some());

    service
        .mutate(|state| {
            state.members.clear();
            Ok(())
        })
        .await
        .expect("remove member");
    let (report, view) = service.pass().await.expect("lost pass");
    assert_eq!(report.target_lost.len(), 1);
    assert_eq!(view["targetLost"], json!(report.target_lost));
    let settled = service.snapshot().await.jobs.remove(0);
    assert_eq!(settled.state, JobState::Failed);
    assert_eq!(settled.failure_reason.as_deref(), Some("target_lost"));
    let error = settled.error.expect("target-lost error");
    assert_eq!(error.kind, JobErrorKind::TargetLost);
    assert!(error.message.contains("box"), "{}", error.message);
    assert!(
        error.message.contains("no longer a hub member"),
        "{}",
        error.message
    );
    assert!(
        error.message.contains("last successful probe"),
        "{}",
        error.message
    );
}

#[tokio::test]
async fn a_missing_members_job_is_reaped_while_a_placed_job_without_a_deadline_runs() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver {
        unreachable: vec![
            "orphan".to_owned(),
            "unfunded".to_owned(),
            "zs-unfunded".to_owned(),
        ],
        ..FakeDriver::default()
    });
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver,
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    let mut lost = job("hj_lost", 0);
    lost.state = JobState::Running;
    lost.target = Some("gone".to_owned());
    lost.run_id = Some("run-lost".to_owned());
    let mut placed = job("hj_placed", 0);
    placed.state = JobState::Running;
    placed.target = Some("box".to_owned());
    placed.run_id = Some("run-placed".to_owned());
    let mut orphan = job("hj_orphan", 0);
    orphan.state = JobState::Running;
    orphan.target = Some("orphan".to_owned());
    orphan.run_id = Some("run-orphan".to_owned());
    let mut unfunded = job("hj_unfunded", 0);
    unfunded.state = JobState::Running;
    unfunded.target = Some("unfunded".to_owned());
    unfunded.run_id = Some("run-unfunded".to_owned());
    let pool = pool::pool_fixture();
    let lease = Lease {
        phase: LeasePhase::Active,
        ..Lease::new(&pool, "hj_unfunded", 0, 0)
    };
    unfunded.target = Some(lease.name.clone());
    let mut orphan_member = member("orphan", &[], 1, 0);
    orphan_member.pool = Some(pool.name.clone());
    orphan_member.unreachable_probes = UNREACHABLE_REAP_THRESHOLD;
    orphan_member.last_probe_ok_at_ms = Some(1);
    let mut unfunded_member = member(&lease.name, &[], 1, 0);
    unfunded_member.pool = Some(pool.name.clone());
    unfunded_member.unreachable_probes = UNREACHABLE_REAP_THRESHOLD;
    unfunded_member.last_probe_ok_at_ms = Some(1);
    service
        .mutate(|state| {
            state.members.push(member("gone", &[], 1, 0));
            state.members.push(member("box", &[], 1, 0));
            state.members.push(orphan_member);
            state.members.push(unfunded_member);
            state.jobs.push(lost);
            state.jobs.push(placed);
            state.jobs.push(orphan);
            state.jobs.push(unfunded);
            state.leases.push(lease);
            Ok(())
        })
        .await
        .expect("seed");
    let (report, _) = service.pass().await.expect("healthy pass");
    assert!(report.target_lost.is_empty());

    service
        .mutate(|state| {
            state.members.retain(|member| member.name != "gone");
            Ok(())
        })
        .await
        .expect("remove member");
    let (report, view) = service.pass().await.expect("reap pass");
    assert_eq!(report.target_lost.len(), 1);
    assert_eq!(view["targetLost"], json!(report.target_lost));

    for _ in 0..UNREACHABLE_REAP_THRESHOLD + 1 {
        let (report, _) = service.pass().await.expect("later pass");
        assert!(
            report.target_lost.is_empty(),
            "a failed job is not reaped again"
        );
    }
    let observed = service.snapshot().await;
    let lost = observed
        .jobs
        .iter()
        .find(|job| job.job_id == "hj_lost")
        .expect("lost job");
    assert_eq!(lost.state, JobState::Failed);
    assert_eq!(lost.failure_reason.as_deref(), Some("target_lost"));
    assert_eq!(
        lost.error.as_ref().map(|error| error.kind),
        Some(JobErrorKind::TargetLost)
    );
    let placed = observed
        .jobs
        .iter()
        .find(|job| job.job_id == "hj_placed")
        .expect("placed job");
    assert_eq!(placed.state, JobState::Running);
    assert!(placed.error.is_none());
    assert!(placed.finished_at_ms.is_none());
    assert!(placed.target_last_ok_at_ms.is_some());
    for (job_id, evidence) in [
        ("hj_orphan", "no pool lease record"),
        ("hj_unfunded", "without a provider token"),
    ] {
        let job = observed
            .jobs
            .iter()
            .find(|job| job.job_id == job_id)
            .expect("withheld pool job");
        assert_eq!(job.state, JobState::Running, "{job:?}");
        assert!(
            job.reaper
                .as_deref()
                .is_some_and(|reason| reason.contains(evidence)),
            "{job:?}"
        );
    }
}

#[tokio::test]
async fn a_status_success_clears_a_stale_probe_streak() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver {
        unreachable: vec!["box".to_owned()],
        status_ok: vec!["box".to_owned()],
        ..FakeDriver::default()
    });
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver,
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    let mut running = job("hj_observed", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-observed".to_owned());
    let mut boxed = member("box", &[], 1, 0);
    boxed.last_probe_ok_at_ms = Some(1);
    boxed.unreachable_probes = UNREACHABLE_REAP_THRESHOLD - 1;
    service
        .mutate(|state| {
            state.members.push(boxed);
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");
    // The status read is direct proof the member still serves this run, so the `list` failure in
    // the same pass starts a fresh streak instead of completing the stale one.
    let (report, _) = service.pass().await.expect("observed pass");
    assert!(report.target_lost.is_empty(), "{report:?}");
    let state = service.snapshot().await;
    assert_eq!(state.member("box").expect("member").unreachable_probes, 1);
    assert_eq!(state.jobs[0].state, JobState::Running);
    assert!(state.jobs[0].reaper.is_none());
}

#[tokio::test]
async fn a_member_unreachable_once_keeps_its_job_until_the_reap_threshold() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver {
        unreachable: vec!["box".to_owned()],
        inconclusive: vec!["slow".to_owned()],
        ..FakeDriver::default()
    });
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("closed port");
    let port = listener.local_addr().expect("address").port();
    drop(listener);
    // Prefer IPv6 so the endpoint check must handle a bracketed URL host, then a live IPv6 or
    // IPv4 listener proves the reaper keeps a starved job whose endpoint still accepts.
    let live = std::net::TcpListener::bind("[::1]:0")
        .or_else(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .expect("live listener");
    let live_address = live.local_addr().expect("live address");
    let live_origin = if live_address.is_ipv6() {
        format!("http://[{}]:{}", live_address.ip(), live_address.port())
    } else {
        format!("http://{}:{}", live_address.ip(), live_address.port())
    };
    let registry = FileTargetRegistry::new(directory.path().join("targets.json"));
    registry
        .insert(TargetRecord {
            id: "00000000-0000-4000-8000-0000000000b0".to_owned(),
            name: "box".to_owned(),
            origin: format!("http://127.0.0.1:{port}"),
            access: TargetAccess::Direct,
        })
        .expect("register box");
    registry
        .insert(TargetRecord {
            id: "00000000-0000-4000-8000-0000000000b1".to_owned(),
            name: "slow".to_owned(),
            origin: live_origin,
            access: TargetAccess::Direct,
        })
        .expect("register slow");
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver,
        registry,
    )
    .expect("service");
    let mut running = job("hj_flapping", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-flapping".to_owned());
    let mut starved = job("hj_starved", 0);
    starved.state = JobState::Running;
    starved.target = Some("slow".to_owned());
    starved.run_id = Some("run-starved".to_owned());
    let mut boxed = member("box", &[], 1, 0);
    boxed.last_probe_ok_at_ms = Some(1);
    let mut slow = member("slow", &[], 1, 0);
    slow.last_probe_ok_at_ms = Some(1);
    slow.unreachable_probes = UNREACHABLE_REAP_THRESHOLD;
    service
        .mutate(|state| {
            state.members.push(boxed);
            state.members.push(slow);
            state.jobs.push(running);
            state.jobs.push(starved);
            Ok(())
        })
        .await
        .expect("seed");
    for _ in 0..UNREACHABLE_REAP_THRESHOLD - 1 {
        let (report, _) = service.pass().await.expect("flapping pass");
        assert!(report.target_lost.is_empty());
        assert_eq!(service.snapshot().await.jobs[0].state, JobState::Running);
    }
    let (report, _) = service.pass().await.expect("threshold pass");
    assert_eq!(report.target_lost.len(), 1);
    let settled = service.snapshot().await.jobs.remove(0);
    assert_eq!(settled.state, JobState::Failed);
    let error = settled.error.expect("target-lost error");
    assert_eq!(error.kind, JobErrorKind::TargetLost);
    assert!(
        error
            .message
            .contains(&format!("{UNREACHABLE_REAP_THRESHOLD} consecutive probes")),
        "{}",
        error.message
    );
    assert!(
        error.message.contains("last successful probe"),
        "{}",
        error.message
    );
    let observed = service.snapshot().await;
    let starved = observed
        .jobs
        .iter()
        .find(|job| job.job_id == "hj_starved")
        .expect("starved job");
    assert_eq!(starved.state, JobState::Running);
    let note = starved.reaper.as_deref().expect("reaper note");
    assert!(note.contains("probe 3x failed"), "{note}");
    assert!(note.contains("inconclusive"), "{note}");
    assert!(note.contains("still accepts TCP connections"), "{note}");
    assert!(
        note.contains("job kept running"),
        "a starved probe never reaps a healthy job: {note}"
    );
    if live_address.is_ipv6() {
        assert!(
            note.contains("[::1]"),
            "an IPv6 target endpoint must be checked without a host lookup: {note}"
        );
    }
}

#[test]
fn rejected_credentials_and_timeouts_are_not_proof_that_a_member_is_gone() {
    for message in [
        "zeroshot list: 401 Unauthorized",
        "authentication failed for target",
        "request timed out",
        "permission denied",
        "too many requests: 429",
    ] {
        let error = HubError::Target {
            message: message.to_owned(),
            diagnostic: None,
        };
        assert_eq!(
            super::schedule::classify_probe_error(&error),
            Reachability::Inconclusive,
            "{message} describes our access, not the member's presence"
        );
    }
    let refused = HubError::Target {
        message: "unreachable: connection refused".to_owned(),
        diagnostic: None,
    };
    assert_eq!(
        super::schedule::classify_probe_error(&refused),
        Reachability::Unreachable
    );
}

/// A timeout, a rate limit or an empty answer is not proof that a member is gone: the streak
/// stays where it was, and only a decisive negative extends it. The job failed by a decisive
/// observation records which observation it was.
#[test]
fn only_decisive_probes_extend_the_unreachable_streak() {
    let mut state = HubState {
        members: vec![member("box", &[], 1, 0)],
        jobs: vec![job("hj_probed", 0)],
        ..HubState::default()
    };
    state.jobs[0].state = JobState::Running;
    state.jobs[0].target = Some("box".to_owned());
    state.jobs[0].run_id = Some("run-probed".to_owned());
    let probe = |reachability| {
        BTreeMap::from([(
            "box".to_owned(),
            Load {
                reachability,
                active: 1,
                error: Some("probe failed".to_owned()),
            },
        )])
    };

    let mut at = 1;
    for _ in 0..UNREACHABLE_REAP_THRESHOLD as u64 + 2 {
        super::schedule::record_health(
            &mut state.members,
            &mut state.jobs,
            &probe(Reachability::Inconclusive),
            at,
        );
        at += 1;
    }
    let boxed = state.member("box").expect("member");
    assert_eq!(
        boxed.unreachable_probes, 0,
        "an inconclusive probe never extends the streak"
    );
    assert_eq!(
        boxed.last_probe_ok_at_ms, None,
        "an inconclusive probe is not a success either"
    );
    assert!(
        super::schedule::lost_targets(&state).is_empty(),
        "a member with no decisive failure keeps its job running"
    );

    for _ in 0..UNREACHABLE_REAP_THRESHOLD as u64 + 1 {
        super::schedule::record_health(
            &mut state.members,
            &mut state.jobs,
            &probe(Reachability::Unreachable),
            at,
        );
        at += 1;
    }
    let boxed = state.member("box").expect("member");
    assert_eq!(boxed.unreachable_probes, UNREACHABLE_REAP_THRESHOLD + 1);
    let lost = super::schedule::lost_targets(&state);
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert_eq!(
        lost[0].classification, None,
        "the streak alone is not the proof"
    );
    let (confirmed, _) =
        super::schedule::confirm_lost_by_probe(lost, &probe(Reachability::Unreachable));
    assert_eq!(confirmed.len(), 1, "{confirmed:?}");
    assert_eq!(
        confirmed[0].classification,
        Some("probe_unreachable"),
        "the deciding probe classifies the loss"
    );
    let failed = super::schedule::settle_lost_targets(&mut state.jobs, at, &confirmed);
    assert_eq!(failed[0]["classification"], json!("probe_unreachable"));
    let job = &state.jobs[0];
    assert_eq!(job.state, JobState::Failed);
    assert_eq!(job.failure_reason.as_deref(), Some("target_lost"));
    assert_eq!(
        job.failure_classification.as_deref(),
        Some("probe_unreachable"),
        "the job record names the observation that failed it"
    );
    let message = job.error.as_ref().expect("error").message.clone();
    assert!(message.contains("(probe_unreachable)"), "{message}");
    assert!(
        message.contains("could not connect to the member"),
        "{message}"
    );
}

#[tokio::test]
async fn a_reachable_drain_member_keeps_its_placed_job_running() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver,
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    let mut running = job("hj_draining", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-draining".to_owned());
    let mut boxed = member("box", &[], 1, 0);
    boxed.state = MemberState::Drain;
    service
        .mutate(|state| {
            state.members.push(boxed);
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");
    for _ in 0..UNREACHABLE_REAP_THRESHOLD + 2 {
        let (report, _) = service.pass().await.expect("draining pass");
        assert!(report.target_lost.is_empty());
    }
    let observed = service.snapshot().await;
    assert_eq!(observed.jobs[0].state, JobState::Running);
    assert_eq!(
        observed.member("box").expect("member").unreachable_probes,
        0
    );
    assert!(
        observed
            .member("box")
            .expect("member")
            .last_probe_ok_at_ms
            .is_some()
    );
}

#[test]
fn a_stalled_run_without_a_cursor_uses_its_last_successful_contact() {
    let mut running = job("hj_contacted", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-contacted".to_owned());
    running.dispatched_at_ms = Some(10);
    running.target_last_ok_at_ms = Some(90);
    assert!(
        super::schedule::stalled_runs(&[running.clone()], 120, |job| {
            super::schedule::StallLimit::for_job(job, Some(50))
        })
        .is_empty(),
        "recent contact is not inactivity"
    );
    assert_eq!(
        super::schedule::stalled_runs(&[running], 150, |job| {
            super::schedule::StallLimit::for_job(job, Some(50))
        })[0]
            .still_ms,
        60
    );
}

#[test]
fn a_cursorless_successful_status_read_resets_the_stall_clock() {
    let mut running = job("hj_cursorless", 0);
    running.state = JobState::Running;
    running.last_cursor_at_ms = Some(10);
    super::schedule::record_cursor(&mut running, &json!({"status": "running"}), 40);
    assert_eq!(running.last_cursor_at_ms, Some(40));
    assert!(running.last_cursor.is_none());
}

#[test]
fn cursor_recording_keeps_time_until_the_cursor_advances() {
    let mut running = job("j1", 0);
    running.state = JobState::Running;
    let status = json!({"atCursor": "c1"});
    super::schedule::record_cursor(&mut running, &status, 10);
    assert_eq!(running.last_cursor.as_deref(), Some("c1"));
    assert_eq!(running.last_cursor_at_ms, Some(10));
    super::schedule::record_cursor(&mut running, &status, 20);
    assert_eq!(running.last_cursor_at_ms, Some(10));
    super::schedule::record_cursor(&mut running, &json!({"atCursor": "c2"}), 30);
    assert_eq!(running.last_cursor.as_deref(), Some("c2"));
    assert_eq!(running.last_cursor_at_ms, Some(30));
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
    assert_eq!(running.token_usage, Some(reported(10, 4, None)));
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
    /// Members whose `list` probe fails but whose `status` reads still answer, like a target
    /// with a broken list route.
    pub status_ok: Vec<String>,
    /// Members whose status probe times out without proving the target is gone.
    pub inconclusive: Vec<String>,
    pub rejecting: Vec<String>,
    pub submissions: Mutex<Vec<(String, String)>>,
    /// Advertised discovery kinds per member; absent members fail discovery.
    pub capabilities: BTreeMap<String, Vec<String>>,
    /// Makes status reads slow, like a target that hangs.
    pub status_delay: Option<std::time::Duration>,
    /// Terminal output of finished runs (default `{}`).
    pub output: Option<Value>,
    /// Submissions still to accept without answering, like a `zeroshot run` that times out
    /// after the target took the run.
    pub unanswered: Mutex<u32>,
    /// Makes submissions slow, so a test can act while one is in flight.
    pub submit_delay: Option<std::time::Duration>,
    /// Run IDs by submission key: the target's idempotency.
    pub accepted: Mutex<BTreeMap<String, String>>,
    /// Archive captures: member and arguments.
    pub captures: Mutex<Vec<(String, Vec<String>)>>,
    /// Makes archive captures fail, like a target that went away.
    pub capture_fails: std::sync::atomic::AtomicBool,
    /// Makes archive captures report the target unreachable, like a deleted pool server.
    pub capture_unreachable: std::sync::atomic::AtomicBool,
    /// Discovery answer for members absent from `capabilities`, such as pool servers whose
    /// names the hub chooses.
    pub discover_any: Option<Vec<String>>,
    /// Watch cursor by run ID; defaults to `<run>-cursor`.
    pub cursors: Mutex<BTreeMap<String, String>>,
    /// `force-stop` calls as (member, run ID).
    pub stops: Mutex<Vec<(String, String)>>,
    /// Makes `force-stop` slow, so a test can overlap a status read with a stop.
    pub stop_delay: Option<std::time::Duration>,
    /// Makes every `force-stop` fail.
    pub fail_stops: std::sync::atomic::AtomicBool,
    /// Makes every `force-stop` answer with a timeout (an uncertain transport failure).
    pub unanswered_stops: std::sync::atomic::AtomicBool,
    /// Status reads that answer with a provider quota failure before normal status: run ID to the
    /// number of remaining refusals.
    pub quota_failures: Mutex<BTreeMap<String, usize>>,
    /// Provider retry instant included in those quota statuses.
    pub quota_retry_at: Option<String>,
    /// Terminal reason of those quota statuses; defaults to `worker_failed`.
    pub quota_reason: Option<String>,
    /// `resume` calls as (member, failed run ID).
    pub resumes: Mutex<Vec<(String, String)>>,
    /// Makes every `resume` fail.
    pub fail_resumes: std::sync::atomic::AtomicBool,
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
        if self.inconclusive.iter().any(|name| name == member) {
            return Err(HubError::Target {
                message: format!("zeroshot status {member} timed out: status_unavailable"),
                diagnostic: None,
            });
        }
        if self.unreachable.iter().any(|name| name == member) {
            return Err(HubError::Target {
                message: "unreachable: connection refused".to_owned(),
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
        if let Some(delay) = self.submit_delay {
            tokio::time::sleep(delay).await;
        }
        self.submissions
            .lock()
            .expect("fake lock")
            .push((request.member.to_owned(), request.submission_key.clone()));
        let mut accepted = self.accepted.lock().expect("fake lock");
        let run_id = if let Some(run_id) = accepted.get(&request.submission_key) {
            run_id.clone()
        } else {
            let mut runs = self.runs.lock().expect("fake lock");
            let entry = runs.entry(request.member.to_owned()).or_default();
            let run_id = format!("{}-run-{}", request.member, entry.len());
            entry.push((run_id.clone(), "running"));
            accepted.insert(request.submission_key, run_id.clone());
            run_id
        };
        let mut unanswered = self.unanswered.lock().expect("fake lock");
        if *unanswered > 0 {
            *unanswered -= 1;
            return Err(HubError::Unanswered("zeroshot run timed out".to_owned()));
        }
        Ok(run_id)
    }

    async fn status(&self, member: &str, run_id: &str) -> Result<Value, HubError> {
        if !self.status_ok.iter().any(|name| name == member) {
            if self.inconclusive.iter().any(|name| name == member) {
                return Err(HubError::Target {
                    message: format!("zeroshot status {member} timed out: status_unavailable"),
                    diagnostic: None,
                });
            }
            if self.unreachable.iter().any(|name| name == member) {
                return Err(HubError::Target {
                    message: "unreachable: connection refused".to_owned(),
                    diagnostic: None,
                });
            }
        }
        if let Some(delay) = self.status_delay {
            tokio::time::sleep(delay).await;
        }
        let quota_refusal = {
            let mut failures = self.quota_failures.lock().expect("fake lock");
            failures.get_mut(run_id).is_some_and(|remaining| {
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            })
        };
        if quota_refusal {
            return Ok(quota_status(
                self.quota_reason.as_deref().unwrap_or("worker_failed"),
                self.quota_retry_at.as_deref(),
            ));
        }
        let cursor = self
            .cursors
            .lock()
            .expect("fake lock")
            .get(run_id)
            .cloned()
            .unwrap_or_else(|| format!("{run_id}-cursor"));
        let runs = self.runs.lock().expect("fake lock");
        let phase = runs
            .get(member)
            .and_then(|runs| runs.iter().find(|(id, _)| id == run_id))
            .map_or("running", |(_, phase)| *phase);
        Ok(if phase == "finished" {
            json!({
                "atCursor": cursor,
                "status": {
                    "phase": "finished",
                    "terminalResult": {"status": "succeeded", "output": self.output.clone().unwrap_or_else(|| json!({}))},
                    "metadata": {"tokenUsage": {"inputTokens": 1000, "outputTokens": 100, "complete": true}}
                }
            })
        } else {
            json!({
                "atCursor": cursor,
                "status": {"phase": "running", "activeExecutions": []}
            })
        })
    }

    async fn stop(&self, member: &str, run_id: &str) -> Result<(), HubError> {
        if let Some(delay) = self.stop_delay {
            tokio::time::sleep(delay).await;
        }
        self.stops
            .lock()
            .expect("fake lock")
            .push((member.to_owned(), run_id.to_owned()));
        if self
            .unanswered_stops
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(HubError::Unanswered(
                "zeroshot force-stop timed out".to_owned(),
            ));
        }
        if self.fail_stops.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(HubError::Target {
                message: "stop refused".to_owned(),
                diagnostic: None,
            });
        }
        Ok(())
    }

    async fn resume(&self, member: &str, run_id: &str) -> Result<String, HubError> {
        if self.fail_resumes.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(HubError::Target {
                message: "resume refused".to_owned(),
                diagnostic: None,
            });
        }
        self.resumes
            .lock()
            .expect("fake lock")
            .push((member.to_owned(), run_id.to_owned()));
        let mut runs = self.runs.lock().expect("fake lock");
        let entry = runs.entry(member.to_owned()).or_default();
        let successor = format!("{run_id}-resumed");
        entry.push((successor.clone(), "running"));
        Ok(successor)
    }

    async fn capabilities(&self, member: &str) -> Result<Vec<String>, HubError> {
        self.capabilities
            .get(member)
            .cloned()
            .or_else(|| self.discover_any.clone())
            .ok_or_else(|| HubError::Target {
                message: format!("{member} discovery failed"),
                diagnostic: None,
            })
    }

    async fn capture(&self, member: &str, arguments: Vec<String>) -> Result<String, HubError> {
        if self
            .capture_unreachable
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(HubError::Target {
                message: format!(
                    "zeroshot status: cannot communicate with target {member}: connection refused"
                ),
                diagnostic: None,
            });
        }
        if self.capture_fails.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(HubError::Unanswered("capture failed".to_owned()));
        }
        let command = arguments.first().cloned().unwrap_or_default();
        self.captures
            .lock()
            .expect("fake lock")
            .push((member.to_owned(), arguments));
        Ok(match command.as_str() {
            "report" => json!({"run": {"authoredGraphHash": "sha256:graph"}}).to_string(),
            other => format!("{{\"{other}\":1}}\n"),
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
        state.jobs[0]
            .token_usage
            .as_ref()
            .and_then(JobUsage::complete)
            .map(|usage| Tokens::of(usage).input),
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
    let error = state.jobs[0].error.as_ref().expect("error");
    assert_eq!(error.kind, JobErrorKind::SubmissionRejected);
    assert!(error.message.contains("rejected"));
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
    finished.token_usage = Some(reported(usage.0, usage.1, Some(10)));
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
    let ineligible = HubError::no_eligible("tags [gpu]".to_owned(), Vec::new());
    assert_eq!(no_capacity.exit_code(), EXIT_NO_CAPACITY);
    assert_eq!(ineligible.exit_code(), EXIT_NO_ELIGIBLE_TARGET);
    assert_eq!(HubError::JobNotFound("j".to_owned()).exit_code(), 1);
    let diagnostic = ineligible.diagnostic();
    assert_eq!(diagnostic["schema"], json!("zeroshot.error/v1"));
    assert_eq!(diagnostic["code"], json!("hub.no_eligible_target"));
    assert_eq!(no_capacity.diagnostic()["code"], json!("hub.no_capacity"));
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

#[test]
fn job_errors_are_typed_and_older_plain_messages_still_load() {
    let mut legacy = serde_json::to_value(job("j1", 0)).expect("job");
    legacy["error"] = json!("zeroshot run: target down");
    let loaded: Job = serde_json::from_value(legacy).expect("an earlier hub's job loads");
    assert_eq!(
        loaded.error,
        Some(JobError::new(
            JobErrorKind::Unclassified,
            "zeroshot run: target down"
        ))
    );

    let mut typed = job("j2", 0);
    typed.error = Some(JobError::new(
        JobErrorKind::SubmissionUnconfirmed,
        "zeroshot run timed out",
    ));
    let stored = serde_json::to_value(&typed).expect("store");
    assert_eq!(
        stored["error"],
        json!({"kind": "submission_unconfirmed", "message": "zeroshot run timed out"})
    );
    assert_eq!(
        serde_json::from_value::<Job>(stored).expect("reload"),
        typed
    );
    let view = typed.view(0);
    assert_eq!(view["error"], json!("zeroshot run timed out"));
    assert_eq!(view["errorKind"], json!("submission_unconfirmed"));

    let mut lost = job("j4", 0);
    lost.error = Some(JobError::new(
        JobErrorKind::TargetLost,
        "box is no longer a hub member",
    ));
    let stored = serde_json::to_value(&lost).expect("store target-lost job");
    assert_eq!(stored["error"]["kind"], json!("target_lost"));
    assert_eq!(
        serde_json::from_value::<Job>(stored).expect("reload target-lost job"),
        lost
    );

    let mut running = job("j3", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    let read = super::schedule::apply_refresh(
        &mut running,
        Err(HubError::Usage("status unreadable".to_owned())),
        5,
    );
    assert!(read.is_none());
    assert_eq!(
        running.error.as_ref().map(|error| error.kind),
        Some(JobErrorKind::StatusUnavailable)
    );
    assert!(running.reaper.is_some());
    let mut members = [member("box", &[], 1, 0)];
    let mut jobs = [running];
    super::schedule::record_health(&mut members, &mut jobs, &loads(&[("box", true, 0)]), 6);
    assert!(
        jobs[0].reaper.is_some(),
        "a successful load probe must not erase a status-unavailable warning"
    );
    let read = super::schedule::apply_refresh(
        &mut jobs[0],
        Ok(json!({"status": {"phase": "running"}})),
        7,
    );
    assert!(read.is_some());
    assert!(jobs[0].reaper.is_none());
    assert!(jobs[0].error.is_none());
}

#[tokio::test]
async fn an_unconfirmed_submission_keeps_the_job_placing_and_retries_with_the_same_key() {
    let driver = FakeDriver {
        unanswered: Mutex::new(2),
        ..FakeDriver::default()
    };
    let mut state = HubState {
        members: vec![member("a", &[], 1, 0)],
        jobs: vec![job("j1", 0), job("j2", 0)],
        ..HubState::default()
    };
    let report = tick(&mut state, &driver, 10).await;
    assert!(report.failed.is_empty(), "a timeout is not a rejection");
    assert_eq!(report.unconfirmed.len(), 1);
    let placing = &state.jobs[0];
    assert!(placing.is_placing());
    assert_eq!(placing.target.as_deref(), Some("a"));
    assert_eq!(placing.attempts, 0, "no attempt is counted");
    assert_eq!(placing.finished_at_ms, None);
    assert_eq!(
        placing.error.as_ref().map(|error| error.kind),
        Some(JobErrorKind::SubmissionUnconfirmed)
    );
    assert_eq!(
        state.jobs[1].state,
        JobState::Queued,
        "the placing job keeps its slot"
    );

    let report = tick(&mut state, &driver, 20).await;
    assert_eq!(report.unconfirmed.len(), 1, "still unanswered");
    let report = tick(&mut state, &driver, 30).await;
    assert_eq!(report.dispatched.len(), 1);
    let placed = &state.jobs[0];
    assert_eq!(placed.state, JobState::Running);
    assert_eq!(placed.run_id.as_deref(), Some("a-run-0"));
    assert_eq!(placed.attempts, 1);
    assert_eq!(
        placed.unconfirmed_submissions, 2,
        "unconfirmed submissions are counted apart from attempts"
    );
    assert_eq!(placed.dispatched_at_ms, Some(30));
    assert_eq!(placed.error, None);
    let submissions = driver.submissions.lock().expect("fake lock").clone();
    assert_eq!(
        submissions,
        vec![("a".to_owned(), "hub-j1-1".to_owned()); 3],
        "every retry reuses the first submission key"
    );
    assert_eq!(
        driver.runs.lock().expect("fake lock")["a"].len(),
        1,
        "the target ran it once"
    );
    let view = report.view(&state, 30);
    assert_eq!(view["dispatchUnconfirmed"], json!([]));
}

#[tokio::test]
async fn too_many_unconfirmed_submissions_fail_the_job_naming_its_key() {
    let driver = FakeDriver {
        unanswered: Mutex::new(5),
        ..FakeDriver::default()
    };
    let mut placing = job("j1", 0);
    let first = super::schedule::dispatch(&mut placing, "a", &driver, (10, 2)).await;
    assert!(matches!(first, super::schedule::Dispatched::Unconfirmed(_)));
    assert_eq!(placing.unconfirmed_submissions, 1);
    assert!(placing.is_placing());
    let second = super::schedule::dispatch(&mut placing, "a", &driver, (20, 2)).await;
    assert!(matches!(second, super::schedule::Dispatched::Rejected(_)));
    assert_eq!(second.label(), "dispatch_failed");
    assert_eq!(placing.state, JobState::Failed);
    assert_eq!(placing.finished_at_ms, Some(20));
    assert_eq!(placing.attempts, 0, "no attempt was confirmed or refused");
    assert_eq!(placing.unconfirmed_submissions, 2);
    let error = placing.error.as_ref().expect("error");
    assert_eq!(error.kind, JobErrorKind::UnconfirmedLimit);
    assert!(
        error.message.contains("did not confirm 2 submissions")
            && error
                .message
                .contains("submission key hub-j1-1 may still exist"),
        "{}",
        error.message
    );
    let view = placing.view(20);
    assert_eq!(view["errorKind"], json!("unconfirmed_limit"));
    assert_eq!(view["unconfirmedSubmissions"], json!(2));
    let stored = serde_json::to_value(&placing).expect("store");
    assert_eq!(stored["unconfirmedSubmissions"], json!(2));
    let mut fresh = serde_json::to_value(job("j2", 0)).expect("store");
    assert!(fresh.get("unconfirmedSubmissions").is_none());
    fresh.as_object_mut().expect("object").remove("attempts");
    let loaded: Job = serde_json::from_value(fresh).expect("an earlier hub's job loads");
    assert_eq!(loaded.unconfirmed_submissions, 0);
}

#[test]
fn the_go_usage_limit_payload_is_recognised_with_its_limit() {
    let payload = json!({
        "status": {
            "phase": "finished",
            "terminalResult": {
                "status": "failed",
                "reason": "worker_failed",
                "context": {
                    "primary": {
                        "message": "provider returned 429 {\"type\":\"GoUsageLimitError\",\"message\":\"Go usage limit exceeded\",\"metadata\":{\"limitName\":\"weekly\"}}"
                    },
                    "recorded": []
                }
            }
        }
    });
    let now = 1_700_000_000_000;
    let failure = super::quota::detect(&payload, now).expect("quota");
    assert_eq!(failure.limit, "weekly");
    assert_eq!(failure.retry_at_ms, None);
}

#[test]
fn a_provider_retry_hint_survives_detection() {
    let now = 1_700_000_000_000;
    let at = now + 3 * 3_600_000;
    let status = json!({
        "status": {
            "phase": "finished",
            "terminalResult": {
                "status": "failed",
                "reason": "review_failed",
                "context": {
                    "primary": {
                        "message": format!(
                            "HTTP 429 Go usage limit exceeded limitName=weekly retryAt={}",
                            super::clock::rfc3339(at)
                        )
                    }
                }
            }
        }
    });
    let failure = super::quota::detect(&status, now).expect("quota");
    assert_eq!(failure.limit, "weekly");
    assert_eq!(failure.retry_at_ms, Some(at));
}

#[test]
fn a_json_provider_retry_hint_is_parsed_from_the_quota_payload() {
    let now = 1_700_000_000_000;
    let at = now + 5 * 3_600_000;
    let retry_at = super::clock::rfc3339(at);
    let status = json!({
        "status": {
            "phase": "finished",
            "terminalResult": {
                "status": "failed",
                "reason": "review_failed",
                "context": {
                    "primary": {
                        "message": format!(
                            "429 {{\"type\":\"GoUsageLimitError\",\"message\":\"Go usage limit exceeded\",\"metadata\":{{\"limitName\":\"weekly\",\"retryAt\":\"{retry_at}\"}}}}"
                        )
                    }
                }
            }
        }
    });
    let failure = super::quota::detect(&status, now).expect("quota");
    assert_eq!(failure.limit, "weekly");
    assert_eq!(failure.retry_at_ms, Some(at));
}

#[test]
fn unrelated_failures_are_not_quota_failures() {
    let status = json!({
        "status": {
            "phase": "finished",
            "terminalResult": {"status": "failed", "reason": "worker_failed"}
        }
    });
    assert!(super::quota::detect(&status, 1).is_none());
}

#[test]
fn quota_backoff_is_at_least_a_minute_and_capped() {
    let delays = (1..=10)
        .map(|attempt| super::quota::backoff_ms(attempt, "hj_test"))
        .collect::<Vec<_>>();
    assert!(delays.iter().all(|delay| *delay >= 60_000));
    assert!(delays.iter().all(|delay| *delay <= 3_600_000 + 360_001));
}

/// A failed run status whose bounded failure context carries the model gateway's quota payload.
fn quota_status(reason: &str, retry_at: Option<&str>) -> Value {
    let message = match retry_at {
        Some(retry_at) => format!(
            "429 GoUsageLimitError Go usage limit exceeded limitName=weekly retryAt={retry_at}"
        ),
        None => "429 {\"type\":\"GoUsageLimitError\",\"message\":\"Go usage limit exceeded\",\"metadata\":{\"limitName\":\"weekly\"}}".to_owned(),
    };
    json!({
        "atCursor": "v2:9",
        "status": {
            "phase": "finished",
            "terminalResult": {
                "status": "failed",
                "reason": reason,
                "context": {
                    "primary": {"message": message},
                    "recorded": [],
                },
            },
            "metadata": {
                "tokenUsage": {"inputTokens": 10, "outputTokens": 2, "complete": true}
            },
        },
    })
}

#[tokio::test]
async fn a_provider_quota_failure_pauses_the_node_then_resumes_and_succeeds() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    driver
        .runs
        .lock()
        .expect("fake lock")
        .entry("box".to_owned())
        .or_default()
        .push(("run-quota".to_owned(), "running"));
    driver
        .quota_failures
        .lock()
        .expect("fake lock")
        .insert("run-quota".to_owned(), 1);
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver.clone(),
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            let mut lease = lease_on("box");
            lease.hold = true;
            state.leases.push(lease);
            let mut running = job("hj_quota", 0);
            running.state = JobState::Running;
            running.target = Some("box".to_owned());
            running.run_id = Some("run-quota".to_owned());
            running.dispatched_at_ms = Some(1);
            running.last_cursor_at_ms = Some(1);
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");

    service.pass().await.expect("quota pass");
    let state = service.snapshot().await;
    let paused = &state.jobs[0];
    assert_eq!(paused.state, JobState::WaitingQuota);
    assert_eq!(paused.failure_reason, None, "a quota wait is not a failure");
    let error = paused.error.as_ref().expect("quota error");
    assert_eq!(error.kind, JobErrorKind::ProviderQuota);
    assert!(error.message.contains("weekly"), "{}", error.message);
    let quota = paused.quota.as_ref().expect("quota wait");
    assert_eq!(quota.limit, "weekly");
    assert_eq!(quota.attempts, 1);
    assert!(
        quota.retry_at_ms >= 60_000,
        "never retry faster than a minute"
    );
    assert!(driver.stops.lock().expect("fake lock").is_empty());
    assert!(
        state.leases[0].quota_hold,
        "the quota pause adds an automatic hold so the machine survives"
    );
    assert!(
        state.leases[0].hold,
        "the quota hold never clears the operator's deliberate hold"
    );
    assert_eq!(
        paused.display_state(super::clock::now_ms()),
        "waiting_quota"
    );
    let view = paused.view(super::clock::now_ms());
    assert_eq!(view["state"], json!("waiting_quota"));
    assert_eq!(view["errorKind"], json!("provider_quota"));
    assert_eq!(view["quota"]["limit"], json!("weekly"));

    // The retry deadline has arrived: resume the same run instead of dispatching a new one.
    service
        .mutate(|state| {
            state.jobs[0].quota.as_mut().expect("quota").retry_at_ms = 0;
            Ok(())
        })
        .await
        .expect("expire wait");
    let (report, _) = service.pass().await.expect("resume pass");
    assert_eq!(report.resumed.len(), 1);
    let state = service.snapshot().await;
    let resumed = &state.jobs[0];
    assert_eq!(resumed.state, JobState::Running);
    assert_eq!(resumed.run_id.as_deref(), Some("run-quota-resumed"));
    assert_eq!(resumed.failure_reason, None);
    assert!(
        !state.leases[0].quota_hold,
        "resuming removes only the automatic quota hold"
    );
    assert!(
        state.leases[0].hold,
        "the operator's hold survives the quota resume"
    );
    assert_eq!(
        driver.resumes.lock().expect("fake lock").as_slice(),
        &[("box".to_owned(), "run-quota".to_owned())]
    );
    assert!(driver.stops.lock().expect("fake lock").is_empty());

    driver.finish_all();
    service.pass().await.expect("finish pass");
    let state = service.snapshot().await;
    assert_eq!(state.jobs[0].state, JobState::Succeeded);
    assert!(state.jobs[0].quota.is_none());
}

#[tokio::test]
async fn a_reviewer_quota_refusal_is_not_reported_as_review_failed() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver {
        quota_reason: Some("review_failed".to_owned()),
        ..FakeDriver::default()
    });
    driver
        .runs
        .lock()
        .expect("fake lock")
        .entry("box".to_owned())
        .or_default()
        .push(("run-review".to_owned(), "running"));
    driver
        .quota_failures
        .lock()
        .expect("fake lock")
        .insert("run-review".to_owned(), 1);
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver.clone(),
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            let mut running = job("hj_review", 0);
            running.state = JobState::Running;
            running.target = Some("box".to_owned());
            running.run_id = Some("run-review".to_owned());
            running.dispatched_at_ms = Some(1);
            running.last_cursor_at_ms = Some(1);
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");
    service.pass().await.expect("quota pass");
    let state = service.snapshot().await;
    let paused = &state.jobs[0];
    assert_eq!(paused.state, JobState::WaitingQuota);
    assert_eq!(
        paused.failure_reason, None,
        "the reviewer never produced a verdict, so the run is not review_failed"
    );
    assert_eq!(
        paused.error.as_ref().expect("quota error").kind,
        JobErrorKind::ProviderQuota
    );
    assert!(paused.quota.is_some());
}

#[tokio::test]
async fn a_far_future_quota_retry_keeps_the_job_waiting_quota() {
    let directory = tempfile::tempdir().expect("tempdir");
    let now = super::clock::now_ms();
    let retry_at = super::clock::rfc3339(now + 2 * 3_600_000);
    let driver = std::sync::Arc::new(FakeDriver {
        quota_retry_at: Some(retry_at.clone()),
        ..FakeDriver::default()
    });
    driver
        .runs
        .lock()
        .expect("fake lock")
        .entry("box".to_owned())
        .or_default()
        .push(("run-far".to_owned(), "running"));
    driver
        .quota_failures
        .lock()
        .expect("fake lock")
        .insert("run-far".to_owned(), 1);
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver.clone(),
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            let mut running = job("hj_far", 0);
            running.state = JobState::Running;
            running.target = Some("box".to_owned());
            running.run_id = Some("run-far".to_owned());
            running.dispatched_at_ms = Some(1);
            running.last_cursor_at_ms = Some(1);
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");
    service.pass().await.expect("quota pass");
    let state = service.snapshot().await;
    let paused = &state.jobs[0];
    assert_eq!(paused.state, JobState::WaitingQuota);
    let quota = paused.quota.as_ref().expect("quota wait");
    assert_eq!(quota.limit, "weekly");
    assert_eq!(quota.retry_at_ms, now + 2 * 3_600_000);
    assert!(driver.resumes.lock().expect("fake lock").is_empty());

    let (report, _) = service.pass().await.expect("waiting pass");
    assert!(report.resumed.is_empty());
    assert_eq!(
        service.snapshot().await.jobs[0].state,
        JobState::WaitingQuota
    );
    assert!(driver.resumes.lock().expect("fake lock").is_empty());
}

#[tokio::test]
async fn a_quota_paused_job_keeps_its_member_and_settles_when_lost() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver,
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.leases.push(lease_on("box"));
            let mut paused = job("hj_paused", 0);
            paused.state = JobState::WaitingQuota;
            paused.target = Some("box".to_owned());
            paused.run_id = Some("run-paused".to_owned());
            paused.quota = Some(super::model::QuotaWait {
                limit: "weekly".to_owned(),
                retry_at_ms: super::clock::now_ms() + 3_600_000,
                attempts: 1,
            });
            state.jobs.push(paused);
            Ok(())
        })
        .await
        .expect("seed");
    assert!(service.snapshot().await.leases[0].quota_hold);

    let error = service
        .remove("box")
        .await
        .expect_err("a held job blocks removal");
    let HubError::Conflict(message) = error else {
        panic!("expected a conflict, got {error:?}");
    };
    assert!(message.contains("hj_paused"), "{message}");

    service
        .mutate(|state| {
            state.members.clear();
            Ok(())
        })
        .await
        .expect("member gone");
    let (report, _) = service.pass().await.expect("lost pass");
    assert_eq!(report.target_lost.len(), 1);
    let state = service.snapshot().await;
    assert_eq!(state.jobs[0].state, JobState::Failed);
    assert_eq!(state.jobs[0].failure_reason.as_deref(), Some("target_lost"));
    assert_eq!(
        state.jobs[0].error.as_ref().expect("error").kind,
        JobErrorKind::TargetLost
    );
    assert!(state.jobs[0].quota.is_none());
    assert!(
        !state.leases[0].quota_hold,
        "settling the lost quota job releases only its automatic hold"
    );
}

#[test]
fn quota_holds_are_derived_without_clearing_operator_holds() {
    let mut state = HubState {
        members: vec![member("box", &[], 1, 0)],
        jobs: vec![job("hj_paused", 0)],
        leases: vec![lease_on("box")],
        ..HubState::default()
    };
    state.jobs[0].state = JobState::WaitingQuota;
    state.jobs[0].target = Some("box".to_owned());
    state.leases[0].hold = true;
    super::pool::sync_quota_holds(&mut state);
    assert!(state.leases[0].quota_hold);
    assert!(
        state.leases[0].hold,
        "the automatic hold never clears an operator hold"
    );

    state.jobs[0].state = JobState::Running;
    super::pool::sync_quota_holds(&mut state);
    assert!(!state.leases[0].quota_hold);
    assert!(
        state.leases[0].hold,
        "the operator hold survives the quota wait"
    );
}

#[test]
fn placement_is_refused_while_the_provider_quota_is_exhausted() {
    let now = 1_000_000;
    let deadline = super::quota::QuotaDeadline::new();
    deadline.record(now + 60_000, "weekly");
    let refusal = deadline.refusal(now).expect("refused while exhausted");
    assert!(refusal.contains("weekly"), "{refusal}");

    let mut state = HubState {
        members: vec![member("box", &[], 1, 0)],
        jobs: vec![job("hj_waiting", 0)],
        ..HubState::default()
    };
    let mut observed = loads(&[("box", true, 0)]);
    assert!(
        super::schedule::choose_with_quota(&mut state, now, &mut observed, Some(&refusal))
            .is_empty(),
        "no new placement while the gateway is exhausted"
    );
    assert!(
        state.jobs[0]
            .pool_wait
            .as_deref()
            .is_some_and(|waiting| waiting.contains("weekly"))
    );

    assert!(deadline.refusal(now + 60_001).is_none());
    let chosen = super::schedule::choose(&mut state, now + 60_001, &mut observed);
    assert_eq!(chosen, vec![("hj_waiting".to_owned(), "box".to_owned())]);

    let mut waiting_run = job("hj_paused", 0);
    waiting_run.state = JobState::WaitingQuota;
    waiting_run.target = Some("box".to_owned());
    assert!(
        !super::pool::drained(&[waiting_run], "box"),
        "a quota-paused run keeps its machine busy"
    );
}

#[path = "tests/hetzner.rs"]
mod hetzner;

#[tokio::test]
async fn stopping_a_run_whose_member_is_absent_settles_runtime_lost() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver.clone(),
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    let now = super::clock::now_ms();
    let mut running = job("hj_corpse", 0);
    running.state = JobState::Running;
    running.target = Some("zs-gone".to_owned());
    running.run_id = Some("run-corpse".to_owned());
    running.dispatched_at_ms = Some(now.saturating_sub(60_000));
    running.target_last_ok_at_ms = Some(now.saturating_sub(30_000));
    service
        .mutate(|state| {
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");

    let stopped = service.stop("hj_corpse").await.expect("stop settles");
    assert_eq!(stopped["job"]["state"], json!("failed"));
    assert_eq!(stopped["job"]["failureReason"], json!("runtime_lost"));
    let settled = service.snapshot().await.jobs.remove(0);
    assert_eq!(settled.state, JobState::Failed);
    assert_eq!(settled.failure_reason.as_deref(), Some("runtime_lost"));
    assert_eq!(
        settled.failure_classification.as_deref(),
        Some(super::schedule::CLASSIFIED_STOP_UNREACHABLE)
    );
    let error = settled.error.expect("runtime-lost error");
    assert_eq!(error.kind, JobErrorKind::RuntimeLost);
    assert!(error.message.contains("zs-gone"), "{}", error.message);
    assert!(error.message.contains("run-corpse"), "{}", error.message);
    assert!(
        error.message.contains("no longer a hub member"),
        "{}",
        error.message
    );
    assert!(
        error.message.contains("last successful probe"),
        "{}",
        error.message
    );
    assert!(
        driver.stops.lock().expect("fake lock").is_empty(),
        "an absent member is settled without asking it to stop"
    );
}

#[tokio::test]
async fn stopping_a_run_on_a_slow_member_keeps_it_running() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver {
        unanswered_stops: std::sync::atomic::AtomicBool::new(true),
        ..FakeDriver::default()
    });
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver.clone(),
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    let now = super::clock::now_ms();
    let mut running = job("hj_slow", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-slow".to_owned());
    running.dispatched_at_ms = Some(now.saturating_sub(5_000));
    running.target_last_ok_at_ms = Some(now.saturating_sub(1_000));
    let mut boxed = member("box", &[], 1, 0);
    boxed.last_probe_ok_at_ms = Some(now.saturating_sub(1_000));
    service
        .mutate(|state| {
            state.members.push(boxed);
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");

    let error = service.stop("hj_slow").await.expect_err("stop fails");
    assert!(
        matches!(error, HubError::Unanswered(_)),
        "a timeout keeps the same answer the target gave: {error:?}"
    );
    let state = service.snapshot().await;
    assert_eq!(state.jobs[0].state, JobState::Running);
    assert_eq!(state.jobs[0].failure_reason, None);
    assert_eq!(state.jobs[0].finished_at_ms, None);
    assert_eq!(driver.stops.lock().expect("fake lock").len(), 1);
}

#[tokio::test]
async fn a_second_stop_on_a_settled_job_keeps_its_finished_time() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver::default());
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver,
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    let mut running = job("hj_settled", 0);
    running.state = JobState::Running;
    running.target = Some("zs-gone".to_owned());
    running.run_id = Some("run-settled".to_owned());
    service
        .mutate(|state| {
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");

    let first = service.stop("hj_settled").await.expect("first stop");
    let finished_at = first["job"]["finishedAt"].clone();
    assert!(finished_at.is_string(), "{first}");
    let second = service.stop("hj_settled").await.expect("second stop");
    assert_eq!(second["job"]["state"], json!("failed"));
    assert_eq!(second["job"]["finishedAt"], finished_at);
    assert_eq!(
        second["job"]["failureReason"],
        first["job"]["failureReason"]
    );
}

#[tokio::test]
async fn an_inconclusive_member_past_the_stall_limit_settles_runtime_lost() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = std::sync::Arc::new(FakeDriver {
        inconclusive: vec!["slow".to_owned()],
        ..FakeDriver::default()
    });
    let service = super::service::HubService::open(
        super::store::HubStore::new(directory.path().join("hub")),
        driver,
        crate::native_v2_target::FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service")
    .with_max_stall(Some(100));
    let now = super::clock::now_ms();
    let mut running = job("hj_unobservable", 0);
    running.state = JobState::Running;
    running.target = Some("slow".to_owned());
    running.run_id = Some("run-unobservable".to_owned());
    running.dispatched_at_ms = Some(now.saturating_sub(5_000));
    running.target_last_ok_at_ms = Some(now.saturating_sub(5_000));
    running.last_cursor = Some("run-unobservable-cursor".to_owned());
    // Keep the stall clock fresh: this test is about the inconclusive probe, not a silent run.
    running.last_cursor_at_ms = Some(now.saturating_sub(10));
    let mut slow = member("slow", &[], 1, 0);
    slow.last_probe_ok_at_ms = Some(now.saturating_sub(5_000));
    slow.unreachable_probes = UNREACHABLE_REAP_THRESHOLD;
    service
        .mutate(|state| {
            state.members.push(slow);
            state.jobs.push(running);
            Ok(())
        })
        .await
        .expect("seed");

    let (report, view) = service.pass().await.expect("pass");
    assert_eq!(report.runtime_lost.len(), 1, "{report:?}");
    assert_eq!(view["runtimeLost"], json!(report.runtime_lost));
    let settled = service.snapshot().await.jobs.remove(0);
    assert_eq!(settled.state, JobState::Failed);
    assert_eq!(settled.failure_reason.as_deref(), Some("runtime_lost"));
    assert_eq!(
        settled.failure_classification.as_deref(),
        Some(super::schedule::CLASSIFIED_INCONCLUSIVE)
    );
    let error = settled.error.expect("runtime-lost error");
    assert_eq!(error.kind, JobErrorKind::RuntimeLost);
    assert!(
        error.message.contains("stayed inconclusive for "),
        "{}",
        error.message
    );
    assert!(
        error.message.contains("stall limit 100ms from hub default"),
        "{}",
        error.message
    );
    assert!(
        error.message.contains("probe 3x failed"),
        "the reaper reason is in the diagnostic: {}",
        error.message
    );
    assert!(
        error.message.contains("last successful probe"),
        "{}",
        error.message
    );
}
