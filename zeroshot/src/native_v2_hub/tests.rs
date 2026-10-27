use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{Value, json};

use super::clock::{parse_duration_ms, parse_future_ms, parse_past_ms, rfc3339, utc_day};
use super::driver::{HubDriver, SubmitRequest};
use super::error::{EXIT_NO_CAPACITY, EXIT_NO_ELIGIBLE_TARGET, HubError};
use super::jobs::{repository_name, runtime_models};
use super::model::{
    HubState, Job, JobError, JobErrorKind, JobState, Member, MemberState, PlacementSpec,
    Requirements, Strategy,
};
use super::pool::{Lease, LeasePhase};
use super::usage::{JobUsage, Tokens, reported};
use super::placement::{Load, Placement, describe, place};
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
        submitted_at_ms,
        not_before_ms: None,
        attempts: 0,
        unconfirmed_submissions: 0,
        target: None,
        run_id: None,
        dispatched_at_ms: None,
        last_cursor: None,
        last_cursor_at_ms: None,
        target_last_ok_at_ms: None,
        reaper: None,
        finished_at_ms: None,
        failure_reason: None,
        token_usage: None,
        error: None,
        schedule_id: None,
        output: None,
        archive: None,
        graph_hash: None,
        pool_wait: None,
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
    let error = settled.error.expect("stall error");
    assert_eq!(error.kind, JobErrorKind::Stalled);
    assert!(
        error.message.contains("run-stalled-cursor"),
        "{}",
        error.message
    );
    assert!(error.message.contains("ms"), "{}", error.message);
    assert_eq!(
        driver.stops.lock().expect("fake lock").as_slice(),
        [("box".to_owned(), "run-stalled".to_owned())]
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
    /// Discovery answer for members absent from `capabilities`, such as pool servers whose
    /// names the hub chooses.
    pub discover_any: Option<Vec<String>>,
    /// Watch cursor by run ID; defaults to `<run>-cursor`.
    pub cursors: Mutex<BTreeMap<String, String>>,
    /// `force-stop` calls as (member, run ID).
    pub stops: Mutex<Vec<(String, String)>>,
    /// Makes every `force-stop` fail.
    pub fail_stops: std::sync::atomic::AtomicBool,
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
        if let Some(delay) = self.status_delay {
            tokio::time::sleep(delay).await;
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
        self.stops
            .lock()
            .expect("fake lock")
            .push((member.to_owned(), run_id.to_owned()));
        if self.fail_stops.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(HubError::Target {
                message: "stop refused".to_owned(),
                diagnostic: None,
            });
        }
        Ok(())
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

#[path = "tests/hetzner.rs"]
mod hetzner;
