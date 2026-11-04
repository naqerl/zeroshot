//! Pool lease reconciliation: stale capacity must never wedge placement, and a retained
//! definition must come back without an operator retyping it.

use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::super::clock::now_ms;
use super::super::model::{HubState, JobErrorKind, JobState, MemberState, PlacementSpec};
use super::super::pool::{self, RECONCILED_STALE_LEASE, STALE_LEASE_AFTER_MS};
use super::super::service::HubService;
use super::super::store::HubStore;
use super::{FakeDriver, job, member};
use crate::native_v2_target::{FileTargetRegistry, TargetAccess, TargetRecord, TargetRegistry};

fn registry(path: &Path) -> FileTargetRegistry {
    FileTargetRegistry::new(path.join("targets.json"))
}

/// A registered direct target; the hub host's own configuration for the box behind a member.
fn record(name: &str) -> TargetRecord {
    TargetRecord {
        id: format!("00000000-0000-4000-8000-{:012x}", name.len() * 7919),
        name: name.to_owned(),
        origin: "http://127.0.0.1:9".to_owned(),
        access: TargetAccess::Direct,
    }
}

fn open_shared(directory: &Path, driver: Arc<FakeDriver>) -> Arc<HubService> {
    Arc::new(
        HubService::open(
            HubStore::new(directory.join("hub")),
            driver,
            registry(directory),
        )
        .expect("service"),
    )
}

fn open(directory: &Path, driver: FakeDriver) -> Arc<HubService> {
    open_shared(directory, Arc::new(driver))
}

/// A queued job pinned to one member, like a job submitted with `--target`.
fn pinned(id: &str, member: &str, submitted_at_ms: u64) -> super::super::model::Job {
    super::super::model::Job {
        placement: PlacementSpec {
            target: Some(member.to_owned()),
            ..PlacementSpec::default()
        },
        ..job(id, submitted_at_ms)
    }
}

#[tokio::test]
async fn a_phantom_lease_never_fails_the_waiting_job_and_heals_after_a_restart() {
    let directory = tempfile::tempdir().expect("tempdir");
    registry(directory.path())
        .insert(record("box"))
        .expect("register box");
    let driver = FakeDriver {
        unreachable: vec!["box".to_owned()],
        ..FakeDriver::default()
    };
    let service = open(directory.path(), driver);
    let now = now_ms();
    let mut boxed = member("box", &["linux"], 2, 3);
    boxed.unreachable_probes = pool::STALE_LEASE_MIN_PROBES;
    boxed.unreachable_since_ms = Some(now.saturating_sub(STALE_LEASE_AFTER_MS + 1_000));
    service
        .mutate(|state| {
            state.members.push(boxed);
            state.jobs.push(pinned("hj_phantom", "box", now));
            Ok(())
        })
        .await
        .expect("seed");

    let (report, view) = service.pass().await.expect("pass");
    assert_eq!(report.reconciled.len(), 1, "{view}");
    assert_eq!(report.reconciled[0]["member"], json!("box"));
    assert_eq!(
        report.reconciled[0]["reason"],
        json!(RECONCILED_STALE_LEASE)
    );
    assert_eq!(report.reconciled[0]["phase"], json!("failed"));
    let state = service.snapshot().await;
    assert_eq!(state.members[0].state, MemberState::Stale);
    // The definition survives, so nothing has to be retyped.
    assert_eq!(state.members[0].tags, ["linux"]);
    assert_eq!(state.members[0].slots, 2);
    assert_eq!(state.members[0].priority, 3);
    // The waiting job is never failed with a misleading checkout error.
    assert_eq!(state.jobs[0].state, JobState::Queued);
    assert!(state.jobs[0].error.is_none(), "{:?}", state.jobs[0].error);
    let rendered = format!(
        "{view}{}",
        serde_json::to_string(&state).expect("state json")
    );
    assert!(
        !rendered.contains("source_checkout_unavailable"),
        "{rendered}"
    );

    // One record per dropped lease: the next pass reports nothing new.
    let (again, _) = service.pass().await.expect("pass");
    assert!(again.reconciled.is_empty(), "{:?}", again.reconciled);

    // The operator recreates the box: the first pass after the restart places immediately,
    // with no reset and no removed member to add again.
    drop(service);
    let service = open(directory.path(), FakeDriver::default());
    let (report, view) = service.pass().await.expect("pass");
    assert_eq!(report.reconciled.len(), 1, "{view}");
    assert_eq!(report.reconciled[0]["member"], json!("box"));
    assert_eq!(report.reconciled[0]["phase"], json!("active"));
    let state = service.snapshot().await;
    assert_eq!(state.members[0].state, MemberState::Active);
    assert!(state.members[0].stale.is_none());
    assert_eq!(state.jobs[0].state, JobState::Running);
    assert_eq!(state.jobs[0].target.as_deref(), Some("box"));
    assert!(
        report
            .dispatched
            .iter()
            .any(|entry| entry["jobId"] == json!("hj_phantom")),
        "{:?}",
        report.dispatched
    );
}

#[tokio::test]
async fn reconciliation_reports_each_dropped_lease_once() {
    let directory = tempfile::tempdir().expect("tempdir");
    registry(directory.path())
        .insert(record("box"))
        .expect("register box");
    let service = open(directory.path(), FakeDriver::default());
    service
        .mutate(|state| {
            // Two members whose box registration is gone, and one live member.
            state.members.push(member("ghost-a", &[], 1, 0));
            state.members.push(member("box", &[], 1, 0));
            state.members.push(member("ghost-b", &[], 1, 0));
            Ok(())
        })
        .await
        .expect("seed");

    let (report, view) = service.pass().await.expect("pass");
    let dropped = report
        .reconciled
        .iter()
        .map(|entry| entry["member"].as_str().unwrap_or_default().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(dropped, ["ghost-a", "ghost-b"], "{view}");
    for entry in &report.reconciled {
        assert_eq!(entry["reason"], json!(RECONCILED_STALE_LEASE));
        assert_eq!(entry["phase"], json!("failed"));
        assert!(
            entry["detail"]
                .as_str()
                .is_some_and(|detail| !detail.is_empty()),
            "{entry}"
        );
    }
    let state = service.snapshot().await;
    let alive = state.member("box").expect("box");
    assert_eq!(alive.state, MemberState::Active, "a healthy lease is kept");
    assert!(alive.stale.is_none());

    let (again, _) = service.pass().await.expect("pass");
    assert!(again.reconciled.is_empty(), "{:?}", again.reconciled);
}

#[test]
fn reconciliation_ignores_local_drained_disabled_and_recently_failed_members() {
    let now = 10_000_000;
    let mut local = member("local", &[], 1, 0);
    local.unreachable_probes = 9;
    local.unreachable_since_ms = Some(now - STALE_LEASE_AFTER_MS * 2);
    let mut drained = member("drained", &[], 1, 0);
    drained.state = MemberState::Drain;
    drained.unreachable_probes = 9;
    drained.unreachable_since_ms = Some(now - STALE_LEASE_AFTER_MS * 2);
    let mut disabled = member("disabled", &[], 1, 0);
    disabled.state = MemberState::Disabled;
    let mut healthy = member("box", &[], 1, 0);
    healthy.unreachable_probes = 9;
    healthy.last_probe_ok_at_ms = Some(now);
    // Unreachable past the budget, but with too few agreeing probes: still trusted.
    let mut flapping = member("flapping", &[], 1, 0);
    flapping.unreachable_probes = 1;
    flapping.unreachable_since_ms = Some(now - STALE_LEASE_AFTER_MS * 2);
    let mut members = vec![local, drained, disabled, healthy, flapping];

    let dropped = pool::reconcile(&mut members, now, |name| Some(name != "flapping"));
    let names = dropped
        .iter()
        .map(|entry| entry.member.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["flapping"]);
    assert_eq!(
        members[0].state,
        MemberState::Active,
        "local is always live"
    );
    assert_eq!(members[3].state, MemberState::Active, "healthy stays live");
    assert_eq!(members[4].state, MemberState::Stale);
    assert_eq!(
        members[4].stale.as_ref().map(|stale| stale.reason.as_str()),
        Some(RECONCILED_STALE_LEASE)
    );
}

#[tokio::test]
async fn reset_rearms_a_stale_definition_without_retyping_anything() {
    let directory = tempfile::tempdir().expect("tempdir");
    registry(directory.path())
        .insert(record("box"))
        .expect("register box");
    let driver = FakeDriver {
        unreachable: vec!["box".to_owned()],
        ..FakeDriver::default()
    };
    let service = open(directory.path(), driver);
    let now = now_ms();
    let mut boxed = member("box", &["linux", "gpu"], 4, 7);
    boxed.unreachable_probes = pool::STALE_LEASE_MIN_PROBES;
    boxed.unreachable_since_ms = Some(now.saturating_sub(STALE_LEASE_AFTER_MS + 1_000));
    service
        .mutate(|state| {
            state.members.push(boxed);
            state.jobs.push(pinned("hj_reset", "box", now));
            Ok(())
        })
        .await
        .expect("seed");
    let (report, _) = service.pass().await.expect("pass");
    assert_eq!(report.reconciled.len(), 1);

    let reset = service.reset("box").await.expect("reset");
    assert_eq!(reset["member"]["state"], json!("active"));
    assert_eq!(reset["member"]["tags"], json!(["linux", "gpu"]));
    assert_eq!(reset["member"]["slots"], json!(4));
    assert_eq!(reset["member"]["priority"], json!(7));
    assert_eq!(reset["member"]["stale"], json!(null));
    let state = service.snapshot().await;
    assert_eq!(state.members[0].state, MemberState::Active);
    assert!(state.members[0].stale.is_none());

    let missing = service.reset("ghost").await.expect_err("unknown member");
    assert!(
        missing.to_string().contains("not a hub member"),
        "{missing}"
    );

    // The reset definition is immediately placeable again, with the same tags and slots.
    drop(service);
    let service = open(directory.path(), FakeDriver::default());
    let (report, _) = service.pass().await.expect("pass");
    let state = service.snapshot().await;
    assert_eq!(state.members[0].tags, ["linux", "gpu"]);
    assert_eq!(state.members[0].slots, 4);
    assert_eq!(state.jobs[0].state, JobState::Running);
    assert_eq!(state.jobs[0].target.as_deref(), Some("box"));
    assert!(
        report
            .dispatched
            .iter()
            .any(|entry| entry["jobId"] == json!("hj_reset")),
        "{:?}",
        report.dispatched
    );
}

#[tokio::test]
async fn a_pool_at_max_slots_still_keeps_extra_jobs_queued() {
    let directory = tempfile::tempdir().expect("tempdir");
    registry(directory.path())
        .insert(record("box"))
        .expect("register box");
    let service = open(directory.path(), FakeDriver::default());
    let now = now_ms();
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.jobs.push(job("hj_first", now));
            state.jobs.push(job("hj_second", now));
            Ok(())
        })
        .await
        .expect("seed");

    let (report, view) = service.pass().await.expect("pass");
    assert!(report.reconciled.is_empty(), "{view}");
    let state = service.snapshot().await;
    let running = state
        .jobs
        .iter()
        .filter(|job| job.state == JobState::Running)
        .count();
    assert_eq!(running, 1, "one free slot takes one job");
    let queued = state
        .jobs
        .iter()
        .filter(|job| job.state == JobState::Queued)
        .count();
    assert_eq!(queued, 1);
    assert_eq!(state.members[0].state, MemberState::Active);
}

#[tokio::test]
async fn reset_is_served_over_the_api_and_keeps_the_definition() {
    let hub = super::flows::hub_with(FakeDriver::default(), None).await;
    hub.client
        .post(
            &["v1", "hub", "members"],
            &json!({"name": "box", "tags": ["linux"], "slots": 2, "priority": 4}),
        )
        .await
        .expect("add box");
    hub.service
        .mutate(|state| {
            let member = state
                .members
                .iter_mut()
                .find(|member| member.name == "box")
                .expect("box");
            member.state = MemberState::Stale;
            member.stale = Some(super::super::model::StaleLease {
                reason: RECONCILED_STALE_LEASE.to_owned(),
                detail: "box was deleted out of band".to_owned(),
                at_ms: now_ms(),
            });
            Ok(())
        })
        .await
        .expect("seed stale lease");

    let reset = hub
        .client
        .post(&["v1", "hub", "members", "box", "reset"], &json!({}))
        .await
        .expect("reset");
    assert_eq!(reset["member"]["state"], json!("active"));
    assert_eq!(reset["member"]["stale"], json!(null));
    assert_eq!(reset["member"]["tags"], json!(["linux"]));
    assert_eq!(reset["member"]["slots"], json!(2));
    assert_eq!(reset["member"]["priority"], json!(4));

    let unknown = hub
        .client
        .post(&["v1", "hub", "members", "ghost", "reset"], &json!({}))
        .await
        .expect_err("unknown member");
    assert_eq!(unknown.diagnostic()["code"], json!("hub.member_not_found"));
}

#[tokio::test]
async fn a_placing_job_on_a_dropped_lease_is_requeued_and_placed_elsewhere() {
    let directory = tempfile::tempdir().expect("tempdir");
    let targets = registry(directory.path());
    targets.insert(record("box")).expect("register box");
    targets.insert(record("other")).expect("register other");
    let driver = Arc::new(FakeDriver {
        unreachable: vec!["box".to_owned()],
        ..FakeDriver::default()
    });
    let service = open_shared(directory.path(), driver.clone());
    let now = now_ms();
    let mut boxed = member("box", &[], 1, 0);
    boxed.unreachable_probes = pool::STALE_LEASE_MIN_PROBES;
    boxed.unreachable_since_ms = Some(now.saturating_sub(STALE_LEASE_AFTER_MS + 1_000));
    let mut placing = job("hj_placing", now);
    placing.state = JobState::Running;
    placing.target = Some("box".to_owned());
    service
        .mutate(|state| {
            state.members.push(boxed);
            state.members.push(member("other", &[], 1, 0));
            state.jobs.push(placing);
            Ok(())
        })
        .await
        .expect("seed");

    let (report, view) = service.pass().await.expect("pass");
    assert_eq!(report.reconciled.len(), 1, "{view}");
    assert_eq!(report.requeued.len(), 1, "{view}");
    assert_eq!(report.requeued[0]["jobId"], json!("hj_placing"));
    assert_eq!(report.requeued[0]["member"], json!("box"));
    assert_eq!(report.requeued[0]["reason"], json!(RECONCILED_STALE_LEASE));
    assert_eq!(
        report.requeued[0]["submissionKey"],
        json!("hub-hj_placing-1"),
        "the record names the key the old member may have accepted"
    );
    assert_eq!(report.requeued[0]["runId"], json!(null));
    let state = service.snapshot().await;
    let job = &state.jobs[0];
    assert_eq!(job.state, JobState::Running);
    assert_eq!(job.target.as_deref(), Some("other"), "{job:?}");
    assert!(
        report
            .dispatched
            .iter()
            .any(|entry| entry["jobId"] == json!("hj_placing") && entry["target"] == json!("other")),
        "{:?}",
        report.dispatched
    );
    assert_eq!(
        driver
            .submissions
            .lock()
            .expect("fake lock")
            .iter()
            .map(|(member, _)| member.as_str())
            .collect::<Vec<_>>(),
        ["other"],
        "the dropped lease never receives the submission"
    );
    assert_eq!(
        driver
            .submissions
            .lock()
            .expect("fake lock")
            .iter()
            .map(|(_, key)| key.as_str())
            .collect::<Vec<_>>(),
        ["hub-hj_placing-2"],
        "the attempt advances past the abandoned key"
    );
}

#[tokio::test]
async fn a_requeued_job_names_the_abandoned_submission_key() {
    let directory = tempfile::tempdir().expect("tempdir");
    registry(directory.path())
        .insert(record("box"))
        .expect("register box");
    let driver = FakeDriver {
        unreachable: vec!["box".to_owned()],
        ..FakeDriver::default()
    };
    let service = open(directory.path(), driver);
    let now = now_ms();
    let mut boxed = member("box", &[], 1, 0);
    boxed.unreachable_probes = pool::STALE_LEASE_MIN_PROBES;
    boxed.unreachable_since_ms = Some(now.saturating_sub(STALE_LEASE_AFTER_MS + 1_000));
    let mut placing = job("hj_orphan", now);
    placing.state = JobState::Running;
    placing.target = Some("box".to_owned());
    service
        .mutate(|state| {
            state.members.push(boxed);
            state.jobs.push(placing);
            Ok(())
        })
        .await
        .expect("seed");

    let (report, _) = service.pass().await.expect("pass");
    assert_eq!(report.requeued.len(), 1, "{:?}", report.requeued);
    assert_eq!(
        report.requeued[0]["submissionKey"],
        json!("hub-hj_orphan-1")
    );
    let state = service.snapshot().await;
    let job = &state.jobs[0];
    assert_eq!(job.state, JobState::Queued, "no live capacity to place on");
    let error = job.error.as_ref().expect("the abandoned key is recorded");
    assert_eq!(error.kind, JobErrorKind::SubmissionUnconfirmed);
    assert!(
        error.message.contains("hub-hj_orphan-1"),
        "{}",
        error.message
    );
    assert_eq!(job.attempts, 1, "the abandoned attempt is counted");
}

#[tokio::test]
async fn a_lease_reconciled_at_boot_is_reported_by_the_first_pass() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    {
        let lock = store.lock().expect("lock");
        let mut state = HubState::default();
        state.members.push(member("ghost", &[], 1, 0));
        store.save(&lock, &state).expect("save");
    }
    let service = open(directory.path(), FakeDriver::default());
    assert_eq!(
        service.snapshot().await.members[0].state,
        MemberState::Stale,
        "boot reconciliation ran"
    );

    let (first, view) = service.pass().await.expect("pass");
    assert_eq!(first.reconciled.len(), 1, "{view}");
    assert_eq!(first.reconciled[0]["member"], json!("ghost"));
    assert_eq!(first.reconciled[0]["reason"], json!(RECONCILED_STALE_LEASE));
    let (second, _) = service.pass().await.expect("pass");
    assert!(second.reconciled.is_empty(), "{:?}", second.reconciled);
}

#[tokio::test]
async fn submit_reconciles_leases_before_placing() {
    let directory = tempfile::tempdir().expect("tempdir");
    registry(directory.path())
        .insert(record("box"))
        .expect("register box");
    let driver = Arc::new(FakeDriver::default());
    let service = open_shared(directory.path(), driver);
    let now = now_ms();
    service
        .mutate(|state| {
            // `ghost` has no target registration behind it; `box` is live capacity.
            state.members.push(member("ghost", &[], 1, 0));
            state.members.push(member("box", &[], 1, 0));
            Ok(())
        })
        .await
        .expect("seed");

    let submitted = service
        .submit(job("hj_submit", now), false, Vec::new())
        .await
        .expect("submit");
    assert_eq!(submitted["job"]["target"], json!("box"), "{submitted}");
    let state = service.snapshot().await;
    assert_eq!(
        state.member("ghost").expect("ghost").state,
        MemberState::Stale
    );
}

#[test]
fn rejections_consider_retained_definitions_that_cannot_run_the_job() {
    use super::super::model::Requirements;
    use super::super::placement::rejections;

    let requirements = Requirements {
        capabilities: vec!["openengine.node-runtime-lanes/v1".to_owned()],
        ..Requirements::default()
    };
    let spec = PlacementSpec::default();
    let mut retained = member("stale-box", &[], 1, 0);
    retained.state = MemberState::Stale;
    retained.capabilities = Some(Vec::new());
    let mut live = member("live-box", &[], 1, 0);
    live.capabilities = Some(Vec::new());

    let rejected = rejections(&[retained.clone(), live.clone()], &spec, &requirements)
        .expect("no member can run it");
    assert_eq!(
        rejected
            .iter()
            .map(|rejection| rejection.member.as_str())
            .collect::<Vec<_>>(),
        ["stale-box", "live-box"],
        "a retained definition known not to support the job is reported"
    );

    // Undiscovered capabilities may still support the job once the definition is re-armed.
    retained.capabilities = None;
    assert!(
        rejections(&[retained, live], &spec, &requirements).is_none(),
        "a retained definition that might run the job is not rejected"
    );
}
