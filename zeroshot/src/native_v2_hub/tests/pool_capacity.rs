//! Server type choice and capacity fallback against the fake provider: no cloud is called.

use serde_json::json;

use super::super::capacity::QUEUE_EXPIRY_MS;
use super::super::clock::now_ms;
use super::super::model::{HubState, Job, JobErrorKind, JobState, Strategy};
use super::super::pool::ResourceKind;
use super::super::provision::{Progress, Refusal};
use super::pool::{Rig, TAG, add_request, catalog_fixture, job_named, rig};
use super::{job, spec};

fn sized(id: &str, tags: &[&str]) -> Job {
    let mut queued = job(id, now_ms());
    let mut all = vec![TAG];
    all.extend_from_slice(tags);
    queued.placement = spec(None, &all, Strategy::FirstFree);
    queued
}

fn refuse(rig: &Rig, places: &[(&str, &str)], refusal: Refusal) {
    rig.cloud.with(|cloud| {
        for (server_type, location) in places {
            cloud
                .refuse
                .insert(((*server_type).to_owned(), (*location).to_owned()), refusal);
        }
    });
}

fn attempts(rig: &Rig) -> Vec<(String, String)> {
    rig.cloud.with(|cloud| cloud.attempts.clone())
}

fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(server_type, location)| ((*server_type).to_owned(), (*location).to_owned()))
        .collect()
}

/// Let every capacity shortage retry now.
pub(super) async fn shortages_due(rig: &Rig) {
    rig.set(|state: &mut HubState| {
        for pool in &mut state.pools {
            for shortage in pool.shortages.values_mut() {
                shortage.retry_at_ms = 0;
            }
        }
    })
    .await;
}

#[tokio::test]
async fn capacity_refusals_try_the_next_location_then_the_next_type_and_sweep_leftovers() {
    let rig = rig();
    rig.add_pool(1).await;
    rig.cloud.with(|cloud| cloud.leave_partial = true);
    refuse(
        &rig,
        &[("cpx31", "fsn1"), ("cpx31", "nbg1")],
        Refusal::Capacity,
    );
    rig.enqueue("hj_one").await;
    let view = rig.pass().await;
    assert_eq!(
        attempts(&rig),
        pairs(&[("cpx31", "fsn1"), ("cpx31", "nbg1"), ("cx43", "fsn1")])
    );
    let created = rig.cloud.with(|cloud| cloud.created[0].clone());
    assert_eq!(
        (created.server_type.as_str(), created.location.as_str()),
        ("cx43", "fsn1")
    );
    assert_eq!(
        view["pools"]["reaped"].as_array().map(Vec::len),
        Some(2),
        "{view}"
    );
    assert_eq!(rig.cloud.servers(), 1);
    rig.cloud.with(|cloud| assert_eq!(cloud.resources.len(), 2));
    let lease = rig.lease().await;
    let machine = lease.machine.expect("machine");
    assert_eq!((machine.server_type.as_str(), machine.cores), ("cx43", 8));
    let pool = rig.state().await.pools[0].clone();
    assert!(pool.shortages.is_empty());
    assert_eq!(pool.unavailable["cpx31"]["nbg1"], 1);
    let metrics = rig.service.render_metrics().await;
    assert!(
        metrics.contains(
            r#"zeroshot_hub_pool_capacity_unavailable_total{pool="burst",server_type="cpx31",location="fsn1"} 1"#
        ),
        "{metrics}"
    );
}

#[tokio::test]
async fn a_type_the_catalog_shows_unavailable_is_skipped_without_a_create() {
    let rig = rig();
    rig.add_pool(1).await;
    let mut catalog = catalog_fixture();
    catalog.locations[0].available.remove("cpx31");
    rig.cloud.with(|cloud| cloud.catalog = Some(catalog));
    rig.enqueue("hj_one").await;
    rig.pass().await;
    assert_eq!(attempts(&rig), pairs(&[("cpx31", "nbg1")]));
    assert_eq!(rig.state().await.pools[0].unavailable["cpx31"]["fsn1"], 1);
}

#[tokio::test]
async fn no_capacity_anywhere_keeps_the_job_queued_with_a_reason_and_backs_off() {
    let rig = rig();
    rig.add_pool(1).await;
    rig.cloud.with(|cloud| {
        cloud.refuse_all = Some(Refusal::Capacity);
        cloud.leave_partial = true;
    });
    rig.enqueue("hj_one").await;
    let view = rig.pass().await;
    let reason = "waiting: no capacity for cpx31/cx43 in fsn1/nbg1";
    assert_eq!(
        view["pools"]["shortages"][0]["reason"],
        json!(reason),
        "{view}"
    );
    assert_eq!(attempts(&rig).len(), 4);
    let state = rig.state().await;
    assert!(state.leases.is_empty(), "the lease is given up");
    assert_eq!(
        rig.cloud.with(|cloud| cloud.resources.len()),
        0,
        "leftovers swept"
    );
    let pool = &state.pools[0];
    assert_eq!(pool.boot_failures, 0, "a shortage is not a failed boot");
    assert!(pool.paused_until_ms.is_none());
    let shortage = &pool.shortages["pool"];
    assert_eq!(shortage.rounds, 1);
    assert_eq!(shortage.retry_at_ms - shortage.since_ms, 30_000);

    rig.pass().await;
    let job = job_named(&rig.state().await, "hj_one").clone();
    assert_eq!(job.state, JobState::Queued);
    let waiting = job.pool_wait.clone().expect("waiting reason");
    assert!(waiting.starts_with(reason), "{waiting}");
    assert_eq!(job.view(now_ms())["waiting"], json!(waiting));
    assert_eq!(attempts(&rig).len(), 4, "no create while backing off");
    let listed = rig.service.list_pools().await;
    assert!(
        listed["pools"][0]["waiting"]
            .as_str()
            .unwrap_or_default()
            .starts_with(reason)
    );
    let metrics = rig.service.render_metrics().await;
    assert!(
        metrics.contains(r#"zeroshot_hub_pool_holding{pool="burst",reason="capacity"} 1"#),
        "{metrics}"
    );

    shortages_due(&rig).await;
    rig.pass().await;
    let pool = rig.state().await.pools[0].clone();
    assert_eq!(pool.shortages["pool"].rounds, 2);
    let shortage = &pool.shortages["pool"];
    assert!(shortage.retry_at_ms >= shortage.since_ms + 60_000);

    rig.cloud.with(|cloud| cloud.refuse_all = None);
    shortages_due(&rig).await;
    rig.pass().await;
    assert!(rig.state().await.pools[0].shortages.is_empty());
    assert_eq!(rig.cloud.servers(), 1);
}

#[tokio::test]
async fn quota_or_an_invalid_request_blocks_the_pool_until_resume() {
    for refusal in [Refusal::Quota, Refusal::Invalid] {
        let rig = rig();
        rig.add_pool(1).await;
        rig.cloud.with(|cloud| cloud.refuse_all = Some(refusal));
        rig.enqueue("hj_one").await;
        let view = rig.pass().await;
        assert_eq!(
            view["pools"]["blocked"][0]["pool"],
            json!("burst"),
            "{view}"
        );
        assert_eq!(attempts(&rig).len(), 1, "{refusal:?}: no other candidate");
        let state = rig.state().await;
        let blocked = state.pools[0].blocked.clone().expect("blocked");
        assert!(blocked.contains("cpx31 in fsn1"), "{blocked}");
        rig.pass().await;
        assert_eq!(attempts(&rig).len(), 1);
        let waiting = job_named(&rig.state().await, "hj_one").pool_wait.clone();
        assert!(
            waiting.is_some_and(|reason| reason.contains("zeroshot hub pool resume burst")),
            "{refusal:?}"
        );

        rig.cloud.with(|cloud| cloud.refuse_all = None);
        let resumed = rig.service.resume_pool("burst").await.expect("resume");
        assert!(resumed["blocked"].is_null(), "{resumed}");
        rig.pass().await;
        assert_eq!(rig.cloud.servers(), 1);
        assert!(rig.service.resume_pool("absent").await.is_err());
    }
}

#[tokio::test]
async fn rate_limits_and_catalog_failures_retry_the_lease_without_holding_the_pool() {
    let rig = rig();
    rig.add_pool(1).await;
    refuse(&rig, &[("cpx31", "fsn1")], Refusal::Capacity);
    refuse(&rig, &[("cpx31", "nbg1")], Refusal::RateLimited);
    rig.enqueue("hj_one").await;
    let view = rig.pass().await;
    assert!(
        view["pools"]["errors"][0]
            .as_str()
            .unwrap_or_default()
            .contains("RateLimited")
    );
    let state = rig.state().await;
    assert_eq!(state.leases.len(), 1, "the lease is retried");
    assert!(state.pools[0].blocked.is_none());
    assert!(state.pools[0].shortages.is_empty());
    assert_eq!(
        state.pools[0].unavailable["cpx31"]["fsn1"], 1,
        "counted before the retry"
    );

    rig.cloud.with(|cloud| {
        cloud.refuse.clear();
        cloud.catalog_fails = true;
    });
    let view = rig.pass().await;
    assert!(
        view["pools"]["errors"][0]
            .as_str()
            .unwrap_or_default()
            .contains("catalog unavailable"),
        "{view}"
    );
    assert_eq!(rig.state().await.leases.len(), 1);
    rig.cloud.with(|cloud| cloud.catalog_fails = false);
    rig.pass().await;
    assert_eq!(rig.cloud.servers(), 1);
}

#[tokio::test]
async fn wait_max_keeps_a_job_queued_behind_a_busy_pool_and_it_starts_when_the_slot_frees() {
    let rig = rig();
    let mut request = add_request(rig.directory.path(), 1);
    request.wait_max_ms = Some(60_000);
    rig.service.add_pool(request).await.expect("pool");
    rig.running_job_on_server_of_added_pool().await;
    rig.enqueue("hj_two").await;
    // Far past the pool's --wait-max, but this job has no lease of its own: the slot is busy,
    // so it must keep queueing instead of failing for waiting.
    rig.set(|state| {
        let job = state
            .jobs
            .iter_mut()
            .find(|job| job.job_id == "hj_two")
            .expect("queued job");
        job.submitted_at_ms = now_ms().saturating_sub(120_000);
        job.not_before_ms = None;
    })
    .await;
    rig.pass().await;
    let state = rig.state().await;
    let waiting_job = job_named(&state, "hj_two");
    assert_eq!(waiting_job.state, JobState::Queued);
    assert!(waiting_job.error.is_none(), "{:?}", waiting_job.error);
    let waiting = waiting_job.pool_wait.clone().expect("waiting reason");
    assert!(waiting.contains("burst"), "{waiting}");
    assert!(waiting.contains("--max of 1"), "{waiting}");
    assert!(waiting.contains("hj_one"), "{waiting}");
    assert_eq!(waiting_job.view(now_ms())["waiting"], json!(waiting));
    assert_eq!(state.leases.len(), 1, "the busy slot is untouched");

    // The first job finishing drains its lease; the waiting job starts on the freed slot.
    rig.driver.finish_all();
    for _ in 0..4 {
        rig.pass().await;
    }
    let state = rig.state().await;
    let started = job_named(&state, "hj_two");
    assert_eq!(started.state, JobState::Running, "{started:?}");
    assert!(started.error.is_none(), "{:?}", started.error);
    let active = state
        .leases
        .iter()
        .find(|lease| lease.phase == super::super::pool::LeasePhase::Active)
        .expect("a lease for the second job");
    assert_eq!(started.target.as_deref(), Some(active.name.as_str()));
}

#[tokio::test]
async fn wait_max_expires_a_lease_that_never_reached_its_target() {
    let rig = rig();
    let mut request = add_request(rig.directory.path(), 1);
    request.wait_max_ms = Some(60_000);
    rig.service.add_pool(request).await.expect("pool");
    rig.cloud
        .with(|cloud| cloud.ready = Some(Progress::Pending));
    rig.enqueue("hj_one").await;
    rig.pass().await;
    let lease = rig.lease().await;
    let server_id = lease.server_id.expect("server id");
    rig.set(|state| {
        // The queue clock is irrelevant here: this job's own lease attempt ran out.
        state.jobs[0].submitted_at_ms = now_ms();
        let lease = &mut state.leases[0];
        lease.phase = super::super::pool::LeasePhase::Connecting;
        lease.event_phase = Some(super::super::pool::PoolPhase::Preparing);
        lease.created_at_ms = now_ms().saturating_sub(120_000);
        lease.progress_at_ms = now_ms().saturating_sub(120_000);
        lease.drain_started_ms = None;
    })
    .await;

    let view = rig.pass().await;
    assert_eq!(view["pools"]["expired"], json!(["hj_one"]), "{view}");
    let state = rig.state().await;
    let job = job_named(&state, "hj_one").clone();
    assert_eq!(job.state, JobState::Failed);
    let error = job.error.expect("timeout error");
    assert_eq!(error.kind, JobErrorKind::PoolWaitTimeout);
    assert!(error.message.contains(&lease.name), "{}", error.message);
    assert!(
        error.message.contains("--wait-max of 60s"),
        "{}",
        error.message
    );
    assert!(!error.message.contains("queue"), "{}", error.message);
    assert!(
        state.leases.iter().all(|slot| slot.name != lease.name),
        "the expired lease was retired"
    );
    assert!(
        rig.cloud
            .with(|cloud| cloud.destroyed.contains(&(ResourceKind::Server, server_id))),
        "the live server behind the failed attempt was deleted"
    );
}

#[tokio::test]
async fn a_job_unplaced_for_a_day_expires_with_queue_expired() {
    let rig = rig();
    let mut request = add_request(rig.directory.path(), 1);
    request.wait_max_ms = Some(60_000);
    rig.service.add_pool(request).await.expect("pool");
    rig.enqueue("hj_one").await;
    rig.set(|state| {
        state.jobs[0].submitted_at_ms =
            now_ms().saturating_sub(QUEUE_EXPIRY_MS.saturating_add(60_000));
        state.jobs[0].not_before_ms = None;
    })
    .await;
    let view = rig.pass().await;
    assert_eq!(view["pools"]["expired"], json!([]), "{view}");
    assert_eq!(view["pools"]["queueExpired"], json!(["hj_one"]), "{view}");
    let state = rig.state().await;
    assert!(
        state.leases.is_empty(),
        "an expired queue never got a lease"
    );
    let job = job_named(&state, "hj_one");
    assert_eq!(job.state, JobState::Failed);
    let error = job.error.as_ref().expect("expiry error");
    assert_eq!(error.kind, JobErrorKind::QueueExpired);
    assert!(error.message.contains("24h"), "{}", error.message);
    assert!(error.message.contains("queue"), "{}", error.message);
    assert!(error.message.contains("burst"), "{}", error.message);
    assert!(job.pool_wait.is_none());
    assert_eq!(
        job.view(now_ms())["errorKind"],
        json!("queue_expired"),
        "{}",
        job.view(now_ms())
    );

    let stored = serde_json::to_value(&state).expect("state json");
    let loaded: HubState = serde_json::from_value(stored).expect("state loads");
    assert_eq!(
        job_named(&loaded, "hj_one")
            .error
            .as_ref()
            .map(|error| error.kind),
        Some(JobErrorKind::QueueExpired),
        "the kind round-trips through state.json"
    );
}

#[tokio::test]
async fn a_due_scheduled_job_keeps_queueing_instead_of_consuming_the_lease_budget() {
    let rig = rig();
    let mut request = add_request(rig.directory.path(), 1);
    request.wait_max_ms = Some(60_000);
    rig.service.add_pool(request).await.expect("pool");
    rig.running_job_on_server_of_added_pool().await;
    rig.enqueue("hj_two").await;
    // It was submitted and became due two minutes ago; a clock longer than --wait-max must
    // still not fail it while it waits for the busy slot.
    rig.set(|state| {
        let job = state
            .jobs
            .iter_mut()
            .find(|job| job.job_id == "hj_two")
            .expect("queued job");
        job.submitted_at_ms = now_ms().saturating_sub(120_000);
        job.not_before_ms = Some(now_ms().saturating_sub(120_000));
    })
    .await;
    rig.pass().await;
    let state = rig.state().await;
    let job = job_named(&state, "hj_two");
    assert_eq!(job.state, JobState::Queued);
    assert!(job.error.is_none(), "{:?}", job.error);
    let waiting = job.pool_wait.clone().expect("waiting reason");
    assert!(waiting.contains("--max of 1"), "{waiting}");
}

#[tokio::test]
async fn size_requests_pick_the_first_type_that_fits() {
    let rig = rig();
    rig.add_pool(2).await;
    rig.service
        .enqueue(sized("hj_big", &["min-vcpu=8"]), Vec::new())
        .await
        .expect("served by cx43");
    rig.pass().await;
    let created = rig.cloud.with(|cloud| cloud.created[0].clone());
    assert_eq!(created.server_type, "cx43");
    let lease = rig.lease().await;
    assert_eq!(lease.spec.want.describe(), "min-vcpu=8");
    assert_eq!(lease.view()["spec"]["source"], json!("job"));

    for tags in [
        &["server-type=cx23"][..],
        &["min-ram-gb=64"],
        &["min-vcpu=99"],
    ] {
        let error = rig
            .service
            .enqueue(sized("hj_none", tags), Vec::new())
            .await
            .expect_err("no type fits");
        assert!(
            error.to_string().contains("no active hub member matches"),
            "{tags:?}: {error}"
        );
    }
    rig.service
        .enqueue(
            sized("hj_named", &["server-type=cpx31", "min-ram-gb=8"]),
            Vec::new(),
        )
        .await
        .expect("cpx31 has 8 GB");
}

#[tokio::test]
async fn pool_types_are_checked_against_the_catalog() {
    let rig = rig();
    let folder = rig.directory.path();
    type Case = (&'static str, fn(&mut super::super::wire::AddPool));
    let cases: [Case; 6] = [
        ("unknown server type", |request| {
            request.server_types = vec!["cx99".to_owned()];
        }),
        ("deprecated", |request| {
            request.server_types = vec!["cx11".to_owned()]
        }),
        ("ARM", |request| {
            request.server_types = vec!["cax21".to_owned()]
        }),
        ("unknown location", |request| {
            request.locations = vec!["ash".to_owned()]
        }),
        ("listed twice", |request| {
            request.server_types = vec!["cx43".to_owned(), "cx43".to_owned()];
        }),
        ("at least one server type", |request| {
            request.server_types.clear()
        }),
    ];
    for (message, change) in cases {
        let mut request = add_request(folder, 1);
        change(&mut request);
        let error = rig.service.add_pool(request).await.expect_err(message);
        assert!(error.to_string().contains(message), "{message}: {error}");
    }
    let mut arm = add_request(folder, 1);
    arm.server_types = vec!["cax21".to_owned(), "cpx31".to_owned()];
    arm.locations.clear();
    arm.multi_arch = true;
    let added = rig.service.add_pool(arm).await.expect("multi-arch");
    assert_eq!(
        added["pool"]["locations"],
        json!(["fsn1", "nbg1", "hel1"]),
        "{added}"
    );

    let types = rig.service.pool_types().await.expect("types");
    assert_eq!(types["schema"], json!("zeroshot.hub.pool-types/v1"));
    let names = types["types"]
        .as_array()
        .expect("types")
        .iter()
        .map(|server_type| server_type["name"].as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["cx23", "cpx31", "cx43", "cax21"],
        "deprecated hidden"
    );
    assert_eq!(types["types"][3]["needsMultiArch"], json!(true));
    assert_eq!(types["types"][1]["hourlyGross"]["fsn1"], json!("0.0148"));
}

#[tokio::test]
async fn a_starting_server_only_counts_its_spare_slots_for_other_jobs() {
    for (slots, servers) in [(1, 2), (2, 1)] {
        let rig = rig();
        let mut request = add_request(rig.directory.path(), 3);
        request.slots = slots;
        rig.service.add_pool(request).await.expect("pool");
        rig.enqueue("hj_one").await;
        rig.pass().await;
        assert_eq!(
            rig.lease().await.phase,
            super::super::pool::LeasePhase::Booting
        );
        rig.enqueue("hj_two").await;
        rig.pass().await;
        assert_eq!(rig.state().await.leases.len(), servers, "slots {slots}");
    }
}

#[tokio::test]
async fn a_blocked_pool_wait_names_the_reason_and_wait_max_does_not_expire_it() {
    let rig = rig();
    let mut request = add_request(rig.directory.path(), 1);
    request.wait_max_ms = Some(60_000);
    rig.service.add_pool(request).await.expect("pool");
    rig.cloud
        .with(|cloud| cloud.refuse_all = Some(Refusal::Quota));
    rig.enqueue("hj_one").await;
    let submitted = now_ms();
    rig.set(|state| {
        for job in &mut state.jobs {
            job.submitted_at_ms = submitted;
        }
    })
    .await;
    rig.pass().await;
    rig.pass().await;

    let state = rig.state().await;
    let job = job_named(&state, "hj_one").clone();
    assert_eq!(job.state, JobState::Queued);
    let error = job.error.as_ref().expect("blocked error");
    assert_eq!(error.kind, JobErrorKind::PoolBlocked);
    let failure = job.failure_reason.clone().expect("failure reason");
    assert!(failure.contains("burst"), "{failure}");
    assert!(failure.contains("Quota"), "{failure}");
    assert_eq!(
        job.view(now_ms())["errorKind"],
        json!("pool_blocked"),
        "{}",
        job.view(now_ms())
    );
    let waiting = job.pool_wait.clone().expect("waiting reason");
    assert!(waiting.contains("blocked until"), "{waiting}");
    assert!(
        waiting.contains("zeroshot hub pool resume burst"),
        "{waiting}"
    );

    rig.set(|state| {
        for job in &mut state.jobs {
            job.submitted_at_ms = job.submitted_at_ms.saturating_sub(120_000);
        }
    })
    .await;
    rig.pass().await;
    let state = rig.state().await;
    let job = job_named(&state, "hj_one");
    assert_eq!(
        job.state,
        JobState::Queued,
        "a blocked pool wait is queue time, not a lease attempt"
    );
    let error = job.error.as_ref().expect("blocked error stays");
    assert_eq!(error.kind, JobErrorKind::PoolBlocked);
    assert!(error.message.contains("burst"), "{}", error.message);
    assert!(error.message.contains("Quota"), "{}", error.message);
    let failure = job.failure_reason.clone().expect("block reason stays");
    assert!(failure.contains("burst"), "{failure}");
    assert!(failure.contains("Quota"), "{failure}");
    assert_eq!(
        job.view(now_ms())["errorKind"],
        json!("pool_blocked"),
        "{}",
        job.view(now_ms())
    );
}
