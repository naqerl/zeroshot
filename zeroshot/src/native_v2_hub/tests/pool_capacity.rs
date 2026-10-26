//! Server type choice and capacity fallback against the fake provider: no cloud is called.

use serde_json::json;

use super::super::clock::now_ms;
use super::super::model::{HubState, Job, JobErrorKind, JobState, Strategy};
use super::super::provision::Refusal;
use super::pool::{Rig, TAG, add_request, catalog_fixture, job_named, rig};
use super::{job, spec};

fn sized(id: &str, tags: &[&str]) -> Job {
    let mut queued = job(id, 1);
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
async fn wait_max_expires_a_job_that_found_no_server() {
    let rig = rig();
    let mut request = add_request(rig.directory.path(), 1);
    request.wait_max_ms = Some(60_000);
    rig.service.add_pool(request).await.expect("pool");
    rig.cloud
        .with(|cloud| cloud.refuse_all = Some(Refusal::Capacity));
    rig.enqueue("hj_one").await;
    let submitted = now_ms();
    rig.set(|state| {
        for job in &mut state.jobs {
            job.submitted_at_ms = submitted;
        }
    })
    .await;
    rig.pass().await;
    assert_eq!(
        job_named(&rig.state().await, "hj_one").state,
        JobState::Queued
    );
    rig.set(|state| {
        for job in &mut state.jobs {
            job.submitted_at_ms = job.submitted_at_ms.saturating_sub(120_000);
        }
    })
    .await;
    let view = rig.pass().await;
    assert_eq!(view["pools"]["expired"], json!(["hj_one"]), "{view}");
    let job = job_named(&rig.state().await, "hj_one").clone();
    assert_eq!(job.state, JobState::Failed);
    let error = job.error.expect("error");
    assert_eq!(error.kind, JobErrorKind::PoolWaitTimeout);
    assert!(
        error.message.contains("--wait-max of 60s"),
        "{}",
        error.message
    );
    assert!(error.message.contains("no capacity"), "{}", error.message);
    assert!(
        rig.state().await.leases.is_empty(),
        "no server for an expired job"
    );
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
