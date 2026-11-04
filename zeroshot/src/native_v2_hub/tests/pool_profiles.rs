//! Tag profiles: which machine a pool job gets, against the fake provider only.

use serde_json::json;

use super::super::clock::now_ms;
use super::super::model::{Job, JobState, Strategy};
use super::super::pool::{Lease, LeasePhase, lease_id};
use super::super::profile::{self, SpecSource};
use super::super::provision::Refusal;
use super::super::wire::SetTagProfile;
use super::pool::{Rig, TAG, job_named, rig, tagged};
use super::{job, spec};

fn with_tags(id: &str, tags: &[&str]) -> Job {
    let mut queued = job(id, 1);
    let mut all = vec![TAG];
    all.extend_from_slice(tags);
    queued.placement = spec(None, &all, Strategy::FirstFree);
    queued
}

fn profile(types: &[&str]) -> SetTagProfile {
    SetTagProfile {
        server_types: types.iter().map(|name| (*name).to_owned()).collect(),
        ..SetTagProfile::default()
    }
}

async fn set(rig: &Rig, tag: &str, request: SetTagProfile) {
    rig.service
        .set_tag_profile(("burst", tag), request)
        .await
        .expect("set profile");
}

async fn submit(rig: &Rig, id: &str, tags: &[&str]) {
    rig.service
        .enqueue(with_tags(id, tags), Vec::new())
        .await
        .expect("enqueue");
}

async fn lease_of(rig: &Rig, id: &str) -> Lease {
    rig.state()
        .await
        .leases
        .into_iter()
        .find(|lease| lease.id == lease_id(id))
        .expect("lease for job")
}

async fn spec_of(rig: &Rig, tags: &[&str]) -> profile::Spec {
    let tags = tags.iter().map(|tag| (*tag).to_owned()).collect::<Vec<_>>();
    profile::resolve_spec(&rig.state().await.pools[0], &tags)
}

#[tokio::test]
async fn a_job_size_beats_its_tag_profile_which_beats_the_pool_default() {
    let rig = rig();
    rig.add_pool(3).await;
    set(&rig, "heavy", profile(&["cx43"])).await;
    submit(&rig, "hj_pool", &[]).await;
    submit(&rig, "hj_tag", &["heavy"]).await;
    submit(&rig, "hj_job", &["heavy", "server-type=cpx31"]).await;
    rig.pass().await;

    for (id, source, tag, server_type) in [
        ("hj_pool", SpecSource::Pool, None, "cpx31"),
        ("hj_tag", SpecSource::Tag, Some("heavy"), "cx43"),
        ("hj_job", SpecSource::Job, Some("heavy"), "cpx31"),
    ] {
        let lease = lease_of(&rig, id).await;
        assert_eq!(lease.spec.source, source, "{id}");
        assert_eq!(lease.spec.tag.as_deref(), tag, "{id}");
        assert_eq!(
            lease.machine.expect("machine").server_type,
            server_type,
            "{id}"
        );
        let state = rig.state().await;
        let job = job_named(&state, id);
        assert_eq!(job.requirements.machine.as_ref(), Some(&lease.spec), "{id}");
        assert_eq!(job.view(now_ms())["spec"]["source"], json!(source), "{id}");
    }
    let heavy = spec_of(&rig, &[TAG, "heavy"]).await;
    assert_eq!(heavy.want.describe(), "min-vcpu=8,min-ram-gb=16");
    assert_eq!(spec_of(&rig, &[TAG]).await, profile::Spec::default());
}

#[tokio::test]
async fn several_profiled_tags_pick_by_priority_then_size_and_ties_are_refused() {
    let rig = rig();
    rig.add_pool(1).await;
    set(&rig, "heavy", profile(&["cx43"])).await;
    set(&rig, "mid", profile(&["cpx31"])).await;
    let both = [TAG, "mid", "heavy"];
    assert_eq!(
        spec_of(&rig, &both).await.tag.as_deref(),
        Some("heavy"),
        "bigger wins"
    );
    set(
        &rig,
        "mid",
        SetTagProfile {
            priority: 5,
            ..profile(&["cpx31"])
        },
    )
    .await;
    assert_eq!(
        spec_of(&rig, &both).await.tag.as_deref(),
        Some("mid"),
        "priority wins"
    );

    let error = rig
        .service
        .set_tag_profile(("burst", "twin"), profile(&["cx43"]))
        .await
        .expect_err("ties with heavy");
    assert!(error.to_string().contains("would tie"), "{error}");
    let listed = rig.service.list_tag_profiles("burst").await.expect("list");
    assert_eq!(listed["schema"], json!("zeroshot.hub.pool-tags/v1"));
    assert_eq!(
        listed["profiles"].as_array().map(Vec::len),
        Some(2),
        "{listed}"
    );
}

#[tokio::test]
async fn a_small_job_reuses_a_bigger_idle_member_before_creating_a_server() {
    let rig = rig();
    rig.add_pool(2).await;
    set(
        &rig,
        "heavy",
        SetTagProfile {
            slots: Some(2),
            ..profile(&["cx43"])
        },
    )
    .await;
    submit(&rig, "hj_one", &["heavy"]).await;
    for _ in 0..4 {
        rig.pass().await;
    }
    let lease = rig.lease().await;
    assert_eq!(lease.phase, LeasePhase::Active);
    let state = rig.state().await;
    let member = state
        .members
        .iter()
        .find(|member| member.name == lease.name)
        .expect("member");
    assert_eq!(member.slots, 2, "slots from the profile");
    assert_eq!(
        member.spec.as_ref().and_then(|spec| spec.tag.as_deref()),
        Some("heavy")
    );
    assert!(member.tags.iter().any(|tag| tag == "heavy"));
    let metrics = rig.service.render_metrics().await;
    assert!(
        metrics.contains(&format!(
            r#"zeroshot_hub_member_machine_info{{member="{}",server_type="cx43",location="fsn1",source="tag",profile="heavy"}} 1"#,
            lease.name
        )),
        "{metrics}"
    );
    rig.driver.finish_all();
    rig.pass().await;

    rig.enqueue("hj_two").await;
    rig.pass().await;
    let state = rig.state().await;
    assert_eq!(
        job_named(&state, "hj_two").target.as_deref(),
        Some(lease.name.as_str())
    );
    assert_eq!(state.leases.len(), 1, "no new server");
    assert_eq!(rig.cloud.with(|cloud| cloud.created.len()), 1);
}

#[tokio::test]
async fn a_tagged_job_never_lands_on_an_undersized_member() {
    let rig = rig();
    rig.add_pool(2).await;
    rig.enqueue("hj_one").await;
    for _ in 0..4 {
        rig.pass().await;
    }
    let small = rig.lease().await;
    assert_eq!(
        small.machine.as_ref().expect("machine").server_type,
        "cpx31"
    );
    rig.driver.finish_all();
    rig.pass().await;

    set(&rig, "heavy", profile(&["cx43"])).await;
    let state = rig.state().await;
    let member = state
        .members
        .iter()
        .find(|member| member.name == small.name)
        .expect("member");
    assert!(member.tags.iter().any(|tag| tag == "heavy"), "tags synced");
    submit(&rig, "hj_big", &["heavy"]).await;
    rig.pass().await;
    let state = rig.state().await;
    let big = job_named(&state, "hj_big");
    assert_eq!(big.state, JobState::Queued, "not on the cpx31");
    assert_eq!(
        rig.cloud.with(|cloud| cloud.created[1].server_type.clone()),
        "cx43"
    );
    for _ in 0..4 {
        rig.pass().await;
    }
    let state = rig.state().await;
    let target = job_named(&state, "hj_big").target.clone().expect("placed");
    assert_ne!(target, small.name);
    assert_eq!(lease_of(&rig, "hj_big").await.name, target);

    rig.service
        .remove_tag_profile("burst", "heavy")
        .await
        .expect("remove");
    let state = rig.state().await;
    assert!(
        state
            .members
            .iter()
            .all(|member| !member.tags.iter().any(|tag| tag == "heavy"))
    );
    assert!(
        rig.service
            .remove_tag_profile("burst", "heavy")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_tag_falls_back_through_its_own_list_and_waits_alone_when_all_are_full() {
    let rig = rig();
    rig.add_pool(3).await;
    set(
        &rig,
        "heavy",
        SetTagProfile {
            locations: vec!["hel1".to_owned(), "fsn1".to_owned()],
            min_cores: Some(4),
            ..profile(&["cx43", "cpx31"])
        },
    )
    .await;
    rig.cloud.with(|cloud| {
        for place in [("cx43", "hel1"), ("cx43", "fsn1")] {
            cloud
                .refuse
                .insert((place.0.to_owned(), place.1.to_owned()), Refusal::Capacity);
        }
    });
    submit(&rig, "hj_one", &["heavy"]).await;
    rig.pass().await;
    let tried = rig.cloud.with(|cloud| cloud.attempts.clone());
    let expected = [("cx43", "hel1"), ("cx43", "fsn1"), ("cpx31", "hel1")]
        .map(|(server_type, location)| (server_type.to_owned(), location.to_owned()));
    assert_eq!(tried, expected);

    rig.cloud.with(|cloud| {
        for place in [("cpx31", "hel1"), ("cpx31", "fsn1")] {
            cloud
                .refuse
                .insert((place.0.to_owned(), place.1.to_owned()), Refusal::Capacity);
        }
    });
    submit(&rig, "hj_two", &["heavy"]).await;
    rig.enqueue("hj_plain").await;
    let view = rig.pass().await;
    let reason = "waiting: tag heavy needs cx43/cpx31, no capacity in hel1/fsn1";
    assert_eq!(
        view["pools"]["shortages"][0]["reason"],
        json!(reason),
        "{view}"
    );
    let state = rig.state().await;
    let pool = &state.pools[0];
    assert_eq!(pool.shortages.keys().collect::<Vec<_>>(), ["tag:heavy"]);
    let waiting = job_named(&state, "hj_two").pool_wait.clone();
    rig.pass().await;
    let state = rig.state().await;
    let waiting = job_named(&state, "hj_two").pool_wait.clone().or(waiting);
    assert!(waiting.is_some_and(|text| text.starts_with(reason)));
    let plain = lease_of(&rig, "hj_plain").await;
    assert_eq!(
        plain
            .machine
            .expect("plain job still gets a server")
            .location,
        "nbg1"
    );
    let metrics = rig.service.render_metrics().await;
    assert!(
        metrics.contains(
            r#"zeroshot_hub_pool_queued_jobs{pool="burst",source="tag",profile="heavy"} 2"#
        ),
        "{metrics}"
    );

    set(&rig, "heavy", profile(&["cx43"])).await;
    assert!(
        rig.state().await.pools[0].shortages.is_empty(),
        "a new profile retries now"
    );
}

#[tokio::test]
async fn tag_profiles_are_validated_when_set() {
    let rig = rig();
    let missing = rig
        .service
        .set_tag_profile(("burst", "heavy"), profile(&["cx43"]))
        .await
        .expect_err("no pool");
    assert!(missing.to_string().contains("does not exist"), "{missing}");
    rig.add_pool(1).await;
    let cases: [(&str, &str, SetTagProfile); 7] = [
        ("min-vcpu=4", "size request", profile(&["cx43"])),
        ("two words", "invalid tag", profile(&["cx43"])),
        (
            "heavy",
            "smaller than tag heavy needs",
            SetTagProfile {
                min_cores: Some(8),
                ..profile(&["cpx31"])
            },
        ),
        (
            "heavy",
            "at least 1",
            SetTagProfile {
                slots: Some(0),
                ..profile(&["cx43"])
            },
        ),
        (
            "heavy",
            "none of the pool's types",
            SetTagProfile {
                min_memory_gb: Some(64),
                ..SetTagProfile::default()
            },
        ),
        ("heavy", "ARM", profile(&["cax21"])),
        ("heavy", "unknown server type", profile(&["cx99"])),
    ];
    for (tag, message, request) in cases {
        let error = rig
            .service
            .set_tag_profile(("burst", tag), request)
            .await
            .expect_err(message);
        assert!(error.to_string().contains(message), "{tag}: {error}");
    }
    let derived = rig
        .service
        .set_tag_profile(
            ("burst", "heavy"),
            SetTagProfile {
                min_cores: Some(8),
                ..SetTagProfile::default()
            },
        )
        .await
        .expect("pool types filtered");
    assert_eq!(
        derived["serverTypes"][0]["name"],
        json!("cx43"),
        "{derived}"
    );
    assert_eq!(derived["locations"], json!(["fsn1", "nbg1"]));
    assert!(rig.service.list_tag_profiles("absent").await.is_err());
    assert!(
        rig.service
            .remove_tag_profile("absent", "heavy")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_job_with_only_a_profiled_tag_is_served_by_the_pool() {
    let rig = rig();
    rig.add_pool(1).await;
    set(&rig, "heavy", profile(&["cx43"])).await;
    let mut queued = tagged("hj_one");
    queued.placement = spec(None, &["heavy"], Strategy::FirstFree);
    rig.service
        .enqueue(queued, Vec::new())
        .await
        .expect("served");
    rig.pass().await;
    assert_eq!(
        lease_of(&rig, "hj_one").await.spec.tag.as_deref(),
        Some("heavy")
    );
}
