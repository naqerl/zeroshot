use std::collections::BTreeMap;

use openengine_cluster_protocol::{GIT_TRANSPORT_KIND, NODE_RUNTIME_LANES_KIND};
use serde_json::json;
use zeroshot_engine::native_v2_cli::hub::{HubJobSpec, HubStrategy};

use super::super::driver::discovery_capabilities;
use super::super::error::{EXIT_NO_ELIGIBLE_TARGET, HubError};
use super::super::jobs::{SourceFacts, requirements, runtime_has_lanes};
use super::super::model::{HubState, JobState, Requirements, Strategy};
use super::super::placement::{Fit, Placement, Rejection, fit, place, rejections};
use super::super::schedule::{discover, tick};
use super::{FakeDriver, job, loads, member, spec};

const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

fn job_spec() -> HubJobSpec {
    HubJobSpec {
        title: "t".to_owned(),
        run_args: Vec::new(),
        target: None,
        tags: Vec::new(),
        strategy: HubStrategy::FirstFree,
        labels: Vec::new(),
        runtime_files: Vec::new(),
        repository: None,
        branch: None,
        revision: None,
        source_connection: None,
        environment: false,
        cwd: None,
        submission_key: None,
    }
}

const PUSHED: SourceFacts<'static> = SourceFacts {
    pinned_revision: Some(REVISION),
    unpushed: None,
    lanes: false,
};

fn lanes() -> Requirements {
    Requirements {
        capabilities: vec![NODE_RUNTIME_LANES_KIND.to_owned()],
        ..Requirements::default()
    }
}

#[test]
fn requirements_mirror_run_admission_rules() {
    assert_eq!(requirements(&job_spec(), &PUSHED), Requirements::default());

    let explicit = HubJobSpec {
        repository: Some("naqerl/zeroshot".to_owned()),
        ..job_spec()
    };
    let explicit = requirements(
        &explicit,
        &SourceFacts {
            pinned_revision: None,
            ..PUSHED
        },
    );
    assert!(explicit.local_unsupported.is_some());
    assert!(explicit.remote_unsupported.is_none());
    assert!(
        explicit.capabilities.is_empty(),
        "GitHub owner/name needs no generic transport"
    );

    let hooks = HubJobSpec {
        environment: true,
        ..job_spec()
    };
    assert!(
        requirements(&hooks, &PUSHED)
            .local_unsupported
            .is_some_and(|reason| reason.contains("hooks"))
    );

    let no_git = SourceFacts {
        pinned_revision: None,
        ..PUSHED
    };
    assert!(
        requirements(&job_spec(), &no_git)
            .remote_unsupported
            .is_some_and(|reason| reason.contains("no committed Git revision"))
    );
    let unpushed = SourceFacts {
        unpushed: Some(REVISION),
        ..PUSHED
    };
    assert!(
        requirements(&job_spec(), &unpushed)
            .remote_unsupported
            .is_some_and(
                |reason| reason.contains("HEAD 0123456789ab is on no remote-tracking branch")
            )
    );

    for generic in [
        HubJobSpec {
            source_connection: Some("git".to_owned()),
            ..job_spec()
        },
        HubJobSpec {
            repository: Some("https://git.example/team/app.git".to_owned()),
            ..job_spec()
        },
        HubJobSpec {
            revision: Some("a".repeat(64)),
            ..job_spec()
        },
    ] {
        assert_eq!(
            requirements(&generic, &PUSHED).capabilities,
            [GIT_TRANSPORT_KIND],
            "{generic:?}"
        );
    }
    let laned = SourceFacts {
        lanes: true,
        ..PUSHED
    };
    assert_eq!(
        requirements(&job_spec(), &laned).capabilities,
        [NODE_RUNTIME_LANES_KIND]
    );
}

#[test]
fn runtime_lanes_are_detected_from_node_bindings() {
    let directory = tempfile::tempdir().expect("tempdir");
    let laned = directory.path().join("laned.json");
    std::fs::write(
        &laned,
        r#"{"nodes":{"worker":{"lane":{"harness":"claude","provider":"anthropic"}},"r":{}}}"#,
    )
    .expect("runtime");
    let plain = directory.path().join("plain.json");
    std::fs::write(&plain, r#"{"nodes":{"worker":{"model":"m1","lane":null}}}"#).expect("runtime");
    assert!(runtime_has_lanes(&laned));
    assert!(!runtime_has_lanes(&plain));
    assert!(!runtime_has_lanes(&directory.path().join("missing.json")));
}

#[test]
fn discovery_capabilities_collect_every_advertised_kind() {
    let document = json!({
        "kind": "openengine.native-v2-target/v1",
        "authentication": {"kind": "bootstrap"},
        "extensions": {
            "node_runtime_lanes": {"kind": NODE_RUNTIME_LANES_KIND},
            "git_transport": {"kind": GIT_TRANSPORT_KIND},
            "run_history": {"kind": "openengine.run-history/v1", "pageLimit": 50}
        }
    });
    assert_eq!(
        discovery_capabilities(&document),
        [
            GIT_TRANSPORT_KIND,
            "openengine.native-v2-target/v1",
            NODE_RUNTIME_LANES_KIND,
            "openengine.run-history/v1"
        ]
    );
    assert!(discovery_capabilities(&json!({})).is_empty());
}

#[test]
fn fit_separates_supported_unknown_and_unsupported_members() {
    let local = member("local", &[], 1, 0);
    let mut modern = member("modern", &[], 1, 0);
    modern.capabilities = Some(vec![NODE_RUNTIME_LANES_KIND.to_owned()]);
    let mut legacy = member("legacy", &[], 1, 0);
    legacy.capabilities = Some(Vec::new());
    let undiscovered = member("new", &[], 1, 0);

    assert_eq!(
        fit(&local, &lanes()),
        Fit::Supported,
        "local is not discovery gated"
    );
    assert_eq!(fit(&modern, &lanes()), Fit::Supported);
    assert_eq!(fit(&undiscovered, &lanes()), Fit::Unknown);
    assert_eq!(
        fit(&legacy, &lanes()),
        Fit::Unsupported(format!(
            "target does not advertise {NODE_RUNTIME_LANES_KIND}"
        ))
    );
    let local_only = Requirements {
        remote_unsupported: Some("no git".to_owned()),
        ..Requirements::default()
    };
    assert_eq!(fit(&local, &local_only), Fit::Supported);
    assert_eq!(
        fit(&modern, &local_only),
        Fit::Unsupported("no git".to_owned())
    );
}

#[test]
fn placement_skips_unsupported_members_and_reports_when_none_supports_the_run() {
    let mut legacy = member("legacy", &["linux"], 4, 9);
    legacy.capabilities = Some(Vec::new());
    let mut modern = member("modern", &["linux"], 1, 0);
    modern.capabilities = Some(vec![NODE_RUNTIME_LANES_KIND.to_owned()]);
    let members = [legacy.clone(), modern];
    let linux = spec(None, &["linux"], Strategy::FirstFree);
    let free = loads(&[("legacy", true, 0), ("modern", true, 0)]);
    assert_eq!(
        place(&members, &linux, &lanes(), &free),
        Placement::Place("modern".to_owned()),
        "the higher-priority member cannot run per-node lanes"
    );
    let busy = loads(&[("legacy", true, 0), ("modern", true, 1)]);
    assert_eq!(
        place(&members, &linux, &lanes(), &busy),
        Placement::Busy {
            matching: vec!["modern".to_owned()]
        }
    );
    let only_legacy = [legacy];
    let expected = vec![Rejection {
        member: "legacy".to_owned(),
        reason: format!("target does not advertise {NODE_RUNTIME_LANES_KIND}"),
    }];
    assert_eq!(
        place(&only_legacy, &linux, &lanes(), &free),
        Placement::NoEligible {
            rejected: expected.clone()
        }
    );
    assert_eq!(
        rejections(&only_legacy, &linux, &lanes()),
        Some(expected.clone())
    );
    let undiscovered = [member("new", &["linux"], 1, 0)];
    assert_eq!(
        rejections(&undiscovered, &linux, &lanes()),
        None,
        "an undiscovered member may still support the run"
    );

    let error = HubError::no_eligible("tags [linux]".to_owned(), expected);
    assert_eq!(error.exit_code(), EXIT_NO_ELIGIBLE_TARGET);
    assert!(
        error
            .to_string()
            .contains("legacy: target does not advertise")
    );
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic["code"], json!("hub.no_eligible_target"));
    assert_eq!(
        diagnostic["details"]["rejected"][0]["member"],
        json!("legacy")
    );
}

#[tokio::test]
async fn discovery_refreshes_stale_members_and_keeps_known_capabilities_on_error() {
    let driver = FakeDriver {
        capabilities: BTreeMap::from([("a".to_owned(), vec![GIT_TRANSPORT_KIND.to_owned()])]),
        ..FakeDriver::default()
    };
    let mut known = member("b", &[], 1, 0);
    known.capabilities = Some(vec!["old".to_owned()]);
    known.capabilities_at_ms = Some(0);
    let mut members = vec![member("local", &[], 1, 0), member("a", &[], 1, 0), known];
    discover(&mut members, &driver, 10, 5).await;
    assert_eq!(members[0].capabilities, None, "local is never discovered");
    assert_eq!(
        members[1].capabilities.as_deref(),
        Some(&[GIT_TRANSPORT_KIND.to_owned()][..])
    );
    assert_eq!(members[1].capabilities_at_ms, Some(10));
    assert_eq!(
        members[2].capabilities.as_deref(),
        Some(&["old".to_owned()][..])
    );
    assert!(members[2].capability_error.is_some());
    assert_eq!(members[2].capability_failures, 1);
    assert_eq!(
        members[2].capability_retry_at_ms,
        Some(10 + 60_000),
        "a failed discovery backs off instead of adding its timeout to every pass"
    );
    members[1].capabilities = None;
    discover(&mut members, &driver, 12, 5).await;
    assert_eq!(
        members[1].capabilities, None,
        "fresh entries are not refetched"
    );
    assert_eq!(
        members[2].capability_failures, 1,
        "a backed-off member is not rediscovered before its retry time"
    );
}

#[tokio::test]
async fn tick_places_capability_bound_jobs_only_on_supporting_members() {
    let driver = FakeDriver {
        capabilities: BTreeMap::from([
            ("legacy".to_owned(), Vec::new()),
            (
                "modern".to_owned(),
                vec![NODE_RUNTIME_LANES_KIND.to_owned()],
            ),
        ]),
        ..FakeDriver::default()
    };
    let mut laned = job("laned", 0);
    laned.requirements = lanes();
    let mut state = HubState {
        members: vec![member("legacy", &[], 1, 5), member("modern", &[], 1, 0)],
        jobs: vec![laned, job("plain", 1)],
        ..HubState::default()
    };
    let report = tick(&mut state, &driver, 100).await;
    assert_eq!(report.dispatched.len(), 2);
    assert_eq!(state.jobs[0].target.as_deref(), Some("modern"));
    assert_eq!(state.jobs[1].target.as_deref(), Some("legacy"));
    assert!(state.jobs.iter().all(|job| job.state == JobState::Running));
}
