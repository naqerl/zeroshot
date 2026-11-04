//! `hub stop`: dequeue, cancel a running run with retained evidence, free the member, and
//! refuse a job that already finished.

use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::super::model::{JobState, Strategy};
use super::super::service::{HubService, STOP_LOG_TAIL_LINES};
use super::super::store::HubStore;
use super::{FakeDriver, job, member, spec};
use crate::native_v2_target::FileTargetRegistry;

fn service(driver: Arc<FakeDriver>, root: &Path) -> HubService {
    HubService::open(
        HubStore::new(root.join("hub")),
        driver,
        FileTargetRegistry::new(root.join("targets.json")),
    )
    .expect("service")
}

#[tokio::test]
async fn stopping_a_queued_job_dequeues_it() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = Arc::new(FakeDriver::default());
    let service = service(driver, directory.path());
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.jobs.push(job("hj_queued", 0));
            Ok(())
        })
        .await
        .expect("seed");

    let stopped = service
        .stop("hj_queued", Some("not needed".to_owned()))
        .await
        .expect("stop");
    assert_eq!(stopped["job"]["state"], json!("dequeued"));
    assert_eq!(service.snapshot().await.jobs[0].state, JobState::Dequeued);
}

#[tokio::test]
async fn stopping_a_running_job_cancels_it_retains_evidence_and_frees_the_member() {
    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    std::fs::write(workspace.join("transcript.txt"), "kept").expect("workspace file");
    let logs = (0..300)
        .map(|line| format!("line {line}"))
        .collect::<Vec<_>>();
    let driver = Arc::new(FakeDriver {
        logs,
        ..FakeDriver::default()
    });
    let service = service(driver.clone(), directory.path());
    let mut running = job("hj_run", 0);
    running.state = JobState::Running;
    running.target = Some("box".to_owned());
    running.run_id = Some("run-stop".to_owned());
    running.cwd = workspace.clone();
    running.submitted_at_ms = 0;
    let mut next = job("hj_next", 1);
    next.placement = spec(Some("box"), &[], Strategy::FirstFree);
    service
        .mutate(|state| {
            state.members.push(member("box", &[], 1, 0));
            state.jobs.push(running);
            state.jobs.push(next);
            Ok(())
        })
        .await
        .expect("seed");

    let stopped = service
        .stop("hj_run", Some("runaway".to_owned()))
        .await
        .expect("stop");
    assert_eq!(stopped["job"]["state"], json!("cancelled"));
    assert_eq!(stopped["job"]["errorKind"], json!("cancelled"));
    assert_eq!(stopped["job"]["failureReason"], json!("cancelled"));
    assert_eq!(stopped["job"]["stopReason"], json!("runaway"));
    assert!(stopped["job"]["stoppedAt"].is_string());
    assert_eq!(
        driver.stops.lock().expect("fake lock").as_slice(),
        &[("box".to_owned(), "run-stop".to_owned())]
    );

    let artifacts = &stopped["job"]["artifacts"];
    assert_eq!(
        artifacts["workspace"].as_str().map(Path::new),
        Some(workspace.as_path())
    );
    assert!(workspace.join("transcript.txt").exists(), "workspace kept");
    let logs_path = artifacts["logs"].as_str().expect("captured log tail");
    let text = std::fs::read_to_string(logs_path).expect("log tail");
    assert_eq!(text.lines().count(), STOP_LOG_TAIL_LINES);
    assert!(text.contains("line 299"), "the newest lines are kept");
    assert!(
        !text.contains("line 99\n"),
        "only the bounded tail is written"
    );

    let (_, view) = service.pass().await.expect("pass");
    let state = service.snapshot().await;
    let next = state
        .jobs
        .iter()
        .find(|job| job.job_id == "hj_next")
        .expect("next job");
    assert_eq!(next.state, JobState::Running);
    assert_eq!(next.target.as_deref(), Some("box"));
    assert!(view["dispatched"].is_array());
}

#[tokio::test]
async fn stopping_a_finished_job_is_a_conflict_with_a_clear_message() {
    let directory = tempfile::tempdir().expect("tempdir");
    let service = service(Arc::new(FakeDriver::default()), directory.path());
    let mut finished = job("hj_done", 0);
    finished.state = JobState::Succeeded;
    service
        .mutate(|state| {
            state.jobs.push(finished);
            Ok(())
        })
        .await
        .expect("seed");

    let error = service
        .stop("hj_done", None)
        .await
        .expect_err("a finished job cannot be stopped");
    assert_eq!(error.http_status(), 409);
    assert!(
        error.to_string().contains("already finished"),
        "the message names the finished job: {error}"
    );
    assert!(
        !error.to_string().contains("panicked"),
        "no stack trace in the public message: {error}"
    );
}
