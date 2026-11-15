//! Evals-v2 tasks, arms, runs, trials, bundles, and coexistence with the older eval subsystem.

use std::sync::Arc;

use serde_json::{Value, json};

use super::super::eval::{AddCase, CaptureSource, CreateRun};
use super::super::evals::task::{BaselineMetrics, Cost};
use super::super::evals::{CreateArm, CreateEvalRun, CreateTask};
use super::super::evals::{canonical, sha256_hex, short_hash, trial_id, trial_job_id};
use super::super::service::HubService;
use super::super::store::{HubStore, migrate};
use super::FakeDriver;
use crate::native_v2_target::FileTargetRegistry;

fn task(title: &str) -> CreateTask {
    CreateTask {
        title: title.to_owned(),
        prompt: format!("implement {title}"),
        baseline_commits: vec!["a".repeat(40)],
        pinned_revision: "b".repeat(40),
        baseline: BaselineMetrics {
            cost_usd: Cost(0.25),
            duration_ms: 1_000,
        },
    }
}

fn arm(name: &str, graph_ref: &str) -> CreateArm {
    CreateArm {
        name: name.to_owned(),
        graph_ref: graph_ref.to_owned(),
        harness_patch: Some(json!({
            "harness": "codex",
            "provider": "openai",
            "size": "small",
            "effort": "high",
        })),
        base_revision: "c".repeat(40),
        model_ids: vec!["model-a".to_owned(), "model-b".to_owned()],
        node_budgets: Some(json!({"worker": 600_000})),
    }
}

fn run(tasks: &[&str], arms: &[&str], repeat: u32) -> CreateEvalRun {
    CreateEvalRun {
        tasks: tasks.iter().map(|task| (*task).to_owned()).collect(),
        arms: arms.iter().map(|arm| (*arm).to_owned()).collect(),
        repeat,
        judge: None,
    }
}

async fn service() -> (tempfile::TempDir, HubService, Arc<FakeDriver>) {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = Arc::new(FakeDriver::default());
    let service = HubService::open(
        HubStore::new(directory.path().join("hub")),
        driver.clone(),
        FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    service
        .mutate(|state| {
            state.members.push(super::member("box", &[], 1, 0));
            Ok(())
        })
        .await
        .expect("member");
    (directory, service, driver)
}

#[tokio::test]
async fn tasks_arms_and_runs_are_deterministic_and_create_exactly_the_cartesian_trials() {
    let (_directory, service, _driver) = service().await;
    let first = service.evals_task_create(task("one")).await.expect("task");
    let again = service
        .evals_task_create(task("one"))
        .await
        .expect("task again");
    assert_eq!(first["taskId"], again["taskId"]);
    assert_eq!(first["taskHash"], again["taskHash"]);
    assert_eq!(first["created"], json!(true));
    assert_eq!(again["created"], json!(false));
    assert_eq!(
        first["taskId"].as_str().expect("id").len(),
        "ev-".len() + 12
    );

    let first_arm = service
        .evals_arm_create(arm("a", "graph.json"))
        .await
        .expect("arm");
    let again_arm = service
        .evals_arm_create(arm("a", "graph.json"))
        .await
        .expect("arm again");
    assert_eq!(first_arm["armId"], again_arm["armId"]);
    assert_eq!(first_arm["armHash"], again_arm["armHash"]);
    assert_eq!(again_arm["created"], json!(false));

    let second_arm = service
        .evals_arm_create(arm("b", "other.json"))
        .await
        .expect("second arm");
    let created = service
        .evals_run_create(run(
            &[first["taskId"].as_str().expect("task id")],
            &[
                first_arm["armId"].as_str().expect("arm id"),
                second_arm["armId"].as_str().expect("arm id"),
            ],
            2,
        ))
        .await
        .expect("run");
    assert_eq!(created["trialIds"].as_array().map(Vec::len), Some(4));
    let run_id = created["runId"].as_str().expect("run id").to_owned();
    let repeated = service
        .evals_run_create(run(
            &[first["taskId"].as_str().expect("task id")],
            &[
                first_arm["armId"].as_str().expect("arm id"),
                second_arm["armId"].as_str().expect("arm id"),
            ],
            2,
        ))
        .await
        .expect("same run");
    assert_eq!(repeated["trialIds"], created["trialIds"]);

    let state = service.snapshot().await;
    assert_eq!(state.evals_v2.tasks.len(), 1);
    assert_eq!(state.evals_v2.arms.len(), 2);
    assert_eq!(state.evals_v2.runs.len(), 1);
    assert_eq!(state.evals_v2.trials.len(), 4);
    assert!(
        state
            .evals_v2
            .trials
            .iter()
            .all(|trial| trial.job_id.is_some())
    );

    let document = service.evals_run(&run_id).await.expect("get run");
    assert_eq!(document["run"]["runId"], json!(run_id));
    assert_eq!(document["trials"].as_array().map(Vec::len), Some(4));
    assert_eq!(document["run"]["trials"].as_array().map(Vec::len), Some(4));
}

#[tokio::test]
async fn a_trial_without_a_member_is_unplaceable_instead_of_hanging() {
    let directory = tempfile::tempdir().expect("tempdir");
    let service = HubService::open(
        HubStore::new(directory.path().join("hub")),
        Arc::new(FakeDriver::default()),
        FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    let task = service.evals_task_create(task("one")).await.expect("task");
    let arm = service
        .evals_arm_create(arm("a", "graph.json"))
        .await
        .expect("arm");
    let created = service
        .evals_run_create(run(
            &[task["taskId"].as_str().expect("task id")],
            &[arm["armId"].as_str().expect("arm id")],
            1,
        ))
        .await
        .expect("run");
    let run_id = created["runId"].as_str().expect("run id");
    let trial = service.snapshot().await.evals_v2.trials[0].clone();
    assert!(trial.bundle_written);
    let bundle_path = trial.bundle_path.expect("unplaceable bundle");
    assert!(bundle_path.is_file(), "{}", bundle_path.display());
    let bundle: Value =
        serde_json::from_slice(&std::fs::read(&bundle_path).expect("bundle")).expect("json");
    assert_eq!(bundle["failureKind"], json!("unplaceable"));
    let document = service.evals_run(run_id).await.expect("get run");
    assert_eq!(document["trials"][0]["state"], json!("failed"));
    assert_eq!(document["trials"][0]["failureKind"], json!("unplaceable"));
}

#[tokio::test]
async fn a_terminal_trial_writes_a_bundle_and_a_missing_bundle_is_failed() {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = Arc::new(FakeDriver {
        output: Some(json!({
            "diff": "diff --git a/x b/x",
            "checks": [{"name": "cargo test", "passed": true}],
        })),
        ..FakeDriver::default()
    });
    let service = HubService::open(
        HubStore::new(directory.path().join("hub")),
        driver.clone(),
        FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    service
        .mutate(|state| {
            state.members.push(super::member("box", &[], 1, 0));
            Ok(())
        })
        .await
        .expect("member");
    let task = service.evals_task_create(task("one")).await.expect("task");
    let arm = service
        .evals_arm_create(arm("a", "graph.json"))
        .await
        .expect("arm");
    let created = service
        .evals_run_create(run(
            &[task["taskId"].as_str().expect("task id")],
            &[arm["armId"].as_str().expect("arm id")],
            1,
        ))
        .await
        .expect("run");
    let run_id = created["runId"].as_str().expect("run id").to_owned();
    let job_id = service.snapshot().await.evals_v2.trials[0]
        .job_id
        .clone()
        .expect("job id");
    service
        .mutate(|state| {
            let job = state
                .jobs
                .iter_mut()
                .find(|job| job.job_id == job_id)
                .expect("job");
            job.state = super::super::model::JobState::Succeeded;
            job.target = Some("box".to_owned());
            job.run_id = Some("run-bundle".to_owned());
            job.dispatched_at_ms = Some(job.submitted_at_ms + 10);
            job.finished_at_ms = Some(job.submitted_at_ms + 110);
            job.output = driver.output.clone();
            Ok(())
        })
        .await
        .expect("finish job");
    service.evals_reconcile().await.expect("reconcile");
    let trial = service.snapshot().await.evals_v2.trials[0].clone();
    let bundle_path = trial.bundle_path.clone().expect("bundle path");
    assert!(bundle_path.is_file(), "{}", bundle_path.display());
    let bundle: Value =
        serde_json::from_slice(&std::fs::read(&bundle_path).expect("bundle")).expect("json");
    assert_eq!(bundle["trialId"], json!(trial.trial_id));
    assert_eq!(bundle["runId"], json!(run_id));
    assert_eq!(bundle["taskHash"], task["taskHash"]);
    assert_eq!(bundle["armHash"], arm["armHash"]);
    assert_eq!(bundle["seed"], json!(0));
    assert_eq!(bundle["pinnedRevision"], json!("b".repeat(40)));
    assert_eq!(bundle["modelIds"], json!(["model-a", "model-b"]));
    assert_eq!(bundle["diff"], json!("diff --git a/x b/x"));
    assert_eq!(bundle["checks"][0]["name"], json!("cargo test"));
    assert!(bundle["sessionLogPaths"].is_array());
    assert_eq!(bundle["sessionLogPaths"], bundle["sessionLogs"]);

    std::fs::remove_file(&bundle_path).expect("remove bundle");
    service.evals_reconcile().await.expect("reconcile deletion");
    assert!(!bundle_path.exists(), "a written bundle is never recreated");
    let document = service.evals_run(&run_id).await.expect("get run");
    assert_eq!(document["trials"][0]["state"], json!("failed"));
    assert_eq!(
        document["trials"][0]["failureKind"],
        json!("bundle_missing")
    );
    assert!(!bundle_path.exists(), "GET never recreates a bundle");
}

#[tokio::test]
async fn a_successful_job_without_diff_is_not_a_successful_trial() {
    let (_directory, service, _driver) = service().await;
    let task = service.evals_task_create(task("one")).await.expect("task");
    let arm = service
        .evals_arm_create(arm("a", "graph.json"))
        .await
        .expect("arm");
    let created = service
        .evals_run_create(run(
            &[task["taskId"].as_str().expect("task id")],
            &[arm["armId"].as_str().expect("arm id")],
            1,
        ))
        .await
        .expect("run");
    let run_id = created["runId"].as_str().expect("run id").to_owned();
    let job_id = service.snapshot().await.evals_v2.trials[0]
        .job_id
        .clone()
        .expect("job id");
    service
        .mutate(|state| {
            let job = state
                .jobs
                .iter_mut()
                .find(|job| job.job_id == job_id)
                .expect("job");
            job.state = super::super::model::JobState::Succeeded;
            job.target = Some("box".to_owned());
            job.run_id = Some("run-no-diff".to_owned());
            job.finished_at_ms = Some(job.submitted_at_ms + 1);
            Ok(())
        })
        .await
        .expect("finish job");
    service.evals_reconcile().await.expect("reconcile");
    let trial = service.snapshot().await.evals_v2.trials[0].clone();
    assert!(trial.bundle_written);
    assert!(
        trial
            .bundle_path
            .as_ref()
            .is_some_and(|path| path.is_file())
    );
    let document = service.evals_run(&run_id).await.expect("get run");
    assert_eq!(document["trials"][0]["state"], json!("failed"));
    assert_eq!(
        document["trials"][0]["failureKind"],
        json!("diff_unavailable")
    );
}

#[tokio::test]
async fn a_mid_run_enqueue_failure_leaves_no_orphan_job() {
    let directory = tempfile::tempdir().expect("tempdir");
    let service = HubService::open(
        HubStore::new(directory.path().join("hub")),
        Arc::new(FakeDriver::default()),
        FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    service
        .mutate(|state| {
            state.members.push(super::member("box", &[], 1, 0));
            Ok(())
        })
        .await
        .expect("member");
    let task = service.evals_task_create(task("one")).await.expect("task");
    let first_arm = service
        .evals_arm_create(arm("a", "graph.json"))
        .await
        .expect("arm");
    let second_arm = service
        .evals_arm_create(arm("b", "other.json"))
        .await
        .expect("arm");
    let request = run(
        &[task["taskId"].as_str().expect("task id")],
        &[
            first_arm["armId"].as_str().expect("arm id"),
            second_arm["armId"].as_str().expect("arm id"),
        ],
        1,
    );
    let request_value = serde_json::to_value(&request).expect("request json");
    let run_id = format!(
        "er-{}",
        short_hash(&sha256_hex(canonical(&request_value).as_bytes()))
    );
    let blocked_trial = trial_id(
        &run_id,
        task["taskHash"].as_str().expect("task hash"),
        second_arm["armHash"].as_str().expect("arm hash"),
        0,
    );
    let blocked_job = trial_job_id(&blocked_trial);
    let spool = directory.path().join("hub").join("spool");
    std::fs::create_dir_all(&spool).expect("spool");
    std::fs::write(spool.join(blocked_job), b"blocked").expect("blocked slot");

    let created = service.evals_run_create(request).await.expect("run");
    assert_eq!(created["trialIds"].as_array().map(Vec::len), Some(2));
    let state = service.snapshot().await;
    assert_eq!(state.evals_v2.runs.len(), 1);
    assert_eq!(state.evals_v2.trials.len(), 2);
    let failed = state
        .evals_v2
        .trials
        .iter()
        .find(|trial| trial.failure_kind.as_deref() == Some("enqueue_failed"))
        .expect("failed trial");
    assert!(failed.job_id.is_none());
    assert!(
        failed
            .bundle_path
            .as_ref()
            .is_some_and(|path| path.is_file())
    );
    for job in &state.jobs {
        if job.job_id.starts_with("hj_") {
            assert!(
                state
                    .evals_v2
                    .trials
                    .iter()
                    .any(|trial| trial.job_id.as_ref() == Some(&job.job_id)),
                "orphan job {}",
                job.job_id
            );
        }
    }
}

#[tokio::test]
async fn a_trial_submits_its_revision_once_and_requires_a_named_target() {
    let (_directory, service, _driver) = service().await;
    let task = service.evals_task_create(task("one")).await.expect("task");
    let arm = service
        .evals_arm_create(arm("a", "graph.json"))
        .await
        .expect("arm");
    service
        .evals_run_create(run(
            &[task["taskId"].as_str().expect("task id")],
            &[arm["armId"].as_str().expect("arm id")],
            1,
        ))
        .await
        .expect("run");
    let state = service.snapshot().await;
    let trial = &state.evals_v2.trials[0];
    let job = state
        .jobs
        .iter()
        .find(|job| Some(&job.job_id) == trial.job_id.as_ref())
        .expect("job");
    assert_eq!(
        job.run_args
            .iter()
            .filter(|argument| argument.as_str() == "--revision")
            .count(),
        1
    );
    assert!(job.pinned_revision.is_none());
    assert!(job.requirements.local_unsupported.is_some());
}

#[test]
fn a_version_two_state_without_evals_v2_still_loads() {
    let state = migrate(br#"{"version":2,"members":[],"jobs":[]}"#).expect("v2 loads");
    assert_eq!(state.version, super::super::model::STATE_VERSION);
    assert!(state.evals_v2.is_empty());
}

#[tokio::test]
async fn old_eval_and_evals_v2_runs_coexist_in_one_state_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    let service = HubService::open(
        store.clone(),
        Arc::new(FakeDriver::default()),
        FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    service
        .mutate(|state| {
            state.members.push(super::member("box", &[], 1, 0));
            Ok(())
        })
        .await
        .expect("member");
    let mut source = super::job("hj_old_source", 0);
    source.cwd = directory.path().to_path_buf();
    source.run_args = vec!["--title".to_owned(), "old".to_owned()];
    let case = service
        .eval_case_add(AddCase {
            suites: Vec::new(),
            name: Some("old".to_owned()),
            labels: Vec::new(),
            source: CaptureSource::Flags {
                job: Box::new(source),
                files: Vec::new(),
            },
        })
        .await
        .expect("old case");
    let case_id = case["case"]["caseId"].as_str().expect("case id").to_owned();
    let old = service
        .eval_run_create(CreateRun {
            cases: vec![case_id],
            repeat: 1,
            ..CreateRun::default()
        })
        .await
        .expect("old run");
    let old_id = old["run"]["runId"].as_str().expect("old run id").to_owned();

    let task = service.evals_task_create(task("new")).await.expect("task");
    let arm = service
        .evals_arm_create(arm("a", "graph.json"))
        .await
        .expect("arm");
    let new = service
        .evals_run_create(run(
            &[task["taskId"].as_str().expect("task id")],
            &[arm["armId"].as_str().expect("arm id")],
            1,
        ))
        .await
        .expect("new run");
    let new_id = new["runId"].as_str().expect("new run id").to_owned();

    let stored: Value = serde_json::from_slice(
        &std::fs::read(directory.path().join("hub").join("state.json")).expect("state"),
    )
    .expect("json");
    assert!(stored.get("evals").is_some());
    assert!(stored.get("evalsV2").is_some());

    drop(service);
    let reopened = HubService::open(
        store,
        Arc::new(FakeDriver::default()),
        FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("reopen");
    let old_document = reopened.eval_run(&old_id).await.expect("old run readable");
    assert_eq!(old_document["run"]["runId"], json!(old_id));
    let new_document = reopened.evals_run(&new_id).await.expect("new run readable");
    assert_eq!(new_document["run"]["runId"], json!(new_id));
    assert_eq!(new_document["trials"].as_array().map(Vec::len), Some(1));
}
