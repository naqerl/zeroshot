//! Evals-v2 tasks, arms, runs, trials, bundles, and coexistence with the older eval subsystem.

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{Value, json};

use super::super::error::HubError;
use super::super::eval::{AddCase, CaptureSource, CreateRun};
use super::super::evals::Trial;
use super::super::evals::bundle::{self, Bundle};
use super::super::evals::report::{EvalsReportRequest, LossParams};
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

// --- slice 2: report-time loss and the paired per-task report ---

/// One created evals-v2 run with its stored arms and trials.
struct Fixture {
    directory: tempfile::TempDir,
    service: HubService,
    run_id: String,
    hub: PathBuf,
    arms: Vec<(String, String)>,
    trials: Vec<Trial>,
}

async fn fixture(repeat: u32, arms: &[(&str, &str)]) -> Fixture {
    let (directory, service, _driver) = service().await;
    let task = service.evals_task_create(task("one")).await.expect("task");
    let mut created_arms = Vec::new();
    for (name, graph) in arms {
        let created = service
            .evals_arm_create(arm(name, graph))
            .await
            .expect("arm");
        created_arms.push((
            (*name).to_owned(),
            created["armId"].as_str().expect("arm id").to_owned(),
        ));
    }
    let arm_ids = created_arms
        .iter()
        .map(|(_, arm_id)| arm_id.as_str())
        .collect::<Vec<_>>();
    let created = service
        .evals_run_create(run(
            &[task["taskId"].as_str().expect("task id")],
            &arm_ids,
            repeat,
        ))
        .await
        .expect("run");
    let run_id = created["runId"].as_str().expect("run id").to_owned();
    let trials = service
        .snapshot()
        .await
        .evals_v2
        .trials
        .iter()
        .filter(|trial| trial.run_id == run_id)
        .cloned()
        .collect::<Vec<_>>();
    let hub = directory.path().join("hub");
    Fixture {
        directory,
        service,
        run_id,
        hub,
        arms: created_arms,
        trials,
    }
}

/// The measurement written into a trial bundle by a report test.
struct Measured {
    quality: Option<f64>,
    cost_usd: Option<f64>,
    duration_ms: u64,
}

impl Measured {
    const fn graded(quality: f64, cost_usd: f64, duration_ms: u64) -> Self {
        Self {
            quality: Some(quality),
            cost_usd: Some(cost_usd),
            duration_ms,
        }
    }

    const fn ungraded(cost_usd: f64, duration_ms: u64) -> Self {
        Self {
            quality: None,
            cost_usd: Some(cost_usd),
            duration_ms,
        }
    }
}

impl Fixture {
    fn arm_id(&self, name: &str) -> String {
        self.arms
            .iter()
            .find(|(arm_name, _)| arm_name == name)
            .map(|(_, arm_id)| arm_id.clone())
            .unwrap_or_else(|| panic!("arm {name} does not exist"))
    }

    /// Write a bundle for every trial of `name` with the given measurement.
    fn measure(&self, name: &str, measured: Measured) {
        let arm_id = self.arm_id(name);
        for trial in self.trials.iter().filter(|trial| trial.arm_id == arm_id) {
            let mut checks = serde_json::Map::new();
            if let Some(quality) = measured.quality {
                checks.insert("quality".to_owned(), json!(quality));
            }
            if let Some(cost_usd) = measured.cost_usd {
                checks.insert("costUsd".to_owned(), json!(cost_usd));
            }
            let bundle = Bundle {
                trial_id: trial.trial_id.clone(),
                run_id: trial.run_id.clone(),
                task_hash: trial.task_hash.clone(),
                arm_hash: trial.arm_hash.clone(),
                task_id: trial.task_id.clone(),
                arm_id: trial.arm_id.clone(),
                seed: trial.seed,
                pinned_revision: "b".repeat(40),
                model_ids: Vec::new(),
                diff: String::new(),
                checks: Value::Object(checks),
                failure_kind: None,
                token_usage: None,
                duration_ms: Some(measured.duration_ms),
                queue_wait_ms: None,
                session_log_paths: Vec::new(),
            };
            bundle
                .write(&bundle::path(&self.hub, trial))
                .expect("bundle");
        }
    }

    async fn report(&self, a: f64, b: f64) -> Value {
        self.service
            .evals_report(
                &self.run_id,
                EvalsReportRequest {
                    loss: LossParams { a, b },
                    resamples: None,
                },
            )
            .await
            .expect("report")
    }

    fn arm<'a>(&self, report: &'a Value, arm_id: &str) -> &'a Value {
        report["tasks"][0]["arms"]
            .as_array()
            .expect("arms")
            .iter()
            .find(|arm| arm["armId"] == json!(arm_id))
            .expect("arm in report")
    }
}

/// True when any object key in `value` is `key`.
fn has_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(map) => {
            map.contains_key(key) || map.values().any(|value| has_key(value, key))
        }
        Value::Array(items) => items.iter().any(|item| has_key(item, key)),
        _ => false,
    }
}

/// Every key name in `value`, so a test can assert the summary publishes no pooled average.
fn keys(value: &Value) -> Vec<String> {
    let mut names = Vec::new();
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                names.push(key.clone());
                names.extend(keys(value));
            }
        }
        Value::Array(items) => {
            for item in items {
                names.extend(keys(item));
            }
        }
        _ => {}
    }
    names
}

#[tokio::test]
async fn a_report_is_a_function_of_its_loss_and_stores_no_loss() {
    let fixture = fixture(
        2,
        &[
            ("baseline", "graph-baseline.json"),
            ("candidate", "graph-candidate.json"),
        ],
    )
    .await;
    fixture.measure("baseline", Measured::graded(0.6, 0.25, 1_000));
    // 4x the baseline cost, same duration: the exponent split changes the ratio.
    fixture.measure("candidate", Measured::graded(0.9, 1.0, 1_000));
    let candidate = fixture.arm_id("candidate");
    let state_before = std::fs::read(fixture.hub.join("state.json")).expect("state");
    let bundles_before = fixture
        .trials
        .iter()
        .map(|trial| std::fs::read(bundle::path(&fixture.hub, trial)).expect("bundle"))
        .collect::<Vec<_>>();

    let symmetric = fixture.report(0.5, 0.5).await;
    let cost_heavy = fixture.report(0.2, 0.8).await;
    let symmetric_loss = fixture.arm(&symmetric, &candidate)["loss"]
        .as_f64()
        .expect("loss at (0.5,0.5)");
    let other_loss = fixture.arm(&cost_heavy, &candidate)["loss"]
        .as_f64()
        .expect("loss at (0.2,0.8)");
    assert!(
        (symmetric_loss - other_loss).abs() > 1e-6,
        "the report must depend on a and b: {symmetric_loss} vs {other_loss}"
    );
    assert_eq!(symmetric["loss"], json!({"a": 0.5, "b": 0.5}));
    assert_eq!(cost_heavy["loss"], json!({"a": 0.2, "b": 0.8}));

    let state_bytes = std::fs::read(fixture.hub.join("state.json")).expect("state");
    assert_eq!(state_bytes, state_before, "a report must not write state");
    let state: Value = serde_json::from_slice(&state_bytes).expect("state json");
    assert!(!has_key(&state, "loss"), "state must not store a loss");
    for (trial, before) in fixture.trials.iter().zip(&bundles_before) {
        let path = bundle::path(&fixture.hub, trial);
        let bytes = std::fs::read(&path).expect("bundle");
        assert_eq!(
            &bytes,
            before,
            "a report must not rewrite {}",
            path.display()
        );
        let document: Value = serde_json::from_slice(&bytes).expect("bundle json");
        assert!(
            !has_key(&document, "loss"),
            "{} must not store a loss",
            path.display()
        );
    }
    let _ = fixture.directory;
}

#[tokio::test]
async fn a_single_trial_per_arm_is_insufficient_data_not_a_verdict() {
    let fixture = fixture(
        1,
        &[
            ("baseline", "graph-baseline.json"),
            ("candidate", "graph-candidate.json"),
        ],
    )
    .await;
    fixture.measure("baseline", Measured::graded(0.5, 0.25, 1_000));
    fixture.measure("candidate", Measured::graded(0.9, 0.25, 1_000));
    let report = fixture.report(0.5, 0.5).await;
    assert_eq!(report["tasks"][0]["verdict"], json!("insufficient_data"));
    assert_eq!(report["summary"]["verdict"], json!("insufficient_data"));
    // The numbers still list what exists.
    assert_eq!(report["tasks"][0]["arms"][0]["trials"], json!(1));
    assert!(report["tasks"][0]["arms"][0]["loss"].is_number());
    assert!(report["summary"]["medianDelta"].is_null());
}

#[tokio::test]
async fn a_paired_report_medians_deltas_and_drops_a_dominated_arm() {
    let fixture = fixture(
        3,
        &[
            ("baseline", "graph-baseline.json"),
            ("good", "graph-good.json"),
            ("dominated", "graph-dominated.json"),
        ],
    )
    .await;
    fixture.measure("baseline", Measured::graded(0.5, 0.25, 1_000));
    fixture.measure("good", Measured::graded(0.9, 0.25, 1_000));
    fixture.measure("dominated", Measured::graded(0.4, 0.5, 2_000));
    let report = fixture.report(0.5, 0.5).await;
    assert_eq!(report["tasks"][0]["verdict"], json!("measured"));
    let summary = &report["summary"];
    assert_eq!(summary["verdict"], json!("measured"));
    assert!(summary["medianDelta"].as_f64().is_some());
    let ci = summary["ci95"].as_array().expect("ci95");
    assert_eq!(ci.len(), 2);
    let (low, high) = (ci[0].as_f64().expect("low"), ci[1].as_f64().expect("high"));
    assert!(low <= high, "CI must be ordered: {low}..{high}");
    assert_eq!(summary["resamples"], json!(2000));
    assert!(summary["wins"].as_u64().expect("wins") >= 1);

    let dominated = fixture.arm_id("dominated");
    let good = fixture.arm_id("good");
    let pareto = summary["pareto"].as_array().expect("pareto");
    assert!(
        pareto
            .iter()
            .all(|point| point["armId"] != json!(dominated)),
        "a dominated arm must not be on the frontier: {pareto:?}"
    );
    assert!(pareto.iter().any(|point| point["armId"] == json!(good)));
    assert_eq!(pareto[0].as_object().map(|object| object.len()), Some(4));
}

#[tokio::test]
async fn a_trial_without_quality_is_excluded_with_a_reason() {
    let fixture = fixture(
        3,
        &[
            ("baseline", "graph-baseline.json"),
            ("candidate", "graph-candidate.json"),
        ],
    )
    .await;
    fixture.measure("baseline", Measured::graded(0.5, 0.25, 1_000));
    fixture.measure("candidate", Measured::ungraded(0.25, 1_000));
    let report = fixture.report(0.5, 0.5).await;
    let candidate = fixture.arm_id("candidate");
    let arm = fixture.arm(&report, &candidate);
    assert_eq!(arm["trials"], json!(0));
    assert!(arm["quality"].is_null());
    assert!(arm["loss"].is_null());
    let excluded = report["tasks"][0]["excluded"].as_array().expect("excluded");
    assert_eq!(excluded.len(), 3);
    assert!(
        excluded
            .iter()
            .all(|entry| entry["reason"] == json!("no_quality"))
    );
    assert_eq!(report["excluded"], report["tasks"][0]["excluded"]);
    assert_eq!(report["tasks"][0]["verdict"], json!("insufficient_data"));
}

#[tokio::test]
async fn a_task_without_a_named_baseline_arm_contributes_nothing_to_the_summary() {
    let fixture = fixture(
        3,
        &[
            ("control", "graph-control.json"),
            ("candidate", "graph-candidate.json"),
        ],
    )
    .await;
    fixture.measure("control", Measured::graded(0.5, 0.25, 1_000));
    fixture.measure("candidate", Measured::graded(0.9, 0.25, 1_000));
    let report = fixture.report(0.5, 0.5).await;
    let task = &report["tasks"][0];
    // The task still lists what exists, but it has no paired baseline to compare with.
    assert_eq!(task["verdict"], json!("no_baseline"));
    assert_eq!(task["arms"][0]["trials"], json!(3));
    assert!(task["arms"][0]["quality"].is_number());
    assert!(task["arms"][0]["loss"].is_number());
    assert!(task["delta"].is_null());

    let summary = &report["summary"];
    assert_eq!(summary["verdict"], json!("no_baseline"));
    assert_eq!(summary["tasks"], json!(0));
    assert!(summary["medianDelta"].is_null());
    assert!(summary["ci95"].is_null());
    assert_eq!(summary["wins"], json!(0));
    assert_eq!(summary["losses"], json!(0));
    assert_eq!(summary["ties"], json!(0));
    let pareto = summary["pareto"].as_array().expect("pareto");
    assert!(
        pareto.is_empty(),
        "a no_baseline task must not contribute frontier points: {pareto:?}"
    );
    for name in ["control", "candidate"] {
        assert!(
            pareto
                .iter()
                .all(|point| point["armId"] != json!(fixture.arm_id(name)))
        );
    }
}

#[tokio::test]
async fn a_zero_baseline_excludes_the_trial_and_names_the_reason() {
    let (directory, service, _driver) = service().await;
    let mut request = task("one");
    request.baseline.cost_usd = Cost(0.0);
    let task = service.evals_task_create(request).await.expect("task");
    let baseline_arm = service
        .evals_arm_create(arm("baseline", "graph-baseline.json"))
        .await
        .expect("baseline arm");
    let candidate_arm = service
        .evals_arm_create(arm("candidate", "graph-candidate.json"))
        .await
        .expect("candidate arm");
    let created = service
        .evals_run_create(run(
            &[task["taskId"].as_str().expect("task id")],
            &[
                baseline_arm["armId"].as_str().expect("arm id"),
                candidate_arm["armId"].as_str().expect("arm id"),
            ],
            2,
        ))
        .await
        .expect("run");
    let run_id = created["runId"].as_str().expect("run id").to_owned();
    let hub = directory.path().join("hub");
    let trials = service.snapshot().await.evals_v2.trials.clone();
    for trial in &trials {
        let bundle = Bundle {
            trial_id: trial.trial_id.clone(),
            run_id: trial.run_id.clone(),
            task_hash: trial.task_hash.clone(),
            arm_hash: trial.arm_hash.clone(),
            task_id: trial.task_id.clone(),
            arm_id: trial.arm_id.clone(),
            seed: trial.seed,
            pinned_revision: "b".repeat(40),
            model_ids: Vec::new(),
            diff: String::new(),
            checks: json!({"quality": 0.8, "costUsd": 0.25}),
            failure_kind: None,
            token_usage: None,
            duration_ms: Some(1_000),
            queue_wait_ms: None,
            session_log_paths: Vec::new(),
        };
        bundle.write(&bundle::path(&hub, trial)).expect("bundle");
    }
    let report = service
        .evals_report(
            &run_id,
            EvalsReportRequest {
                loss: LossParams { a: 0.5, b: 0.5 },
                resamples: None,
            },
        )
        .await
        .expect("report");
    let excluded = report["tasks"][0]["excluded"].as_array().expect("excluded");
    assert_eq!(excluded.len(), 4);
    assert!(
        excluded
            .iter()
            .all(|entry| entry["reason"] == json!("no_baseline_cost"))
    );
    assert_eq!(report["summary"]["medianDelta"], Value::Null);
}

#[tokio::test]
async fn two_identical_reports_are_byte_identical_and_publish_no_pooled_average() {
    let fixture = fixture(
        3,
        &[
            ("baseline", "graph-baseline.json"),
            ("candidate", "graph-candidate.json"),
        ],
    )
    .await;
    fixture.measure("baseline", Measured::graded(0.5, 0.25, 1_000));
    fixture.measure("candidate", Measured::graded(0.8, 0.3, 1_100));
    let first = fixture.report(0.4, 0.6).await;
    let second = fixture.report(0.4, 0.6).await;
    assert_eq!(first, second);
    assert_eq!(
        serde_json::to_string(&first).expect("first json"),
        serde_json::to_string(&second).expect("second json")
    );
    let summary_keys = keys(&first["summary"]);
    assert!(
        summary_keys
            .iter()
            .all(|key| !key.contains("average") && !key.contains("meanLoss")),
        "the summary must not pool a loss average: {summary_keys:?}"
    );
}

#[tokio::test]
async fn a_loss_parameter_outside_zero_to_two_is_a_usage_error_naming_the_field() {
    let fixture = fixture(2, &[("baseline", "graph-baseline.json")]).await;
    for (loss, field) in [
        (LossParams { a: 2.5, b: 0.5 }, "loss.a"),
        (LossParams { a: 0.5, b: -0.1 }, "loss.b"),
        (
            LossParams {
                a: f64::INFINITY,
                b: 0.5,
            },
            "loss.a",
        ),
    ] {
        let error = fixture
            .service
            .evals_report(
                &fixture.run_id,
                EvalsReportRequest {
                    loss,
                    resamples: None,
                },
            )
            .await
            .expect_err("invalid loss");
        assert!(matches!(error, HubError::Usage(_)), "{error:?}");
        assert!(
            error.to_string().contains(field),
            "{field} must be named: {error}"
        );
    }
    let error = fixture
        .service
        .evals_report(
            &fixture.run_id,
            EvalsReportRequest {
                loss: LossParams { a: 0.5, b: 0.5 },
                resamples: Some(0),
            },
        )
        .await
        .expect_err("invalid resamples");
    assert!(error.to_string().contains("resamples"), "{error}");

    // A malformed body names the field before the service is reached.
    for (body, field) in [
        (json!({}), "loss"),
        (json!({"loss": {"a": "0.5", "b": 0.5}}), "loss.a"),
        (json!({"loss": {"b": 0.5}}), "loss.a"),
        (json!({"loss": {"a": 0.5, "b": 3.0}}), "loss.b"),
        (json!({"loss": {"a": 0.5, "b": 0.5, "c": 1.0}}), "loss.c"),
    ] {
        let error: serde_json::Error =
            serde_json::from_value::<EvalsReportRequest>(body).expect_err("malformed loss");
        assert!(
            error.to_string().contains(field),
            "{field} must be named: {error}"
        );
    }
}
