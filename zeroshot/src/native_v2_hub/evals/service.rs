//! The evals-v2 hub operations: creating tasks, arms, runs and trials, and reconciling trials
//! with their submitted hub jobs into durable bundles.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use super::super::clock::now_ms;
use super::super::error::HubError;
use super::super::model::{HubState, Job, JobState, PlacementSpec, Requirements};
use super::super::service::{HubService, create_private_directory, replace_private};
use super::super::usage::JobUsage;
use super::super::wire::JobFile;
use super::arm::{Arm, CreateArm};
use super::bundle::{self, Bundle};
use super::task::{CreateTask, EvalTask};
use super::trial::{CreateEvalRun, EvalV2Run, Trial, TrialState};
use super::{canonical, sha256_hex, short_hash, trial_id, trial_job_id};

const MAX_REPEAT: u32 = 100;

fn task_document(task: &EvalTask, created: Option<bool>) -> Value {
    let mut document = json!({
        "schema": "zeroshot.hub.evals.task/v1",
        "taskId": task.task_id,
        "taskHash": task.task_hash,
        "task": task.view(),
    });
    if let Some(created) = created {
        document["created"] = json!(created);
    }
    document
}

fn arm_document(arm: &Arm, created: Option<bool>) -> Value {
    let mut document = json!({
        "schema": "zeroshot.hub.evals.arm/v1",
        "armId": arm.arm_id,
        "armHash": arm.arm_hash,
        "arm": arm.view(),
    });
    if let Some(created) = created {
        document["created"] = json!(created);
    }
    document
}

fn run_document(run: &EvalV2Run, trials: Vec<Value>) -> Value {
    let mut view = run.view();
    view["trials"] = json!(trials);
    json!({
        "schema": "zeroshot.hub.evals.run/v1",
        "runId": run.run_id,
        "run": view,
        "trials": trials,
    })
}

fn run_created_document(run: &EvalV2Run) -> Value {
    json!({
        "schema": "zeroshot.hub.evals.run-created/v1",
        "runId": run.run_id,
        "trialIds": run.trial_ids,
        "run": run.view(),
    })
}

fn ready_at(job: &Job) -> u64 {
    job.not_before_ms.unwrap_or(0).max(job.submitted_at_ms)
}

fn queue_wait_ms(job: &Job) -> Option<u64> {
    job.dispatched_at_ms
        .map(|dispatched| dispatched.saturating_sub(ready_at(job)))
}

fn duration_ms(job: &Job) -> Option<u64> {
    job.dispatched_at_ms
        .zip(job.finished_at_ms)
        .map(|(dispatched, finished)| finished.saturating_sub(dispatched))
}

fn job_failure_kind(job: &Job) -> Option<String> {
    job.error
        .as_ref()
        .and_then(|error| serde_json::to_value(error.kind).ok())
        .and_then(|kind| kind.as_str().map(str::to_owned))
        .or_else(|| job.failure_reason.clone())
        .or_else(|| {
            (job.state != JobState::Succeeded).then(|| job.display_state(now_ms()).to_owned())
        })
}

/// The failure kind recorded inside an existing bundle, or `None` for a successful bundle.
fn bundle_failure_kind(path: &Path) -> Result<Option<String>, HubError> {
    let bytes = std::fs::read(path).map_err(|error| {
        HubError::storage(&format!("{} could not be read", path.display()), error)
    })?;
    let document: Value = serde_json::from_slice(&bytes).map_err(|error| {
        HubError::storage(&format!("{} is not a bundle", path.display()), error)
    })?;
    Ok(document
        .get("failureKind")
        .and_then(Value::as_str)
        .map(str::to_owned))
}

/// The first string `diff` field anywhere in a report or archive document.
fn find_diff(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            if let Some(diff) = map.get("diff").and_then(Value::as_str) {
                return Some(diff.to_owned());
            }
            map.values().find_map(find_diff)
        }
        Value::Array(items) => items.iter().find_map(find_diff),
        _ => None,
    }
}

fn last_json(text: &str) -> Option<Value> {
    serde_json::from_str::<Value>(text.trim()).ok().or_else(|| {
        text.lines()
            .rev()
            .find_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
    })
}

fn budget_number(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.get("timeoutMs").and_then(Value::as_u64))
}

/// Apply `nodeBudgets` to the authored graph's step/verifier nodes.
fn apply_node_budgets(value: &mut Value, budgets: &Value) -> bool {
    let mut changed = false;
    match value {
        Value::Object(map) => {
            if let (Some(name), Some(kind)) = (
                map.get("name").and_then(Value::as_str),
                map.get("kind").and_then(Value::as_str),
            ) && matches!(kind, "step" | "verifier")
                && let Some(ms) = budgets.get(name).and_then(budget_number)
            {
                map.insert("timeoutMs".to_owned(), json!(ms));
                changed = true;
            }
            for child in map.values_mut() {
                changed |= apply_node_budgets(child, budgets);
            }
        }
        Value::Array(items) => {
            for item in items {
                changed |= apply_node_budgets(item, budgets);
            }
        }
        _ => {}
    }
    changed
}

/// Materialize the arm's harness patch and models as a runtime file when it names a runtime.
fn arm_runtime(arm: &Arm) -> Option<(&'static str, Value)> {
    let patch = arm.harness_patch.as_ref()?.as_object()?;
    let mut runtime = Value::Object(patch.clone());
    let uniform = runtime.get("nodes").is_none()
        && runtime.get("harness").is_some()
        && runtime.get("provider").is_some();
    if uniform {
        if let Some(model) = arm.model_ids.first() {
            runtime["model"] = json!(model);
        }
        runtime.get("model")?;
        return Some(("--uniform-runtime-config", runtime));
    }
    runtime.get("nodes")?;
    if let Some(nodes) = runtime.get_mut("nodes").and_then(Value::as_object_mut) {
        let mut names = nodes
            .iter()
            .filter(|(_, node)| node.get("kind").and_then(Value::as_str) == Some("agent"))
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        names.sort();
        if !arm.model_ids.is_empty() {
            for (index, name) in names.iter().enumerate() {
                if let Some(node) = nodes.get_mut(name).and_then(Value::as_object_mut) {
                    node.insert(
                        "model".to_owned(),
                        json!(arm.model_ids[index % arm.model_ids.len()]),
                    );
                }
            }
        }
    }
    Some(("--runtime-config", runtime))
}

/// The effective trial view. A terminal trial without its bundle is never reported as a success.
fn trial_view(trial: &Trial, state: &HubState, hub_directory: &Path) -> Value {
    let mut view = trial.view();
    let bundle = trial
        .bundle_path
        .clone()
        .unwrap_or_else(|| bundle::path(hub_directory, trial));
    let job = trial
        .job_id
        .as_ref()
        .and_then(|job_id| state.jobs.iter().find(|job| &job.job_id == job_id));
    let bundle_failure = bundle
        .is_file()
        .then(|| bundle_failure_kind(&bundle).unwrap_or(Some("bundle_missing".to_owned())));
    match job {
        Some(job) if job.state.is_terminal() => {
            if bundle.is_file() {
                let failure = bundle_failure.clone().flatten().or_else(|| {
                    (job.state != JobState::Succeeded)
                        .then(|| job_failure_kind(job))
                        .flatten()
                });
                if let Some(failure) = failure {
                    view["state"] = json!("failed");
                    view["failureKind"] = json!(failure);
                } else {
                    view["state"] = json!("succeeded");
                    view["failureKind"] = Value::Null;
                }
            } else {
                view["state"] = json!("failed");
                view["failureKind"] = json!("bundle_missing");
            }
        }
        Some(job) => {
            let next = if matches!(job.state, JobState::Running | JobState::WaitingQuota) {
                "running"
            } else {
                "queued"
            };
            view["state"] = json!(next);
        }
        None if trial.job_id.is_some() => {
            if matches!(trial.state, TrialState::Succeeded | TrialState::Failed) {
                view["state"] = json!("failed");
                view["failureKind"] = json!(if bundle.is_file() {
                    bundle_failure
                        .clone()
                        .flatten()
                        .unwrap_or_else(|| "job_missing".to_owned())
                } else {
                    "bundle_missing".to_owned()
                });
            }
        }
        None => {
            if let Some(failure) = bundle_failure.flatten() {
                view["state"] = json!("failed");
                view["failureKind"] = json!(failure);
            } else if matches!(trial.state, TrialState::Succeeded | TrialState::Failed)
                && !bundle.is_file()
            {
                view["state"] = json!("failed");
                view["failureKind"] = json!("bundle_missing");
            }
        }
    }
    view
}

/// One trial's job and the files its submission needs.
struct TrialPlan<'a> {
    run_id: &'a str,
    task: &'a EvalTask,
    arm: &'a Arm,
    trial: &'a Trial,
}

fn trial_job(hub_directory: &Path, plan: &TrialPlan<'_>) -> Result<(Job, Vec<JobFile>), HubError> {
    let (task, arm, trial) = (plan.task, plan.arm, plan.trial);
    let title = format!("{} [{} #{}]", task.title, arm.name, trial.seed);
    let revision = if arm.base_revision.trim().is_empty() {
        task.pinned_revision.trim()
    } else {
        arm.base_revision.trim()
    };
    let input = json!({
        "task": task.prompt,
        "title": task.title,
        "baselineCommits": task.baseline_commits,
        "pinnedRevision": task.pinned_revision,
        "baseline": {
            "costUsd": task.baseline.cost_usd.0,
            "durationMs": task.baseline.duration_ms,
        },
        "arm": {
            "name": arm.name,
            "graphRef": arm.graph_ref,
            "harnessPatch": arm.harness_patch,
            "baseRevision": arm.base_revision,
            "modelIds": arm.model_ids,
            "nodeBudgets": arm.node_budgets,
        },
        "seed": trial.seed,
        "runId": plan.run_id,
        "trialId": trial.trial_id,
    });
    let input = serde_json::to_string_pretty(&input)
        .map_err(|error| HubError::storage("trial input could not be encoded", error))?;
    let mut run_args = vec![
        "--title".to_owned(),
        title.clone(),
        "--input".to_owned(),
        "trial-input.json".to_owned(),
    ];
    let mut files = vec![JobFile {
        argument: "trial-input.json".to_owned(),
        content: input,
    }];
    let graph_ref = arm.graph_ref.trim();
    if !graph_ref.is_empty() {
        let mut graph_file = if graph_ref.starts_with('{') {
            Some(graph_ref.to_owned())
        } else {
            let candidate = Path::new(graph_ref);
            let path = if candidate.is_absolute() {
                candidate.to_path_buf()
            } else {
                hub_directory.join(candidate)
            };
            let path = if path.is_file() {
                Some(path)
            } else if candidate.is_file() {
                Some(candidate.to_path_buf())
            } else {
                None
            };
            path.map(|path| {
                std::fs::read_to_string(&path).map_err(|error| {
                    HubError::storage(
                        &format!("arm graph {} could not be read", path.display()),
                        error,
                    )
                })
            })
            .transpose()?
        };
        if let (Some(content), Some(budgets)) = (graph_file.as_mut(), arm.node_budgets.as_ref())
            && let Ok(mut graph) = serde_json::from_str::<Value>(content)
            && apply_node_budgets(&mut graph, budgets)
        {
            *content = serde_json::to_string_pretty(&graph)
                .map_err(|error| HubError::storage("arm graph could not be encoded", error))?;
        }
        if let Some(content) = graph_file {
            files.push(JobFile {
                argument: "trial-graph.json".to_owned(),
                content,
            });
            run_args.extend(["--graph".to_owned(), "trial-graph.json".to_owned()]);
        } else {
            run_args.extend(["--template".to_owned(), graph_ref.to_owned()]);
        }
    }
    if let Some((flag, runtime)) = arm_runtime(arm) {
        let content = serde_json::to_string_pretty(&runtime)
            .map_err(|error| HubError::storage("arm runtime could not be encoded", error))?;
        files.push(JobFile {
            argument: "trial-runtime.json".to_owned(),
            content,
        });
        run_args.extend([flag.to_owned(), "trial-runtime.json".to_owned()]);
    }
    if !revision.is_empty() {
        run_args.extend(["--revision".to_owned(), revision.to_owned()]);
    }
    let mut labels = BTreeMap::new();
    labels.insert("evalv2.run".to_owned(), plan.run_id.to_owned());
    labels.insert("evalv2.task".to_owned(), task.task_id.clone());
    labels.insert("evalv2.arm".to_owned(), arm.arm_id.clone());
    labels.insert("evalv2.trial".to_owned(), trial.trial_id.clone());
    labels.insert("evalv2.seed".to_owned(), trial.seed.to_string());
    labels.insert("evalv2.taskHash".to_owned(), task.task_hash.clone());
    labels.insert("evalv2.armHash".to_owned(), arm.arm_hash.clone());
    let job = Job {
        job_id: trial_job_id(&trial.trial_id),
        state: JobState::Queued,
        title,
        run_args,
        cwd: hub_directory.to_path_buf(),
        pinned_revision: None,
        placement: PlacementSpec::default(),
        requirements: Requirements {
            local_unsupported: (!revision.is_empty())
                .then(|| "--revision needs a named target".to_owned()),
            ..Requirements::default()
        },
        priority: 0,
        labels,
        project: None,
        models: arm.model_ids.clone(),
        submission_key: Some(format!("evalv2:{}", trial.trial_id)),
        max_stall_ms: None,
        submitted_at_ms: now_ms(),
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
        last_status_error_at_ms: None,
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
    };
    Ok((job, files))
}

/// What writing a bundle learned about a terminal trial.
struct TrialOutcome {
    token_usage: Option<JobUsage>,
    duration_ms: Option<u64>,
    queue_wait_ms: Option<u64>,
    failure_kind: Option<String>,
    bundle_path: PathBuf,
}

struct TrialUpdate {
    trial_id: String,
    state: TrialState,
    failure_kind: Option<String>,
    token_usage: Option<JobUsage>,
    duration_ms: Option<u64>,
    queue_wait_ms: Option<u64>,
    bundle_path: Option<PathBuf>,
    bundle_written: bool,
}

impl HubService {
    pub(crate) async fn evals_task_create(&self, request: CreateTask) -> Result<Value, HubError> {
        let now = now_ms();
        let task = EvalTask::from_request(&request, now);
        self.mutate(|state| {
            let created = state.evals_v2.task(&task.task_hash).is_none();
            if created {
                state.evals_v2.tasks.push(task.clone());
            }
            let stored = state
                .evals_v2
                .task(&task.task_hash)
                .cloned()
                .unwrap_or(task);
            Ok(task_document(&stored, Some(created)))
        })
        .await
    }

    pub(crate) async fn evals_arm_create(&self, request: CreateArm) -> Result<Value, HubError> {
        let now = now_ms();
        let arm = Arm::from_request(&request, now);
        self.mutate(|state| {
            let created = state.evals_v2.arm(&arm.arm_hash).is_none();
            if created {
                state.evals_v2.arms.push(arm.clone());
            }
            let stored = state.evals_v2.arm(&arm.arm_hash).cloned().unwrap_or(arm);
            Ok(arm_document(&stored, Some(created)))
        })
        .await
    }

    pub(crate) async fn evals_run_create(&self, request: CreateEvalRun) -> Result<Value, HubError> {
        if request.repeat == 0 || request.repeat > MAX_REPEAT {
            return Err(HubError::Usage(format!("repeat must be 1..={MAX_REPEAT}")));
        }
        if request.tasks.is_empty() || request.arms.is_empty() {
            return Err(HubError::Usage(
                "an evals run needs at least one task and one arm".to_owned(),
            ));
        }
        let run_hash = sha256_hex(
            canonical(&serde_json::to_value(&request).unwrap_or(Value::Null)).as_bytes(),
        );
        let run_id = format!("er-{}", short_hash(&run_hash));
        let state = self.snapshot().await;
        if let Some(existing) = state.evals_v2.run(&run_id).cloned() {
            self.evals_reconcile().await?;
            let state = self.snapshot().await;
            let existing = state
                .evals_v2
                .run(&existing.run_id)
                .cloned()
                .unwrap_or(existing);
            return Ok(run_created_document(&existing));
        }
        let mut tasks = Vec::new();
        for reference in &request.tasks {
            let task = state.evals_v2.task(reference).cloned().ok_or_else(|| {
                HubError::EvalNotFound(format!("evals task {reference} does not exist"))
            })?;
            if !tasks
                .iter()
                .any(|existing: &EvalTask| existing.task_hash == task.task_hash)
            {
                tasks.push(task);
            }
        }
        let mut arms = Vec::new();
        for reference in &request.arms {
            let arm = state.evals_v2.arm(reference).cloned().ok_or_else(|| {
                HubError::EvalNotFound(format!("evals arm {reference} does not exist"))
            })?;
            if !arms
                .iter()
                .any(|existing: &Arm| existing.arm_hash == arm.arm_hash)
            {
                arms.push(arm);
            }
        }
        let mut trials = Vec::new();
        for task in &tasks {
            for arm in &arms {
                for seed in 0..request.repeat {
                    let trial_id = trial_id(&run_id, &task.task_hash, &arm.arm_hash, seed);
                    trials.push(Trial {
                        trial_id,
                        run_id: run_id.clone(),
                        task_hash: task.task_hash.clone(),
                        arm_hash: arm.arm_hash.clone(),
                        task_id: task.task_id.clone(),
                        arm_id: arm.arm_id.clone(),
                        seed,
                        state: TrialState::Queued,
                        job_id: None,
                        failure_kind: None,
                        token_usage: None,
                        duration_ms: None,
                        queue_wait_ms: None,
                        bundle_path: None,
                        bundle_written: false,
                    });
                }
            }
        }
        // Build and validate every job before changing state, so an unreadable graph or a bad
        // arm cannot leave a subset of jobs behind.
        let mut plans = Vec::new();
        for trial in &mut trials {
            let task = tasks
                .iter()
                .find(|task| task.task_hash == trial.task_hash)
                .ok_or_else(|| {
                    HubError::EvalNotFound(format!("evals task {} does not exist", trial.task_hash))
                })?;
            let arm = arms
                .iter()
                .find(|arm| arm.arm_hash == trial.arm_hash)
                .ok_or_else(|| {
                    HubError::EvalNotFound(format!("evals arm {} does not exist", trial.arm_hash))
                })?;
            let plan = TrialPlan {
                run_id: &run_id,
                task,
                arm,
                trial,
            };
            let (job, files) = trial_job(self.store().directory(), &plan)?;
            trial.job_id = Some(job.job_id.clone());
            plans.push((trial.trial_id.clone(), job, files));
        }
        let run = EvalV2Run {
            run_id: run_id.clone(),
            task_hashes: tasks.iter().map(|task| task.task_hash.clone()).collect(),
            arm_hashes: arms.iter().map(|arm| arm.arm_hash.clone()).collect(),
            repeat: request.repeat,
            judge: request.judge.clone(),
            created_at_ms: now_ms(),
            trial_ids: trials.iter().map(|trial| trial.trial_id.clone()).collect(),
        };
        // Persist the run and trials before submitting any job. A failed enqueue then belongs to
        // a durable trial, never to an untracked job.
        let inserted = self
            .mutate(|state| {
                if state.evals_v2.run(&run.run_id).is_some() {
                    return Ok(false);
                }
                for trial in &trials {
                    if state.evals_v2.trial(&trial.trial_id).is_none() {
                        state.evals_v2.trials.push(trial.clone());
                    }
                }
                state.evals_v2.runs.push(run.clone());
                Ok(true)
            })
            .await?;
        if !inserted {
            self.evals_reconcile().await?;
            let state = self.snapshot().await;
            let existing = state.evals_v2.run(&run.run_id).cloned().ok_or_else(|| {
                HubError::EvalNotFound(format!("evals run {} does not exist", run.run_id))
            })?;
            return Ok(run_created_document(&existing));
        }
        let mut updates: Vec<(String, Option<String>, TrialState, String)> = Vec::new();
        for (trial_id, job, files) in plans {
            match self.enqueue(job, files).await {
                Ok(_) => {}
                Err(HubError::NoEligibleTarget { .. }) => {
                    updates.push((trial_id, None, TrialState::Failed, "unplaceable".to_owned()))
                }
                Err(_) => updates.push((
                    trial_id,
                    None,
                    TrialState::Failed,
                    "enqueue_failed".to_owned(),
                )),
            }
        }
        if !updates.is_empty() {
            self.mutate(|state| {
                for (trial_id, job_id, trial_state, failure) in &updates {
                    if let Some(trial) = state
                        .evals_v2
                        .trials
                        .iter_mut()
                        .find(|trial| &trial.trial_id == trial_id)
                    {
                        trial.job_id.clone_from(job_id);
                        trial.state = *trial_state;
                        trial.failure_kind = Some(failure.clone());
                    }
                }
                Ok(())
            })
            .await?;
        }
        // Terminal trials without jobs (unplaceable or rejected at submission) still get their
        // minimal bundle; this is not part of the read path.
        self.evals_reconcile().await?;
        let state = self.snapshot().await;
        let stored = state.evals_v2.run(&run.run_id).cloned().unwrap_or(run);
        Ok(run_created_document(&stored))
    }

    pub(crate) async fn evals_run(&self, run_id: &str) -> Result<Value, HubError> {
        let state = self.snapshot().await;
        let run =
            state.evals_v2.run(run_id).cloned().ok_or_else(|| {
                HubError::EvalNotFound(format!("evals run {run_id} does not exist"))
            })?;
        let trials = state
            .evals_v2
            .trials
            .iter()
            .filter(|trial| trial.run_id == run.run_id)
            .map(|trial| trial_view(trial, &state, self.store().directory()))
            .collect::<Vec<_>>();
        Ok(run_document(&run, trials))
    }

    /// Measure a run at the caller's `(a,b)`. This is a pure read: it writes neither state nor
    /// bundles, so the same request over unchanged data answers with the same document.
    pub(crate) async fn evals_report(
        &self,
        run_id: &str,
        request: super::report::EvalsReportRequest,
    ) -> Result<Value, HubError> {
        let resamples = request.validate()?;
        let state = self.snapshot().await;
        let run =
            state.evals_v2.run(run_id).cloned().ok_or_else(|| {
                HubError::EvalNotFound(format!("evals run {run_id} does not exist"))
            })?;
        super::report::report(super::report::ReportInput {
            run: &run,
            state: &state,
            hub_directory: self.store().directory(),
            params: &request.loss,
            resamples,
        })
    }

    /// Move every trial whose job changed forward and write bundles for terminal trials. This
    /// runs beside dispatching, before a pool lease can discard the trial's workspace.
    pub(crate) async fn evals_reconcile(&self) -> Result<(), HubError> {
        let state = self.snapshot().await;
        let mut updates = Vec::new();
        for trial in &state.evals_v2.trials {
            let job = trial
                .job_id
                .as_ref()
                .and_then(|job_id| state.jobs.iter().find(|job| &job.job_id == job_id));
            let bundle_path = trial
                .bundle_path
                .clone()
                .unwrap_or_else(|| bundle::path(self.store().directory(), trial));
            let bundle_exists = bundle_path.is_file();
            if let Some(job) = job {
                if !job.state.is_terminal() {
                    let next = if matches!(job.state, JobState::Running | JobState::WaitingQuota) {
                        TrialState::Running
                    } else {
                        TrialState::Queued
                    };
                    if trial.state != next || trial.failure_kind.is_some() {
                        updates.push(TrialUpdate {
                            trial_id: trial.trial_id.clone(),
                            state: next,
                            failure_kind: None,
                            token_usage: trial.token_usage.clone(),
                            duration_ms: duration_ms(job),
                            queue_wait_ms: queue_wait_ms(job),
                            bundle_path: trial.bundle_path.clone(),
                            bundle_written: trial.bundle_written,
                        });
                    }
                    continue;
                }
                if trial.bundle_written && !bundle_exists {
                    if trial.state != TrialState::Failed
                        || trial.failure_kind.as_deref() != Some("bundle_missing")
                    {
                        updates.push(TrialUpdate {
                            trial_id: trial.trial_id.clone(),
                            state: TrialState::Failed,
                            failure_kind: Some("bundle_missing".to_owned()),
                            token_usage: trial.token_usage.clone(),
                            duration_ms: trial.duration_ms.or_else(|| duration_ms(job)),
                            queue_wait_ms: trial.queue_wait_ms.or_else(|| queue_wait_ms(job)),
                            bundle_path: Some(bundle_path),
                            bundle_written: true,
                        });
                    }
                    continue;
                }
                if bundle_exists {
                    match bundle_failure_kind(&bundle_path) {
                        Ok(failure) => {
                            let next = if job.state == JobState::Succeeded && failure.is_none() {
                                TrialState::Succeeded
                            } else {
                                TrialState::Failed
                            };
                            let failure_kind = if next == TrialState::Succeeded {
                                None
                            } else {
                                failure.or_else(|| job_failure_kind(job))
                            };
                            if trial.state != next
                                || trial.bundle_path.as_ref() != Some(&bundle_path)
                                || trial.failure_kind != failure_kind
                                || !trial.bundle_written
                            {
                                updates.push(TrialUpdate {
                                    trial_id: trial.trial_id.clone(),
                                    state: next,
                                    failure_kind,
                                    token_usage: trial.token_usage.clone(),
                                    duration_ms: trial.duration_ms.or_else(|| duration_ms(job)),
                                    queue_wait_ms: trial
                                        .queue_wait_ms
                                        .or_else(|| queue_wait_ms(job)),
                                    bundle_path: Some(bundle_path),
                                    bundle_written: true,
                                });
                            }
                        }
                        Err(_) => {
                            if trial.state != TrialState::Failed
                                || trial.failure_kind.as_deref() != Some("bundle_missing")
                            {
                                updates.push(TrialUpdate {
                                    trial_id: trial.trial_id.clone(),
                                    state: TrialState::Failed,
                                    failure_kind: Some("bundle_missing".to_owned()),
                                    token_usage: trial.token_usage.clone(),
                                    duration_ms: trial.duration_ms.or_else(|| duration_ms(job)),
                                    queue_wait_ms: trial
                                        .queue_wait_ms
                                        .or_else(|| queue_wait_ms(job)),
                                    bundle_path: Some(bundle_path),
                                    bundle_written: true,
                                });
                            }
                        }
                    }
                    continue;
                }
                match self.write_trial_bundle(trial, Some(job), &state).await {
                    Ok(outcome) => {
                        let next =
                            if job.state == JobState::Succeeded && outcome.failure_kind.is_none() {
                                TrialState::Succeeded
                            } else {
                                TrialState::Failed
                            };
                        updates.push(TrialUpdate {
                            trial_id: trial.trial_id.clone(),
                            state: next,
                            failure_kind: outcome.failure_kind,
                            token_usage: outcome.token_usage,
                            duration_ms: outcome.duration_ms,
                            queue_wait_ms: outcome.queue_wait_ms,
                            bundle_path: Some(outcome.bundle_path),
                            bundle_written: true,
                        });
                    }
                    Err(_) => {
                        updates.push(TrialUpdate {
                            trial_id: trial.trial_id.clone(),
                            state: TrialState::Failed,
                            failure_kind: Some("bundle_missing".to_owned()),
                            token_usage: trial.token_usage.clone(),
                            duration_ms: trial.duration_ms.or_else(|| duration_ms(job)),
                            queue_wait_ms: trial.queue_wait_ms.or_else(|| queue_wait_ms(job)),
                            bundle_path: Some(bundle_path),
                            bundle_written: false,
                        });
                    }
                }
                continue;
            }

            if matches!(trial.state, TrialState::Succeeded | TrialState::Failed) {
                if trial.bundle_written && !bundle_exists {
                    if trial.state != TrialState::Failed
                        || trial.failure_kind.as_deref() != Some("bundle_missing")
                    {
                        updates.push(TrialUpdate {
                            trial_id: trial.trial_id.clone(),
                            state: TrialState::Failed,
                            failure_kind: Some("bundle_missing".to_owned()),
                            token_usage: trial.token_usage.clone(),
                            duration_ms: trial.duration_ms,
                            queue_wait_ms: trial.queue_wait_ms,
                            bundle_path: Some(bundle_path),
                            bundle_written: true,
                        });
                    }
                    continue;
                }
                if bundle_exists {
                    let failure = bundle_failure_kind(&bundle_path)
                        .ok()
                        .flatten()
                        .or_else(|| trial.failure_kind.clone());
                    let next = if failure.is_some() {
                        TrialState::Failed
                    } else {
                        trial.state
                    };
                    if trial.state != next || !trial.bundle_written || trial.failure_kind != failure
                    {
                        updates.push(TrialUpdate {
                            trial_id: trial.trial_id.clone(),
                            state: next,
                            failure_kind: failure,
                            token_usage: trial.token_usage.clone(),
                            duration_ms: trial.duration_ms,
                            queue_wait_ms: trial.queue_wait_ms,
                            bundle_path: Some(bundle_path),
                            bundle_written: true,
                        });
                    }
                    continue;
                }
                let mut own = trial.clone();
                if own.failure_kind.is_none() {
                    own.failure_kind = Some("job_missing".to_owned());
                }
                match self.write_trial_bundle(&own, None, &state).await {
                    Ok(outcome) => {
                        let next = if outcome.failure_kind.is_none() {
                            TrialState::Succeeded
                        } else {
                            TrialState::Failed
                        };
                        updates.push(TrialUpdate {
                            trial_id: trial.trial_id.clone(),
                            state: next,
                            failure_kind: outcome.failure_kind,
                            token_usage: outcome.token_usage,
                            duration_ms: outcome.duration_ms,
                            queue_wait_ms: outcome.queue_wait_ms,
                            bundle_path: Some(outcome.bundle_path),
                            bundle_written: true,
                        });
                    }
                    Err(_) => {
                        updates.push(TrialUpdate {
                            trial_id: trial.trial_id.clone(),
                            state: TrialState::Failed,
                            failure_kind: Some("bundle_missing".to_owned()),
                            token_usage: trial.token_usage.clone(),
                            duration_ms: trial.duration_ms,
                            queue_wait_ms: trial.queue_wait_ms,
                            bundle_path: Some(bundle_path),
                            bundle_written: false,
                        });
                    }
                }
                continue;
            }

            let failure = if trial.job_id.is_some() {
                "job_missing"
            } else {
                "unplaceable"
            };
            updates.push(TrialUpdate {
                trial_id: trial.trial_id.clone(),
                state: TrialState::Failed,
                failure_kind: Some(failure.to_owned()),
                token_usage: trial.token_usage.clone(),
                duration_ms: trial.duration_ms,
                queue_wait_ms: trial.queue_wait_ms,
                bundle_path: trial.bundle_path.clone(),
                bundle_written: trial.bundle_written,
            });
        }
        if updates.is_empty() {
            return Ok(());
        }
        self.mutate(|state| {
            for update in &updates {
                if let Some(trial) = state
                    .evals_v2
                    .trials
                    .iter_mut()
                    .find(|trial| trial.trial_id == update.trial_id)
                {
                    trial.state = update.state;
                    trial.failure_kind.clone_from(&update.failure_kind);
                    trial.token_usage.clone_from(&update.token_usage);
                    trial.duration_ms = update.duration_ms;
                    trial.queue_wait_ms = update.queue_wait_ms;
                    trial.bundle_path.clone_from(&update.bundle_path);
                    trial.bundle_written = update.bundle_written;
                }
            }
            Ok(())
        })
        .await
    }

    async fn write_trial_bundle(
        &self,
        trial: &Trial,
        job: Option<&Job>,
        state: &HubState,
    ) -> Result<TrialOutcome, HubError> {
        let task = state.evals_v2.task(&trial.task_hash).ok_or_else(|| {
            HubError::EvalNotFound(format!("evals task {} does not exist", trial.task_hash))
        })?;
        let arm = state.evals_v2.arm(&trial.arm_hash).ok_or_else(|| {
            HubError::EvalNotFound(format!("evals arm {} does not exist", trial.arm_hash))
        })?;
        let status = match job.and_then(|job| Some((job.target.as_ref()?, job.run_id.as_ref()?))) {
            Some((target, run_id)) => self
                .driver()
                .status(target, run_id)
                .await
                .unwrap_or(Value::Null),
            None => Value::Null,
        };
        let status_output = status.pointer("/status/terminalResult/output");
        let job_output = job.and_then(|job| job.output.as_ref());
        let mut diff = status_output
            .and_then(|output| output.get("diff"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                job_output
                    .and_then(|output| output.get("diff"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        if diff.is_none() {
            diff = status
                .pointer("/status/terminalResult/diff")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        let mut diff_present = diff.is_some();
        let checks = status_output
            .and_then(|output| output.get("checks"))
            .cloned()
            .or_else(|| job_output.and_then(|output| output.get("checks")).cloned())
            .or_else(|| status.pointer("/status/metadata/checks").cloned())
            .unwrap_or_else(|| json!([]));
        let token_usage = job
            .and_then(|job| job.token_usage.clone())
            .or_else(|| JobUsage::from_status(&status));
        let path = bundle::path(self.store().directory(), trial);
        let folder = path.parent().ok_or_else(|| {
            HubError::InvalidState(format!("bundle path {} has no folder", path.display()))
        })?;
        create_private_directory(folder)?;
        let mut session_logs = Vec::new();
        if let Some(job) = job
            && let (Some(target), Some(run_id)) = (&job.target, &job.run_id)
        {
            if let Ok(text) = self
                .driver()
                .capture(target, vec!["report".to_owned(), run_id.clone()])
                .await
            {
                let report_path = folder.join("report.json");
                replace_private(&report_path, text.as_bytes())?;
                session_logs.push(report_path.to_string_lossy().into_owned());
                if diff.is_none()
                    && let Some(document) = last_json(&text)
                    && let Some(found) = find_diff(&document)
                {
                    diff = Some(found);
                    diff_present = true;
                }
            }
            for (command, name) in [("logs", "logs.ndjson"), ("watch", "watch.ndjson")] {
                if let Ok(text) = self
                    .driver()
                    .capture(target, vec![command.to_owned(), run_id.clone()])
                    .await
                {
                    let log_path = folder.join(name);
                    replace_private(&log_path, text.as_bytes())?;
                    session_logs.push(log_path.to_string_lossy().into_owned());
                }
            }
        }
        if let Some(archive) = job.and_then(|job| job.archive.as_ref()) {
            for name in super::super::archive::FILES {
                let archived = archive.join(name);
                if archived.is_file() {
                    if diff.is_none()
                        && matches!(name, "report.json" | "status.json")
                        && let Ok(text) = std::fs::read_to_string(&archived)
                        && let Some(document) = last_json(&text)
                        && let Some(found) = find_diff(&document)
                    {
                        diff = Some(found);
                        diff_present = true;
                    }
                    session_logs.push(archived.to_string_lossy().into_owned());
                }
            }
        }
        let failure_kind = match job {
            None => trial
                .failure_kind
                .clone()
                .or_else(|| Some("job_missing".to_owned())),
            Some(job) if job.state == JobState::Succeeded => {
                if diff_present {
                    None
                } else {
                    Some("diff_unavailable".to_owned())
                }
            }
            Some(job) => job_failure_kind(job),
        };
        let bundle = Bundle {
            trial_id: trial.trial_id.clone(),
            run_id: trial.run_id.clone(),
            task_hash: trial.task_hash.clone(),
            arm_hash: trial.arm_hash.clone(),
            task_id: trial.task_id.clone(),
            arm_id: trial.arm_id.clone(),
            seed: trial.seed,
            pinned_revision: task.pinned_revision.clone(),
            model_ids: arm.model_ids.clone(),
            diff: diff.unwrap_or_default(),
            checks,
            failure_kind: failure_kind.clone(),
            token_usage: token_usage.clone(),
            duration_ms: job.and_then(duration_ms),
            queue_wait_ms: job.and_then(queue_wait_ms),
            session_log_paths: session_logs,
        };
        bundle.write(&path)?;
        Ok(TrialOutcome {
            token_usage,
            duration_ms: job.and_then(duration_ms),
            queue_wait_ms: job.and_then(queue_wait_ms),
            failure_kind,
            bundle_path: path,
        })
    }
}
