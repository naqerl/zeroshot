use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use openengine_cluster_protocol::{
    ConnectionKey, EnvironmentVariableName, GitTransportCredentials, IdempotencyKey,
    RunAttachParams, RunForceParams, RunId, RunListParams, RunLogsParams, RunResumeParams,
    RunResumeResult, RunSize, RunStatusParams, RunSubmitResult, RunTitle, RunWatchParams,
    StaticConnectionValues, TokenCount, TokenUsage,
};
use serde_json::json;

use super::{
    BackendSampleRequest, DeliveryReceipt, EvalRunRequest, EvalSamplePlan, FakeRunner,
    FakeSampleRequest, PlatformRunner, ResumeCredentials, SAMPLE_FILE, SampleArtifact,
    SampleOutcome, SampleRunner, SampleStatus, run_eval, run_sample,
};
use crate::native_v2_cli::{
    CliRunForceResult, CliRunListResult, CliRunStatusResult, CliSubscription, CliSubscriptionItem,
    NativeV2CliBackend, PreparedRunRequest, TargetAdd,
};
use crate::native_v2_contract::RunSubmissionIntent;
use crate::native_v2_eval::{
    BackendRunRequest, EvalManifest, EvalRunCommand, RunnerSelection, run_manifest_with_backend,
    select_runner,
};

fn usage() -> TokenUsage {
    TokenUsage {
        input_tokens: TokenCount::new(11).unwrap(),
        output_tokens: TokenCount::new(7).unwrap(),
        cache_read_input_tokens: None,
        cache_creation_input_tokens: None,
        complete: true,
    }
}

fn sample_outcome_for_test(status: SampleStatus) -> SampleOutcome {
    SampleOutcome {
        run_id: RunId::new("old-run"),
        status,
        timings: super::SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 1,
        },
        token_usage: None,
        delivered_revision: None,
        delivered_branch: None,
        delivery: None,
        failure_reason: Some("worker_failed".to_owned()),
        output: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        patch: None,
    }
}

fn prepared_request() -> PreparedRunRequest {
    PreparedRunRequest {
        run_id: RunId::new("request-run"),
        intent: RunSubmissionIntent {
            title: serde_json::from_value::<RunTitle>(json!("Eval sample")).unwrap(),
            graph: crate::native_v2_candidate::test_support::full_graph(vec![
                crate::native_v2_candidate::test_support::success_node(),
            ]),
            initial_input: serde_json::Value::Null,
            runtime: crate::native_v2_candidate::test_support::codex_runtime(
                RunSize::Small,
                BTreeMap::new(),
            ),
            environment: None,
            branch: None,
            submission_key: serde_json::from_value::<IdempotencyKey>(json!("eval-request"))
                .unwrap(),
        },
        connections: BTreeMap::new(),
        github_token: None,
        git_credentials: None,
        cwd: None,
        source_connection: None,
        source: None,
        profile: None,
    }
}

fn status(status: serde_json::Value) -> CliRunStatusResult {
    serde_json::from_value(json!({
        "runId": "run-scripted",
        "title": "Eval sample",
        "size": "small",
        "atCursor": "v2:1",
        "status": status,
    }))
    .unwrap()
}

fn queued() -> CliRunStatusResult {
    status(json!({"phase": "queued"}))
}

fn running() -> CliRunStatusResult {
    status(json!({"phase": "running", "activeExecutions": []}))
}

fn finished_succeeded() -> CliRunStatusResult {
    status(json!({
        "phase": "finished",
        "terminalResult": {
            "status": "succeeded",
            "output": {
                "delivery": {
                    "version": "v3",
                    "mode": "merge",
                    "outcome": "merged",
                    "repository": "acme/project",
                    "targetBranch": "main",
                    "headBranch": "zeroshot/run",
                    "headRevision": "b".repeat(40),
                    "mergeRevision": "c".repeat(40),
                }
            }
        },
        "metadata": {
            "tokenUsage": {
                "inputTokens": 12,
                "outputTokens": 4,
                "complete": true
            }
        }
    }))
}

fn finished_failed() -> CliRunStatusResult {
    status(json!({
        "phase": "finished",
        "terminalResult": {"status": "failed", "reason": "worker_failed"},
        "metadata": {}
    }))
}

#[derive(Default)]
struct ScriptState {
    statuses: VecDeque<CliRunStatusResult>,
    last: Option<CliRunStatusResult>,
    forced: Vec<RunId>,
    resumed: Vec<RunResumeParams>,
    requests: Vec<PreparedRunRequest>,
    submits: usize,
    fail_next_submit: bool,
    fail_next_status: bool,
    fail_next_status_lost: bool,
    fail_next_resume: bool,
}

struct ScriptedBackend {
    state: Mutex<ScriptState>,
    delay: Duration,
}

impl ScriptedBackend {
    fn new(statuses: Vec<CliRunStatusResult>, delay: Duration) -> Self {
        Self {
            state: Mutex::new(ScriptState {
                statuses: statuses.into(),
                ..ScriptState::default()
            }),
            delay,
        }
    }

    fn forced(&self) -> Vec<RunId> {
        self.state.lock().unwrap().forced.clone()
    }

    fn resumed(&self) -> Vec<RunResumeParams> {
        self.state.lock().unwrap().resumed.clone()
    }

    fn submits(&self) -> usize {
        self.state.lock().unwrap().submits
    }

    fn last_request(&self) -> Option<PreparedRunRequest> {
        self.state.lock().unwrap().requests.last().cloned()
    }

    fn fail_next_submit(&self) {
        self.state.lock().unwrap().fail_next_submit = true;
    }

    fn fail_next_status(&self) {
        self.state.lock().unwrap().fail_next_status = true;
    }

    fn fail_next_status_lost(&self) {
        self.state.lock().unwrap().fail_next_status_lost = true;
    }

    fn fail_next_resume(&self) {
        self.state.lock().unwrap().fail_next_resume = true;
    }
}

struct PendingSubscription<E>(PhantomData<E>);

#[async_trait]
impl<E: Send> CliSubscription<E> for PendingSubscription<E> {
    async fn next(
        &mut self,
    ) -> Result<Option<CliSubscriptionItem<E>>, crate::native_v2_cli::NativeV2CliError> {
        std::future::pending::<()>().await;
        unreachable!()
    }
}

#[async_trait]
impl NativeV2CliBackend for ScriptedBackend {
    type Watch = PendingSubscription<crate::native_v2_cli::CliRunWatchEventNotification>;
    type Logs = PendingSubscription<openengine_cluster_protocol::RunLogEventNotification>;
    type Attach = PendingSubscription<openengine_cluster_protocol::RunAttachEventNotification>;

    async fn target_add(
        &self,
        _request: TargetAdd,
    ) -> Result<(), crate::native_v2_cli::NativeV2CliError> {
        unreachable!()
    }

    async fn target_login(
        &self,
        _name: &str,
    ) -> Result<(), crate::native_v2_cli::NativeV2CliError> {
        unreachable!()
    }

    async fn run_submit(
        &self,
        _target: Option<&str>,
        request: PreparedRunRequest,
    ) -> Result<RunSubmitResult, crate::native_v2_cli::NativeV2CliError> {
        let mut state = self.state.lock().unwrap();
        state.submits += 1;
        state.requests.push(request);
        if state.fail_next_submit {
            state.fail_next_submit = false;
            return Err(crate::native_v2_cli::NativeV2CliError::Protocol(
                "transient submit failure".to_owned(),
            ));
        }
        Ok(RunSubmitResult {
            run_id: RunId::new("run-scripted"),
        })
    }

    async fn run_list(
        &self,
        _target: Option<&str>,
        _params: RunListParams,
    ) -> Result<CliRunListResult, crate::native_v2_cli::NativeV2CliError> {
        Ok(CliRunListResult { runs: Vec::new() })
    }

    async fn run_status(
        &self,
        _target: Option<&str>,
        _params: RunStatusParams,
    ) -> Result<CliRunStatusResult, crate::native_v2_cli::NativeV2CliError> {
        let (fail, lost) = {
            let mut state = self.state.lock().unwrap();
            let fail = state.fail_next_status;
            let lost = state.fail_next_status_lost;
            state.fail_next_status = false;
            state.fail_next_status_lost = false;
            (fail, lost)
        };
        if lost {
            return Err(crate::native_v2_cli::NativeV2CliError::RunNotFound {
                run_id: "run-scripted".to_owned(),
            });
        }
        if fail {
            return Err(crate::native_v2_cli::NativeV2CliError::Protocol(
                "transient status failure".to_owned(),
            ));
        }
        tokio::time::sleep(self.delay).await;
        let mut state = self.state.lock().unwrap();
        let status = state.statuses.pop_front().unwrap_or_else(|| {
            state
                .last
                .clone()
                .expect("scripted backend always has a status")
        });
        state.last = Some(status.clone());
        Ok(status)
    }

    async fn run_watch(
        &self,
        _target: Option<&str>,
        _params: RunWatchParams,
    ) -> Result<Self::Watch, crate::native_v2_cli::NativeV2CliError> {
        Ok(PendingSubscription(PhantomData))
    }

    async fn run_logs(
        &self,
        _target: Option<&str>,
        _params: RunLogsParams,
    ) -> Result<Self::Logs, crate::native_v2_cli::NativeV2CliError> {
        Ok(PendingSubscription(PhantomData))
    }

    async fn run_attach(
        &self,
        _target: Option<&str>,
        _params: RunAttachParams,
    ) -> Result<Self::Attach, crate::native_v2_cli::NativeV2CliError> {
        Ok(PendingSubscription(PhantomData))
    }

    async fn run_force(
        &self,
        _target: Option<&str>,
        params: RunForceParams,
    ) -> Result<CliRunForceResult, crate::native_v2_cli::NativeV2CliError> {
        let mut state = self.state.lock().unwrap();
        state.forced.push(params.run_id);
        Ok(state
            .last
            .clone()
            .unwrap_or_else(|| status(json!({"phase": "admitted"}))))
    }

    async fn run_resume(
        &self,
        _target: Option<&str>,
        params: RunResumeParams,
    ) -> Result<RunResumeResult, crate::native_v2_cli::NativeV2CliError> {
        let mut state = self.state.lock().unwrap();
        if state.fail_next_resume {
            state.fail_next_resume = false;
            return Err(crate::native_v2_cli::NativeV2CliError::Protocol(
                "transient resume failure".to_owned(),
            ));
        }
        let successor = RunId::new("run-successor");
        state.resumed.push(params);
        Ok(RunResumeResult {
            run_id: successor.clone(),
            resumed_from: RunId::new("run-scripted"),
        })
    }
}

#[test]
fn delivery_receipt_is_found_in_a_wrapped_terminal_output() {
    let output = json!({
        "message": "published",
        "delivery": {
            "version": "v2",
            "mode": "push",
            "outcome": "pushed",
            "repository": "acme/project",
            "targetBranch": "main",
            "headBranch": "zeroshot/run",
            "headRevision": "a".repeat(40),
        }
    });
    let receipt = DeliveryReceipt::from_terminal_output(&output).unwrap();
    assert_eq!(receipt.repository, "acme/project");
    assert_eq!(receipt.head_branch.as_deref(), Some("zeroshot/run"));
    assert_eq!(receipt.delivered_revision(), Some("a".repeat(40)));
    assert_eq!(
        DeliveryReceipt::from_terminal_output(&json!({"message": "not a delivery"})),
        None
    );
}

fn finished_with_judge_output() -> CliRunStatusResult {
    status(json!({
        "phase": "finished",
        "terminalResult": {
            "status": "succeeded",
            "output": {
                "criterion": "correctness",
                "score": 4.0,
                "reasoning": "The captured patch adds the guard.",
                "errors": []
            }
        },
        "metadata": {
            "tokenUsage": {
                "inputTokens": 12,
                "outputTokens": 4,
                "complete": true
            }
        }
    }))
}

#[tokio::test]
async fn platform_runner_polls_queue_running_and_terminal_states() {
    let backend = Arc::new(ScriptedBackend::new(
        vec![queued(), running(), finished_succeeded()],
        Duration::from_millis(5),
    ));
    let runner = PlatformRunner::new(Arc::clone(&backend), "platform")
        .with_poll_interval(Duration::from_millis(1));
    let run = runner
        .submit(BackendSampleRequest {
            sample_id: "sample-1".to_owned(),
            concurrency: 3,
            request: prepared_request(),
        })
        .await
        .unwrap();
    let outcome = runner.wait(&run).await.unwrap();
    assert_eq!(outcome.status, SampleStatus::Succeeded);
    assert_eq!(outcome.run_id.as_str(), "run-scripted");
    assert_eq!(outcome.timings.concurrency, 3);
    assert!(outcome.timings.started_at_ms.unwrap() > run.submitted_at_ms);
    assert!(outcome.timings.queue_wait_ms < outcome.timings.wall_clock_ms);
    assert_eq!(outcome.token_usage.unwrap().input_tokens.get(), 12);
    assert_eq!(outcome.delivered_revision, Some("b".repeat(40)));
    assert_eq!(outcome.delivered_branch.as_deref(), Some("zeroshot/run"));
}

#[tokio::test]
async fn platform_runner_maps_a_failed_terminal_result() {
    let backend = Arc::new(ScriptedBackend::new(
        vec![queued(), running(), finished_failed()],
        Duration::from_millis(1),
    ));
    let runner =
        PlatformRunner::new(backend, "platform").with_poll_interval(Duration::from_millis(1));
    let run = runner
        .submit(BackendSampleRequest {
            sample_id: "sample-failed".to_owned(),
            concurrency: 1,
            request: prepared_request(),
        })
        .await
        .unwrap();
    let outcome = runner.wait(&run).await.unwrap();
    assert_eq!(outcome.status, SampleStatus::Failed);
    assert_eq!(outcome.failure_reason.as_deref(), Some("worker_failed"));
    assert_eq!(outcome.delivered_revision, None);
}

#[tokio::test]
async fn platform_runner_enforces_a_deadline_and_requests_stop() {
    let backend = Arc::new(ScriptedBackend::new(
        vec![queued()],
        Duration::from_millis(5),
    ));
    let runner = PlatformRunner::new(Arc::clone(&backend), "platform")
        .with_poll_interval(Duration::from_millis(1))
        .with_deadline(Duration::from_millis(10));
    let run = runner
        .submit(BackendSampleRequest {
            sample_id: "sample-timeout".to_owned(),
            concurrency: 1,
            request: prepared_request(),
        })
        .await
        .unwrap();
    assert!(runner.wait(&run).await.is_err());
    assert_eq!(backend.forced(), vec![RunId::new("run-scripted")]);
}

#[tokio::test]
async fn platform_runner_resume_supplies_fresh_connection_values() {
    let backend = Arc::new(ScriptedBackend::new(Vec::new(), Duration::ZERO));
    let runner = PlatformRunner::new(Arc::clone(&backend), "platform");
    let connections = BTreeMap::from([(
        ConnectionKey::new("provider").unwrap(),
        StaticConnectionValues::new(BTreeMap::from([(
            EnvironmentVariableName::new("PROVIDER_TOKEN").unwrap(),
            "fresh-secret".to_owned(),
        )]))
        .unwrap(),
    )]);
    let resumed = runner
        .resume(
            super::SampleRun {
                sample_id: "sample-resume".to_owned(),
                run_id: RunId::new("run-scripted"),
                target: Some("platform".to_owned()),
                submitted_at_ms: 1,
                concurrency: 2,
            },
            ResumeCredentials {
                connections: connections.clone(),
                github_token: Some("fresh-github-token".to_owned()),
                git_credentials: Some(GitTransportCredentials::Https {
                    username: "fresh-user".to_owned(),
                    password: "fresh-git-secret".to_owned(),
                }),
            },
        )
        .await
        .unwrap();
    assert_eq!(resumed.run_id.as_str(), "run-successor");
    let recorded = &backend.resumed()[0];
    assert_eq!(recorded.connections, connections);
    assert_eq!(recorded.github_token.as_deref(), Some("fresh-github-token"));
    assert_eq!(
        recorded.git_credentials,
        Some(GitTransportCredentials::Https {
            username: "fresh-user".to_owned(),
            password: "fresh-git-secret".to_owned(),
        })
    );
}

#[tokio::test]
async fn run_sample_against_the_platform_backend_writes_sample_json() {
    let backend = Arc::new(ScriptedBackend::new(
        vec![queued(), finished_succeeded()],
        Duration::from_millis(1),
    ));
    let runner =
        PlatformRunner::new(backend, "platform").with_poll_interval(Duration::from_millis(1));
    let directory = tempfile::tempdir().unwrap();
    let artifact = run_sample(
        &runner,
        BackendSampleRequest {
            sample_id: "sample-platform".to_owned(),
            concurrency: 2,
            request: prepared_request(),
        },
        directory.path(),
        |_, _| None,
    )
    .await
    .unwrap();
    assert_eq!(artifact.outcome.run_id.as_str(), "run-scripted");
    assert_eq!(SampleArtifact::read(directory.path()).unwrap(), artifact);
}

#[tokio::test]
async fn driver_schedules_samples_and_skips_completed_work() {
    let runner = FakeRunner::new();
    let directory = tempfile::tempdir().unwrap();
    let samples = vec![
        EvalSamplePlan::new(
            "sample-a",
            directory.path().join("a"),
            FakeSampleRequest {
                run_id: Some(RunId::new("run-a")),
                concurrency: 2,
                ..FakeSampleRequest::succeeded("sample-a")
            },
        ),
        EvalSamplePlan::new(
            "sample-b",
            directory.path().join("b"),
            FakeSampleRequest {
                run_id: Some(RunId::new("run-b")),
                concurrency: 2,
                ..FakeSampleRequest::succeeded("sample-b")
            },
        ),
    ];
    let report = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(2),
        journal_directory: directory.path().to_owned(),
        samples,
    })
    .await
    .unwrap();
    assert_eq!(report.capacity.concurrency, 2);
    assert_eq!(report.capacity.capacity_source.label(), "concurrency");
    assert_eq!(report.artifacts.len(), 2);
    assert!(report.failures.is_empty());
    assert!(
        report
            .artifacts
            .iter()
            .all(|artifact| artifact.outcome.timings.concurrency == 2)
    );
    assert_eq!(runner.submitted().await.len(), 2);

    let rerun = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(1),
        journal_directory: directory.path().to_owned(),
        samples: vec![
            EvalSamplePlan::new(
                "sample-a",
                directory.path().join("a"),
                FakeSampleRequest::succeeded("sample-a"),
            ),
            EvalSamplePlan::new(
                "sample-b",
                directory.path().join("b"),
                FakeSampleRequest::succeeded("sample-b"),
            ),
        ],
    })
    .await
    .unwrap();
    assert_eq!(rerun.artifacts.len(), 2);
    assert_eq!(runner.submitted().await.len(), 2);
    assert_eq!(rerun.capacity_warning.unwrap().previous, 2);
}

#[tokio::test]
async fn terminal_failed_reattach_uses_workspace_recovery() {
    let backend = Arc::new(ScriptedBackend::new(
        vec![finished_failed(), finished_succeeded()],
        Duration::from_millis(1),
    ));
    let runner = PlatformRunner::new(Arc::clone(&backend), "platform")
        .with_poll_interval(Duration::from_millis(1));
    let directory = tempfile::tempdir().unwrap();
    let run = runner
        .submit(BackendSampleRequest {
            sample_id: "sample-failed-resume".to_owned(),
            concurrency: 1,
            request: prepared_request(),
        })
        .await
        .unwrap();
    let mut journal = crate::native_v2_eval::ResumeJournal::default();
    journal.claim("sample-failed-resume", run);
    journal.save(directory.path()).unwrap();
    let report = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(1),
        journal_directory: directory.path().to_owned(),
        samples: vec![EvalSamplePlan::<PlatformRunner<ScriptedBackend>> {
            sample_id: "sample-failed-resume".to_owned(),
            directory: directory.path().join("sample"),
            request: None,
            capture: None,
            concurrency: 1,
            resume_credentials: Default::default(),
        }],
    })
    .await
    .unwrap();
    assert!(report.failures.is_empty());
    assert_eq!(report.artifacts.len(), 1);
    assert_eq!(report.artifacts[0].outcome.status, SampleStatus::Succeeded);
    assert_eq!(backend.submits(), 1);
    assert_eq!(backend.resumed().len(), 1);
}

#[tokio::test]
async fn transient_wait_failure_keeps_the_run_resumable_without_duplicate_submission() {
    let backend = Arc::new(ScriptedBackend::new(
        vec![queued(), running(), finished_succeeded()],
        Duration::from_millis(1),
    ));
    let runner = PlatformRunner::new(Arc::clone(&backend), "platform")
        .with_poll_interval(Duration::from_millis(1));
    let directory = tempfile::tempdir().unwrap();
    let plan = || {
        EvalSamplePlan::<PlatformRunner<ScriptedBackend>>::new(
            "sample-retry",
            directory.path().join("sample"),
            BackendSampleRequest {
                sample_id: "sample-retry".to_owned(),
                concurrency: 1,
                request: prepared_request(),
            },
        )
    };
    backend.fail_next_status();
    let first = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(1),
        journal_directory: directory.path().to_owned(),
        samples: vec![plan()],
    })
    .await
    .unwrap();
    assert_eq!(first.failures.len(), 1);
    assert_eq!(backend.submits(), 1);
    assert!(matches!(
        crate::native_v2_eval::ResumeJournal::load(directory.path())
            .unwrap()
            .action("sample-retry"),
        crate::native_v2_eval::ResumeAction::Resume(_)
    ));

    let second = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(1),
        journal_directory: directory.path().to_owned(),
        samples: vec![plan()],
    })
    .await
    .unwrap();
    assert!(second.failures.is_empty());
    assert_eq!(second.artifacts.len(), 1);
    assert_eq!(backend.submits(), 1);
    assert!(backend.resumed().is_empty());
}

#[tokio::test]
async fn transient_resume_failure_does_not_fall_back_to_a_new_submission() {
    let backend = Arc::new(ScriptedBackend::new(
        vec![finished_succeeded()],
        Duration::from_millis(1),
    ));
    let runner = PlatformRunner::new(Arc::clone(&backend), "platform")
        .with_poll_interval(Duration::from_millis(1));
    let directory = tempfile::tempdir().unwrap();
    let plan = || {
        EvalSamplePlan::<PlatformRunner<ScriptedBackend>>::new(
            "sample-resume-error",
            directory.path().join("sample"),
            BackendSampleRequest {
                sample_id: "sample-resume-error".to_owned(),
                concurrency: 1,
                request: prepared_request(),
            },
        )
    };
    backend.fail_next_status_lost();
    let _ = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(1),
        journal_directory: directory.path().to_owned(),
        samples: vec![plan()],
    })
    .await
    .unwrap();
    backend.fail_next_status_lost();
    backend.fail_next_resume();
    let failed = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(1),
        journal_directory: directory.path().to_owned(),
        samples: vec![plan()],
    })
    .await
    .unwrap();
    assert_eq!(failed.failures.len(), 1);
    assert_eq!(backend.submits(), 1);
}

#[tokio::test]
async fn driver_resumes_an_interrupted_sample_without_a_fresh_request() {
    let runner = FakeRunner::new();
    let old = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-interrupted")),
            concurrency: 2,
            ..FakeSampleRequest::succeeded("sample-resume")
        })
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut journal = crate::native_v2_eval::ResumeJournal::default();
    journal.claim("sample-resume", old);
    journal.save(directory.path()).unwrap();
    let report = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(1),
        journal_directory: directory.path().to_owned(),
        samples: vec![EvalSamplePlan::<FakeRunner> {
            sample_id: "sample-resume".to_owned(),
            directory: directory.path().join("sample"),
            request: None,
            capture: None,
            concurrency: 1,
            resume_credentials: Default::default(),
        }],
    })
    .await
    .unwrap();
    assert_eq!(report.artifacts.len(), 1);
    assert!(report.failures.is_empty());
    assert_eq!(runner.submitted().await.len(), 1);
    assert_eq!(
        report.artifacts[0].outcome.run_id.as_str(),
        "run-interrupted"
    );
}

#[tokio::test]
async fn failed_submission_keeps_the_previous_sample_artifacts() {
    let backend = Arc::new(ScriptedBackend::new(
        vec![finished_succeeded()],
        Duration::from_millis(1),
    ));
    backend.fail_next_submit();
    let runner =
        PlatformRunner::new(backend, "platform").with_poll_interval(Duration::from_millis(1));
    let directory = tempfile::tempdir().unwrap();
    let sample = directory.path().join("sample");
    std::fs::create_dir_all(sample.join("judge")).unwrap();
    std::fs::write(sample.join("patch.diff"), "previous patch").unwrap();
    std::fs::write(sample.join("judge").join("previous.txt"), "previous judge").unwrap();
    SampleArtifact::new(
        "sample-submit-failed",
        super::tests::sample_outcome_for_test(SampleStatus::Failed),
    )
    .write(&sample)
    .unwrap();
    let report = run_eval(EvalRunRequest {
        runner: &runner,
        capacity_source: None,
        concurrency: Some(1),
        journal_directory: directory.path().to_owned(),
        samples: vec![EvalSamplePlan::<PlatformRunner<ScriptedBackend>>::new(
            "sample-submit-failed",
            &sample,
            BackendSampleRequest {
                sample_id: "sample-submit-failed".to_owned(),
                concurrency: 1,
                request: prepared_request(),
            },
        )],
    })
    .await
    .unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(sample.join("sample.json").is_file());
    assert_eq!(
        std::fs::read_to_string(sample.join("patch.diff")).unwrap(),
        "previous patch"
    );
    assert_eq!(
        std::fs::read_to_string(sample.join("judge").join("previous.txt")).unwrap(),
        "previous judge"
    );
}

#[tokio::test]
async fn failed_sample_retry_does_not_reuse_an_old_artifact_after_interruption() {
    let runner = FakeRunner::new();
    let new_run = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("new-run")),
            ..FakeSampleRequest::succeeded("sample-stale")
        })
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let sample_directory = directory.path().join("sample");
    std::fs::create_dir_all(sample_directory.join("judge")).unwrap();
    std::fs::write(sample_directory.join("patch.diff"), "stale patch").unwrap();
    std::fs::write(
        sample_directory.join("judge").join("old.txt"),
        "stale judge",
    )
    .unwrap();
    let mut old_outcome = super::tests::sample_outcome_for_test(SampleStatus::Failed);
    old_outcome.run_id = RunId::new("old-run");
    SampleArtifact::new("sample-stale", old_outcome)
        .write(&sample_directory)
        .unwrap();
    let mut journal = crate::native_v2_eval::ResumeJournal::default();
    journal.claim("sample-stale", new_run);
    let journal = tokio::sync::Mutex::new(journal);
    let artifact = super::run_eval_sample(
        &runner,
        EvalSamplePlan::<FakeRunner> {
            sample_id: "sample-stale".to_owned(),
            directory: sample_directory.clone(),
            request: None,
            capture: None,
            concurrency: 1,
            resume_credentials: Default::default(),
        },
        &journal,
        directory.path(),
    )
    .await
    .unwrap();
    assert_eq!(artifact.outcome.run_id.as_str(), "new-run");
    assert_eq!(artifact.outcome.status, SampleStatus::Succeeded);
    assert!(!sample_directory.join("patch.diff").exists());
    assert!(!sample_directory.join("judge").exists());
}

#[tokio::test]
async fn capture_failure_keeps_the_measured_sample_artifact() {
    let runner = FakeRunner::new();
    let directory = tempfile::tempdir().unwrap();
    let capture = crate::native_v2_eval::CaptureRequest::new(
        directory.path(),
        "https://example.invalid/acme/project.git",
        "a".repeat(40),
        DeliveryReceipt::default(),
    );
    let artifact = run_sample(
        &runner,
        FakeSampleRequest {
            run_id: Some(RunId::new("run-capture-failed")),
            ..FakeSampleRequest::succeeded("sample-capture-failed")
        },
        directory.path(),
        |_, _| Some(capture),
    )
    .await
    .unwrap();
    assert!(artifact.captured.is_none());
    assert!(artifact.capture_error.is_some());
    let stored = SampleArtifact::read(directory.path()).unwrap();
    assert_eq!(stored.outcome.run_id.as_str(), "run-capture-failed");
    assert!(stored.capture_error.is_some());
}

#[tokio::test]
async fn fake_runner_reports_timings_tokens_and_delivery() {
    let runner = FakeRunner::new();
    let receipt = DeliveryReceipt {
        repository: "acme/project".to_owned(),
        mode: Some("push".to_owned()),
        outcome: Some("pushed".to_owned()),
        head_branch: Some("zeroshot/run".to_owned()),
        head_revision: Some("b".repeat(40)),
        ..DeliveryReceipt::default()
    };
    let run = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-1")),
            queue_wait_ms: 15,
            wall_clock_ms: 120,
            token_usage: Some(usage()),
            delivery: Some(receipt.clone()),
            ..FakeSampleRequest::succeeded("sample-1")
        })
        .await
        .unwrap();
    let outcome = runner.wait(&run).await.unwrap();
    assert_eq!(outcome.status, SampleStatus::Succeeded);
    assert_eq!(outcome.run_id.as_str(), "run-1");
    assert_eq!(outcome.timings.queue_wait_ms, 15);
    assert_eq!(outcome.timings.wall_clock_ms, 120);
    assert_eq!(
        outcome.timings.started_at_ms,
        Some(run.submitted_at_ms + 15)
    );
    assert_eq!(outcome.token_usage, Some(usage()));
    assert_eq!(outcome.delivered_revision, Some("b".repeat(40)));
    assert_eq!(outcome.delivered_branch.as_deref(), Some("zeroshot/run"));
    assert_eq!(outcome.delivery, Some(receipt));
}

#[tokio::test]
async fn fake_runner_reports_failure_and_stop() {
    let runner = FakeRunner::new();
    let run = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-2")),
            ..FakeSampleRequest::failed("sample-2", "worker_failed")
        })
        .await
        .unwrap();
    let outcome = runner.wait(&run).await.unwrap();
    assert_eq!(outcome.status, SampleStatus::Failed);
    assert_eq!(outcome.failure_reason.as_deref(), Some("worker_failed"));

    let stopped = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-3")),
            ..FakeSampleRequest::succeeded("sample-3")
        })
        .await
        .unwrap();
    runner.stop(&stopped).await.unwrap();
    assert_eq!(runner.stopped().await, vec![RunId::new("run-3")]);
    assert!(runner.wait(&stopped).await.is_err());
}

#[tokio::test]
async fn run_sample_writes_the_sample_artifact() {
    let runner = FakeRunner::new();
    let directory = tempfile::tempdir().unwrap();
    let artifact = run_sample(
        &runner,
        FakeSampleRequest {
            run_id: Some(RunId::new("run-direct")),
            ..FakeSampleRequest::succeeded("sample-direct")
        },
        directory.path(),
        |_, _| None,
    )
    .await
    .unwrap();
    assert_eq!(artifact.sample_id, "sample-direct");
    assert_eq!(artifact.outcome.run_id.as_str(), "run-direct");
    assert_eq!(SampleArtifact::read(directory.path()).unwrap(), artifact);
}

#[tokio::test]
async fn sample_artifact_round_trips_a_partial_eval_sample() {
    let runner = FakeRunner::new();
    let run = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-4")),
            token_usage: Some(usage()),
            ..FakeSampleRequest::succeeded("sample-4")
        })
        .await
        .unwrap();
    let outcome: SampleOutcome = runner.wait(&run).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let artifact = SampleArtifact::new("sample-4", outcome);
    let path = artifact.write(directory.path()).unwrap();
    assert_eq!(path.file_name().unwrap(), SAMPLE_FILE);
    let stored = SampleArtifact::read(directory.path()).unwrap();
    assert_eq!(stored, artifact);
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(value["runId"], "run-4");
    assert_eq!(value["timings"]["wallClockMs"], 1);
    assert_eq!(value["tokenUsage"]["inputTokens"], 11);
}

#[tokio::test]
async fn cli_backend_path_runs_the_selected_platform_runner() {
    let temporary = tempfile::tempdir().unwrap();
    let source = include_str!("../../fixtures/eval/synthetic.toml")
        .replace("runner = \"fake\"", "runner = \"platform\"")
        .replace("judgeModel = \"synthetic-judge\"\n", "")
        .replace("judgeTemplate = \"eval-judge\"\n", "");
    let manifest_path = temporary.path().join("platform.toml");
    std::fs::write(&manifest_path, &source).unwrap();
    let manifest = EvalManifest::parse(&source).unwrap();
    assert_eq!(select_runner(&manifest).unwrap(), RunnerSelection::Platform);

    let backend = Arc::new(ScriptedBackend::new(
        vec![queued(), finished_succeeded()],
        Duration::from_millis(1),
    ));
    let data = run_manifest_with_backend(BackendRunRequest {
        command: EvalRunCommand {
            manifest: manifest_path,
            directory: temporary.path().to_path_buf(),
            id: Some("fixture-platform".to_owned()),
            scorer: None,
            judge_template: None,
            judge_model: None,
        },
        backend,
        target: Some("platform".to_owned()),
        capacity_source: None,
        concurrency: Some(2),
        journal_directory: temporary.path().join("run"),
        request: |_: &crate::native_v2_eval::EvalId, sample_id: &str, _: &_, _: u32, _: &_| {
            BackendSampleRequest {
                sample_id: sample_id.to_owned(),
                concurrency: 1,
                request: prepared_request(),
            }
        },
    })
    .await
    .unwrap();
    assert_eq!(data.samples.len(), 6);
    assert!(
        data.samples
            .iter()
            .all(|sample| sample.status == SampleStatus::Succeeded)
    );
    let report = data.report.expect("platform run writes a report");
    let capacity = report.capacity.expect("report records capacity provenance");
    assert_eq!(capacity.concurrency, 2);
    assert_eq!(capacity.capacity_source.label(), "concurrency");
}

#[tokio::test]
async fn local_judge_graph_executor_applies_the_model_and_parses_terminal_output() {
    use crate::native_v2_contract::{DeclaredConnections, ModelId, NodeRuntimeBinding, RuntimePlan};
    use crate::native_v2_eval::{
        EvalCriterion, EvalSample, Judge, JudgeRequest, LocalJudgeGraphExecutor, SampleStatus,
        SampleTiming, TemplateJudge, TokenUsage, resolve_judge_template,
    };
    use openengine_cluster_protocol::{NodeName, RunSize, SessionScope};
    use std::collections::BTreeMap;

    let backend = Arc::new(ScriptedBackend::new(
        vec![queued(), finished_with_judge_output()],
        Duration::from_millis(1),
    ));
    let node = NodeName::new("judge").expect("node");
    let mut nodes = BTreeMap::new();
    nodes.insert(
        node.clone(),
        NodeRuntimeBinding::Agent {
            lane: None,
            model: ModelId::new("authored-model").expect("model"),
            effort: None,
            session_scope: SessionScope::Execution,
            connections: DeclaredConnections::empty(),
        },
    );
    let runtime = crate::native_v2_candidate::test_support::codex_runtime(RunSize::Small, nodes);
    let live_workspace = tempfile::tempdir().expect("live workspace");
    let executor =
        LocalJudgeGraphExecutor::new(Arc::clone(&backend), runtime, live_workspace.path());
    let template = resolve_judge_template("eval-judge").expect("template");
    let judge = TemplateJudge::new(executor, template);
    let sample = EvalSample {
        id: "baseline-0000".to_owned(),
        run_id: "run".to_owned(),
        arm_id: "baseline".to_owned(),
        run_index: 0,
        status: SampleStatus::Succeeded,
        timing: SampleTiming::default(),
        tokens: TokenUsage::default(),
        delivered_revision: None,
        failure_reason: None,
        patch: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        judge_model: None,
        judge_template: None,
        judge_checkout: None,
        judge_executor: None,
    };
    let request = JudgeRequest::new(
        sample,
        vec![EvalCriterion {
            id: "correctness".to_owned(),
            description: "correct".to_owned(),
            weight: 1.0,
        }],
        "patch",
        "per-arm-model",
    );
    let result = judge.judge(request).await.expect("judge executes");
    assert_eq!(result.judgements["correctness"].score, 4.0);
    let submitted = backend.last_request().expect("request submitted");
    let nodes = match submitted.intent.runtime {
        RuntimePlan::Codex { nodes, .. } => nodes,
        _ => panic!("codex runtime"),
    };
    match nodes.get(&node).expect("judge binding") {
        NodeRuntimeBinding::Agent { model, .. } => assert_eq!(model.as_str(), "per-arm-model"),
        _ => panic!("agent binding"),
    }
    assert!(
        submitted
            .cwd
            .expect("scratch cwd")
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("zeroshot-judge-"))
    );
}

#[tokio::test]
async fn local_judge_graph_executor_rejects_connection_requirements_before_submission() {
    use crate::native_v2_contract::{
        DeclaredConnections, DeclaredEnvironment, ModelId, NodeRuntimeBinding,
    };
    use crate::native_v2_eval::{JudgeGraphExecutor, LocalJudgeGraphExecutor, resolve_judge_template};
    use openengine_cluster_protocol::{EnvironmentVariableName, NodeName, RunSize, SessionScope};
    use std::collections::BTreeMap;

    let backend = Arc::new(ScriptedBackend::new(Vec::new(), Duration::ZERO));
    let node = NodeName::new("judge").expect("node");
    let mut nodes = BTreeMap::new();
    nodes.insert(
        node,
        NodeRuntimeBinding::Agent {
            lane: None,
            model: ModelId::new("authored-model").expect("model"),
            effort: None,
            session_scope: SessionScope::Execution,
            connections: DeclaredConnections::single(
                "gateway",
                DeclaredEnvironment::new(
                    [EnvironmentVariableName::new("GATEWAY_API_KEY").unwrap()],
                )
                .unwrap(),
            )
            .unwrap(),
        },
    );
    let runtime = crate::native_v2_candidate::test_support::codex_runtime(RunSize::Small, nodes);
    let live_workspace = tempfile::tempdir().expect("live workspace");
    let executor =
        LocalJudgeGraphExecutor::new(Arc::clone(&backend), runtime, live_workspace.path());
    let template = resolve_judge_template("eval-judge").expect("template");
    let error = executor
        .execute(
            &template,
            "per-arm-model",
            json!({"criterion":"correctness"}),
        )
        .await
        .expect_err("connection requirements fail closed");
    assert!(error.to_string().contains("connection requirements"));
    assert_eq!(backend.submits(), 0);
}
