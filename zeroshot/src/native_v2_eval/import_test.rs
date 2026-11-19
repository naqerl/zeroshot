use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::process::Command;
use std::sync::Mutex;

use async_trait::async_trait;
use openengine_cluster_protocol::{
    RunAttachEventNotification, RunAttachParams, RunForceParams, RunListParams,
    RunLogEventNotification, RunLogsParams, RunResumeParams, RunResumeResult, RunStatusParams,
    RunSubmitResult, RunWatchParams, SubscriptionCloseReason,
};
use serde_json::{Value, json};

use super::{EvalImportRunsCommand, QUEUE_NOTE, RunSelection, import_runs, parse_runs_file};
use crate::native_v2_cli::{
    CliRunForceResult, CliRunListResult, CliRunStatusResult, CliRunWatchEventNotification,
    CliSubscription, CliSubscriptionItem, NativeV2CliBackend, NativeV2CliError, PreparedRunRequest,
    TargetAdd,
};
use crate::native_v2_eval::{EvalError, EvalId, EvalStore, SampleStatus};

const MANIFEST: &str = include_str!("../../fixtures/eval/synthetic.toml");

/// Read-only platform double: `run list`, `run status` by id, and a finite `run logs` replay.
#[derive(Default)]
struct MockPlatform {
    runs: BTreeMap<String, CliRunStatusResult>,
    logs: BTreeMap<String, Vec<u64>>,
    calls: Mutex<Vec<String>>,
}

impl MockPlatform {
    fn with(mut self, status: CliRunStatusResult, log_times: &[u64]) -> Self {
        let id = status.run_id.as_str().to_owned();
        if !log_times.is_empty() {
            self.logs.insert(id.clone(), log_times.to_vec());
        }
        self.runs.insert(id, status);
        self
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

struct Replay<E>(VecDeque<CliSubscriptionItem<E>>);

#[async_trait]
impl<E: Send> CliSubscription<E> for Replay<E> {
    async fn next(&mut self) -> Result<Option<CliSubscriptionItem<E>>, NativeV2CliError> {
        Ok(self.0.pop_front())
    }
}

fn closed<E>() -> Replay<E> {
    Replay(VecDeque::from([CliSubscriptionItem::Closed {
        reason: SubscriptionCloseReason::Done,
    }]))
}

#[async_trait]
impl NativeV2CliBackend for MockPlatform {
    type Watch = Replay<CliRunWatchEventNotification>;
    type Logs = Replay<RunLogEventNotification>;
    type Attach = Replay<RunAttachEventNotification>;

    async fn target_add(&self, _request: TargetAdd) -> Result<(), NativeV2CliError> {
        unreachable!("import never adds targets")
    }

    async fn target_login(&self, _name: &str) -> Result<(), NativeV2CliError> {
        unreachable!("import never logs in")
    }

    async fn run_submit(
        &self,
        _target: Option<&str>,
        _request: PreparedRunRequest,
    ) -> Result<RunSubmitResult, NativeV2CliError> {
        unreachable!("import never submits runs")
    }

    async fn run_list(
        &self,
        target: Option<&str>,
        _params: RunListParams,
    ) -> Result<CliRunListResult, NativeV2CliError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("list {}", target.unwrap_or_default()));
        Ok(CliRunListResult {
            runs: self.runs.values().cloned().collect(),
        })
    }

    async fn run_status(
        &self,
        _target: Option<&str>,
        params: RunStatusParams,
    ) -> Result<CliRunStatusResult, NativeV2CliError> {
        let id = params.run_id.as_str().to_owned();
        self.calls.lock().unwrap().push(format!("status {id}"));
        self.runs
            .get(&id)
            .cloned()
            .ok_or(NativeV2CliError::RunNotFound { run_id: id })
    }

    async fn run_watch(
        &self,
        _target: Option<&str>,
        _params: RunWatchParams,
    ) -> Result<Self::Watch, NativeV2CliError> {
        unreachable!("import never watches runs")
    }

    async fn run_logs(
        &self,
        _target: Option<&str>,
        params: RunLogsParams,
    ) -> Result<Self::Logs, NativeV2CliError> {
        let id = params.run_id.as_str().to_owned();
        self.calls.lock().unwrap().push(format!("logs {id}"));
        let mut items = VecDeque::new();
        for (index, at) in self.logs.get(&id).into_iter().flatten().enumerate() {
            let event: RunLogEventNotification = serde_json::from_value(json!({
                "subscriptionId": "logs-1",
                "runId": id,
                "cursor": format!("v2:{}", index + 1),
                "timestamp": at,
                "record": {"level": "info", "target": "worker", "message": "step"}
            }))
            .expect("valid log event");
            items.push_back(CliSubscriptionItem::Event(event));
        }
        items.extend(closed().0);
        Ok(Replay(items))
    }

    async fn run_attach(
        &self,
        _target: Option<&str>,
        _params: RunAttachParams,
    ) -> Result<Self::Attach, NativeV2CliError> {
        unreachable!("import never attaches")
    }

    async fn run_force(
        &self,
        _target: Option<&str>,
        _params: RunForceParams,
    ) -> Result<CliRunForceResult, NativeV2CliError> {
        unreachable!("import never forces runs")
    }

    async fn run_resume(
        &self,
        _target: Option<&str>,
        _params: RunResumeParams,
    ) -> Result<RunResumeResult, NativeV2CliError> {
        unreachable!("import never resumes runs")
    }
}

fn status(run_id: &str, title: &str, base: &str, status: Value) -> CliRunStatusResult {
    serde_json::from_value(json!({
        "runId": run_id,
        "title": title,
        "size": "small",
        "atCursor": "v2:9",
        "source": {
            "repository": "acme/project",
            "branch": "main",
            "revision": base,
            "remote": {"url": "https://git.example/acme/project.git"}
        },
        "status": status,
    }))
    .expect("valid status")
}

fn succeeded(run_id: &str, base: &str, head: &str, usage: Value) -> CliRunStatusResult {
    status(
        run_id,
        &format!("baseline: {run_id}"),
        base,
        json!({
            "phase": "finished",
            "terminalResult": {
                "status": "succeeded",
                "output": {
                    "delivery": {
                        "version": "v3",
                        "mode": "push",
                        "outcome": "pushed",
                        "repository": "acme/project",
                        "targetBranch": "main",
                        "headBranch": "zeroshot/run",
                        "headRevision": head
                    }
                }
            },
            "metadata": {"tokenUsage": usage}
        }),
    )
}

fn failed(run_id: &str, base: &str) -> CliRunStatusResult {
    status(
        run_id,
        "baseline: failed",
        base,
        json!({
            "phase": "finished",
            "terminalResult": {"status": "failed", "reason": "worker_failed"},
            "metadata": {}
        }),
    )
}

fn full_usage() -> Value {
    json!({
        "inputTokens": 1000,
        "outputTokens": 200,
        "cacheReadInputTokens": 300,
        "cacheCreationInputTokens": 40,
        "complete": true
    })
}

fn git(directory: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(directory)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Eval Test")
        .env("GIT_AUTHOR_EMAIL", "eval@example.invalid")
        .env("GIT_COMMITTER_NAME", "Eval Test")
        .env("GIT_COMMITTER_EMAIL", "eval@example.invalid")
        .args(arguments)
        .output()
        .expect("Git must be available for the import test");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

/// A bare remote with `main` at the base and `zeroshot/run` at the delivered head.
struct Remote {
    url: String,
    base: String,
    head: String,
}

fn remote(root: &Path) -> Remote {
    let bare = root.join("remote.git");
    git(root, &["init", "--bare", "remote.git"]);
    let work = root.join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &["init", "-b", "main"]);
    std::fs::write(work.join("file.txt"), "base\n").unwrap();
    git(&work, &["add", "file.txt"]);
    git(&work, &["commit", "-m", "base"]);
    let base = git(&work, &["rev-parse", "HEAD"]);
    git(&work, &["remote", "add", "origin", bare.to_str().unwrap()]);
    git(&work, &["push", "origin", "main"]);
    git(&work, &["checkout", "-b", "zeroshot/run"]);
    std::fs::write(work.join("file.txt"), "delivered\n").unwrap();
    git(&work, &["commit", "-am", "delivered"]);
    let head = git(&work, &["rev-parse", "HEAD"]);
    git(&work, &["push", "origin", "zeroshot/run"]);
    Remote {
        url: bare.to_str().unwrap().to_owned(),
        base,
        head,
    }
}

fn command(root: &Path, runs: &[&str]) -> EvalImportRunsCommand {
    let manifest = root.join("eval.toml");
    std::fs::write(&manifest, MANIFEST).unwrap();
    EvalImportRunsCommand {
        id: "imported".to_owned(),
        manifest: Some(manifest),
        runs: runs.iter().map(|run| (*run).to_owned()).collect(),
        ..EvalImportRunsCommand::default()
    }
}

#[test]
fn runs_file_takes_ids_optional_minutes_and_comments() {
    let runs = parse_runs_file("# baseline\nrun-a 12.5\n\nrun-b,7 # slow\nrun-c\n").unwrap();
    assert_eq!(
        runs,
        vec![
            RunSelection {
                run_id: "run-a".to_owned(),
                minutes: Some(12.5)
            },
            RunSelection {
                run_id: "run-b".to_owned(),
                minutes: Some(7.0)
            },
            RunSelection {
                run_id: "run-c".to_owned(),
                minutes: None
            },
        ]
    );
    assert!(parse_runs_file("run-a soon").is_err());
    assert!(parse_runs_file("run-a -1").is_err());
}

#[tokio::test]
async fn finished_runs_become_unjudged_baseline_samples_with_t_tokens_and_patch() {
    let root = tempfile::tempdir().unwrap();
    let git_remote = remote(root.path());
    let backend = MockPlatform::default()
        .with(
            succeeded("run-a", &git_remote.base, &git_remote.head, full_usage()),
            &[1_000, 61_000, 181_000],
        )
        .with(
            succeeded(
                "run-b",
                &git_remote.base,
                &git_remote.head,
                json!({"inputTokens": 50, "outputTokens": 5, "complete": false}),
            ),
            &[5_000, 65_000],
        );
    let store = EvalStore::for_workspace(root.path());
    let mut request = command(root.path(), &["run-a", "run-b"]);
    request.capture_remote = Some(git_remote.url.clone());

    let summary = import_runs(&store, &request, &backend).await.unwrap();
    assert!(summary.created);
    assert_eq!(summary.arm_id, "baseline");
    assert_eq!(summary.target, "platform");
    assert_eq!(summary.imported.len(), 2);
    assert!(summary.next.contains("zeroshot eval judge imported"));
    assert!(summary.pairing.contains("baseline = 2, candidate = 0"));

    let data = store.load(&EvalId::new("imported").unwrap()).unwrap();
    let a = data.samples.iter().find(|s| s.run_id == "run-a").unwrap();
    assert_eq!(a.id, "baseline-0000");
    assert_eq!(a.run_index, 0);
    assert_eq!(a.status, SampleStatus::Succeeded);
    assert_eq!(a.timing.wall_clock_ms, 180_000);
    assert_eq!(a.timing.started_at_ms, 1_000);
    assert_eq!(a.timing.queue_wait_ms, 0);
    assert_eq!(a.tokens.input, 1000);
    assert_eq!(a.tokens.output, 200);
    assert_eq!(a.tokens.cache_read, 300);
    assert_eq!(a.tokens.cache_write, 40);
    assert_eq!(a.tokens.cache, 340);
    assert_eq!(
        a.delivered_revision.as_deref(),
        Some(git_remote.head.as_str())
    );
    assert!(a.criteria.is_empty(), "imported samples are unjudged");
    assert_eq!(a.notes, vec![QUEUE_NOTE.to_owned()]);
    let patch = a.patch.as_ref().expect("the delivered patch is stored");
    let diff = store
        .read_patch(&EvalId::new("imported").unwrap(), &patch.path)
        .unwrap();
    assert!(diff.contains("-base"));
    assert!(diff.contains("+delivered"));

    let b = data.samples.iter().find(|s| s.run_id == "run-b").unwrap();
    assert_eq!(b.id, "baseline-0001");
    assert_eq!(b.timing.wall_clock_ms, 60_000);
    assert!(b.notes.iter().any(|note| note.contains("incomplete")));
    assert!(
        b.notes
            .iter()
            .any(|note| note.contains("no cache token counts"))
    );

    let report = data.report.expect("the report is rebuilt");
    let rendered = serde_json::to_string(&report).unwrap();
    assert!(
        rendered.contains("incomplete"),
        "per-arm notes reach the report"
    );
    // Read-only: status and logs only.
    assert!(
        backend
            .calls()
            .iter()
            .all(|call| call.starts_with("status ") || call.starts_with("logs "))
    );
}

#[tokio::test]
async fn missing_logs_failed_runs_and_runs_file_minutes_are_recorded_as_notes() {
    let root = tempfile::tempdir().unwrap();
    let base = "a".repeat(40);
    let backend = MockPlatform::default()
        .with(failed("run-failed", &base), &[])
        .with(
            status(
                "run-quiet",
                "baseline: quiet",
                &base,
                json!({
                    "phase": "finished",
                    "terminalResult": {"status": "succeeded", "output": {"value": 1}},
                    "metadata": {}
                }),
            ),
            &[],
        );
    let runs_file = root.path().join("runs.txt");
    std::fs::write(&runs_file, "run-quiet 4.5\n").unwrap();
    let store = EvalStore::for_workspace(root.path());
    let mut request = command(root.path(), &["run-failed"]);
    request.runs_file = Some(runs_file);

    let summary = import_runs(&store, &request, &backend).await.unwrap();
    assert_eq!(summary.imported.len(), 2);
    let data = store.load(&EvalId::new("imported").unwrap()).unwrap();

    let failed = data
        .samples
        .iter()
        .find(|s| s.run_id == "run-failed")
        .unwrap();
    assert_eq!(failed.status, SampleStatus::Failed);
    assert_eq!(failed.failure_reason.as_deref(), Some("worker_failed"));
    assert_eq!(failed.timing.wall_clock_ms, 0);
    assert!(
        failed
            .notes
            .iter()
            .any(|note| note.contains("no log records"))
    );
    assert!(
        failed
            .notes
            .iter()
            .any(|note| note.contains("no token usage"))
    );
    assert!(failed.patch.is_none());

    let quiet = data
        .samples
        .iter()
        .find(|s| s.run_id == "run-quiet")
        .unwrap();
    assert_eq!(quiet.timing.wall_clock_ms, 270_000);
    assert!(
        quiet
            .notes
            .iter()
            .any(|note| note.contains("from the runs file"))
    );
    assert!(
        quiet
            .notes
            .iter()
            .any(|note| note.contains("no delivery receipt"))
    );
    assert!(quiet.patch.is_none());
}

#[tokio::test]
async fn an_unfinished_run_rejects_the_whole_import_and_writes_nothing() {
    let root = tempfile::tempdir().unwrap();
    let base = "a".repeat(40);
    let backend = MockPlatform::default()
        .with(failed("run-done", &base), &[1, 2])
        .with(
            status(
                "run-live",
                "baseline: live",
                &base,
                json!({"phase": "queued"}),
            ),
            &[],
        );
    let store = EvalStore::for_workspace(root.path());
    let error = import_runs(
        &store,
        &command(root.path(), &["run-done", "run-live"]),
        &backend,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("run-live"), "{error}");
    assert!(matches!(
        store.load(&EvalId::new("imported").unwrap()),
        Err(EvalError::NotFound(_))
    ));
}

#[tokio::test]
async fn title_prefix_selects_finished_runs_and_duplicates_are_rejected_on_append() {
    let root = tempfile::tempdir().unwrap();
    let base = "a".repeat(40);
    let backend = MockPlatform::default()
        .with(failed("run-1", &base), &[1, 2])
        .with(
            status("run-2", "baseline: second", &base, json!({"phase": "queued"})),
            &[],
        )
        .with(
            status(
                "run-3",
                "other: third",
                &base,
                json!({"phase": "finished", "terminalResult": {"status": "failed", "reason": "worker_failed"}, "metadata": {}}),
            ),
            &[],
        );
    let store = EvalStore::for_workspace(root.path());
    let mut request = command(root.path(), &[]);
    request.title_prefix = Some("baseline:".to_owned());
    request.target = Some("staging".to_owned());
    let summary = import_runs(&store, &request, &backend).await.unwrap();
    assert_eq!(summary.target, "staging");
    assert_eq!(
        summary
            .imported
            .iter()
            .map(|run| run.run_id.as_str())
            .collect::<Vec<_>>(),
        vec!["run-1"],
        "only finished runs with the prefix are selected"
    );
    assert!(backend.calls().contains(&"list staging".to_owned()));

    // Appending the same run again is refused; appending a new run continues the run index.
    let again = import_runs(&store, &command(root.path(), &["run-1"]), &backend).await;
    assert!(again.unwrap_err().to_string().contains("already a sample"));
    let mut append = command(root.path(), &["run-3"]);
    append.manifest = None;
    let summary = import_runs(&store, &append, &backend).await.unwrap();
    assert!(!summary.created);
    assert_eq!(summary.imported[0].sample_id, "baseline-0001");
}

#[tokio::test]
async fn a_missing_eval_without_manifest_and_an_unknown_arm_are_errors() {
    let root = tempfile::tempdir().unwrap();
    let store = EvalStore::for_workspace(root.path());
    let backend = MockPlatform::default().with(failed("run-1", &"a".repeat(40)), &[1]);
    let mut request = command(root.path(), &["run-1"]);
    request.manifest = None;
    let error = import_runs(&store, &request, &backend).await.unwrap_err();
    assert!(error.to_string().contains("--manifest"), "{error}");
    let mut request = command(root.path(), &["run-1"]);
    request.arm = Some("nope".to_owned());
    let error = import_runs(&store, &request, &backend).await.unwrap_err();
    assert!(error.to_string().contains("nope"), "{error}");
}

#[tokio::test]
async fn imported_baseline_pairs_with_fresh_candidate_runs_of_a_different_count() {
    let root = tempfile::tempdir().unwrap();
    let manifest = root.path().join("eval.toml");
    std::fs::write(&manifest, MANIFEST).unwrap();
    // Fresh candidate runs only (3 runs, fake runner), then two imported baseline runs.
    crate::native_v2_eval::cli::run_manifest_through_fake(crate::native_v2_eval::EvalRunCommand {
        manifest: manifest.clone(),
        directory: root.path().to_path_buf(),
        id: Some("imported".to_owned()),
        scorer: None,
        judge_template: None,
        judge_runner: None,
        runner: None,
        workspace: None,
        arms: vec!["candidate".to_owned()],
    })
    .await
    .unwrap();
    let store = EvalStore::for_workspace(root.path());
    let before = store.load(&EvalId::new("imported").unwrap()).unwrap();
    assert!(
        before
            .samples
            .iter()
            .all(|sample| sample.arm_id == "candidate")
    );
    assert_eq!(before.samples.len(), 3);

    let base = "a".repeat(40);
    let backend = MockPlatform::default()
        .with(failed("run-1", &base), &[1_000, 61_000])
        .with(failed("run-2", &base), &[1_000, 181_000]);
    let mut request = command(root.path(), &["run-1", "run-2"]);
    request.manifest = None;
    let summary = import_runs(&store, &request, &backend).await.unwrap();
    assert!(summary.pairing.contains("baseline = 2, candidate = 3"));

    let data = store.load(&EvalId::new("imported").unwrap()).unwrap();
    let report = serde_json::to_value(data.report.unwrap()).unwrap();
    let rendered = report.to_string();
    assert!(
        rendered.contains("pairs are formed by run index"),
        "unequal run counts are explained in the report: {rendered}"
    );
}
