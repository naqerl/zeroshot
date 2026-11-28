use std::path::Path;

use serde_json::json;
use openengine_cluster_protocol::GraphSpec;
use tempfile::TempDir;

use super::*;
use crate::native_v2_eval::{EvalDocument, EvalStore};

fn git(workspace: &Path, arguments: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .current_dir(workspace)
        .args(arguments)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env("GIT_AUTHOR_NAME", "Zeroshot")
        .env("GIT_AUTHOR_EMAIL", "eval@zeroshot.invalid")
        .env("GIT_COMMITTER_NAME", "Zeroshot")
        .env("GIT_COMMITTER_EMAIL", "eval@zeroshot.invalid")
        .output()
        .assert_value();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).assert_value()
}

fn fixture_workspace() -> (TempDir, String) {
    let workspace = TempDir::new().assert_value();
    git(
        workspace.path(),
        &["init", "--quiet", "--initial-branch=main"],
    );
    git(
        workspace.path(),
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/acme/project.git",
        ],
    );
    std::fs::write(workspace.path().join("README.md"), "eval fixture\n").assert_value();
    git(workspace.path(), &["add", "README.md"]);
    git(
        workspace.path(),
        &["commit", "--quiet", "-m", "eval fixture"],
    );
    git(workspace.path(), &["branch", "candidate"]);
    let candidate = git(workspace.path(), &["rev-parse", "candidate^{commit}"]);
    (workspace, candidate.trim().to_owned())
}

fn agent_arm_runtime() -> RuntimePlan {
    let graph = BuiltinGraphTemplate::SingleWorker
        .materialize(TemplateDelivery::None)
        .assert_value();
    let mut nodes = serde_json::Map::new();
    for (name, _) in crate::native_v2_admission::executable_runtime_roles(&graph.root) {
        nodes.insert(
            name.as_str().to_owned(),
            json!({"kind":"agent","model":"gpt-6.1-sol","effort":"high"}),
        );
    }
    serde_json::from_value(json!({
        "harness":"codex",
        "provider":"openai",
        "size":"medium",
        "globalAgents":{"postmortem":{"sourceNode":"worker"}},
        "nodes":nodes
    }))
    .assert_value()
}

fn agent_arm_graph() -> GraphSpec {
    BuiltinGraphTemplate::SingleWorker
        .materialize(TemplateDelivery::None)
        .assert_value()
}

fn document(slug: &str, head: &str, target: Option<&str>, judge: bool) -> EvalDocument {
    let mut document: EvalDocument = serde_json::from_value(json!({
        "schema":"zeroshot.eval/v1",
        "slug":slug,
        "description":"Judge one eval sample",
        "runsPerArm":1,
        "input":{"task":"repair the fixture"},
        "scoring":{
            "criteria":[{"id":"quality","description":"Review quality","weight":2.0}]
        },
        "arms":[{
            "id":"arm",
            "head":head,
            "graph":agent_arm_graph(),
            "runtime":agent_arm_runtime()
        }]
    }))
    .assert_value();
    document.target = target.map(str::to_owned);
    document.judge = judge.then(|| crate::native_v2_eval::EvalJudge {
        template: "eval-judge".to_owned(),
    });
    document
}

fn store(workspace: &Path, document: &EvalDocument) -> std::path::PathBuf {
    let store = EvalStore::for_workspace(workspace);
    store.create(document, false).assert_value();
    store.root().to_path_buf()
}

fn run_command(root: &Path, target: Option<&str>) -> NativeV2CliCommand {
    NativeV2CliCommand::Eval(EvalCommand::Run(EvalRunCommand {
        root: root.to_path_buf(),
        slug: "alpha".to_owned(),
        target: target.map(str::to_owned),
        restart: false,
        stamp: None,
        retry_failed: false,
    }))
}

async fn execute(
    command: NativeV2CliCommand,
    backend: &FakeBackend,
) -> (Result<CliOutcome, NativeV2CliError>, String) {
    let mut output = Vec::new();
    let outcome = execute_native_v2_cli(command, backend, &mut NeverDetach, &mut output).await;
    (outcome, String::from_utf8(output).assert_value())
}

#[tokio::test]
async fn eval_run_submits_every_sample_to_the_target_the_flag_names() {
    let (workspace, candidate) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), false),
    );
    let backend = FakeBackend::with_run_outputs(vec![json!({"patch":"diff --git a/x b/x"})]);

    let (outcome, output) = execute(run_command(&root, Some("staging")), &backend).await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    let summary: Value = serde_json::from_str(&output).assert_value();
    assert_eq!(summary["target"], "staging");

    let calls = backend.calls();
    let submit = calls
        .iter()
        .find_map(|call| match call {
            Call::Submit {
                target,
                branch,
                revision,
                ..
            } => Some((target, branch, revision)),
            _ => None,
        })
        .assert_value();
    assert_eq!(submit.0.as_deref(), Some("staging"));
    assert_eq!(*submit.1, None, "the branch name is never submitted");
    assert_eq!(submit.2.as_deref(), Some(candidate.as_str()));
    assert!(calls.iter().any(|call| matches!(
        call,
        Call::Status { target: Some(target), .. } if target == "staging"
    )));

    let recorded: crate::native_v2_eval::config::ArmSource = serde_json::from_slice(
        &std::fs::read(
            root.join("runs/alpha")
                .join(summary["stamp"].as_str().assert_value())
                .join("arm-0/source.json"),
        )
        .assert_value(),
    )
    .assert_value();
    assert_eq!(recorded.branch, None);
    assert_eq!(
        recorded.revision.as_ref().map(|revision| revision.as_str()),
        Some(candidate.as_str())
    );
}

#[tokio::test]
async fn eval_run_falls_back_to_the_document_target_and_refuses_to_run_without_one() {
    let (workspace, _) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), false),
    );
    let backend = FakeBackend::with_run_outputs(vec![json!({"patch":"diff --git a/x b/x"})]);
    let (outcome, output) = execute(run_command(&root, None), &backend).await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    assert_eq!(
        serde_json::from_str::<Value>(&output).assert_value()["target"],
        "prod"
    );

    let (workspace, _) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", None, false),
    );
    let backend = FakeBackend::default();
    let (outcome, _) = execute(run_command(&root, None), &backend).await;
    assert!(
        matches!(
            &outcome,
            Err(NativeV2CliError::Usage(message))
                if message.contains("requires a nonempty target")
        ),
        "{outcome:?}"
    );
    assert!(
        backend.calls().is_empty(),
        "no target means no run was sent"
    );
}

#[tokio::test]
async fn eval_run_fails_a_judge_that_records_no_verdicts() {
    let (workspace, _) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), true),
    );
    let backend = FakeBackend::with_run_outputs(vec![
        json!({"patch":"diff --git a/x b/x"}),
        json!({"verdicts":[]}),
    ]);

    let (outcome, output) = execute(run_command(&root, Some("prod")), &backend).await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    let summary: Value = serde_json::from_str(&output).assert_value();
    assert_eq!(summary["samples"][0]["status"], "failed");
    assert!(
        summary["samples"][0]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("it recorded no verdicts")),
        "{}",
        summary["samples"][0]["reason"]
    );
    assert_eq!(
        backend
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Submit { .. }))
            .count(),
        2,
        "the sample and its judge are submitted to the same target"
    );
}

async fn execute_summary(
    command: NativeV2CliCommand,
    backend: &FakeBackend,
) -> (Result<CliOutcome, NativeV2CliError>, Value) {
    let (outcome, output) = execute(command, backend).await;
    let summary = if output.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&output).assert_value()
    };
    (outcome, summary)
}

/// Detach once an earlier `wait` has stayed pending, so a pass journals a submission and then
/// stops before polling it.
struct DetachAfter {
    pending_waits: usize,
}

#[async_trait::async_trait]
impl DetachSignal for DetachAfter {
    async fn wait(&mut self) {
        if self.pending_waits == 0 {
            return;
        }
        self.pending_waits -= 1;
        std::future::pending::<()>().await;
    }
}

/// The stamp options one pass runs with, as the command line spells them.
#[derive(Default)]
struct StampFlags {
    stamp: Option<String>,
    restart: bool,
    retry_failed: bool,
}

fn run_command_with(root: &Path, target: Option<&str>, flags: StampFlags) -> NativeV2CliCommand {
    NativeV2CliCommand::Eval(EvalCommand::Run(EvalRunCommand {
        root: root.to_path_buf(),
        slug: "alpha".to_owned(),
        target: target.map(str::to_owned),
        restart: flags.restart,
        stamp: flags.stamp,
        retry_failed: flags.retry_failed,
    }))
}

/// Execute one pass with a detach signal that stops it after its first submitted sample.
async fn execute_until_detached(
    command: NativeV2CliCommand,
    backend: &FakeBackend,
) -> (Result<CliOutcome, NativeV2CliError>, Value) {
    let mut output = Vec::new();
    let mut signal = DetachAfter { pending_waits: 1 };
    let outcome = execute_native_v2_cli(command, backend, &mut signal, &mut output).await;
    (outcome, serde_json::from_slice(&output).assert_value())
}

/// Detach at the first opportunity, before a retired stage is submitted again.
struct DetachNow;

#[async_trait::async_trait]
impl DetachSignal for DetachNow {
    async fn wait(&mut self) {}
}

/// Execute one pass that retires a failed stage and stops before newly submitting it.
async fn execute_until_detached_before_submitting(
    command: NativeV2CliCommand,
    backend: &FakeBackend,
) -> (Result<CliOutcome, NativeV2CliError>, Value) {
    let mut output = Vec::new();
    let mut signal = DetachNow;
    let outcome = execute_native_v2_cli(command, backend, &mut signal, &mut output).await;
    (outcome, serde_json::from_slice(&output).assert_value())
}

/// Every submission key a pass asked the target to admit, in submission order.
fn submission_keys(backend: &FakeBackend) -> Vec<String> {
    backend
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::Submit { submission_key, .. } => Some(submission_key),
            _ => None,
        })
        .collect()
}

fn stamp_root(root: &Path, stamp: &Value) -> std::path::PathBuf {
    root.join("runs/alpha").join(stamp.as_str().assert_value())
}

#[tokio::test]
async fn eval_run_resumes_the_stamp_it_journaled_instead_of_resubmitting() {
    let (workspace, _) = fixture_workspace();
    let mut document = document("alpha", "candidate", Some("prod"), false);
    document.runs_per_arm = 2;
    let root = store(workspace.path(), &document);

    let backend = FakeBackend::default();
    let (outcome, interrupted) = execute_until_detached(
        run_command_with(&root, Some("prod"), StampFlags::default()),
        &backend,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Detached);
    assert_eq!(interrupted["interrupted"], true);
    assert_eq!(interrupted["samples"].as_array().assert_value().len(), 0);
    assert_eq!(
        backend
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Submit { .. }))
            .count(),
        1,
        "the interrupted pass submitted its first sample"
    );
    assert!(
        !backend
            .calls()
            .iter()
            .any(|call| matches!(call, Call::Status { .. })),
        "the journal was written before the run was polled"
    );

    let interrupted_keys = submission_keys(&backend);
    assert_eq!(interrupted_keys.len(), 1);
    assert_eq!(
        interrupted_keys[0].len(),
        "eval-".len() + 64,
        "the submission key is one bounded digest, not a minted id: {interrupted_keys:?}"
    );
    assert!(interrupted_keys[0].starts_with("eval-"));

    let stamp = interrupted["stamp"].clone();
    let sample = stamp_root(&root, &stamp).join("arm-0");
    let journal: Value = serde_json::from_slice(
        &std::fs::read(sample.parent().assert_value().join("resume.json")).assert_value(),
    )
    .assert_value();
    assert_eq!(journal["samples"][0]["runId"], "run-public");
    assert_eq!(journal["samples"][0]["stage"], "sample");
    assert!(
        journal["samples"][0].get("settledAt").is_none(),
        "an unsettled submission must not claim to have settled: {journal}"
    );
    let submitted_input = std::fs::read(sample.join("input.json")).assert_value();

    let backend =
        FakeBackend::with_run_outputs(vec![json!({"patch":"first"}), json!({"patch":"second"})]);
    let (outcome, summary) = execute_summary(
        run_command_with(&root, Some("prod"), StampFlags::default()),
        &backend,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    assert_eq!(summary["stamp"], stamp, "the pass continued its stamp");
    assert_eq!(summary["samples"].as_array().assert_value().len(), 2);
    assert_eq!(summary["samples"][0]["status"], "succeeded");
    assert_eq!(summary["samples"][0]["runId"], "run-public");
    assert!(!summary["interrupted"].as_bool().unwrap_or(false));
    assert_eq!(
        backend
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Submit { .. }))
            .count(),
        1,
        "only the sample the stamp had not submitted was submitted again"
    );
    assert_eq!(
        std::fs::read(sample.join("input.json")).assert_value(),
        submitted_input,
        "a resumed pass reuses the evidence bytes the first pass published"
    );
    let resumed_keys = submission_keys(&backend);
    assert_eq!(resumed_keys.len(), 1);
    assert_ne!(
        resumed_keys[0], interrupted_keys[0],
        "each sample stage derives its own submission key"
    );
    let report =
        std::fs::read_to_string(sample.parent().assert_value().join("report.md")).assert_value();
    assert!(
        report.contains("| arm | 2 | 2 | 0 |"),
        "the report was regenerated from every sample's evidence: {report}"
    );
}

#[tokio::test]
async fn eval_run_fails_a_sample_whose_recorded_run_vanished() {
    for missing in [MissingRun::Remote, MissingRun::NotFound] {
        let (workspace, _) = fixture_workspace();
        let root = store(
            workspace.path(),
            &document("alpha", "candidate", Some("prod"), false),
        );
        let backend = FakeBackend::default();
        let (outcome, interrupted) = execute_until_detached(
            run_command_with(&root, Some("prod"), StampFlags::default()),
            &backend,
        )
        .await;
        assert_eq!(outcome.assert_value(), CliOutcome::Detached);
        let stamp = interrupted["stamp"].clone();

        let backend = FakeBackend::with_missing_run(missing);
        let (outcome, summary) = execute_summary(
            run_command_with(&root, Some("prod"), StampFlags::default()),
            &backend,
        )
        .await;
        assert_eq!(outcome.assert_value(), CliOutcome::Completed);
        assert_eq!(summary["samples"].as_array().assert_value().len(), 1);
        assert_eq!(summary["samples"][0]["status"], "failed");
        assert_eq!(
            summary["samples"][0]["reason"],
            crate::native_v2_eval::run::VANISHED_REASON
        );
        assert_eq!(
            backend
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Submit { .. }))
                .count(),
            0,
            "a vanished run is never replaced by a silent second submission"
        );

        let directory = stamp_root(&root, &stamp).join("arm-0");
        let recorded: crate::native_v2_eval::run::SampleResult =
            serde_json::from_slice(&std::fs::read(directory.join("result.json")).assert_value())
                .assert_value();
        assert_eq!(
            recorded.reason.as_deref(),
            Some(crate::native_v2_eval::run::VANISHED_REASON)
        );
        let journal: Value = serde_json::from_slice(
            &std::fs::read(directory.parent().assert_value().join("resume.json")).assert_value(),
        )
        .assert_value();
        assert_eq!(journal["samples"][0]["failed"], true);
    }
}

#[tokio::test]
async fn eval_run_retries_a_retired_failure_only_with_retry_failed() {
    let (workspace, _) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), false),
    );
    let backend = FakeBackend::default();
    let (outcome, interrupted) = execute_until_detached(
        run_command_with(&root, Some("prod"), StampFlags::default()),
        &backend,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Detached);
    let stamp = interrupted["stamp"].as_str().assert_value().to_owned();

    let failed_keys = submission_keys(&backend);
    let backend = FakeBackend::with_missing_run(MissingRun::Remote);
    let (outcome, failed) = execute_summary(
        run_command_with(&root, Some("prod"), StampFlags::default()),
        &backend,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    assert_eq!(failed["samples"][0]["status"], "failed");

    let backend = FakeBackend::with_run_outputs(vec![json!({"patch":"retried"})]);
    let (outcome, summary) = execute_summary(
        run_command_with(
            &root,
            Some("prod"),
            StampFlags {
                stamp: Some(stamp.to_owned()),
                retry_failed: true,
                ..StampFlags::default()
            },
        ),
        &backend,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    assert_eq!(summary["samples"][0]["status"], "succeeded");
    assert_eq!(
        backend
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Submit { .. }))
            .count(),
        1,
        "the retry submits the retired stage again instead of polling its old run"
    );
    let retried_keys = submission_keys(&backend);
    assert_eq!(retried_keys.len(), 1);
    assert_ne!(
        retried_keys[0], failed_keys[0],
        "the retry claims a new attempt, so the target cannot replay the retired run"
    );

    let directory = root.join("runs/alpha").join(&stamp).join("arm-0");
    let retired = directory.join("result.failed-1.json");
    assert!(
        retired.is_file(),
        "the failed evidence is kept under its attempt"
    );
    let retired: crate::native_v2_eval::run::SampleResult =
        serde_json::from_slice(&std::fs::read(&retired).assert_value()).assert_value();
    assert_eq!(
        retired.reason.as_deref(),
        Some(crate::native_v2_eval::run::VANISHED_REASON)
    );
    let recorded: crate::native_v2_eval::run::SampleResult =
        serde_json::from_slice(&std::fs::read(directory.join("result.json")).assert_value())
            .assert_value();
    assert_eq!(
        recorded.status,
        crate::native_v2_eval::run::SampleStatus::Succeeded
    );
}

#[tokio::test]
async fn eval_run_resumes_a_retry_that_retired_evidence_before_claiming_its_replacement() {
    let (workspace, _) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), false),
    );
    let first = FakeBackend::default();
    let (outcome, interrupted) = execute_until_detached(
        run_command_with(&root, Some("prod"), StampFlags::default()),
        &first,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Detached);
    let stamp = interrupted["stamp"].as_str().assert_value().to_owned();
    let failed_key = submission_keys(&first).pop().assert_value();

    let (outcome, failed) = execute_summary(
        run_command_with(&root, Some("prod"), StampFlags::default()),
        &FakeBackend::with_missing_run(MissingRun::Remote),
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    assert_eq!(failed["samples"][0]["status"], "failed");

    // The retry pass retires the failed result, then detaches before claiming its replacement.
    let interrupted_retry = FakeBackend::default();
    let (outcome, _) = execute_until_detached_before_submitting(
        run_command_with(
            &root,
            Some("prod"),
            StampFlags {
                stamp: Some(stamp.clone()),
                retry_failed: true,
                ..StampFlags::default()
            },
        ),
        &interrupted_retry,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Detached);
    assert_eq!(
        submission_keys(&interrupted_retry).len(),
        0,
        "the retirement happened before the replacement was submitted"
    );
    let directory = root.join("runs/alpha").join(&stamp).join("arm-0");
    assert!(
        !directory.join("result.json").exists(),
        "the failed canonical result was removed by the interrupted retry"
    );
    let journal: Value = serde_json::from_slice(
        &std::fs::read(directory.parent().assert_value().join("resume.json")).assert_value(),
    )
    .assert_value();
    assert_eq!(
        journal["samples"][0]["retired"], "sample",
        "the journal records the retirement before the replacement exists"
    );

    let retry = FakeBackend::with_run_outputs(vec![json!({"patch":"retried"})]);
    let (outcome, summary) = execute_summary(
        run_command_with(
            &root,
            Some("prod"),
            StampFlags {
                stamp: Some(stamp),
                retry_failed: true,
                ..StampFlags::default()
            },
        ),
        &retry,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    assert_eq!(summary["samples"][0]["status"], "succeeded");
    let retried_keys = submission_keys(&retry);
    assert_eq!(retried_keys.len(), 1);
    assert_ne!(
        retried_keys[0], failed_key,
        "the resumed retry is the next attempt, not a replay of the failed run's key"
    );
}

#[tokio::test]
async fn eval_run_refuses_a_stamp_whose_pin_no_longer_matches() {
    let (workspace, _) = fixture_workspace();
    let document = document("alpha", "candidate", Some("prod"), false);
    let root = store(workspace.path(), &document);
    let backend = FakeBackend::with_run_outputs(vec![json!({"patch":"diff --git a/x b/x"})]);
    let (outcome, summary) = execute_summary(run_command(&root, Some("prod")), &backend).await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    let stamp = summary["stamp"].as_str().assert_value().to_owned();

    let error = execute_summary(
        run_command_with(
            &root,
            Some("staging"),
            StampFlags {
                stamp: Some(stamp.to_owned()),
                ..StampFlags::default()
            },
        ),
        &FakeBackend::default(),
    )
    .await
    .0
    .assert_error();
    assert!(
        matches!(
            &error,
            NativeV2CliError::Usage(message)
                if message.contains("target moved from \"prod\" to \"staging\"")
                    && message.contains("--restart")
        ),
        "{error:?}"
    );

    let mut changed = document;
    changed.description = Some("A different eval".to_owned());
    EvalStore::for_workspace(workspace.path())
        .create(&changed, true)
        .assert_value();
    let error = execute_summary(
        run_command_with(
            &root,
            Some("prod"),
            StampFlags {
                stamp: Some(stamp.to_owned()),
                ..StampFlags::default()
            },
        ),
        &FakeBackend::default(),
    )
    .await
    .0
    .assert_error();
    assert!(
        matches!(
            &error,
            NativeV2CliError::Usage(message) if message.contains("the eval document changed")
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn eval_run_restart_starts_a_new_stamp_without_touching_the_old_one() {
    let (workspace, _) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), false),
    );
    let backend = FakeBackend::with_run_outputs(vec![json!({"patch":"diff --git a/x b/x"})]);
    let (outcome, first) = execute_summary(run_command(&root, Some("prod")), &backend).await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    let first_stamp = first["stamp"].as_str().assert_value().to_owned();

    let backend = FakeBackend::with_run_outputs(vec![json!({"patch":"diff --git a/y b/y"})]);
    let (outcome, second) = execute_summary(
        run_command_with(
            &root,
            Some("prod"),
            StampFlags {
                restart: true,
                ..StampFlags::default()
            },
        ),
        &backend,
    )
    .await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    assert_ne!(second["stamp"].as_str().assert_value(), first_stamp);
    assert!(
        root.join("runs/alpha")
            .join(&first_stamp)
            .join("report.md")
            .is_file(),
        "restarting never removes the evidence of an earlier stamp"
    );
}

#[tokio::test]
async fn eval_run_starts_a_new_stamp_beside_a_legacy_one_without_a_resume_record() {
    let (workspace, candidate) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), false),
    );
    // A stamp an older build left behind: settled evidence, but no `resume.json` saying what was
    // measured, so nothing here may be continued or rewritten.
    let legacy = root.join("runs/alpha/00000000-legacy");
    let arm = legacy.join("arm-0");
    std::fs::create_dir_all(&arm).assert_value();
    std::fs::write(
        arm.join("source.json"),
        serde_json::to_vec(&json!({"revision": candidate})).assert_value(),
    )
    .assert_value();
    std::fs::write(
        arm.join("result.json"),
        serde_json::to_vec(&json!({
            "runId":"run-legacy",
            "status":"succeeded",
            "output":{"patch":"legacy"}
        }))
        .assert_value(),
    )
    .assert_value();
    let legacy_result = std::fs::read(arm.join("result.json")).assert_value();

    let backend = FakeBackend::with_run_outputs(vec![json!({"patch":"fresh"})]);
    let (outcome, summary) = execute_summary(run_command(&root, Some("prod")), &backend).await;
    assert_eq!(outcome.assert_value(), CliOutcome::Completed);
    let stamp = summary["stamp"].as_str().assert_value();
    assert_ne!(
        stamp, "00000000-legacy",
        "a stamp with evidence but no resume record is never continued"
    );
    assert_eq!(
        backend
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Submit { .. }))
            .count(),
        1,
        "only the new stamp's sample is submitted"
    );
    assert_eq!(
        summary["samples"].as_array().assert_value().len(),
        1,
        "the aggregate reports the new stamp, not the legacy evidence"
    );
    assert_eq!(
        std::fs::read(arm.join("result.json")).assert_value(),
        legacy_result,
        "the legacy stamp's evidence is left byte for byte as it was found"
    );
    assert!(!legacy.join("resume.json").exists());
    assert!(
        stamp_root(&root, &summary["stamp"])
            .join("resume.json")
            .is_file(),
        "the new stamp records what it is measuring"
    );
}

#[tokio::test]
async fn eval_run_refuses_a_stamp_name_that_is_not_one_directory() {
    let (workspace, _) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), false),
    );
    let backend = FakeBackend::default();
    let error = execute_summary(
        run_command_with(
            &root,
            Some("prod"),
            StampFlags {
                stamp: Some("../escape".to_owned()),
                ..StampFlags::default()
            },
        ),
        &backend,
    )
    .await
    .0
    .assert_error();
    assert!(
        matches!(
            &error,
            NativeV2CliError::Usage(message)
                if message.contains("--stamp must name one stamp directory")
        ),
        "{error:?}"
    );
    assert!(backend.calls().is_empty());
}

#[tokio::test]
async fn eval_run_refuses_retry_failed_without_a_resumed_stamp() {
    let (workspace, _) = fixture_workspace();
    let root = store(
        workspace.path(),
        &document("alpha", "candidate", Some("prod"), false),
    );
    let backend = FakeBackend::default();
    let error = execute_summary(
        run_command_with(
            &root,
            Some("prod"),
            StampFlags {
                retry_failed: true,
                ..StampFlags::default()
            },
        ),
        &backend,
    )
    .await
    .0
    .assert_error();
    assert!(
        matches!(
            &error,
            NativeV2CliError::Usage(message) if message.contains("only retries the failed stages")
        ),
        "{error:?}"
    );
    assert!(backend.calls().is_empty());
}
