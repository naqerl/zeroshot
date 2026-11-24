use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use tempfile::TempDir;

use super::*;

/// The graph and runtime one arm's samples run, taken from a shipped template.
///
/// The judge borrows an arm's lane, so the arm has to be a graph with an agent in it: this uses the
/// single-worker template rather than a hand-written graph, which keeps the fixture as admitted as
/// a real eval's arms are.
fn arm_definitions() -> (Value, Value) {
    let graph = BuiltinGraphTemplate::SingleWorker
        .materialize(TemplateDelivery::None)
        .assert_value();
    let mut nodes = serde_json::Map::new();
    for (name, delivery) in crate::native_v2_admission::executable_runtime_roles(&graph.root) {
        assert!(!delivery, "single-worker binds no delivery worker");
        nodes.insert(
            name.as_str().to_owned(),
            json!({"kind":"agent","model":"gpt-6.1-sol","effort":"high"}),
        );
    }
    (
        serde_json::to_value(&graph).assert_value(),
        json!({"harness":"codex","provider":"openai","nodes":nodes}),
    )
}

fn document(
    slug: &str,
    target: Option<&str>,
    judge: Option<&str>,
) -> crate::native_v2_eval::EvalDocument {
    let (graph, runtime) = arm_definitions();
    let arm = |id: &str, head: &str| {
        json!({
            "id": id,
            "head": head,
            "graph": graph,
            "runtime": runtime,
        })
    };
    serde_json::from_value(json!({
        "schema": "zeroshot.eval/v1",
        "slug": slug,
        "description": "compare a baseline against a candidate",
        "runsPerArm": 1,
        "target": target,
        "judge": judge.map(|template| json!({"template": template})),
        "input": {"task": "repair the fixture"},
        "scoring": {
            "criteria": [{
                "id": "quality",
                "description": "Review quality",
                "weight": 2.0
            }],
            "baseline": "baseline",
            "candidate": "candidate"
        },
        "arms": [
            arm("baseline", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            arm("candidate", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        ]
    }))
    .assert_value()
}

/// Run one Git command in `workspace`, failing the test with its native diagnostics.
fn git(workspace: &Path, arguments: &[&str]) {
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
}

/// Make `workspace` an attached Git checkout with one eligible remote.
///
/// `eval run` submits to a named target, so it resolves the sample repository from the eval's own
/// workspace the way any named run does. Both arms pin exact revisions, so nothing here needs the
/// remote to be reachable.
fn git_workspace(workspace: &Path) {
    git(workspace, &["init", "--quiet", "--initial-branch=main"]);
    fs::write(workspace.join("README.md"), "eval fixture\n").assert_value();
    git(workspace, &["add", "README.md"]);
    git(workspace, &["commit", "--quiet", "-m", "eval fixture"]);
    git(
        workspace,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/acme/project.git",
        ],
    );
}

/// The terminal output one FakeBackend run reports, good for both a sample and its judge.
fn settled_output() -> Value {
    json!({
        "patch": "diff --git a/a b/a\n--- a/a\n+++ b/a\n+fixed",
        "verdicts": [{"id": "quality", "pass": true, "reason": "the change is sound"}]
    })
}

async fn eval_run(
    workspace: &Path,
    slug: &str,
    extra: &[&str],
    backend: &FakeBackend,
) -> Result<(CliOutcome, Vec<u8>), NativeV2CliError> {
    let root_argument = workspace.to_string_lossy().into_owned();
    let mut arguments = vec!["eval", "--directory", root_argument.as_str(), "run", slug];
    arguments.extend_from_slice(extra);
    let command = parse_native_v2_args(args(&arguments)).assert_value();
    let mut output = Vec::new();
    let outcome = execute_native_v2_cli(command, backend, &mut NeverDetach, &mut output).await?;
    Ok((outcome, output))
}

fn stamps(store: &crate::native_v2_eval::EvalStore, slug: &str) -> Vec<PathBuf> {
    let mut stamps = fs::read_dir(store.runs_root(slug))
        .assert_value()
        .map(|entry| entry.assert_value().path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    stamps.sort();
    stamps
}

fn submitted_titles(backend: &FakeBackend) -> Vec<(Option<String>, String)> {
    backend
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::Submit { target, title, .. } => Some((target, title.as_str().to_owned())),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn eval_run_submits_the_samples_to_its_own_target_and_publishes_judged_evidence() {
    let workspace = TempDir::new().assert_value();
    git_workspace(workspace.path());
    let store = crate::native_v2_eval::EvalStore::for_workspace(workspace.path());
    store
        .create(
            &document("compare", Some("prod"), Some("eval-judge")),
            false,
        )
        .assert_value();

    let backend = FakeBackend::with_finished_output(settled_output());
    let (outcome, output) = match eval_run(workspace.path(), "compare", &[], &backend).await {
        Ok(result) => result,
        Err(error) => panic!("eval run failed: {error:?}"),
    };
    assert_eq!(outcome, CliOutcome::Completed);

    // The eval's own target received both runs: the sample and the judge that read it.
    assert_eq!(
        submitted_titles(&backend),
        [
            (
                Some("prod".to_owned()),
                "eval compare baseline sample 0".to_owned()
            ),
            (
                Some("prod".to_owned()),
                "eval compare judge sample 0".to_owned()
            ),
            (
                Some("prod".to_owned()),
                "eval compare candidate sample 0".to_owned()
            ),
            (
                Some("prod".to_owned()),
                "eval compare judge sample 0".to_owned()
            ),
        ]
    );

    let summary = serde_json::from_slice::<Value>(&output).assert_value();
    assert_eq!(summary.pointer("/slug"), Some(&json!("compare")));
    assert_eq!(summary.pointer("/target"), Some(&json!("prod")));
    assert_eq!(summary.pointer("/judge"), Some(&json!("eval-judge")));
    assert_eq!(
        summary.pointer("/samples/0/status"),
        Some(&json!("succeeded"))
    );
    assert_eq!(
        summary.pointer("/samples/0/verdicts/0/pass"),
        Some(&json!(true))
    );
    assert_eq!(summary.pointer("/arms/0/verdicts"), Some(&json!(1)));
    assert_eq!(summary.pointer("/arms/0/score"), Some(&json!(1.0)));

    let stamps = stamps(&store, "compare");
    assert_eq!(stamps.len(), 1);
    let stamp = stamps.first().assert_value();
    assert_eq!(
        summary.pointer("/stamp"),
        Some(&json!(stamp.file_name().assert_value().to_string_lossy()))
    );
    for artifact in [
        "baseline-0/input.json",
        "baseline-0/graph.json",
        "baseline-0/runtime.json",
        "baseline-0/result.json",
        "baseline-0/judge/input.json",
        "baseline-0/judge/graph.json",
        "baseline-0/judge/runtime.json",
        "baseline-0/judge/result.json",
    ] {
        assert!(
            stamp.join(artifact).is_file(),
            "the run publishes {artifact}"
        );
    }
    let judged = serde_json::from_slice::<Value>(
        &fs::read(stamp.join("baseline-0/judge/input.json")).assert_value(),
    )
    .assert_value();
    assert_eq!(judged.pointer("/task"), Some(&json!("repair the fixture")));
    let traits: Value = serde_json::from_str(
        judged
            .pointer("/traits")
            .and_then(Value::as_str)
            .assert_value(),
    )
    .assert_value();
    assert_eq!(
        traits,
        json!([{"id": "quality", "description": "Review quality"}])
    );
    assert!(
        judged
            .pointer("/patch")
            .and_then(Value::as_str)
            .assert_value()
            .contains("+fixed"),
        "the judge reads the patch the sample delivered"
    );
    let report = fs::read_to_string(stamp.join("report.md")).assert_value();
    assert!(report.contains("| baseline | 1 | 1 | 0 | 1 |"));
    assert_eq!(
        serde_json::from_slice::<Value>(
            &fs::read(store.runs_root("compare").join("summary.json")).assert_value()
        )
        .assert_value(),
        summary
    );
}

#[tokio::test]
async fn eval_run_flag_overrides_the_target_the_eval_names() {
    let workspace = TempDir::new().assert_value();
    git_workspace(workspace.path());
    let store = crate::native_v2_eval::EvalStore::for_workspace(workspace.path());
    store
        .create(&document("compare", Some("prod"), None), false)
        .assert_value();

    let backend = FakeBackend::with_finished_output(settled_output());
    let (outcome, output) = eval_run(
        workspace.path(),
        "compare",
        &["--target", "staging"],
        &backend,
    )
    .await
    .assert_value();
    assert_eq!(outcome, CliOutcome::Completed);
    assert!(
        submitted_titles(&backend)
            .iter()
            .all(|(target, _)| target.as_deref() == Some("staging")),
        "every sample goes to the target the flag names"
    );
    let summary = serde_json::from_slice::<Value>(&output).assert_value();
    assert_eq!(summary.pointer("/target"), Some(&json!("staging")));
    // No judge was authored, so a sample's own result is what the run records.
    assert_eq!(summary.pointer("/judge"), None);
    assert_eq!(summary.pointer("/samples/0/verdicts"), None);
    let stamp = stamps(&store, "compare").into_iter().next().assert_value();
    assert!(stamp.join("baseline-0/result.json").is_file());
    assert!(!stamp.join("baseline-0/judge").exists());
    // A run that authored no judge is not a failure: the report says so instead of claiming the
    // sample failed.
    let report = fs::read_to_string(stamp.join("report.md")).assert_value();
    assert!(
        report.contains("| baseline | 0 | - | unjudged | no judge configured for this eval |"),
        "the report never calls an unjudged sample failed: {report}"
    );
}

#[tokio::test]
async fn a_failed_judge_run_fails_the_sample_and_names_the_judge_in_the_report() {
    let workspace = TempDir::new().assert_value();
    git_workspace(workspace.path());
    let store = crate::native_v2_eval::EvalStore::for_workspace(workspace.path());
    store
        .create(
            &document("compare", Some("prod"), Some("eval-judge")),
            false,
        )
        .assert_value();

    let backend = FakeBackend::with_failed_judge();
    let (outcome, output) = eval_run(workspace.path(), "compare", &[], &backend)
        .await
        .assert_value();
    assert_eq!(outcome, CliOutcome::Completed, "the eval ran to its end");
    let summary = serde_json::from_slice::<Value>(&output).assert_value();
    assert_eq!(summary.pointer("/samples/0/status"), Some(&json!("failed")));
    let reason = summary
        .pointer("/samples/0/reason")
        .and_then(Value::as_str)
        .assert_value();
    assert!(
        reason.contains("judge run run-public failed"),
        "the aggregate names the judge that failed: {reason}"
    );
    assert_eq!(summary.pointer("/samples/0/verdicts"), None);
    assert_eq!(summary.pointer("/arms/0/succeeded"), Some(&json!(0)));
    assert_eq!(summary.pointer("/arms/0/failed"), Some(&json!(1)));
    assert_eq!(summary.pointer("/arms/0/score"), None);

    let stamp = stamps(&store, "compare").into_iter().next().assert_value();
    let judged = serde_json::from_slice::<Value>(
        &fs::read(stamp.join("baseline-0/judge/result.json")).assert_value(),
    )
    .assert_value();
    assert_eq!(judged.pointer("/status"), Some(&json!("failed")));
    assert_eq!(judged.pointer("/verdicts"), None);
    // The sample's own run is untouched evidence: only the judgement of it failed.
    let own = serde_json::from_slice::<Value>(
        &fs::read(stamp.join("baseline-0/result.json")).assert_value(),
    )
    .assert_value();
    assert_eq!(own.pointer("/status"), Some(&json!("succeeded")));
    let report = fs::read_to_string(stamp.join("report.md")).assert_value();
    assert!(
        report.contains("judge run run-public failed"),
        "the report records the failed judge: {report}"
    );
}

#[tokio::test]
async fn ctrl_c_ends_the_settlement_poll_instead_of_waiting_for_the_target() {
    let workspace = TempDir::new().assert_value();
    git_workspace(workspace.path());
    let store = crate::native_v2_eval::EvalStore::for_workspace(workspace.path());
    store
        .create(&document("compare", Some("prod"), None), false)
        .assert_value();

    // Status says it started and then never answers, exactly like a target that lost the run.
    let (notification, _) = tokio::sync::watch::channel(());
    let backend = FakeBackend::with_unanswered_status_after_early_close(notification.clone());
    let mut signal = EdgeDetachSignal { notification };
    let root_argument = workspace.path().to_string_lossy().into_owned();
    let command = parse_native_v2_args(args(&[
        "eval",
        "--directory",
        root_argument.as_str(),
        "run",
        "compare",
    ]))
    .assert_value();
    let mut output = Vec::new();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        execute_native_v2_cli(command, &backend, &mut signal, &mut output),
    )
    .await
    .assert_value_with("Ctrl-C ends the settlement status read")
    .assert_value();

    assert_eq!(outcome, CliOutcome::Detached);
    let summary = serde_json::from_slice::<Value>(&output).assert_value();
    assert_eq!(summary.pointer("/interrupted"), Some(&json!(true)));
    // The sample was started but never settled, so the aggregate claims no verdict about it.
    assert_eq!(summary.pointer("/samples"), Some(&json!([])));
    let stamp = stamps(&store, "compare").into_iter().next().assert_value();
    assert!(stamp.join("baseline-0/input.json").is_file());
    assert!(!stamp.join("baseline-0/result.json").exists());
}

#[tokio::test]
async fn eval_run_without_any_target_is_a_named_error_and_submits_nothing() {
    let workspace = TempDir::new().assert_value();
    let store = crate::native_v2_eval::EvalStore::for_workspace(workspace.path());
    store
        .create(&document("compare", None, None), false)
        .assert_value();

    let backend = FakeBackend::with_finished_output(settled_output());
    let error = eval_run(workspace.path(), "compare", &[], &backend)
        .await
        .assert_error();
    assert!(
        matches!(&error, NativeV2CliError::Usage(message) if message.contains("requires a nonempty target")),
        "unexpected error: {error:?}"
    );
    assert!(backend.calls().is_empty(), "nothing is submitted");
    assert!(!store.runs_root("compare").exists(), "nothing is published");
}

#[tokio::test]
async fn eval_run_names_the_judge_template_it_cannot_expand() {
    let workspace = TempDir::new().assert_value();
    let store = crate::native_v2_eval::EvalStore::for_workspace(workspace.path());
    store
        .create(
            &document("compare", Some("prod"), Some("no-such-judge")),
            false,
        )
        .assert_value();

    let backend = FakeBackend::with_finished_output(settled_output());
    let error = eval_run(workspace.path(), "compare", &[], &backend)
        .await
        .assert_error();
    let NativeV2CliError::Usage(message) = &error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(
        message.contains("unknown judge template \"no-such-judge\""),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("eval-judge"),
        "the error lists the templates this binary ships: {message}"
    );
    assert!(backend.calls().is_empty(), "nothing is submitted");
}

#[tokio::test]
async fn eval_run_keeps_earlier_evidence_and_publishes_a_fresh_stamp() {
    let workspace = TempDir::new().assert_value();
    git_workspace(workspace.path());
    let store = crate::native_v2_eval::EvalStore::for_workspace(workspace.path());
    store
        .create(&document("compare", Some("prod"), None), false)
        .assert_value();

    let backend = FakeBackend::with_finished_output(settled_output());
    let (_, first) = eval_run(workspace.path(), "compare", &[], &backend)
        .await
        .assert_value();
    let first_stamps = stamps(&store, "compare");
    let first_stamp = first_stamps.first().assert_value();
    let first_evidence = fs::read(first_stamp.join("baseline-0/result.json")).assert_value();

    let (_, second) = eval_run(workspace.path(), "compare", &[], &backend)
        .await
        .assert_value();
    assert_ne!(
        serde_json::from_slice::<Value>(&first)
            .assert_value()
            .pointer("/stamp"),
        serde_json::from_slice::<Value>(&second)
            .assert_value()
            .pointer("/stamp"),
        "a rerun publishes beside the earlier one"
    );
    let stamps = stamps(&store, "compare");
    assert_eq!(stamps.len(), 2);
    assert_eq!(
        fs::read(stamps.first().assert_value().join("baseline-0/result.json")).assert_value(),
        first_evidence,
        "rerunning never rewrites an earlier sample's evidence"
    );
    // Only the aggregate moves to the newest stamp.
    assert_eq!(
        serde_json::from_slice::<Value>(
            &fs::read(store.runs_root("compare").join("summary.json")).assert_value()
        )
        .assert_value(),
        serde_json::from_slice::<Value>(&second).assert_value()
    );
}
