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
