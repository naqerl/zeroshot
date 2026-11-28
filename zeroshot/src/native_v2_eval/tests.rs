use std::fs;
use std::io;

use serde_json::{Value, json};
use tempfile::TempDir;

use super::{EvalDocument, EvalError, EvalStore, publish_atomic};
use crate::native_v2_cli::{NativeV2CliCommand, execution::try_execute_native_v2_static};

fn arm(id: &str, head: &str) -> Value {
    json!({
        "id": id,
        "head": head,
        "graph": {
            "profile": "openengine.graph.full/v1",
            "initialInput": {"kind": "record", "fields": {}},
            "policy": {"policy": "policy.native-v2@1", "default": "deny"},
            "root": {
                "kind": "succeed",
                "name": "done",
                "output": {"kind": "null"},
                "bindings": []
            }
        },
        "runtime": {
            "harness": "codex",
            "provider": "openai",
            "size": "medium",
            "nodes": {}
        }
    })
}

fn document(slug: &str) -> EvalDocument {
    serde_json::from_value(json!({
        "schema": "zeroshot.eval/v1",
        "slug": slug,
        "description": format!("{slug} description"),
        "runsPerArm": 2,
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
    .expect("fixture document")
}

fn temporary_files(root: &std::path::Path) -> Vec<String> {
    fs::read_dir(root)
        .map(|entries| {
            entries
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|name| name.ends_with(".tmp"))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn loads_two_self_contained_evals() {
    let workspace = TempDir::new().unwrap();
    let store = EvalStore::for_workspace(workspace.path());
    store.create(&document("alpha"), false).unwrap();
    store.create(&document("beta"), false).unwrap();

    let catalog = store.load_all().unwrap();
    assert_eq!(catalog.len(), 2);
    assert_eq!(
        catalog
            .iter()
            .map(|(slug, _)| slug.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    assert_eq!(catalog.get("alpha").unwrap().arms.len(), 2);
    assert_eq!(
        catalog.get("beta").unwrap().input,
        json!({"task": "repair the fixture"})
    );

    assert_eq!(
        store
            .list()
            .unwrap()
            .into_iter()
            .map(|summary| summary.slug)
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
}

#[test]
fn duplicate_slug_is_a_named_error() {
    let workspace = TempDir::new().unwrap();
    let root = workspace.path().join(".zeroshot/evals");
    fs::create_dir_all(&root).unwrap();

    let mut first = document("alpha");
    first.slug = "same".to_owned();
    let mut second = document("beta");
    second.slug = "same".to_owned();
    fs::write(
        root.join("first.json"),
        serde_json::to_vec_pretty(&first).unwrap(),
    )
    .unwrap();
    fs::write(
        root.join("second.json"),
        serde_json::to_vec_pretty(&second).unwrap(),
    )
    .unwrap();

    let error = EvalStore::new(&root).load_all().unwrap_err();
    let message = error.to_string();
    assert!(
        matches!(error, EvalError::DuplicateSlug { ref slug, .. } if slug == "same"),
        "{error:?}"
    );
    assert!(message.contains("duplicate eval slug"), "{message}");
    assert!(message.contains("first.json"), "{message}");
    assert!(message.contains("second.json"), "{message}");
}

#[test]
fn failed_atomic_replace_keeps_the_previous_document() {
    let workspace = TempDir::new().unwrap();
    let store = EvalStore::for_workspace(workspace.path());
    let path = store.create(&document("alpha"), false).unwrap();
    let before = fs::read(&path).unwrap();

    let error = publish_atomic(&path, b"{\n  \"slug\": \"partial\"", true, |temporary| {
        fs::write(temporary, b"{\n  \"slug\": \"partial\"")?;
        Err(io::Error::other("simulated interruption before rename"))
    })
    .unwrap_err();

    assert!(matches!(error, EvalError::Io { .. }), "{error:?}");
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(store.load("alpha").unwrap(), document("alpha"));
    assert!(temporary_files(store.root()).is_empty());
}

#[test]
fn write_once_refuses_then_overwrite_replaces() {
    let workspace = TempDir::new().unwrap();
    let store = EvalStore::for_workspace(workspace.path());
    store.create(&document("alpha"), false).unwrap();
    let before = fs::read(store.root().join("alpha.json")).unwrap();

    let mut replacement = document("alpha");
    replacement.description = Some("replacement".to_owned());
    let error = store.create(&replacement, false).unwrap_err();
    let message = error.to_string();
    assert!(
        matches!(error, EvalError::AlreadyExists { ref slug, .. } if slug == "alpha"),
        "{error:?}"
    );
    assert!(message.contains("already exists"), "{message}");
    assert_eq!(
        fs::read(store.root().join("alpha.json")).unwrap(),
        before,
        "a refused write must leave the existing document untouched"
    );

    store.create(&replacement, true).unwrap();
    assert_eq!(
        store.load("alpha").unwrap().description.as_deref(),
        Some("replacement")
    );
    assert!(temporary_files(store.root()).is_empty());
}

#[test]
fn empty_directory_lists_as_stable_json() {
    let workspace = TempDir::new().unwrap();
    let store = EvalStore::for_workspace(workspace.path());
    assert!(store.list().unwrap().is_empty());
    assert!(store.load_all().unwrap().is_empty());

    let mut output = Vec::new();
    let outcome = try_execute_native_v2_static(
        &NativeV2CliCommand::Eval(crate::native_v2_cli::EvalCommand::List {
            root: store.root().to_path_buf(),
        }),
        &mut output,
    )
    .unwrap()
    .unwrap();
    assert_eq!(outcome, crate::native_v2_cli::CliOutcome::Completed);
    assert_eq!(output, b"[]\n");
}

#[test]
fn rejects_slug_that_does_not_match_the_file_name() {
    let workspace = TempDir::new().unwrap();
    let root = workspace.path().join(".zeroshot/evals");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("renamed.json"),
        serde_json::to_vec_pretty(&document("alpha")).unwrap(),
    )
    .unwrap();

    let error = EvalStore::new(&root).load_all().unwrap_err();
    assert!(
        matches!(error, EvalError::SlugMismatch { ref slug, .. } if slug == "alpha"),
        "{error:?}"
    );
}

#[test]
fn parses_eval_commands_against_a_workspace() {
    let workspace = TempDir::new().unwrap();
    let root = workspace.path().join(".zeroshot/evals");
    let parse = |values: &[&str]| {
        crate::native_v2_cli::parse_native_v2_args(values.iter().map(std::ffi::OsString::from))
            .unwrap()
    };
    let directory = workspace.path().to_str().unwrap();

    assert_eq!(
        parse(&["eval", "--directory", directory, "list"]),
        NativeV2CliCommand::Eval(crate::native_v2_cli::EvalCommand::List { root: root.clone() })
    );
    assert_eq!(
        parse(&["eval", "--directory", directory, "show", "alpha"]),
        NativeV2CliCommand::Eval(crate::native_v2_cli::EvalCommand::Show {
            root: root.clone(),
            slug: "alpha".to_owned(),
        })
    );
    assert_eq!(
        parse(&[
            "eval",
            "--directory",
            directory,
            "create",
            "eval.json",
            "--overwrite"
        ]),
        NativeV2CliCommand::Eval(crate::native_v2_cli::EvalCommand::Create {
            root: root.clone(),
            file: std::path::PathBuf::from("eval.json"),
            overwrite: true,
        })
    );
    assert_eq!(
        parse(&["eval", "--directory", directory, "clean", "alpha"]),
        NativeV2CliCommand::Eval(crate::native_v2_cli::EvalCommand::Clean {
            root: root.clone(),
            slug: "alpha".to_owned(),
        })
    );
    assert_eq!(
        parse(&["eval", "--directory", directory, "run", "alpha"]),
        NativeV2CliCommand::Eval(crate::native_v2_cli::EvalCommand::Run(
            crate::native_v2_cli::EvalRunCommand {
                root: root.clone(),
                slug: "alpha".to_owned(),
                target: None,
                restart: false,
                stamp: None,
                retry_failed: false,
            }
        ))
    );
    assert_eq!(
        parse(&[
            "eval",
            "--directory",
            directory,
            "run",
            "alpha",
            "--target",
            "prod"
        ]),
        NativeV2CliCommand::Eval(crate::native_v2_cli::EvalCommand::Run(
            crate::native_v2_cli::EvalRunCommand {
                root,
                slug: "alpha".to_owned(),
                target: Some("prod".to_owned()),
                restart: false,
                stamp: None,
                retry_failed: false,
            }
        ))
    );
}

fn agent_document(slug: &str) -> EvalDocument {
    use crate::native_v2_cli::{BuiltinGraphTemplate, TemplateDelivery};

    let graph = BuiltinGraphTemplate::SingleWorker
        .materialize(TemplateDelivery::None)
        .unwrap();
    let mut nodes = serde_json::Map::new();
    for (name, delivery) in crate::native_v2_admission::executable_runtime_roles(&graph.root) {
        assert!(!delivery, "single-worker binds no delivery worker");
        nodes.insert(
            name.as_str().to_owned(),
            json!({"kind":"agent","model":"gpt-6.1-sol","effort":"high"}),
        );
    }
    let graph = serde_json::to_value(&graph).unwrap();
    let runtime = json!({
        "harness":"codex",
        "provider":"openai",
        "size":"medium",
        "globalAgents":{"postmortem":{"sourceNode":"worker"}},
        "nodes":nodes
    });
    let mut evaluation = document(slug);
    for arm in &mut evaluation.arms {
        arm.graph = serde_json::from_value(graph.clone()).unwrap();
        arm.runtime = serde_json::from_value(runtime.clone()).unwrap();
    }
    evaluation
}

#[test]
fn judge_runtime_clears_an_arm_postmortem_source_the_judge_graph_lacks() {
    use crate::native_v2_cli::{BuiltinGraphTemplate, TemplateDelivery};

    let evaluation = agent_document("alpha");
    assert_eq!(
        evaluation.arms[0]
            .runtime
            .global_agents
            .postmortem
            .source_node
            .as_ref()
            .map(|node| node.as_str()),
        Some("worker"),
        "the fixture reproduces an arm whose postmortem names its own worker"
    );
    let template = BuiltinGraphTemplate::EvalJudge;
    let graph = template.materialize(TemplateDelivery::None).unwrap();
    let runtime = super::config::judge_runtime(&evaluation, template, &graph).unwrap();

    assert_eq!(
        runtime.global_agents.postmortem.source_node, None,
        "the judge runtime must not name a node its own graph lacks"
    );
    assert_eq!(
        runtime
            .nodes
            .keys()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
        ["judge"]
    );
}

#[tokio::test]
async fn judge_plan_is_validated_against_the_judge_graph_before_sampling() {
    use crate::native_v2_cli::{BuiltinGraphTemplate, TemplateDelivery};

    let evaluation = agent_document("alpha");
    let template = BuiltinGraphTemplate::EvalJudge;
    let graph = template.materialize(TemplateDelivery::None).unwrap();
    let mut runtime = super::config::judge_runtime(&evaluation, template, &graph).unwrap();
    runtime.nodes.clear();
    let plan = super::config::JudgePlan { graph, runtime };

    let error = super::config::validate_judge_plan(&evaluation, &plan)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, EvalError::Invalid(message) if message.contains("cannot derive a judge runtime")),
        "{error:?}"
    );
}

#[test]
fn verdicts_reject_unknown_criteria_and_score_duplicates_at_most_once() {
    use super::run::{Verdict, verdicts, weighted_score};

    let evaluation = document("alpha");
    let criteria = &evaluation.scoring.criteria;
    let output = json!({
        "verdicts": [
            {"id": "quality", "pass": true, "reason": "first"},
            {"id": "quality", "pass": false, "reason": "duplicate must not score"},
            {"id": "missing", "pass": false, "reason": "declared only in the eval"}
        ]
    });

    let error = verdicts(&output, criteria).unwrap_err();
    assert!(
        matches!(&error, EvalError::Invalid(message) if message.contains("unknown criterion \"missing\"")),
        "{error:?}"
    );

    let output = json!({
        "verdicts": [
            {"id": "quality", "pass": true, "reason": "first"},
            {"id": "quality", "pass": false, "reason": "duplicate must not score"}
        ]
    });
    let parsed = verdicts(&output, criteria).unwrap();
    assert_eq!(
        parsed,
        [Verdict {
            id: "quality".to_owned(),
            pass: true,
            reason: "first".to_owned(),
        }]
    );
    let recorded = parsed.iter().collect::<Vec<_>>();
    assert_eq!(weighted_score(criteria, &recorded), Some(1.0));

    let output = json!({"verdicts": [{"id": "quality", "pass": false, "reason": "no"}]});
    let parsed = verdicts(&output, criteria).unwrap();
    let recorded = parsed.iter().collect::<Vec<_>>();
    assert_eq!(weighted_score(criteria, &recorded), Some(0.0));
}

fn git(workspace: &std::path::Path, arguments: &[&str]) -> String {
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
        .unwrap();
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test]
async fn a_branch_named_arm_is_resolved_once_to_the_same_revision() {
    let workspace = TempDir::new().unwrap();
    git(
        workspace.path(),
        &["init", "--quiet", "--initial-branch=main"],
    );
    fs::write(workspace.path().join("README.md"), "eval fixture\n").unwrap();
    git(workspace.path(), &["add", "README.md"]);
    git(
        workspace.path(),
        &["commit", "--quiet", "-m", "eval fixture"],
    );
    git(workspace.path(), &["branch", "candidate"]);
    let expected = git(workspace.path(), &["rev-parse", "candidate^{commit}"]);
    let expected = expected.trim();

    let mut evaluation = document("alpha");
    evaluation.arms[0].head = "candidate".to_owned();
    let source = super::config::pin_arm_source(workspace.path(), &evaluation, &evaluation.arms[0])
        .await
        .unwrap();
    assert_eq!(source.branch, None, "the branch name is never submitted");
    assert_eq!(
        source.revision.as_ref().map(|revision| revision.as_str()),
        Some(expected)
    );
    let first = source.revision.clone();

    git(workspace.path(), &["checkout", "--quiet", "candidate"]);
    fs::write(workspace.path().join("README.md"), "moved\n").unwrap();
    git(workspace.path(), &["add", "README.md"]);
    git(
        workspace.path(),
        &["commit", "--quiet", "-m", "move candidate"],
    );
    let moved = super::config::pin_arm_source(workspace.path(), &evaluation, &evaluation.arms[0])
        .await
        .unwrap();
    assert_ne!(
        moved.revision, first,
        "a fresh run resolves the branch again"
    );
    assert_eq!(first, source.revision);
}

#[tokio::test]
async fn an_unresolvable_branch_is_refused_instead_of_sent_as_a_name() {
    let workspace = TempDir::new().unwrap();
    git(
        workspace.path(),
        &["init", "--quiet", "--initial-branch=main"],
    );
    let mut evaluation = document("alpha");
    evaluation.arms[0].head = "no-such-branch".to_owned();
    let error = super::config::pin_arm_source(workspace.path(), &evaluation, &evaluation.arms[0])
        .await
        .unwrap_err();
    assert!(
        matches!(&error, EvalError::Invalid(message)
            if message.contains("cannot resolve head branch \"no-such-branch\"")),
        "{error:?}"
    );
}

fn stage<'a>(arm: &'a str, sample: u32, stage: super::run::RunStage) -> super::run::StageId<'a> {
    super::run::StageId { arm, sample, stage }
}

fn pin(target: &str, revision: &str, runs_per_arm: u32) -> super::run::StampPin {
    super::run::StampPin {
        document_sha256: "sha".to_owned(),
        target: target.to_owned(),
        judge: None,
        runs_per_arm,
        arms: vec![super::run::StampArm {
            id: "arm-0".to_owned(),
            revision: revision.to_owned(),
        }],
    }
}

#[test]
fn resume_journal_records_each_stage_attempt_and_its_outcome() {
    let stamp = TempDir::new().unwrap();
    let at = std::time::SystemTime::now();
    let mut journal = super::run::ResumeJournal::new(pin("prod", "abc", 1));

    assert_eq!(journal.recorded("arm-0", 0), None);
    assert_eq!(journal.attempt("arm-0", 0, super::run::RunStage::Sample), 0);
    assert_eq!(
        journal.claim(stage("arm-0", 0, super::run::RunStage::Sample), "run-1", at),
        1
    );
    journal.save(stamp.path()).unwrap();

    let mut loaded = super::run::ResumeJournal::load(stamp.path())
        .unwrap()
        .unwrap();
    assert_eq!(loaded, journal);
    assert_eq!(
        loaded.recorded("arm-0", 0),
        Some((super::run::RunStage::Sample, "run-1"))
    );
    loaded.settle("arm-0", 0, at);
    assert!(!loaded.samples[0].failed);
    assert_eq!(
        loaded.claim(stage("arm-0", 0, super::run::RunStage::Judge), "run-2", at),
        1,
        "a sample's judge stage keeps its own attempt count"
    );
    assert_eq!(
        loaded.claim(stage("arm-0", 0, super::run::RunStage::Judge), "run-3", at),
        2,
        "each retry of one stage is counted"
    );
    loaded.fail("arm-0", 0, at);
    assert!(loaded.samples[0].failed);
    assert_eq!(loaded.samples.len(), 1, "one sample has one journal entry");
    loaded.save(stamp.path()).unwrap();
    let reloaded = super::run::ResumeJournal::load(stamp.path())
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.samples[0].run_id, "run-3");
    assert!(reloaded.samples[0].failed);
}

#[test]
fn stamp_pin_names_the_first_fact_that_no_longer_matches() {
    let pinned = pin("prod", "abc", 1);
    assert_eq!(pinned.difference(&pin("prod", "abc", 1)), None);
    assert_eq!(
        pin("prod", "abc", 1).difference(&pin("prod", "def", 1)),
        Some("arm \"arm-0\" moved from abc to def".to_owned())
    );
    let no_arms = super::run::StampPin {
        arms: Vec::new(),
        ..pin("prod", "abc", 1)
    };
    assert_eq!(
        pinned.difference(&no_arms),
        Some("arm \"arm-0\" was removed".to_owned())
    );
    assert_eq!(
        no_arms.difference(&super::run::StampPin {
            arms: vec![super::run::StampArm {
                id: "arm-1".to_owned(),
                revision: "abc".to_owned(),
            }],
            ..pin("prod", "abc", 1)
        }),
        Some("arm \"arm-1\" was added".to_owned())
    );
    assert_eq!(
        pinned.difference(&pin("staging", "abc", 1)),
        Some("target moved from \"prod\" to \"staging\"".to_owned())
    );
    assert_eq!(
        pinned.difference(&pin("prod", "abc", 2)),
        Some("runsPerArm moved from 1 to 2".to_owned())
    );
    assert_eq!(
        pinned.difference(&super::run::StampPin {
            judge: Some("eval-judge".to_owned()),
            ..pin("prod", "abc", 1)
        }),
        Some("judge moved from None to Some(\"eval-judge\")".to_owned())
    );
    assert!(
        pinned
            .difference(&super::run::StampPin {
                document_sha256: "sha2".to_owned(),
                ..pin("prod", "abc", 1)
            })
            .is_some_and(|message| message.starts_with("the eval document changed from sha"))
    );
}

#[test]
fn write_json_checked_reuses_identical_bytes_and_refuses_changed_ones() {
    let stamp = TempDir::new().unwrap();
    let destination = stamp.path().join("input.json");
    let value = json!({"task": "repair"});
    super::run::write_json_checked(&destination, &value).unwrap();
    let published = fs::read(&destination).unwrap();
    super::run::write_json_checked(&destination, &value).unwrap();
    assert_eq!(fs::read(&destination).unwrap(), published);

    let error =
        super::run::write_json_checked(&destination, &json!({"task": "other"})).unwrap_err();
    assert!(
        matches!(&error, EvalError::Invalid(message)
            if message.contains("does not match the evidence this run would write")),
        "{error:?}"
    );
    assert_eq!(
        fs::read(&destination).unwrap(),
        published,
        "a refused rewrite never replaces the recorded evidence"
    );
}

#[test]
fn retire_result_keeps_the_failed_attempt_beside_the_canonical_result() {
    let stamp = TempDir::new().unwrap();
    let result = stamp.path().join("result.json");
    fs::write(&result, b"{\"status\":\"failed\"}").unwrap();
    super::run::retire_result(&result, 3).unwrap();
    assert!(!result.exists());
    assert_eq!(
        fs::read(stamp.path().join("result.failed-3.json")).unwrap(),
        b"{\"status\":\"failed\"}"
    );
}
