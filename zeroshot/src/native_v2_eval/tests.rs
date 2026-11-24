use std::fs;
use std::io;

use serde_json::{Value, json};
use tempfile::TempDir;

use super::{EvalDocument, EvalError, EvalStore, publish_atomic};
use crate::native_v2_cli::{
    EvalCommand, EvalRunCommand, NativeV2CliCommand, execution::try_execute_native_v2_static,
};

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
            arm("candidate", "candidate-branch")
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
        &NativeV2CliCommand::Eval(EvalCommand::List {
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
        NativeV2CliCommand::Eval(EvalCommand::List { root: root.clone() })
    );
    assert_eq!(
        parse(&["eval", "--directory", directory, "show", "alpha"]),
        NativeV2CliCommand::Eval(EvalCommand::Show {
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
        NativeV2CliCommand::Eval(EvalCommand::Create {
            root: root.clone(),
            file: std::path::PathBuf::from("eval.json"),
            overwrite: true,
        })
    );
    assert_eq!(
        parse(&["eval", "--directory", directory, "clean", "alpha"]),
        NativeV2CliCommand::Eval(EvalCommand::Clean {
            root: root.clone(),
            slug: "alpha".to_owned(),
        })
    );
    assert_eq!(
        parse(&[
            "eval",
            "--directory",
            directory,
            "run",
            "alpha",
            "--target",
            "platform"
        ]),
        NativeV2CliCommand::Eval(EvalCommand::Run(EvalRunCommand {
            root,
            slug: "alpha".to_owned(),
            target: Some("platform".to_owned()),
        }))
    );
}

#[test]
fn a_target_is_required_before_any_sample_runs() {
    let mut evaluation = document("alpha");
    let error = super::config::resolve_target(&evaluation, None).unwrap_err();
    assert!(
        matches!(&error, EvalError::Invalid(message) if message.contains("requires a nonempty target")),
        "{error:?}"
    );

    evaluation.target = Some("prod".to_owned());
    assert_eq!(
        super::config::resolve_target(&evaluation, None).unwrap(),
        "prod"
    );
    assert_eq!(
        super::config::resolve_target(&evaluation, Some("staging")).unwrap(),
        "staging",
        "the command line wins so one stored eval compares on several targets"
    );
    assert_eq!(
        super::config::resolve_target(&evaluation, Some("  ")).unwrap(),
        "prod",
        "a blank flag falls back to the document instead of naming a target"
    );
}

#[test]
fn an_unknown_judge_template_is_named_with_the_shipped_ones() {
    let mut evaluation = document("alpha");
    assert!(super::config::judge_plan(&evaluation).unwrap().is_none());

    evaluation.judge = Some(super::EvalJudge {
        template: "no-such-judge".to_owned(),
    });
    let error = super::config::judge_plan(&evaluation).unwrap_err();
    let EvalError::Invalid(message) = &error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(
        message.contains("unknown judge template \"no-such-judge\""),
        "{message}"
    );
    assert!(message.contains("eval-judge"), "{message}");
}

#[test]
fn judge_runtime_borrows_the_lane_of_the_first_arm_that_binds_an_agent() {
    let mut evaluation = document("alpha");
    evaluation.arms[1].runtime.nodes.insert(
        openengine_cluster_protocol::NodeName::new("worker").unwrap(),
        serde_json::from_value(json!({
            "kind": "agent",
            "model": "gpt-6.1-sol",
            "effort": "high",
            "sessionScope": "node_instance"
        }))
        .unwrap(),
    );
    let template = crate::native_v2_cli::BuiltinGraphTemplate::EvalJudge;
    let graph = template
        .materialize(crate::native_v2_cli::TemplateDelivery::None)
        .unwrap();
    let runtime = super::config::judge_runtime(&evaluation, template, &graph).unwrap();

    assert_eq!(runtime.harness.as_str(), "codex");
    assert_eq!(runtime.provider.as_str(), "openai");
    assert_eq!(
        runtime
            .nodes
            .keys()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
        ["judge"],
        "every agent the judge graph declares is bound, and nothing else"
    );
    let Some(binding) = runtime.nodes.values().next() else {
        panic!("the judge is bound");
    };
    assert!(
        matches!(binding, openengine_cluster_protocol::NodeRuntimeBinding::Agent { model, .. }
            if model.as_str() == "gpt-6.1-sol"),
        "{binding:?}"
    );
}

#[test]
fn judge_runtime_refuses_a_template_that_needs_a_delivery_worker() {
    let mut evaluation = document("alpha");
    evaluation.arms[0].runtime.nodes.insert(
        openengine_cluster_protocol::NodeName::new("worker").unwrap(),
        serde_json::from_value(json!({
            "kind": "agent",
            "model": "gpt-6.1-sol"
        }))
        .unwrap(),
    );
    let template = crate::native_v2_cli::BuiltinGraphTemplate::SoftwareChange;
    let graph = template
        .materialize(crate::native_v2_cli::TemplateDelivery::PullRequest)
        .unwrap();
    let error = super::config::judge_runtime(&evaluation, template, &graph).unwrap_err();
    assert!(
        matches!(&error, EvalError::Invalid(message) if message.contains("needs a delivery worker")),
        "{error:?}"
    );
}

#[test]
fn judge_runtime_refuses_an_eval_whose_arms_bind_no_agent() {
    let evaluation = document("alpha");
    let template = crate::native_v2_cli::BuiltinGraphTemplate::EvalJudge;
    let graph = template
        .materialize(crate::native_v2_cli::TemplateDelivery::None)
        .unwrap();
    let error = super::config::judge_runtime(&evaluation, template, &graph).unwrap_err();
    assert!(
        matches!(&error, EvalError::Invalid(message) if message.contains("no arm binds an agent")),
        "{error:?}"
    );
}

#[test]
fn an_arm_head_is_a_revision_only_when_it_names_a_git_object() {
    let evaluation = document("alpha");
    let branch = super::config::arm_source(&evaluation, &evaluation.arms[1]).unwrap();
    assert_eq!(branch.branch.unwrap().as_str(), "candidate-branch");
    assert!(
        branch.revision.is_none(),
        "a branch is never sent as a revision"
    );

    for (head, revision) in [
        ("a".repeat(40), true),
        ("a".repeat(64), true),
        ("a".repeat(39), false),
        ("a".repeat(63), false),
        ("g".repeat(40), false),
        ("main".to_owned(), false),
    ] {
        let mut evaluation = document("alpha");
        evaluation.arms[0].head = head.clone();
        let source = super::config::arm_source(&evaluation, &evaluation.arms[0]).unwrap();
        assert_eq!(source.revision.is_some(), revision, "head {head:?}");
        assert_eq!(source.branch.is_some(), !revision, "head {head:?}");
    }

    let mut evaluation = document("alpha");
    evaluation.arms[0].head = "  ".to_owned();
    let error = super::config::arm_source(&evaluation, &evaluation.arms[0]).unwrap_err();
    assert!(
        matches!(&error, EvalError::Invalid(message) if message.contains("requires a nonempty head")),
        "{error:?}"
    );
}

#[test]
fn a_stamp_is_created_exclusively_and_sorts_after_the_runs_before_it() {
    let workspace = TempDir::new().unwrap();
    let runs_root = workspace.path().join("runs");
    let now = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_123);
    let first = super::run::create_stamp(&runs_root, now).unwrap();
    let second = super::run::create_stamp(&runs_root, now).unwrap();

    assert_ne!(first, second, "two runs can never share a stamp");
    assert!(first.is_dir() && second.is_dir());
    assert!(first < second, "the collision suffix keeps creation order");
    assert_eq!(
        first.file_name().unwrap().to_string_lossy(),
        "1700000000123"
    );
    assert_eq!(
        super::run::sample_directory(&first, "baseline", 0)
            .file_name()
            .unwrap(),
        "baseline-0"
    );
}

#[test]
fn a_report_never_calls_an_unjudged_sample_a_failed_verdict() {
    use super::run::{ArmSummary, RunSummary, SampleStatus, SampleSummary, report};

    fn sample(status: SampleStatus, reason: Option<&str>) -> SampleSummary {
        SampleSummary {
            arm: "baseline".to_owned(),
            sample: 0,
            run_id: "run-public".to_owned(),
            status,
            reason: reason.map(str::to_owned),
            verdicts: Vec::new(),
        }
    }

    fn aggregate(judge: Option<&str>, sample: SampleSummary) -> RunSummary {
        RunSummary {
            slug: "compare".to_owned(),
            stamp: "1700000000123".to_owned(),
            target: "prod".to_owned(),
            judge: judge.map(str::to_owned),
            arms: vec![ArmSummary {
                id: "baseline".to_owned(),
                samples: 1,
                succeeded: u32::from(sample.status == SampleStatus::Succeeded),
                failed: u32::from(sample.status == SampleStatus::Failed),
                verdicts: 0,
                passed: 0,
                score: None,
            }],
            samples: vec![sample],
            interrupted: false,
        }
    }

    // A sample the eval never asked anyone to judge stays a success in the verdict table too.
    let unjudged = report(&aggregate(None, sample(SampleStatus::Succeeded, None)));
    assert!(
        unjudged.contains("| baseline | 0 | - | unjudged | no judge configured for this eval |"),
        "unexpected report: {unjudged}"
    );
    assert!(
        !unjudged.contains("| failed |"),
        "an unjudged sample is never a failure: {unjudged}"
    );

    // A sample whose judge run stopped early says why it carries no verdict.
    let stopped = report(&aggregate(
        Some("eval-judge"),
        sample(
            SampleStatus::Succeeded,
            Some("the judge run did not finish before the eval stopped"),
        ),
    ));
    assert!(
        stopped.contains(
            "| baseline | 0 | - | unjudged | the judge run did not finish before the eval stopped |"
        ),
        "unexpected report: {stopped}"
    );

    // A sample that really failed reports its own recorded reason, not the judge's absence.
    let failed = report(&aggregate(
        None,
        sample(SampleStatus::Failed, Some("worker_failed")),
    ));
    assert!(
        failed.contains("| baseline | 0 | - | failed | worker_failed |"),
        "unexpected report: {failed}"
    );
    let unexplained = report(&aggregate(None, sample(SampleStatus::Failed, None)));
    assert!(
        unexplained.contains("| baseline | 0 | - | failed | the sample run did not succeed |"),
        "unexpected report: {unexplained}"
    );
}
