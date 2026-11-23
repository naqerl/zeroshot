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
        &NativeV2CliCommand::EvalList {
            root: store.root().to_path_buf(),
        },
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
        NativeV2CliCommand::EvalList { root: root.clone() }
    );
    assert_eq!(
        parse(&["eval", "--directory", directory, "show", "alpha"]),
        NativeV2CliCommand::EvalShow {
            root: root.clone(),
            slug: "alpha".to_owned(),
        }
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
        NativeV2CliCommand::EvalCreate {
            root,
            file: std::path::PathBuf::from("eval.json"),
            overwrite: true,
        }
    );
}
