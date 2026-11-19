use crate::native_v2_eval::{
    EvalAction, EvalCommand, EvalData, EvalError, EvalJudgeCommand, EvalStore, SampleStatus,
    run_fixture,
};

use super::{execute, resolve_manifest_path, resolve_template_reference};

fn fixture_source() -> &'static str {
    include_str!("../../fixtures/eval/synthetic.toml")
}

fn write_fixture(directory: &std::path::Path, source: &str) -> std::path::PathBuf {
    let path = directory.join("eval.toml");
    std::fs::write(&path, source).expect("fixture manifest is writable");
    path
}

async fn run_fixture_in(directory: &std::path::Path, id: &str) -> EvalData {
    let manifest = write_fixture(directory, fixture_source());
    run_fixture(manifest, directory, Some(id.to_owned()))
        .await
        .expect("fixture eval runs")
}

#[tokio::test]
async fn cli_judge_action_without_a_runtime_names_the_missing_runtime_config() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-cli-no-runtime").await;
    let error = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Judge(EvalJudgeCommand {
            id: data.id.to_string(),
            judge_runner: Some("local".to_owned()),
            judge_template: Some("eval-judge".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect_err("a local judge without a runtime fails closed");
    assert!(matches!(error, EvalError::Judge(_)));
    assert!(error.to_string().contains("judgeRuntimeConfig"));
}

#[test]
fn relative_judge_runtime_paths_resolve_against_the_manifest_directory() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let directory = temporary
        .path()
        .join(".zeroshot")
        .join("eval")
        .join("example");
    let resolved =
        resolve_manifest_path(&directory, "judge-runtime.json").expect("relative path resolves");
    assert_eq!(resolved, directory.join("judge-runtime.json"));
    let absolute = temporary.path().join("judge-runtime.json");
    assert_eq!(
        resolve_manifest_path(&directory, absolute.to_str().unwrap())
            .expect("absolute path stays absolute"),
        absolute
    );
}

#[tokio::test]
async fn synthetic_judge_refuses_to_relabel_graph_judged_scores() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-graph-judged").await;
    let store = EvalStore::for_workspace(temporary.path());
    let mut loaded = store.load(&data.id).expect("stored eval loads");
    let mut before = std::collections::BTreeMap::new();
    for sample in &mut loaded.samples {
        if sample.status == SampleStatus::Succeeded {
            sample.judge_executor = Some("graph".to_owned());
            store
                .write_sample(&loaded.id, sample)
                .expect("graph-judged sample writes");
            before.insert(sample.id.clone(), sample.criteria.clone());
        }
    }

    let error = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Judge(EvalJudgeCommand {
            id: data.id.to_string(),
            judge_runner: Some("synthetic".to_owned()),
            judge_template: Some("eval-judge".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect_err("the synthetic judge must not relabel graph scores");
    assert!(matches!(error, EvalError::Judge(_)));
    assert!(error.to_string().contains("refuses to relabel"));

    let stored = store.load(&data.id).expect("judged eval reloads");
    for sample in stored.samples {
        let Some(expected) = before.get(&sample.id) else {
            continue;
        };
        assert_eq!(&sample.criteria, expected);
        assert_eq!(sample.judge_executor.as_deref(), Some("graph"));
    }
}

#[test]
fn judge_template_overrides_resolve_against_the_invoking_folder() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let manifest_directory = temporary
        .path()
        .join(".zeroshot")
        .join("eval")
        .join("example");
    let override_path = resolve_template_reference(&manifest_directory, "graph.json", true)
        .expect("override path resolves");
    assert_eq!(
        std::path::PathBuf::from(override_path),
        std::path::absolute("graph.json").expect("invoking folder path")
    );
    let manifest_path = resolve_template_reference(&manifest_directory, "graph.json", false)
        .expect("manifest path resolves");
    assert_eq!(
        std::path::PathBuf::from(manifest_path),
        manifest_directory.join("graph.json")
    );
    assert_eq!(
        resolve_template_reference(&manifest_directory, "eval-judge", true)
            .expect("built-in template"),
        "eval-judge"
    );
}
