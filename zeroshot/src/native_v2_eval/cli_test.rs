use crate::native_v2_eval::{
    EvalAction, EvalCommand, EvalData, EvalError, EvalJudgeCommand, run_fixture,
};

use super::{execute, resolve_manifest_path};

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
            judge_model: Some("relabel-attempt".to_owned()),
            judge_template: Some("eval-judge".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect_err("eval judge --judge-model must not use the fixture executor");
    assert!(matches!(error, EvalError::Judge(_)));
    assert!(error.to_string().contains("judgeRuntimeConfig"));
    assert!(error.to_string().contains("relabel-attempt"));
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
