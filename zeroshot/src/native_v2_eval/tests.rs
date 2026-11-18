use super::*;
use serde_json::json;

fn fixture_source() -> &'static str {
    include_str!("../../fixtures/eval/synthetic.toml")
}

fn write_fixture(directory: &std::path::Path, source: &str) -> std::path::PathBuf {
    let path = directory.join("eval.toml");
    std::fs::write(&path, source).expect("fixture manifest is writable");
    path
}

async fn run_fixture_in(directory: &std::path::Path, id: &str, source: &str) -> EvalData {
    let manifest = write_fixture(directory, source);
    run_fixture(manifest, directory, Some(id.to_owned()))
        .await
        .expect("fixture eval runs")
}

#[test]
fn manifest_validation_failure_is_named_and_never_panics() {
    let invalid_runs = fixture_source().replace("runsPerArm = 3", "runsPerArm = 0");
    let error = EvalManifest::parse(&invalid_runs).expect_err("zero runs must be rejected");
    assert!(matches!(error, EvalError::Manifest(_)));
    assert!(error.to_string().contains("runsPerArm"));

    let invalid_arm = fixture_source().replace(
        "id = \"baseline\"\ntemplate = \"single-worker\"",
        "id = \"baseline\"\ntemplate = \"single-worker\"\ngraph = \"graphs/other.json\"",
    );
    let error = EvalManifest::parse(&invalid_arm).expect_err("two references must be rejected");
    assert!(
        error
            .to_string()
            .contains("exactly one of graph or template")
    );

    let unknown = format!("{}\nunexpectedRoot = true\n", fixture_source());
    assert!(EvalManifest::parse(&unknown).is_err());
}

#[tokio::test]
async fn full_fake_run_writes_a_trait_matrix_and_nonzero_score() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-run", fixture_source()).await;

    assert_eq!(data.samples.len(), 6);
    let counts = data.sample_counts();
    assert_eq!(counts.succeeded, 4);
    assert_eq!(counts.failed, 2);
    assert_eq!(counts.skipped, 0);

    let report = data.report.as_ref().expect("run writes a report");
    assert_eq!(report.schema, REPORT_SCHEMA);
    assert!(report.aggregate_score > 0.0);
    assert_eq!(report.matrix.rows.len(), 6);
    assert_eq!(
        report
            .matrix
            .columns
            .iter()
            .map(|column| column.id.as_str())
            .collect::<Vec<_>>(),
        vec!["correctness", "efficiency"]
    );
    let correctness = report
        .matrix
        .traits
        .iter()
        .find(|summary| summary.id == "correctness")
        .expect("correctness column");
    assert_eq!(correctness.attempted, 6);
    assert_eq!(correctness.passed, 4);
    assert!((correctness.rate - (4.0 / 6.0)).abs() < 1e-12);
    assert!(report.cost_time.fit.is_some());
    assert_eq!(
        report
            .cost_time
            .arms
            .iter()
            .map(|arm| arm.arm_id.as_str())
            .collect::<Vec<_>>(),
        vec!["baseline", "candidate"]
    );

    let eval_directory = temporary
        .path()
        .join(".zeroshot")
        .join("eval")
        .join("fixture-run");
    let stored: EvalReport = serde_json::from_slice(
        &std::fs::read(eval_directory.join("report.json")).expect("report.json exists"),
    )
    .expect("report.json is valid");
    assert_eq!(&stored, report);
    let markdown =
        std::fs::read_to_string(eval_directory.join("report.md")).expect("report.md exists");
    assert_eq!(markdown, render_markdown(report));
    assert!(markdown.contains("## Trait matrix"));
    assert!(markdown.contains("| correctness |"));

    let failed = data
        .samples
        .iter()
        .find(|sample| sample.status == SampleStatus::Failed)
        .expect("failed sample is stored");
    assert!(failed.failure_reason.is_some());
    assert!(failed.patch.is_none());
    let succeeded = data
        .samples
        .iter()
        .find(|sample| sample.status == SampleStatus::Succeeded)
        .expect("succeeded sample is stored");
    let patch = succeeded.patch.as_ref().expect("successful patch artifact");
    assert!(eval_directory.join(&patch.path).is_file());
    assert!(patch.bytes > 0);
}

#[tokio::test]
async fn report_and_score_reconstruct_from_stored_data_after_the_run() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let source = fixture_source().replace("scorer = \"quality-vs-cost\"", "scorer = \"none\"");
    let data = run_fixture_in(temporary.path(), "fixture-score", &source).await;
    let id = data.id.clone();
    assert_eq!(
        data.report.as_ref().map(|report| report.scorer.as_str()),
        Some("none")
    );
    assert!(
        data.report
            .as_ref()
            .and_then(|report| report.cost_time.fit.as_ref())
            .is_none()
    );

    // A new store instance reads only what is on disk: no in-memory state from the run survives.
    let store = EvalStore::for_workspace(temporary.path());
    let loaded = store.load(&id).expect("stored eval loads");
    assert_eq!(loaded.samples.len(), data.samples.len());
    assert_eq!(loaded.report, data.report);

    let report_command = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Report { id: id.to_string() },
    })
    .await
    .expect("report command succeeds");
    assert_eq!(report_command["scorer"], "none");

    let scored = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Score {
            id: id.to_string(),
            scorer: "quality-vs-cost".to_owned(),
        },
    })
    .await
    .expect("score command succeeds");
    assert_eq!(scored["scorer"], "quality-vs-cost");
    let fit = scored["costTime"]["fit"]
        .as_object()
        .expect("quality-vs-cost fits the stored arms");
    assert!(fit["a"].is_number());
    assert!(fit["b"].is_number());

    let rescored = store.load(&id).expect("rescored eval loads");
    let report = rescored.report.expect("rescored report is stored");
    assert_eq!(report.scorer, "quality-vs-cost");
    assert!(report.cost_time.fit.is_some());
}

#[tokio::test]
async fn failed_samples_are_recorded_and_reported() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let source = fixture_source().replace("failEvery = 2", "failEvery = 1");
    let data = run_fixture_in(temporary.path(), "fixture-failed", &source).await;
    assert_eq!(data.samples.len(), 6);
    assert!(
        data.samples
            .iter()
            .all(|sample| sample.status == SampleStatus::Failed)
    );
    assert!(data.samples.iter().all(|sample| {
        sample
            .failure_reason
            .as_deref()
            .is_some_and(|reason| !reason.is_empty())
    }));
    let report = data.report.as_ref().expect("failed eval still reports");
    assert_eq!(report.matrix.rows.len(), 6);
    assert!(
        report
            .matrix
            .rows
            .iter()
            .all(|row| row.status == SampleStatus::Failed)
    );
    assert!(
        report
            .matrix
            .rows
            .iter()
            .all(|row| { row.cells.values().all(|cell| cell.passed == Some(false)) })
    );
    assert!(
        report
            .matrix
            .traits
            .iter()
            .all(|summary| summary.attempted == 6 && summary.passed == 0)
    );

    let store = EvalStore::for_workspace(temporary.path());
    let loaded = store.load(&data.id).expect("failed eval reloads");
    assert_eq!(loaded.samples.len(), 6);
    assert!(loaded.report.is_some());
}

#[tokio::test]
async fn unmatched_reasoning_and_clean_are_visible() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-clean", fixture_source()).await;
    let id = data.id.clone();
    let store = EvalStore::for_workspace(temporary.path());
    let mut loaded = store.load(&id).expect("stored eval loads");
    if let Some(sample) = loaded.samples.first_mut() {
        sample
            .reasoning
            .insert("unknownTrait".to_owned(), "unmatched note".to_owned());
    }
    store
        .write_sample(&id, &loaded.samples[0])
        .expect("sample rewrites");
    let report = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Score {
            id: id.to_string(),
            scorer: "none".to_owned(),
        },
    })
    .await
    .expect("rescore succeeds");
    assert_eq!(
        report["unmatchedReasoning"][0]["key"],
        json!("unknownTrait")
    );

    let removed = store.clean_all().expect("clean all succeeds");
    assert_eq!(removed, vec![id]);
    assert!(store.list().expect("empty store lists").is_empty());
}
