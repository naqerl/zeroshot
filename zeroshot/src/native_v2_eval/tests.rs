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
fn cli_selects_the_unified_runner_from_the_manifest() {
    let fixture = EvalManifest::parse(fixture_source()).expect("fixture manifest parses");
    assert_eq!(
        select_runner(&fixture).expect("fixture runner is selected"),
        RunnerSelection::Fake
    );

    let local =
        EvalManifest::parse(&fixture_source().replace("runner = \"fake\"", "runner = \"local\""))
            .expect("local manifest parses");
    assert_eq!(
        select_runner(&local).expect("local runner is selected"),
        RunnerSelection::Local
    );

    let platform = EvalManifest::parse(
        &fixture_source().replace("runner = \"fake\"", "runner = \"platform\""),
    )
    .expect("platform manifest parses");
    assert_eq!(
        select_runner(&platform).expect("platform runner is selected"),
        RunnerSelection::Platform
    );
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
    let capacity = report.capacity.expect("fixture report records capacity");
    assert_eq!(capacity.concurrency, 1);
    assert_eq!(capacity.capacity_source.label(), "fallback");
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

    assert!(
        data.samples
            .iter()
            .all(|sample| { sample.run_id.starts_with(&format!("fake-{}-", data.id)) })
    );

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
            weights: Default::default(),
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
            weights: Default::default(),
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

fn criterion(id: &str) -> EvalCriterion {
    EvalCriterion {
        id: id.to_owned(),
        description: format!("{id} description"),
        weight: 1.0,
    }
}

#[test]
fn judge_parser_reads_trait_verdicts_and_names_the_invalid_field() {
    let criteria = vec![criterion("correctness"), criterion("tests")];
    let valid = r#"{
      "traits": [
        {"id": "correctness", "pass": true, "reason": "The patch updates the parser."},
        {"id": "tests", "pass": false, "reason": "line one\nline two\tindented"}
      ]
    }"#;
    let parsed = parse_judgement(valid, &criteria).expect("valid verdicts parse");
    assert!(parsed.judgements["correctness"].passed());
    assert!(!parsed.judgements["tests"].passed());
    assert!(parsed.judgements["tests"].reason.contains("line two"));
    let stored = parsed.criteria();
    assert!(stored["correctness"].passed);
    assert_eq!(stored["correctness"].score, Some(1.0));
    assert_eq!(stored["tests"].score, Some(0.0));
    assert_eq!(
        parsed.reasoning()["correctness"],
        "The patch updates the parser."
    );

    let one = r#"{"id":"correctness","pass":true,"reason":"ok"}"#;
    let cases = [
        // malformed output
        ("not json".to_owned(), "<json>", "expected"),
        ("[]".to_owned(), "<root>", "JSON object"),
        (
            r#"{"judgements":[]}"#.to_owned(),
            "judgements",
            "expected `traits`",
        ),
        (r#"{"traits":{}}"#.to_owned(), "traits", "must be an array"),
        (
            r#"{"traits":["x"]}"#.to_owned(),
            "traits[0]",
            "must be an object",
        ),
        (
            format!(r#"{{"traits":[{one},{{"id":"tests","reason":"ok"}}]}}"#),
            "traits[1].pass",
            "is required",
        ),
        (
            format!(r#"{{"traits":[{one},{{"id":"tests","pass":"yes","reason":"ok"}}]}}"#),
            "traits[1].pass",
            "must be a boolean",
        ),
        (
            format!(r#"{{"traits":[{one},{{"id":"tests","pass":true,"reason":"  "}}]}}"#),
            "traits[1].reason",
            "must not be empty",
        ),
        (
            format!(
                r#"{{"traits":[{one},{{"id":"tests","pass":true,"reason":"bad\u0001text"}}]}}"#
            ),
            "traits[1].reason",
            "control characters",
        ),
        (
            format!(r#"{{"traits":[{one},{{"id":"tests","pass":true,"reason":"ok","score":4}}]}}"#),
            "traits[1].score",
            "unknown field",
        ),
        (
            format!(r#"{{"traits":[{one},{{"pass":true,"reason":"ok"}}]}}"#),
            "traits[1].id",
            "is required",
        ),
        // unknown, duplicate, and missing traits
        (
            format!(r#"{{"traits":[{one},{{"id":"style","pass":true,"reason":"ok"}}]}}"#),
            "traits[1].id",
            "unknown trait \"style\"",
        ),
        (
            format!(r#"{{"traits":[{one},{one}]}}"#),
            "traits[1].id",
            "duplicate verdict",
        ),
        (
            format!(r#"{{"traits":[{one}]}}"#),
            "traits",
            "missing verdict for trait \"tests\"",
        ),
    ];
    for (source, field, message) in cases {
        let error = parse_judgement(&source, &criteria).expect_err("invalid verdicts are rejected");
        assert_eq!(error.field, field, "{source}");
        assert!(
            error.message.contains(message),
            "{source}: {}",
            error.message
        );
        assert!(error.to_string().contains(field));
    }
}

struct FakeJudge;

#[async_trait::async_trait]
impl Judge for FakeJudge {
    async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError> {
        let pass = request.sample.arm_id == "baseline";
        let judgements = request
            .criteria
            .iter()
            .map(|criterion| {
                (
                    criterion.id.clone(),
                    Judgement {
                        pass,
                        reason: format!(
                            "{} judge for {} run {} inspected the captured patch.diff",
                            criterion.id, request.sample.arm_id, request.sample.run_index
                        ),
                    },
                )
            })
            .collect();
        Ok(JudgeResult { judgements })
    }
}

#[tokio::test]
async fn judge_runner_judges_captured_samples_and_records_template_provenance() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-judge", fixture_source()).await;
    let store = EvalStore::for_workspace(temporary.path());
    let mut loaded = store.load(&data.id).expect("stored eval loads");
    let judged = judge_eval(
        &store,
        &mut loaded,
        &FakeJudge,
        JudgeOptions::default().with_template("eval-judge"),
    )
    .await
    .expect("judge run succeeds");

    assert_eq!(judged.template, "eval-judge");
    assert_eq!(judged.judged.len(), 4);
    assert_eq!(judged.skipped.len(), 2);
    assert!(loaded.samples.iter().all(|sample| {
        sample.status != SampleStatus::Succeeded
            || sample.judge_template.as_deref() == Some("eval-judge")
    }));
    let report = loaded.report.as_ref().expect("judged report exists");
    let comparison = report.comparison.as_ref().expect("X/Y comparison exists");
    assert_eq!(comparison.arm_x, "baseline");
    assert_eq!(comparison.arm_y, "candidate");
    let correctness = comparison
        .traits
        .iter()
        .find(|trait_summary| trait_summary.id == "correctness")
        .expect("correctness trait");
    assert_eq!(correctness.scored_pairs, 2);
    assert_eq!(correctness.x_mean, Some(1.0));
    assert_eq!(correctness.y_mean, Some(0.0));
    assert_eq!(correctness.paired_difference, Some(-1.0));
    assert!(!correctness.outliers.is_empty());

    let markdown = render_markdown(report);
    assert!(markdown.contains("## X vs Y"));
    assert!(markdown.contains("correctness judge for baseline run 0"));
    assert!(markdown.contains("correctness judge for candidate run 0"));

    let stored = store.load(&loaded.id).expect("judged eval reloads");
    assert_eq!(stored.report.as_ref(), Some(report));
}

#[tokio::test]
async fn judge_report_markdown_carries_per_trait_reasoning() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-reasons", fixture_source()).await;
    let store = EvalStore::for_workspace(temporary.path());
    let mut loaded = store.load(&data.id).expect("stored eval loads");
    judge_eval(&store, &mut loaded, &FakeJudge, JudgeOptions::default())
        .await
        .expect("judge run succeeds");
    let report = loaded.report.as_ref().expect("judged report exists");
    let markdown = render_markdown(report);
    assert!(markdown.contains("### Outlier reasoning"));
    assert!(markdown.contains("inspected the captured patch.diff"));
    assert!(markdown.contains("  - X: "));
    assert!(markdown.contains("  - Y: "));
    for trait_summary in &report.matrix.traits {
        assert!(markdown.contains(&format!("| {} |", trait_summary.id)));
    }
}

#[test]
fn eval_judge_template_is_a_valid_builtin_graph() {
    let template = resolve_judge_template("eval-judge").expect("built-in template parses");
    assert_eq!(template.name, "eval-judge");
    assert_eq!(template.graph.root.name().as_str(), "run");
    assert!(template.source.contains("policy.native-v2@1"));
    assert_eq!(
        template.source,
        crate::native_v2_cli::EVAL_JUDGE_GRAPH_JSON,
        "the eval judge is the shipped `eval-judge` template"
    );
    let openengine_cluster_protocol::PayloadType::Record { fields } = &template.graph.initial_input
    else {
        panic!("judge input is a record");
    };
    assert_eq!(
        fields
            .keys()
            .map(|field| field.as_str())
            .collect::<Vec<_>>(),
        ["patch", "task", "traits", "verdicts"]
    );
    assert!(
        !template.source.contains("model"),
        "the template names no model"
    );
}

#[tokio::test]
async fn eval_judge_is_a_builtin_template_that_passes_validate_only() {
    let shown = crate::native_v2_cli::BuiltinGraphTemplate::parse("eval-judge")
        .expect("eval-judge is in the built-in catalog")
        .materialize(crate::native_v2_cli::TemplateDelivery::None)
        .expect("eval-judge materializes");
    let template = resolve_judge_template("eval-judge").expect("built-in template");
    assert_eq!(shown, template.graph);
    let workspace = tempfile::tempdir().expect("workspace");
    let runtime = workspace.path().join("runtime.json");
    std::fs::write(
        &runtime,
        r#"{"harness":"claude","provider":"anthropic","model":"any-model"}"#,
    )
    .expect("runtime");
    let input = workspace.path().join("input.json");
    std::fs::write(
        &input,
        r#"{"task":"t","traits":"[]","patch":"","verdicts":[]}"#,
    )
    .expect("input");
    let command = crate::native_v2_cli::parse_native_v2_args(
        [
            "run",
            "--title",
            "judge",
            "--template",
            "eval-judge",
            "--uniform-runtime-config",
            runtime.to_str().expect("utf-8"),
            "--input",
            input.to_str().expect("utf-8"),
            "--validate-only",
        ]
        .map(std::ffi::OsString::from)
        .to_vec(),
    )
    .expect("`zeroshot run --template eval-judge --validate-only` parses");
    let mut output = Vec::new();
    let outcome = crate::native_v2_cli::try_execute_native_v2_preflight(&command, &mut output)
        .await
        .expect("eval-judge validates");
    assert!(outcome.is_some());
    assert_eq!(output, b"{\"valid\":true}\n");
}

#[tokio::test]
async fn cli_judge_action_records_the_synthetic_judge_and_comparison() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-cli-judge", fixture_source()).await;
    let report = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Judge(EvalJudgeCommand {
            id: data.id.to_string(),
            judge_runner: Some("synthetic".to_owned()),
            judge_template: Some("eval-judge".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect("judge action succeeds");
    assert_eq!(report["comparison"]["armX"], "baseline");
    assert_eq!(report["comparison"]["armY"], "candidate");
    assert_eq!(report["comparison"]["traits"][0]["scoredPairs"], 2);
    let markdown = std::fs::read_to_string(
        temporary
            .path()
            .join(".zeroshot/eval/fixture-cli-judge/report.md"),
    )
    .expect("judged markdown is stored");
    assert!(markdown.contains("| synthetic |"));
    assert!(markdown.contains("### Outlier reasoning"));
}

#[test]
fn custom_judge_template_loads_from_a_graph_json_path() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let path = temporary.path().join("custom-judge.json");
    std::fs::write(&path, JUDGE_TEMPLATE_JSON).expect("custom graph is writable");
    let template = resolve_judge_template(path.to_str().expect("utf-8 path"))
        .expect("custom graph template parses");
    assert_eq!(template.name, path.to_string_lossy());
    assert_eq!(template.graph.root.name().as_str(), "run");
}

#[tokio::test]
async fn manifest_judge_defaults_judge_a_plain_run() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-default-judge", fixture_source()).await;
    let report = data
        .report
        .as_ref()
        .expect("plain run writes a judged report");
    assert!(report.comparison.is_some());
    assert!(report.matrix.rows.iter().any(|row| {
        row.status == SampleStatus::Succeeded && row.judge_executor.as_deref() == Some("synthetic")
    }));
}

#[derive(Clone)]
struct RecordingExecutor {
    inputs: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

#[async_trait::async_trait]
impl JudgeGraphExecutor for RecordingExecutor {
    async fn execute(
        &self,
        _template: &JudgeTemplate,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, EvalError> {
        self.inputs.lock().unwrap().push(input.clone());
        let traits: Vec<serde_json::Value> =
            serde_json::from_str(input["traits"].as_str().expect("traits")).expect("JSON");
        Ok(json!({
            "traits": traits
                .iter()
                .map(|item| json!({
                    "id": item["id"],
                    "pass": item["id"] == "correctness",
                    "reason": format!("The captured patch handles {}.", item["id"]),
                }))
                .collect::<Vec<_>>(),
        }))
    }
}

#[tokio::test]
async fn template_judge_executes_the_graph_once_per_sample_with_every_trait() {
    let executor = RecordingExecutor {
        inputs: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
    };
    let template = resolve_judge_template("eval-judge").expect("built-in template");
    let judge = TemplateJudge::new(executor.clone(), template);
    let sample = EvalSample {
        id: "sample-1".to_owned(),
        run_id: "run-1".to_owned(),
        arm_id: "baseline".to_owned(),
        run_index: 0,
        status: SampleStatus::Succeeded,
        timing: SampleTiming::default(),
        tokens: TokenUsage::default(),
        cost_usd: None,
        delivered_revision: None,
        failure_reason: None,
        patch: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        judge_template: None,
        judge_checkout: None,
        judge_executor: None,
        notes: Vec::new(),
    };
    let criteria = vec![criterion("correctness"), criterion("efficiency")];
    let result = judge
        .judge(JudgeRequest::new(sample, criteria, "patch body").with_task("Do the task."))
        .await
        .expect("template judge succeeds");
    assert_eq!(result.judgements.len(), 2);
    let inputs = executor.inputs.lock().unwrap();
    assert_eq!(inputs.len(), 1, "one judge run per sample");
    assert_eq!(inputs[0]["patch"], "patch body");
    assert_eq!(inputs[0]["task"], "Do the task.");
    assert!(
        inputs[0].get("model").is_none(),
        "no model is passed to the judge"
    );
    let traits: serde_json::Value =
        serde_json::from_str(inputs[0]["traits"].as_str().expect("traits")).expect("JSON");
    assert_eq!(traits[0]["id"], "correctness");
    assert_eq!(traits[1]["id"], "efficiency");
    assert_eq!(traits[1]["description"], "efficiency description");
    assert!(result.judgements["correctness"].pass);
    assert!(!result.judgements["efficiency"].pass);
    assert_eq!(
        result.judgements["correctness"].reason,
        "The captured patch handles \"correctness\"."
    );
}

#[test]
fn captured_sample_reads_the_flattened_slice_two_artifact() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let outcome = crate::native_v2_eval::SampleOutcome {
        run_id: openengine_cluster_protocol::RunId::new("run-capture"),
        status: crate::native_v2_eval::SampleStatus::Succeeded,
        timings: crate::native_v2_eval::SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 1,
        },
        token_usage: None,
        delivered_revision: Some("a".repeat(40)),
        delivered_branch: Some("zeroshot/run".to_owned()),
        delivery: None,
        failure_reason: None,
        output: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        patch: None,
    };
    let artifact = crate::native_v2_eval::SampleArtifact::new("sample-capture", outcome)
        .with_capture(crate::native_v2_eval::CapturedSample {
            base_revision: "b".repeat(40),
            delivered_revision: "a".repeat(40),
            delivered_branch: Some("zeroshot/run".to_owned()),
            patch: std::path::PathBuf::from("patch.diff"),
            patch_bytes: 11,
            mode: Some("push".to_owned()),
            outcome: Some("pushed".to_owned()),
            judge: None,
        });
    artifact
        .write(temporary.path())
        .expect("sample.json is written");
    std::fs::write(temporary.path().join("patch.diff"), "diff body\n").expect("patch is written");
    let captured = captured_sample_from_directory(temporary.path(), "baseline", 0)
        .expect("flattened capture parses");
    assert_eq!(captured.sample.id, "sample-capture");
    assert_eq!(captured.sample.run_id, "run-capture");
    assert_eq!(captured.sample.arm_id, "baseline");
    assert_eq!(captured.patch.as_deref(), Some("diff body\n"));
    assert_eq!(captured.sample_json["status"], "succeeded");
}

fn write_capture(directory: &std::path::Path, sample_id: &str, run_id: &str) {
    std::fs::create_dir_all(directory).expect("capture directory");
    let outcome = crate::native_v2_eval::SampleOutcome {
        run_id: openengine_cluster_protocol::RunId::new(run_id),
        status: crate::native_v2_eval::SampleStatus::Succeeded,
        timings: crate::native_v2_eval::SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 1,
        },
        token_usage: None,
        delivered_revision: None,
        delivered_branch: None,
        delivery: None,
        failure_reason: None,
        output: None,
        criteria: std::collections::BTreeMap::from([
            (
                "correctness".to_owned(),
                CriterionResult {
                    passed: true,
                    score: Some(5.0),
                    reasoning: Some("captured correctness evidence".to_owned()),
                    errors: Vec::new(),
                },
            ),
            (
                "efficiency".to_owned(),
                CriterionResult {
                    passed: true,
                    score: Some(5.0),
                    reasoning: Some("captured efficiency evidence".to_owned()),
                    errors: Vec::new(),
                },
            ),
        ]),
        reasoning: Default::default(),
        patch: None,
    };
    crate::native_v2_eval::SampleArtifact::new(sample_id, outcome)
        .write(directory)
        .expect("sample.json is written");
    std::fs::write(directory.join("patch.diff"), "diff --git a/file b/file\n")
        .expect("patch is written");
}

#[tokio::test]
async fn judge_captures_command_imports_slice_two_directories() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let manifest = write_fixture(temporary.path(), fixture_source());
    std::fs::write(
        temporary.path().join("custom-judge.json"),
        JUDGE_TEMPLATE_JSON,
    )
    .expect("custom template is writable");
    let captures = temporary.path().join("captures");
    for arm in ["baseline", "candidate"] {
        for run in 0..2 {
            write_capture(
                &captures.join(arm).join(format!("run-{run}")),
                &format!("{arm}-{run:04}"),
                &format!("run-{arm}-{run}"),
            );
        }
    }
    let report = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest,
            captures,
            id: Some("captured-judged".to_owned()),
            judge_runner: None,
            judge_template: Some("custom-judge.json".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect("capture judge command succeeds");
    assert_eq!(report["comparison"]["armX"], "baseline");
    assert_eq!(report["comparison"]["armY"], "candidate");
    let store = EvalStore::for_workspace(temporary.path());
    let loaded = store
        .load(&EvalId::new("captured-judged").unwrap())
        .expect("captured eval loads");
    assert_eq!(loaded.samples.len(), 4);
    let resolved = temporary.path().join("custom-judge.json");
    assert_eq!(
        loaded.manifest.options.judge_template.as_deref(),
        Some(resolved.to_string_lossy().as_ref())
    );
    assert!(loaded.samples.iter().all(|sample| {
        sample.judge_executor.as_deref() == Some("synthetic")
            && sample.patch.is_some()
            && sample.judge_template.as_deref() == Some(resolved.to_string_lossy().as_ref())
    }));
}

#[tokio::test]
async fn judge_captures_fail_closed_without_recorded_evidence_or_runtime() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let manifest = write_fixture(temporary.path(), fixture_source());
    let captures = temporary.path().join("captures");
    write_checkout_capture(&captures.join("baseline").join("run-0"), "baseline-0000");
    let error = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest,
            captures,
            id: Some("no-evidence".to_owned()),
            judge_runner: Some("synthetic".to_owned()),
            judge_template: Some("eval-judge".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect_err("capture without criteria must not be fabricated");
    assert!(error.to_string().contains("no recorded evidence"));
}

#[tokio::test]
async fn eval_judge_template_passes_production_admission() {
    use openengine_cluster_protocol::{
        IdempotencyKey, ModelId, NodeName, RunSize, RunTitle, SessionScope,
    };
    use crate::native_v2_admission::{DeliveryPolicy, NativeV2Admission};
    use crate::native_v2_contract::{NodeRuntimeBinding, RunSubmissionIntent};

    let template = resolve_judge_template("eval-judge").expect("built-in template");
    let node = NodeName::new("judge").expect("judge node");
    let mut nodes = std::collections::BTreeMap::new();
    nodes.insert(
        node,
        NodeRuntimeBinding::Agent {
            lane: None,
            model: ModelId::new("judge-model").expect("model"),
            effort: None,
            session_scope: SessionScope::Execution,
            connections: Default::default(),
        },
    );
    let runtime = crate::native_v2_candidate::test_support::codex_runtime(RunSize::Small, nodes);
    let initial_input = json!({
        "task": "Fix the parser.",
        "traits": "[{\"id\":\"correctness\",\"description\":\"The change satisfies the request.\"}]",
        "patch": "diff --git a/file b/file",
        "verdicts": []
    });
    NativeV2Admission
        .validate_intent(
            &RunSubmissionIntent {
                title: RunTitle::new("Eval judge").expect("title"),
                graph: template.graph,
                initial_input,
                runtime,
                environment: None,
                branch: None,
                submission_key: IdempotencyKey::new("eval-judge-admission").expect("key"),
            },
            DeliveryPolicy::Optional,
        )
        .await
        .expect("judge graph admits through the production verifier");
}

fn write_failed_capture(directory: &std::path::Path, sample_id: &str, run_id: &str) {
    std::fs::create_dir_all(directory).expect("capture directory");
    let outcome = crate::native_v2_eval::SampleOutcome {
        run_id: openengine_cluster_protocol::RunId::new(run_id),
        status: crate::native_v2_eval::SampleStatus::Failed,
        timings: crate::native_v2_eval::SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 1,
        },
        token_usage: None,
        delivered_revision: None,
        delivered_branch: None,
        delivery: None,
        failure_reason: Some("worker_failed".to_owned()),
        output: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        patch: None,
    };
    crate::native_v2_eval::SampleArtifact::new(sample_id, outcome)
        .write(directory)
        .expect("sample.json is written");
}

#[tokio::test]
async fn judge_captures_keeps_failed_captures_without_a_patch_reproducibly() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let manifest = write_fixture(temporary.path(), fixture_source());
    let captures = temporary.path().join("captures");
    write_capture(
        &captures.join("baseline").join("run-0"),
        "baseline-0000",
        "run-baseline-0",
    );
    write_failed_capture(
        &captures.join("baseline").join("run-1"),
        "baseline-0001",
        "run-baseline-1",
    );
    write_capture(
        &captures.join("candidate").join("run-0"),
        "candidate-0000",
        "run-candidate-0",
    );
    let report = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest,
            captures,
            id: Some("captured-with-failure".to_owned()),
            judge_runner: None,
            judge_template: Some("eval-judge".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect("capture judge command tolerates a failed sample");
    assert_eq!(report["matrix"]["rows"].as_array().unwrap().len(), 3);
    let store = EvalStore::for_workspace(temporary.path());
    let id = EvalId::new("captured-with-failure").unwrap();
    let loaded = store.load(&id).expect("captured eval loads");
    assert_eq!(loaded.samples.len(), 3);
    assert_eq!(
        loaded
            .samples
            .iter()
            .filter(|sample| sample.patch.is_none())
            .count(),
        1
    );
    let rescored = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Score {
            weights: Default::default(),
            id: id.to_string(),
            scorer: "none".to_owned(),
        },
    })
    .await
    .expect("rescore succeeds");
    assert_eq!(rescored["matrix"]["rows"].as_array().unwrap().len(), 3);
}

fn write_checkout_capture(directory: &std::path::Path, sample_id: &str) -> String {
    std::fs::create_dir_all(directory.join("judge")).expect("checkout directory");
    std::fs::write(directory.join("judge").join("file.txt"), "delivered file\n")
        .expect("checkout file");
    let outcome = crate::native_v2_eval::SampleOutcome {
        run_id: openengine_cluster_protocol::RunId::new("run-checkout"),
        status: crate::native_v2_eval::SampleStatus::Succeeded,
        timings: crate::native_v2_eval::SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 1,
        },
        token_usage: None,
        delivered_revision: None,
        delivered_branch: None,
        delivery: None,
        failure_reason: None,
        output: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        patch: None,
    };
    let captured = crate::native_v2_eval::CapturedSample {
        base_revision: "a".repeat(40),
        delivered_revision: "b".repeat(40),
        delivered_branch: None,
        patch: std::path::PathBuf::from("captured.patch"),
        patch_bytes: 9,
        mode: None,
        outcome: None,
        judge: Some(crate::native_v2_eval::JudgeCheckout {
            directory: std::path::PathBuf::from("judge"),
            revision: "b".repeat(40),
            base_revision: "a".repeat(40),
            patch: std::path::PathBuf::from("captured.patch"),
        }),
    };
    crate::native_v2_eval::SampleArtifact::new(sample_id, outcome)
        .with_capture(captured)
        .write(directory)
        .expect("sample.json is written");
    std::fs::write(directory.join("captured.patch"), "captured diff\n").expect("captured patch");
    directory.join("judge").to_string_lossy().into_owned()
}

#[tokio::test]
async fn judge_uses_the_captured_patch_and_read_only_checkout() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    write_checkout_capture(temporary.path(), "baseline-0000");
    let captured =
        captured_sample_from_directory(temporary.path(), "baseline", 0).expect("capture parses");
    assert_eq!(captured.patch.as_deref(), Some("captured diff\n"));
    let checkout = captured.checkout.expect("checkout recorded");
    let mut sample = captured.sample;
    sample.judge_checkout = Some(checkout.clone());
    let criteria = vec![criterion("correctness")];

    struct CheckoutJudge;
    #[async_trait::async_trait]
    impl Judge for CheckoutJudge {
        async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError> {
            let checkout = request.checkout.expect("checkout reaches the judge");
            let file = std::fs::read_to_string(std::path::Path::new(&checkout).join("file.txt"))
                .expect("checkout file is readable");
            Ok(JudgeResult {
                judgements: std::collections::BTreeMap::from([(
                    "correctness".to_owned(),
                    Judgement {
                        pass: true,
                        reason: format!("The checked-out file contains {file:?}."),
                    },
                )]),
            })
        }
    }
    let result = CheckoutJudge
        .judge(JudgeRequest::new(sample, criteria, captured.patch.unwrap()).with_checkout(checkout))
        .await
        .expect("judge cites checkout");
    assert!(
        result.judgements["correctness"]
            .reason
            .contains("delivered file")
    );
}

#[test]
fn captured_sample_ids_cannot_escape_the_store() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    write_capture(
        &temporary.path().join("sample"),
        "../../escape",
        "run-escape",
    );
    let error = captured_sample_from_directory(&temporary.path().join("sample"), "baseline", 0)
        .expect_err("traversal id is rejected");
    assert!(error.to_string().contains("sample id"));
}

#[tokio::test]
async fn judge_capture_pairing_uses_the_sample_id_run_suffix() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let manifest = write_fixture(temporary.path(), fixture_source());
    let captures = temporary.path().join("captures");
    write_capture(
        &captures.join("baseline").join("arbitrary-directory"),
        "baseline-0007",
        "run-baseline-7",
    );
    write_capture(
        &captures.join("candidate").join("another-directory"),
        "candidate-0007",
        "run-candidate-7",
    );
    execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest,
            captures,
            id: Some("paired-by-id".to_owned()),
            judge_runner: None,
            judge_template: Some("eval-judge".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect("capture judge command succeeds");
    let loaded = EvalStore::for_workspace(temporary.path())
        .load(&EvalId::new("paired-by-id").unwrap())
        .expect("captured eval loads");
    assert!(loaded.samples.iter().all(|sample| sample.run_index == 7));
    assert_eq!(
        loaded.report.unwrap().comparison.unwrap().traits[0].scored_pairs,
        1
    );
}

#[tokio::test]
async fn report_omits_xy_when_no_scores_exist_and_renders_judge_errors() {
    struct ErrorJudge;
    #[async_trait::async_trait]
    impl Judge for ErrorJudge {
        async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError> {
            let judgements = request
                .criteria
                .iter()
                .map(|criterion| {
                    (
                        criterion.id.clone(),
                        Judgement {
                            pass: false,
                            reason: "The patch misses the requested guard in parser.rs.".to_owned(),
                        },
                    )
                })
                .collect();
            Ok(JudgeResult { judgements })
        }
    }

    let temporary = tempfile::tempdir().expect("temporary workspace");
    let source = fixture_source().replace("judgeTemplate = \"eval-judge\"\n", "");
    let data = run_fixture_in(temporary.path(), "fixture-no-scores", &source).await;
    let store = EvalStore::for_workspace(temporary.path());
    let mut unscored = store.load(&data.id).expect("stored eval loads");
    for sample in &mut unscored.samples {
        for result in sample.criteria.values_mut() {
            result.score = None;
        }
        store
            .write_sample(&unscored.id, sample)
            .expect("unscored sample writes");
    }
    let report = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Score {
            weights: Default::default(),
            id: unscored.id.to_string(),
            scorer: "none".to_owned(),
        },
    })
    .await
    .expect("unscored report rebuilds");
    assert!(report["comparison"].is_null());
    let unscored = store.load(&unscored.id).expect("unscored eval reloads");
    assert!(!render_markdown(unscored.report.as_ref().unwrap()).contains("## X vs Y"));

    let mut loaded = store.load(&data.id).expect("stored eval loads");
    judge_eval(&store, &mut loaded, &ErrorJudge, JudgeOptions::default())
        .await
        .expect("error judge runs");
    let markdown = render_markdown(loaded.report.as_ref().expect("judged report"));
    assert!(markdown.contains("The patch misses the requested guard in parser.rs."));
}

#[tokio::test]
async fn relative_judge_runtime_path_is_persisted_against_the_manifest_directory() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let source = fixture_source()
        .replace("judgeTemplate = \"eval-judge\"\n", "")
        .replace(
            "seed = 7",
            "seed = 7\njudgeRuntimeConfig = \"judge-runtime.json\"",
        );
    let data = run_fixture_in(temporary.path(), "fixture-runtime-path", &source).await;
    let loaded = EvalStore::for_workspace(temporary.path())
        .load(&data.id)
        .expect("stored eval loads");
    let configured = loaded
        .manifest
        .options
        .judge_runtime_config
        .expect("runtime path persisted");
    assert!(std::path::Path::new(&configured).is_absolute());
    assert!(configured.ends_with("judge-runtime.json"));
}

#[tokio::test]
async fn duplicate_capture_ids_are_rejected() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let manifest = write_fixture(temporary.path(), fixture_source());
    let captures = temporary.path().join("captures");
    write_capture(
        &captures.join("baseline").join("one"),
        "baseline-0000",
        "run-one",
    );
    write_capture(
        &captures.join("candidate").join("two"),
        "baseline-0000",
        "run-two",
    );
    let error = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest,
            captures,
            id: Some("duplicate-ids".to_owned()),
            judge_runner: None,
            judge_template: Some("eval-judge".to_owned()),
            scorer: None,
        }),
    })
    .await
    .expect_err("duplicate ids are rejected");
    assert!(error.to_string().contains("duplicate captured sample id"));
}

#[test]
fn captured_artifact_paths_must_stay_inside_the_capture_directory() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let sample = temporary.path().join("sample");
    std::fs::create_dir_all(&sample).expect("sample directory");
    let outcome = crate::native_v2_eval::SampleOutcome {
        run_id: openengine_cluster_protocol::RunId::new("run-escape"),
        status: crate::native_v2_eval::SampleStatus::Succeeded,
        timings: crate::native_v2_eval::SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 1,
        },
        token_usage: None,
        delivered_revision: None,
        delivered_branch: None,
        delivery: None,
        failure_reason: None,
        output: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        patch: None,
    };
    for (patch, judge_directory) in [
        (
            std::path::PathBuf::from("../outside.patch"),
            std::path::PathBuf::from("judge"),
        ),
        (
            std::path::PathBuf::from("patch.diff"),
            std::path::PathBuf::from("../outside-judge"),
        ),
    ] {
        let captured = crate::native_v2_eval::CapturedSample {
            base_revision: "a".repeat(40),
            delivered_revision: "b".repeat(40),
            delivered_branch: None,
            patch,
            patch_bytes: 9,
            mode: None,
            outcome: None,
            judge: Some(crate::native_v2_eval::JudgeCheckout {
                directory: judge_directory,
                revision: "b".repeat(40),
                base_revision: "a".repeat(40),
                patch: std::path::PathBuf::from("patch.diff"),
            }),
        };
        crate::native_v2_eval::SampleArtifact::new("baseline-0000", outcome.clone())
            .with_capture(captured)
            .write(&sample)
            .expect("sample.json is written");
        let error = captured_sample_from_directory(&sample, "baseline", 0)
            .expect_err("escaping capture path is rejected");
        assert!(error.to_string().contains("capture directory"));
    }
}

#[tokio::test]
async fn runner_artifact_checkout_round_trips_into_the_judge() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let store = EvalStore::for_workspace(temporary.path());
    let id = EvalId::new("runner-checkout").expect("eval id");
    store
        .create(fixture_source(), &id, 1)
        .expect("store creates");
    let sample_directory = temporary.path().join("samples/baseline-0000");
    std::fs::create_dir_all(sample_directory.join("judge")).expect("checkout directory");
    std::fs::write(sample_directory.join("judge/file.txt"), "runner checkout\n")
        .expect("checkout file");
    std::fs::write(sample_directory.join("captured.patch"), "captured diff\n")
        .expect("captured patch file");
    let outcome = crate::native_v2_eval::SampleOutcome {
        run_id: openengine_cluster_protocol::RunId::new("run-checkout"),
        status: crate::native_v2_eval::SampleStatus::Succeeded,
        timings: crate::native_v2_eval::SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 1,
        },
        token_usage: None,
        delivered_revision: None,
        delivered_branch: None,
        delivery: None,
        failure_reason: None,
        output: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        patch: None,
    };
    let artifact = crate::native_v2_eval::SampleArtifact::new("baseline-0000", outcome)
        .with_capture(crate::native_v2_eval::CapturedSample {
            base_revision: "a".repeat(40),
            delivered_revision: "b".repeat(40),
            delivered_branch: None,
            patch: std::path::PathBuf::from("captured.patch"),
            patch_bytes: 14,
            mode: None,
            outcome: None,
            judge: Some(crate::native_v2_eval::JudgeCheckout {
                directory: std::path::PathBuf::from("judge"),
                revision: "b".repeat(40),
                base_revision: "a".repeat(40),
                patch: std::path::PathBuf::from("patch.diff"),
            }),
        });
    let meta = crate::native_v2_eval::cli::SamplePlanMeta {
        sample_id: "baseline-0000".to_owned(),
        arm_id: "baseline".to_owned(),
        run_index: 0,
        directory: sample_directory,
    };
    let sample = crate::native_v2_eval::cli::sample_from_artifact(&store, &id, artifact, &meta)
        .expect("artifact converts");
    assert!(sample.judge_checkout.is_some());
    assert!(sample.patch.is_some());

    struct CheckoutJudge;
    #[async_trait::async_trait]
    impl Judge for CheckoutJudge {
        async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError> {
            let checkout = request.checkout.expect("checkout reaches judge");
            let file = std::fs::read_to_string(std::path::Path::new(&checkout).join("file.txt"))
                .expect("checkout file");
            Ok(JudgeResult {
                judgements: request
                    .criteria
                    .iter()
                    .map(|criterion| {
                        (
                            criterion.id.clone(),
                            Judgement {
                                pass: true,
                                reason: format!("Runner checkout contains {file:?}."),
                            },
                        )
                    })
                    .collect(),
            })
        }
    }
    let manifest = EvalManifest::parse(fixture_source()).expect("manifest");
    let mut data = EvalData {
        id,
        created_at_ms: 1,
        manifest,
        samples: vec![sample],
        report: None,
    };
    judge_eval(&store, &mut data, &CheckoutJudge, JudgeOptions::default())
        .await
        .expect("judge runs with runner checkout");
    assert!(data.samples[0].judge_checkout.is_some());
    assert!(
        data.samples[0].criteria["correctness"]
            .reasoning
            .as_deref()
            .unwrap()
            .contains("runner checkout")
    );
}

#[tokio::test]
async fn eval_judge_input_admits_after_filtering_to_the_declared_fields() {
    use openengine_cluster_protocol::{
        IdempotencyKey, ModelId, NodeName, RunSize, RunTitle, SessionScope,
    };
    use crate::native_v2_admission::{DeliveryPolicy, NativeV2Admission};
    use crate::native_v2_contract::{NodeRuntimeBinding, RunSubmissionIntent};

    let template = resolve_judge_template("eval-judge").expect("built-in judge template");
    let graph = template.graph.clone();
    let input = judge_input(
        &JudgeRequest::new(
            crate::native_v2_eval::EvalSample {
                id: "baseline-0000".to_owned(),
                run_id: "run".to_owned(),
                arm_id: "baseline".to_owned(),
                run_index: 0,
                status: SampleStatus::Succeeded,
                timing: SampleTiming::default(),
                tokens: TokenUsage::default(),
                cost_usd: None,
                delivered_revision: None,
                failure_reason: None,
                patch: None,
                criteria: Default::default(),
                reasoning: Default::default(),
                judge_template: None,
                judge_checkout: None,
                judge_executor: None,
                notes: Vec::new(),
            },
            vec![criterion("correctness")],
            "patch",
        )
        .with_task("Fix the parser"),
    );
    // The runner offers sample and checkout; eval-judge declares neither, so they are dropped.
    assert!(input.get("sample").is_some());
    let filtered = judge_input_for_template(&template, input).expect("filtered input");
    assert!(filtered.get("sample").is_none());
    assert!(filtered.get("checkout").is_none());
    assert_eq!(filtered["task"], "Fix the parser");
    assert_eq!(filtered["verdicts"], json!([]));
    let traits: serde_json::Value =
        serde_json::from_str(filtered["traits"].as_str().expect("traits string")).expect("JSON");
    assert_eq!(traits[0]["id"], "correctness");

    let node = NodeName::new("judge").expect("judge node");
    let mut nodes = std::collections::BTreeMap::new();
    nodes.insert(
        node,
        NodeRuntimeBinding::Agent {
            lane: None,
            model: ModelId::new("judge-model").expect("model"),
            effort: None,
            session_scope: SessionScope::Execution,
            connections: Default::default(),
        },
    );
    let runtime = crate::native_v2_candidate::test_support::codex_runtime(RunSize::Small, nodes);
    NativeV2Admission
        .validate_intent(
            &RunSubmissionIntent {
                title: RunTitle::new("Custom eval judge").expect("title"),
                graph,
                initial_input: filtered,
                runtime,
                environment: None,
                branch: None,
                submission_key: IdempotencyKey::new("custom-eval-judge").expect("key"),
            },
            DeliveryPolicy::Optional,
        )
        .await
        .expect("user-shaped custom graph admits");
}

#[test]
fn captured_skipped_sample_is_retained_as_skipped() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let outcome = crate::native_v2_eval::SampleOutcome {
        run_id: openengine_cluster_protocol::RunId::new("run-skipped"),
        status: crate::native_v2_eval::SampleStatus::Skipped,
        timings: crate::native_v2_eval::SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 1,
        },
        token_usage: None,
        delivered_revision: None,
        delivered_branch: None,
        delivery: None,
        failure_reason: Some("skipped by runner".to_owned()),
        output: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        patch: None,
    };
    crate::native_v2_eval::SampleArtifact::new("baseline-0000", outcome)
        .write(temporary.path())
        .expect("sample.json is written");
    let captured = captured_sample_from_directory(temporary.path(), "baseline", 0)
        .expect("skipped capture parses");
    assert_eq!(captured.sample.status, SampleStatus::Skipped);
    assert!(captured.patch.is_none());
}

#[tokio::test]
async fn judge_eval_errors_on_a_succeeded_sample_without_a_patch() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let store = EvalStore::for_workspace(temporary.path());
    let id = EvalId::new("missing-patch").expect("eval id");
    store
        .create(fixture_source(), &id, 1)
        .expect("store creates");
    let manifest = EvalManifest::parse(fixture_source()).expect("manifest");
    let sample = EvalSample {
        id: "baseline-0000".to_owned(),
        run_id: "run".to_owned(),
        arm_id: "baseline".to_owned(),
        run_index: 0,
        status: SampleStatus::Succeeded,
        timing: SampleTiming::default(),
        tokens: TokenUsage::default(),
        cost_usd: None,
        delivered_revision: None,
        failure_reason: None,
        patch: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        judge_template: None,
        judge_checkout: None,
        judge_executor: None,
        notes: Vec::new(),
    };
    let mut data = EvalData {
        id,
        created_at_ms: 1,
        manifest,
        samples: vec![sample],
        report: None,
    };
    let error = judge_eval(&store, &mut data, &FakeJudge, JudgeOptions::default())
        .await
        .expect_err("missing patch fails closed");
    assert!(error.to_string().contains("without a captured patch"));
}

#[tokio::test]
async fn judge_eval_rejects_an_in_process_judgement_outside_the_contract() {
    struct InvalidJudge;
    #[async_trait::async_trait]
    impl Judge for InvalidJudge {
        async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError> {
            Ok(JudgeResult {
                judgements: request
                    .criteria
                    .iter()
                    .map(|criterion| {
                        (
                            criterion.id.clone(),
                            Judgement {
                                pass: true,
                                reason: String::new(),
                            },
                        )
                    })
                    .collect(),
            })
        }
    }
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-invalid-judge", fixture_source()).await;
    let store = EvalStore::for_workspace(temporary.path());
    let mut loaded = store.load(&data.id).expect("stored eval loads");
    let error = judge_eval(&store, &mut loaded, &InvalidJudge, JudgeOptions::default())
        .await
        .expect_err("invalid judgement is rejected");
    assert!(error.to_string().contains("correctness"));
    assert!(error.to_string().contains("reason"));
}

#[test]
fn nested_outcome_capture_document_is_still_importable() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let document = json!({
        "sampleId": "baseline-0000",
        "outcome": {
            "runId": "run-nested",
            "status": "succeeded",
            "timings": {
                "submittedAtMs": 1,
                "startedAtMs": 2,
                "finishedAtMs": 3,
                "queueWaitMs": 1,
                "wallClockMs": 2,
                "concurrency": 1
            }
        }
    });
    std::fs::write(
        temporary.path().join("sample.json"),
        serde_json::to_vec(&document).unwrap(),
    )
    .expect("nested sample.json");
    std::fs::write(temporary.path().join("patch.diff"), "nested patch\n").expect("nested patch");
    let captured = captured_sample_from_directory(temporary.path(), "baseline", 0)
        .expect("nested capture imports");
    assert_eq!(captured.sample.id, "baseline-0000");
    assert_eq!(captured.sample.run_id, "run-nested");
    assert_eq!(captured.patch.as_deref(), Some("nested patch\n"));
}

#[tokio::test]
async fn judge_captured_samples_fails_closed_without_a_patch() {
    let captured = CapturedSampleInput {
        sample: EvalSample {
            id: "baseline-0000".to_owned(),
            run_id: "run".to_owned(),
            arm_id: "baseline".to_owned(),
            run_index: 0,
            status: SampleStatus::Succeeded,
            timing: SampleTiming::default(),
            tokens: TokenUsage::default(),
            cost_usd: None,
            delivered_revision: None,
            failure_reason: None,
            patch: None,
            criteria: Default::default(),
            reasoning: Default::default(),
            judge_template: None,
            judge_checkout: None,
            judge_executor: None,
            notes: Vec::new(),
        },
        patch: None,
        sample_json: serde_json::Value::Null,
        checkout: None,
    };
    let error = judge_captured_samples(
        &FakeJudge,
        &[criterion("correctness")],
        vec![captured],
        JudgeOptions::default(),
    )
    .await
    .expect_err("missing patch fails closed");
    assert!(error.to_string().contains("without a captured patch"));
}

#[tokio::test]
async fn judge_captures_rejects_missing_or_empty_capture_roots_without_creating_an_eval() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let manifest = write_fixture(temporary.path(), fixture_source());
    let store = EvalStore::for_workspace(temporary.path());
    let missing = temporary.path().join("missing-captures");
    let error = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest: manifest.clone(),
            captures: missing,
            id: Some("missing-root".to_owned()),
            judge_runner: None,
            judge_template: None,
            scorer: None,
        }),
    })
    .await
    .expect_err("missing capture root is rejected");
    assert!(error.to_string().contains("not a directory"));
    assert!(store.list().expect("store lists").is_empty());

    let empty = temporary.path().join("empty-captures");
    std::fs::create_dir_all(&empty).expect("empty capture root");
    let error = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest,
            captures: empty,
            id: Some("empty-root".to_owned()),
            judge_runner: None,
            judge_template: None,
            scorer: None,
        }),
    })
    .await
    .expect_err("empty capture root is rejected");
    assert!(error.to_string().contains("contains no sample.json"));
    assert!(store.list().expect("store lists").is_empty());
}

#[tokio::test]
async fn judge_captures_rolls_back_a_new_eval_when_a_later_judgement_is_invalid() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let manifest = write_fixture(temporary.path(), fixture_source());
    let captures = temporary.path().join("captures");
    write_capture(
        &captures.join("baseline").join("run-0"),
        "baseline-0000",
        "run-baseline-0",
    );
    let candidate = captures.join("candidate").join("run-0");
    write_capture(&candidate, "candidate-0000", "run-candidate-0");
    let sample_path = candidate.join("sample.json");
    let mut document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&sample_path).unwrap()).unwrap();
    document["criteria"]["correctness"]["reasoning"] = serde_json::json!("");
    std::fs::write(&sample_path, serde_json::to_vec(&document).unwrap()).expect("rewrite capture");

    let store = EvalStore::for_workspace(temporary.path());
    let error = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest,
            captures,
            id: Some("rollback".to_owned()),
            judge_runner: None,
            judge_template: None,
            scorer: None,
        }),
    })
    .await
    .expect_err("invalid later judgement fails the command");
    assert!(error.to_string().contains("reason"), "{error}");
    assert!(store.list().expect("store lists").is_empty());
}

fn talk_manifests() -> [(&'static str, &'static str); 2] {
    [
        (
            "eval1-ci-before-review",
            include_str!("../../fixtures/eval/talk/eval1-ci-before-review.toml"),
        ),
        (
            "eval2-delete-comments",
            include_str!("../../fixtures/eval/talk/eval2-delete-comments.toml"),
        ),
    ]
}

#[tokio::test]
async fn talk_evals_dry_run_produce_the_slide_table_and_exports() {
    for (name, source) in talk_manifests() {
        let temporary = tempfile::tempdir().expect("temporary workspace");
        let manifest = write_fixture(temporary.path(), source);
        let parsed = EvalManifest::parse(source).expect("talk manifest parses");
        assert_eq!(parsed.options.score.baseline.as_deref(), Some("baseline"));
        let value = execute(&EvalCommand {
            directory: temporary.path().to_path_buf(),
            action: EvalAction::Run(EvalRunCommand {
                manifest,
                directory: temporary.path().to_path_buf(),
                id: Some(format!("{name}-dry")),
                scorer: None,
                judge_template: None,
                judge_runner: None,
                runner: Some("fake".to_owned()),
                workspace: None,
                arms: Vec::new(),
            }),
        })
        .await
        .expect("dry run succeeds");
        let report: EvalReport = serde_json::from_value(value).expect("report json");
        let comparison = report.baseline.as_ref().expect("baseline comparison");
        assert_eq!(comparison.pairs.len(), 5, "{name}");
        assert_eq!(comparison.cost_unit, CostUnit::Usd, "{name}");
        assert_eq!(comparison.baseline.runs, 5);
        assert_eq!(comparison.candidate.runs, 5);
        let ratio = comparison.mean_ratio.expect("ratio is defined");
        assert!(
            (report.aggregate_score - ratio).abs() < 1e-12,
            "baseline-ratio scorer"
        );
        let means = |side: &ArmAggregate| {
            side.quality.expect("q")
                / (side.cost.expect("c").powf(0.5) * side.time_minutes.expect("t").powf(0.5))
        };
        assert!((ratio - means(&comparison.candidate) / means(&comparison.baseline)).abs() < 1e-12);
        let directory = temporary
            .path()
            .join(".zeroshot/eval")
            .join(format!("{name}-dry"));
        let table = std::fs::read_to_string(directory.join(TABLE_FILE)).expect("table.csv");
        // Header, five runs, then mean, median, ratio_mean and ratio_median rows.
        assert_eq!(table.lines().count(), 1 + 5 + 4, "{table}");
        let rows = table.lines().collect::<Vec<_>>();
        assert!(rows[6].starts_with("mean,") && rows[7].starts_with("median,"));
        let last = |row: &str| row.rsplit(',').next().unwrap().parse::<f64>().unwrap();
        assert!(rows[8].starts_with("ratio_mean,") && (last(rows[8]) - ratio).abs() < 1e-6);
        let median_ratio = comparison.median_ratio.expect("median ratio is defined");
        assert!(
            rows[9].starts_with("ratio_median,") && (last(rows[9]) - median_ratio).abs() < 1e-6
        );
        let runs = std::fs::read_to_string(directory.join(RUNS_FILE)).expect("runs.csv");
        assert_eq!(runs.lines().count(), 1 + 10, "{runs}");
        let stored: BaselineComparison =
            serde_json::from_slice(&std::fs::read(directory.join(SCORE_FILE)).expect("score.json"))
                .expect("score.json parses");
        assert_eq!(stored.pairs.len(), comparison.pairs.len());
        assert_eq!(stored.candidate_arm, comparison.candidate_arm);
        assert!((stored.mean_ratio.expect("stored ratio") - ratio).abs() < 1e-12);
        let markdown = std::fs::read_to_string(directory.join("report.md")).expect("report.md");
        assert!(markdown.contains("## Baseline vs candidate"));

        let exported = temporary.path().join("export");
        let value = execute(&EvalCommand {
            directory: temporary.path().to_path_buf(),
            action: EvalAction::Export {
                id: format!("{name}-dry"),
                output: Some(exported.clone()),
                weights: ScoreWeights {
                    a: Some(0.2),
                    b: Some(0.8),
                    pricing: Default::default(),
                },
            },
        })
        .await
        .expect("export succeeds");
        assert_eq!(value["a"], json!(0.2));
        let reweighted: BaselineComparison = serde_json::from_slice(
            &std::fs::read(exported.join(SCORE_FILE)).expect("exported score.json"),
        )
        .expect("exported score parses");
        let weighted = |side: &ArmAggregate| {
            side.quality.expect("q")
                / (side.cost.expect("c").powf(0.2) * side.time_minutes.expect("t").powf(0.8))
        };
        let expected = weighted(&reweighted.candidate) / weighted(&reweighted.baseline);
        assert!((reweighted.mean_ratio.expect("ratio") - expected).abs() < 1e-12);
        assert!(exported.join(TABLE_FILE).is_file());
        assert!(exported.join(RUNS_FILE).is_file());
        // The stored report keeps the manifest weights.
        let stored_again: BaselineComparison =
            serde_json::from_slice(&std::fs::read(directory.join(SCORE_FILE)).expect("score.json"))
                .expect("score.json parses");
        assert!((stored_again.a - 0.5).abs() < f64::EPSILON);
    }
}

/// A mocked judge graph run: answers in the `eval-judge` output shape, failing `efficiency` for
/// the candidate arm only.
struct VerdictGraph;

#[async_trait::async_trait]
impl JudgeGraphExecutor for VerdictGraph {
    async fn execute(
        &self,
        _template: &JudgeTemplate,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, EvalError> {
        let sample: serde_json::Value =
            serde_json::from_str(input["sample"].as_str().expect("sample")).expect("JSON");
        let traits: Vec<serde_json::Value> =
            serde_json::from_str(input["traits"].as_str().expect("traits")).expect("JSON");
        let candidate = sample["armId"] == "candidate";
        Ok(json!({
            "traits": traits
                .iter()
                .map(|item| json!({
                    "id": item["id"],
                    "pass": !(candidate && item["id"] == "efficiency"),
                    "reason": format!("{} verdict from patch", item["id"].as_str().unwrap_or("")),
                }))
                .collect::<Vec<_>>(),
        }))
    }
}

#[tokio::test]
async fn judged_trait_verdicts_drive_per_run_quality() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let store = EvalStore::for_workspace(temporary.path());
    let mut data = run_fixture_in(temporary.path(), "judged-quality", fixture_source()).await;
    let judge = TemplateJudge::new(
        VerdictGraph,
        resolve_judge_template("eval-judge").expect("template"),
    );
    judge_eval(&store, &mut data, &judge, JudgeOptions::default())
        .await
        .expect("mock judge runs");
    let report = data.report.as_ref().expect("report");
    let comparison = report.baseline.as_ref().expect("comparison");
    // failEvery = 2 fails run 1 on both arms: Q = 0 with measured T and C.
    let baseline_runs = comparison
        .pairs
        .iter()
        .map(|pair| pair.baseline.as_ref().expect("baseline run").quality)
        .collect::<Vec<_>>();
    assert_eq!(baseline_runs, vec![Some(1.0), Some(0.0), Some(1.0)]);
    let candidate = comparison.pairs[0]
        .candidate
        .as_ref()
        .expect("candidate run");
    // correctness (weight 2) passes, efficiency (weight 1) fails.
    assert!((candidate.quality.expect("q") - 2.0 / 3.0).abs() < 1e-12);
    assert_eq!((candidate.traits_passed, candidate.traits_judged), (1, 2));
    assert!((comparison.candidate.quality.expect("q") - 4.0 / 9.0).abs() < 1e-12);
    let sample = data
        .samples
        .iter()
        .find(|sample| sample.id == "candidate-0000")
        .expect("candidate sample");
    let verdict = sample
        .criteria
        .get("efficiency")
        .expect("per-trait verdict");
    assert!(!verdict.passed);
    assert_eq!(verdict.score, Some(0.0));
    assert_eq!(
        verdict.reasoning.as_deref(),
        Some("efficiency verdict from patch")
    );
    assert_eq!(sample.judge_template.as_deref(), Some("eval-judge"));
}

#[test]
fn captured_cost_and_cache_split_are_imported() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    std::fs::write(
        temporary.path().join("sample.json"),
        serde_json::to_vec(&json!({
            "sampleId": "baseline-0003",
            "runId": "run-cost",
            "status": "succeeded",
            "timings": { "startedAtMs": 1, "wallClockMs": 120000, "queueWaitMs": 0 },
            "tokenUsage": {
                "inputTokens": 10,
                "outputTokens": 20,
                "cacheReadInputTokens": 30,
                "cacheCreationInputTokens": 40
            },
            "costUsd": 1.25
        }))
        .expect("json"),
    )
    .expect("sample.json is written");
    let captured =
        captured_sample_from_directory(temporary.path(), "baseline", 3).expect("capture parses");
    assert_eq!(captured.sample.cost_usd, Some(1.25));
    assert_eq!(captured.sample.tokens.cache, 70);
    assert_eq!(captured.sample.tokens.cache_read, 30);
    assert_eq!(captured.sample.tokens.cache_write, 40);

    std::fs::write(
        temporary.path().join("sample.json"),
        br#"{"sampleId":"baseline-0003","runId":"r","status":"succeeded","costUsd":-1}"#,
    )
    .expect("sample.json is written");
    let error = captured_sample_from_directory(temporary.path(), "baseline", 3)
        .expect_err("negative cost is rejected");
    assert!(error.to_string().contains("costUsd"));
}

#[test]
fn local_runner_arms_become_zeroshot_run_commands() {
    use crate::native_v2_cli::{RunGraph, RunRuntime, RunSelection, TemplateDelivery};

    let manifest = EvalManifest::parse(include_str!(
        "../../fixtures/eval/talk/eval1-ci-before-review.toml"
    ))
    .expect("talk manifest parses");
    let base = std::path::Path::new("/evals/talk");
    let workspace = std::path::Path::new("/repo");
    let input = std::path::Path::new("/tmp/input.json");
    let context = local::LocalArmContext {
        manifest_directory: base,
        workspace,
        input,
        title: "eval sample",
    };
    let baseline = local::run_command(
        local::run_arguments(&manifest.arms[0], &context),
        &manifest.arms[0].id,
    )
    .expect("template arm parses");
    assert_eq!(baseline.cwd.as_deref(), Some(workspace));
    assert!(baseline.target.is_none());
    match &baseline.selection {
        RunSelection::Inline {
            graph: RunGraph::Template { delivery, .. },
            runtime: RunRuntime::Uniform(runtime),
        } => {
            assert_eq!(*delivery, TemplateDelivery::Push);
            assert_eq!(runtime, &base.join("runtime.json"));
        }
        selection => panic!("unexpected selection {selection:?}"),
    }
    let candidate = local::run_command(
        local::run_arguments(&manifest.arms[1], &context),
        &manifest.arms[1].id,
    )
    .expect("graph arm parses");
    match &candidate.selection {
        RunSelection::Inline {
            graph: RunGraph::File(graph),
            ..
        } => assert_eq!(graph, &base.join("graphs/ci-before-review.json")),
        selection => panic!("unexpected selection {selection:?}"),
    }

    for extra in [["--target", "platform"], ["--detach", "--title"]] {
        let mut arm = manifest.arms[1].clone();
        arm.run_args = extra.iter().map(|value| (*value).to_owned()).collect();
        if extra[0] == "--detach" {
            arm.run_args.truncate(1);
        }
        let error = local::run_command(
            local::run_arguments(&arm, &context),
            &format!("arm {:?}", arm.id),
        )
        .expect_err("unsupported local runArgs are rejected");
        assert!(error.to_string().contains("runArgs"), "{error}");
    }
    let mut arm = manifest.arms[1].clone();
    arm.run_args = vec!["--no-such-flag".to_owned()];
    let error = local::run_command(
        local::run_arguments(&arm, &context),
        &format!("arm {:?}", arm.id),
    )
    .expect_err("unknown flags are rejected");
    assert!(
        error.to_string().contains("not a valid `zeroshot run`"),
        "{error}"
    );
}

#[test]
fn local_arm_options_are_validated() {
    let source = include_str!("../../fixtures/eval/talk/eval2-delete-comments.toml");
    for (from, to, message) in [
        ("delivery = \"push\"", "delivery = \"ftp\"", "delivery"),
        (
            "uniformRuntimeConfig = \"runtime.json\"\nprompt",
            "uniformRuntimeConfig = \"runtime.json\"\nruntimeConfig = \"plan.json\"\nprompt",
            "at most one of runtimeConfig",
        ),
        (
            "deadlineMinutes = 90",
            "deadlineMinutes = 0",
            "deadlineMinutes",
        ),
        ("input = 3.0", "input = -3.0", "options.pricing.input"),
    ] {
        assert!(source.contains(from), "fixture contains {from}");
        let error = EvalManifest::parse(&source.replacen(from, to, 1))
            .expect_err("invalid manifest is rejected");
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn judge_runner_defaults_to_synthetic_only_for_the_fake_runner() {
    let fake = EvalManifest::parse(fixture_source()).expect("fixture manifest parses");
    assert_eq!(fake.judge_runner(None).expect("default"), "synthetic");
    assert_eq!(fake.judge_runner(Some("local")).expect("override"), "local");
    let local =
        EvalManifest::parse(&fixture_source().replace("runner = \"fake\"", "runner = \"local\""))
            .expect("local manifest parses");
    assert_eq!(local.judge_runner(None).expect("default"), "local");
    assert!(fake.judge_runner(Some("some-model")).is_err());
    let pinned = EvalManifest::parse(&fixture_source().replace(
        "judgeTemplate = \"eval-judge\"",
        "judgeTemplate = \"eval-judge\"\njudgeRunner = \"local\"",
    ))
    .expect("pinned manifest parses");
    assert_eq!(pinned.judge_runner(None).expect("manifest"), "local");
}

#[tokio::test]
async fn score_price_overrides_turn_token_costs_into_usd() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "priced", fixture_source()).await;
    let unpriced = data
        .report
        .as_ref()
        .and_then(|report| report.baseline.as_ref())
        .expect("comparison");
    assert_eq!(unpriced.cost_unit, CostUnit::Mtok);
    assert!(
        unpriced
            .notes
            .iter()
            .any(|note| note.contains("without a price"))
    );
    let report = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Score {
            weights: ScoreWeights {
                a: None,
                b: None,
                pricing: Pricing {
                    input: Some(2.0),
                    output: Some(10.0),
                    cache_read: Some(0.5),
                    cache_write: Some(0.5),
                },
            },
            id: data.id.to_string(),
            scorer: "baseline-ratio".to_owned(),
        },
    })
    .await
    .expect("rescore with prices");
    assert_eq!(report["baseline"]["costUnit"], "usd");
    let baseline = &report["baseline"]["pairs"][0]["baseline"];
    let tokens = |field: &str| baseline[field].as_f64().expect("token count");
    let expected =
        (tokens("inputTokens") * 2.0 + tokens("outputTokens") * 10.0 + tokens("cacheTokens") * 0.5)
            / 1_000_000.0;
    assert!((baseline["cost"].as_f64().expect("cost") - expected).abs() < 1e-12);
    assert_eq!(baseline["costSource"], "priced");
}
