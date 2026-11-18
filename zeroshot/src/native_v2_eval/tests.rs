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

fn criterion(id: &str) -> EvalCriterion {
    EvalCriterion {
        id: id.to_owned(),
        description: format!("{id} description"),
        weight: 1.0,
    }
}

#[test]
fn judge_parser_table_names_the_invalid_field() {
    let criteria = vec![criterion("correctness")];
    let valid = r#"{
      "judgements": [
        {
          "criterion": "correctness",
          "score": 4,
          "reasoning": "The patch updates the parser and adds a regression test.",
          "errors": []
        }
      ]
    }"#;
    let parsed = parse_judgement(valid, &criteria).expect("valid judgement parses");
    let judgement = parsed
        .judgements
        .get("correctness")
        .expect("criterion exists");
    assert_eq!(judgement.score, 4.0);
    assert!(judgement.passed());
    assert!(judgement.errors.is_empty());

    let multiline = parse_judgement(
        "{\"judgements\":[{\"criterion\":\"correctness\",\"score\":4,\"reasoning\":\"line one\\nline two\\tindented\",\"errors\":[]}]}",
        &criteria,
    )
    .expect("multiline reasoning is valid prose");
    assert!(
        multiline.judgements["correctness"]
            .reasoning
            .contains("line two")
    );

    let cases = [
        (
            r#"{"judgements":[{"criterion":"correctness","reasoning":"ok","errors":[]}]}"#,
            "judgements[0].score",
            "is required",
        ),
        (
            r#"{"judgements":[{"criterion":"correctness","score":6,"reasoning":"ok","errors":[]}]}"#,
            "judgements[0].score",
            "between 1 and 5",
        ),
        (
            r#"{"judgements":[{"criterion":"correctness","score":0,"reasoning":"ok","errors":[]}]}"#,
            "judgements[0].score",
            "between 1 and 5",
        ),
        (
            r#"{"judgements":[{"criterion":"correctness","score":3,"reasoning":"  ","errors":[]}]}"#,
            "judgements[0].reasoning",
            "must not be empty",
        ),
        (
            r#"{"judgements":[{"criterion":"correctness","score":3,"reasoning":"bad\u0001text","errors":[]}]}"#,
            "judgements[0].reasoning",
            "control characters",
        ),
        (
            r#"{"judgements":[{"criterion":"correctness","score":3,"reasoning":"ok","errors":["bad\u001b[31mtext"]}]}"#,
            "judgements[0].errors[0]",
            "control characters",
        ),
        (
            r#"{"judgements":[{"criterion":"correctness","score":3,"reasoning":"ok"}]}"#,
            "judgements[0].errors",
            "is required",
        ),
        (
            r#"{"judgements":[{"criterion":"correctness","score":3,"reasoning":"ok","errors":[""]}]}"#,
            "judgements[0].errors[0]",
            "must not be empty",
        ),
    ];
    for (source, field, message) in cases {
        let error = parse_judgement(source, &criteria).expect_err("invalid judgement is rejected");
        assert_eq!(error.field, field, "{source}");
        assert!(error.message.contains(message), "{}", error.message);
        assert!(error.to_string().contains(field));
    }

    let error = parse_judgement(
        r#"{"judgements":[{"criterion":"missing","score":3,"reasoning":"ok","errors":[]}]}"#,
        &criteria,
    )
    .expect_err("unknown criterion is rejected");
    assert_eq!(error.field, "judgements[0].criterion");
    assert!(error.message.contains("unknown criterion"));
}

struct FakeJudge;

#[async_trait::async_trait]
impl Judge for FakeJudge {
    async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError> {
        let score = match request.sample.arm_id.as_str() {
            "baseline" => 5.0,
            _ => 2.0,
        };
        let judgements = request
            .criteria
            .iter()
            .map(|criterion| {
                (
                    criterion.id.clone(),
                    Judgement {
                        score,
                        reasoning: format!(
                            "{} judge for {} run {} inspected the captured patch.diff",
                            criterion.id, request.sample.arm_id, request.sample.run_index
                        ),
                        errors: Vec::new(),
                    },
                )
            })
            .collect();
        Ok(JudgeResult { judgements })
    }
}

#[tokio::test]
async fn judge_runner_judges_captured_samples_and_records_model_provenance() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-judge", fixture_source()).await;
    let store = EvalStore::for_workspace(temporary.path());
    let mut loaded = store.load(&data.id).expect("stored eval loads");
    let judged = judge_eval(
        &store,
        &mut loaded,
        &FakeJudge,
        JudgeOptions::default()
            .with_model("judge-test-model")
            .with_template("eval-judge"),
    )
    .await
    .expect("judge run succeeds");

    assert_eq!(judged.model, "judge-test-model");
    assert_eq!(judged.template, "eval-judge");
    assert_eq!(judged.judged.len(), 4);
    assert_eq!(judged.skipped.len(), 2);
    assert!(loaded.samples.iter().all(|sample| {
        sample.status != SampleStatus::Succeeded
            || sample.judge_model.as_deref() == Some("judge-test-model")
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
    assert_eq!(correctness.x_mean, Some(5.0));
    assert_eq!(correctness.y_mean, Some(2.0));
    assert_eq!(correctness.paired_difference, Some(-3.0));
    assert!(!correctness.outliers.is_empty());

    let markdown = render_markdown(report);
    assert!(markdown.contains("## X vs Y"));
    assert!(markdown.contains("correctness judge for baseline run 0"));
    assert!(markdown.contains("correctness judge for candidate run 0"));
    assert!(markdown.contains("judge-test-model"));

    let stored = store.load(&loaded.id).expect("judged eval reloads");
    assert_eq!(stored.report.as_ref(), Some(report));
}

#[tokio::test]
async fn judge_report_markdown_carries_per_trait_reasoning() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-reasons", fixture_source()).await;
    let store = EvalStore::for_workspace(temporary.path());
    let mut loaded = store.load(&data.id).expect("stored eval loads");
    judge_eval(
        &store,
        &mut loaded,
        &FakeJudge,
        JudgeOptions::default().with_model("reasoning-model"),
    )
    .await
    .expect("judge run succeeds");
    let report = loaded.report.as_ref().expect("judged report exists");
    let markdown = render_markdown(report);
    assert!(markdown.contains("### Outlier reasoning"));
    assert!(markdown.contains("inspected the captured patch.diff"));
    assert!(markdown.contains("X (`reasoning-model`)"));
    assert!(markdown.contains("Y (`reasoning-model`)"));
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
}

#[tokio::test]
async fn cli_judge_action_records_synthetic_model_and_comparison() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let data = run_fixture_in(temporary.path(), "fixture-cli-judge", fixture_source()).await;
    let report = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::Judge(EvalJudgeCommand {
            id: data.id.to_string(),
            judge_model: Some("cli-judge-model".to_owned()),
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
    assert!(markdown.contains("cli-judge-model"));
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
        row.status == SampleStatus::Succeeded
            && row.judge_model.as_deref() == Some("synthetic-judge")
            && row.judge_executor.as_deref() == Some("fixture")
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
        model: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, EvalError> {
        self.inputs.lock().unwrap().push(input.clone());
        Ok(json!({
            "criterion": input["criterion"],
            "score": 4,
            "reasoning": format!("The captured patch handles this criterion for {model}."),
            "errors": [],
        }))
    }
}

#[tokio::test]
async fn template_judge_executes_the_graph_once_per_criterion() {
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
        delivered_revision: None,
        failure_reason: None,
        patch: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        judge_model: None,
        judge_template: None,
        judge_checkout: None,
        judge_executor: None,
    };
    let criteria = vec![criterion("correctness"), criterion("efficiency")];
    let result = judge
        .judge(JudgeRequest::new(
            sample,
            criteria,
            "patch body",
            "judge-model",
        ))
        .await
        .expect("template judge succeeds");
    assert_eq!(result.judgements.len(), 2);
    let inputs = executor.inputs.lock().unwrap();
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0]["criterion"], "correctness");
    assert_eq!(inputs[1]["criterion"], "efficiency");
    assert_eq!(inputs[0]["patch"], "patch body");
    assert_eq!(inputs[0]["model"], "judge-model");
    assert_eq!(
        result.judgements["correctness"].reasoning,
        "The captured patch handles this criterion for judge-model."
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

#[test]
fn judge_runtime_applies_the_effective_model_to_agent_bindings() {
    use openengine_cluster_protocol::{ModelId, NodeName, RunSize, SessionScope};
    use crate::native_v2_contract::NodeRuntimeBinding;

    let node = NodeName::new("judge").expect("node name");
    let mut nodes = std::collections::BTreeMap::new();
    nodes.insert(
        node.clone(),
        NodeRuntimeBinding::Agent {
            lane: None,
            model: ModelId::new("authored-model").expect("model"),
            effort: None,
            session_scope: SessionScope::Execution,
            connections: Default::default(),
        },
    );
    let runtime = crate::native_v2_candidate::test_support::codex_runtime(RunSize::Small, nodes);
    let updated = runtime_with_model(&runtime, "per-arm-model").expect("runtime model applies");
    let nodes = match updated {
        openengine_cluster_protocol::RuntimePlan::Codex { nodes, .. } => nodes,
        _ => panic!("fixture runtime is Codex"),
    };
    match nodes.get(&node).expect("judge binding") {
        NodeRuntimeBinding::Agent { model, .. } => assert_eq!(model.as_str(), "per-arm-model"),
        _ => panic!("judge binding is an agent"),
    }
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
            judge_model: Some("capture-judge".to_owned()),
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
        sample.judge_model.as_deref() == Some("capture-judge")
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
            judge_model: Some("provider/real-judge".to_owned()),
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
        "criterion": "correctness",
        "criterionDescription": "The change satisfies the request.",
        "patch": "diff --git a/file b/file",
        "sample": "{}",
        "model": "judge-model",
        "checkout": "",
        "score": 0.0
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
            judge_model: Some("capture-judge".to_owned()),
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
                        score: 5.0,
                        reasoning: format!("The checked-out file contains {file:?}."),
                        errors: Vec::new(),
                    },
                )]),
            })
        }
    }
    let result = CheckoutJudge
        .judge(
            JudgeRequest::new(sample, criteria, captured.patch.unwrap(), "judge-model")
                .with_checkout(checkout),
        )
        .await
        .expect("judge cites checkout");
    assert!(
        result.judgements["correctness"]
            .reasoning
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
            judge_model: Some("capture-judge".to_owned()),
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
                            score: 2.0,
                            reasoning: "The patch misses the requested guard.".to_owned(),
                            errors: vec!["missing guard in parser.rs".to_owned()],
                        },
                    )
                })
                .collect();
            Ok(JudgeResult { judgements })
        }
    }

    let temporary = tempfile::tempdir().expect("temporary workspace");
    let source = fixture_source()
        .replace("judgeModel = \"synthetic-judge\"\n", "")
        .replace("judgeTemplate = \"eval-judge\"\n", "");
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
    judge_eval(
        &store,
        &mut loaded,
        &ErrorJudge,
        JudgeOptions::default().with_model("error-model"),
    )
    .await
    .expect("error judge runs");
    let markdown = render_markdown(loaded.report.as_ref().expect("judged report"));
    assert!(markdown.contains("## Judge errors"));
    assert!(markdown.contains("missing guard in parser.rs"));
}

#[tokio::test]
async fn relative_judge_runtime_path_is_persisted_against_the_manifest_directory() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let source = fixture_source()
        .replace("judgeModel = \"synthetic-judge\"\n", "")
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
            judge_model: Some("capture-judge".to_owned()),
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
                                score: 5.0,
                                reasoning: format!("Runner checkout contains {file:?}."),
                                errors: Vec::new(),
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
    judge_eval(
        &store,
        &mut data,
        &CheckoutJudge,
        JudgeOptions::default().with_model("checkout-judge"),
    )
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
async fn user_shaped_custom_graph_admits_without_the_internal_score_placeholder() {
    use openengine_cluster_protocol::{
        IdempotencyKey, ModelId, NodeName, RunSize, RunTitle, SessionScope,
    };
    use crate::native_v2_admission::{DeliveryPolicy, NativeV2Admission};
    use crate::native_v2_contract::{NodeRuntimeBinding, RunSubmissionIntent};

    let mut value: serde_json::Value =
        serde_json::from_str(JUDGE_TEMPLATE_JSON).expect("template JSON");
    value["initialInput"]["fields"]
        .as_object_mut()
        .unwrap()
        .remove("score");
    value["root"]["state"]["fields"]
        .as_object_mut()
        .unwrap()
        .remove("score");
    value["root"]["children"][0]["output"]["fields"]
        .as_object_mut()
        .unwrap()
        .remove("score");
    value["root"]["children"][0]["writeBindings"]
        .as_array_mut()
        .unwrap()
        .retain(|binding| binding["target"][0] != "score");
    value["root"]["children"][1]["output"]["fields"]
        .as_object_mut()
        .unwrap()
        .remove("score");
    value["root"]["children"][1]["bindings"]
        .as_array_mut()
        .unwrap()
        .retain(|binding| binding["target"][0] != "score");
    let graph: openengine_cluster_protocol::GraphSpec =
        serde_json::from_value(value).expect("custom graph parses");
    let template = JudgeTemplate {
        name: "custom".to_owned(),
        graph: graph.clone(),
        source: String::new(),
    };
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
                delivered_revision: None,
                failure_reason: None,
                patch: None,
                criteria: Default::default(),
                reasoning: Default::default(),
                judge_model: None,
                judge_template: None,
                judge_checkout: None,
                judge_executor: None,
            },
            vec![criterion("correctness")],
            "patch",
            "model",
        ),
        &criterion("correctness"),
    );
    let filtered = judge_input_for_template(
        &template,
        &crate::native_v2_eval::JudgeRequest::new(
            crate::native_v2_eval::EvalSample {
                id: "baseline-0000".to_owned(),
                run_id: "run".to_owned(),
                arm_id: "baseline".to_owned(),
                run_index: 0,
                status: SampleStatus::Succeeded,
                timing: SampleTiming::default(),
                tokens: TokenUsage::default(),
                delivered_revision: None,
                failure_reason: None,
                patch: None,
                criteria: Default::default(),
                reasoning: Default::default(),
                judge_model: None,
                judge_template: None,
                judge_checkout: None,
                judge_executor: None,
            },
            vec![criterion("correctness")],
            "patch",
            "model",
        ),
        &criterion("correctness"),
    )
    .expect("filtered input");
    assert!(input.get("score").is_some());
    assert!(filtered.get("score").is_none());
    assert!(filtered.get("checkout").is_some());

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
        delivered_revision: None,
        failure_reason: None,
        patch: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        judge_model: None,
        judge_template: None,
        judge_checkout: None,
        judge_executor: None,
    };
    let mut data = EvalData {
        id,
        created_at_ms: 1,
        manifest,
        samples: vec![sample],
        report: None,
    };
    let error = judge_eval(
        &store,
        &mut data,
        &FakeJudge,
        JudgeOptions::default().with_model("judge-model"),
    )
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
                                score: 6.0,
                                reasoning: String::new(),
                                errors: vec![String::new()],
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
    let error = judge_eval(
        &store,
        &mut loaded,
        &InvalidJudge,
        JudgeOptions::default().with_model("invalid-judge"),
    )
    .await
    .expect_err("invalid judgement is rejected");
    assert!(error.to_string().contains("correctness"));
    assert!(error.to_string().contains("score"));
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
            delivered_revision: None,
            failure_reason: None,
            patch: None,
            criteria: Default::default(),
            reasoning: Default::default(),
            judge_model: None,
            judge_template: None,
            judge_checkout: None,
            judge_executor: None,
        },
        patch: None,
        sample_json: serde_json::Value::Null,
        checkout: None,
    };
    let error = judge_captured_samples(
        &FakeJudge,
        &[criterion("correctness")],
        vec![captured],
        JudgeOptions::default().with_model("judge-model"),
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
            judge_model: Some("judge".to_owned()),
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
            judge_model: Some("judge".to_owned()),
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
    document["criteria"]["correctness"]["score"] = serde_json::json!(6.0);
    std::fs::write(&sample_path, serde_json::to_vec(&document).unwrap()).expect("rewrite capture");

    let store = EvalStore::for_workspace(temporary.path());
    let error = execute(&EvalCommand {
        directory: temporary.path().to_path_buf(),
        action: EvalAction::JudgeCaptures(EvalJudgeCapturesCommand {
            manifest,
            captures,
            id: Some("rollback".to_owned()),
            judge_model: Some("judge".to_owned()),
            judge_template: None,
            scorer: None,
        }),
    })
    .await
    .expect_err("invalid later judgement fails the command");
    assert!(error.to_string().contains("score"));
    assert!(store.list().expect("store lists").is_empty());
}
