use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::{
    HubCommand, HubEvalCaseAdd, HubEvalCaseSource, HubEvalCommand, HubEvalRun, HubEvalVariant,
    HubJobSpec, HubJudgeMode, HubMemberAdd, HubStrategy,
};

use super::super::api::{ApiState, router};
use super::super::client::{HubClient, execute};
use super::super::context::{Resolved, TokenRef};
use super::super::error::HubError;
use super::super::eval::client::report_exit_code;
use super::super::eval::judge::{parse_reply, parse_verdict};
#[cfg(unix)]
use super::super::eval::judge::HostCommand;
use super::super::eval::judge::{Ended, Finished};
#[cfg(unix)]
use zeroshot_engine::native_v2_cli::hub::HubEvalReport;
use super::super::eval::report::{Against, Thresholds, report, stat, table};
use super::super::stats::ModelPrice;
use super::super::eval::variant::{Change, Material, Request, merge_patch};
use super::super::eval::{
    Baseline, Case, EvalRun, JudgeMode, Outcome, Sample, SampleState, Score, Verdict, canonical,
    sample_job_id,
};
use super::super::model::{PlacementSpec, Requirements};
use super::super::usage::reported;
use super::super::service::HubService;
use super::super::store::HubStore;
use super::FakeDriver;
use crate::native_v2_target::FileTargetRegistry;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

fn material(name: &str, value: &Value) -> Material {
    Material {
        name: name.to_owned(),
        content: serde_json::to_vec(value).expect("json"),
    }
}

fn change(key: &str, value: &str, file: Option<Material>) -> Change {
    Change {
        key: key.to_owned(),
        value: value.to_owned(),
        file,
    }
}

fn captured() -> Request {
    let runtime = json!({
        "model": "big",
        "nodes": {
            "plan": {"model": "big", "harness": "codex"},
            "build": {"harness": "codex"}
        }
    });
    Request {
        run_args: [
            "--title",
            "T",
            "--input",
            "file:--input",
            "--template",
            "solo",
            "--runtime-config",
            "file:--runtime-config",
            "--no-environment",
        ]
        .map(str::to_owned)
        .to_vec(),
        files: BTreeMap::from([
            (
                "--input".to_owned(),
                material("input.json", &json!({"task": "x"})),
            ),
            (
                "--runtime-config".to_owned(),
                material("runtime.json", &runtime),
            ),
        ]),
    }
}

fn runtime_of(request: &Request) -> Value {
    let flag = request.runtime_flag().expect("runtime flag");
    serde_json::from_slice(&request.files[flag].content).expect("runtime json")
}

#[test]
fn variant_overrides_replace_topology_and_patch_the_runtime() {
    let graph = material("g.json", &json!({"nodes": {}}));
    let patched = captured()
        .apply(&[
            change("graph", "", Some(graph.clone())),
            change("model", "small", None),
            change(
                "runtime-patch",
                "",
                Some(material(
                    "patch.json",
                    &json!({"nodes": {"build": {"lane": "gpu", "harness": null}}, "effort": "low"}),
                )),
            ),
        ])
        .expect("apply");
    let arguments = patched.run_args.join(" ");
    assert!(!arguments.contains("--template"), "{arguments}");
    assert!(arguments.contains("--graph file:--graph"), "{arguments}");
    assert_eq!(patched.files["--graph"], graph);
    let runtime = runtime_of(&patched);
    assert_eq!(runtime["model"], "small");
    assert_eq!(runtime["nodes"]["plan"]["model"], "small");
    // Only nodes that already named a model get the new one.
    assert!(runtime["nodes"]["build"].get("model").is_none());
    assert_eq!(runtime["nodes"]["build"]["lane"], "gpu");
    assert!(runtime["nodes"]["build"].get("harness").is_none());
    assert_eq!(runtime["effort"], "low");
    assert_eq!(patched.models(), vec!["small".to_owned()]);
    assert!(patched.lanes());

    // The template wins back over a graph; uniform runtime replaces runtime config; an
    // environment drops --no-environment.
    let uniform = material("u.json", &json!({"model": "mid"}));
    let swapped = patched
        .apply(&[
            change("template", "duo", None),
            change("uniform-runtime-config", "", Some(uniform)),
            change("environment", "", Some(material("env.json", &json!({})))),
            change("provider", "acme", None),
        ])
        .expect("apply");
    let arguments = swapped.run_args.join(" ");
    assert!(arguments.contains("--template duo") && !arguments.contains("--graph"));
    assert!(!arguments.contains("--runtime-config") && !arguments.contains("--no-environment"));
    assert!(arguments.contains("--uniform-runtime-config file:--uniform-runtime-config"));
    assert!(arguments.contains("--environment file:--environment"));
    assert_eq!(
        runtime_of(&swapped),
        json!({"model": "mid", "provider": "acme"})
    );
    assert_eq!(swapped.models(), vec!["mid".to_owned()]);
    assert!(!swapped.lanes());
}

#[test]
fn runtime_field_overrides_need_a_runtime_file() {
    let bare = Request {
        run_args: ["--title", "T"].map(str::to_owned).to_vec(),
        files: BTreeMap::new(),
    };
    let error = bare
        .apply(&[change("model", "small", None)])
        .expect_err("no runtime file");
    assert!(error.to_string().contains("runtime-config=FILE"), "{error}");
    let error = captured()
        .apply(&[change("graph", "", None)])
        .expect_err("file content missing");
    assert!(error.to_string().contains("needs file content"), "{error}");
}

#[test]
fn merge_patch_follows_rfc_7386() {
    let mut target = json!({"a": "b", "c": {"d": "e", "f": "g"}});
    merge_patch(&mut target, &json!({"a": "z", "c": {"f": null}}));
    assert_eq!(target, json!({"a": "z", "c": {"d": "e"}}));
    let mut target = json!({"a": [1]});
    merge_patch(&mut target, &json!({"a": {"b": 1}}));
    assert_eq!(target, json!({"a": {"b": 1}}));
    let mut target = json!({"a": 1});
    merge_patch(&mut target, &json!(["x"]));
    assert_eq!(target, json!(["x"]));
}

#[test]
fn judge_replies_are_validated_against_the_mode() {
    let verdict = parse_reply(
        "thinking...\n{\"note\": 1}\n{\"score\": 0.75, \"verdict\": \"pass\", \"rationale\": \"ok\"}\n",
        JudgeMode::Absolute,
    );
    assert_eq!(verdict.score, Some(Score(0.75)));
    assert_eq!(verdict.verdict.as_deref(), Some("pass"));
    assert_eq!(verdict.rationale.as_deref(), Some("ok"));
    assert!(verdict.error.is_none());

    let negative = parse_verdict(&json!({"score": -0.5}), JudgeMode::Pairwise);
    assert_eq!(negative.score, Some(Score(-0.5)));
    let out_of_range = parse_verdict(&json!({"score": -0.5}), JudgeMode::Absolute);
    assert!(out_of_range.score.is_none());
    assert!(
        out_of_range
            .error
            .as_deref()
            .is_some_and(|error| error.contains("outside"))
    );
    let missing = parse_reply("{\"verdict\": \"pass\"}", JudgeMode::Absolute);
    assert!(
        missing
            .error
            .as_deref()
            .is_some_and(|error| error.contains("numeric score"))
    );
    assert!(
        parse_reply("no json here", JudgeMode::Absolute)
            .error
            .is_some()
    );
}

// The judge is a POSIX shell script.
#[cfg(unix)]
#[tokio::test]
async fn host_commands_get_the_request_on_stdin_and_the_eval_environment() {
    let directory = tempfile::tempdir().expect("tempdir");
    let script = directory.path().join("judge.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\nread -r request\necho \"log: $ZEROSHOT_EVAL_VARIANT\" >&2\n\
         case \"$request\" in *absolute*) mode=absolute ;; *) mode=unknown ;; esac\n\
         echo \"{\\\"score\\\": 1, \\\"verdict\\\": \\\"pass\\\", \\\"rationale\\\": \\\"$mode\\\"}\"\n",
    )
    .expect("script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    }
    let env = vec![("ZEROSHOT_EVAL_VARIANT".to_owned(), "fast".to_owned())];
    let finished = HostCommand {
        program: &script,
        arguments: &[],
        cwd: directory.path(),
        env: &env,
    }
    .run(
        b"{\"mode\":\"absolute\"}\n",
        std::time::Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        finished.ended,
        Ended::Exited(Some(0)),
        "{}",
        finished.stderr
    );
    assert!(finished.stderr.contains("log: fast"));
    let verdict = parse_reply(&finished.stdout, JudgeMode::Absolute);
    assert_eq!(verdict.score, Some(Score(1.0)));
    assert_eq!(verdict.rationale.as_deref(), Some("absolute"));
}

fn sample(
    case: &str,
    variant: &str,
    repeat: u32,
    measured: (bool, u64, u64, Option<f64>),
) -> Sample {
    let (succeeded, tokens, duration, score) = measured;
    Sample {
        case_id: case.to_owned(),
        variant: variant.to_owned(),
        repeat,
        job_id: sample_job_id("er_1", case, variant, repeat),
        state: SampleState::Judged,
        outcome: Some(Outcome {
            status: if succeeded { "succeeded" } else { "failed" }.to_owned(),
            failure_reason: None,
            target: Some("local".to_owned()),
            run_id: None,
            queue_wait_ms: Some(0),
            duration_ms: Some(duration),
            token_usage: Some(reported(tokens, 0, None)),
            model: "m".to_owned(),
        }),
        checks: Vec::new(),
        judge: score.map(|score| Verdict {
            score: Some(Score(score)),
            ..Verdict::default()
        }),
        judge_job: None,
        error: None,
    }
}

fn eval_run(samples: Vec<Sample>, mode: Option<JudgeMode>) -> EvalRun {
    EvalRun {
        run_id: "er_1".to_owned(),
        key: None,
        suite: None,
        cases: vec!["ec_a".to_owned(), "ec_b".to_owned()],
        baseline: Baseline::Variant {
            name: "baseline".to_owned(),
        },
        variants: Vec::new(),
        repeat: 2,
        placement: None,
        priority: 0,
        judge: mode.map(|mode| super::super::eval::Judge {
            kind: super::super::eval::JudgeKind::Cmd,
            mode,
            program: super::super::eval::BlobRef {
                name: "judge".to_owned(),
                sha256: "0".repeat(64),
            },
            runtime: None,
        }),
        checks: Vec::new(),
        prices: None,
        created_at_ms: 0,
        samples,
    }
}

fn own_baseline(run: &EvalRun) -> Against<'_> {
    Against {
        label: json!({"runId": run.run_id, "variant": "baseline"}),
        samples: run
            .samples
            .iter()
            .filter(|sample| sample.variant == "baseline")
            .collect(),
        pairwise_reference: run.judge_mode() == Some(JudgeMode::Pairwise),
    }
}

fn variant_row<'a>(report: &'a Value, name: &str) -> &'a Value {
    report["variants"]
        .as_array()
        .expect("variants")
        .iter()
        .find(|row| row["variant"] == name)
        .expect("variant row")
}

#[test]
fn report_deltas_flag_regressions_and_improvements_by_threshold() {
    let run = eval_run(
        vec![
            sample("ec_a", "baseline", 1, (true, 1000, 1000, Some(0.8))),
            sample("ec_a", "baseline", 2, (true, 1000, 1000, Some(0.8))),
            sample("ec_b", "baseline", 1, (true, 1000, 1000, Some(0.8))),
            sample("ec_b", "baseline", 2, (true, 1000, 1000, Some(0.8))),
            // cheap: same quality, 40% fewer tokens -> improved.
            sample("ec_a", "cheap", 1, (true, 600, 1000, Some(0.8))),
            sample("ec_a", "cheap", 2, (true, 600, 1000, Some(0.8))),
            sample("ec_b", "cheap", 1, (true, 600, 1000, Some(0.8))),
            sample("ec_b", "cheap", 2, (true, 600, 1000, Some(0.8))),
            // broken: fails case b, scores lower -> regressed (and cheaper -> mixed).
            sample("ec_a", "broken", 1, (true, 500, 1000, Some(0.6))),
            sample("ec_a", "broken", 2, (true, 500, 1000, Some(0.6))),
            sample("ec_b", "broken", 1, (false, 500, 1000, Some(0.2))),
            sample("ec_b", "broken", 2, (false, 500, 1000, Some(0.2))),
        ],
        Some(JudgeMode::Absolute),
    );
    let thresholds = Thresholds::default();
    let document = report(&run, Some(&own_baseline(&run)), &thresholds, None);
    assert_eq!(document["schema"], "zeroshot.hub.eval.report/v1");
    assert_eq!(document["complete"], true);
    assert_eq!(variant_row(&document, "baseline")["role"], "baseline");
    let cheap = variant_row(&document, "cheap");
    assert_eq!(cheap["verdict"], "improved");
    assert_eq!(cheap["delta"]["tokens"]["relative"], -0.4);
    assert_eq!(cheap["delta"]["passRate"]["absolute"], 0.0);
    let broken = variant_row(&document, "broken");
    assert_eq!(broken["verdict"], "mixed");
    assert_eq!(broken["delta"]["passRate"]["absolute"], -0.5);
    assert_eq!(broken["delta"]["judgeScore"]["absolute"], -0.4);
    let flagged = broken["flags"]
        .as_array()
        .expect("flags")
        .iter()
        .map(|flag| {
            (
                flag["metric"].as_str().unwrap_or_default(),
                flag["direction"].as_str().unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        flagged,
        vec![
            ("passRate", "regressed"),
            ("judgeScore", "regressed"),
            ("tokens", "improved")
        ]
    );
    assert_eq!(document["regressions"], 1);
    assert_eq!(document["improvements"], 1);
    assert_eq!(document["regressed"], true);
    assert_eq!(report_exit_code(&document, true), 6);
    assert_eq!(report_exit_code(&document, false), 0);
    // Per case: broken is unchanged on case a for pass rate but regresses on case b.
    let case_b = document["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .find(|case| case["caseId"] == "ec_b")
        .expect("case b");
    let broken_b = case_b["variants"]
        .as_array()
        .expect("rows")
        .iter()
        .find(|row| row["variant"] == "broken")
        .expect("row");
    assert_eq!(broken_b["delta"]["passRate"]["absolute"], -1.0);

    // Raising thresholds or turning metrics off changes the verdicts.
    let relaxed = Thresholds::parse(&[
        ("passRate".to_owned(), "off".to_owned()),
        ("judgeScore".to_owned(), "0.5".to_owned()),
        ("tokens".to_owned(), "0.6".to_owned()),
    ])
    .expect("thresholds");
    let document = report(&run, Some(&own_baseline(&run)), &relaxed, None);
    assert_eq!(variant_row(&document, "broken")["verdict"], "unchanged");
    assert_eq!(variant_row(&document, "cheap")["verdict"], "unchanged");
    assert_eq!(report_exit_code(&document, true), 0);
    assert!(Thresholds::parse(&[("speed".to_owned(), "1".to_owned())]).is_err());
    assert!(Thresholds::parse(&[("tokens".to_owned(), "-1".to_owned())]).is_err());
}

#[test]
fn pairwise_scores_are_deltas_and_incomplete_runs_exit_3() {
    let mut samples = vec![
        sample("ec_a", "baseline", 1, (true, 1000, 1000, None)),
        sample("ec_a", "next", 1, (true, 1000, 1000, Some(-0.5))),
        sample("ec_a", "next", 2, (true, 1000, 1000, Some(-0.3))),
    ];
    samples[0].judge = Some(Verdict {
        verdict: Some("reference".to_owned()),
        ..Verdict::default()
    });
    let mut run = eval_run(samples, Some(JudgeMode::Pairwise));
    let document = report(
        &run,
        Some(&own_baseline(&run)),
        &Thresholds::default(),
        None,
    );
    let next = variant_row(&document, "next");
    assert_eq!(next["delta"]["judgeScore"]["absolute"], -0.4);
    assert_eq!(next["verdict"], "regressed");
    let ci = &next["aggregate"]["judgeScore"]["ci95"];
    assert!(ci[0].as_f64().expect("low") < -0.4 && ci[1].as_f64().expect("high") > -0.4);
    run.samples[2].state = SampleState::Pending;
    let document = report(
        &run,
        Some(&own_baseline(&run)),
        &Thresholds::default(),
        None,
    );
    assert_eq!(document["complete"], false);
    assert_eq!(report_exit_code(&document, true), 3);
}

#[test]
fn confidence_interval_is_a_normal_approximation() {
    let single = stat(&[0.5]);
    assert_eq!(single.mean, Some(0.5));
    assert_eq!(single.ci95, None);
    let pair = stat(&[0.0, 1.0]);
    let (low, high) = pair.ci95.expect("ci");
    // s = 0.7071, half = 1.96 * s / sqrt(2) = 0.98
    assert!((low - (0.5 - 0.98)).abs() < 1e-9 && (high - (0.5 + 0.98)).abs() < 1e-9);
}

/// A baseline and a cheaper variant over a short and a long case id.
fn table_run() -> (EvalRun, BTreeMap<String, ModelPrice>) {
    let long_case = "ec_0123456789abcdef_long_case_id";
    let mut samples = vec![
        sample("ec_a", "baseline", 1, (true, 1_000_000, 1000, Some(0.8))),
        sample("ec_a", "baseline", 2, (true, 1_000_000, 1000, Some(0.8))),
        sample(long_case, "baseline", 1, (true, 1_000_000, 1000, Some(0.8))),
        sample(long_case, "baseline", 2, (true, 1_000_000, 1000, Some(0.8))),
        // cheap: fails one long case, half the tokens, twice the wall clock.
        sample("ec_a", "cheap", 1, (true, 500_000, 2000, Some(0.3))),
        sample("ec_a", "cheap", 2, (true, 500_000, 2000, Some(0.3))),
        sample(long_case, "cheap", 1, (false, 500_000, 2000, Some(0.3))),
        sample(long_case, "cheap", 2, (true, 500_000, 2000, Some(0.3))),
    ];
    for sample in samples
        .iter_mut()
        .filter(|sample| sample.variant == "cheap")
    {
        sample.outcome.as_mut().expect("outcome").model = "n".to_owned();
    }
    let prices = BTreeMap::from([
        (
            "m".to_owned(),
            ModelPrice {
                input: 2.0,
                ..ModelPrice::default()
            },
        ),
        (
            "n".to_owned(),
            ModelPrice {
                input: 3.0,
                ..ModelPrice::default()
            },
        ),
    ]);
    (eval_run(samples, Some(JudgeMode::Absolute)), prices)
}

#[test]
fn table_is_self_describing_and_names_units_and_delta_kinds() {
    let (run, prices) = table_run();
    let document = report(
        &run,
        Some(&own_baseline(&run)),
        &Thresholds::default(),
        Some(&prices),
    );
    let rendered = table(&document);
    let mut lines = rendered.lines();
    assert_eq!(
        lines.next().expect("title"),
        "Eval report er_1  baseline=er_1:baseline  complete=true"
    );
    let header = lines.next().expect("header");
    for name in [
        "CASE",
        "VARIANT",
        "SAMPLES(count)",
        "PASS_RATE(0..1)",
        "JUDGE_SCORE(0..1)",
        "TOKENS(tok)",
        "COST_USD(USD)",
        "DURATION_MS(ms)",
        "VERDICT",
    ] {
        assert!(header.contains(name), "{name} missing from {header}");
    }
    let data = lines
        .by_ref()
        .take_while(|line| !line.is_empty())
        .collect::<Vec<_>>();
    assert!(!data.is_empty());
    let cheap = data
        .iter()
        .find(|line| line.contains("ALL CASES (aggregate)") && line.contains("cheap"))
        .expect("cheap aggregate row");
    // pass rate and judge score deltas are absolute, the rest are relative.
    assert!(cheap.contains("0.75 (-0.250)"), "{cheap}");
    assert!(cheap.contains("0.30 (-0.500)"), "{cheap}");
    assert!(cheap.contains("500000 (-50.0%)"), "{cheap}");
    assert!(cheap.contains("1.5000 (-25.0%)"), "{cheap}");
    assert!(cheap.contains("2000 (+100.0%)"), "{cheap}");
    // A long case id is truncated, so every row keeps the same column alignment.
    let long = data
        .iter()
        .find(|line| line.contains("ec_0123456789abcdef") && line.contains("cheap"))
        .expect("long case row");
    assert!(!long.contains("ec_0123456789abcdef_long_case_id"), "{long}");
    let column =
        |line: &str, needle: &str| line[..line.find(needle).expect("column")].chars().count();
    assert_eq!(
        column(long, "cheap"),
        column(cheap, "cheap"),
        "{long}\n{cheap}"
    );
    assert_eq!(
        column(long, "2000 (+100.0%)"),
        column(cheap, "2000 (+100.0%)")
    );
    let legend = lines.collect::<Vec<_>>();
    assert!(legend[0].starts_with("Legend:"), "{legend:?}");
    assert!(
        legend
            .iter()
            .any(|line| line.contains("PASS_RATE") && line.contains("absolute"))
    );
    assert!(
        legend
            .iter()
            .any(|line| line.contains("TOKENS") && line.contains("relative"))
    );
    assert!(
        legend
            .iter()
            .any(|line| line.contains("All variants are compared with the baseline"))
    );
    assert!(
        legend
            .iter()
            .any(|line| line.contains("'-' means unavailable"))
    );
    for line in legend {
        assert!(
            line.chars().count() <= 80,
            "{} columns: {line}",
            line.chars().count()
        );
    }
}

#[test]
fn table_prints_a_dash_for_a_missing_cost() {
    let (run, _) = table_run();
    let document = report(
        &run,
        Some(&own_baseline(&run)),
        &Thresholds::default(),
        None,
    );
    let rendered = table(&document);
    let data = rendered
        .lines()
        .skip_while(|line| !line.starts_with("CASE"))
        .skip(1)
        .take_while(|line| !line.is_empty())
        .collect::<Vec<_>>();
    assert!(!data.is_empty());
    for line in data {
        assert!(
            line.contains(" - "),
            "missing cost not rendered as '-': {line}"
        );
    }
}

#[test]
fn case_ids_hash_the_captured_request_not_its_name() {
    let mut case = Case {
        case_id: String::new(),
        name: Some("one".to_owned()),
        title: "T".to_owned(),
        run_args: vec!["--title".to_owned(), "T".to_owned()],
        files: BTreeMap::new(),
        cwd: "/w".into(),
        pinned_revision: Some("abc".to_owned()),
        placement: PlacementSpec::default(),
        requirements: Requirements::default(),
        labels: BTreeMap::new(),
        project: None,
        models: Vec::new(),
        source_job: None,
        created_at_ms: 1,
    };
    case.assign_id();
    let first = case.case_id.clone();
    case.name = Some("two".to_owned());
    case.created_at_ms = 2;
    case.assign_id();
    assert_eq!(case.case_id, first);
    assert!(first.starts_with("ec_") && first.len() == 19);
    case.pinned_revision = Some("def".to_owned());
    case.assign_id();
    assert_ne!(case.case_id, first);
    assert_eq!(
        canonical(&json!({"b": 1, "a": [{"d": 1, "c": 2}]})),
        r#"{"a":[{"c":2,"d":1}],"b":1}"#
    );
}

// ------------------------------------------------------------------ end to end

struct Hub {
    service: Arc<HubService>,
    driver: Arc<FakeDriver>,
    client: HubClient,
    directory: tempfile::TempDir,
}

#[cfg(unix)]
async fn hub() -> Hub {
    hub_with(FakeDriver::default()).await
}

async fn hub_with(driver: FakeDriver) -> Hub {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = Arc::new(driver);
    let service = Arc::new(
        HubService::open(
            HubStore::new(directory.path().join("hub")),
            driver.clone(),
            FileTargetRegistry::new(directory.path().join("targets.json")),
        )
        .expect("service"),
    );
    let state = Arc::new(ApiState {
        service: service.clone(),
        token: TOKEN.to_owned(),
        heartbeat: Arc::new(Mutex::new(json!({}))),
        executable: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, router(state)).await });
    let token = directory.path().join("token");
    std::fs::write(&token, TOKEN).expect("token");
    let client = HubClient::new(&Resolved {
        name: "lab".to_owned(),
        url,
        token: TokenRef::File(token),
        source: "flag",
    })
    .expect("client");
    Hub {
        service,
        driver,
        client,
        directory,
    }
}

async fn call(client: &HubClient, command: HubCommand) -> Result<(i32, Vec<u8>), HubError> {
    let mut output = Vec::new();
    let code = execute(client, command, &mut output).await?;
    Ok((code, output))
}

async fn json_call(client: &HubClient, command: HubCommand) -> Value {
    let (_, output) = call(client, command).await.expect("command");
    serde_json::from_slice(&output).expect("json")
}

fn write(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec(value).expect("json")).expect("write");
}

// The judge is a POSIX shell script.
#[cfg(unix)]
#[tokio::test]
async fn eval_runs_baseline_and_variant_through_the_hub_and_reports_deltas() {
    let hub = hub().await;
    let folder = hub.directory.path();
    let (input, runtime, judge) = (
        folder.join("input.json"),
        folder.join("runtime.json"),
        folder.join("judge.sh"),
    );
    write(&input, &json!({"task": "fix the parser"}));
    write(
        &runtime,
        &json!({"model": "big", "nodes": {"plan": {"model": "big"}}}),
    );
    // Scores the variant lower than the baseline, from the request it reads on stdin.
    std::fs::write(
        &judge,
        "#!/bin/sh\nif grep -q '\"variant\":\"small\"'; then s=0.4; else s=0.9; fi\n\
         echo \"{\\\"score\\\": $s, \\\"verdict\\\": \\\"pass\\\", \\\"rationale\\\": \\\"variant scored $s\\\"}\"\n",
    )
    .expect("judge");
    json_call(
        &hub.client,
        HubCommand::Add(HubMemberAdd {
            name: "local".to_owned(),
            tags: Vec::new(),
            slots: 8,
            priority: 0,
        }),
    )
    .await;
    let suite = json_call(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::SuiteAdd {
            name: "parser".to_owned(),
            description: Some("Parser fixes".to_owned()),
        }),
    )
    .await;
    assert_eq!(suite["suite"]["name"], "parser");
    let spec = HubJobSpec {
        title: "Fix parser".to_owned(),
        run_args: [
            "--title".to_owned(),
            "Fix parser".to_owned(),
            "--input".to_owned(),
            input.display().to_string(),
            "--runtime-config".to_owned(),
            runtime.display().to_string(),
        ]
        .to_vec(),
        target: Some("local".to_owned()),
        tags: Vec::new(),
        strategy: HubStrategy::FirstFree,
        labels: vec![("task".to_owned(), "parser".to_owned())],
        runtime_files: vec![runtime.clone()],
        repository: None,
        branch: None,
        revision: None,
        source_connection: None,
        environment: false,
        cwd: None,
        submission_key: None,
    };
    let add_case = |name: &str| {
        HubCommand::Eval(HubEvalCommand::CaseAdd(Box::new(HubEvalCaseAdd {
            suites: vec!["parser".to_owned()],
            name: Some(name.to_owned()),
            labels: Vec::new(),
            source: HubEvalCaseSource::Flags(Box::new(spec.clone())),
        })))
    };
    let case = json_call(&hub.client, add_case("parse-1")).await;
    assert_eq!(case["created"], true);
    let case_id = case["case"]["caseId"].as_str().expect("case id").to_owned();
    assert!(case_id.starts_with("ec_"));
    assert_eq!(case["case"]["models"], json!(["big"]));
    // Capturing the same task again is idempotent.
    let again = json_call(&hub.client, add_case("other-name")).await;
    assert_eq!(again["created"], false);
    assert_eq!(again["case"]["caseId"], case_id.as_str());
    assert_eq!(again["case"]["name"], "parse-1");

    let run_command = || {
        HubCommand::Eval(HubEvalCommand::Run(Box::new(HubEvalRun {
            suite: Some("parser".to_owned()),
            cases: Vec::new(),
            baseline: None,
            variants: vec![HubEvalVariant {
                name: "small".to_owned(),
                overrides: vec![("model".to_owned(), "small".to_owned())],
            }],
            repeat: 2,
            target: None,
            tags: Vec::new(),
            priority: 0,
            judge: Some(format!("cmd:{}", judge.display())),
            judge_mode: HubJudgeMode::Absolute,
            judge_runtime: None,
            checks: vec![(
                "status".to_owned(),
                "test -f status.json && test -f job.json".to_owned(),
            )],
            prices: None,
            key: Some("nightly-1".to_owned()),
        })))
    };
    let run = json_call(&hub.client, run_command()).await;
    let run_id = run["run"]["runId"].as_str().expect("run id").to_owned();
    assert!(run_id.starts_with("er_"));
    assert_eq!(run["run"]["samples"]["total"], 4);
    // The same key returns the same run instead of enqueueing again.
    let repeated = json_call(&hub.client, run_command()).await;
    assert_eq!(repeated["run"]["runId"], run_id.as_str());
    let state = hub.service.snapshot().await;
    assert_eq!(state.jobs.len(), 4);
    let small = state
        .jobs
        .iter()
        .find(|job| job.labels.get("eval.variant").map(String::as_str) == Some("small"))
        .expect("small job");
    assert_eq!(small.models, vec!["small".to_owned()]);
    assert_eq!(small.labels["eval.run"], run_id);
    assert_eq!(small.labels["task"], "parser");
    let runtime_argument = small
        .run_args
        .windows(2)
        .find(|pair| pair[0] == "--runtime-config")
        .map(|pair| pair[1].clone())
        .expect("runtime argument");
    let written: Value =
        serde_json::from_slice(&std::fs::read(&runtime_argument).expect("spooled runtime"))
            .expect("json");
    assert_eq!(written["nodes"]["plan"]["model"], "small");

    // Before anything ran, the report is incomplete: exit 3 with --fail-on-regression.
    let report_command = |table: bool| {
        HubCommand::Eval(HubEvalCommand::Report(HubEvalReport {
            run: run_id.clone(),
            baseline: None,
            thresholds: Vec::new(),
            prices: None,
            fail_on_regression: true,
            wait: None,
            table,
        }))
    };
    let (code, _) = call(&hub.client, report_command(false))
        .await
        .expect("report");
    assert_eq!(code, 3);

    hub.service.pass().await.expect("dispatch");
    hub.driver.finish_all();
    hub.service.pass().await.expect("poll");
    assert_eq!(hub.service.evaluate().await.expect("measure"), 4);
    assert_eq!(hub.service.evaluate().await.expect("judge"), 4);
    assert_eq!(hub.service.evaluate().await.expect("idle"), 0);

    let status = json_call(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::Status {
            run: run_id.clone(),
        }),
    )
    .await;
    assert_eq!(status["run"]["complete"], true);
    let samples = status["samples"].as_array().expect("samples");
    assert!(
        samples.iter().all(|sample| sample["state"] == "judged"),
        "{samples:?}"
    );
    assert!(
        samples
            .iter()
            .all(|sample| sample["checks"][0]["passed"] == true),
        "{samples:?}"
    );
    let small_sample = samples
        .iter()
        .find(|sample| sample["variant"] == "small")
        .expect("small");
    assert_eq!(small_sample["judge"]["score"], 0.4);
    assert_eq!(small_sample["judge"]["rationale"], "variant scored 0.4");
    assert_eq!(small_sample["outcome"]["tokenUsage"]["inputTokens"], 1000);

    let (code, output) = call(&hub.client, report_command(false))
        .await
        .expect("report");
    let document: Value = serde_json::from_slice(&output).expect("json");
    assert_eq!(code, 6, "{document}");
    let small_row = variant_row(&document, "small");
    assert_eq!(small_row["verdict"], "regressed");
    assert_eq!(small_row["delta"]["judgeScore"]["absolute"], -0.5);
    let (_, table) = call(&hub.client, report_command(true))
        .await
        .expect("table");
    let table = String::from_utf8(table).expect("utf8");
    assert!(
        table.contains("small") && table.contains("regressed"),
        "{table}"
    );

    let listed = json_call(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::List {
            suite: Some("parser".to_owned()),
            limit: None,
        }),
    )
    .await;
    assert_eq!(listed["runs"][0]["runId"], run_id.as_str());
    let metrics = hub.service.render_metrics().await;
    assert!(
        metrics.contains("zeroshot_hub_eval_samples{state=\"judged\"} 4"),
        "{metrics}"
    );
    let error = call(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::Status {
            run: "er_missing".to_owned(),
        }),
    )
    .await
    .expect_err("missing run");
    assert_eq!(error.diagnostic()["code"], "hub.eval_not_found");
}

#[tokio::test]
async fn zeroshot_judges_run_as_hub_jobs_and_their_output_is_the_verdict() {
    let hub = hub_with(FakeDriver {
        output: Some(json!({"score": 0.25, "verdict": "better", "rationale": "clearer fix"})),
        ..FakeDriver::default()
    })
    .await;
    let folder = hub.directory.path();
    let (input, graph) = (folder.join("input.json"), folder.join("judge-graph.json"));
    write(&input, &json!({"task": "x"}));
    write(&graph, &json!({"nodes": {"judge": {}}}));
    json_call(
        &hub.client,
        HubCommand::Add(HubMemberAdd {
            name: "local".to_owned(),
            tags: Vec::new(),
            slots: 8,
            priority: 0,
        }),
    )
    .await;
    let mut spec = HubJobSpec {
        title: "Task".to_owned(),
        run_args: vec![
            "--title".to_owned(),
            "Task".to_owned(),
            "--input".to_owned(),
            input.display().to_string(),
        ],
        target: Some("local".to_owned()),
        tags: Vec::new(),
        strategy: HubStrategy::FirstFree,
        labels: Vec::new(),
        runtime_files: Vec::new(),
        repository: None,
        branch: None,
        revision: None,
        source_connection: None,
        environment: false,
        cwd: None,
        submission_key: None,
    };
    spec.labels.push(("task".to_owned(), "judge".to_owned()));
    let case = json_call(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::CaseAdd(Box::new(HubEvalCaseAdd {
            suites: Vec::new(),
            name: None,
            labels: Vec::new(),
            source: HubEvalCaseSource::Flags(Box::new(spec)),
        }))),
    )
    .await;
    let case_id = case["case"]["caseId"].as_str().expect("case").to_owned();
    let run = json_call(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::Run(Box::new(HubEvalRun {
            suite: None,
            cases: vec![case_id],
            baseline: None,
            variants: vec![HubEvalVariant {
                name: "duo".to_owned(),
                overrides: vec![("template".to_owned(), "duo".to_owned())],
            }],
            repeat: 1,
            target: None,
            tags: Vec::new(),
            priority: 0,
            judge: Some(format!("zeroshot:{}", graph.display())),
            judge_mode: HubJudgeMode::Pairwise,
            judge_runtime: None,
            checks: Vec::new(),
            prices: None,
            key: None,
        }))),
    )
    .await;
    let run_id = run["run"]["runId"].as_str().expect("run").to_owned();
    hub.service.pass().await.expect("dispatch");
    hub.driver.finish_all();
    hub.service.pass().await.expect("poll");
    hub.service.evaluate().await.expect("measure");
    // The baseline is the pairwise reference; the variant gets a judge job.
    hub.service.evaluate().await.expect("start judge");
    let state = hub.service.snapshot().await;
    let judge_job = state
        .jobs
        .iter()
        .find(|job| job.labels.get("eval.role").map(String::as_str) == Some("judge"))
        .expect("judge job");
    assert_eq!(judge_job.labels["eval.variant"], "duo");
    assert!(judge_job.run_args.contains(&"--graph".to_owned()));
    let request_path = judge_job
        .run_args
        .windows(2)
        .find(|pair| pair[0] == "--input")
        .map(|pair| pair[1].clone())
        .expect("judge input");
    let request: Value =
        serde_json::from_slice(&std::fs::read(request_path).expect("request")).expect("json");
    assert_eq!(request["schema"], "zeroshot.hub.eval.judge-request/v1");
    assert_eq!(request["mode"], "pairwise");
    assert_eq!(request["candidate"]["variant"], "duo");
    assert_eq!(request["baseline"]["variant"], "baseline");
    hub.service.pass().await.expect("dispatch judge");
    hub.driver.finish_all();
    hub.service.pass().await.expect("poll judge");
    hub.service.evaluate().await.expect("settle");
    let status = hub.service.eval_run(&run_id).await.expect("status");
    let duo = status["samples"]
        .as_array()
        .expect("samples")
        .iter()
        .find(|sample| sample["variant"] == "duo")
        .expect("duo");
    assert_eq!(duo["state"], "judged");
    assert_eq!(duo["judge"]["score"], 0.25);
    assert_eq!(duo["judge"]["verdict"], "better");
    assert_eq!(duo["judgeJob"], judge_job.job_id.as_str());
    let report = hub
        .service
        .eval_report(&run_id, super::super::eval::ReportRequest::default())
        .await
        .expect("report");
    assert_eq!(report["complete"], true);
    let row = variant_row(&report, "duo");
    assert_eq!(row["delta"]["judgeScore"]["absolute"], 0.25);
    assert_eq!(row["verdict"], "improved");
}

#[test]
fn materialized_blobs_are_replaced_atomically_and_never_rewritten_in_place() {
    let directory = tempfile::tempdir().expect("tempdir");
    let blobs = super::super::eval::Blobs::new(directory.path());
    let folder = directory.path().join("programs");
    let first = blobs
        .put("judge.sh", b"#!/bin/sh\necho first\n")
        .expect("put");
    let path = blobs.materialize(&first, &folder).expect("materialize");
    assert_eq!(path, folder.join("judge.sh"));

    // A parallel sample materializing the same blob leaves the running copy alone.
    let before = std::fs::metadata(&path).expect("metadata").modified().ok();
    blobs.materialize(&first, &folder).expect("again");
    assert_eq!(
        std::fs::metadata(&path).expect("metadata").modified().ok(),
        before
    );

    // Different content replaces the file by rename: a reader that opened the old file still
    // sees all of its bytes instead of a truncated file. (Windows refuses to rename over an
    // open file, so only Unix keeps the reader open.)
    #[cfg(unix)]
    let mut running = std::fs::File::open(&path).expect("open");
    let second = blobs
        .put("judge.sh", b"#!/bin/sh\necho second\n")
        .expect("put");
    blobs.materialize(&second, &folder).expect("replace");
    #[cfg(unix)]
    {
        use std::io::Read;
        let mut old = String::new();
        running.read_to_string(&mut old).expect("read");
        assert_eq!(old, "#!/bin/sh\necho first\n");
    }
    assert_eq!(
        std::fs::read_to_string(&path).expect("read"),
        "#!/bin/sh\necho second\n"
    );
    let names: Vec<_> = std::fs::read_dir(&folder)
        .expect("list")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(names, vec![std::ffi::OsString::from("judge.sh")]);
}

#[test]
fn judge_text_is_cut_on_character_boundaries() {
    use super::super::eval::judge::{tail, truncate};
    assert_eq!(truncate("short", 10), "short");
    assert_eq!(
        truncate("ab\u{e9}cd", 3),
        "ab…",
        "never split the two-byte é"
    );
    assert_eq!(truncate("abcdef", 4), "abcd…");
    assert_eq!(tail("short", 10), "short");
    assert_eq!(tail("ab\u{e9}cd", 3), "…cd", "never split the two-byte é");
    assert_eq!(tail("abcdef", 2), "…ef");
}

#[tokio::test]
async fn host_commands_report_programs_that_cannot_start_or_finish() {
    use super::super::eval::judge::HostCommand;
    let directory = tempfile::tempdir().expect("tempdir");
    let absent = directory.path().join("absent-judge");
    let finished = HostCommand {
        program: &absent,
        arguments: &[],
        cwd: directory.path(),
        env: &[],
    }
    .run(b"{}", std::time::Duration::from_secs(5))
    .await;
    assert_eq!(finished.exit_code(), None);
    assert!(
        matches!(&finished.ended, Ended::NotStarted(reason) if reason.contains("absent-judge could not start")),
        "{:?}",
        finished.ended
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let slow = directory.path().join("slow.sh");
        std::fs::write(&slow, "#!/bin/sh\nexec sleep 30\n").expect("script");
        std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        let started = std::time::Instant::now();
        let finished = HostCommand {
            program: &slow,
            arguments: &[],
            cwd: directory.path(),
            env: &[],
        }
        .run(b"", std::time::Duration::from_millis(200))
        .await;
        assert_eq!(finished.exit_code(), None);
        assert_eq!(finished.ended.describe(), "timed out after 0s");
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
    }
}

/// A program still open for writing cannot be executed (`ETXTBSY`); the judge retries briefly
/// and then reports the failure instead of hanging.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn host_commands_retry_then_report_a_program_held_open_for_writing() {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    use super::super::eval::judge::HostCommand;
    let directory = tempfile::tempdir().expect("tempdir");
    let busy = directory.path().join("busy.sh");
    let mut writer = std::fs::File::create(&busy).expect("create");
    writer.write_all(b"#!/bin/sh\necho never\n").expect("write");
    std::fs::set_permissions(&busy, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    let started = std::time::Instant::now();
    let finished = HostCommand {
        program: &busy,
        arguments: &[],
        cwd: directory.path(),
        env: &[],
    }
    .run(b"", std::time::Duration::from_secs(5))
    .await;
    assert!(
        finished.ended.describe().contains("could not start"),
        "{:?}",
        finished.ended
    );
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(150),
        "busy programs are retried before giving up"
    );
    drop(writer);
}

/// Add `local`, one flags case, and a zeroshot-judged run with `variants`; returns the run id.
async fn zeroshot_judged_run(
    hub: &Hub,
    variants: &[&str],
    judge_runtime: Option<std::path::PathBuf>,
) -> String {
    let folder = hub.directory.path();
    let (input, graph) = (folder.join("input.json"), folder.join("judge-graph.json"));
    write(&input, &json!({"task": "x"}));
    write(&graph, &json!({"nodes": {"judge": {}}}));
    json_call(
        &hub.client,
        HubCommand::Add(HubMemberAdd {
            name: "local".to_owned(),
            tags: Vec::new(),
            slots: 8,
            priority: 0,
        }),
    )
    .await;
    let spec = HubJobSpec {
        title: "Task".to_owned(),
        run_args: vec![
            "--title".to_owned(),
            "Task".to_owned(),
            "--input".to_owned(),
            input.display().to_string(),
        ],
        target: Some("local".to_owned()),
        tags: Vec::new(),
        strategy: HubStrategy::FirstFree,
        labels: Vec::new(),
        runtime_files: Vec::new(),
        repository: None,
        branch: None,
        revision: None,
        source_connection: None,
        environment: false,
        cwd: None,
        submission_key: None,
    };
    let case = json_call(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::CaseAdd(Box::new(HubEvalCaseAdd {
            suites: Vec::new(),
            name: None,
            labels: Vec::new(),
            source: HubEvalCaseSource::Flags(Box::new(spec)),
        }))),
    )
    .await;
    let case_id = case["case"]["caseId"].as_str().expect("case").to_owned();
    let run = json_call(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::Run(Box::new(HubEvalRun {
            suite: None,
            cases: vec![case_id],
            baseline: None,
            variants: variants
                .iter()
                .map(|name| HubEvalVariant {
                    name: (*name).to_owned(),
                    overrides: vec![("template".to_owned(), (*name).to_owned())],
                })
                .collect(),
            repeat: 1,
            target: None,
            tags: Vec::new(),
            priority: 0,
            judge: Some(format!("zeroshot:{}", graph.display())),
            judge_mode: HubJudgeMode::Absolute,
            judge_runtime,
            checks: Vec::new(),
            prices: None,
            key: None,
        }))),
    )
    .await;
    run["run"]["runId"].as_str().expect("run").to_owned()
}

#[tokio::test]
async fn zeroshot_judge_jobs_that_fail_or_answer_without_a_document_settle_with_errors() {
    use super::super::model::JobState;
    let hub = hub_with(FakeDriver::default()).await;
    let runtime = hub.directory.path().join("judge-runtime.json");
    write(&runtime, &json!({"providers": {}}));
    let run_id = zeroshot_judged_run(&hub, &["duo", "solo"], Some(runtime)).await;
    hub.service.pass().await.expect("dispatch");
    hub.driver.finish_all();
    hub.service.pass().await.expect("poll");
    hub.service.evaluate().await.expect("measure");
    hub.service.evaluate().await.expect("start judges");
    // A second claim sees the judge jobs already queued and only records them.
    hub.service.evaluate().await.expect("judges already queued");
    let judges = hub
        .service
        .snapshot()
        .await
        .jobs
        .into_iter()
        .filter(|job| job.labels.get("eval.role").map(String::as_str) == Some("judge"))
        .collect::<Vec<_>>();
    assert_eq!(judges.len(), 3, "absolute judging scores every variant");
    for judge in &judges {
        assert!(
            judge
                .run_args
                .contains(&"--uniform-runtime-config".to_owned()),
            "{:?}",
            judge.run_args
        );
    }
    hub.service.pass().await.expect("dispatch judges");
    hub.driver.finish_all();
    hub.service.pass().await.expect("poll judges");
    hub.service
        .mutate(|state| {
            for job in &mut state.jobs {
                match job.labels.get("eval.variant").map(String::as_str) {
                    _ if job.labels.get("eval.role").map(String::as_str) != Some("judge") => {}
                    Some("baseline") => {
                        job.output = Some(json!("{\"score\": 0.5, \"verdict\": \"fine\"}"));
                    }
                    Some("duo") => job.output = None,
                    _ => {
                        job.state = JobState::Failed;
                        job.failure_reason = Some("judge graph crashed".to_owned());
                    }
                }
            }
            Ok(())
        })
        .await
        .expect("shape judge results");
    hub.service.evaluate().await.expect("settle");
    let status = hub.service.eval_run(&run_id).await.expect("status");
    let sample = |variant: &str| {
        status["samples"]
            .as_array()
            .expect("samples")
            .iter()
            .find(|sample| sample["variant"] == variant)
            .cloned()
            .expect("sample")
    };
    let baseline = sample("baseline");
    assert_eq!(baseline["judge"]["score"], 0.5, "{baseline}");
    assert_eq!(baseline["judge"]["verdict"], "fine");
    let duo = sample("duo");
    assert!(duo.to_string().contains("finished without output"), "{duo}");
    let solo = sample("solo");
    assert!(solo.to_string().contains("judge graph crashed"), "{solo}");
}

#[tokio::test]
async fn samples_whose_hub_job_vanished_fail_instead_of_waiting() {
    let hub = hub_with(FakeDriver::default()).await;
    let run_id = zeroshot_judged_run(&hub, &["duo"], None).await;
    hub.service
        .mutate(|state| {
            state
                .jobs
                .retain(|job| job.labels.get("eval.variant").map(String::as_str) != Some("duo"));
            Ok(())
        })
        .await
        .expect("drop a sample job");
    hub.service.evaluate().await.expect("claim");
    let status = hub.service.eval_run(&run_id).await.expect("status");
    let duo = status["samples"]
        .as_array()
        .expect("samples")
        .iter()
        .find(|sample| sample["variant"] == "duo")
        .cloned()
        .expect("duo");
    assert_eq!(duo["state"], "failed", "{duo}");
    assert!(duo.to_string().contains("no longer exists"), "{duo}");
}

/// The daemon claims eval work beside dispatching and runs it in the background.
#[tokio::test]
async fn the_daemon_measures_finished_eval_samples() {
    use zeroshot_engine::native_v2_cli::hub::{HubApiOptions, HubDaemonRun};
    let hub = hub_with(FakeDriver::default()).await;
    let run_id = zeroshot_judged_run(&hub, &["duo"], None).await;
    hub.service.pass().await.expect("dispatch");
    hub.driver.finish_all();
    hub.service.pass().await.expect("poll");
    let folder = hub.directory.path().to_path_buf();
    let store = HubStore::new(folder.join("hub"));
    let options = HubDaemonRun {
        interval: "1s".to_owned(),
        api: HubApiOptions {
            listen: Some("127.0.0.1:0".to_owned()),
            token_file: Some(folder.join("daemon-token")),
            metrics_listen: None,
            metrics_labels: Vec::new(),
            unconfirmed_limit: None,
        },
    };
    let open = |store| {
        HubService::open(
            store,
            Arc::new(FakeDriver::default()),
            FileTargetRegistry::new(folder.join("targets.json")),
        )
    };
    let daemon = super::super::daemon::Daemon {
        store: store.clone(),
        options: &options,
        output: &mut Vec::new(),
    };
    super::super::daemon::run_until(daemon, open, async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    })
    .await
    .expect("daemon");
    let reopened = HubService::open(
        store,
        Arc::new(FakeDriver::default()),
        FileTargetRegistry::new(folder.join("targets.json")),
    )
    .expect("reopen");
    let status = reopened.eval_run(&run_id).await.expect("status");
    let states = status["samples"]
        .as_array()
        .expect("samples")
        .iter()
        .map(|sample| sample["state"].as_str().unwrap_or_default().to_owned())
        .collect::<Vec<_>>();
    assert!(
        states.iter().all(|state| state != "pending"),
        "the daemon measured every finished sample: {states:?}"
    );
}

/// Write `body` as an executable POSIX shell script in `folder`.
#[cfg(unix)]
fn script(folder: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = folder.join(name);
    std::fs::write(&path, body).expect("script");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    path
}

/// More input than any pipe buffers, so a command that stops reading blocks the writer.
#[cfg(unix)]
fn large_request() -> Vec<u8> {
    let mut request = b"{\"mode\":\"absolute\",\"padding\":\"".to_vec();
    request.extend(std::iter::repeat_n(b'x', 4 << 20));
    request.extend_from_slice(b"\"}\n");
    request
}

#[cfg(unix)]
#[tokio::test]
async fn a_judge_that_never_reads_its_input_ends_by_the_timeout() {
    let directory = tempfile::tempdir().expect("tempdir");
    let judge = script(directory.path(), "deaf.sh", "#!/bin/sh\nexec sleep 30\n");
    let started = std::time::Instant::now();
    let finished = HostCommand {
        program: &judge,
        arguments: &[],
        cwd: directory.path(),
        env: &[],
    }
    .run(&large_request(), std::time::Duration::from_millis(500))
    .await;
    assert_eq!(
        finished.ended,
        Ended::TimedOut(std::time::Duration::from_millis(500))
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the input is written within the timeout, not before it"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_judge_that_answers_without_reading_all_its_input_is_not_accepted() {
    let directory = tempfile::tempdir().expect("tempdir");
    let judge = script(
        directory.path(),
        "hasty.sh",
        "#!/bin/sh\nhead -c 16 >/dev/null\necho '{\"score\": 1}'\n",
    );
    let finished = HostCommand {
        program: &judge,
        arguments: &[],
        cwd: directory.path(),
        env: &[],
    }
    .run(&large_request(), std::time::Duration::from_secs(30))
    .await;
    assert!(
        matches!(finished.ended, Ended::IncompleteInput(_)),
        "{:?}",
        finished.ended
    );
    assert_eq!(finished.exit_code(), None, "its exit code is not trusted");
    assert!(
        finished
            .ended
            .describe()
            .starts_with("did not read all of its input"),
        "{}",
        finished.ended.describe()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn host_command_output_is_capped_while_it_is_read() {
    use super::super::eval::judge::OUTPUT_LIMIT;
    let directory = tempfile::tempdir().expect("tempdir");
    let chatty = script(
        directory.path(),
        "chatty.sh",
        "#!/bin/sh\nhead -c 3000000 /dev/zero | tr '\\0' a\necho\necho '{\"score\": 0.5}'\n\
         head -c 3000000 /dev/zero >&2\n",
    );
    let finished = HostCommand {
        program: &chatty,
        arguments: &[],
        cwd: directory.path(),
        env: &[],
    }
    .run(b"", std::time::Duration::from_secs(30))
    .await;
    assert_eq!(finished.ended, Ended::Exited(Some(0)));
    assert!(finished.stdout.len() <= OUTPUT_LIMIT);
    assert!(finished.stderr.len() <= OUTPUT_LIMIT);
    assert_eq!(
        parse_reply(&finished.stdout, JudgeMode::Absolute).score,
        Some(Score(0.5)),
        "the reply at the end of the output is kept"
    );
}

#[test]
fn checks_the_hub_could_not_run_leave_the_sample_unmeasured() {
    use super::super::eval::worker::check_result;
    let finished = |ended: Ended| Finished {
        ended,
        stdout: "out\n".to_owned(),
        stderr: String::new(),
    };
    let passed = check_result("tests", &finished(Ended::Exited(Some(0))));
    assert!(passed.passed && passed.error.is_none());
    assert_eq!(passed.exit_code, Some(0));
    assert_eq!(passed.output, "out\n");
    let failed = check_result("tests", &finished(Ended::Exited(Some(1))));
    assert!(!failed.passed && failed.error.is_none());
    let killed = check_result("tests", &finished(Ended::Exited(None)));
    assert_eq!((killed.passed, killed.exit_code), (false, None));
    let slow = check_result(
        "tests",
        &finished(Ended::TimedOut(std::time::Duration::from_secs(600))),
    );
    assert!(
        !slow.passed && slow.error.is_none(),
        "a hung check fails the candidate"
    );
    assert_eq!(slow.output, "out\ntimed out after 600s");
    let unrun = check_result(
        "tests",
        &finished(Ended::NotStarted("sh could not start".to_owned())),
    );
    assert!(!unrun.passed);
    assert_eq!(unrun.error.as_deref(), Some("sh could not start"));

    let mut sample = sample("a", "fast", 0, (true, 10, 100, None));
    sample.checks = vec![passed.clone()];
    assert_eq!(sample.passed(None), Some(true));
    sample.checks = vec![passed.clone(), failed];
    assert_eq!(sample.passed(None), Some(false));
    sample.checks = vec![passed, unrun];
    assert_eq!(
        sample.passed(None),
        None,
        "a hub-side failure is not a candidate failure"
    );
}
