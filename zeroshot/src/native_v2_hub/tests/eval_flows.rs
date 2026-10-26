//! Eval suites, cases, runs, and reports through the hub API, including refusals.

use std::path::Path;

use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::{
    HubCommand, HubEvalCaseAdd, HubEvalCaseSource, HubEvalCommand, HubEvalReport, HubEvalRun,
    HubEvalVariant, HubJudgeMode, HubMemberAdd, HubSubmit,
};

use super::super::client::execute;
use super::super::error::HubError;
use super::FakeDriver;
use super::flows::{Hub, hub_with, spec};

async fn call(hub: &Hub, command: HubEvalCommand) -> Result<Value, HubError> {
    let mut output = Vec::new();
    execute(&hub.client, HubCommand::Eval(command), &mut output).await?;
    Ok(serde_json::from_slice(&output).expect("json"))
}

fn run_request(suite: Option<&str>, cases: &[&str]) -> HubEvalRun {
    HubEvalRun {
        suite: suite.map(str::to_owned),
        cases: cases.iter().map(|case| (*case).to_owned()).collect(),
        baseline: None,
        variants: Vec::new(),
        repeat: 1,
        target: None,
        tags: Vec::new(),
        priority: 0,
        judge: None,
        judge_mode: HubJudgeMode::Absolute,
        judge_runtime: None,
        checks: Vec::new(),
        prices: None,
        key: None,
    }
}

fn variant(name: &str, overrides: &[(&str, &str)]) -> HubEvalVariant {
    HubEvalVariant {
        name: name.to_owned(),
        overrides: overrides
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    }
}

async fn refused(hub: &Hub, run: HubEvalRun, expected: &str) {
    let error = call(hub, HubEvalCommand::Run(Box::new(run)))
        .await
        .expect_err(expected);
    assert!(error.to_string().contains(expected), "{expected}: {error}");
}

fn case_add(folder: &Path, suites: &[&str], name: &str, title: &str) -> HubEvalCommand {
    HubEvalCommand::CaseAdd(Box::new(HubEvalCaseAdd {
        suites: suites.iter().map(|suite| (*suite).to_owned()).collect(),
        name: Some(name.to_owned()),
        labels: vec![("eval.reserved".to_owned(), "x".to_owned())],
        source: HubEvalCaseSource::Flags(Box::new(spec(folder, title, None))),
    }))
}

#[tokio::test]
async fn suites_and_cases_are_created_listed_and_shown() {
    let hub = hub_with(FakeDriver::default(), None).await;
    let folder = hub.directory.path().to_path_buf();
    let suite = |name: &str, description: Option<&str>| HubEvalCommand::SuiteAdd {
        name: name.to_owned(),
        description: description.map(str::to_owned),
    };
    call(&hub, suite("parser", Some("first")))
        .await
        .expect("suite");
    let kept = call(&hub, suite("parser", None)).await.expect("again");
    assert_eq!(kept["suite"]["description"], json!("first"));
    let renamed = call(&hub, suite("parser", Some("second")))
        .await
        .expect("describe");
    assert_eq!(renamed["suite"]["description"], json!("second"));
    assert!(matches!(
        call(&hub, suite("no spaces", None)).await,
        Err(HubError::Remote { .. })
    ));
    call(&hub, suite("other", None)).await.expect("other");

    let case = call(&hub, case_add(&folder, &["parser"], "one", "One"))
        .await
        .expect("case");
    let case_id = case["case"]["caseId"].as_str().expect("id").to_owned();
    assert_eq!(case["case"]["labels"]["team"], json!("flows"), "{case}");
    call(&hub, case_add(&folder, &[], "two", "Two"))
        .await
        .expect("loose case");
    let missing_suite = call(&hub, case_add(&folder, &["absent"], "three", "Three"))
        .await
        .expect_err("absent suite");
    assert!(
        missing_suite
            .to_string()
            .contains("eval suite absent does not exist")
    );

    // Capture a hub job as a case: its input is read on the hub host.
    let mut output = Vec::new();
    execute(
        &hub.client,
        HubCommand::Add(HubMemberAdd {
            name: "local".to_owned(),
            tags: Vec::new(),
            slots: 1,
            priority: 0,
        }),
        &mut output,
    )
    .await
    .expect("member");
    let mut output = Vec::new();
    execute(
        &hub.client,
        HubCommand::Submit(Box::new(HubSubmit {
            job: spec(&folder, "captured", None),
            detach: true,
            wait: None,
            queue: true,
            expires: None,
        })),
        &mut output,
    )
    .await
    .expect("job");
    let job: Value = serde_json::from_slice(&output).expect("json");
    let job_id = job["job"]["jobId"].as_str().expect("job id").to_owned();
    let captured = call(
        &hub,
        HubEvalCommand::CaseAdd(Box::new(HubEvalCaseAdd {
            suites: vec!["other".to_owned()],
            name: None,
            labels: vec![("origin".to_owned(), "job".to_owned())],
            source: HubEvalCaseSource::FromJob(job_id.clone()),
        })),
    )
    .await
    .expect("from job");
    assert_eq!(captured["case"]["sourceJob"], json!(job_id));
    let missing_job = call(
        &hub,
        HubEvalCommand::CaseAdd(Box::new(HubEvalCaseAdd {
            suites: Vec::new(),
            name: None,
            labels: Vec::new(),
            source: HubEvalCaseSource::FromJob("hj_absent".to_owned()),
        })),
    )
    .await
    .expect_err("absent job");
    assert_eq!(missing_job.diagnostic()["code"], json!("hub.job_not_found"));

    let suites = call(&hub, HubEvalCommand::SuiteList).await.expect("suites");
    assert_eq!(suites["suites"].as_array().map(Vec::len), Some(2));
    let all = call(&hub, HubEvalCommand::CaseList { suite: None })
        .await
        .expect("cases");
    assert_eq!(all["cases"].as_array().map(Vec::len), Some(3));
    let parser = call(
        &hub,
        HubEvalCommand::CaseList {
            suite: Some("parser".to_owned()),
        },
    )
    .await
    .expect("parser cases");
    assert_eq!(parser["cases"][0]["caseId"], json!(case_id));
    assert_eq!(parser["cases"][0]["suites"], json!(["parser"]));
    assert!(
        call(
            &hub,
            HubEvalCommand::CaseList {
                suite: Some("absent".to_owned()),
            },
        )
        .await
        .is_err()
    );
    let shown = call(
        &hub,
        HubEvalCommand::CaseShow {
            case: "one".to_owned(),
        },
    )
    .await
    .expect("show by name");
    assert_eq!(shown["case"]["caseId"], json!(case_id));
    assert!(
        call(
            &hub,
            HubEvalCommand::CaseShow {
                case: "ec_absent".to_owned(),
            },
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn run_requests_are_validated_before_anything_is_enqueued() {
    let hub = hub_with(FakeDriver::default(), None).await;
    let folder = hub.directory.path().to_path_buf();
    call(
        &hub,
        HubEvalCommand::SuiteAdd {
            name: "parser".to_owned(),
            description: None,
        },
    )
    .await
    .expect("suite");
    call(&hub, case_add(&folder, &["parser"], "one", "One"))
        .await
        .expect("one");
    call(&hub, case_add(&folder, &["parser"], "two", "Two"))
        .await
        .expect("two");

    let mut zero = run_request(Some("parser"), &[]);
    zero.repeat = 0;
    refused(&hub, zero, "--repeat must be").await;
    refused(
        &hub,
        run_request(Some("absent"), &[]),
        "eval suite absent does not exist",
    )
    .await;
    refused(
        &hub,
        run_request(None, &["ghost"]),
        "eval case ghost does not exist",
    )
    .await;
    refused(&hub, run_request(None, &[]), "selects no cases").await;
    let mut named = run_request(Some("parser"), &[]);
    named.variants = vec![variant("bad name", &[])];
    refused(&hub, named, "invalid variant name").await;
    let mut twice = run_request(Some("parser"), &[]);
    twice.variants = vec![variant("fast", &[]), variant("fast", &[])];
    refused(&hub, twice, "is given twice").await;
    let mut unknown = run_request(Some("parser"), &[]);
    unknown.variants = vec![variant("fast", &[("colour", "red")])];
    refused(&hub, unknown, "unknown variant key").await;
    let mut absent_file = run_request(Some("parser"), &[]);
    absent_file.variants = vec![variant(
        "fast",
        &[(
            "runtime-config",
            folder.join("absent.json").to_str().expect("utf8"),
        )],
    )];
    refused(&hub, absent_file, "could not be read").await;

    let mut job_baseline = run_request(Some("parser"), &[]);
    job_baseline.baseline = Some("hj_anything".to_owned());
    refused(&hub, job_baseline, "select exactly one case").await;
    let mut missing_job = run_request(None, &["one"]);
    missing_job.baseline = Some("hj_absent".to_owned());
    refused(&hub, missing_job, "hj_absent").await;
    let mut stranger = run_request(Some("parser"), &[]);
    stranger.baseline = Some("stranger".to_owned());
    refused(&hub, stranger, "is not a variant of this run").await;

    let judge = folder.join("judge.sh");
    std::fs::write(&judge, "#!/bin/sh\necho '{\"score\": 1}'\n").expect("judge");
    let mut pairwise = run_request(Some("parser"), &[]);
    pairwise.baseline = Some("none".to_owned());
    pairwise.judge = Some(format!("cmd:{}", judge.display()));
    pairwise.judge_mode = HubJudgeMode::Pairwise;
    refused(&hub, pairwise, "a pairwise judge needs a baseline").await;
    let mut kind = run_request(Some("parser"), &[]);
    kind.judge = Some(format!("python:{}", judge.display()));
    refused(&hub, kind, "must be cmd:SCRIPT or zeroshot:GRAPH").await;
    let mut check = run_request(Some("parser"), &[]);
    check.checks = vec![("tests".to_owned(), "   ".to_owned())];
    refused(&hub, check, "invalid --check").await;
    let prices = folder.join("prices.json");
    std::fs::write(&prices, "[1]").expect("prices");
    let mut priced = run_request(Some("parser"), &[]);
    priced.prices = Some(prices);
    refused(&hub, priced, "price table").await;
    assert!(
        hub.service.snapshot().await.jobs.is_empty(),
        "nothing was enqueued"
    );

    // A valid run with placement, a file override, and a judge runtime is accepted.
    let runtime = folder.join("runtime.json");
    std::fs::write(&runtime, "{\"model\": \"small\"}").expect("runtime");
    let mut accepted = run_request(None, &["one"]);
    accepted.baseline = Some("fast".to_owned());
    accepted.variants = vec![
        variant(
            "fast",
            &[("runtime-config", runtime.to_str().expect("utf8"))],
        ),
        variant("slow", &[]),
    ];
    accepted.tags = vec!["linux".to_owned()];
    accepted.judge = Some(format!("cmd:{}", judge.display()));
    accepted.judge_runtime = Some(runtime.clone());
    refused(
        &hub,
        accepted.clone(),
        "no active hub member matches tags [linux]",
    )
    .await;
    execute(
        &hub.client,
        HubCommand::Add(HubMemberAdd {
            name: "local".to_owned(),
            tags: vec!["linux".to_owned()],
            slots: 1,
            priority: 0,
        }),
        &mut Vec::new(),
    )
    .await
    .expect("member");
    let run = call(&hub, HubEvalCommand::Run(Box::new(accepted)))
        .await
        .expect("accepted");
    let run_id = run["run"]["runId"].as_str().expect("run id").to_owned();
    assert_eq!(run["run"]["samples"]["total"], json!(2), "{run}");

    let none = {
        let mut request = run_request(Some("parser"), &[]);
        request.baseline = Some("none".to_owned());
        request
    };
    let unbased = call(&hub, HubEvalCommand::Run(Box::new(none)))
        .await
        .expect("no baseline");
    assert_eq!(unbased["run"]["samples"]["total"], json!(2), "{unbased}");

    let listed = call(
        &hub,
        HubEvalCommand::List {
            suite: Some("parser".to_owned()),
            limit: Some(5),
        },
    )
    .await
    .expect("list");
    assert_eq!(listed["runs"].as_array().map(Vec::len), Some(1));
    let last = call(
        &hub,
        HubEvalCommand::List {
            suite: None,
            limit: Some(1),
        },
    )
    .await
    .expect("list one");
    assert_eq!(last["runs"].as_array().map(Vec::len), Some(1));
    assert!(
        call(
            &hub,
            HubEvalCommand::Status {
                run: "er_absent".to_owned(),
            },
        )
        .await
        .is_err()
    );

    // Reports compare against another run, a variant, or refuse unknown references.
    let report = |baseline: Option<&str>| {
        HubEvalCommand::Report(HubEvalReport {
            run: run_id.clone(),
            baseline: baseline.map(str::to_owned),
            thresholds: Vec::new(),
            prices: None,
            fail_on_regression: false,
            wait: baseline.is_none().then(|| Some("1s".to_owned())),
            table: false,
        })
    };
    let other = unbased["run"]["runId"].as_str().expect("other").to_owned();
    let against_run = call(&hub, report(Some(&format!("{other}:baseline"))))
        .await
        .expect("against run");
    assert_eq!(
        against_run["baseline"]["runId"],
        json!(other),
        "{against_run}"
    );
    let against_variant = call(&hub, report(Some("slow"))).await.expect("variant");
    let waited = call(&hub, report(None)).await.expect("waited");
    assert_eq!(
        waited["complete"],
        json!(false),
        "nothing ran within the wait"
    );
    assert_eq!(against_variant["baseline"]["variant"], json!("slow"));
    for (reference, expected) in [
        ("er_absent".to_owned(), "does not exist"),
        (format!("{other}:missing"), "has no missing samples"),
        ("missing".to_owned(), "is not a variant of"),
    ] {
        let error = call(&hub, report(Some(&reference)))
            .await
            .expect_err(expected);
        assert!(error.to_string().contains(expected), "{reference}: {error}");
    }
    let mut table = Vec::new();
    execute(
        &hub.client,
        HubCommand::Eval(HubEvalCommand::Report(HubEvalReport {
            run: run_id.clone(),
            baseline: None,
            thresholds: Vec::new(),
            prices: Some(folder.join("absent-prices.json")),
            fail_on_regression: true,
            wait: None,
            table: true,
        })),
        &mut table,
    )
    .await
    .expect_err("absent price file");
}
