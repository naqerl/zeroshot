//! `zeroshot hub eval ...` on the client: build requests (reading every local file the
//! request names, so a remote hub needs none of them) and turn reports into exit codes.

use std::io::Write;
use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::{
    HubEvalCaseSource, HubEvalCommand, HubEvalReport, HubEvalRun, HubJudgeMode,
};

use super::super::client::{HubClient, job_files};
use super::super::clock::{now_ms, parse_duration_ms};
use super::super::error::{EXIT_NO_CAPACITY, EXIT_REGRESSION, HubError};
use super::super::jobs;
use super::super::write_json;
use super::report;
use super::{
    AddCase, AddSuite, CaptureSource, CaseQuery, Check, CreateRun, JudgeKind, JudgeMode,
    JudgeRequest, OverrideRequest, ReportRequest, RunQuery, Upload, VariantRequest, variant,
};

const REPORT_POLL: Duration = Duration::from_secs(5);
const UPLOAD_LIMIT: u64 = 8 * 1024 * 1024;

fn upload(path: &Path, what: &str) -> Result<Upload, HubError> {
    let size = std::fs::metadata(path)
        .map_err(|error| {
            HubError::Usage(format!(
                "{what} {} could not be read: {error}",
                path.display()
            ))
        })?
        .len();
    if size > UPLOAD_LIMIT {
        return Err(HubError::Usage(format!(
            "{what} {} is larger than {} MiB",
            path.display(),
            UPLOAD_LIMIT / 1024 / 1024
        )));
    }
    let content = std::fs::read_to_string(path).map_err(|error| {
        HubError::Usage(format!(
            "{what} {} could not be read: {error}",
            path.display()
        ))
    })?;
    let name = path.file_name().map_or_else(
        || "file".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    );
    Ok(Upload { name, content })
}

fn json_file(path: &Path, what: &str) -> Result<Value, HubError> {
    serde_json::from_str(&upload(path, what)?.content)
        .map_err(|error| HubError::Usage(format!("{what} {} is not JSON: {error}", path.display())))
}

pub(crate) fn run_request(run: HubEvalRun) -> Result<CreateRun, HubError> {
    let variants = run
        .variants
        .into_iter()
        .map(|requested| {
            let overrides = requested
                .overrides
                .into_iter()
                .map(|(key, value)| {
                    let upload = variant::is_file_key(&key)
                        .then(|| {
                            upload(
                                Path::new(&value),
                                &format!("variant {} {key}", requested.name),
                            )
                        })
                        .transpose()?;
                    Ok(OverrideRequest { key, value, upload })
                })
                .collect::<Result<Vec<_>, HubError>>()?;
            Ok(VariantRequest {
                name: requested.name,
                overrides,
            })
        })
        .collect::<Result<Vec<_>, HubError>>()?;
    let judge = run
        .judge
        .as_deref()
        .map(|judge| {
            let (kind, path) = if let Some(path) = judge.strip_prefix("cmd:") {
                (JudgeKind::Cmd, path)
            } else if let Some(path) = judge.strip_prefix("zeroshot:") {
                (JudgeKind::Zeroshot, path)
            } else {
                return Err(HubError::Usage(format!(
                    "--judge {judge:?} must be cmd:SCRIPT or zeroshot:GRAPH"
                )));
            };
            Ok(JudgeRequest {
                kind,
                mode: match run.judge_mode {
                    HubJudgeMode::Absolute => JudgeMode::Absolute,
                    HubJudgeMode::Pairwise => JudgeMode::Pairwise,
                },
                program: upload(Path::new(path), "--judge")?,
                runtime: run
                    .judge_runtime
                    .as_deref()
                    .map(|path| upload(path, "--judge-runtime"))
                    .transpose()?,
            })
        })
        .transpose()?;
    Ok(CreateRun {
        key: run.key,
        suite: run.suite,
        cases: run.cases,
        baseline: run.baseline,
        variants,
        repeat: run.repeat,
        target: run.target,
        tags: run.tags,
        priority: run.priority,
        judge,
        checks: run
            .checks
            .into_iter()
            .map(|(name, command)| Check { name, command })
            .collect(),
        prices: run
            .prices
            .as_deref()
            .map(|path| json_file(path, "--prices"))
            .transpose()?,
    })
}

/// Exit code of a report: 3 while incomplete, 6 on a regression (only with
/// `--fail-on-regression`), else 0.
pub(crate) fn report_exit_code(report: &Value, fail_on_regression: bool) -> i32 {
    if !fail_on_regression {
        return 0;
    }
    if report["complete"] != json!(true) {
        EXIT_NO_CAPACITY
    } else if report["regressed"] == json!(true) {
        EXIT_REGRESSION
    } else {
        0
    }
}

async fn report(
    client: &HubClient,
    request: HubEvalReport,
    output: &mut impl Write,
) -> Result<i32, HubError> {
    let body = ReportRequest {
        baseline: request.baseline,
        thresholds: request.thresholds,
        prices: request
            .prices
            .as_deref()
            .map(|path| json_file(path, "--prices"))
            .transpose()?,
    };
    let deadline = match &request.wait {
        None => None,
        Some(None) => Some(u64::MAX),
        Some(Some(duration)) => Some(now_ms().saturating_add(parse_duration_ms(duration)?)),
    };
    let path = ["v1", "hub", "eval", "runs", request.run.as_str(), "report"];
    let document = loop {
        let document = client.post(&path, &body).await?;
        match deadline {
            Some(deadline) if document["complete"] != json!(true) && now_ms() < deadline => {
                let remaining = Duration::from_millis(deadline.saturating_sub(now_ms()));
                tokio::time::sleep(remaining.min(REPORT_POLL)).await;
            }
            _ => break document,
        }
    };
    if request.table {
        output
            .write_all(report::table(&document).as_bytes())
            .map_err(|error| HubError::storage("report could not be written", error))?;
    } else {
        write_json(output, &document)?;
    }
    Ok(report_exit_code(&document, request.fail_on_regression))
}

pub(crate) async fn execute(
    client: &HubClient,
    command: HubEvalCommand,
    output: &mut impl Write,
) -> Result<i32, HubError> {
    let document = match command {
        HubEvalCommand::SuiteAdd { name, description } => {
            client
                .post(
                    &["v1", "hub", "eval", "suites"],
                    &AddSuite { name, description },
                )
                .await?
        }
        HubEvalCommand::SuiteList => client.get(&["v1", "hub", "eval", "suites"]).await?,
        HubEvalCommand::CaseAdd(request) => {
            let request = *request;
            let source = match request.source {
                HubEvalCaseSource::FromJob(job_id) => CaptureSource::Job { job_id },
                HubEvalCaseSource::Flags(spec) => {
                    let job = jobs::new_job(&spec, now_ms())?;
                    let files = job_files(&job)?;
                    CaptureSource::Flags {
                        job: Box::new(job),
                        files,
                    }
                }
            };
            let body = AddCase {
                suites: request.suites,
                name: request.name,
                labels: request.labels,
                source,
            };
            client.post(&["v1", "hub", "eval", "cases"], &body).await?
        }
        HubEvalCommand::CaseList { suite } => {
            client
                .post(
                    &["v1", "hub", "eval", "cases", "query"],
                    &CaseQuery { suite },
                )
                .await?
        }
        HubEvalCommand::CaseShow { case } => {
            client.get(&["v1", "hub", "eval", "cases", &case]).await?
        }
        HubEvalCommand::Run(run) => {
            client
                .post(&["v1", "hub", "eval", "runs"], &run_request(*run)?)
                .await?
        }
        HubEvalCommand::List { suite, limit } => {
            client
                .post(
                    &["v1", "hub", "eval", "runs", "query"],
                    &RunQuery { suite, limit },
                )
                .await?
        }
        HubEvalCommand::Status { run } => client.get(&["v1", "hub", "eval", "runs", &run]).await?,
        HubEvalCommand::Report(request) => return report(client, request, output).await,
    };
    write_json(output, &document)?;
    Ok(0)
}
