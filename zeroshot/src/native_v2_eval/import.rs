//! Import already-finished target runs (for example `platform` runs) as one arm's samples.
//!
//! `zeroshot eval import-baseline` reads each run's durable status and log replay through the
//! ordinary CLI backend, so it uses the same target login as `zeroshot status --target`. It
//! records `T`, the token counts behind `C`, and the delivered `base..head` patch, leaves the
//! samples unjudged, and rebuilds the report. Anything the target does not expose is recorded as a
//! per-sample note instead of being guessed.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use openengine_cluster_protocol::{
    RunId, RunListParams, RunLogsParams, RunStatus, RunStatusParams, TerminalResult,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::native_v2_cli::{
    CliRunStatus, CliRunStatusResult, CliSubscription, CliSubscriptionItem, NativeV2CliBackend,
};

use super::{
    CaptureCredentials, CaptureRequest, DeliveryReceipt, EvalData, EvalError, EvalId, EvalManifest,
    EvalSample, EvalStore, PatchArtifact, SampleStatus, SampleTiming, TokenUsage, capture_patch,
    store::timestamp,
};

/// How long one log replay may stay silent before the import stops reading it.
pub const LOG_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
/// Upper bound on replayed log records per run.
const MAX_LOG_RECORDS: usize = 1_000_000;

/// Note recorded on every imported sample: finished runs carry no queue timestamps.
pub const QUEUE_NOTE: &str = "queue time is not exposed for a finished run; T is the span from the first to the last log record (target clock), so queue wait before the first record is excluded";

/// `zeroshot eval import-baseline <id> --run <run-id>...`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EvalImportRunsCommand {
    /// Eval to import into. It is created from `manifest` when it does not exist yet.
    pub id: String,
    pub manifest: Option<PathBuf>,
    /// Arm the runs become. Defaults to `options.score.baseline`, else the first arm.
    pub arm: Option<String>,
    pub runs: Vec<String>,
    /// One `run-id [minutes]` per line; `#` starts a comment. `minutes` overrides `T`.
    pub runs_file: Option<PathBuf>,
    /// Also import every finished run whose title starts with this prefix.
    pub title_prefix: Option<String>,
    /// Target that owns the runs. Defaults to `platform`.
    pub target: Option<String>,
    /// Git remote that holds the delivered branches. Defaults to `options.captureRemote`, then the
    /// run's recorded source remote.
    pub capture_remote: Option<String>,
}

/// One run to import, with an optional caller-measured wall time.
#[derive(Clone, Debug, PartialEq)]
pub struct RunSelection {
    pub run_id: String,
    pub minutes: Option<f64>,
}

/// Summary printed by the command.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportSummary {
    pub eval_id: String,
    pub arm_id: String,
    pub target: String,
    pub created: bool,
    pub imported: Vec<ImportedRun>,
    pub pairing: String,
    pub next: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportedRun {
    pub sample_id: String,
    pub run_id: String,
    pub status: SampleStatus,
    pub time_minutes: f64,
    pub tokens: TokenUsage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_bytes: Option<u64>,
    pub notes: Vec<String>,
}

/// Parse a runs file: one `run-id [minutes]` per line, blank lines and `#` comments ignored.
pub fn parse_runs_file(source: &str) -> Result<Vec<RunSelection>, EvalError> {
    let mut runs = Vec::new();
    for (number, line) in source.lines().enumerate() {
        let line = line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split(|c: char| c.is_whitespace() || c == ',');
        let run_id = fields.next().unwrap_or_default().trim().to_owned();
        let minutes = match fields.find(|field| !field.trim().is_empty()) {
            Some(field) => {
                let minutes = field.trim().parse::<f64>().map_err(|_| {
                    EvalError::Command(format!(
                        "runs file line {}: {field:?} is not a number of minutes",
                        number + 1
                    ))
                })?;
                if !minutes.is_finite() || minutes < 0.0 {
                    return Err(EvalError::Command(format!(
                        "runs file line {}: minutes must be a nonnegative number",
                        number + 1
                    )));
                }
                Some(minutes)
            }
            None => None,
        };
        runs.push(RunSelection { run_id, minutes });
    }
    Ok(runs)
}

/// Import the selected finished runs into `store` through `backend`.
pub async fn import_runs<B: NativeV2CliBackend>(
    store: &EvalStore,
    command: &EvalImportRunsCommand,
    backend: &B,
) -> Result<ImportSummary, EvalError> {
    let target = command
        .target
        .clone()
        .unwrap_or_else(|| "platform".to_owned());
    let id = EvalId::new(command.id.clone())?;
    let (mut data, created, manifest_source) = match store.load(&id) {
        Ok(data) => (data, false, None),
        Err(EvalError::NotFound(_)) => {
            let path = command.manifest.as_ref().ok_or_else(|| {
                EvalError::Command(format!(
                    "eval {id} does not exist; pass --manifest to create it"
                ))
            })?;
            let (manifest, source) = EvalManifest::from_file(path)?;
            (
                EvalData {
                    id: id.clone(),
                    created_at_ms: timestamp(),
                    manifest,
                    samples: Vec::new(),
                    report: None,
                },
                true,
                Some(source),
            )
        }
        Err(error) => return Err(error),
    };
    let arm_id = match &command.arm {
        Some(arm) => arm.clone(),
        None => data
            .manifest
            .options
            .score
            .baseline
            .clone()
            .or_else(|| data.manifest.arms.first().map(|arm| arm.id.clone()))
            .ok_or_else(|| EvalError::Manifest("the manifest declares no arms".to_owned()))?,
    };
    if !data.manifest.arms.iter().any(|arm| arm.id == arm_id) {
        return Err(EvalError::Command(format!(
            "arm {arm_id:?} is not declared in the eval manifest"
        )));
    }

    let selection = select_runs(command, backend, &target).await?;
    let already = data
        .samples
        .iter()
        .map(|sample| sample.run_id.clone())
        .collect::<BTreeSet<_>>();
    if let Some(duplicate) = selection.iter().find(|run| already.contains(&run.run_id)) {
        return Err(EvalError::Command(format!(
            "run {} is already a sample of eval {id}",
            duplicate.run_id
        )));
    }

    // Read every run before writing anything, so one unfinished run leaves the store unchanged.
    let mut records = Vec::with_capacity(selection.len());
    let mut unfinished = Vec::new();
    for run in &selection {
        let status = backend
            .run_status(
                Some(&target),
                RunStatusParams {
                    run_id: RunId::new(run.run_id.clone()),
                },
            )
            .await
            .map_err(|error| {
                EvalError::Command(format!("could not read run {}: {error}", run.run_id))
            })?;
        if finished(&status).is_none() {
            unfinished.push(run.run_id.clone());
        }
        records.push((run.clone(), status));
    }
    if !unfinished.is_empty() {
        return Err(EvalError::Command(format!(
            "runs are not finished yet: {}; import them after they finish",
            unfinished.join(", ")
        )));
    }

    let capture_remote = command
        .capture_remote
        .clone()
        .or_else(|| data.manifest.options.capture_remote.clone());
    let first_index = data
        .samples
        .iter()
        .filter(|sample| sample.arm_id == arm_id)
        .map(|sample| sample.run_index + 1)
        .max()
        .unwrap_or(0);
    let mut prepared = Vec::with_capacity(records.len());
    for (run_index, (selection, status)) in (first_index..).zip(records) {
        let sample_id = format!("{arm_id}-{run_index:04}");
        let (sample, patch) = import_one(ImportOne {
            backend,
            target: &target,
            selection: &selection,
            status: &status,
            arm_id: &arm_id,
            run_index,
            sample_id,
            capture_remote: capture_remote.as_deref(),
        })
        .await?;
        prepared.push((sample, patch));
    }

    if let Some(source) = &manifest_source {
        store.create(source, &id, data.created_at_ms)?;
    }
    let mut imported = Vec::with_capacity(prepared.len());
    for (mut sample, patch) in prepared {
        let patch_bytes = match patch {
            Some(patch) => {
                let path = store.write_patch(&id, &sample.id, &patch)?;
                let bytes = u64::try_from(patch.len()).unwrap_or(u64::MAX);
                sample.patch = Some(PatchArtifact {
                    path,
                    sha256: hex_digest(patch.as_bytes()),
                    bytes,
                });
                Some(bytes)
            }
            None => None,
        };
        store.write_sample(&id, &sample)?;
        imported.push(ImportedRun {
            sample_id: sample.id.clone(),
            run_id: sample.run_id.clone(),
            status: sample.status,
            time_minutes: sample.timing.wall_clock_ms as f64 / 60_000.0,
            tokens: sample.tokens,
            patch_bytes,
            notes: sample.notes.clone(),
        });
        data.samples.push(sample);
    }
    let scorer = data.report.as_ref().map_or_else(
        || data.manifest.options.scorer.clone(),
        |report| report.scorer.clone(),
    );
    let capacity = data.report.as_ref().and_then(|report| report.capacity);
    let report = super::cli::recompute(&data, &scorer, capacity)?;
    store.write_report(&id, &report)?;
    store.write_report_markdown(&id, &report)?;
    let counts = data
        .manifest
        .arms
        .iter()
        .map(|arm| {
            format!(
                "{} = {}",
                arm.id,
                data.samples
                    .iter()
                    .filter(|sample| sample.arm_id == arm.id)
                    .count()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(ImportSummary {
        eval_id: id.to_string(),
        arm_id,
        target,
        created,
        imported,
        pairing: format!(
            "runs per arm: {counts}; pairs are formed by run index, rows without a partner show one side, and means and medians are per arm over all of its runs"
        ),
        next: format!(
            "the imported samples are unjudged; run `zeroshot eval judge {id}` to judge them with the eval-judge graph"
        ),
    })
}

async fn select_runs<B: NativeV2CliBackend>(
    command: &EvalImportRunsCommand,
    backend: &B,
    target: &str,
) -> Result<Vec<RunSelection>, EvalError> {
    let mut selection = command
        .runs
        .iter()
        .map(|run_id| RunSelection {
            run_id: run_id.trim().to_owned(),
            minutes: None,
        })
        .collect::<Vec<_>>();
    if let Some(path) = &command.runs_file {
        let source = std::fs::read_to_string(path).map_err(|error| EvalError::io(path, error))?;
        selection.extend(parse_runs_file(&source)?);
    }
    if let Some(prefix) = &command.title_prefix {
        let listed = backend
            .run_list(Some(target), RunListParams {})
            .await
            .map_err(|error| EvalError::Command(format!("could not list runs: {error}")))?;
        let mut matched = listed
            .runs
            .into_iter()
            .filter(|run| run.title.as_str().starts_with(prefix.as_str()))
            .filter(|run| finished(run).is_some())
            .map(|run| run.run_id.as_str().to_owned())
            .collect::<Vec<_>>();
        matched.sort();
        if matched.is_empty() {
            return Err(EvalError::Command(format!(
                "no finished run on target {target} has a title starting with {prefix:?}"
            )));
        }
        selection.extend(matched.into_iter().map(|run_id| RunSelection {
            run_id,
            minutes: None,
        }));
    }
    let mut seen = BTreeSet::new();
    let mut unique = Vec::with_capacity(selection.len());
    for run in selection {
        if run.run_id.is_empty() {
            return Err(EvalError::Command(
                "an empty run id was selected".to_owned(),
            ));
        }
        if seen.insert(run.run_id.clone()) {
            unique.push(run);
        } else if let Some(minutes) = run.minutes {
            if let Some(existing) = unique.iter_mut().find(|item| item.run_id == run.run_id) {
                existing.minutes.get_or_insert(minutes);
            }
        }
    }
    if unique.is_empty() {
        return Err(EvalError::Command(
            "no runs selected; pass --run, --runs-file, or --title-prefix".to_owned(),
        ));
    }
    Ok(unique)
}

fn finished(
    status: &CliRunStatusResult,
) -> Option<(&TerminalResult, &openengine_cluster_protocol::RunMetadata)> {
    match &status.status {
        CliRunStatus::Target(RunStatus::Finished {
            terminal_result,
            metadata,
        }) => Some((terminal_result, metadata)),
        _ => None,
    }
}

struct ImportOne<'a, B> {
    backend: &'a B,
    target: &'a str,
    selection: &'a RunSelection,
    status: &'a CliRunStatusResult,
    arm_id: &'a str,
    run_index: u32,
    sample_id: String,
    capture_remote: Option<&'a str>,
}

async fn import_one<B: NativeV2CliBackend>(
    request: ImportOne<'_, B>,
) -> Result<(EvalSample, Option<String>), EvalError> {
    let ImportOne {
        backend,
        target,
        selection,
        status,
        arm_id,
        run_index,
        sample_id,
        capture_remote,
    } = request;
    let (terminal, metadata) = finished(status)
        .ok_or_else(|| EvalError::Command(format!("run {} is not finished", selection.run_id)))?;
    let mut notes = vec![QUEUE_NOTE.to_owned()];

    let span = log_span(backend, target, &selection.run_id).await;
    let (started_at_ms, wall_clock_ms) = match (selection.minutes, &span) {
        (Some(minutes), _) => {
            notes.push(format!("T = {minutes} min from the runs file"));
            let started = span
                .as_ref()
                .ok()
                .and_then(|span| span.first_ms)
                .unwrap_or(0);
            (started, (minutes * 60_000.0).round() as u64)
        }
        (None, Ok(span)) => {
            if span.partial {
                notes.push(
                    "log replay stopped before it closed; T may be shorter than the run".to_owned(),
                );
            }
            match (span.first_ms, span.last_ms) {
                (Some(first), Some(last)) => (first, last.saturating_sub(first)),
                _ => {
                    notes.push(
                        "the run has no log records, so T is unknown; give its minutes in --runs-file"
                            .to_owned(),
                    );
                    (0, 0)
                }
            }
        }
        (None, Err(error)) => {
            notes.push(format!(
                "could not replay the run's logs ({error}), so T is unknown; give its minutes in --runs-file"
            ));
            (0, 0)
        }
    };

    let tokens = match &metadata.token_usage {
        Some(usage) => {
            if !usage.complete {
                notes.push(
                    "token usage is incomplete: at least one agent invocation reported no counters"
                        .to_owned(),
                );
            }
            match (
                usage.cache_read_input_tokens,
                usage.cache_creation_input_tokens,
            ) {
                (None, None) => notes.push(
                    "no cache token counts were reported (no cacheRead/cacheWrite split); cache tokens count as 0 in C"
                        .to_owned(),
                ),
                (None, Some(_)) => notes.push(
                    "cacheRead tokens were not reported and count as 0 in C".to_owned(),
                ),
                (Some(_), None) => notes.push(
                    "cacheWrite tokens were not reported and count as 0 in C".to_owned(),
                ),
                (Some(_), Some(_)) => {}
            }
            super::cli::eval_tokens(Some(usage.clone()))
        }
        None => {
            notes.push("the run reported no token usage, so C is unknown (0 tokens)".to_owned());
            TokenUsage::default()
        }
    };

    let (sample_status, failure_reason, output) = match terminal {
        TerminalResult::Succeeded { output } => (SampleStatus::Succeeded, None, Some(output)),
        TerminalResult::Failed { reason, .. } => (
            SampleStatus::Failed,
            Some(
                serde_json::to_value(reason)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| format!("{reason:?}")),
            ),
            None,
        ),
    };
    let receipt = output.and_then(DeliveryReceipt::from_terminal_output);
    let delivered_revision = receipt
        .as_ref()
        .and_then(DeliveryReceipt::delivered_revision);
    let mut patch = None;
    if sample_status == SampleStatus::Succeeded {
        match (&receipt, &status.source) {
            (None, _) => notes.push(
                "the run output has no delivery receipt, so there is no patch to judge".to_owned(),
            ),
            (Some(_), None) => notes.push(
                "the run records no source revision, so base..head cannot be diffed".to_owned(),
            ),
            (Some(receipt), Some(source)) => {
                let remote = capture_remote.map(str::to_owned).or_else(|| {
                    source.remote.as_ref().and_then(|remote| {
                        serde_json::to_value(&remote.url)
                            .ok()
                            .and_then(|value| value.as_str().map(str::to_owned))
                    })
                });
                let base = json_string(&source.revision);
                let base_branch = json_string(&source.branch);
                match (remote, base) {
                    (Some(remote), Some(base)) => {
                        match capture(&remote, &base, base_branch, receipt).await {
                            Ok(diff) => patch = Some(diff),
                            Err(error) => notes.push(format!("patch capture failed: {error}")),
                        }
                    }
                    (None, _) => notes.push(
                        "no Git remote for the delivered branch; pass --capture-remote".to_owned(),
                    ),
                    (_, None) => {
                        notes.push("the run's source revision is unreadable".to_owned());
                    }
                }
            }
        }
    }

    Ok((
        EvalSample {
            id: sample_id,
            run_id: selection.run_id.clone(),
            arm_id: arm_id.to_owned(),
            run_index,
            status: sample_status,
            timing: SampleTiming {
                started_at_ms,
                wall_clock_ms,
                queue_wait_ms: 0,
            },
            tokens,
            cost_usd: None,
            delivered_revision,
            failure_reason,
            patch: None,
            criteria: Default::default(),
            reasoning: Default::default(),
            judge_template: None,
            judge_checkout: None,
            judge_executor: None,
            notes,
        },
        patch,
    ))
}

fn json_string<T: Serialize>(value: &T) -> Option<String> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
}

async fn capture(
    remote: &str,
    base: &str,
    base_branch: Option<String>,
    receipt: &DeliveryReceipt,
) -> Result<String, EvalError> {
    let directory = tempfile::tempdir().map_err(|error| EvalError::io(".", error))?;
    let mut request = CaptureRequest::new(directory.path(), remote, base, receipt.clone())
        .with_credentials(CaptureCredentials {
            token: std::env::var("GH_TOKEN")
                .ok()
                .filter(|token| !token.is_empty()),
            git: None,
        });
    if let Some(branch) = base_branch {
        request = request.with_base_branch(branch);
    }
    let captured = capture_patch(&request)
        .await
        .map_err(|error| EvalError::Command(error.to_string()))?;
    let path = directory.path().join(&captured.patch);
    std::fs::read_to_string(&path).map_err(|error| EvalError::io(&path, error))
}

#[derive(Debug, Default)]
struct LogSpan {
    first_ms: Option<u64>,
    last_ms: Option<u64>,
    partial: bool,
}

async fn log_span<B: NativeV2CliBackend>(
    backend: &B,
    target: &str,
    run_id: &str,
) -> Result<LogSpan, String> {
    let mut subscription = backend
        .run_logs(
            Some(target),
            RunLogsParams {
                run_id: RunId::new(run_id.to_owned()),
                from_cursor: None,
                execution: None,
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut span = LogSpan::default();
    for _ in 0..MAX_LOG_RECORDS {
        match tokio::time::timeout(LOG_IDLE_TIMEOUT, subscription.next()).await {
            Err(_) => {
                span.partial = span.first_ms.is_some();
                return Ok(span);
            }
            Ok(Err(error)) => return Err(error.to_string()),
            Ok(Ok(None)) => return Ok(span),
            Ok(Ok(Some(CliSubscriptionItem::Closed { reason }))) => {
                span.partial = !matches!(
                    reason,
                    openengine_cluster_protocol::SubscriptionCloseReason::Done
                );
                return Ok(span);
            }
            Ok(Ok(Some(CliSubscriptionItem::Event(event)))) => {
                let at = event.timestamp.get();
                span.first_ms = Some(span.first_ms.map_or(at, |first| first.min(at)));
                span.last_ms = Some(span.last_ms.map_or(at, |last| last.max(at)));
            }
        }
    }
    span.partial = true;
    Ok(span)
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
#[path = "import_test.rs"]
mod tests;
