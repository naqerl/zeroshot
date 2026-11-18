//! Sample submission, waiting, and cancellation behind one trait.
//!
//! `PlatformRunner` and `LocalRunner` adapt the existing native-v2 CLI backend; `FakeRunner` is
//! the deterministic test double. All timings are captured at this boundary so an eval can compare
//! samples without consulting provider-specific output.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use openengine_cluster_protocol::{
    RunForceParams, RunId, RunStatus, RunStatusParams, TerminalResult, TokenUsage,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::native_v2_cli::{CliRunStatus, NativeV2CliBackend, PreparedRunRequest};

use super::capture::{CaptureError, CaptureRequest, CapturedSample, DeliveryReceipt, capture_patch};
use super::now_ms;

pub const SAMPLE_FILE: &str = "sample.json";
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// One sample's request. The eval manifest supplies `sample_id`; the CLI has already prepared the
/// immutable native-v2 submission.
#[derive(Clone, Debug)]
pub struct BackendSampleRequest {
    pub sample_id: String,
    pub concurrency: usize,
    pub request: PreparedRunRequest,
}

/// A submitted sample. `submitted_at_ms` is captured before any platform queueing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SampleRun {
    pub sample_id: String,
    pub run_id: RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub submitted_at_ms: u64,
    pub concurrency: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleStatus {
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SampleTimings {
    pub submitted_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: u64,
    /// Submission to the first non-queued observation. Kept separate so parallelism does not
    /// pollute the sample's own wall clock.
    pub queue_wait_ms: u64,
    pub wall_clock_ms: u64,
    pub concurrency: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SampleOutcome {
    pub run_id: RunId,
    pub status: SampleStatus,
    pub timings: SampleTimings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    /// Final graph output. It is stored so a later scorer can recompute from the sample artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
}

/// Persisted `sample.json` plus the captured patch path.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleArtifact {
    pub sample_id: String,
    #[serde(flatten)]
    pub outcome: SampleOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured: Option<CapturedSample>,
}

impl SampleArtifact {
    #[must_use]
    pub fn new(sample_id: impl Into<String>, outcome: SampleOutcome) -> Self {
        Self {
            sample_id: sample_id.into(),
            outcome,
            captured: None,
        }
    }

    #[must_use]
    pub fn with_capture(mut self, captured: CapturedSample) -> Self {
        self.captured = Some(captured);
        self
    }

    pub fn write(&self, directory: &Path) -> std::io::Result<PathBuf> {
        let path = directory.join(SAMPLE_FILE);
        super::write_json_atomic(&path, self)?;
        Ok(path)
    }

    pub fn read(directory: &Path) -> std::io::Result<Self> {
        let bytes = std::fs::read(directory.join(SAMPLE_FILE))?;
        serde_json::from_slice(&bytes).map_err(std::io::Error::other)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SampleRunnerError {
    #[error("sample backend failed: {0}")]
    Backend(String),
    #[error("sample request is invalid: {0}")]
    InvalidRequest(String),
    #[error("sample runner was stopped before completion")]
    Stopped,
}

#[async_trait]
pub trait SampleRunner: Send + Sync {
    type Request: Send;

    async fn submit(&self, request: Self::Request) -> Result<SampleRun, SampleRunnerError>;

    async fn wait(&self, run: &SampleRun) -> Result<SampleOutcome, SampleRunnerError>;

    async fn stop(&self, run: &SampleRun) -> Result<(), SampleRunnerError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SampleExecutionError {
    #[error(transparent)]
    Runner(#[from] SampleRunnerError),
    #[error(transparent)]
    Capture(#[from] CaptureError),
    #[error("sample artifact filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Run one sample end to end and persist its `sample.json`.
///
/// The optional capture callback receives the submitted run and final outcome, so the caller can
/// derive the exact source remote and revision without duplicating timing bookkeeping.
pub async fn run_sample<R, F>(
    runner: &R,
    request: R::Request,
    directory: &Path,
    capture: F,
) -> Result<SampleArtifact, SampleExecutionError>
where
    R: SampleRunner,
    F: FnOnce(&SampleRun, &SampleOutcome) -> Option<CaptureRequest>,
{
    std::fs::create_dir_all(directory)?;
    let run = runner.submit(request).await?;
    let outcome = runner.wait(&run).await?;
    let captured = match outcome.status {
        SampleStatus::Succeeded => match capture(&run, &outcome) {
            Some(request) => Some(capture_patch(&request).await?),
            None => None,
        },
        SampleStatus::Failed => None,
    };
    let artifact = SampleArtifact {
        sample_id: run.sample_id,
        outcome,
        captured,
    };
    artifact.write(directory)?;
    Ok(artifact)
}

/// A runner backed by a hosted/platform native-v2 target.
pub struct PlatformRunner<B> {
    backend: Arc<B>,
    target: String,
    poll_interval: Duration,
}

impl<B> PlatformRunner<B> {
    #[must_use]
    pub fn new(backend: Arc<B>, target: impl Into<String>) -> Self {
        Self {
            backend,
            target: target.into(),
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    #[must_use]
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval.max(Duration::from_millis(1));
        self
    }
}

/// A runner backed by the local native-v2 controller.
pub struct LocalRunner<B> {
    backend: Arc<B>,
    poll_interval: Duration,
}

impl<B> LocalRunner<B> {
    #[must_use]
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    #[must_use]
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval.max(Duration::from_millis(1));
        self
    }
}

#[async_trait]
impl<B: NativeV2CliBackend> SampleRunner for PlatformRunner<B> {
    type Request = BackendSampleRequest;

    async fn submit(&self, request: Self::Request) -> Result<SampleRun, SampleRunnerError> {
        submit(self.backend.as_ref(), Some(self.target.as_str()), request).await
    }

    async fn wait(&self, run: &SampleRun) -> Result<SampleOutcome, SampleRunnerError> {
        wait_for_run(
            self.backend.as_ref(),
            Some(self.target.as_str()),
            run,
            self.poll_interval,
        )
        .await
    }

    async fn stop(&self, run: &SampleRun) -> Result<(), SampleRunnerError> {
        stop(self.backend.as_ref(), Some(self.target.as_str()), run).await
    }
}

#[async_trait]
impl<B: NativeV2CliBackend> SampleRunner for LocalRunner<B> {
    type Request = BackendSampleRequest;

    async fn submit(&self, request: Self::Request) -> Result<SampleRun, SampleRunnerError> {
        submit(self.backend.as_ref(), None, request).await
    }

    async fn wait(&self, run: &SampleRun) -> Result<SampleOutcome, SampleRunnerError> {
        wait_for_run(self.backend.as_ref(), None, run, self.poll_interval).await
    }

    async fn stop(&self, run: &SampleRun) -> Result<(), SampleRunnerError> {
        stop(self.backend.as_ref(), None, run).await
    }
}

async fn submit<B: NativeV2CliBackend>(
    backend: &B,
    target: Option<&str>,
    request: BackendSampleRequest,
) -> Result<SampleRun, SampleRunnerError> {
    if request.sample_id.trim().is_empty() || request.concurrency == 0 {
        return Err(SampleRunnerError::InvalidRequest(
            "sample id and concurrency are required".to_owned(),
        ));
    }
    let submitted_at_ms = now_ms();
    let receipt = backend
        .run_submit(target, request.request)
        .await
        .map_err(|error| SampleRunnerError::Backend(error.to_string()))?;
    Ok(SampleRun {
        sample_id: request.sample_id,
        run_id: receipt.run_id,
        target: target.map(str::to_owned),
        submitted_at_ms,
        concurrency: request.concurrency,
    })
}

async fn stop<B: NativeV2CliBackend>(
    backend: &B,
    target: Option<&str>,
    run: &SampleRun,
) -> Result<(), SampleRunnerError> {
    backend
        .run_force(
            target,
            RunForceParams {
                run_id: run.run_id.clone(),
            },
        )
        .await
        .map(|_| ())
        .map_err(|error| SampleRunnerError::Backend(error.to_string()))
}

async fn wait_for_run<B: NativeV2CliBackend>(
    backend: &B,
    target: Option<&str>,
    run: &SampleRun,
    poll_interval: Duration,
) -> Result<SampleOutcome, SampleRunnerError> {
    let mut started_at_ms = None;
    loop {
        let status = backend
            .run_status(
                target,
                RunStatusParams {
                    run_id: run.run_id.clone(),
                },
            )
            .await
            .map_err(|error| SampleRunnerError::Backend(error.to_string()))?;
        let observed_at_ms = now_ms();
        match &status.status {
            CliRunStatus::Queued(_) => {}
            CliRunStatus::Target(RunStatus::Admitted {}) => {}
            CliRunStatus::Target(RunStatus::Running { .. } | RunStatus::Stopping { .. }) => {
                started_at_ms.get_or_insert(observed_at_ms);
            }
            CliRunStatus::Target(RunStatus::Finished {
                terminal_result,
                metadata,
            }) => {
                return Ok(outcome_from_terminal(
                    run,
                    terminal_result,
                    TerminalObservation {
                        token_usage: metadata.token_usage.clone(),
                        started_at_ms: started_at_ms.unwrap_or(observed_at_ms),
                        finished_at_ms: observed_at_ms,
                    },
                ));
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}

struct TerminalObservation {
    token_usage: Option<TokenUsage>,
    started_at_ms: u64,
    finished_at_ms: u64,
}

fn outcome_from_terminal(
    run: &SampleRun,
    terminal: &TerminalResult,
    observation: TerminalObservation,
) -> SampleOutcome {
    let TerminalObservation {
        token_usage,
        started_at_ms,
        finished_at_ms,
    } = observation;
    let wall_clock_ms = finished_at_ms.saturating_sub(run.submitted_at_ms);
    let queue_wait_ms = started_at_ms.saturating_sub(run.submitted_at_ms);
    let timings = SampleTimings {
        submitted_at_ms: run.submitted_at_ms,
        started_at_ms: Some(started_at_ms),
        finished_at_ms,
        queue_wait_ms,
        wall_clock_ms,
        concurrency: run.concurrency,
    };
    match terminal {
        TerminalResult::Succeeded { output } => {
            let delivery = DeliveryReceipt::from_terminal_output(output);
            SampleOutcome {
                run_id: run.run_id.clone(),
                status: SampleStatus::Succeeded,
                timings,
                token_usage,
                delivered_revision: delivery
                    .as_ref()
                    .and_then(DeliveryReceipt::delivered_revision),
                delivered_branch: delivery
                    .as_ref()
                    .and_then(|value| value.head_branch.clone()),
                delivery,
                failure_reason: None,
                output: Some(output.clone()),
            }
        }
        TerminalResult::Failed { reason, .. } => SampleOutcome {
            run_id: run.run_id.clone(),
            status: SampleStatus::Failed,
            timings,
            token_usage,
            delivered_revision: None,
            delivered_branch: None,
            delivery: None,
            failure_reason: Some(reason.as_str().to_owned()),
            output: None,
        },
    }
}

/// A deterministic in-memory runner used by focused eval tests.
#[derive(Clone, Debug)]
pub struct FakeSampleRequest {
    pub sample_id: String,
    pub run_id: Option<RunId>,
    pub queue_wait_ms: u64,
    pub wall_clock_ms: u64,
    pub status: SampleStatus,
    pub token_usage: Option<TokenUsage>,
    pub delivery: Option<DeliveryReceipt>,
    pub failure_reason: Option<String>,
}

impl FakeSampleRequest {
    #[must_use]
    pub fn succeeded(sample_id: impl Into<String>) -> Self {
        Self {
            sample_id: sample_id.into(),
            run_id: None,
            queue_wait_ms: 0,
            wall_clock_ms: 1,
            status: SampleStatus::Succeeded,
            token_usage: None,
            delivery: None,
            failure_reason: None,
        }
    }

    #[must_use]
    pub fn failed(sample_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            status: SampleStatus::Failed,
            failure_reason: Some(reason.into()),
            ..Self::succeeded(sample_id)
        }
    }
}

#[derive(Clone, Debug)]
struct FakeSubmission {
    request: FakeSampleRequest,
}

#[derive(Default)]
struct FakeState {
    submissions: BTreeMap<RunId, FakeSubmission>,
    order: Vec<SampleRun>,
    stopped: Vec<RunId>,
}

#[derive(Clone, Default)]
pub struct FakeRunner {
    state: Arc<Mutex<FakeState>>,
}

impl FakeRunner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn submitted(&self) -> Vec<SampleRun> {
        self.state.lock().await.order.clone()
    }

    pub async fn stopped(&self) -> Vec<RunId> {
        self.state.lock().await.stopped.clone()
    }
}

#[async_trait]
impl SampleRunner for FakeRunner {
    type Request = FakeSampleRequest;

    async fn submit(&self, request: Self::Request) -> Result<SampleRun, SampleRunnerError> {
        if request.sample_id.trim().is_empty() {
            return Err(SampleRunnerError::InvalidRequest(
                "sample id is required".to_owned(),
            ));
        }
        let run_id = request
            .run_id
            .clone()
            .unwrap_or_else(|| RunId::new(uuid::Uuid::now_v7().to_string()));
        let run = SampleRun {
            sample_id: request.sample_id.clone(),
            run_id: run_id.clone(),
            target: None,
            submitted_at_ms: now_ms(),
            concurrency: 1,
        };
        let mut state = self.state.lock().await;
        state.order.push(run.clone());
        state.submissions.insert(run_id, FakeSubmission { request });
        Ok(run)
    }

    async fn wait(&self, run: &SampleRun) -> Result<SampleOutcome, SampleRunnerError> {
        let state = self.state.lock().await;
        let submission = state
            .submissions
            .get(&run.run_id)
            .ok_or_else(|| SampleRunnerError::InvalidRequest("run was not submitted".to_owned()))?;
        if state.stopped.contains(&run.run_id) {
            return Err(SampleRunnerError::Stopped);
        }
        let request = &submission.request;
        let started_at_ms = run.submitted_at_ms.saturating_add(request.queue_wait_ms);
        let finished_at_ms = started_at_ms.saturating_add(request.wall_clock_ms);
        let delivery = request.delivery.clone();
        Ok(SampleOutcome {
            run_id: run.run_id.clone(),
            status: request.status,
            timings: SampleTimings {
                submitted_at_ms: run.submitted_at_ms,
                started_at_ms: Some(started_at_ms),
                finished_at_ms,
                queue_wait_ms: request.queue_wait_ms,
                wall_clock_ms: request.wall_clock_ms,
                concurrency: run.concurrency,
            },
            token_usage: request.token_usage.clone(),
            delivered_revision: delivery
                .as_ref()
                .and_then(DeliveryReceipt::delivered_revision),
            delivered_branch: delivery
                .as_ref()
                .and_then(|value| value.head_branch.clone()),
            delivery,
            failure_reason: request.failure_reason.clone(),
            output: None,
        })
    }

    async fn stop(&self, run: &SampleRun) -> Result<(), SampleRunnerError> {
        self.state.lock().await.stopped.push(run.run_id.clone());
        Ok(())
    }
}

#[cfg(test)]
#[path = "runner_test.rs"]
mod tests;
