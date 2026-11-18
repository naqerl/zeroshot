//! Sample submission, waiting, cancellation, capture, and resume behind one trait.
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
    GitTransportCredentials, RunConnectionValues, RunForceParams, RunId, RunResumeParams,
    RunStatus, RunStatusParams, TerminalResult, TokenUsage,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::native_v2_cli::{CliRunStatus, NativeV2CliBackend, PreparedRunRequest};

use super::capacity::{Capacity, CapacityError, CapacitySource, resolve_capacity, run_bounded};
use super::capture::{
    CaptureError, CaptureRequest, CapturedSample, DeliveryReceipt, capture_patch, checkout_patch,
};
use super::now_ms;
use super::resume::{CapacityWarning, ResumeAction, ResumeError, ResumeJournal};

pub const SAMPLE_FILE: &str = "sample.json";
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_STORED_OUTPUT_BYTES: usize = 1_024 * 1_024;

/// One sample's request. The eval manifest supplies `sample_id`; the CLI has already prepared the
/// immutable native-v2 submission.
#[derive(Clone, Debug)]
pub struct BackendSampleRequest {
    pub sample_id: String,
    pub concurrency: usize,
    pub request: PreparedRunRequest,
}

/// A request whose recorded scheduler width can be aligned with the eval's resolved capacity.
pub trait SampleRequest: Send {
    fn with_concurrency(self, concurrency: usize) -> Self;
}

impl SampleRequest for BackendSampleRequest {
    fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

/// Fresh resume authority for one attempt. It is never serialized or written to the journal.
#[derive(Clone, Default)]
pub struct ResumeCredentials {
    pub connections: RunConnectionValues,
    pub github_token: Option<String>,
    pub git_credentials: Option<GitTransportCredentials>,
}

impl std::fmt::Debug for ResumeCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResumeCredentials")
            .field("connections", &self.connections.keys().collect::<Vec<_>>())
            .field(
                "github_token",
                &self.github_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("git_credentials", &self.git_credentials)
            .finish()
    }
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
    /// Bounded final graph output. Delivery fields are also projected above so truncation cannot
    /// hide the captured revision.
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
    /// A successful run whose fetch or patch application failed. The outcome remains renderable;
    /// resume can retry capture without pretending the run did not happen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_error: Option<String>,
    /// True while a successful run has not yet finished its patch and judge capture. This is
    /// written before any capture so an interrupted process remains resumable.
    #[serde(default)]
    pub capture_pending: bool,
}

impl SampleArtifact {
    #[must_use]
    pub fn new(sample_id: impl Into<String>, outcome: SampleOutcome) -> Self {
        Self {
            sample_id: sample_id.into(),
            outcome,
            captured: None,
            capture_error: None,
            capture_pending: false,
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
    #[error("sample exceeded its configured deadline")]
    Deadline,
    #[error("sample runner cannot resume retained runs")]
    ResumeUnsupported,
    #[error("the submitted run no longer exists")]
    RunLost,
    #[error("the submitted run cannot be recovered")]
    ResumeUnavailable(String),
}

impl SampleRunnerError {
    /// A transport/backend failure may clear on a later attempt. The in-flight run must stay
    /// recoverable rather than being replaced by a fresh submission.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Backend(_))
    }

    #[must_use]
    pub const fn is_run_lost(&self) -> bool {
        matches!(self, Self::RunLost)
    }
}

#[async_trait]
pub trait SampleRunner: Send + Sync {
    type Request: SampleRequest;

    async fn submit(&self, request: Self::Request) -> Result<SampleRun, SampleRunnerError>;

    async fn wait(&self, run: &SampleRun) -> Result<SampleOutcome, SampleRunnerError>;

    async fn stop(&self, run: &SampleRun) -> Result<(), SampleRunnerError>;

    /// Resume a run that was already submitted but never reached a terminal outcome. Fresh
    /// authority stays out of the journal and is supplied for this attempt only.
    async fn resume(
        &self,
        run: SampleRun,
        credentials: ResumeCredentials,
    ) -> Result<SampleRun, SampleRunnerError> {
        let _ = (run, credentials);
        Err(SampleRunnerError::ResumeUnsupported)
    }
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
/// derive the exact source remote and revision without duplicating timing bookkeeping. A capture
/// failure is recorded on the artifact after the run outcome has already been persisted.
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
    let run = runner.submit(request).await?;
    let outcome = runner.wait(&run).await?;
    let capture = if outcome.status == SampleStatus::Succeeded {
        capture(&run, &outcome)
    } else {
        None
    };
    persist_sample(run.sample_id, outcome, directory, capture).await
}

fn remove_existing_judge(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path)?;
    }
    Ok(())
}

async fn persist_sample(
    sample_id: String,
    outcome: SampleOutcome,
    directory: &Path,
    capture: Option<CaptureRequest>,
) -> Result<SampleArtifact, SampleExecutionError> {
    std::fs::create_dir_all(directory)?;
    let mut artifact = SampleArtifact::new(sample_id, outcome);
    let capture_requested = capture.is_some() && artifact.outcome.status == SampleStatus::Succeeded;
    artifact.capture_pending = capture_requested;
    // Persist the measured run before any network capture so a failed fetch never erases it, and
    // mark capture as pending so a crash in the middle of capture is retried later.
    artifact.write(directory)?;
    if let Some(mut request) =
        capture.filter(|_| artifact.outcome.status == SampleStatus::Succeeded)
    {
        request.directory = directory.to_owned();
        request.receipt = artifact.outcome.delivery.clone().unwrap_or_default();
        match capture_patch(&request).await {
            Ok(mut captured) => {
                let judge = directory.join("judge");
                let checkout = match remove_existing_judge(&judge) {
                    Ok(()) => checkout_patch(&request, &judge).await,
                    Err(error) => Err(CaptureError::Io(error)),
                };
                match checkout {
                    Ok(judge) => captured.judge = Some(judge),
                    Err(error) => artifact.capture_error = Some(error.to_string()),
                }
                artifact.captured = Some(captured);
            }
            Err(error) => artifact.capture_error = Some(error.to_string()),
        }
        artifact.capture_pending = false;
        artifact.write(directory)?;
    }
    Ok(artifact)
}

/// A runner backed by a hosted/platform native-v2 target.
pub struct PlatformRunner<B> {
    backend: Arc<B>,
    target: String,
    poll_interval: Duration,
    deadline: Option<Duration>,
}

impl<B> PlatformRunner<B> {
    #[must_use]
    pub fn new(backend: Arc<B>, target: impl Into<String>) -> Self {
        Self {
            backend,
            target: target.into(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            deadline: None,
        }
    }

    #[must_use]
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval.max(Duration::from_millis(1));
        self
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline.max(Duration::from_millis(1)));
        self
    }
}

/// A runner backed by the local native-v2 controller.
pub struct LocalRunner<B> {
    backend: Arc<B>,
    poll_interval: Duration,
    deadline: Option<Duration>,
}

impl<B> LocalRunner<B> {
    #[must_use]
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            poll_interval: DEFAULT_POLL_INTERVAL,
            deadline: None,
        }
    }

    #[must_use]
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval.max(Duration::from_millis(1));
        self
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline.max(Duration::from_millis(1)));
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
            WaitPolicy {
                poll_interval: self.poll_interval,
                deadline: self.deadline,
            },
        )
        .await
    }

    async fn stop(&self, run: &SampleRun) -> Result<(), SampleRunnerError> {
        stop(self.backend.as_ref(), Some(self.target.as_str()), run).await
    }

    async fn resume(
        &self,
        run: SampleRun,
        credentials: ResumeCredentials,
    ) -> Result<SampleRun, SampleRunnerError> {
        resume(
            self.backend.as_ref(),
            Some(self.target.as_str()),
            run,
            credentials,
        )
        .await
    }
}

#[async_trait]
impl<B: NativeV2CliBackend> SampleRunner for LocalRunner<B> {
    type Request = BackendSampleRequest;

    async fn submit(&self, request: Self::Request) -> Result<SampleRun, SampleRunnerError> {
        submit(self.backend.as_ref(), None, request).await
    }

    async fn wait(&self, run: &SampleRun) -> Result<SampleOutcome, SampleRunnerError> {
        wait_for_run(
            self.backend.as_ref(),
            None,
            run,
            WaitPolicy {
                poll_interval: self.poll_interval,
                deadline: self.deadline,
            },
        )
        .await
    }

    async fn stop(&self, run: &SampleRun) -> Result<(), SampleRunnerError> {
        stop(self.backend.as_ref(), None, run).await
    }

    async fn resume(
        &self,
        run: SampleRun,
        credentials: ResumeCredentials,
    ) -> Result<SampleRun, SampleRunnerError> {
        resume(self.backend.as_ref(), None, run, credentials).await
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

async fn resume<B: NativeV2CliBackend>(
    backend: &B,
    target: Option<&str>,
    run: SampleRun,
    credentials: ResumeCredentials,
) -> Result<SampleRun, SampleRunnerError> {
    let successor = RunId::new(uuid::Uuid::now_v7().to_string());
    let result = backend
        .run_resume(
            target,
            RunResumeParams {
                run_id: run.run_id.clone(),
                successor_run_id: successor,
                from: None,
                connections: credentials.connections,
                connection_resolver: None,
                github_token: credentials.github_token,
                git_credentials: credentials.git_credentials,
            },
        )
        .await
        .map_err(resume_error)?;
    Ok(SampleRun {
        run_id: result.run_id,
        submitted_at_ms: now_ms(),
        ..run
    })
}

fn resume_error(error: crate::native_v2_cli::NativeV2CliError) -> SampleRunnerError {
    use crate::native_v2_cli::NativeV2CliError;
    match error {
        NativeV2CliError::RunNotFound { .. } => SampleRunnerError::RunLost,
        NativeV2CliError::TargetTransport { .. }
        | NativeV2CliError::Disconnected
        | NativeV2CliError::Remote { .. }
        | NativeV2CliError::Protocol(_) => SampleRunnerError::Backend(error.to_string()),
        other => SampleRunnerError::ResumeUnavailable(other.to_string()),
    }
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

struct WaitPolicy {
    poll_interval: Duration,
    deadline: Option<Duration>,
}

async fn wait_for_run<B: NativeV2CliBackend>(
    backend: &B,
    target: Option<&str>,
    run: &SampleRun,
    policy: WaitPolicy,
) -> Result<SampleOutcome, SampleRunnerError> {
    let mut started_at_ms = None;
    let deadline_at = policy.deadline.map(|deadline| Instant::now() + deadline);
    loop {
        let status = backend.run_status(
            target,
            RunStatusParams {
                run_id: run.run_id.clone(),
            },
        );
        let status = match deadline_at {
            Some(deadline_at) => match tokio::time::timeout(
                deadline_at.saturating_duration_since(Instant::now()),
                status,
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    let _ = stop(backend, target, run).await;
                    return Err(SampleRunnerError::Deadline);
                }
            },
            None => status.await,
        }
        .map_err(wait_error)?;
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
        let wait = if let Some(deadline_at) = deadline_at {
            let now = Instant::now();
            if now >= deadline_at {
                let _ = stop(backend, target, run).await;
                return Err(SampleRunnerError::Deadline);
            }
            policy
                .poll_interval
                .min(deadline_at.saturating_duration_since(now))
        } else {
            policy.poll_interval
        };
        tokio::time::sleep(wait).await;
    }
}

fn wait_error(error: crate::native_v2_cli::NativeV2CliError) -> SampleRunnerError {
    if matches!(
        error,
        crate::native_v2_cli::NativeV2CliError::RunNotFound { .. }
    ) {
        SampleRunnerError::RunLost
    } else {
        SampleRunnerError::Backend(error.to_string())
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
                output: bounded_output(output),
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

fn bounded_output(output: &serde_json::Value) -> Option<serde_json::Value> {
    let bytes = serde_json::to_vec(output).ok()?;
    if bytes.len() <= MAX_STORED_OUTPUT_BYTES {
        return Some(output.clone());
    }
    Some(serde_json::json!({
        "truncated": true,
        "originalBytes": bytes.len(),
    }))
}

/// A deterministic in-memory runner used by focused eval tests.
#[derive(Clone, Debug)]
pub struct FakeSampleRequest {
    pub sample_id: String,
    pub run_id: Option<RunId>,
    pub concurrency: usize,
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
            concurrency: 1,
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

impl SampleRequest for FakeSampleRequest {
    fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
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
            concurrency: request.concurrency.max(1),
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

    async fn resume(
        &self,
        run: SampleRun,
        _credentials: ResumeCredentials,
    ) -> Result<SampleRun, SampleRunnerError> {
        let mut state = self.state.lock().await;
        let submission =
            state.submissions.get(&run.run_id).cloned().ok_or_else(|| {
                SampleRunnerError::InvalidRequest("run was not submitted".to_owned())
            })?;
        let successor = RunId::new(uuid::Uuid::now_v7().to_string());
        let resumed = SampleRun {
            run_id: successor.clone(),
            submitted_at_ms: now_ms(),
            ..run
        };
        state.order.push(resumed.clone());
        state.submissions.insert(successor, submission);
        Ok(resumed)
    }
}

/// One sample in an eval driver invocation. `request` is required for a fresh start; a completed
/// sample can be skipped from its persisted artifact without one.
pub struct EvalSamplePlan<R: SampleRunner> {
    pub sample_id: String,
    pub directory: PathBuf,
    pub request: Option<R::Request>,
    pub capture: Option<CaptureRequest>,
    pub concurrency: usize,
    /// Fresh authority for resuming a retained run. Never persisted in the journal.
    pub resume_credentials: ResumeCredentials,
}

impl<R: SampleRunner> EvalSamplePlan<R> {
    #[must_use]
    pub fn new(
        sample_id: impl Into<String>,
        directory: impl Into<PathBuf>,
        request: R::Request,
    ) -> Self {
        Self {
            sample_id: sample_id.into(),
            directory: directory.into(),
            request: Some(request),
            capture: None,
            concurrency: 1,
            resume_credentials: ResumeCredentials::default(),
        }
    }

    #[must_use]
    pub fn with_capture(mut self, capture: CaptureRequest) -> Self {
        self.capture = Some(capture);
        self
    }

    #[must_use]
    pub fn with_resume_connections(mut self, connections: RunConnectionValues) -> Self {
        self.resume_credentials.connections = connections;
        self
    }

    #[must_use]
    pub fn with_resume_credentials(mut self, credentials: ResumeCredentials) -> Self {
        self.resume_credentials = credentials;
        self
    }
}

/// A complete eval attempt: resolve capacity once, journal the attempt, and run each sample.
pub struct EvalRunRequest<'a, R: SampleRunner> {
    pub runner: &'a R,
    pub capacity_source: Option<&'a dyn CapacitySource>,
    pub concurrency: Option<usize>,
    pub journal_directory: PathBuf,
    pub samples: Vec<EvalSamplePlan<R>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SampleFailure {
    pub sample_id: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EvalRunReport {
    pub capacity: Capacity,
    pub capacity_warning: Option<CapacityWarning>,
    pub artifacts: Vec<SampleArtifact>,
    pub failures: Vec<SampleFailure>,
}

#[derive(Debug, thiserror::Error)]
pub enum EvalRunError {
    #[error(transparent)]
    Capacity(#[from] CapacityError),
    #[error(transparent)]
    Journal(#[from] ResumeError),
    #[error("eval run filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Resolve `platform`, then `--concurrency`, then one; journal the capacity and each sample; run
/// samples through the bounded scheduler; return every artifact plus per-sample failures.
pub async fn run_eval<R>(request: EvalRunRequest<'_, R>) -> Result<EvalRunReport, EvalRunError>
where
    R: SampleRunner,
{
    let capacity = resolve_capacity(request.capacity_source, request.concurrency)?;
    std::fs::create_dir_all(&request.journal_directory)?;
    let mut journal = ResumeJournal::load_or_default(&request.journal_directory)?;
    let capacity_warning = journal.observe_capacity(capacity);
    journal.save(&request.journal_directory)?;
    let journal = Arc::new(Mutex::new(journal));
    let journal_directory = request.journal_directory;
    let runner = request.runner;
    let mut samples = request.samples;
    for sample in &mut samples {
        sample.concurrency = capacity.concurrency;
        sample.request = sample
            .request
            .take()
            .map(|request| request.with_concurrency(capacity.concurrency));
    }
    let completed = run_bounded(samples, capacity.concurrency, |_index, sample| {
        let journal = Arc::clone(&journal);
        let journal_directory = journal_directory.clone();
        async move {
            (
                sample.sample_id.clone(),
                run_eval_sample(runner, sample, &journal, &journal_directory).await,
            )
        }
    })
    .await?;
    let mut artifacts = Vec::new();
    let mut failures = Vec::new();
    for (_, (sample_id, result)) in completed {
        match result {
            Ok(artifact) => artifacts.push(artifact),
            Err(error) => failures.push(SampleFailure { sample_id, error }),
        }
    }
    Ok(EvalRunReport {
        capacity,
        capacity_warning,
        artifacts,
        failures,
    })
}

struct CompletionContext<'a> {
    journal: &'a Mutex<ResumeJournal>,
    directory: &'a Path,
}

async fn run_eval_sample<R: SampleRunner>(
    runner: &R,
    mut sample: EvalSamplePlan<R>,
    journal: &Mutex<ResumeJournal>,
    journal_directory: &Path,
) -> Result<SampleArtifact, String> {
    let context = CompletionContext {
        journal,
        directory: journal_directory,
    };
    let sample_id = sample.sample_id.clone();
    let (action, current_run_id) = {
        let journal = journal.lock().await;
        (
            journal.action(&sample_id),
            journal.current_run_id(&sample_id).cloned(),
        )
    };
    let recovering_failed = matches!(action, ResumeAction::Recover(_));
    let mut existing_artifact = None;
    if action != ResumeAction::Start {
        if let Ok(existing) = SampleArtifact::read(&sample.directory) {
            if !recovering_failed && current_run_id.as_ref() == Some(&existing.outcome.run_id) {
                let artifact = retry_capture(existing, &sample.directory, sample.capture.take())
                    .await
                    .map_err(|error| error.to_string())?;
                journal_update(journal, journal_directory, |journal| {
                    journal.record_artifact(&artifact);
                })
                .await
                .map_err(|error| error.to_string())?;
                return Ok(artifact);
            }
            existing_artifact = Some(existing);
        }
    }
    let resume_run = match action {
        ResumeAction::Resume(run) | ResumeAction::Recover(run) => Some(run),
        ResumeAction::Skip | ResumeAction::Capture | ResumeAction::Start => None,
    };
    let mut run = match resume_run {
        Some(run) => {
            // A running or failed journal entry normally means the previous controller was
            // interrupted, not that the target already lost the run. Reattach first.
            let mut terminal_failure = None;
            match runner.wait(&run).await {
                Ok(outcome) if outcome.status == SampleStatus::Succeeded => {
                    // The existing run is already successful; replace any stale attempt output.
                    remove_sample_artifact(&sample.directory).map_err(|error| error.to_string())?;
                    return complete_sample(&sample, run.sample_id, outcome, &context).await;
                }
                Ok(outcome) => terminal_failure = Some(outcome),
                Err(error) if error.is_run_lost() => {}
                Err(error) => {
                    let _ = journal_update(journal, journal_directory, |journal| {
                        journal.record_interruption(&sample_id, error.to_string());
                    })
                    .await;
                    return Err(format!("reattach interrupted: {error}"));
                }
            }
            // Only a terminal failed run or a lost run reaches workspace recovery. A successful
            // reattach above never asks the target to resume.
            match runner
                .resume(run.clone(), sample.resume_credentials.clone())
                .await
            {
                Ok(run) => run,
                Err(error) if error.is_transient() => {
                    let _ = journal_update(journal, journal_directory, |journal| {
                        journal.record_interruption(&sample_id, error.to_string());
                    })
                    .await;
                    return Err(format!("resume pending: {error}"));
                }
                Err(error) => match sample.request.take() {
                    Some(request) => runner
                        .submit(request)
                        .await
                        .map_err(|error| error.to_string())?,
                    None => {
                        if let Some(existing) = existing_artifact {
                            if current_run_id.as_ref() == Some(&existing.outcome.run_id) {
                                // No replacement run was created; retain the measured failure.
                                return Ok(existing);
                            }
                        }
                        if let Some(outcome) = terminal_failure {
                            remove_sample_artifact(&sample.directory)
                                .map_err(|error| error.to_string())?;
                            return complete_sample(&sample, run.sample_id, outcome, &context)
                                .await;
                        }
                        let message = format!("resume failed: {error}");
                        let _ = journal_update(journal, journal_directory, |journal| {
                            journal.record_error(&sample_id, &message);
                        })
                        .await;
                        return Err(message);
                    }
                },
            }
        }
        None => {
            // Take the request and submit before touching previous artifacts. A transient
            // submission failure must leave the last measured sample intact.
            let request = sample
                .request
                .take()
                .ok_or_else(|| "sample has no request to start".to_owned())?;
            runner
                .submit(request)
                .await
                .map_err(|error| error.to_string())?
        }
    };
    run.concurrency = sample.concurrency;
    journal_update(journal, journal_directory, |journal| {
        journal.claim(&sample_id, run.clone());
    })
    .await
    .map_err(|error| error.to_string())?;
    // The replacement run is now claimed. Discard prior sample-local evidence before waiting.
    remove_sample_artifact(&sample.directory).map_err(|error| error.to_string())?;
    let outcome = match runner.wait(&run).await {
        Ok(outcome) => outcome,
        Err(error) => {
            let _ = journal_update(journal, journal_directory, |journal| {
                journal.record_interruption(&sample_id, error.to_string());
            })
            .await;
            return Err(error.to_string());
        }
    };
    complete_sample(&sample, run.sample_id, outcome, &context).await
}

async fn complete_sample<R: SampleRunner>(
    sample: &EvalSamplePlan<R>,
    sample_id: String,
    outcome: SampleOutcome,
    context: &CompletionContext<'_>,
) -> Result<SampleArtifact, String> {
    let capture = sample.capture.clone().map(|mut request| {
        request.directory = sample.directory.clone();
        request.receipt = outcome.delivery.clone().unwrap_or_default();
        request
    });
    let artifact = match persist_sample(sample_id, outcome, &sample.directory, capture).await {
        Ok(artifact) => artifact,
        Err(error) => {
            let message = error.to_string();
            let _ = journal_update(context.journal, context.directory, |journal| {
                journal.record_interruption(&sample.sample_id, &message);
            })
            .await;
            return Err(message);
        }
    };
    journal_update(context.journal, context.directory, |journal| {
        journal.record_artifact(&artifact);
    })
    .await
    .map_err(|error| error.to_string())?;
    Ok(artifact)
}

fn remove_sample_artifact(directory: &Path) -> std::io::Result<()> {
    for file in [SAMPLE_FILE, "patch.diff"] {
        match std::fs::remove_file(directory.join(file)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    let judge = directory.join("judge");
    match std::fs::remove_dir_all(&judge) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

async fn retry_capture(
    existing: SampleArtifact,
    directory: &Path,
    capture: Option<CaptureRequest>,
) -> Result<SampleArtifact, SampleExecutionError> {
    if (!existing.capture_pending && existing.capture_error.is_none())
        || existing.outcome.status != SampleStatus::Succeeded
    {
        return Ok(existing);
    }
    let Some(mut request) = capture else {
        return Ok(existing);
    };
    request.directory = directory.to_owned();
    request.receipt = existing.outcome.delivery.clone().unwrap_or_default();
    persist_sample(
        existing.sample_id,
        existing.outcome,
        directory,
        Some(request),
    )
    .await
}

async fn journal_update<T>(
    journal: &Mutex<ResumeJournal>,
    directory: &Path,
    update: impl FnOnce(&mut ResumeJournal) -> T,
) -> Result<T, ResumeError> {
    let mut journal = journal.lock().await;
    let value = update(&mut journal);
    journal.save(directory)?;
    Ok(value)
}

#[cfg(test)]
#[path = "runner_test.rs"]
mod tests;
