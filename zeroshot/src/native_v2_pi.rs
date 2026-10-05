//! Pi CLI adapter for native-v2 graph nodes.
//!
//! One adapter serves the graph-wide Pi provider lane. Admission has already selected the model,
//! effort, session scope, and declared environment for each node. Pi exposes no permission
//! control of its own, so containment and the shared prompt renderer are the isolation boundary,
//! exactly as for the Copilot lane.
//!
//! The adapter drives `pi --mode json`: one process per turn, the prompt on stdin, and an LF-framed
//! JSONL event stream on stdout. Nothing here appends a permission argument, because Pi has no
//! bypass flag to append.

#[path = "native_v2_pi/command.rs"]
mod command;
#[path = "native_v2_pi/gateway_config.rs"]
mod gateway_config;
#[path = "native_v2_pi/session.rs"]
mod session;
#[path = "native_v2_pi/session_id.rs"]
mod session_id;
#[path = "native_v2_pi/transcript.rs"]
mod transcript;
#[path = "native_v2_pi/turn_process.rs"]
mod turn_process;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::execution::process::{HostedProcessPool, ProcessSessionCommand, ProcessStdout};
use crate::native_v2_capsule::provider_process::{
    ClosedSessionFailure, LocalHarnessEnvironment, PI_LOCAL_ENVIRONMENT, ProviderExecution,
    ProviderExecutionFiles, ProviderProcessRunners, provider_redactions, report_provider_error,
    with_driver_detail,
};
use crate::native_v2_contract::PiProvider;
use crate::native_v2_runner::{
    AgentResponse, DriverControl, DriverInvocation, LiveOutput, LiveOutputStream, NodeRunnerError,
    ResolvedEnvironment, render_agent_prompt, resolve_agent_response,
};
use command::{PiCommandRequest, command};
use gateway_config::{base_url as gateway_base_url, write_models};
use session::{PiRunState, PiSession, PiTurnAdvance};
use session_id::{agent_directory, observe_session, session_directory};
use transcript::{PiAttempt, PiEmission, PiResult, PiTranscript};
use turn_process::PiProcessStart;

/// Renders the shared provider-neutral node turn contract.
fn prompt(invocation: &DriverInvocation) -> Result<String, NodeRunnerError> {
    render_agent_prompt(
        invocation.agent_instructions()?,
        &invocation.node.input,
        &invocation.response,
    )
    .map_err(|error| with_driver_detail(error, "Pi prompt could not be serialized"))
}

/// Minimal environment every Pi child needs to launch and run its own tools.
const MINIMAL_ENVIRONMENT_NAMES: [&str; 7] = [
    "HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "TERM",
    "TMPDIR",
    "ZEROSHOT_TOOLS",
];

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PiAdapterConfigError {
    #[error("Pi executable must not be empty")]
    EmptyExecutable,
    #[error("base process environment contains non-minimal name {0}")]
    NonMinimalEnvironment(String),
    #[error("base process environment contains an invalid value")]
    InvalidEnvironment,
}

/// Explicit non-secret environment needed to launch the CLI and its tools.
///
/// The controller supplies this value. It is deliberately not populated from the ambient process,
/// and only a small allowlist can cross this boundary.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct PiProcessEnvironment(BTreeMap<String, String>);

impl PiProcessEnvironment {
    pub fn new(values: BTreeMap<String, String>) -> Result<Self, PiAdapterConfigError> {
        for (name, value) in &values {
            if !MINIMAL_ENVIRONMENT_NAMES.contains(&name.as_str()) {
                return Err(PiAdapterConfigError::NonMinimalEnvironment(name.clone()));
            }
            if value.contains('\0') {
                return Err(PiAdapterConfigError::InvalidEnvironment);
            }
        }
        Ok(Self(values))
    }

    pub(crate) fn clone_values(&self) -> BTreeMap<String, String> {
        self.0.clone()
    }

    pub(crate) fn for_capsule(
        &self,
        runtime_home: &Path,
        default_path: &str,
    ) -> Result<Self, PiAdapterConfigError> {
        let runtime_home = runtime_home
            .to_str()
            .filter(|value| !value.is_empty())
            .ok_or(PiAdapterConfigError::InvalidEnvironment)?;
        let mut values = self.clone_values();
        values.insert("HOME".to_owned(), runtime_home.to_owned());
        values
            .entry("PATH".to_owned())
            .or_insert_with(|| default_path.to_owned());
        Self::new(values)
    }
}

impl std::fmt::Debug for PiProcessEnvironment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("PiProcessEnvironment")
            .field(&self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Runtime capabilities required to launch Pi. None of these paths are durable run data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PiConfig {
    pub provider: PiProvider,
    pub executable: String,
    pub prefix_arguments: Vec<String>,
    pub workspace: PathBuf,
    pub runtime_home: PathBuf,
    /// Current-user home is available only to the built-in local target.
    pub local_user_home: Option<PathBuf>,
    /// Invoking-shell snapshot available only to the built-in local target.
    pub native_environment: LocalHarnessEnvironment,
    pub base_environment: PiProcessEnvironment,
    pub process_pool: HostedProcessPool,
}

/// One graph-wide Pi provider lane.
pub struct PiAdapter {
    provider: PiProvider,
    executable: String,
    prefix_arguments: Vec<String>,
    workspace: PathBuf,
    runtime_home: PathBuf,
    contained: bool,
    native_local: bool,
    base_environment: PiProcessEnvironment,
    runners: ProviderProcessRunners,
    local_environment: BTreeMap<String, String>,
}

impl PiAdapter {
    /// Hosted adapter. It never inherits ambient configuration or a stored login.
    pub fn new(mut configuration: PiConfig) -> Result<Self, PiAdapterConfigError> {
        configuration.local_user_home = None;
        configuration.native_environment = LocalHarnessEnvironment::default();
        let runners = ProviderProcessRunners::hosted(configuration.process_pool);
        Self::configured(configuration, runners, true)
    }

    /// Local adapter. A native-local lane reuses the user's stored Pi login; every other lane
    /// receives a private agent directory and drops the ambient credentials of its lane.
    pub fn new_local(configuration: PiConfig) -> Result<Self, PiAdapterConfigError> {
        let native_local = configuration.provider.natively_authenticated();
        let local_environment = configuration
            .native_environment
            .selected(PI_LOCAL_ENVIRONMENT);
        let mut adapter = Self::configured(configuration, ProviderProcessRunners::local(), false)?;
        adapter.local_environment = local_environment;
        if !native_local {
            adapter
                .local_environment
                .retain(|name, _| !is_pi_credential(name));
        }
        Ok(adapter)
    }

    fn configured(
        configuration: PiConfig,
        runners: ProviderProcessRunners,
        contained: bool,
    ) -> Result<Self, PiAdapterConfigError> {
        let provider = configuration.provider;
        if configuration.executable.is_empty() {
            return Err(PiAdapterConfigError::EmptyExecutable);
        }
        Ok(Self {
            provider: configuration.provider,
            executable: configuration.executable,
            prefix_arguments: configuration.prefix_arguments,
            workspace: configuration.workspace,
            runtime_home: configuration.runtime_home,
            contained,
            native_local: !contained && provider.natively_authenticated(),
            base_environment: configuration.base_environment,
            runners,
            local_environment: BTreeMap::new(),
        })
    }

    /// Per-session private homes. The agent directory holds Pi's configuration, model catalog, and
    /// `models.json`; sessions live in a sibling directory so a node instance keeps one session
    /// across loop revisits.
    fn homes(&self, files: &ProviderExecutionFiles) -> (PathBuf, PathBuf) {
        let home = files.home();
        (agent_directory(home), session_directory(home))
    }

    fn command(
        &self,
        invocation: &DriverInvocation,
        files: &ProviderExecutionFiles,
        session_id: &str,
    ) -> Result<ProcessSessionCommand, NodeRunnerError> {
        let (agent_dir, sessions) = self.homes(files);
        // The gateway lane's endpoint is caller-owned, so it synthesizes its own provider file in
        // the private agent directory. The base URL is read from the declared connection and the
        // secret stays there too, referenced through Pi's own interpolation.
        if self.provider == PiProvider::Gateway && !self.native_local() {
            let base_url = gateway_base_url(&invocation.environment)?;
            write_models(&agent_dir, &base_url).map_err(|error| {
                NodeRunnerError::DriverDetail(format!(
                    "Pi gateway provider configuration failed: {error}"
                ))
            })?;
        }
        command(PiCommandRequest {
            provider: self.provider,
            native_local: self.native_local(),
            contained: self.contained,
            executable: &self.executable,
            prefix_arguments: &self.prefix_arguments,
            invocation,
            files,
            local_environment: &self.local_environment,
            agent_dir: &agent_dir,
            session_dir: &sessions,
            session_id,
        })
    }

    fn redactions(&self, resolved: &ResolvedEnvironment) -> Vec<String> {
        provider_redactions(resolved, &self.local_environment)
    }

    /// True when the lane reuses the user's stored Pi login for its provider.
    #[must_use]
    pub(super) fn native_local(&self) -> bool {
        self.native_local
    }
}

fn is_pi_credential(name: &str) -> bool {
    matches!(
        name,
        command::ANTHROPIC_KEY
            | "ANTHROPIC_AUTH_TOKEN"
            | "ANTHROPIC_OAUTH_TOKEN"
            | command::OPENAI_KEY
            | command::OPENROUTER_KEY
            | command::AWS_BEARER_TOKEN_BEDROCK
    )
}

impl PiAdapter {
    async fn advance_turn(
        &self,
        turn: &PiTurn<'_>,
        state: &mut PiRunState,
        prompt: String,
    ) -> Result<PiTurnAdvance, NodeRunnerError> {
        turn.session
            .core
            .ensure_live(ClosedSessionFailure::SessionLost)?;
        let attempt = self.execute_turn(turn, state, prompt).await?;
        let redactions = self.redactions(&turn.invocation.environment);
        let expected = state.session_id.clone();
        match attempt {
            PiAttempt::Complete(result) => {
                observe_session(result.session_id.as_deref(), &expected)
                    .map_err(|detail| NodeRunnerError::DriverDetail(detail.to_owned()))?;
                resolve_pi_response(turn, result, &redactions).await
            }
            PiAttempt::Failed(failure) => {
                observe_session(failure.session_id.as_deref(), &expected)
                    .map_err(|detail| NodeRunnerError::DriverDetail(detail.to_owned()))?;
                Ok(PiTurnAdvance::ProviderFailure {
                    retryable: failure.retryable,
                    diagnostic: failure.diagnostic,
                })
            }
        }
    }

    async fn execute_turn(
        &self,
        turn: &PiTurn<'_>,
        state: &PiRunState,
        prompt: String,
    ) -> Result<PiAttempt, NodeRunnerError> {
        let files = match turn.execution.prepare(turn.control).await {
            Ok(files) => files,
            Err(error) => {
                return match turn_process::failed_before_start(error, turn.control)? {
                    PiProcessStart::Ready(_) => {
                        unreachable!("a preparation failure cannot be ready")
                    }
                    PiProcessStart::Failed(attempt) => Ok(attempt),
                };
            }
        };
        let mut command = self.command(turn.invocation, &files, &state.session_id)?;
        // The minimal base environment supplies the executable search path and any run tool
        // directory. It is never read from the ambient process, and a declared value wins.
        for (name, value) in self.base_environment.clone_values() {
            command
                .environment
                .entry(name)
                .or_insert_with(|| value.clone());
        }
        let transcript = PiTranscript::new(self.redactions(&turn.invocation.environment));
        let mut process = match turn_process::open(files, command, turn.control).await? {
            PiProcessStart::Ready(process) => process,
            PiProcessStart::Failed(attempt) => return Ok(attempt),
        };
        turn_process::finish_process(&mut process, prompt.as_bytes(), transcript, turn.control)
            .await
    }
}

async fn resolve_pi_response(
    turn: &PiTurn<'_>,
    result: PiResult,
    redactions: &[String],
) -> Result<PiTurnAdvance, NodeRunnerError> {
    let response = resolve_agent_response(&turn.invocation.response, &result.message)?;
    if let Some(error) = response.correction_error() {
        report_provider_error("Pi", &error, redactions, turn.control).await?;
    }
    if matches!(response, AgentResponse::Correction { .. }) {
        turn.control
            .emit(LiveOutput::new(
                LiveOutputStream::System,
                "Pi final output rejected; requesting correction",
            )?)
            .await?;
    }
    Ok(PiTurnAdvance::Response(response))
}

pub(crate) struct PiTurn<'a> {
    pub(super) invocation: &'a DriverInvocation,
    pub(super) session: &'a PiSession,
    pub(super) control: &'a DriverControl,
    pub(super) execution: &'a ProviderExecution<'a>,
}

pub(crate) async fn collect_transcript(
    mut stdout: ProcessStdout,
    transcript: &mut PiTranscript,
    control: &DriverControl,
) -> Result<(), NodeRunnerError> {
    let mut delivery_error = None;
    while let Some(chunk) = stdout.recv().await {
        let emissions = transcript.push(chunk.as_slice());
        if delivery_error.is_none() {
            delivery_error = emit_pi(control, emissions).await.err();
        }
    }
    let emissions = transcript.finish_stream();
    if delivery_error.is_none() {
        delivery_error = emit_pi(control, emissions).await.err();
    }
    delivery_error.map_or(Ok(()), Err)
}

async fn emit_pi(
    control: &DriverControl,
    emissions: Vec<PiEmission>,
) -> Result<(), NodeRunnerError> {
    for emission in emissions {
        control
            .emit(LiveOutput::new(emission.stream, emission.text)?)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "native_v2_pi/tests.rs"]
mod tests;
