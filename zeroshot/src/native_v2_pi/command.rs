//! Pi command construction and provider credential wiring.
//!
//! The node prompt travels on stdin so a large input payload can never exceed the platform
//! argument limit. Pi resolves a piped prompt only at stdin EOF, which is what the shared process
//! helpers already do when they close the input stream.

use std::collections::BTreeMap;
use std::path::Path;

use crate::execution::driver::WorkspaceCapability;
use crate::execution::process::ProcessSessionCommand;
use crate::native_v2_capsule::gateway;
use crate::native_v2_capsule::provider_process::{
    ProviderExecutionFiles, agent_workspace_access, effort_token, with_driver_detail,
};
use crate::native_v2_contract::{NodeRuntimeBinding, PiProvider};
use crate::native_v2_runner::{DriverInvocation, NodeRunnerError, ResolvedEnvironment};
use crate::worker_catalog::ReasoningEffort;

pub(super) const AWS_BEARER_TOKEN_BEDROCK: &str = "AWS_BEARER_TOKEN_BEDROCK";
pub(super) const AWS_REGION: &str = "AWS_REGION";
pub(super) const OPENROUTER_KEY: &str = "OPENROUTER_API_KEY";
pub(super) const ANTHROPIC_KEY: &str = "ANTHROPIC_API_KEY";
/// Caller-owned Anthropic Messages endpoint. Pi ignores this variable for endpoint selection, so
/// the adapter turns a declared value into a provider override.
pub(super) const ANTHROPIC_BASE_URL: &str = "ANTHROPIC_BASE_URL";
const ANTHROPIC_AUTH: &str = "ANTHROPIC_AUTH_TOKEN";
const ANTHROPIC_OAUTH: &str = "ANTHROPIC_OAUTH_TOKEN";
pub(super) const OPENAI_KEY: &str = "OPENAI_API_KEY";

/// Pi's agent directory. Contained runs and every non-native-local lane point this at a private
/// home so no ambient configuration, model catalog, or stored login can be inherited.
pub(super) const AGENT_DIR: &str = "PI_CODING_AGENT_DIR";
/// Pi's session storage. Always passed explicitly, because Pi reads a project `sessionDir`
/// setting before it resolves project trust and declining trust cannot undo that lookup.
pub(super) const SESSION_DIR: &str = "PI_CODING_AGENT_SESSION_DIR";
pub(super) const PI_OFFLINE: &str = "PI_OFFLINE";
pub(super) const PI_TELEMETRY: &str = "PI_TELEMETRY";

/// Credentials Pi accepts for the anthropic lane: an API key, an OAuth API credential, or bearer
/// authentication. Pi treats these as distinct forms, so any one satisfies the lane.
const ANTHROPIC_CREDENTIALS: [&str; 3] = [ANTHROPIC_KEY, ANTHROPIC_AUTH, ANTHROPIC_OAUTH];
const OPENAI_CREDENTIALS: [&str; 1] = [OPENAI_KEY];

/// Credentials belonging to any other lane. A declared value from one of these must not stand in
/// for the selected lane's credential.
const FOREIGN_CREDENTIALS: [&str; 5] = [
    ANTHROPIC_KEY,
    ANTHROPIC_AUTH,
    ANTHROPIC_OAUTH,
    OPENAI_KEY,
    OPENROUTER_KEY,
];

/// Names the adapter reserves. A declared connection colliding with one is a provider failure
/// rather than a silent override.
const RESERVED: [&str; 6] = [
    AGENT_DIR,
    SESSION_DIR,
    PI_OFFLINE,
    PI_TELEMETRY,
    "HOME",
    "TMPDIR",
];

/// Everything one Pi turn needs to build its argv. The caller-owned model and effort are supplied
/// separately from the invocation so argv construction stays pure and directly testable.
pub(super) struct PiCommandInput<'a> {
    pub(super) provider: PiProvider,
    pub(super) model: &'a str,
    pub(super) effort: Option<ReasoningEffort>,
    pub(super) session_id: &'a str,
    /// Private per-session directory; the agent directory is its parent.
    pub(super) session_dir: &'a Path,
    /// True only for hosted placements, which never inherit ambient settings.
    pub(super) contained: bool,
}

/// Pi's own provider identifier for one admitted lane. The gateway lane reuses the built-in
/// `openai` provider with a base-URL override rather than registering a provider, which keeps the
/// caller-owned model identifiers resolvable without a Zeroshot-owned model catalog.
const fn provider_name(provider: PiProvider) -> &'static str {
    match provider {
        PiProvider::Anthropic => "anthropic",
        PiProvider::OpenAi | PiProvider::Gateway => "openai",
        PiProvider::OpenRouter => "openrouter",
        PiProvider::Bedrock => "amazon-bedrock",
    }
}

/// Per-turn argv. Contained runs additionally pin the default tool set and refuse project-local
/// resources so a workspace cannot inject extensions, MCP servers, skills, or system prompts.
/// Context files stay enabled in every placement because repository-declared setup is part of the
/// shared prompt contract.
pub(super) fn pi_arguments(prefix_arguments: &[String], input: &PiCommandInput<'_>) -> Vec<String> {
    let mut argv = prefix_arguments.to_vec();
    argv.extend(["--mode".to_owned(), "json".to_owned()]);
    argv.extend([
        "--provider".to_owned(),
        provider_name(input.provider).to_owned(),
    ]);
    argv.extend(["--model".to_owned(), input.model.to_owned()]);
    argv.extend(["--session-dir".to_owned(), text(input.session_dir)]);
    argv.extend(["--session-id".to_owned(), input.session_id.to_owned()]);
    if let Some(effort) = input.effort {
        argv.extend(["--thinking".to_owned(), effort_token(effort).to_owned()]);
    }
    if input.contained {
        argv.extend(["--no-extensions".to_owned()]);
        argv.extend(["--no-approve".to_owned()]);
        argv.extend(["--tools".to_owned(), "read,bash,edit,write".to_owned()]);
    }
    argv
}

/// Everything one turn needs to build its process. A request struct keeps the launch inputs in one
/// named value rather than raising the repository's four-parameter Clippy ceiling.
pub(super) struct PiCommandRequest<'a> {
    pub(super) provider: PiProvider,
    /// True when the lane reuses the user's stored Pi login, so no agent directory is set.
    pub(super) native_local: bool,
    /// True only for hosted placements, which never inherit ambient settings.
    pub(super) contained: bool,
    pub(super) executable: &'a str,
    pub(super) prefix_arguments: &'a [String],
    pub(super) invocation: &'a DriverInvocation,
    pub(super) files: &'a ProviderExecutionFiles,
    /// Invoking-shell snapshot for the local target. It fills only names the adapter and the
    /// declared connections left unset, and credential validation sees it.
    pub(super) local_environment: &'a BTreeMap<String, String>,
    /// Provider-private agent directory: configuration, model catalog, and `models.json`.
    pub(super) agent_dir: &'a Path,
    /// Provider-private session storage, always explicit so project settings cannot redirect it.
    pub(super) session_dir: &'a Path,
    pub(super) session_id: &'a str,
}

pub(super) fn command(
    request: PiCommandRequest<'_>,
) -> Result<ProcessSessionCommand, NodeRunnerError> {
    let PiCommandRequest {
        provider,
        native_local,
        contained,
        executable,
        prefix_arguments,
        invocation,
        files,
        local_environment,
        agent_dir,
        session_dir,
        session_id,
    } = request;
    if executable.is_empty() {
        return Err(NodeRunnerError::DriverDetail(
            "Pi executable must not be empty".to_owned(),
        ));
    }
    let NodeRuntimeBinding::Agent { model, effort, .. } = &invocation.node.binding else {
        return Err(NodeRunnerError::DriverDetail(
            "Pi command requires an agent runtime binding".to_owned(),
        ));
    };
    let access = agent_workspace_access(invocation.role)
        .map_err(|error| with_driver_detail(error, "Pi workspace policy rejected the node role"))?;
    let argv = pi_arguments(
        prefix_arguments,
        &PiCommandInput {
            provider,
            model: model.as_str(),
            effort: *effort,
            session_id,
            session_dir,
            contained,
        },
    );
    let mut environment = EnvironmentRequest {
        provider,
        native_local,
        contained,
        agent_dir,
        session_dir,
        resolved: &invocation.environment,
        local_environment,
    }
    .build()
    .map_err(|error| with_driver_detail(error, "Pi provider environment is invalid"))?;
    environment.insert(
        "TMPDIR".to_owned(),
        files
            .scratch_text()
            .map_err(|error| NodeRunnerError::DriverDetail(error.to_string()))?,
    );
    Ok(ProcessSessionCommand {
        program: executable.to_owned(),
        argv,
        environment,
        workspace: WorkspaceCapability {
            current_dir: files.workspace.clone(),
            mode: access,
        },
        deadline: None,
    })
}

/// The declared and harness-resolved values a Pi child needs, resolved once per turn.
struct EnvironmentRequest<'a> {
    provider: PiProvider,
    native_local: bool,
    contained: bool,
    agent_dir: &'a Path,
    session_dir: &'a Path,
    resolved: &'a ResolvedEnvironment,
    local_environment: &'a BTreeMap<String, String>,
}

impl EnvironmentRequest<'_> {
    /// Pi resolves credentials from its own agent directory ahead of the process environment, so a
    /// native-local lane leaves `PI_CODING_AGENT_DIR` unset and reuses the user's stored login while
    /// every other lane receives a private agent directory.
    fn build(self) -> Result<BTreeMap<String, String>, NodeRunnerError> {
        let Self {
            provider,
            native_local,
            contained,
            agent_dir,
            session_dir,
            resolved,
            local_environment,
        } = self;
        validate_reserved(resolved)?;
        let mut environment = BTreeMap::from([
            (SESSION_DIR.to_owned(), text(session_dir)),
            (PI_TELEMETRY.to_owned(), "0".to_owned()),
        ]);
        if !native_local {
            environment.insert(AGENT_DIR.to_owned(), text(agent_dir));
        }
        if contained {
            // Pi expands this into a suppressed version check itself. It gates only Pi's own startup
            // traffic, so a contained run stays deterministic without affecting provider requests.
            environment.insert(PI_OFFLINE.to_owned(), "1".to_owned());
        }
        extend_declared(&mut environment, resolved)?;
        // Ambient shell values fill only what the adapter and the declared connections left
        // unset, so an authored value always wins. They are merged before credential validation
        // so a credential belonging to another lane is refused rather than smuggled into the
        // selected provider's process.
        for (name, value) in local_environment {
            environment
                .entry(name.clone())
                .or_insert_with(|| value.clone());
        }
        configure_provider(&mut environment, provider, native_local)?;
        Ok(environment)
    }
}

/// Builds the child environment. Pi resolves credentials from its own agent directory ahead of
/// the process environment, so a native-local lane leaves `PI_CODING_AGENT_DIR` unset and reuses
/// the user's stored login, while every other lane receives a private agent directory.
/// Rejects a declared connection that would shadow adapter-reserved process configuration.
fn validate_reserved(resolved: &ResolvedEnvironment) -> Result<(), NodeRunnerError> {
    for (name, _) in resolved.iter() {
        if RESERVED.contains(&name.as_str()) {
            return Err(NodeRunnerError::DriverDetail(format!(
                "Pi declared environment conflicts with reserved process configuration {name}"
            )));
        }
    }
    Ok(())
}

fn extend_declared(
    environment: &mut BTreeMap<String, String>,
    resolved: &ResolvedEnvironment,
) -> Result<(), NodeRunnerError> {
    for (name, value) in resolved.iter() {
        if value.contains('\0') || environment.contains_key(name.as_str()) {
            return Err(NodeRunnerError::Driver);
        }
        environment.insert(name.as_str().to_owned(), value.to_owned());
    }
    Ok(())
}

pub(super) fn configure_provider(
    environment: &mut BTreeMap<String, String>,
    provider: PiProvider,
    native_local: bool,
) -> Result<(), NodeRunnerError> {
    match provider {
        PiProvider::Anthropic => accept_lane(environment, &ANTHROPIC_CREDENTIALS, native_local)?,
        PiProvider::OpenAi => accept_lane(environment, &OPENAI_CREDENTIALS, native_local)?,
        PiProvider::OpenRouter => accept_lane(environment, &[OPENROUTER_KEY], native_local)?,
        PiProvider::Bedrock => accept_bedrock(environment)?,
        PiProvider::Gateway => accept_gateway(environment)?,
    }
    Ok(())
}

/// Requires a nonempty credential from the selected lane and refuses credentials belonging to
/// another lane, so an ambient or mistakenly declared value can never authenticate a different
/// provider than the caller selected.
///
/// A native-local lane authenticates from Pi's own stored credential, so it requires no declared
/// field. Declared values are still validated when present: an authored empty value must fail
/// closed instead of silently falling back to that stored login.
fn accept_lane(
    environment: &BTreeMap<String, String>,
    accepted: &[&str],
    native_local: bool,
) -> Result<(), NodeRunnerError> {
    reject_empty(environment, accepted)?;
    let foreign: Vec<&str> = FOREIGN_CREDENTIALS
        .iter()
        .copied()
        .filter(|name| !accepted.contains(name))
        .collect();
    if accepted.iter().any(|name| environment.contains_key(*name)) || native_local {
        return validate_absent(environment, &foreign);
    }
    Err(NodeRunnerError::Driver)
}

/// Bedrock is never a native-local lane, so its credential and region are always required together.
fn accept_bedrock(environment: &BTreeMap<String, String>) -> Result<(), NodeRunnerError> {
    reject_empty(environment, &[AWS_BEARER_TOKEN_BEDROCK, AWS_REGION])?;
    let complete =
        environment.contains_key(AWS_BEARER_TOKEN_BEDROCK) && environment.contains_key(AWS_REGION);
    if !complete {
        return Err(NodeRunnerError::Driver);
    }
    validate_absent(environment, &FOREIGN_CREDENTIALS[..4])
}

/// The gateway lane pins the OpenAI Responses implementation through Pi's own `models.json`, so
/// the caller's base URL and key stay in the declared connection and the synthesized file holds no
/// secret. Pinning the discriminant is an adapter-owned choice, never protocol detection.
fn accept_gateway(environment: &BTreeMap<String, String>) -> Result<(), NodeRunnerError> {
    let _ = gateway::connection(environment)?;
    validate_absent(environment, &FOREIGN_CREDENTIALS[..4])
}

/// A present-but-empty declared credential fails closed instead of falling back to a stored login.
fn reject_empty(
    environment: &BTreeMap<String, String>,
    names: &[&str],
) -> Result<(), NodeRunnerError> {
    for name in names {
        if environment.get(*name).is_some_and(|value| value.is_empty()) {
            return Err(NodeRunnerError::Driver);
        }
    }
    Ok(())
}

fn validate_absent(
    environment: &BTreeMap<String, String>,
    names: &[&str],
) -> Result<(), NodeRunnerError> {
    if names.iter().any(|name| environment.contains_key(*name)) {
        return Err(NodeRunnerError::Driver);
    }
    Ok(())
}

fn text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
