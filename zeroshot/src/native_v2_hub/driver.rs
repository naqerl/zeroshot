use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::process::Command;
use zeroshot_engine::native_v2_cli::{ERROR_FORMAT_ENV, JSON_ERROR_FORMAT};

use openengine_cluster_protocol::TARGET_DISCOVERY_PATH;

use super::error::HubError;
use super::model::{Job, LOCAL_MEMBER};
use crate::native_v2_target::{FileTargetRegistry, TargetRegistry};

/// One placement attempt of a job on a member.
pub(crate) struct SubmitRequest<'a> {
    pub member: &'a str,
    pub job: &'a Job,
    pub submission_key: String,
}

/// Target operations the hub delegates to the existing CLI contract.
#[async_trait]
pub(crate) trait HubDriver: Send + Sync {
    /// Runs reported by `zeroshot list` for one member.
    async fn list_runs(&self, member: &str) -> Result<Vec<Value>, HubError>;
    /// Detached `zeroshot run`; returns the run ID from its receipt.
    async fn submit(&self, request: SubmitRequest<'_>) -> Result<String, HubError>;
    /// `zeroshot status` of one run.
    async fn status(&self, member: &str, run_id: &str) -> Result<Value, HubError>;
    /// `zeroshot force-stop` of one run, the same operation `zeroshot hub force-stop` uses.
    async fn stop(&self, _member: &str, _run_id: &str) -> Result<(), HubError> {
        Ok(())
    }
    /// Capability kinds advertised by a named target's discovery document.
    async fn capabilities(&self, member: &str) -> Result<Vec<String>, HubError>;
    /// Complete standard output of a read-only run command (`report`, `status`, `watch`,
    /// `logs`) routed to `member`, for the archive kept before a pool server is deleted.
    async fn capture(&self, member: &str, arguments: Vec<String>) -> Result<String, HubError>;
}

/// Delegates to this executable (or `ZEROSHOT_HUB_EXECUTABLE`) in a child process, so every hub
/// operation uses exactly the same target registry, authentication, and admission as the CLI.
pub(crate) struct ProcessDriver {
    executable: PathBuf,
    timeout: Duration,
    /// Limit of one archive capture: a full watch or log replay of a finished run.
    capture_timeout: Duration,
    registry: FileTargetRegistry,
    client: reqwest::Client,
}

impl ProcessDriver {
    pub(crate) fn production(registry_path: PathBuf) -> Result<Self, HubError> {
        Self::new(hub_executable()?, registry_path, Duration::from_secs(60))
    }

    /// Delegate to `executable`, giving each command `timeout` to finish.
    pub(crate) fn new(
        executable: PathBuf,
        registry_path: PathBuf,
        timeout: Duration,
    ) -> Result<Self, HubError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("zeroshot-hub/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| HubError::storage("HTTP client could not be initialized", error))?;
        Ok(Self {
            executable,
            timeout,
            capture_timeout: timeout.saturating_mul(10),
            registry: FileTargetRegistry::new(registry_path),
            client,
        })
    }

    async fn call(
        &self,
        arguments: Vec<String>,
        cwd: Option<&PathBuf>,
    ) -> Result<String, HubError> {
        self.call_within(arguments, cwd, self.timeout).await
    }

    async fn call_within(
        &self,
        arguments: Vec<String>,
        cwd: Option<&PathBuf>,
        timeout: Duration,
    ) -> Result<String, HubError> {
        let mut command = Command::new(&self.executable);
        command
            .args(&arguments)
            .env(ERROR_FORMAT_ENV, JSON_ERROR_FORMAT)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        let operation = arguments.first().cloned().unwrap_or_default();
        let output = tokio::time::timeout(timeout, command.output())
            .await
            .map_err(|_| HubError::Unanswered(format!("zeroshot {operation} timed out")))?
            .map_err(|error| {
                target_error(
                    format!("zeroshot {operation} could not start: {error}"),
                    None,
                )
            })?;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        if output.status.success() {
            return Ok(stdout);
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        let diagnostic = stderr
            .lines()
            .rev()
            .find_map(|line| serde_json::from_str::<Value>(line).ok());
        let message = diagnostic
            .as_ref()
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .map_or_else(|| stderr.trim().to_owned(), str::to_owned);
        Err(target_error(
            format!("zeroshot {operation}: {message}"),
            diagnostic,
        ))
    }
}

#[async_trait]
impl HubDriver for ProcessDriver {
    async fn list_runs(&self, member: &str) -> Result<Vec<Value>, HubError> {
        let mut arguments = vec!["list".to_owned()];
        arguments.extend(route(member));
        let stdout = self.call(arguments, None).await?;
        let document = last_json(&stdout)
            .ok_or_else(|| target_error("zeroshot list returned no JSON".to_owned(), None))?;
        Ok(document
            .get("runs")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    async fn submit(&self, request: SubmitRequest<'_>) -> Result<String, HubError> {
        let mut arguments = vec!["run".to_owned()];
        arguments.extend(request.job.run_args.iter().cloned());
        arguments.extend(route(request.member));
        if request.member != LOCAL_MEMBER {
            if let Some(revision) = &request.job.pinned_revision {
                arguments.extend(["--revision".to_owned(), revision.clone()]);
            }
        }
        arguments.extend([
            "--submission-key".to_owned(),
            request.submission_key,
            "--detach".to_owned(),
        ]);
        let stdout = self
            .call(arguments, Some(&request.job.cwd))
            .await
            .map_err(unanswered_when_uncertain)?;
        stdout
            .lines()
            .rev()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find_map(|value| {
                value
                    .get("runId")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .ok_or_else(|| HubError::Unanswered("zeroshot run returned no run receipt".to_owned()))
    }

    async fn status(&self, member: &str, run_id: &str) -> Result<Value, HubError> {
        let mut arguments = vec!["status".to_owned(), run_id.to_owned()];
        arguments.extend(route(member));
        let stdout = self.call(arguments, None).await?;
        last_json(&stdout)
            .ok_or_else(|| target_error("zeroshot status returned no JSON".to_owned(), None))
    }

    async fn stop(&self, member: &str, run_id: &str) -> Result<(), HubError> {
        self.call(force_stop_arguments(member, run_id), None)
            .await?;
        Ok(())
    }

    async fn capabilities(&self, member: &str) -> Result<Vec<String>, HubError> {
        let record = self
            .registry
            .get(member)
            .map_err(|error| target_error(format!("{member}: {error}"), None))?;
        let url = format!(
            "{}{TARGET_DISCOVERY_PATH}",
            record.origin.trim_end_matches('/')
        );
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|error| target_error(format!("discovery {url} failed: {error}"), None))?;
        if !response.status().is_success() {
            return Err(target_error(
                format!(
                    "discovery {url} returned HTTP {}",
                    response.status().as_u16()
                ),
                None,
            ));
        }
        let document = response
            .json::<Value>()
            .await
            .map_err(|error| target_error(format!("discovery {url} is not JSON: {error}"), None))?;
        Ok(discovery_capabilities(&document))
    }

    async fn capture(&self, member: &str, mut arguments: Vec<String>) -> Result<String, HubError> {
        arguments.extend(route(member));
        self.call_within(arguments, None, self.capture_timeout)
            .await
    }
}

/// Every advertised `kind`: the document kind and each extension marker's kind.
pub(crate) fn discovery_capabilities(document: &Value) -> Vec<String> {
    let mut kinds = document
        .get("extensions")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|extensions| extensions.values())
        .chain(std::iter::once(document))
        .filter_map(|value| value.get("kind").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    kinds.sort();
    kinds.dedup();
    kinds
}

/// `--target NAME` for named members; nothing for the local controller.
pub(crate) fn route(member: &str) -> Vec<String> {
    if member == LOCAL_MEMBER {
        Vec::new()
    } else {
        vec!["--target".to_owned(), member.to_owned()]
    }
}

/// The exact `zeroshot` invocation used by `hub force-stop` and by deadline/stall enforcement.
pub(crate) fn force_stop_arguments(member: &str, run_id: &str) -> Vec<String> {
    let mut arguments = vec!["force-stop".to_owned(), run_id.to_owned()];
    arguments.extend(route(member));
    arguments
}

pub(crate) fn hub_executable() -> Result<PathBuf, HubError> {
    if let Some(path) =
        std::env::var_os("ZEROSHOT_HUB_EXECUTABLE").filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(path));
    }
    std::env::current_exe()
        .map_err(|error| HubError::storage("executable path is unavailable", error))
}

fn last_json(stdout: &str) -> Option<Value> {
    stdout
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<Value>(line).ok())
}

/// A failed `zeroshot run` that may still have reached the target: its transport failed
/// (`target.unavailable`) or its answer was unreadable (a `protocol` diagnostic). Refusals the
/// target or the CLI reported (invalid requests, submission conflicts, backend errors) stay
/// rejections.
fn unanswered_when_uncertain(error: HubError) -> HubError {
    let uncertain = matches!(
        &error,
        HubError::Target { diagnostic: Some(diagnostic), .. }
            if diagnostic.get("kind") == Some(&Value::from("protocol"))
                || diagnostic.get("code") == Some(&Value::from("target.unavailable"))
    );
    if uncertain {
        HubError::Unanswered(error.to_string())
    } else {
        error
    }
}

const fn target_error(message: String, diagnostic: Option<Value>) -> HubError {
    HubError::Target {
        message,
        diagnostic,
    }
}
