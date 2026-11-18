//! Delivered-branch capture and judge checkout.
//!
//! Delivery receipts name the exact published revision. This module fetches that revision through
//! the existing trusted Git transport, writes `patch.diff` relative to the eval's source revision,
//! and can reconstruct the delivered tree from that patch without asking an agent to run Git.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use openengine_cluster_protocol::GitTransportCredentials;
use serde::{Deserialize, Serialize};

use crate::native_v2_delivery::transport::{
    GitTransport, GitTransportConfig, object_format, source_credential,
};
use crate::source_code_provider::{SourceDeliveryError, valid_git_revision};

const DEFAULT_CAPTURE_DEADLINE: Duration = Duration::from_secs(15 * 60);

/// Private credentials for capture. The token is never serialized, logged, or written into the
/// sample directory.
#[derive(Clone, Default)]
pub struct CaptureCredentials {
    pub token: Option<String>,
    pub git: Option<GitTransportCredentials>,
}

impl std::fmt::Debug for CaptureCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CaptureCredentials")
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("git", &self.git)
            .finish()
    }
}

/// Git delivery receipt fields needed by capture. Unknown delivery fields are ignored so receipts
/// from newer delivery workers remain readable.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryReceipt {
    #[serde(default)]
    pub repository: String,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub target_branch: Option<String>,
    #[serde(default)]
    pub head_branch: Option<String>,
    #[serde(default)]
    pub head_revision: Option<String>,
    #[serde(default)]
    pub merge_revision: Option<String>,
}

impl DeliveryReceipt {
    /// Find a delivery receipt in a terminal graph output.
    ///
    /// Graphs commonly wrap the delivery worker result as `{"delivery": {...}}`; a root result is
    /// also accepted. Only the delivery receipt shape is accepted, so an unrelated object such as
    /// a review scope that also carries `headRevision` is never mistaken for delivery.
    #[must_use]
    pub fn from_terminal_output(output: &serde_json::Value) -> Option<Self> {
        fn receipt_like(object: &serde_json::Map<String, serde_json::Value>) -> bool {
            ["headRevision", "repository", "mode", "outcome"]
                .into_iter()
                .all(|key| object.get(key).is_some_and(serde_json::Value::is_string))
        }

        fn find(value: &serde_json::Value, depth: usize) -> Option<DeliveryReceipt> {
            if depth > 8 {
                return None;
            }
            let object = value.as_object()?;
            if receipt_like(object) {
                return serde_json::from_value(value.clone()).ok();
            }
            for key in ["delivery", "result", "output"] {
                if let Some(found) = object.get(key).and_then(|value| find(value, depth + 1)) {
                    return Some(found);
                }
            }
            object.values().find_map(|value| find(value, depth + 1))
        }
        find(output, 0)
    }

    /// The candidate revision for this sample. `mergeRevision` is target-branch metadata and is
    /// never used as the sample patch revision.
    #[must_use]
    pub fn delivered_revision(&self) -> Option<String> {
        self.head_revision
            .as_deref()
            .filter(|revision| valid_git_revision(revision))
            .map(str::to_owned)
    }

    #[must_use]
    pub fn delivered_branch(&self) -> Option<&str> {
        self.head_branch.as_deref()
    }
}

/// Everything needed to capture one delivered sample.
#[derive(Clone)]
pub struct CaptureRequest {
    /// The sample directory; `patch.diff` is written here.
    pub directory: PathBuf,
    /// Source or fork remote that holds the delivered branch.
    pub remote: String,
    /// Revision the candidate started from.
    pub base_revision: String,
    /// Optional branch that advertises the base revision.
    pub base_branch: Option<String>,
    pub receipt: DeliveryReceipt,
    pub credentials: CaptureCredentials,
    pub git_program: PathBuf,
    pub deadline: Duration,
}

impl std::fmt::Debug for CaptureRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CaptureRequest")
            .field("directory", &self.directory)
            .field("remote", &self.remote)
            .field("base_revision", &self.base_revision)
            .field("base_branch", &self.base_branch)
            .field("receipt", &self.receipt)
            .field("credentials", &self.credentials)
            .field("git_program", &self.git_program)
            .field("deadline", &self.deadline)
            .finish()
    }
}

impl CaptureRequest {
    #[must_use]
    pub fn new(
        directory: impl Into<PathBuf>,
        remote: impl Into<String>,
        base_revision: impl Into<String>,
        receipt: DeliveryReceipt,
    ) -> Self {
        Self {
            directory: directory.into(),
            remote: remote.into(),
            base_revision: base_revision.into(),
            base_branch: None,
            receipt,
            credentials: CaptureCredentials::default(),
            git_program: default_git_program(),
            deadline: DEFAULT_CAPTURE_DEADLINE,
        }
    }

    #[must_use]
    pub fn with_base_branch(mut self, branch: impl Into<String>) -> Self {
        self.base_branch = Some(branch.into());
        self
    }

    #[must_use]
    pub fn with_credentials(mut self, credentials: CaptureCredentials) -> Self {
        self.credentials = credentials;
        self
    }

    #[must_use]
    pub fn with_git_program(mut self, git_program: impl Into<PathBuf>) -> Self {
        self.git_program = git_program.into();
        self
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CapturedSample {
    pub base_revision: String,
    pub delivered_revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_branch: Option<String>,
    /// Path relative to the sample directory.
    pub patch: PathBuf,
    pub patch_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<JudgeCheckout>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JudgeCheckout {
    /// Path relative to the sample directory.
    pub directory: PathBuf,
    /// Revision whose tree the judge sees, which is the delivered revision.
    pub revision: String,
    pub base_revision: String,
    /// Path relative to the sample directory.
    pub patch: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("delivery receipt has no delivered revision")]
    MissingRevision,
    #[error("revision {0:?} is not a valid Git revision")]
    InvalidRevision(String),
    #[error("checkout destination is not empty: {0}")]
    DestinationNotEmpty(PathBuf),
    #[error("base and delivered revisions use different Git object formats")]
    ObjectFormatMismatch,
    #[error("Git capture failed: {0}")]
    Git(String),
    #[error("capture filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Source(#[from] SourceDeliveryError),
}

/// Fetch the delivered branch, write `patch.diff`, and return its provenance.
pub async fn capture_patch(request: &CaptureRequest) -> Result<CapturedSample, CaptureError> {
    let delivered_revision = request
        .receipt
        .delivered_revision()
        .ok_or(CaptureError::MissingRevision)?;
    let delivered_branch = request.receipt.delivered_branch().map(str::to_owned);
    validate_revisions(&request.base_revision, &delivered_revision)?;
    std::fs::create_dir_all(&request.directory)?;
    let workspace = tempfile::Builder::new()
        .prefix(".capture-")
        .tempdir_in(&request.directory)?;
    let transport = open_transport(request, workspace.path())?;
    let credential = credential(request);
    initialize(
        &transport,
        request,
        credential,
        object_format(&delivered_revision),
    )
    .await?;
    ensure_revision(
        &transport,
        request,
        RevisionLookup {
            revision: &delivered_revision,
            branch: delivered_branch.as_deref(),
        },
        credential,
    )
    .await?;
    ensure_revision(
        &transport,
        request,
        RevisionLookup {
            revision: &request.base_revision,
            branch: base_branch(request),
        },
        credential,
    )
    .await?;
    let patch_path = request.directory.join("patch.diff");
    let patch_bytes = write_diff(
        &transport,
        &request.base_revision,
        &delivered_revision,
        &patch_path,
    )
    .await?;
    Ok(CapturedSample {
        base_revision: request.base_revision.clone(),
        delivered_revision,
        delivered_branch,
        patch: PathBuf::from("patch.diff"),
        patch_bytes,
        mode: request.receipt.mode.clone(),
        outcome: request.receipt.outcome.clone(),
        judge: None,
    })
}

/// Reconstruct the delivered tree at `destination` by checking out the base revision and applying
/// the captured `patch.diff`. The judge is expected to inspect this checkout read-only.
pub async fn checkout_patch(
    request: &CaptureRequest,
    destination: &Path,
) -> Result<JudgeCheckout, CaptureError> {
    let delivered_revision = request
        .receipt
        .delivered_revision()
        .ok_or(CaptureError::MissingRevision)?;
    validate_revisions(&request.base_revision, &delivered_revision)?;
    if destination.exists() && std::fs::read_dir(destination)?.next().is_some() {
        return Err(CaptureError::DestinationNotEmpty(destination.to_owned()));
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    // Build the complete tree in a sibling staging directory. A failed fetch or patch leaves no
    // partial destination, so a later capture retry can start cleanly.
    let staging = tempfile::Builder::new()
        .prefix(".checkout-stage-")
        .tempdir_in(parent)?;
    let workspace = staging.path().to_owned();
    let private = tempfile::Builder::new()
        .prefix(".checkout-private-")
        .tempdir_in(parent)?;
    std::fs::create_dir_all(private.path().join("home"))?;
    std::fs::create_dir_all(private.path().join("private"))?;
    let transport = GitTransport::new(
        GitTransportConfig {
            git_program: request.git_program.clone(),
            workspace: workspace.clone(),
            home_directory: private.path().join("home"),
            private_directory: private.path().join("private"),
            identity: None,
            local_environment: None,
            deadline: request.deadline,
        },
        request.remote.clone(),
    );
    let credential = credential(request);
    initialize(
        &transport,
        request,
        credential,
        object_format(&delivered_revision),
    )
    .await?;
    advertise_base(&transport, request, credential).await?;
    transport
        .success(
            &["checkout", "--detach", &request.base_revision],
            credential,
        )
        .await?;
    let patch = request.directory.join("patch.diff");
    let patch_bytes = std::fs::read(&patch)?;
    if !patch_bytes.iter().all(u8::is_ascii_whitespace) {
        let patch = patch
            .to_str()
            .ok_or_else(|| CaptureError::Io(std::io::Error::other("patch path is not UTF-8")))?;
        transport
            .success(
                &["apply", "--binary", "--index", "--whitespace=nowarn", patch],
                credential,
            )
            .await?;
    }
    if destination.exists() {
        std::fs::remove_dir(destination)?;
    }
    std::fs::rename(&workspace, destination)?;
    Ok(JudgeCheckout {
        directory: destination
            .file_name()
            .map_or_else(|| destination.to_owned(), PathBuf::from),
        revision: delivered_revision,
        base_revision: request.base_revision.clone(),
        patch: PathBuf::from("patch.diff"),
    })
}

fn validate_revisions(base: &str, delivered: &str) -> Result<(), CaptureError> {
    for revision in [base, delivered] {
        if !valid_git_revision(revision) {
            return Err(CaptureError::InvalidRevision(revision.to_owned()));
        }
    }
    if base.len() != delivered.len() {
        return Err(CaptureError::ObjectFormatMismatch);
    }
    Ok(())
}

async fn write_diff(
    transport: &GitTransport,
    base_revision: &str,
    delivered_revision: &str,
    path: &Path,
) -> Result<u64, CaptureError> {
    let config = transport.config();
    let mut temporary = tempfile::NamedTempFile::new_in(
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?;
    let stdout = temporary.as_file().try_clone()?;
    let mut command = tokio::process::Command::new(&config.git_program);
    command
        .current_dir(&config.workspace)
        .env_clear()
        .env("HOME", &config.home_directory)
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_TERMINAL_PROMPT", "0")
        .args([
            "diff",
            "--binary",
            "--no-ext-diff",
            "--no-textconv",
            "--full-index",
            base_revision,
            delivered_revision,
        ])
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = command.spawn()?;
    let output = tokio::time::timeout(config.deadline, child.wait_with_output())
        .await
        .map_err(|_| CaptureError::Git("git diff exceeded its deadline".to_owned()))??;
    if !output.status.success() {
        let mut stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        stderr.truncate(4_096);
        return Err(CaptureError::Git(format!(
            "git diff failed with {}: {stderr}",
            output.status
        )));
    }
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist(path)
        .map_err(|error| CaptureError::Io(error.error))?;
    Ok(std::fs::metadata(path)?.len())
}

fn default_git_program() -> PathBuf {
    let environment = std::env::vars().collect::<BTreeMap<_, _>>();
    let directory = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    crate::execution::platform::find_executable("git", &environment, &directory)
        .unwrap_or_else(|| PathBuf::from("git"))
}

fn open_transport(
    request: &CaptureRequest,
    workspace: &Path,
) -> Result<GitTransport, CaptureError> {
    let home = workspace.join(".home");
    let private = workspace.join(".private");
    std::fs::create_dir_all(&home)?;
    std::fs::create_dir_all(&private)?;
    Ok(GitTransport::new(
        GitTransportConfig {
            git_program: request.git_program.clone(),
            workspace: workspace.to_owned(),
            home_directory: home,
            private_directory: private,
            identity: None,
            local_environment: None,
            deadline: request.deadline,
        },
        request.remote.clone(),
    ))
}

fn credential(
    request: &CaptureRequest,
) -> crate::source_code_provider::SourceDeliveryCredential<'_> {
    source_credential(
        request.credentials.token.as_deref().unwrap_or(""),
        request.credentials.git.as_ref(),
    )
}

async fn initialize(
    transport: &GitTransport,
    request: &CaptureRequest,
    credential: crate::source_code_provider::SourceDeliveryCredential<'_>,
    object_format: &str,
) -> Result<(), CaptureError> {
    transport
        .success(&["init", "--quiet", object_format], credential)
        .await?;
    transport
        .success(
            &["remote", "add", "origin", request.remote.as_str()],
            credential,
        )
        .await?;
    Ok(())
}

struct RevisionLookup<'a> {
    revision: &'a str,
    branch: Option<&'a str>,
}

async fn ensure_revision(
    transport: &GitTransport,
    request: &CaptureRequest,
    lookup: RevisionLookup<'_>,
    credential: crate::source_code_provider::SourceDeliveryCredential<'_>,
) -> Result<(), CaptureError> {
    if has_commit(transport, lookup.revision, credential).await? {
        return Ok(());
    }
    if let Some(branch) = lookup.branch {
        fetch_branch(transport, request, branch, credential).await?;
    }
    if has_commit(transport, lookup.revision, credential).await? {
        return Ok(());
    }
    transport.fetch_exact(lookup.revision, credential).await?;
    Ok(())
}

async fn advertise_base(
    transport: &GitTransport,
    request: &CaptureRequest,
    credential: crate::source_code_provider::SourceDeliveryCredential<'_>,
) -> Result<(), CaptureError> {
    ensure_revision(
        transport,
        request,
        RevisionLookup {
            revision: &request.base_revision,
            branch: base_branch(request),
        },
        credential,
    )
    .await
}

fn base_branch(request: &CaptureRequest) -> Option<&str> {
    request
        .base_branch
        .as_deref()
        .or(request.receipt.target_branch.as_deref())
        .or(request.receipt.head_branch.as_deref())
}

async fn fetch_branch(
    transport: &GitTransport,
    request: &CaptureRequest,
    branch: &str,
    credential: crate::source_code_provider::SourceDeliveryCredential<'_>,
) -> Result<(), CaptureError> {
    transport
        .success(
            &[
                "fetch",
                "--no-tags",
                "--quiet",
                "--no-write-fetch-head",
                "--",
                request.remote.as_str(),
                &format!("refs/heads/{branch}"),
            ],
            credential,
        )
        .await?;
    Ok(())
}

async fn has_commit(
    transport: &GitTransport,
    revision: &str,
    credential: crate::source_code_provider::SourceDeliveryCredential<'_>,
) -> Result<bool, CaptureError> {
    let object = format!("{revision}^{{commit}}");
    Ok(transport
        .success(&["cat-file", "-e", &object], credential)
        .await
        .is_ok())
}

#[cfg(test)]
#[path = "capture_test.rs"]
mod tests;
