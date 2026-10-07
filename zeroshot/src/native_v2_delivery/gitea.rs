//! Gitea and Forgejo Git delivery over the shared Gitea-compatible REST API.
//!
//! The authority performs authenticated repository access, pushes the candidate branch, opens or
//! updates the run pull request, and reads the branch's required checks/status. It reuses the
//! bounded Git command plumbing shared with the GitHub authority, so credentials never reach an
//! agent and every network operation stays inside the configured deadline.

use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::process::Command;

use super::command::{capture, local_git_command_with_identity, GitCommandFailure};
use super::git::{GitError, SystemGit};
use super::{
    valid_revision, GitHubAuthorityError, GitHubChecks, GitHubConflictMaterialization,
    GitHubConflictOutcome, GitHubConflictRequest, GitHubCredential, GitHubDeliveryAuthority,
    GitHubDeliveryRead, GitHubDeliverySnapshot, GitHubHeadReconciliation,
    GitHubMergeRequestOutcome, GitHubPushRequest, GitHubReconciliationOutcome,
    GitHubReviewFeedback, GitHubReviewFeedbackItem, GitHubReviewObservation, GitHubReviewReceipt,
    GitHubReviewRequest, GitHubReviewState, GitHubTargetIntegration, GitHubTargetReconciliation,
};

const DEFAULT_API_DEADLINE: Duration = Duration::from_secs(2 * 60);
const DEFAULT_PUSH_DEADLINE: Duration = Duration::from_secs(10 * 60);
const MAX_RESPONSE_BYTES: usize = 1_048_576;
const MAX_CONFLICT_PATH_OUTPUT_BYTES: usize = 6 * 1_024;
const MAX_DIAGNOSTIC_BYTES: usize = 8 * 1_024;
const PULL_PAGE_SIZE: usize = 50;
const MAX_FEEDBACK_PAGES: usize = 20;
const MAX_FEEDBACK_ITEMS: usize = 100_000;

/// Configuration for one Gitea or Forgejo delivery authority.
#[derive(Clone, Debug)]
pub struct GiteaAuthorityConfig {
    /// Pinned workspace owner for hosted Git subprocesses; local execution inherits the caller.
    pub git_identity: Option<crate::execution::process::HostedProcessIdentity>,
    pub git_program: PathBuf,
    /// Normalized instance origin such as `https://gitea.example.com`.
    pub base_url: String,
    pub api_deadline: Duration,
    pub push_deadline: Duration,
}

impl GiteaAuthorityConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>, git_program: PathBuf) -> Self {
        Self {
            git_identity: None,
            git_program,
            base_url: base_url.into(),
            api_deadline: DEFAULT_API_DEADLINE,
            push_deadline: DEFAULT_PUSH_DEADLINE,
        }
    }
}

/// Delivery authority for a Gitea or Forgejo instance.
#[derive(Clone)]
pub struct GiteaDeliveryAuthority {
    config: GiteaAuthorityConfig,
    client: reqwest::Client,
}

impl std::fmt::Debug for GiteaDeliveryAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GiteaDeliveryAuthority")
            .field("base_url", &self.config.base_url)
            .finish_non_exhaustive()
    }
}

struct RestCall<'a> {
    method: Method,
    path: String,
    credential: GitHubCredential<'a>,
    body: Option<Value>,
}

impl<'a> RestCall<'a> {
    fn get(path: String, credential: GitHubCredential<'a>) -> Self {
        Self {
            method: Method::GET,
            path,
            credential,
            body: None,
        }
    }

    fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    fn body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }
}

struct ReviewQuery<'a> {
    repository: &'a str,
    head_branch: &'a str,
    target_branch: &'a str,
    credential: GitHubCredential<'a>,
}

struct FetchCommit<'a> {
    workspace: &'a Path,
    repository: &'a str,
    revision: &'a str,
    credential: GitHubCredential<'a>,
}

impl GiteaDeliveryAuthority {
    #[must_use]
    pub fn new(config: GiteaAuthorityConfig) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(config.api_deadline.min(Duration::from_secs(30)))
            .user_agent("zeroshot")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { config, client }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}/api/v1{path}", self.config.base_url)
    }

    fn remote_url(&self, repository: &str) -> String {
        format!("{}/{repository}.git", self.config.base_url)
    }

    fn host_scope(&self) -> String {
        self.config.base_url.clone()
    }

    fn workspace_git(&self) -> SystemGit {
        SystemGit::new(self.config.git_program.clone()).with_identity(self.config.git_identity)
    }

    fn local_command(&self, workspace: &Path) -> Command {
        local_git_command_with_identity(
            &self.config.git_program,
            workspace,
            self.config.git_identity,
        )
    }

    fn authenticated_git_command(
        &self,
        workspace: &Path,
        credential: GitHubCredential<'_>,
    ) -> Command {
        let mut command = self.local_command(workspace);
        let authorization = format!("AUTHORIZATION: token {}", credential.expose());
        command
            .env("GH_TOKEN", credential.expose())
            .env("GIT_CONFIG_COUNT", "2")
            .env("GIT_CONFIG_KEY_0", "credential.helper")
            .env("GIT_CONFIG_VALUE_0", "")
            .env(
                "GIT_CONFIG_KEY_1",
                format!("http.{}/.extraheader", self.host_scope()),
            )
            .env("GIT_CONFIG_VALUE_1", authorization);
        command
    }

    async fn rest(&self, call: RestCall<'_>) -> Result<Option<Value>, GitHubAuthorityError> {
        let RestCall {
            method,
            path,
            credential,
            body,
        } = call;
        let request = self
            .client
            .request(method.clone(), self.endpoint(&path))
            .header(reqwest::header::ACCEPT, "application/json")
            .header(
                reqwest::header::AUTHORIZATION,
                format!("token {}", credential.expose()),
            );
        let request = match body {
            Some(body) => request.json(&body),
            None => request,
        };
        let response = tokio::time::timeout(self.config.api_deadline, request.send())
            .await
            .map_err(|_| {
                GitHubAuthorityError::api(
                    None,
                    format!("Gitea {method} {path} exceeded its deadline"),
                )
                .temporary()
            })?
            .map_err(|error| {
                GitHubAuthorityError::api(
                    None,
                    format!(
                        "Gitea {method} {path} transport failure: {}",
                        redact(&error.to_string(), credential)
                    ),
                )
                .temporary()
            })?;
        let status = response.status();
        let bytes = tokio::time::timeout(self.config.api_deadline, response.bytes())
            .await
            .map_err(|_| {
                GitHubAuthorityError::api(
                    Some(status.as_u16()),
                    format!("Gitea {method} {path} response exceeded its deadline"),
                )
                .temporary()
            })?
            .map_err(|error| {
                GitHubAuthorityError::api(
                    Some(status.as_u16()),
                    format!(
                        "Gitea {method} {path} response failure: {}",
                        redact(&error.to_string(), credential)
                    ),
                )
                .temporary()
            })?;
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(GitHubAuthorityError::api(
                Some(status.as_u16()),
                format!("Gitea {method} {path} response exceeded {MAX_RESPONSE_BYTES} bytes"),
            ));
        }
        let text = String::from_utf8_lossy(&bytes);
        if !status.is_success() {
            let diagnostic = format!(
                "Gitea {method} {path} failed: HTTP {status}: {}",
                bounded(&text)
            );
            let error = GitHubAuthorityError::api(Some(status.as_u16()), diagnostic);
            return if status.as_u16() == 429 || status.is_server_error() {
                Err(error.temporary())
            } else {
                Err(error)
            };
        }
        if bytes.is_empty() {
            return Ok(None);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            GitHubAuthorityError::api(
                Some(status.as_u16()),
                format!("Gitea {method} {path} returned malformed JSON"),
            )
        })?;
        Ok(Some(value))
    }

    async fn rest_pages<T: serde::de::DeserializeOwned>(
        &self,
        path: String,
        credential: GitHubCredential<'_>,
    ) -> Result<Vec<T>, GitHubAuthorityError> {
        let mut values = Vec::new();
        for page in 1..=MAX_FEEDBACK_PAGES {
            let value = self
                .rest(RestCall::get(
                    format!("{path}?limit={PULL_PAGE_SIZE}&page={page}"),
                    credential,
                ))
                .await?;
            let mut page_values: Vec<T> = decode(value)?;
            let complete = page_values.len() < PULL_PAGE_SIZE;
            values.append(&mut page_values);
            if values.len() > MAX_FEEDBACK_ITEMS {
                return Err(GitHubAuthorityError::api(
                    None,
                    format!(
                        "Gitea PR feedback exceeded the absolute backstop of {MAX_FEEDBACK_ITEMS} \
                         items"
                    ),
                ));
            }
            if complete {
                return Ok(values);
            }
        }
        Err(GitHubAuthorityError::api(
            None,
            "Gitea PR feedback pagination exceeded its bound".to_owned(),
        )
        .temporary())
    }

    async fn branch_revision(
        &self,
        repository: &str,
        branch: &str,
        credential: GitHubCredential<'_>,
    ) -> Result<Option<String>, GitHubAuthorityError> {
        let result = self
            .rest(RestCall::get(
                format!("/repos/{repository}/branches/{branch}"),
                credential,
            ))
            .await;
        match result {
            Ok(value) => {
                let wire: BranchWire = decode(value)?;
                if !valid_revision(&wire.commit.id) {
                    return Err(GitHubAuthorityError::Rejected);
                }
                Ok(Some(wire.commit.id))
            }
            Err(error) if error.api_status() == Some(404) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn find_review(
        &self,
        query: ReviewQuery<'_>,
    ) -> Result<Option<GitHubReviewReceipt>, GitHubAuthorityError> {
        let ReviewQuery {
            repository,
            head_branch,
            target_branch,
            credential,
        } = query;
        // Gitea's list-pulls endpoint does not honour `head`/`base`; the dedicated
        // by-base-head route returns the run pull request directly, so lookup never depends on a
        // bounded scan of a busy repository's pull list.
        let path = format!(
            "/repos/{repository}/pulls/{}/{}",
            encode_base_segment(target_branch),
            encode_head_path(head_branch),
        );
        match self.rest(RestCall::get(path, credential)).await {
            Ok(value) => {
                let wire: PullWire = decode(value)?;
                let receipt = receipt_from_pull(&wire, repository)?;
                if receipt.head_branch != head_branch || receipt.target_branch != target_branch {
                    return Err(GitHubAuthorityError::identity(
                        "Gitea returned a pull request for a different branch pair",
                    ));
                }
                Ok(Some(receipt))
            }
            Err(error) if error.api_status() == Some(404) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn observe_pull(
        &self,
        repository: &str,
        review_id: &str,
        credential: GitHubCredential<'_>,
    ) -> Result<PullWire, GitHubAuthorityError> {
        let value = self
            .rest(RestCall::get(
                format!("/repos/{repository}/pulls/{review_id}"),
                credential,
            ))
            .await?;
        decode(value)
    }

    async fn checks(
        &self,
        repository: &str,
        revision: &str,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubChecks, GitHubAuthorityError> {
        let value = self
            .rest(RestCall::get(
                format!("/repos/{repository}/commits/{revision}/status"),
                credential,
            ))
            .await?;
        let wire: StatusWire = decode(value)?;
        Ok(classify_checks(&wire))
    }

    async fn confirm_pushed_head(
        &self,
        request: &GitHubPushRequest,
        credential: GitHubCredential<'_>,
    ) -> Result<(), GitHubAuthorityError> {
        match self
            .branch_revision(&request.target.repository, &request.head_branch, credential)
            .await?
        {
            Some(revision) if revision == request.head_revision => Ok(()),
            Some(revision) => Err(GitHubAuthorityError::identity(format!(
                "Gitea branch {} is at {revision}; expected {}",
                request.head_branch, request.head_revision
            ))),
            None => Err(GitHubAuthorityError::api(
                Some(404),
                format!("Gitea branch {} is not visible", request.head_branch),
            )
            .temporary()),
        }
    }

    async fn fetch_commit(&self, request: FetchCommit<'_>) -> Result<(), GitHubAuthorityError> {
        let FetchCommit {
            workspace,
            repository,
            revision,
            credential,
        } = request;
        let mut fetch = self.authenticated_git_command(workspace, credential);
        fetch.args([
            "fetch",
            "--no-tags",
            "--quiet",
            "--no-write-fetch-head",
            &self.remote_url(repository),
            revision,
        ]);
        bounded_status(fetch, self.config.push_deadline).await?;
        let mut verify = self.local_command(workspace);
        verify.args(["cat-file", "-e", &format!("{revision}^{{commit}}")]);
        bounded_status(verify, self.config.api_deadline).await
    }

    async fn configure_identity(&self, workspace: &Path) -> Result<(), GitHubAuthorityError> {
        for (key, value) in [
            ("user.name", "Zeroshot"),
            ("user.email", "delivery@zeroshot.invalid"),
        ] {
            let mut command = self.local_command(workspace);
            command.args(["config", "--local", "--replace-all", key, value]);
            bounded_status(command, self.config.api_deadline).await?;
        }
        Ok(())
    }

    async fn is_ancestor(
        &self,
        workspace: &Path,
        ancestor: &str,
        descendant: &str,
    ) -> Result<bool, GitHubAuthorityError> {
        let output = capture(
            self.local_command(workspace).args([
                "merge-base",
                "--is-ancestor",
                ancestor,
                descendant,
            ]),
            self.config.api_deadline,
        )
        .await?;
        if output.exit_status == Some(1) {
            return Ok(false);
        }
        output.require_success()?;
        Ok(true)
    }

    async fn has_conflicts(&self, workspace: &Path) -> Result<bool, GitHubAuthorityError> {
        let output = bounded_git_output(
            self.local_command(workspace)
                .args(["diff", "--name-only", "--diff-filter=U"]),
            self.config.api_deadline,
        )
        .await?;
        Ok(!output.is_empty())
    }

    async fn require_completed_integration(
        &self,
        workspace: &Path,
        target_revision: &str,
        candidate: &str,
    ) -> Result<(), GitHubAuthorityError> {
        let git = self.workspace_git();
        let (head, dirty) = git.workspace_state(workspace).await.map_err(git_error)?;
        if dirty
            || !self.is_ancestor(workspace, candidate, &head).await?
            || !self.is_ancestor(workspace, target_revision, &head).await?
        {
            return Err(GitHubAuthorityError::repairable(
                "Git merge reported success without a clean, completed integration preserving both \
                 the candidate and captured target ancestry; inspect the preserved workspace",
            ));
        }
        Ok(())
    }

    async fn reconcile_delivery_target_inner(
        &self,
        request: GitHubTargetReconciliation<'_>,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubTargetIntegration, GitHubAuthorityError> {
        let repository = &request.target.repository;
        let target_revision = self
            .branch_revision(repository, &request.target.target_branch, credential)
            .await?
            .ok_or_else(|| {
                GitHubAuthorityError::api(
                    Some(404),
                    format!(
                        "Gitea target branch {} is not visible",
                        request.target.target_branch
                    ),
                )
                .temporary()
            })?;
        self.fetch_commit(FetchCommit {
            workspace: request.workspace,
            repository,
            revision: &target_revision,
            credential,
        })
        .await?;
        let git = self.workspace_git();
        let (head, _) = git
            .workspace_state(request.workspace)
            .await
            .map_err(git_error)?;
        let source_revision = &request.target.base_revision;
        let mut provenance = self.local_command(request.workspace);
        provenance.args(["merge-base", "--is-ancestor", source_revision, &head]);
        bounded_status(provenance, self.config.api_deadline).await?;
        if self
            .is_ancestor(request.workspace, &target_revision, &head)
            .await?
        {
            return Ok(GitHubTargetIntegration {
                target_revision,
                outcome: GitHubReconciliationOutcome::Unchanged,
            });
        }
        let candidate = git
            .prepare_revision(request.workspace, source_revision, request.commit_message)
            .await
            .map_err(git_error)?;
        self.configure_identity(request.workspace).await?;
        let mut command = self.local_command(request.workspace);
        command.args([
            "-c",
            "rerere.enabled=false",
            "merge",
            "--ff",
            "--no-squash",
            "--commit",
            "--no-edit",
            "--no-verify",
            &target_revision,
        ]);
        let output = capture(&mut command, self.config.api_deadline).await?;
        let diagnostic = format!(
            "trusted delivery fetched and integrated captured target revision {target_revision}; \
             inspect and test the resulting workspace, resolving any conflicts, before another \
             delivery\n{output}",
        );
        match output.exit_status {
            Some(0) => {
                self.require_completed_integration(request.workspace, &target_revision, &candidate)
                    .await?;
                Ok(GitHubTargetIntegration {
                    target_revision,
                    outcome: GitHubReconciliationOutcome::NeedsWork(diagnostic),
                })
            }
            Some(1)
                if self
                    .workspace_has_exact_conflict(request.workspace, &target_revision, &candidate)
                    .await? =>
            {
                Ok(GitHubTargetIntegration {
                    target_revision,
                    outcome: GitHubReconciliationOutcome::NeedsWork(diagnostic),
                })
            }
            _ => Err(output.into()),
        }
    }

    async fn workspace_has_exact_conflict(
        &self,
        workspace: &Path,
        target_revision: &str,
        candidate: &str,
    ) -> Result<bool, GitHubAuthorityError> {
        for (reference, expected) in [("HEAD", candidate), ("MERGE_HEAD", target_revision)] {
            let value = bounded_git_output(
                self.local_command(workspace)
                    .args(["rev-parse", "--verify", reference]),
                self.config.api_deadline,
            )
            .await?;
            if value.trim() != expected {
                return Ok(false);
            }
        }
        let output = capture(
            self.local_command(workspace)
                .args(["ls-files", "--unmerged", "-z"]),
            self.config.api_deadline,
        )
        .await?
        .require_success()?;
        Ok(!output.stdout.is_empty() || output.stdout_truncated)
    }

    async fn workspace_head(&self, workspace: &Path) -> Result<String, GitHubAuthorityError> {
        let mut command = self.local_command(workspace);
        command.args(["rev-parse", "HEAD"]);
        bounded_git_output(&mut command, self.config.api_deadline).await
    }

    async fn require_clean_review_head(
        &self,
        workspace: &Path,
        head_revision: &str,
    ) -> Result<(), GitHubAuthorityError> {
        let head = self.workspace_head(workspace).await?;
        let status = capture(
            self.local_command(workspace).args([
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ]),
            self.config.api_deadline,
        )
        .await?
        .require_success()?;
        let clean = status.stdout.is_empty() && !status.stdout_truncated;
        (head.trim() == head_revision && clean)
            .then_some(())
            .ok_or(GitHubAuthorityError::Rejected)
    }

    async fn merge_in_progress(&self, workspace: &Path) -> Result<bool, GitHubAuthorityError> {
        let mut command = self.local_command(workspace);
        command.args(["rev-parse", "-q", "--verify", "MERGE_HEAD"]);
        match capture(&mut command, self.config.api_deadline)
            .await?
            .exit_status
        {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(GitHubAuthorityError::Rejected),
        }
    }

    async fn merge_target(
        &self,
        workspace: &Path,
        target_revision: &str,
    ) -> Result<i32, GitHubAuthorityError> {
        self.configure_identity(workspace).await?;
        let mut command = self.local_command(workspace);
        command.args([
            "-c",
            "rerere.enabled=false",
            "-c",
            "user.name=Zeroshot",
            "-c",
            "user.email=delivery@zeroshot.invalid",
            "merge",
            "--no-commit",
            "--no-ff",
            "--no-edit",
            target_revision,
        ]);
        let output = capture(&mut command, self.config.api_deadline).await?;
        match output.exit_status {
            Some(code @ (0 | 1)) => Ok(code),
            _ => Err(output.into()),
        }
    }

    async fn conflicted_paths(
        &self,
        workspace: &Path,
    ) -> Result<Vec<String>, GitHubAuthorityError> {
        let output = capture(
            self.local_command(workspace)
                .args(["diff", "--name-only", "--diff-filter=U", "-z"]),
            self.config.api_deadline,
        )
        .await?
        .require_success()?;
        if output.stdout_truncated || output.stdout.len() > MAX_CONFLICT_PATH_OUTPUT_BYTES {
            return Err(GitHubAuthorityError::repairable(
                "Git conflict path inspection was truncated after merge materialization",
            ));
        }
        if !output.stdout.is_empty() && !output.stdout.ends_with('\0') {
            return Err(GitHubAuthorityError::repairable(
                "Git conflict path inspection returned malformed output after merge materialization",
            ));
        }
        output
            .stdout
            .split_terminator('\0')
            .map(|path| {
                let valid = !path.is_empty()
                    && !Path::new(path).is_absolute()
                    && Path::new(path)
                        .components()
                        .all(|component| matches!(component, Component::Normal(_)));
                valid.then(|| path.to_owned()).ok_or_else(|| {
                    GitHubAuthorityError::repairable(
                        "Git conflict path inspection returned an unsafe path after merge \
                             materialization",
                    )
                })
            })
            .collect()
    }

    async fn restore_clean_review_head(
        &self,
        workspace: &Path,
        head_revision: &str,
    ) -> Result<(), GitHubAuthorityError> {
        if self.merge_in_progress(workspace).await? {
            let mut command = self.local_command(workspace);
            command.args(["merge", "--abort"]);
            bounded_status(command, self.config.api_deadline).await?;
        }
        self.require_clean_review_head(workspace, head_revision)
            .await
    }

    async fn require_merge_head(
        &self,
        workspace: &Path,
        target_revision: &str,
    ) -> Result<(), GitHubAuthorityError> {
        let mut command = self.local_command(workspace);
        command.args(["rev-parse", "-q", "--verify", "MERGE_HEAD"]);
        bounded_git_output(&mut command, self.config.api_deadline)
            .await
            .ok()
            .filter(|merge_head| merge_head.trim() == target_revision)
            .map(|_| ())
            .ok_or_else(|| {
                GitHubAuthorityError::repairable(
                    "Git merge did not retain the expected target revision in MERGE_HEAD",
                )
            })
    }

    async fn materialize_conflict_inner(
        &self,
        request: &GitHubConflictRequest,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubConflictOutcome, GitHubAuthorityError> {
        let workspace = request.workspace.as_path();
        let review = &request.review;
        self.require_clean_review_head(workspace, &review.head_revision)
            .await?;
        let target_revision = self
            .branch_revision(&review.repository, &review.target_branch, credential)
            .await?
            .ok_or_else(|| {
                GitHubAuthorityError::api(
                    Some(404),
                    format!(
                        "Gitea target branch {} is not visible",
                        review.target_branch
                    ),
                )
                .temporary()
            })?;
        self.fetch_commit(FetchCommit {
            workspace,
            repository: &review.repository,
            revision: &target_revision,
            credential,
        })
        .await?;
        let merge_status = self.merge_target(workspace, &target_revision).await?;
        let conflicted_paths = self.conflicted_paths(workspace).await?;
        if merge_status == 0 && conflicted_paths.is_empty() {
            self.restore_clean_review_head(workspace, &review.head_revision)
                .await?;
            return Ok(GitHubConflictOutcome::ObservationChanged);
        }
        if merge_status != 1 || conflicted_paths.is_empty() {
            return Err(GitHubAuthorityError::repairable(
                "Git merge did not leave the exact expected conflict state",
            ));
        }
        let head = self.workspace_head(workspace).await?;
        if head.trim() != review.head_revision {
            return Err(GitHubAuthorityError::repairable(
                "Git merge changed the reviewed HEAD while materializing a conflict",
            ));
        }
        self.require_merge_head(workspace, &target_revision).await?;
        Ok(GitHubConflictOutcome::Materialized(
            GitHubConflictMaterialization {
                target_revision,
                conflicted_paths,
            },
        ))
    }

    async fn reconcile_delivery_head_inner(
        &self,
        request: GitHubHeadReconciliation<'_>,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubReconciliationOutcome, GitHubAuthorityError> {
        require_identity(request.published, request.observed)?;
        self.fetch_commit(FetchCommit {
            workspace: request.workspace,
            repository: &request.published.repository,
            revision: &request.observed.head_revision,
            credential,
        })
        .await?;
        if !request.adopting_existing
            && !self
                .is_ancestor(
                    request.workspace,
                    &request.published.head_revision,
                    &request.observed.head_revision,
                )
                .await?
        {
            return Ok(GitHubReconciliationOutcome::Refused(format!(
                "remote history was rewritten: published head {}; observed head {}; local work was \
                 preserved",
                request.published.head_revision, request.observed.head_revision,
            )));
        }
        self.reconcile_workspace(request).await
    }

    async fn reconcile_workspace(
        &self,
        request: GitHubHeadReconciliation<'_>,
    ) -> Result<GitHubReconciliationOutcome, GitHubAuthorityError> {
        let git = self.workspace_git();
        let (head, dirty) = git
            .workspace_state(request.workspace)
            .await
            .map_err(git_error)?;
        if self
            .is_ancestor(request.workspace, &request.observed.head_revision, &head)
            .await?
        {
            return Ok(GitHubReconciliationOutcome::Unchanged);
        }
        if request.adopting_existing {
            if !self
                .is_ancestor(request.workspace, &head, &request.observed.head_revision)
                .await?
            {
                return Ok(GitHubReconciliationOutcome::Refused(format!(
                    "existing remote head {} does not descend from retained local head {head}; local \
                     work was preserved",
                    request.observed.head_revision,
                )));
            }
            let mut anchor = request.observed.clone();
            anchor.head_revision = head;
            return Box::pin(self.reconcile_workspace(GitHubHeadReconciliation {
                published: &anchor,
                adopting_existing: false,
                ..request
            }))
            .await;
        }
        if !self
            .is_ancestor(request.workspace, &request.published.head_revision, &head)
            .await?
        {
            return Ok(GitHubReconciliationOutcome::Refused(format!(
                "local history no longer contains published head {}; local head {head}; observed \
                 head {}; work was preserved",
                request.published.head_revision, request.observed.head_revision,
            )));
        }
        if dirty {
            git.prepare_revision(
                request.workspace,
                &request.published.head_revision,
                request.commit_message,
            )
            .await
            .map_err(git_error)?;
        }
        let authorized =
            request.authorized_update && !dirty && head == request.published.head_revision;
        self.integrate_head(request, authorized).await
    }

    async fn integrate_head(
        &self,
        request: GitHubHeadReconciliation<'_>,
        authorized: bool,
    ) -> Result<GitHubReconciliationOutcome, GitHubAuthorityError> {
        let mut command = self.local_command(request.workspace);
        command.args([
            "-c",
            "user.name=Zeroshot",
            "-c",
            "user.email=delivery@zeroshot.invalid",
            "merge",
            "--no-edit",
        ]);
        if authorized {
            command.arg("--ff-only");
        }
        command.arg(&request.observed.head_revision);
        let output = capture(&mut command, self.config.api_deadline).await?;
        let diagnostic = format!(
            "trusted delivery fetched remote head {} and reconciled published head {} with local \
             work; inspect the resulting workspace before another delivery\n{output}",
            request.observed.head_revision, request.published.head_revision,
        );
        if output.exit_status == Some(0) {
            return Ok(if authorized {
                GitHubReconciliationOutcome::Adopted
            } else {
                GitHubReconciliationOutcome::NeedsWork(diagnostic)
            });
        }
        if self.has_conflicts(request.workspace).await? {
            return Ok(GitHubReconciliationOutcome::NeedsWork(diagnostic));
        }
        Err(output.into())
    }
}

#[async_trait]
impl GitHubDeliveryAuthority for GiteaDeliveryAuthority {
    fn credential_environment(&self) -> &'static str {
        super::GITEA_TOKEN_ENV
    }

    async fn observe_delivery(
        &self,
        request: GitHubDeliveryRead<'_>,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubDeliverySnapshot, GitHubAuthorityError> {
        let repository = &request.target.repository;
        let identity = match (
            request.include_review,
            request
                .known_review
                .filter(|review| !review.review_id.is_empty()),
        ) {
            (false, _) => None,
            (true, Some(review)) => Some(review.clone()),
            (true, None) => {
                self.find_review(ReviewQuery {
                    repository,
                    head_branch: request.head_branch,
                    target_branch: &request.target.target_branch,
                    credential,
                })
                .await?
            }
        };
        let review = match identity {
            Some(identity) => {
                let wire = self
                    .observe_pull(repository, &identity.review_id, credential)
                    .await?;
                let receipt = receipt_from_pull(&wire, repository)?;
                if receipt.review_id != identity.review_id {
                    return Err(GitHubAuthorityError::identity(format!(
                        "expected PR {}; observed PR {}",
                        identity.review_id, receipt.review_id
                    )));
                }
                let state = classify_pull_state(&wire)?;
                Some(receipt.observation(state))
            }
            None => None,
        };
        let head_revision = self
            .branch_revision(repository, request.head_branch, credential)
            .await?;
        require_consistent_head(review.as_ref(), head_revision.as_deref())?;
        Ok(GitHubDeliverySnapshot {
            review,
            head_revision,
        })
    }

    async fn reconcile_delivery_target(
        &self,
        request: GitHubTargetReconciliation<'_>,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubTargetIntegration, GitHubAuthorityError> {
        self.reconcile_delivery_target_inner(request, credential)
            .await
    }

    async fn reconcile_delivery_head(
        &self,
        request: GitHubHeadReconciliation<'_>,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubReconciliationOutcome, GitHubAuthorityError> {
        self.reconcile_delivery_head_inner(request, credential)
            .await
    }

    async fn push_branch(
        &self,
        request: &GitHubPushRequest,
        credential: GitHubCredential<'_>,
    ) -> Result<(), GitHubAuthorityError> {
        let mut command = self.authenticated_git_command(&request.workspace, credential);
        command
            .arg("push")
            .arg("--porcelain")
            .arg("--no-verify")
            .arg(self.remote_url(&request.target.repository))
            .arg(format!(
                "{}:refs/heads/{}",
                request.head_revision, request.head_branch
            ));
        match capture(&mut command, self.config.push_deadline)
            .await
            .and_then(GitCommandFailure::require_success)
        {
            Ok(_) => self.confirm_pushed_head(request, credential).await,
            Err(failure) => match self.confirm_pushed_head(request, credential).await {
                Ok(()) => Ok(()),
                Err(error) if error.authentication_failed() || error.retryable_operation() => {
                    Err(error.with_context(failure))
                }
                Err(error) => Err(failure
                    .with_context(format!("push confirmation failed: {error}"))
                    .into()),
            },
        }
    }

    async fn open_or_update_review(
        &self,
        request: &GitHubReviewRequest,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubReviewReceipt, GitHubAuthorityError> {
        let repository = &request.target.repository;
        if let Some(existing) = self
            .find_review(ReviewQuery {
                repository,
                head_branch: &request.head_branch,
                target_branch: &request.target.target_branch,
                credential,
            })
            .await?
        {
            let value = self
                .rest(
                    RestCall::get(
                        format!("/repos/{repository}/pulls/{}", existing.review_id),
                        credential,
                    )
                    .method(Method::PATCH)
                    .body(json!({
                        "title": request.title,
                        "body": request.description,
                    })),
                )
                .await?;
            let wire: PullWire = decode(value)?;
            let receipt = receipt_from_pull(&wire, repository)?;
            if receipt.review_id != existing.review_id
                || receipt.head_branch != existing.head_branch
                || receipt.target_branch != existing.target_branch
            {
                return Err(GitHubAuthorityError::identity(
                    "Gitea updated a different pull request identity",
                ));
            }
            return Ok(receipt);
        }
        match self
            .branch_revision(repository, &request.head_branch, credential)
            .await?
        {
            Some(revision) if revision == request.head_revision => {}
            Some(revision) => {
                return Err(GitHubAuthorityError::identity(format!(
                    "Gitea run branch {} is at {revision}; expected {}",
                    request.head_branch, request.head_revision
                )));
            }
            None => {
                return Err(GitHubAuthorityError::api(
                    Some(404),
                    format!(
                        "Gitea run branch {} is not visible before review creation",
                        request.head_branch
                    ),
                )
                .temporary());
            }
        }
        let value = self
            .rest(
                RestCall::get(format!("/repos/{repository}/pulls"), credential)
                    .method(Method::POST)
                    .body(json!({
                        "title": request.title,
                        "body": request.description,
                        "head": request.head_branch,
                        "base": request.target.target_branch,
                    })),
            )
            .await?;
        let wire: PullWire = decode(value)?;
        receipt_from_pull(&wire, repository)
    }

    async fn inspect_review(
        &self,
        review: &GitHubReviewReceipt,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubReviewObservation, GitHubAuthorityError> {
        let wire = self
            .observe_pull(&review.repository, &review.review_id, credential)
            .await?;
        let receipt = receipt_from_pull(&wire, &review.repository)?;
        if receipt.review_id != review.review_id
            || receipt.head_branch != review.head_branch
            || receipt.target_branch != review.target_branch
        {
            return Err(GitHubAuthorityError::identity(
                "Gitea review identity changed during inspection",
            ));
        }
        match wire.state.as_str() {
            "closed" if wire.merged => {
                let revision = wire
                    .merge_commit_sha
                    .filter(|revision| valid_revision(revision))
                    .ok_or(GitHubAuthorityError::Rejected)?;
                Ok(receipt.observation(GitHubReviewState::Merged {
                    merge_revision: revision,
                }))
            }
            "closed" => Ok(receipt.observation(GitHubReviewState::Closed)),
            "open" => {
                if wire.mergeable == Some(false) {
                    return Ok(receipt.observation(GitHubReviewState::Conflict));
                }
                let checks = self
                    .checks(&review.repository, &review.head_revision, credential)
                    .await?;
                // Gitea has no separate merge-policy/approval gate to report, so passing checks
                // are immediately mergeable. Reporting `pull_request_ready = false` keeps merge
                // modes on the `Mergeable` path instead of waiting forever for a policy step that
                // never arrives.
                Ok(receipt.observation_with_readiness(
                    GitHubReviewState::Open { checks },
                    false,
                    false,
                ))
            }
            _ => Err(GitHubAuthorityError::Rejected),
        }
    }

    async fn inspect_review_feedback(
        &self,
        review: &GitHubReviewReceipt,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubReviewFeedback, GitHubAuthorityError> {
        let before = self
            .observe_pull(&review.repository, &review.review_id, credential)
            .await?;
        require_review_fence(&before, review, "before feedback read")?;

        let repository = &review.repository;
        let issue_comments: Vec<IssueCommentWire> = self
            .rest_pages(
                format!("/repos/{repository}/issues/{}/comments", review.review_id),
                credential,
            )
            .await?;
        let reviews: Vec<ReviewWire> = self
            .rest_pages(
                format!("/repos/{repository}/pulls/{}/reviews", review.review_id),
                credential,
            )
            .await?;
        let mut review_comments = Vec::new();
        for summary in &reviews {
            let mut comments: Vec<ReviewCommentWire> = self
                .rest_pages(
                    format!(
                        "/repos/{repository}/pulls/{}/reviews/{}/comments",
                        review.review_id, summary.id
                    ),
                    credential,
                )
                .await?;
            review_comments.append(&mut comments);
        }

        let after = self
            .observe_pull(&review.repository, &review.review_id, credential)
            .await?;
        require_review_fence(&after, review, "after feedback read").map_err(|_| {
            GitHubAuthorityError::Unavailable
                .with_context("Gitea PR identity changed while feedback was paginated")
        })?;
        collect_feedback_items(issue_comments, reviews, review_comments)
    }

    async fn request_merge(
        &self,
        review: &GitHubReviewReceipt,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubMergeRequestOutcome, GitHubAuthorityError> {
        let wire = self
            .observe_pull(&review.repository, &review.review_id, credential)
            .await?;
        match wire.state.as_str() {
            "closed" if wire.merged => return Ok(GitHubMergeRequestOutcome::Accepted),
            "closed" => return Err(GitHubAuthorityError::Rejected),
            _ => {}
        }
        if wire.mergeable == Some(false) {
            return Ok(GitHubMergeRequestOutcome::Conflict);
        }
        match self
            .rest(
                RestCall::get(
                    format!(
                        "/repos/{}/pulls/{}/merge",
                        review.repository, review.review_id
                    ),
                    credential,
                )
                .method(Method::POST)
                .body(json!({
                    "Do": "merge",
                    "MergeTitleField": review.head_revision,
                    "MergeMessageField": review.head_revision,
                })),
            )
            .await
        {
            Ok(_) => Ok(GitHubMergeRequestOutcome::Accepted),
            Err(error) if error.api_status() == Some(409) => {
                Ok(GitHubMergeRequestOutcome::Conflict)
            }
            Err(error) => match self
                .observe_pull(&review.repository, &review.review_id, credential)
                .await
            {
                Ok(observed) if observed.state == "closed" && observed.merged => {
                    Ok(GitHubMergeRequestOutcome::Accepted)
                }
                Ok(observed) if observed.mergeable == Some(false) => {
                    Ok(GitHubMergeRequestOutcome::Conflict)
                }
                _ => Err(error),
            },
        }
    }

    async fn materialize_merge_conflict(
        &self,
        request: &GitHubConflictRequest,
        credential: GitHubCredential<'_>,
    ) -> Result<GitHubConflictOutcome, GitHubAuthorityError> {
        self.materialize_conflict_inner(request, credential).await
    }
}

#[derive(Deserialize)]
struct BranchWire {
    commit: CommitWire,
}

#[derive(Deserialize)]
struct CommitWire {
    id: String,
}

#[derive(Deserialize)]
struct RefWire {
    #[serde(rename = "ref")]
    reference: String,
    sha: String,
}

#[derive(Deserialize)]
struct PullWire {
    number: u64,
    state: String,
    #[serde(default)]
    merged: bool,
    #[serde(default)]
    merge_commit_sha: Option<String>,
    head: RefWire,
    base: RefWire,
    #[serde(default)]
    mergeable: Option<bool>,
}

#[derive(Deserialize)]
struct StatusWire {
    state: String,
    #[serde(default)]
    statuses: Vec<Value>,
}

#[derive(Deserialize)]
struct UserWire {
    login: String,
}

#[derive(Deserialize)]
struct IssueCommentWire {
    id: u64,
    #[serde(default)]
    updated_at: String,
    user: UserWire,
    #[serde(default)]
    body: Option<String>,
}

#[derive(Deserialize)]
struct ReviewWire {
    id: u64,
    #[serde(default)]
    state: String,
    #[serde(default)]
    submitted_at: Option<String>,
    #[serde(default)]
    commit_id: Option<String>,
    user: UserWire,
    #[serde(default)]
    body: Option<String>,
}

#[derive(Deserialize)]
struct ReviewCommentWire {
    id: u64,
    #[serde(default)]
    updated_at: String,
    user: UserWire,
    #[serde(default)]
    body: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    position: Option<u64>,
    #[serde(default)]
    original_position: Option<u64>,
    #[serde(default)]
    commit_id: Option<String>,
}

struct FeedbackItemDraft<'a> {
    key: String,
    provider_version: &'a str,
    author: String,
    location: Option<String>,
    body: String,
}

fn require_review_fence(
    wire: &PullWire,
    review: &GitHubReviewReceipt,
    phase: &str,
) -> Result<(), GitHubAuthorityError> {
    let receipt = receipt_from_pull(wire, &review.repository)?;
    if receipt.review_id != review.review_id
        || receipt.head_branch != review.head_branch
        || receipt.target_branch != review.target_branch
    {
        return Err(GitHubAuthorityError::identity(format!(
            "Gitea review identity changed {phase}"
        )));
    }
    Ok(())
}

fn collect_feedback_items(
    issue_comments: Vec<IssueCommentWire>,
    reviews: Vec<ReviewWire>,
    review_comments: Vec<ReviewCommentWire>,
) -> Result<GitHubReviewFeedback, GitHubAuthorityError> {
    let count = issue_comments
        .len()
        .saturating_add(reviews.len())
        .saturating_add(review_comments.len());
    if count > MAX_FEEDBACK_ITEMS {
        return Err(GitHubAuthorityError::api(
            None,
            format!(
                "Gitea PR feedback exceeded the absolute backstop of {MAX_FEEDBACK_ITEMS} items"
            ),
        ));
    }
    let mut items = Vec::with_capacity(count);
    items.extend(issue_comments.into_iter().filter_map(issue_comment_item));
    items.extend(reviews.into_iter().filter_map(review_item));
    items.extend(review_comments.into_iter().filter_map(review_comment_item));
    items.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(GitHubReviewFeedback { items })
}

fn issue_comment_item(comment: IssueCommentWire) -> Option<GitHubReviewFeedbackItem> {
    Some(feedback_item(FeedbackItemDraft {
        key: format!("issue_comment:{}", comment.id),
        provider_version: &comment.updated_at,
        author: comment.user.login,
        location: None,
        body: nonempty_feedback(comment.body.as_deref())?,
    }))
}

fn review_item(summary: ReviewWire) -> Option<GitHubReviewFeedbackItem> {
    let body = nonempty_feedback(summary.body.as_deref()).or_else(|| {
        (summary.state == "REQUEST_CHANGES" || summary.state == "CHANGES_REQUESTED")
            .then(|| "Review requested changes without a written summary.".to_owned())
    })?;
    let location = Some(format!(
        "review state={} commit={}",
        clean_feedback(&summary.state),
        summary
            .commit_id
            .as_deref()
            .map(clean_feedback)
            .unwrap_or_default()
    ));
    Some(feedback_item(FeedbackItemDraft {
        key: format!("review:{}", summary.id),
        provider_version: summary.submitted_at.as_deref().unwrap_or_default(),
        author: summary.user.login,
        location,
        body,
    }))
}

fn review_comment_item(comment: ReviewCommentWire) -> Option<GitHubReviewFeedbackItem> {
    let line = comment.position.or(comment.original_position);
    let location = Some(format!(
        "path={} line={} commit={}",
        comment
            .path
            .as_deref()
            .map(clean_feedback)
            .unwrap_or_default(),
        line.map(|value| value.to_string()).unwrap_or_default(),
        comment
            .commit_id
            .as_deref()
            .map(clean_feedback)
            .unwrap_or_default(),
    ));
    Some(feedback_item(FeedbackItemDraft {
        key: format!("review_comment:{}", comment.id),
        provider_version: &comment.updated_at,
        author: comment.user.login,
        location,
        body: nonempty_feedback(Some(&comment.body))?,
    }))
}

fn feedback_item(draft: FeedbackItemDraft<'_>) -> GitHubReviewFeedbackItem {
    let FeedbackItemDraft {
        key,
        provider_version,
        author,
        location,
        body,
    } = draft;
    let author = clean_feedback(&author);
    let location = location.map(|value| clean_feedback(&value));
    let mut digest = Sha256::new();
    for value in [
        provider_version,
        author.as_str(),
        location.as_deref().unwrap_or_default(),
        body.as_str(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    GitHubReviewFeedbackItem {
        key,
        version: format!("{:x}", digest.finalize()),
        author,
        location,
        body,
    }
}

fn nonempty_feedback(value: Option<&str>) -> Option<String> {
    let value = clean_feedback(value?).trim().to_owned();
    (!value.is_empty()).then_some(value)
}

fn clean_feedback(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
        .collect()
}

fn encode_base_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn encode_head_path(value: &str) -> String {
    value
        .split('/')
        .map(encode_base_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn decode<T: for<'de> Deserialize<'de>>(value: Option<Value>) -> Result<T, GitHubAuthorityError> {
    let value = value.ok_or(GitHubAuthorityError::Rejected)?;
    serde_json::from_value(value).map_err(|_| GitHubAuthorityError::Rejected)
}

fn receipt_from_pull(
    pull: &PullWire,
    repository: &str,
) -> Result<GitHubReviewReceipt, GitHubAuthorityError> {
    if !valid_revision(&pull.head.sha)
        || !valid_revision(&pull.base.sha)
        || pull.head.reference.is_empty()
        || pull.base.reference.is_empty()
    {
        return Err(GitHubAuthorityError::Rejected);
    }
    Ok(GitHubReviewReceipt {
        review_id: pull.number.to_string(),
        repository: repository.to_owned(),
        target_branch: pull.base.reference.clone(),
        head_branch: pull.head.reference.clone(),
        head_revision: pull.head.sha.clone(),
    })
}

fn classify_pull_state(wire: &PullWire) -> Result<GitHubReviewState, GitHubAuthorityError> {
    match wire.state.as_str() {
        "closed" if wire.merged => wire
            .merge_commit_sha
            .clone()
            .filter(|revision| valid_revision(revision))
            .map(|merge_revision| GitHubReviewState::Merged { merge_revision })
            .ok_or(GitHubAuthorityError::Rejected),
        "closed" => Ok(GitHubReviewState::Closed),
        "open" if wire.mergeable == Some(false) => Ok(GitHubReviewState::Conflict),
        "open" => Ok(GitHubReviewState::Open {
            checks: GitHubChecks::Pending,
        }),
        _ => Err(GitHubAuthorityError::Rejected),
    }
}

fn classify_checks(wire: &StatusWire) -> GitHubChecks {
    if wire.statuses.is_empty() {
        return GitHubChecks::NotRequired;
    }
    match wire.state.to_ascii_lowercase().as_str() {
        "success" => GitHubChecks::Passed,
        "pending" => GitHubChecks::Pending,
        "failure" | "error" => GitHubChecks::Failed {
            diagnostic: format!("Gitea reports combined commit status {}", wire.state),
        },
        _ => GitHubChecks::Pending,
    }
}

fn require_consistent_head(
    review: Option<&GitHubReviewObservation>,
    head: Option<&str>,
) -> Result<(), GitHubAuthorityError> {
    if let (Some(review), Some(head)) = (review, head) {
        if matches!(review.state, GitHubReviewState::Open { .. }) && review.head_revision != head {
            return Err(GitHubAuthorityError::Unavailable.with_context(format!(
                "Gitea PR/ref observations changed: PR head {}, ref head {head}",
                review.head_revision,
            )));
        }
    }
    Ok(())
}

fn require_identity(
    published: &GitHubReviewReceipt,
    observed: &GitHubReviewReceipt,
) -> Result<(), GitHubAuthorityError> {
    let bound_review_changed =
        !published.review_id.is_empty() && published.review_id != observed.review_id;
    if bound_review_changed
        || published.repository != observed.repository
        || published.target_branch != observed.target_branch
        || published.head_branch != observed.head_branch
    {
        return Err(GitHubAuthorityError::identity(format!(
            "published identity {published:?}; observed identity {observed:?}",
        )));
    }
    Ok(())
}

fn git_error(error: GitError) -> GitHubAuthorityError {
    match error {
        GitError::Command(failure) => GitHubAuthorityError::Command(failure),
        error => GitHubAuthorityError::repairable(error.to_string()),
    }
}

async fn bounded_status(
    mut command: Command,
    deadline: Duration,
) -> Result<(), GitHubAuthorityError> {
    capture(&mut command, deadline).await?.require_success()?;
    Ok(())
}

async fn bounded_git_output(
    command: &mut Command,
    deadline: Duration,
) -> Result<String, GitHubAuthorityError> {
    let output = capture(command, deadline).await?.require_success()?;
    if output.stdout_truncated || output.stdout.len() > 4 * 1_024 {
        return Err(GitHubAuthorityError::repairable(
            "Git output was truncated during delivery reconciliation",
        ));
    }
    Ok(output.stdout)
}

fn redact(value: &str, credential: GitHubCredential<'_>) -> String {
    value.replace(credential.expose(), "[REDACTED]")
}

fn bounded(value: &str) -> String {
    if value.len() <= MAX_DIAGNOSTIC_BYTES {
        return value.to_owned();
    }
    let mut boundary = MAX_DIAGNOSTIC_BYTES;
    while !value.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    format!("{}[truncated]", &value[..boundary])
}

#[cfg(test)]
#[path = "gitea/tests.rs"]
mod tests;
