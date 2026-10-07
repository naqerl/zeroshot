use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{json, Value};

use super::*;
use crate::native_v2_candidate::test_support::{TestGitRepository, commit_all, git, git_output};
use crate::native_v2_delivery::{DeliveryTarget, delivery_branch};
use openengine_cluster_testkit::assertions::AssertValue;

const HEAD: &str = "0123456789abcdef0123456789abcdef01234567";
const OTHER: &str = "1111111111111111111111111111111111111111";

fn credential() -> ForgeCredential<'static> {
    ForgeCredential("test-token")
}

fn authority(base_url: &str) -> GiteaDeliveryAuthority {
    let mut config = GiteaAuthorityConfig::new(base_url.to_owned(), PathBuf::from("git"));
    config.api_deadline = Duration::from_secs(5);
    GiteaDeliveryAuthority::new(config)
}

fn target() -> crate::native_v2_delivery::DeliveryTarget {
    crate::native_v2_delivery::DeliveryTarget::new("acme/project", "main", OTHER).expect("target")
}

fn reference(reference: &str, sha: &str) -> RefWire {
    RefWire {
        reference: reference.to_owned(),
        sha: sha.to_owned(),
    }
}

fn pull(
    state: &str,
    merged: bool,
    merge_commit_sha: Option<&str>,
    mergeable: Option<bool>,
) -> PullWire {
    PullWire {
        number: 7,
        state: state.to_owned(),
        merged,
        merge_commit_sha: merge_commit_sha.map(str::to_owned),
        head: reference("zeroshot/run/42", HEAD),
        base: reference("main", OTHER),
        mergeable,
    }
}

#[test]
fn combined_commit_status_maps_to_checks() {
    let empty = StatusWire {
        state: "success".to_owned(),
        statuses: Vec::new(),
    };
    assert_eq!(classify_checks(&empty), ForgeChecks::NotRequired);

    let passed = StatusWire {
        state: "success".to_owned(),
        statuses: vec![json!({"status": "success"})],
    };
    assert_eq!(classify_checks(&passed), ForgeChecks::Passed);

    let pending = StatusWire {
        state: "pending".to_owned(),
        statuses: vec![json!({"status": "pending"})],
    };
    assert_eq!(classify_checks(&pending), ForgeChecks::Pending);

    let failed = StatusWire {
        state: "failure".to_owned(),
        statuses: vec![json!({"status": "failure"})],
    };
    assert!(matches!(
        classify_checks(&failed),
        ForgeChecks::Failed { .. }
    ));
}

#[test]
fn pull_state_classification_is_terminal_aware() {
    assert_eq!(
        classify_pull_state(&pull("open", false, None, Some(true))).expect("open"),
        ForgeReviewState::Open {
            checks: ForgeChecks::Pending
        }
    );
    assert_eq!(
        classify_pull_state(&pull("open", false, None, Some(false))).expect("conflict"),
        ForgeReviewState::Conflict
    );
    assert_eq!(
        classify_pull_state(&pull("closed", false, None, Some(true))).expect("closed"),
        ForgeReviewState::Closed
    );
    assert_eq!(
        classify_pull_state(&pull("closed", true, Some(HEAD), None)).expect("merged"),
        ForgeReviewState::Merged {
            merge_revision: HEAD.to_owned()
        }
    );
    assert!(classify_pull_state(&pull("closed", true, None, None)).is_err());
    assert!(classify_pull_state(&pull("weird", false, None, None)).is_err());
}

#[test]
fn receipt_rejects_malformed_pull_identity() {
    let mut malformed = pull("open", false, None, Some(true));
    malformed.head.sha = "not-a-revision".to_owned();
    assert!(receipt_from_pull(&malformed, "acme/project").is_err());

    let receipt =
        receipt_from_pull(&pull("open", false, None, Some(true)), "acme/project").expect("receipt");
    assert_eq!(receipt.review_id, "7");
    assert_eq!(receipt.repository, "acme/project");
    assert_eq!(receipt.head_branch, "zeroshot/run/42");
    assert_eq!(receipt.target_branch, "main");
    assert_eq!(receipt.head_revision, HEAD);
}

#[test]
fn observations_must_agree_on_head_identity() {
    let receipt =
        receipt_from_pull(&pull("open", false, None, Some(true)), "acme/project").expect("receipt");
    let observation = receipt.observation(ForgeReviewState::Open {
        checks: ForgeChecks::Passed,
    });
    assert!(require_consistent_head(Some(&observation), Some(HEAD)).is_ok());
    assert!(require_consistent_head(Some(&observation), Some(OTHER)).is_err());
    assert!(require_consistent_head(Some(&observation), None).is_ok());
}

#[test]
fn published_and_observed_identities_must_match() {
    let published =
        receipt_from_pull(&pull("open", false, None, Some(true)), "acme/project").expect("receipt");
    let mut observed = published.clone();
    observed.head_revision = OTHER.to_owned();
    assert!(require_identity(&published, &observed).is_ok());

    observed.target_branch = "release".to_owned();
    assert!(require_identity(&published, &observed).is_err());
}

#[test]
fn authority_selects_the_gitea_credential_environment() {
    let authority = authority("https://gitea.example.com");
    assert_eq!(
        DeliveryForgeAuthority::credential_environment(&authority),
        "GITEA_TOKEN"
    );
    assert_eq!(
        authority.remote_url("acme/project"),
        "https://gitea.example.com/acme/project.git"
    );
    assert_eq!(authority.host_scope(), "https://gitea.example.com");
}

#[test]
fn authenticated_git_command_scopes_the_credential() {
    let authority = authority("https://gitea.example.com");
    let command = authority.authenticated_git_command(Path::new("/tmp"), credential());
    let environment: std::collections::HashMap<_, _> = command
        .as_std()
        .get_envs()
        .map(|(key, value)| (key.to_owned(), value.map(|value| value.to_owned())))
        .collect();
    let value = |name: &str| {
        environment
            .get(std::ffi::OsStr::new(name))
            .and_then(|value| value.as_deref())
            .map(std::ffi::OsStr::to_string_lossy)
            .map(std::borrow::Cow::into_owned)
    };
    assert_eq!(value("GH_TOKEN").as_deref(), Some("test-token"));
    assert_eq!(
        value("GIT_CONFIG_KEY_1").as_deref(),
        Some("http.https://gitea.example.com/.extraheader")
    );
    assert_eq!(
        value("GIT_CONFIG_VALUE_1").as_deref(),
        Some("AUTHORIZATION: token test-token")
    );
    assert_eq!(value("GIT_CONFIG_COUNT").as_deref(), Some("2"));
}

#[tokio::test]
async fn observe_delivery_reports_the_bound_pull_and_head() {
    let server = TestGitea::start(vec![
        (
            200,
            json!({
                "number": 7,
                "state": "open",
                "merged": false,
                "head": {"ref": "zeroshot/run/42", "sha": HEAD},
                "base": {"ref": "main", "sha": OTHER},
                "mergeable": true,
            })
            .to_string(),
        ),
        (
            200,
            json!({
                "number": 7,
                "state": "open",
                "merged": false,
                "head": {"ref": "zeroshot/run/42", "sha": HEAD},
                "base": {"ref": "main", "sha": OTHER},
                "mergeable": true,
            })
            .to_string(),
        ),
        (
            200,
            json!({"name": "zeroshot/run/42", "commit": {"id": HEAD}}).to_string(),
        ),
    ]);
    let authority = authority(&server.base_url);
    let target = target();
    let snapshot = authority
        .observe_delivery(
            ForgeDeliveryRead {
                target: &target,
                head_branch: "zeroshot/run/42",
                known_review: None,
                include_review: true,
            },
            credential(),
        )
        .await
        .expect("snapshot");
    assert_eq!(snapshot.head_revision.as_deref(), Some(HEAD));
    let review = snapshot.review.expect("review");
    assert_eq!(review.review_id, "7");
    assert_eq!(review.head_branch, "zeroshot/run/42");
    assert!(matches!(review.state, ForgeReviewState::Open { .. }));

    let requests = server.requests();
    assert_eq!(requests.len(), 3, "{requests:?}");
    assert_eq!(
        requests[0].1,
        "/api/v1/repos/acme/project/pulls/main/zeroshot/run/42"
    );
    assert_eq!(requests[1].1, "/api/v1/repos/acme/project/pulls/7");
    assert_eq!(
        requests[2].1,
        "/api/v1/repos/acme/project/branches/zeroshot/run/42"
    );
}

#[tokio::test]
async fn open_review_creates_the_run_pull_request() {
    let server = TestGitea::start(vec![
        (404, json!({"message": "not found"}).to_string()),
        (
            200,
            json!({"name": "zeroshot/run/42", "commit": {"id": HEAD}}).to_string(),
        ),
        (
            201,
            json!({
                "number": 7,
                "state": "open",
                "merged": false,
                "head": {"ref": "zeroshot/run/42", "sha": HEAD},
                "base": {"ref": "main", "sha": OTHER},
                "mergeable": true,
            })
            .to_string(),
        ),
    ]);
    let authority = authority(&server.base_url);
    let receipt = authority
        .open_or_update_review(
            &ForgeReviewRequest {
                target: target(),
                head_branch: "zeroshot/run/42".to_owned(),
                head_revision: HEAD.to_owned(),
                title: "Run 42".to_owned(),
                description: "Body".to_owned(),
                source_issue: None,
            },
            credential(),
        )
        .await
        .expect("receipt");
    assert_eq!(receipt.review_id, "7");
    assert_eq!(receipt.head_revision, HEAD);
    let requests = server.requests();
    assert_eq!(requests.len(), 3, "{requests:?}");
    assert_eq!(requests[2].0, "POST");
    assert_eq!(requests[2].1, "/api/v1/repos/acme/project/pulls");
}

#[tokio::test]
async fn open_review_updates_an_existing_pull_request() {
    let server = TestGitea::start(vec![
        (
            200,
            json!({
                "number": 7,
                "state": "open",
                "merged": false,
                "head": {"ref": "zeroshot/run/42", "sha": HEAD},
                "base": {"ref": "main", "sha": OTHER},
                "mergeable": true,
            })
            .to_string(),
        ),
        (
            200,
            json!({
                "number": 7,
                "state": "open",
                "merged": false,
                "head": {"ref": "zeroshot/run/42", "sha": HEAD},
                "base": {"ref": "main", "sha": OTHER},
                "mergeable": true,
            })
            .to_string(),
        ),
    ]);
    let authority = authority(&server.base_url);
    let receipt = authority
        .open_or_update_review(
            &ForgeReviewRequest {
                target: target(),
                head_branch: "zeroshot/run/42".to_owned(),
                head_revision: HEAD.to_owned(),
                title: "Updated".to_owned(),
                description: "Body".to_owned(),
                source_issue: None,
            },
            credential(),
        )
        .await
        .expect("receipt");
    assert_eq!(receipt.review_id, "7");
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[1].0, "PATCH");
    assert_eq!(requests[1].1, "/api/v1/repos/acme/project/pulls/7");
}

#[tokio::test]
async fn inspect_review_reads_branch_checks() {
    let server = TestGitea::start(vec![
        (
            200,
            json!({
                "number": 7,
                "state": "open",
                "merged": false,
                "head": {"ref": "zeroshot/run/42", "sha": HEAD},
                "base": {"ref": "main", "sha": OTHER},
                "mergeable": true,
            })
            .to_string(),
        ),
        (
            200,
            json!({"state": "success", "statuses": [{"status": "success"}]}).to_string(),
        ),
    ]);
    let authority = authority(&server.base_url);
    let receipt = ForgeReviewReceipt {
        review_id: "7".to_owned(),
        repository: "acme/project".to_owned(),
        target_branch: "main".to_owned(),
        head_branch: "zeroshot/run/42".to_owned(),
        head_revision: HEAD.to_owned(),
    };
    let observation = authority
        .inspect_review(&receipt, credential())
        .await
        .expect("observation");
    // Gitea has no merge-policy gate, so passing checks are mergeable and never "PR ready".
    assert!(!observation.pull_request_ready);
    assert_eq!(
        observation.state,
        ForgeReviewState::Open {
            checks: ForgeChecks::Passed
        }
    );
    let requests = server.requests();
    assert_eq!(
        requests[1].1,
        format!("/api/v1/repos/acme/project/commits/{HEAD}/status")
    );
}

#[tokio::test]
async fn find_review_uses_the_by_base_head_endpoint() {
    let pull = json!({
        "number": 7,
        "state": "open",
        "merged": false,
        "head": {"ref": "zeroshot/run/42", "sha": HEAD},
        "base": {"ref": "main", "sha": OTHER},
        "mergeable": true,
    })
    .to_string();
    let server = TestGitea::start(vec![
        (200, pull),
        (404, json!({"message": "not found"}).to_string()),
    ]);
    let authority = authority(&server.base_url);
    let receipt = authority
        .find_review(ReviewQuery {
            repository: "acme/project",
            head_branch: "zeroshot/run/42",
            target_branch: "main",
            credential: credential(),
        })
        .await
        .expect("lookup")
        .expect("review");
    assert_eq!(receipt.review_id, "7");
    assert!(
        authority
            .find_review(ReviewQuery {
                repository: "acme/project",
                head_branch: "zeroshot/run/99",
                target_branch: "main",
                credential: credential(),
            })
            .await
            .expect("lookup")
            .is_none()
    );
    let requests = server.requests();
    assert_eq!(
        requests[0].1,
        "/api/v1/repos/acme/project/pulls/main/zeroshot/run/42"
    );
    assert_eq!(
        requests[1].1,
        "/api/v1/repos/acme/project/pulls/main/zeroshot/run/99"
    );
}

#[tokio::test]
async fn inspect_review_feedback_reads_all_discussion_surfaces() {
    let pull = json!({
        "number": 7,
        "state": "open",
        "merged": false,
        "head": {"ref": "zeroshot/run/42", "sha": HEAD},
        "base": {"ref": "main", "sha": OTHER},
        "mergeable": true,
    })
    .to_string();
    let server = TestGitea::start(vec![
        (200, pull.clone()),
        (
            200,
            json!([{
                "id": 11,
                "user": {"login": "reviewer"},
                "body": "Please rename this",
                "updated_at": "2024-01-02T03:04:05Z",
            }])
            .to_string(),
        ),
        (
            200,
            json!([{
                "id": 21,
                "state": "REQUEST_CHANGES",
                "commit_id": HEAD,
                "submitted_at": "2024-01-02T03:05:00Z",
                "user": {"login": "approver"},
                "body": "",
            }])
            .to_string(),
        ),
        (
            200,
            json!([{
                "id": 31,
                "user": {"login": "reviewer"},
                "body": "This line is wrong",
                "path": "src/lib.rs",
                "position": 42,
                "commit_id": HEAD,
                "updated_at": "2024-01-02T03:06:00Z",
            }])
            .to_string(),
        ),
        (200, pull),
    ]);
    let authority = authority(&server.base_url);
    let receipt = ForgeReviewReceipt {
        review_id: "7".to_owned(),
        repository: "acme/project".to_owned(),
        target_branch: "main".to_owned(),
        head_branch: "zeroshot/run/42".to_owned(),
        head_revision: HEAD.to_owned(),
    };
    let feedback = authority
        .inspect_review_feedback(&receipt, credential())
        .await
        .expect("feedback");
    assert_eq!(feedback.items.len(), 3, "{feedback:?}");
    assert!(
        feedback
            .items
            .iter()
            .any(|item| item.key == "issue_comment:11")
    );
    assert!(
        feedback
            .items
            .iter()
            .any(|item| item.key == "review:21" && item.author == "approver")
    );
    assert!(
        feedback
            .items
            .iter()
            .any(|item| item.key == "review_comment:31")
    );
    assert!(feedback.items.iter().all(|item| item.version.len() == 64));

    let requests = server.requests();
    let paths: Vec<&str> = requests.iter().map(|(_, path)| path.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "/api/v1/repos/acme/project/pulls/7",
            "/api/v1/repos/acme/project/issues/7/comments?limit=50&page=1",
            "/api/v1/repos/acme/project/pulls/7/reviews?limit=50&page=1",
            "/api/v1/repos/acme/project/pulls/7/reviews/21/comments?limit=50&page=1",
            "/api/v1/repos/acme/project/pulls/7",
        ]
    );
}

#[tokio::test]
async fn request_merge_reports_a_conflict_without_merging() {
    let server = TestGitea::start(vec![(
        200,
        json!({
            "number": 7,
            "state": "open",
            "merged": false,
            "head": {"ref": "zeroshot/run/42", "sha": HEAD},
            "base": {"ref": "main", "sha": OTHER},
            "mergeable": false,
        })
        .to_string(),
    )]);
    let authority = authority(&server.base_url);
    let receipt = ForgeReviewReceipt {
        review_id: "7".to_owned(),
        repository: "acme/project".to_owned(),
        target_branch: "main".to_owned(),
        head_branch: "zeroshot/run/42".to_owned(),
        head_revision: HEAD.to_owned(),
    };
    assert_eq!(
        authority
            .request_merge(&receipt, credential())
            .await
            .expect("outcome"),
        ForgeMergeRequestOutcome::Conflict
    );
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn request_merge_accepts_once_gitea_merges() {
    let server = TestGitea::start(vec![
        (
            200,
            json!({
                "number": 7,
                "state": "open",
                "merged": false,
                "head": {"ref": "zeroshot/run/42", "sha": HEAD},
                "base": {"ref": "main", "sha": OTHER},
                "mergeable": true,
            })
            .to_string(),
        ),
        (200, "{}".to_owned()),
    ]);
    let authority = authority(&server.base_url);
    let receipt = ForgeReviewReceipt {
        review_id: "7".to_owned(),
        repository: "acme/project".to_owned(),
        target_branch: "main".to_owned(),
        head_branch: "zeroshot/run/42".to_owned(),
        head_revision: HEAD.to_owned(),
    };
    assert_eq!(
        authority
            .request_merge(&receipt, credential())
            .await
            .expect("outcome"),
        ForgeMergeRequestOutcome::Accepted
    );
    let requests = server.requests();
    assert_eq!(requests[1].0, "POST");
    assert_eq!(requests[1].1, "/api/v1/repos/acme/project/pulls/7/merge");
}

const MERGE_SHA: &str = "cccccccccccccccccccccccccccccccccccccccc";

/// Scenario selector for the stateful fake Gitea server. Each scenario drives the real
/// `GiteaDeliveryAuthority` REST client through a distinct provider response sequence while the
/// server holds branch, pull-request, check, and merge state across requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(unix), allow(dead_code))]
enum Scenario {
    Happy,
    NoChecks,
    CiFailed,
    PolicyForbidden,
    PolicySchemaInvalid,
    ProtectedBranch,
    StrictBehind,
    PreflightClosed,
    PreflightMerged,
    DeferredMerge,
    NeverConfirmsMerge,
    RegistrationRace,
    MultipleRegistrationWaves,
    CredentialExpires,
    ReviewSyncRace,
    ReviewIdentityMismatch,
}

struct GiteaState {
    remote: PathBuf,
    scenario: Scenario,
    branch: String,
    review: Mutex<Option<PullState>>,
    merge_requests: AtomicUsize,
    review_lookups: AtomicUsize,
    review_creates: AtomicUsize,
    checks_reads: AtomicUsize,
    branch_reads: AtomicUsize,
    credential_rejections: AtomicUsize,
}

struct PullState {
    number: u64,
    state: String,
    merged: bool,
    merge_commit_sha: Option<String>,
    head_ref: String,
    head_sha: String,
    base_ref: String,
    base_sha: String,
    mergeable: bool,
}

impl PullState {
    fn json(&self) -> Value {
        json!({
            "number": self.number,
            "state": self.state,
            "merged": self.merged,
            "merge_commit_sha": self.merge_commit_sha,
            "head": {"ref": self.head_ref, "sha": self.head_sha},
            "base": {"ref": self.base_ref, "sha": self.base_sha},
            "mergeable": self.mergeable,
        })
    }
}

impl GiteaState {
    fn new(remote: PathBuf, scenario: Scenario, branch: String) -> Self {
        let base_sha = remote_branch_sha(&remote, "main").unwrap_or_else(|| OTHER.to_owned());
        let head_sha = remote_branch_sha(&remote, &branch).unwrap_or_else(|| HEAD.to_owned());
        let review = match scenario {
            Scenario::PreflightClosed => Some(PullState {
                number: 7,
                state: "closed".to_owned(),
                merged: false,
                merge_commit_sha: None,
                head_ref: branch.clone(),
                head_sha: head_sha.clone(),
                base_ref: "main".to_owned(),
                base_sha: base_sha.clone(),
                mergeable: true,
            }),
            Scenario::PreflightMerged => Some(PullState {
                number: 7,
                state: "closed".to_owned(),
                merged: true,
                merge_commit_sha: Some(MERGE_SHA.to_owned()),
                head_ref: branch.clone(),
                head_sha: head_sha.clone(),
                base_ref: "main".to_owned(),
                base_sha: base_sha.clone(),
                mergeable: true,
            }),
            _ => None,
        };
        Self {
            remote,
            scenario,
            branch,
            review: Mutex::new(review),
            merge_requests: AtomicUsize::new(0),
            review_lookups: AtomicUsize::new(0),
            review_creates: AtomicUsize::new(0),
            checks_reads: AtomicUsize::new(0),
            branch_reads: AtomicUsize::new(0),
            credential_rejections: AtomicUsize::new(0),
        }
    }

    fn checks_body(&self, read: usize) -> &'static str {
        match self.scenario {
            Scenario::NoChecks => "{\"state\":\"success\",\"statuses\":[]}",
            Scenario::CiFailed => "{\"state\":\"failure\",\"statuses\":[{\"status\":\"failure\"}]}",
            Scenario::RegistrationRace if read == 1 => {
                "{\"state\":\"pending\",\"statuses\":[{\"status\":\"pending\"}]}"
            }
            Scenario::MultipleRegistrationWaves if matches!(read, 2 | 4) => {
                "{\"state\":\"pending\",\"statuses\":[{\"status\":\"pending\"}]}"
            }
            _ => "{\"state\":\"success\",\"statuses\":[{\"status\":\"success\"}]}",
        }
    }
}

fn remote_branch_sha(remote: &Path, branch: &str) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["-c", "core.autocrlf=false", "-C"])
        .arg(remote)
        .args(["rev-parse", "--verify", &format!("refs/heads/{branch}")])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

struct RecordedRequest {
    method: String,
    target: String,
    authorization: Option<String>,
    body: String,
}

struct TestGitea {
    base_url: String,
    recorded: Arc<Mutex<Vec<(String, String)>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    state: Option<Arc<GiteaState>>,
}

impl TestGitea {
    fn start(responses: Vec<(u16, String)>) -> Self {
        Self::spawn(Arc::new(Mutex::new(VecDeque::from(responses))), None)
    }

    fn scenario(remote: PathBuf, scenario: Scenario, run_id: &str) -> Self {
        let state = Arc::new(GiteaState::new(remote, scenario, delivery_branch(run_id)));
        Self::spawn(Arc::new(Mutex::new(VecDeque::new())), Some(state))
    }

    fn spawn(
        responses: Arc<Mutex<VecDeque<(u16, String)>>>,
        state: Option<Arc<GiteaState>>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let address = listener.local_addr().expect("address");
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let recorded = Arc::clone(&recorded);
            let stop = Arc::clone(&stop);
            let state = state.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let _ = stream.set_nonblocking(false);
                            serve(&mut stream, &responses, &recorded, state.as_deref());
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            })
        };
        Self {
            base_url: format!("http://{address}"),
            recorded,
            stop,
            handle: Some(handle),
            state,
        }
    }

    fn requests(&self) -> Vec<(String, String)> {
        self.recorded.lock().expect("recorded").clone()
    }

    fn state(&self) -> &Arc<GiteaState> {
        self.state.as_ref().expect("scenario server state")
    }
}

impl Drop for TestGitea {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn serve(
    stream: &mut TcpStream,
    responses: &Mutex<VecDeque<(u16, String)>>,
    recorded: &Mutex<Vec<(String, String)>>,
    state: Option<&GiteaState>,
) {
    let request = read_request(stream);
    recorded
        .lock()
        .expect("recorded")
        .push((request.method.clone(), request.target.clone()));
    let (status, body) = match state {
        Some(state) => respond_scenario(state, &request),
        None => responses
            .lock()
            .expect("responses")
            .pop_front()
            .unwrap_or((500, "{\"message\":\"unscripted request\"}".to_owned())),
    };
    let response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        reason(status),
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn respond_scenario(state: &GiteaState, request: &RecordedRequest) -> (u16, String) {
    if state.scenario == Scenario::CredentialExpires
        && request.authorization.as_deref() != Some("token refreshed-token")
    {
        state.credential_rejections.fetch_add(1, Ordering::SeqCst);
        return (401, json!({"message": "unauthorized"}).to_string());
    }
    let path = request.target.split('?').next().unwrap_or_default();
    let Some(rest) = path.strip_prefix("/api/v1/repos/acme/project/") else {
        return (404, json!({"message": "unknown repository"}).to_string());
    };
    let segments: Vec<&str> = rest.split('/').collect();
    match (request.method.as_str(), segments.as_slice()) {
        ("GET", ["branches", rest @ ..]) if !rest.is_empty() => {
            let branch = rest.join("/");
            state.branch_reads.fetch_add(1, Ordering::SeqCst);
            match remote_branch_sha(&state.remote, &branch) {
                Some(sha) => (
                    200,
                    json!({"name": branch, "commit": {"id": sha}}).to_string(),
                ),
                None => (404, json!({"message": "branch not found"}).to_string()),
            }
        }
        ("GET", ["commits", _revision, "status"]) => {
            let read = state.checks_reads.fetch_add(1, Ordering::SeqCst) + 1;
            match state.scenario {
                Scenario::PolicyForbidden => {
                    (403, json!({"message": "checks are forbidden"}).to_string())
                }
                Scenario::PolicySchemaInvalid => {
                    (400, json!({"message": "invalid checks schema"}).to_string())
                }
                _ => (200, state.checks_body(read).to_owned()),
            }
        }
        ("GET", ["issues", _id, "comments"]) => (200, "[]".to_owned()),
        ("GET", ["pulls", _id, "reviews"]) => (200, "[]".to_owned()),
        ("GET", ["pulls", _id, "reviews", _rid, "comments"]) => (200, "[]".to_owned()),
        ("POST", ["pulls", _id, "merge"]) => respond_merge(state),
        ("GET", ["pulls", _id]) => respond_pull(state, mismatch_id(state)),
        ("PATCH", ["pulls", _id]) => respond_pull(state, mismatch_id(state)),
        ("POST", ["pulls"]) => respond_create(state, request),
        ("GET", ["pulls", _base, head @ ..]) if !head.is_empty() => {
            let lookup = state.review_lookups.fetch_add(1, Ordering::SeqCst) + 1;
            if state.scenario == Scenario::ReviewSyncRace && lookup == 1 {
                return (404, json!({"message": "not found"}).to_string());
            }
            respond_pull(state, false)
        }
        _ => (404, json!({"message": "unsupported"}).to_string()),
    }
}

fn mismatch_id(state: &GiteaState) -> bool {
    state.scenario == Scenario::ReviewIdentityMismatch
}

fn respond_pull(state: &GiteaState, mismatch: bool) -> (u16, String) {
    let review = state.review.lock().expect("review");
    match review.as_ref() {
        Some(pull) => {
            let mut value = pull.json();
            if mismatch {
                value["number"] = json!(8);
            }
            if state.scenario == Scenario::StrictBehind {
                value["mergeable"] = json!(false);
            }
            (200, value.to_string())
        }
        None => (404, json!({"message": "not found"}).to_string()),
    }
}

fn respond_create(state: &GiteaState, request: &RecordedRequest) -> (u16, String) {
    let create = state.review_creates.fetch_add(1, Ordering::SeqCst) + 1;
    if state.scenario == Scenario::ReviewSyncRace && create == 1 {
        return (
            422,
            json!({"message": "review head is not visible"}).to_string(),
        );
    }
    let body: Value = serde_json::from_str(&request.body).unwrap_or(Value::Null);
    let head_ref = body
        .get("head")
        .and_then(Value::as_str)
        .unwrap_or(&state.branch)
        .to_owned();
    let base_ref = body
        .get("base")
        .and_then(Value::as_str)
        .unwrap_or("main")
        .to_owned();
    let head_sha = remote_branch_sha(&state.remote, &head_ref).unwrap_or_else(|| HEAD.to_owned());
    let base_sha = remote_branch_sha(&state.remote, &base_ref).unwrap_or_else(|| OTHER.to_owned());
    let mut review = state.review.lock().expect("review");
    let pull = PullState {
        number: 7,
        state: "open".to_owned(),
        merged: false,
        merge_commit_sha: None,
        head_ref,
        head_sha,
        base_ref,
        base_sha,
        mergeable: true,
    };
    let response = pull.json().to_string();
    *review = Some(pull);
    (201, response)
}

fn respond_merge(state: &GiteaState) -> (u16, String) {
    let request = state.merge_requests.fetch_add(1, Ordering::SeqCst) + 1;
    match state.scenario {
        Scenario::ProtectedBranch => {
            return (409, json!({"message": "branch is protected"}).to_string());
        }
        Scenario::NeverConfirmsMerge => {
            return (200, json!({"message": "merge requested"}).to_string());
        }
        Scenario::DeferredMerge if request <= 4 => {
            return (200, json!({"message": "merge deferred"}).to_string());
        }
        Scenario::RegistrationRace if request == 1 => {
            return (200, json!({"message": "checks are pending"}).to_string());
        }
        Scenario::MultipleRegistrationWaves if request <= 2 => {
            return (200, json!({"message": "merge deferred"}).to_string());
        }
        _ => {}
    }
    let mut review = state.review.lock().expect("review");
    if let Some(pull) = review.as_mut() {
        pull.state = "closed".to_owned();
        pull.merged = true;
        pull.merge_commit_sha = Some(MERGE_SHA.to_owned());
    }
    (200, json!({"message": "merged"}).to_string())
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        500 => "Internal Server Error",
        _ => "Response",
    }
}

fn read_request(stream: &mut TcpStream) -> RecordedRequest {
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut request_line = String::new();
    reader.read_line(&mut request_line).expect("request line");
    let mut content_length = 0;
    let mut authorization = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).expect("header") == 0 || line == "\r\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap_or(0);
        }
        if let Some(value) = line.strip_prefix("authorization:").or_else(|| {
            line.split_once(": ")
                .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                .map(|(_, value)| value)
        }) {
            authorization = Some(value.trim().to_owned());
        }
    }
    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).expect("body");
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    RecordedRequest {
        method,
        target,
        authorization,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

#[cfg(unix)]
mod conflict {
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

    use openengine_cluster_testkit::assertions::AssertValue;

    use super::*;
    use crate::native_v2_candidate::test_support::{
        TestGitRepository, commit_all, git, git_output, path_text,
    };

    struct ConflictFixture {
        repository: TestGitRepository,
        authority: GiteaDeliveryAuthority,
        request: ForgeConflictRequest,
        target_revision: String,
        git_program: PathBuf,
        _server: TestGitea,
    }

    impl ConflictFixture {
        fn new(target_file: &str) -> Self {
            let repository = TestGitRepository::delivery();
            commit_all(&repository.workspace, "worker change");
            let head_revision = git_output(&repository.workspace, &["rev-parse", "HEAD"]);
            let target = repository.root.child("target");
            git(
                repository.root.path(),
                &["clone", path_text(&repository.remote), path_text(&target)],
            );
            fs::write(target.join(target_file), "target\n").assert_value();
            commit_all(&target, "target change");
            git(&target, &["push", "origin", "main"]);
            let target_revision = git_output(&target, &["rev-parse", "HEAD"]);
            let server = TestGitea::start(vec![(
                200,
                json!({"commit": {"id": target_revision.clone()}}).to_string(),
            )]);
            let git_program = git_wrapper(&repository, &server.base_url);
            let authority = authority_with_program(&server.base_url, git_program.clone());
            let request = ForgeConflictRequest {
                workspace: repository.workspace.clone(),
                review: ForgeReviewReceipt {
                    review_id: "7".to_owned(),
                    repository: "acme/project".to_owned(),
                    target_branch: "main".to_owned(),
                    head_branch: "zeroshot/run/42".to_owned(),
                    head_revision,
                },
            };
            Self {
                repository,
                authority,
                request,
                target_revision,
                git_program,
                _server: server,
            }
        }
    }

    fn authority_with_program(base_url: &str, git_program: PathBuf) -> GiteaDeliveryAuthority {
        let mut config = GiteaAuthorityConfig::new(base_url.to_owned(), git_program);
        config.api_deadline = Duration::from_secs(10);
        config.push_deadline = Duration::from_secs(10);
        GiteaDeliveryAuthority::new(config)
    }

    fn git_wrapper(repository: &TestGitRepository, base_url: &str) -> PathBuf {
        let remote = path_text(&repository.remote);
        assert!(!remote.contains('\''));
        assert!(!base_url.contains('\''));
        repository.root.write_executable(
            "gitea-git-wrapper",
            &format!(
                r#"#!/bin/bash
set -eu
/usr/bin/printf 'arg=%s\n' "${{GH_TOKEN-unset}}" >> "${{0}}.capture"
for argument in "$@"; do /usr/bin/printf 'arg=%s\n' "$argument" >> "${{0}}.capture"; done
arguments=()
for argument in "$@"; do
  if [[ "$argument" == '{base_url}/acme/project.git' ]]; then
    arguments+=('{remote}')
  else
    arguments+=("$argument")
  fi
done
exec /usr/bin/git "${{arguments[@]}}"
"#,
            ),
        )
    }

    #[tokio::test]
    async fn authenticated_target_fetch_leaves_exact_conflict_for_repair() {
        let fixture = ConflictFixture::new("result.txt");

        let outcome = fixture
            .authority
            .materialize_merge_conflict(&fixture.request, credential())
            .await
            .assert_value();
        let ForgeConflictOutcome::Materialized(materialized) = outcome else {
            panic!("expected a materialized merge conflict");
        };

        assert_eq!(materialized.target_revision, fixture.target_revision);
        assert_eq!(materialized.conflicted_paths, vec!["result.txt"]);
        assert_eq!(
            git_output(&fixture.repository.workspace, &["rev-parse", "MERGE_HEAD"]),
            fixture.target_revision
        );
        assert!(
            fs::read_to_string(fixture.repository.workspace.join("result.txt"))
                .assert_value()
                .contains("<<<<<<<")
        );
        assert!(
            git_output(&fixture.repository.workspace, &["status", "--porcelain=v1"])
                .contains("AA result.txt")
        );
        let capture =
            fs::read_to_string(format!("{}.capture", fixture.git_program.display())).assert_value();
        assert!(capture.contains("arg=fetch"));
        assert!(capture.contains(&format!("arg={}", fixture.target_revision)));
    }

    #[tokio::test]
    async fn conflict_handoff_pins_repair_commit_to_delivery_identity() {
        let fixture = ConflictFixture::new("result.txt");
        git(
            &fixture.repository.workspace,
            &["config", "--local", "user.name", "Codex"],
        );
        git(
            &fixture.repository.workspace,
            &["config", "--local", "user.email", "codex@openai.com"],
        );

        let outcome = fixture
            .authority
            .materialize_merge_conflict(&fixture.request, credential())
            .await
            .assert_value();
        assert!(matches!(outcome, ForgeConflictOutcome::Materialized(_)));

        fs::write(
            fixture.repository.workspace.join("result.txt"),
            "resolved\n",
        )
        .assert_value();
        git(&fixture.repository.workspace, &["add", "--all"]);
        git(
            &fixture.repository.workspace,
            &["commit", "--no-verify", "--message", "repair conflict"],
        );

        assert_eq!(
            git_output(
                &fixture.repository.workspace,
                &["show", "--no-patch", "--format=%an <%ae>"]
            ),
            "Zeroshot <delivery@zeroshot.invalid>"
        );
        assert_eq!(
            git_output(
                &fixture.repository.workspace,
                &["show", "--no-patch", "--format=%P"]
            ),
            format!(
                "{} {}",
                fixture.request.review.head_revision, fixture.target_revision
            )
        );
    }

    #[tokio::test]
    async fn stale_conflict_observation_restores_the_review_head_for_reobservation() {
        let fixture = ConflictFixture::new("target.txt");

        let outcome = fixture
            .authority
            .materialize_merge_conflict(&fixture.request, credential())
            .await
            .assert_value();

        assert_eq!(outcome, ForgeConflictOutcome::ObservationChanged);
        assert_eq!(
            git_output(&fixture.repository.workspace, &["rev-parse", "HEAD"]),
            fixture.request.review.head_revision
        );
        assert_eq!(
            git_output(
                &fixture.repository.workspace,
                &["status", "--porcelain=v1", "--untracked-files=all"]
            ),
            ""
        );
        assert!(
            !fixture
                .repository
                .workspace
                .join(".git/MERGE_HEAD")
                .exists()
        );
        assert!(!fixture.repository.workspace.join("target.txt").exists());
        let capture =
            fs::read_to_string(format!("{}.capture", fixture.git_program.display())).assert_value();
        assert!(capture.contains("arg=--abort"));
    }
}

// ---------------------------------------------------------------------------
// Scenario-driven fake Gitea authority tests.
// ---------------------------------------------------------------------------

fn gitea_target(repo: &TestGitRepository) -> DeliveryTarget {
    DeliveryTarget::new("acme/project", "main", repo.base.clone()).assert_value()
}

fn review_request(fixture: &AuthorityScenario) -> ForgeReviewRequest {
    ForgeReviewRequest {
        target: gitea_target(&fixture.repo),
        head_branch: fixture.branch.clone(),
        head_revision: fixture.revision.clone(),
        title: "Deliver the change".to_owned(),
        description: "Body".to_owned(),
        source_issue: None,
    }
}

fn delivery_read<'a>(target: &'a DeliveryTarget, branch: &'a str) -> ForgeDeliveryRead<'a> {
    ForgeDeliveryRead {
        target,
        head_branch: branch,
        known_review: None,
        include_review: true,
    }
}

struct AuthorityScenario {
    repo: TestGitRepository,
    server: TestGitea,
    authority: GiteaDeliveryAuthority,
    branch: String,
    revision: String,
}

impl AuthorityScenario {
    fn new(scenario: Scenario, run_id: &str) -> Self {
        let repo = TestGitRepository::delivery();
        commit_all(&repo.workspace, "candidate");
        let revision = git_output(&repo.workspace, &["rev-parse", "HEAD"]);
        let branch = delivery_branch(run_id);
        git(
            &repo.workspace,
            &["push", "origin", &format!("HEAD:refs/heads/{branch}")],
        );
        let server = TestGitea::scenario(repo.remote.clone(), scenario, run_id);
        let authority = authority(&server.base_url);
        Self {
            repo,
            server,
            authority,
            branch,
            revision,
        }
    }
}

#[tokio::test]
async fn ci_failure_is_reported_as_failed_checks() {
    let fixture = AuthorityScenario::new(Scenario::CiFailed, "gitea-ci");
    let receipt = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    let observation = fixture
        .authority
        .inspect_review(&receipt, credential())
        .await
        .assert_value();
    assert!(matches!(
        observation.state,
        ForgeReviewState::Open {
            checks: ForgeChecks::Failed { .. }
        }
    ));
}

#[tokio::test]
async fn policy_failures_are_typed_api_errors() {
    for (scenario, status) in [
        (Scenario::PolicyForbidden, 403),
        (Scenario::PolicySchemaInvalid, 400),
    ] {
        let fixture = AuthorityScenario::new(scenario, "gitea-policy");
        let receipt = fixture
            .authority
            .open_or_update_review(&review_request(&fixture), credential())
            .await
            .assert_value();
        let failure = fixture
            .authority
            .inspect_review(&receipt, credential())
            .await
            .expect_err("policy failure must be typed");
        assert_eq!(failure.api_status(), Some(status));
        assert!(!failure.retryable_operation());
    }
}

#[tokio::test]
async fn protected_branch_merge_is_reported_as_a_conflict() {
    let fixture = AuthorityScenario::new(Scenario::ProtectedBranch, "gitea-protected");
    let receipt = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    assert_eq!(
        fixture
            .authority
            .request_merge(&receipt, credential())
            .await
            .assert_value(),
        ForgeMergeRequestOutcome::Conflict
    );
    assert_eq!(
        fixture.server.state().merge_requests.load(Ordering::SeqCst),
        1
    );
}

#[tokio::test]
async fn strict_behind_pull_is_reported_as_a_conflict_without_merging() {
    let fixture = AuthorityScenario::new(Scenario::StrictBehind, "gitea-behind");
    let receipt = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    let observation = fixture
        .authority
        .inspect_review(&receipt, credential())
        .await
        .assert_value();
    assert_eq!(observation.state, ForgeReviewState::Conflict);
    assert_eq!(
        fixture
            .authority
            .request_merge(&receipt, credential())
            .await
            .assert_value(),
        ForgeMergeRequestOutcome::Conflict
    );
    assert_eq!(
        fixture.server.state().merge_requests.load(Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn deferred_merge_is_accepted_until_gitea_confirms() {
    let fixture = AuthorityScenario::new(Scenario::DeferredMerge, "gitea-deferred");
    let receipt = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    for _ in 0..5 {
        assert_eq!(
            fixture
                .authority
                .request_merge(&receipt, credential())
                .await
                .assert_value(),
            ForgeMergeRequestOutcome::Accepted
        );
    }
    let observation = fixture
        .authority
        .inspect_review(&receipt, credential())
        .await
        .assert_value();
    assert!(matches!(observation.state, ForgeReviewState::Merged { .. }));
}

#[tokio::test]
async fn never_confirmed_merge_stays_open() {
    let fixture = AuthorityScenario::new(Scenario::NeverConfirmsMerge, "gitea-never");
    let receipt = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    for _ in 0..3 {
        assert_eq!(
            fixture
                .authority
                .request_merge(&receipt, credential())
                .await
                .assert_value(),
            ForgeMergeRequestOutcome::Accepted
        );
    }
    let observation = fixture
        .authority
        .inspect_review(&receipt, credential())
        .await
        .assert_value();
    assert!(matches!(observation.state, ForgeReviewState::Open { .. }));
}

#[tokio::test]
async fn registration_race_observes_pending_checks_before_merge() {
    let fixture = AuthorityScenario::new(Scenario::RegistrationRace, "gitea-race");
    let receipt = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    let pending = fixture
        .authority
        .inspect_review(&receipt, credential())
        .await
        .assert_value();
    assert!(matches!(
        pending.state,
        ForgeReviewState::Open {
            checks: ForgeChecks::Pending
        }
    ));
    let passed = fixture
        .authority
        .inspect_review(&receipt, credential())
        .await
        .assert_value();
    assert!(matches!(
        passed.state,
        ForgeReviewState::Open {
            checks: ForgeChecks::Passed
        }
    ));
}

#[tokio::test]
async fn multiple_registration_waves_repeat_pending_checks() {
    let fixture = AuthorityScenario::new(Scenario::MultipleRegistrationWaves, "gitea-waves");
    let receipt = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    let mut observed = Vec::new();
    for _ in 0..6 {
        observed.push(
            fixture
                .authority
                .inspect_review(&receipt, credential())
                .await
                .assert_value()
                .state,
        );
    }
    let pending = observed
        .iter()
        .filter(|state| {
            matches!(
                state,
                ForgeReviewState::Open {
                    checks: ForgeChecks::Pending
                }
            )
        })
        .count();
    assert_eq!(pending, 2);
}

#[tokio::test]
async fn review_sync_race_is_retryable_then_succeeds() {
    let fixture = AuthorityScenario::new(Scenario::ReviewSyncRace, "gitea-sync");
    let failure = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .expect_err("the first review creation must race");
    assert!(failure.retryable_review_sync());
    let receipt = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    assert_eq!(receipt.review_id, "7");
}

#[tokio::test]
async fn expired_credential_is_an_authentication_failure_then_recovers() {
    let fixture = AuthorityScenario::new(Scenario::CredentialExpires, "gitea-expiry");
    let failure = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .expect_err("the stale credential must be refused");
    assert!(failure.authentication_failed());
    let receipt = fixture
        .authority
        .open_or_update_review(
            &review_request(&fixture),
            ForgeCredential("refreshed-token"),
        )
        .await
        .assert_value();
    assert_eq!(receipt.review_id, "7");
    assert!(
        fixture
            .server
            .state()
            .credential_rejections
            .load(Ordering::SeqCst)
            >= 1
    );
}

#[tokio::test]
async fn preflight_closed_and_merged_observations_are_terminal() {
    for (scenario, merged) in [
        (Scenario::PreflightClosed, false),
        (Scenario::PreflightMerged, true),
    ] {
        let fixture = AuthorityScenario::new(scenario, "gitea-preflight");
        let target = gitea_target(&fixture.repo);
        let snapshot = fixture
            .authority
            .observe_delivery(delivery_read(&target, &fixture.branch), credential())
            .await
            .assert_value();
        let review = snapshot.review.expect("seeded review");
        assert_eq!(review.head_revision, fixture.revision);
        match review.state {
            ForgeReviewState::Closed => assert!(!merged),
            ForgeReviewState::Merged { .. } => assert!(merged),
            other => panic!("unexpected preflight state: {other:?}"),
        }
    }
}

#[tokio::test]
async fn review_identity_mismatch_is_rejected() {
    let fixture = AuthorityScenario::new(Scenario::ReviewIdentityMismatch, "gitea-mismatch");
    let created = fixture
        .authority
        .open_or_update_review(&review_request(&fixture), credential())
        .await
        .assert_value();
    assert_eq!(created.review_id, "7");
    let target = gitea_target(&fixture.repo);
    let read = ForgeDeliveryRead {
        target: &target,
        head_branch: &fixture.branch,
        known_review: Some(&created),
        include_review: true,
    };
    let failure = fixture
        .authority
        .observe_delivery(read, credential())
        .await
        .expect_err("a mismatched review identity must fail closed");
    assert!(failure.to_string().contains("expected PR 7"), "{failure}");
}

// ---------------------------------------------------------------------------
// Adapter-level scenarios (Unix-only: they install executable git wrappers).
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod adapter_scenarios {
    use std::collections::BTreeMap;
    use std::fs;

    use async_trait::async_trait;

    use super::*;
    use crate::native_v2_admission::NativeV2Admission;
    use crate::native_v2_candidate::test_support::{full_graph, success_node};
    use crate::native_v2_contract::{
        AdmittedRun, CodexProvider, DeclaredConnections, DeclaredEnvironment, ExecutionId,
        ExecutionRef, NodeInstanceId, NodeInvocation, NodeRuntimeBinding, PullRequestFeedback,
        ResolvedSource, RunSize, RunSubmission, RunTitle, RuntimePlan, SourceBranchId,
        SourceRepositoryId, SourceRevisionId,
    };
    use crate::native_v2_delivery::contract::{
        delivery_diagnostic_schema, delivery_result_schema, delivery_signal_labels,
        is_matching_success_receipt,
    };
    use crate::native_v2_delivery::{
        DELIVERY_CI_FAILED_LABEL, DELIVERY_MERGED_LABEL, DELIVERY_SIGNAL_FIELD,
        DeliveryForgeAuthority, DeliveryMode, DeliveryPollPolicy, NativeV2DeliveryAdapter,
        NativeV2DeliveryConfig,
    };
    use crate::native_v2_runner::{
        DurableNodeEvent, LiveOutput, NativeNodeRunner, NodeRunRequest, NodeRunner,
        ResolvedEnvironment, RuntimeEnvironmentRefresh, with_environment_refresh,
    };
    use openengine_cluster_protocol::{
        EnvironmentVariableName, FieldName, IdempotencyKey, NodeName, RunId, WorkerErrorCode,
        WorkerOutcome, WorkerRef,
    };

    const GITEA_TOKEN: &str = "GITEA_TOKEN";

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum WrapperMode {
        Normal,
        RejectCredential,
        LosePushResponse,
    }

    fn gitea_git_wrapper(root: &Path, remote: &Path, mode: WrapperMode) -> PathBuf {
        let reject = u8::from(mode == WrapperMode::RejectCredential);
        let lose = u8::from(mode == WrapperMode::LosePushResponse);
        let script = format!(
            r#"#!/bin/sh
set -u
capture="${{0}}.capture"
{{
  /usr/bin/printf '%s\n' '---'
  /usr/bin/printf 'token=%s\n' "${{GH_TOKEN-unset}}"
  /usr/bin/printf 'home=%s\n' "${{HOME-unset}}"
  /usr/bin/printf 'config_value_1=%s\n' "${{GIT_CONFIG_VALUE_1-unset}}"
  for argument in "$@"; do /usr/bin/printf 'arg=%s\n' "$argument"; done
}} >> "$capture"
remote="{remote}"
is_push=0
for argument in "$@"; do
  if [ "$argument" = "push" ]; then is_push=1; fi
done
if [ "$is_push" = "1" ] && [ "{reject}" = "1" ]; then
  /usr/bin/printf "fatal: Authentication failed for '%s'\n" "$remote" >&2
  exit 128
fi
for argument in "$@"; do
  case "$argument" in
    http://*|https://*) set -- "$@" "$remote" ;;
    *) set -- "$@" "$argument" ;;
  esac
  shift
done
/usr/bin/git "$@"
status=$?
if [ "$is_push" = "1" ] && [ "{lose}" = "1" ] && [ "$status" = "0" ]; then
  /usr/bin/printf 'remote: response lost\n' >&2
  exit 128
fi
exit "$status"
"#,
            remote = remote.display(),
        );
        let path = root.join(format!(
            "gitea-git-{}",
            root.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default()
        ));
        openengine_cluster_testkit::fixture::write_executable(&path, &script, 0o755)
            .assert_value_with("write gitea git wrapper");
        path
    }

    struct AdapterScenario {
        repo: TestGitRepository,
        server: TestGitea,
        wrapper: PathBuf,
    }

    impl AdapterScenario {
        fn new(scenario: Scenario, run_id: &str, mode: WrapperMode) -> Self {
            let repo = TestGitRepository::delivery();
            if matches!(
                scenario,
                Scenario::PreflightClosed | Scenario::PreflightMerged
            ) {
                commit_all(&repo.workspace, "candidate");
                git(
                    &repo.workspace,
                    &[
                        "push",
                        "origin",
                        &format!("HEAD:refs/heads/{}", delivery_branch(run_id)),
                    ],
                );
            }
            let wrapper = gitea_git_wrapper(repo.root.path(), &repo.remote, mode);
            let server = TestGitea::scenario(repo.remote.clone(), scenario, run_id);
            Self {
                repo,
                server,
                wrapper,
            }
        }

        fn authority(&self) -> Arc<GiteaDeliveryAuthority> {
            let mut config =
                GiteaAuthorityConfig::new(self.server.base_url.clone(), self.wrapper.clone());
            config.api_deadline = Duration::from_secs(5);
            config.push_deadline = Duration::from_secs(10);
            Arc::new(GiteaDeliveryAuthority::new(config))
        }

        fn capture(&self) -> String {
            fs::read_to_string(format!("{}.capture", self.wrapper.display())).assert_value()
        }
    }

    struct GiteaExecution {
        outcome: WorkerOutcome,
        output: Vec<LiveOutput>,
    }

    struct GiteaRunRequest<'a> {
        repo: &'a TestGitRepository,
        attempts: usize,
        mode: DeliveryMode,
        run_id: &'a str,
        refresh: Option<Arc<dyn RuntimeEnvironmentRefresh>>,
    }

    async fn run_gitea(
        request: GiteaRunRequest<'_>,
        authority: Arc<dyn DeliveryForgeAuthority>,
    ) -> GiteaExecution {
        let config = NativeV2DeliveryConfig {
            delivery_run_id: RunId::new(request.run_id),
            adopt_existing_delivery: false,
            git_identity: None,
            workspace: request.repo.workspace.clone(),
            git_program: PathBuf::from("/usr/bin/git"),
            target: gitea_target(request.repo),
            poll: DeliveryPollPolicy::new(request.attempts, Duration::ZERO).assert_value(),
        };
        let adapter = Arc::new(NativeV2DeliveryAdapter::new(config, authority));
        let admitted = gitea_admitted(request.repo, request.mode).await;
        let runner = NativeNodeRunner::new(&admitted, adapter.clone(), adapter).assert_value();
        let binding = admitted
            .runtime
            .nodes()
            .get(&NodeName::new("deliver").assert_value())
            .assert_value()
            .clone();
        let mut environment = ResolvedEnvironment::exact(
            &binding,
            BTreeMap::from([(
                EnvironmentVariableName::new(GITEA_TOKEN).assert_value(),
                "test-token".to_owned(),
            )]),
        )
        .assert_value();
        if let Some(refresh) = request.refresh {
            environment = with_environment_refresh(environment, refresh);
        }
        let mut handle = runner
            .start(NodeRunRequest {
                invocation: NodeInvocation {
                    reference: ExecutionRef {
                        run_id: RunId::new(request.run_id),
                        node: NodeName::new("deliver").assert_value(),
                        node_instance: NodeInstanceId::new(1).assert_value(),
                        execution: ExecutionId::new(1).assert_value(),
                    },
                    worker: WorkerRef::new(request.mode.worker_ref()).assert_value(),
                    instructions: None,
                    input: Value::Null,
                    binding,
                },
                environment,
            })
            .await
            .assert_value();
        let mut durable = handle.take_initial_output().assert_value();
        let outcome = handle.completion().await.assert_value().outcome;
        let mut output = Vec::new();
        while let Ok(event) = durable.recv().await {
            if let DurableNodeEvent::Output { output: event, .. } = event {
                output.push(event);
            }
        }
        GiteaExecution { outcome, output }
    }

    async fn gitea_admitted(repo: &TestGitRepository, mode: DeliveryMode) -> AdmittedRun {
        let graph = full_graph(vec![gitea_delivery_node(mode), success_node()]);
        let binding = NodeRuntimeBinding::GitDelivery {
            connections: DeclaredConnections::single(
                "gitea",
                DeclaredEnvironment::new(
                    [EnvironmentVariableName::new(GITEA_TOKEN).assert_value()],
                )
                .assert_value(),
            )
            .assert_value(),
            pull_request_feedback: PullRequestFeedback::Consider,
        };
        NativeV2Admission
            .admit(RunSubmission {
                environment: None,
                title: RunTitle::new("Gitea delivery test").assert_value(),
                graph,
                initial_input: Value::Null,
                runtime: RuntimePlan::Codex {
                    provider: CodexProvider::OpenAi,
                    size: RunSize::Medium,
                    nodes: BTreeMap::from([(NodeName::new("deliver").assert_value(), binding)]),
                },
                source: ResolvedSource {
                    repository: SourceRepositoryId::new("acme/project").assert_value(),
                    branch: SourceBranchId::new("main").assert_value(),
                    revision: SourceRevisionId::new(repo.base.clone()).assert_value(),
                },
                submission_key: IdempotencyKey::new(format!(
                    "gitea-{}-{}",
                    repo.base,
                    mode.label()
                ))
                .assert_value(),
            })
            .await
            .assert_value()
    }

    fn gitea_delivery_node(mode: DeliveryMode) -> Value {
        let labels = delivery_signal_labels(mode).assert_value();
        json!({
            "kind":"verifier","name":"deliver","worker":mode.worker_ref(),
            "input":{"kind":"null"},"output":delivery_result_schema(mode).assert_value(),
            "inputBindings":[],"writeBindings":[],"timeoutMs":1000,"attempts":1,
            "signals":{"delivery":labels},"diagnostic":delivery_diagnostic_schema().assert_value()
        })
    }

    fn assert_gitea_signal<'a>(outcome: &'a WorkerOutcome, expected: &str) -> &'a Value {
        let extracted = match outcome {
            WorkerOutcome::Verifier {
                output,
                signals,
                diagnostic,
                artifacts,
            } => Some((output, signals, diagnostic, artifacts)),
            _ => None,
        };
        let (output, signals, diagnostic, artifacts) =
            extracted.assert_value_with("delivery must return a verifier result");
        assert_eq!(
            signals
                .get(&FieldName::new(DELIVERY_SIGNAL_FIELD).assert_value())
                .assert_value()
                .as_str(),
            expected
        );
        assert!(diagnostic.pointer("/message").is_some_and(Value::is_string));
        assert!(artifacts.is_empty());
        output
    }

    fn assert_gitea_receipt(
        output: &Value,
        mode: DeliveryMode,
        repo: &TestGitRepository,
        expected: bool,
    ) {
        assert_eq!(
            is_matching_success_receipt(output, mode, &gitea_target(repo)),
            expected
        );
    }

    #[tokio::test]
    async fn gitea_merge_completes_after_authoritative_confirmation() {
        let scenario = AdapterScenario::new(Scenario::Happy, "gitea-merge", WrapperMode::Normal);
        let execution = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 3,
                mode: DeliveryMode::Merge,
                run_id: "gitea-merge",
                refresh: None,
            },
            scenario.authority(),
        )
        .await;
        assert!(!execution.output.is_empty());
        let output = assert_gitea_signal(&execution.outcome, DELIVERY_MERGED_LABEL);
        assert_gitea_receipt(output, DeliveryMode::Merge, &scenario.repo, true);
        assert_eq!(
            scenario
                .server
                .state()
                .merge_requests
                .load(Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn gitea_no_checks_can_merge() {
        let scenario =
            AdapterScenario::new(Scenario::NoChecks, "gitea-no-checks", WrapperMode::Normal);
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 3,
                mode: DeliveryMode::Merge,
                run_id: "gitea-no-checks",
                refresh: None,
            },
            scenario.authority(),
        )
        .await
        .outcome;
        assert_gitea_signal(&outcome, DELIVERY_MERGED_LABEL);
    }

    #[tokio::test]
    async fn gitea_ci_failure_is_a_routable_verifier_result() {
        let scenario =
            AdapterScenario::new(Scenario::CiFailed, "gitea-ci-run", WrapperMode::Normal);
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 2,
                mode: DeliveryMode::Merge,
                run_id: "gitea-ci-run",
                refresh: None,
            },
            scenario.authority(),
        )
        .await
        .outcome;
        let output = assert_gitea_signal(&outcome, DELIVERY_CI_FAILED_LABEL);
        assert_gitea_receipt(output, DeliveryMode::Merge, &scenario.repo, false);
        assert_eq!(
            scenario
                .server
                .state()
                .merge_requests
                .load(Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn gitea_preflight_closed_refuses_delivery() {
        let scenario = AdapterScenario::new(
            Scenario::PreflightClosed,
            "gitea-closed",
            WrapperMode::Normal,
        );
        let execution = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 2,
                mode: DeliveryMode::Merge,
                run_id: "gitea-closed",
                refresh: None,
            },
            scenario.authority(),
        )
        .await;
        assert_eq!(
            execution.outcome,
            WorkerOutcome::declared_failure(WorkerErrorCode::Refusal)
        );
        assert!(
            execution
                .output
                .iter()
                .any(|output| output.text.contains("was closed")),
            "refusal must explain the closed review: {:?}",
            execution.output
        );
    }

    #[tokio::test]
    async fn gitea_preflight_merged_completes_existing_delivery() {
        let scenario = AdapterScenario::new(
            Scenario::PreflightMerged,
            "gitea-merged",
            WrapperMode::Normal,
        );
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 2,
                mode: DeliveryMode::Merge,
                run_id: "gitea-merged",
                refresh: None,
            },
            scenario.authority(),
        )
        .await
        .outcome;
        let output = assert_gitea_signal(&outcome, DELIVERY_MERGED_LABEL);
        assert_eq!(output.pointer("/mergeRevision"), Some(&json!(MERGE_SHA)));
    }

    #[tokio::test]
    async fn gitea_deferred_merge_eventually_confirms() {
        let scenario =
            AdapterScenario::new(Scenario::DeferredMerge, "gitea-defer", WrapperMode::Normal);
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 7,
                mode: DeliveryMode::Merge,
                run_id: "gitea-defer",
                refresh: None,
            },
            scenario.authority(),
        )
        .await
        .outcome;
        assert_gitea_signal(&outcome, DELIVERY_MERGED_LABEL);
        assert_eq!(
            scenario
                .server
                .state()
                .merge_requests
                .load(Ordering::SeqCst),
            5
        );
    }

    #[tokio::test]
    async fn gitea_never_confirmed_merge_times_out() {
        let scenario = AdapterScenario::new(
            Scenario::NeverConfirmsMerge,
            "gitea-never-run",
            WrapperMode::Normal,
        );
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 2,
                mode: DeliveryMode::Merge,
                run_id: "gitea-never-run",
                refresh: None,
            },
            scenario.authority(),
        )
        .await
        .outcome;
        assert_eq!(
            outcome,
            WorkerOutcome::declared_failure(WorkerErrorCode::Timeout)
        );
        assert_eq!(
            scenario
                .server
                .state()
                .merge_requests
                .load(Ordering::SeqCst),
            2
        );
    }

    #[tokio::test]
    async fn gitea_registration_race_is_reobserved() {
        let scenario =
            AdapterScenario::new(Scenario::RegistrationRace, "gitea-reg", WrapperMode::Normal);
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 4,
                mode: DeliveryMode::Merge,
                run_id: "gitea-reg",
                refresh: None,
            },
            scenario.authority(),
        )
        .await
        .outcome;
        assert_gitea_signal(&outcome, DELIVERY_MERGED_LABEL);
        assert_eq!(
            scenario
                .server
                .state()
                .merge_requests
                .load(Ordering::SeqCst),
            2
        );
    }

    #[tokio::test]
    async fn gitea_multiple_registration_waves_each_allow_a_fresh_merge_attempt() {
        let scenario = AdapterScenario::new(
            Scenario::MultipleRegistrationWaves,
            "gitea-waves-run",
            WrapperMode::Normal,
        );
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 7,
                mode: DeliveryMode::Merge,
                run_id: "gitea-waves-run",
                refresh: None,
            },
            scenario.authority(),
        )
        .await
        .outcome;
        assert_gitea_signal(&outcome, DELIVERY_MERGED_LABEL);
        assert_eq!(
            scenario
                .server
                .state()
                .merge_requests
                .load(Ordering::SeqCst),
            3
        );
    }

    #[tokio::test]
    async fn gitea_review_sync_race_retries_within_the_operation() {
        let scenario = AdapterScenario::new(
            Scenario::ReviewSyncRace,
            "gitea-sync-run",
            WrapperMode::Normal,
        );
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 3,
                mode: DeliveryMode::Merge,
                run_id: "gitea-sync-run",
                refresh: None,
            },
            scenario.authority(),
        )
        .await
        .outcome;
        assert_gitea_signal(&outcome, DELIVERY_MERGED_LABEL);
        assert!(
            scenario
                .server
                .state()
                .review_creates
                .load(Ordering::SeqCst)
                >= 2
        );
    }

    struct RefreshedGiteaEnvironment {
        binding: NodeRuntimeBinding,
    }

    #[async_trait]
    impl RuntimeEnvironmentRefresh for RefreshedGiteaEnvironment {
        async fn refresh(
            &self,
        ) -> Result<ResolvedEnvironment, crate::native_v2_runner::EnvironmentRefreshError> {
            ResolvedEnvironment::exact(
                &self.binding,
                BTreeMap::from([(
                    EnvironmentVariableName::new(GITEA_TOKEN).assert_value(),
                    "refreshed-token".to_owned(),
                )]),
            )
            .map_err(|_| crate::native_v2_runner::EnvironmentRefreshError::Unavailable)
        }
    }

    struct FailedGiteaRefresh {
        error: crate::native_v2_runner::EnvironmentRefreshError,
    }

    #[async_trait]
    impl RuntimeEnvironmentRefresh for FailedGiteaRefresh {
        async fn refresh(
            &self,
        ) -> Result<ResolvedEnvironment, crate::native_v2_runner::EnvironmentRefreshError> {
            Err(self.error)
        }
    }

    async fn gitea_runtime_binding(
        repo: &TestGitRepository,
        mode: DeliveryMode,
    ) -> NodeRuntimeBinding {
        gitea_admitted(repo, mode)
            .await
            .runtime
            .nodes()
            .get(&NodeName::new("deliver").assert_value())
            .assert_value()
            .clone()
    }

    #[tokio::test]
    async fn gitea_refreshes_an_expired_dynamic_credential() {
        let scenario = AdapterScenario::new(
            Scenario::CredentialExpires,
            "gitea-refresh",
            WrapperMode::Normal,
        );
        let binding = gitea_runtime_binding(&scenario.repo, DeliveryMode::Merge).await;
        let outcome = run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 3,
                mode: DeliveryMode::Merge,
                run_id: "gitea-refresh",
                refresh: Some(Arc::new(RefreshedGiteaEnvironment { binding })),
            },
            scenario.authority(),
        )
        .await
        .outcome;
        assert_gitea_signal(&outcome, DELIVERY_MERGED_LABEL);
        assert!(
            scenario
                .server
                .state()
                .credential_rejections
                .load(Ordering::SeqCst)
                >= 1
        );
    }

    #[tokio::test]
    async fn gitea_dynamic_credential_refresh_failures_preserve_typed_outcomes() {
        for (error, expected) in [
            (
                crate::native_v2_runner::EnvironmentRefreshError::Refused,
                WorkerOutcome::authentication_refusal(),
            ),
            (
                crate::native_v2_runner::EnvironmentRefreshError::InvalidResponse,
                WorkerOutcome::malformed(),
            ),
            (
                crate::native_v2_runner::EnvironmentRefreshError::Unavailable,
                WorkerOutcome::declared_failure(WorkerErrorCode::Timeout),
            ),
        ] {
            let scenario = AdapterScenario::new(
                Scenario::CredentialExpires,
                "gitea-refresh-fail",
                WrapperMode::Normal,
            );
            let outcome = run_gitea(
                GiteaRunRequest {
                    repo: &scenario.repo,
                    attempts: 2,
                    mode: DeliveryMode::Merge,
                    run_id: "gitea-refresh-fail",
                    refresh: Some(Arc::new(FailedGiteaRefresh { error })),
                },
                scenario.authority(),
            )
            .await
            .outcome;
            assert_eq!(outcome, expected, "unexpected outcome for {error:?}");
        }
    }

    // ---------------------------------------------------------------------------
    // Transport-level tests against the local bare remote through the wrapper.
    // ---------------------------------------------------------------------------

    fn gitea_push_request(
        repo: &TestGitRepository,
        run_id: &str,
        revision: &str,
    ) -> ForgePushRequest {
        ForgePushRequest {
            workspace: repo.workspace.clone(),
            target: gitea_target(repo),
            head_branch: delivery_branch(run_id),
            head_revision: revision.to_owned(),
        }
    }

    fn transport_authority(server: &TestGitea, wrapper: &Path) -> GiteaDeliveryAuthority {
        let mut config = GiteaAuthorityConfig::new(server.base_url.clone(), wrapper.to_path_buf());
        config.api_deadline = Duration::from_secs(5);
        config.push_deadline = Duration::from_secs(10);
        GiteaDeliveryAuthority::new(config)
    }

    #[tokio::test]
    async fn gitea_push_publishes_the_reviewed_sha_even_when_workspace_head_advances() {
        let repo = TestGitRepository::delivery();
        commit_all(&repo.workspace, "reviewed");
        let reviewed = git_output(&repo.workspace, &["rev-parse", "HEAD"]);
        fs::write(repo.workspace.join("later.txt"), "later\n").assert_value();
        commit_all(&repo.workspace, "later");
        let later = git_output(&repo.workspace, &["rev-parse", "HEAD"]);
        assert_ne!(reviewed, later);
        let wrapper = gitea_git_wrapper(repo.root.path(), &repo.remote, WrapperMode::Normal);
        let server = TestGitea::scenario(repo.remote.clone(), Scenario::Happy, "gitea-transport");
        let request = gitea_push_request(&repo, "gitea-transport", &reviewed);

        transport_authority(&server, &wrapper)
            .push_branch(&request, credential())
            .await
            .assert_value();

        assert_eq!(
            remote_branch_sha(&repo.remote, &request.head_branch),
            Some(reviewed)
        );
        assert_eq!(git_output(&repo.workspace, &["rev-parse", "HEAD"]), later);
    }

    #[tokio::test]
    async fn gitea_accepted_push_with_lost_response_is_confirmed_from_the_remote_ref() {
        let repo = TestGitRepository::delivery();
        commit_all(&repo.workspace, "delivered");
        let reviewed = git_output(&repo.workspace, &["rev-parse", "HEAD"]);
        let wrapper = gitea_git_wrapper(
            repo.root.path(),
            &repo.remote,
            WrapperMode::LosePushResponse,
        );
        let server = TestGitea::scenario(repo.remote.clone(), Scenario::Happy, "gitea-lost");
        let request = gitea_push_request(&repo, "gitea-lost", &reviewed);

        transport_authority(&server, &wrapper)
            .push_branch(&request, credential())
            .await
            .assert_value();

        assert_eq!(
            remote_branch_sha(&repo.remote, &request.head_branch),
            Some(reviewed)
        );
        assert!(
            fs::read_to_string(format!("{}.capture", wrapper.display()))
                .assert_value()
                .contains("arg=push")
        );
    }

    #[tokio::test]
    async fn gitea_rejected_push_credential_cannot_create_a_ref() {
        let repo = TestGitRepository::delivery();
        commit_all(&repo.workspace, "delivered");
        let reviewed = git_output(&repo.workspace, &["rev-parse", "HEAD"]);
        let wrapper = gitea_git_wrapper(
            repo.root.path(),
            &repo.remote,
            WrapperMode::RejectCredential,
        );
        let server = TestGitea::scenario(repo.remote.clone(), Scenario::Happy, "gitea-rejected");
        let request = gitea_push_request(&repo, "gitea-rejected", &reviewed);

        let failure = transport_authority(&server, &wrapper)
            .push_branch(&request, credential())
            .await
            .expect_err("a rejected credential must not publish");
        assert!(failure.retryable_operation(), "{failure}");
        assert!(
            failure.to_string().contains("Authentication failed"),
            "credential rejection must stay in the diagnostic: {failure}"
        );
        assert_eq!(remote_branch_sha(&repo.remote, &request.head_branch), None);
    }

    #[tokio::test]
    async fn gitea_head_adoption_fast_forwards_to_the_remote_descendant() {
        let repo = TestGitRepository::delivery();
        commit_all(&repo.workspace, "base");
        let base = git_output(&repo.workspace, &["rev-parse", "HEAD"]);
        let branch = delivery_branch("gitea-adopt");
        git(
            &repo.workspace,
            &["push", "origin", &format!("HEAD:refs/heads/{branch}")],
        );
        fs::write(repo.workspace.join("advance.txt"), "advance\n").assert_value();
        commit_all(&repo.workspace, "advance");
        let observed = git_output(&repo.workspace, &["rev-parse", "HEAD"]);
        git(
            &repo.workspace,
            &["push", "origin", &format!("HEAD:refs/heads/{branch}")],
        );
        git(&repo.workspace, &["reset", "--hard", &base]);
        assert_eq!(git_output(&repo.workspace, &["rev-parse", "HEAD"]), base);

        let wrapper = gitea_git_wrapper(repo.root.path(), &repo.remote, WrapperMode::Normal);
        let mut config = GiteaAuthorityConfig::new("http://127.0.0.1:1", wrapper);
        config.api_deadline = Duration::from_secs(5);
        config.push_deadline = Duration::from_secs(10);
        let authority = GiteaDeliveryAuthority::new(config);
        let published = ForgeReviewReceipt {
            review_id: "7".to_owned(),
            repository: "acme/project".to_owned(),
            target_branch: "main".to_owned(),
            head_branch: branch.clone(),
            head_revision: base.clone(),
        };
        let observed_receipt = ForgeReviewReceipt {
            head_revision: observed.clone(),
            ..published.clone()
        };

        let outcome = authority
            .reconcile_delivery_head(
                ForgeHeadReconciliation {
                    workspace: &repo.workspace,
                    published: &published,
                    observed: &observed_receipt,
                    commit_message: "adopt the remote delivery head",
                    authorized_update: true,
                    adopting_existing: true,
                },
                credential(),
            )
            .await
            .assert_value();
        assert_eq!(outcome, ForgeReconciliationOutcome::Adopted);
        assert_eq!(
            git_output(&repo.workspace, &["rev-parse", "HEAD"]),
            observed
        );
    }

    // ---------------------------------------------------------------------------
    // Acceptance tests (Unix-only): exact REST surface and clean git environment.
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn gitea_requests_use_the_expected_rest_surface() {
        let scenario = AdapterScenario::new(Scenario::Happy, "gitea-accept", WrapperMode::Normal);
        run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 3,
                mode: DeliveryMode::Merge,
                run_id: "gitea-accept",
                refresh: None,
            },
            scenario.authority(),
        )
        .await;

        let requests = scenario.server.requests();
        assert!(!requests.is_empty());
        for (method, target) in &requests {
            assert!(
                target.starts_with("/api/v1/repos/acme/project/"),
                "unexpected target: {method} {target}"
            );
            assert!(!target.contains("test-token"));
        }
        let branch = delivery_branch("gitea-accept");
        assert!(
            requests.iter().any(|(method, target)| method == "GET"
                && target == &format!("/api/v1/repos/acme/project/pulls/main/{branch}")),
            "missing find_review request: {requests:?}"
        );
        assert!(
            requests.iter().any(|(method, target)| method == "GET"
                && target == &format!("/api/v1/repos/acme/project/branches/{branch}")),
            "missing branch request: {requests:?}"
        );
        let creates = requests
            .iter()
            .filter(|(method, target)| method == "POST" && target.ends_with("/pulls"))
            .count();
        assert_eq!(creates, 1, "{requests:?}");
        let merges = requests
            .iter()
            .filter(|(method, target)| method == "POST" && target.ends_with("/merge"))
            .count();
        assert_eq!(merges, 1, "{requests:?}");
    }

    #[tokio::test]
    async fn gitea_git_environment_is_clean_and_scoped() {
        let scenario = AdapterScenario::new(Scenario::Happy, "gitea-env", WrapperMode::Normal);
        run_gitea(
            GiteaRunRequest {
                repo: &scenario.repo,
                attempts: 3,
                mode: DeliveryMode::Merge,
                run_id: "gitea-env",
                refresh: None,
            },
            scenario.authority(),
        )
        .await;

        let capture = scenario.capture();
        assert!(capture.contains("token=test-token"), "{capture}");
        assert!(capture.contains("home=unset"), "{capture}");
        assert!(
            capture.contains("config_value_1=AUTHORIZATION: token test-token"),
            "{capture}"
        );
    }
}

// ---------------------------------------------------------------------------
// REST failure boundary tests (cross-platform; no executable wrapper needed).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gitea_api_failure_detail_is_bounded_and_control_safe() {
    let secret = format!("secret-{}", "x".repeat(20_000));
    let body = json!({ "message": secret }).to_string();
    let server = TestGitea::start(vec![(500, body)]);
    let repo = TestGitRepository::delivery();
    let target = gitea_target(&repo);
    let authority = authority(&server.base_url);

    let failure = authority
        .observe_delivery(delivery_read(&target, "zeroshot/run/42"), credential())
        .await
        .expect_err("a server failure must surface");
    let message = failure.to_string();
    assert!(failure.retryable_operation(), "{failure}");
    assert!(message.contains("HTTP 500"), "{message}");
    assert!(
        message.len() < 16 * 1024,
        "diagnostic must stay bounded: {} bytes",
        message.len()
    );
}

#[tokio::test]
async fn gitea_malformed_authority_response_is_rejected() {
    let server = TestGitea::start(vec![(200, "not json".to_owned())]);
    let repo = TestGitRepository::delivery();
    let target = gitea_target(&repo);
    let authority = authority(&server.base_url);

    let failure = authority
        .observe_delivery(delivery_read(&target, "zeroshot/run/42"), credential())
        .await
        .expect_err("malformed JSON must be rejected");
    assert!(failure.to_string().contains("malformed JSON"), "{failure}");
}
