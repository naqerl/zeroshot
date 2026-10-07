use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::json;

use super::*;

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

struct TestGitea {
    base_url: String,
    recorded: Arc<Mutex<Vec<(String, String)>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl TestGitea {
    fn start(responses: Vec<(u16, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let address = listener.local_addr().expect("address");
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
        let handle = {
            let recorded = Arc::clone(&recorded);
            let stop = Arc::clone(&stop);
            let responses = Arc::clone(&responses);
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let _ = stream.set_nonblocking(false);
                            serve(&mut stream, &responses, &recorded);
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
        }
    }

    fn requests(&self) -> Vec<(String, String)> {
        self.recorded.lock().expect("recorded").clone()
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
) {
    let (method, target, _body) = read_request(stream);
    recorded
        .lock()
        .expect("recorded")
        .push((method, target.clone()));
    let (status, body) = responses
        .lock()
        .expect("responses")
        .pop_front()
        .unwrap_or((500, "{\"message\":\"unscripted request\"}".to_owned()));
    let response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        reason(status),
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Response",
    }
}

fn read_request(stream: &mut TcpStream) -> (String, String, String) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut request_line = String::new();
    reader.read_line(&mut request_line).expect("request line");
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).expect("header") == 0 || line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).expect("body");
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    (method, target, String::from_utf8_lossy(&body).into_owned())
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
