//! The process driver: every target operation is a `zeroshot` child process.

use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::time::Duration;

use openengine_cluster_protocol::TARGET_DISCOVERY_PATH;
use serde_json::json;

use super::super::driver::{HubDriver, ProcessDriver, route};
#[cfg(unix)]
use super::super::driver::SubmitRequest;
#[cfg(unix)]
use super::super::error::HubError;
use crate::native_v2_target::{FileTargetRegistry, TargetAccess, TargetRecord, TargetRegistry};

/// A stand-in `zeroshot` that answers per command and member, recording run invocations.
#[cfg(unix)]
fn fake_zeroshot(folder: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = folder.join("zeroshot");
    let script = r#"#!/bin/sh
command="$1"; shift
member=local
for argument in "$@"; do
  if [ "$previous" = "--target" ]; then member="$argument"; fi
  previous="$argument"
done
case "$member" in
  down) echo 'progress' >&2; echo '{"schema":"zeroshot.error/v1","message":"target down"}' >&2; exit 1 ;;
  plain) echo 'plain failure' >&2; exit 2 ;;
  quiet) echo 'not json'; exit 0 ;;
  slow) sleep 5; exit 0 ;;
esac
case "$command" in
  list) echo 'noise'; echo '{"runs":[{"status":{"phase":"running"}}]}' ;;
  run) echo "$(pwd) $*" >> "$(dirname "$0")/runs.log"; echo '{"event":"accepted"}'; echo '{"runId":"run-7"}' ;;
  status) echo '{"status":{"phase":"finished"}}' ;;
esac
"#;
    std::fs::write(&path, script).expect("script");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    path
}

fn registry(folder: &Path) -> PathBuf {
    folder.join("targets.json")
}

fn register(folder: &Path, name: &str, origin: &str) {
    FileTargetRegistry::new(registry(folder))
        .insert(TargetRecord {
            id: format!(
                "00000000-0000-4000-8000-{:012x}",
                name.len() * 7919 + name.as_bytes()[0] as usize
            ),
            name: name.to_owned(),
            origin: origin.to_owned(),
            access: TargetAccess::Direct,
        })
        .expect("register");
}

#[cfg(unix)]
#[tokio::test]
async fn targets_are_driven_through_zeroshot_child_processes() {
    let folder = tempfile::tempdir().expect("tempdir");
    let driver = ProcessDriver::new(
        fake_zeroshot(folder.path()),
        registry(folder.path()),
        Duration::from_secs(10),
    )
    .expect("driver");
    let runs = driver.list_runs("local").await.expect("list");
    assert_eq!(runs, vec![json!({"status": {"phase": "running"}})]);
    let status = driver.status("box", "run-7").await.expect("status");
    assert_eq!(status["status"]["phase"], json!("finished"));

    let work = tempfile::tempdir().expect("work");
    let mut job = super::job("hj_driver", 0);
    job.cwd = work.path().to_path_buf();
    job.pinned_revision = Some("abc123".to_owned());
    for member in ["local", "box"] {
        let run_id = driver
            .submit(SubmitRequest {
                member,
                job: &job,
                submission_key: format!("key-{member}"),
            })
            .await
            .expect("submit");
        assert_eq!(run_id, "run-7");
    }
    let log = std::fs::read_to_string(folder.path().join("runs.log")).expect("runs log");
    let lines = log.lines().collect::<Vec<_>>();
    let work = std::fs::canonicalize(work.path()).expect("canonical");
    assert!(lines[0].starts_with(&work.display().to_string()), "{log}");
    assert!(
        lines[0].ends_with("--title hj_driver --submission-key key-local --detach"),
        "{log}"
    );
    assert!(
        lines[1].ends_with(
            "--title hj_driver --target box --revision abc123 --submission-key key-box --detach"
        ),
        "{log}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn child_failures_carry_the_cli_diagnostic_or_its_stderr() {
    let folder = tempfile::tempdir().expect("tempdir");
    let executable = fake_zeroshot(folder.path());
    let driver = ProcessDriver::new(
        executable.clone(),
        registry(folder.path()),
        Duration::from_secs(10),
    )
    .expect("driver");
    let down = driver.list_runs("down").await.expect_err("down");
    let HubError::Target {
        message,
        diagnostic,
    } = &down
    else {
        panic!("{down:?}");
    };
    assert_eq!(message, "zeroshot list: target down");
    assert_eq!(
        diagnostic.as_ref().expect("diagnostic")["message"],
        json!("target down")
    );
    let plain = driver.status("plain", "run-1").await.expect_err("plain");
    assert!(
        plain.to_string().contains("zeroshot status: plain failure"),
        "{plain}"
    );
    for error in [
        driver.list_runs("quiet").await.expect_err("list"),
        driver.status("quiet", "run-1").await.expect_err("status"),
        driver
            .submit(SubmitRequest {
                member: "quiet",
                job: &super::job("hj_quiet", 0),
                submission_key: "k".to_owned(),
            })
            .await
            .expect_err("run"),
    ] {
        assert!(error.to_string().contains("returned no"), "{error}");
    }

    let impatient = ProcessDriver::new(
        executable,
        registry(folder.path()),
        Duration::from_millis(200),
    )
    .expect("driver");
    let slow = impatient.list_runs("slow").await.expect_err("slow");
    assert!(slow.to_string().contains("timed out"), "{slow}");
    let missing = ProcessDriver::new(
        folder.path().join("absent"),
        registry(folder.path()),
        Duration::from_secs(1),
    )
    .expect("driver");
    let absent = missing.list_runs("local").await.expect_err("absent");
    assert!(absent.to_string().contains("could not start"), "{absent}");
}

#[tokio::test]
async fn capabilities_come_from_the_targets_discovery_document() {
    use axum::routing::get;
    let folder = tempfile::tempdir().expect("tempdir");
    let serve = |app: axum::Router| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move { axum::serve(listener, app).await });
        url
    };
    let discovery = serve(axum::Router::new().route(
        TARGET_DISCOVERY_PATH,
        get(|| async {
            axum::Json(json!({
                "kind": "zeroshot.target",
                "extensions": {"a": {"kind": "lanes"}, "b": {"kind": "lanes"}, "c": {}}
            }))
        }),
    ))
    .await;
    let failing = serve(axum::Router::new().route(
        TARGET_DISCOVERY_PATH,
        get(|| async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "later") }),
    ))
    .await;
    let garbage =
        serve(axum::Router::new().route(TARGET_DISCOVERY_PATH, get(|| async { "<html>" }))).await;
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        format!("http://{}", listener.local_addr().expect("addr"))
    };
    register(folder.path(), "good", &discovery);
    register(folder.path(), "failing", &failing);
    register(folder.path(), "garbage", &garbage);
    register(folder.path(), "closed", &closed);
    let driver = ProcessDriver::production(registry(folder.path())).expect("driver");
    assert_eq!(
        driver.capabilities("good").await.expect("good"),
        ["lanes", "zeroshot.target"]
    );
    for (member, expected) in [
        ("failing", "returned HTTP 503"),
        ("garbage", "is not JSON"),
        ("closed", "failed"),
        ("unregistered", "unregistered"),
    ] {
        let error = driver.capabilities(member).await.expect_err(member);
        assert!(error.to_string().contains(expected), "{member}: {error}");
    }
}

#[test]
fn named_members_are_routed_with_target() {
    assert!(route("local").is_empty());
    assert_eq!(route("box"), ["--target", "box"]);
}
