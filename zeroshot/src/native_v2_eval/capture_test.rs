use std::path::{Path, PathBuf};
use std::process::Command;

use super::{CaptureError, CaptureRequest, DeliveryReceipt, capture_patch, checkout_patch};

fn git(directory: &Path, home: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(directory)
        .env("HOME", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Eval Test")
        .env("GIT_AUTHOR_EMAIL", "eval@example.invalid")
        .env("GIT_COMMITTER_NAME", "Eval Test")
        .env("GIT_COMMITTER_EMAIL", "eval@example.invalid")
        .args(arguments)
        .output()
        .expect("Git must be available for the capture test");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Git output must be UTF-8")
        .trim()
        .to_owned()
}

fn revision(directory: &Path, home: &Path) -> String {
    git(directory, home, &["rev-parse", "HEAD"])
}

#[test]
fn receipt_parser_ignores_unrelated_output_and_accepts_forward_fields() {
    let output = serde_json::json!({
        "delivery": {
            "version": "v3",
            "mode": "merge",
            "outcome": "merged",
            "repository": "acme/project",
            "targetBranch": "main",
            "headBranch": "zeroshot/run",
            "headRevision": "c".repeat(40),
            "mergeRevision": "d".repeat(40),
            "futureField": true
        }
    });
    let receipt = DeliveryReceipt::from_terminal_output(&output).unwrap();
    assert_eq!(receipt.delivered_revision(), Some("c".repeat(40)));
    assert_eq!(receipt.merge_revision, Some("d".repeat(40)));
    assert_eq!(receipt.head_branch.as_deref(), Some("zeroshot/run"));
    assert_eq!(
        DeliveryReceipt::from_terminal_output(&serde_json::json!({
            "context": {
                "reviewScope": {
                    "repository": "acme/project",
                    "targetBranch": "main",
                    "headBranch": "feature",
                    "headRevision": "e".repeat(40)
                }
            }
        })),
        None
    );
    assert_eq!(
        DeliveryReceipt::from_terminal_output(&serde_json::json!({"value": 1})),
        None
    );
}

#[tokio::test]
async fn delivered_branch_is_captured_as_a_patch_and_reconstructed_for_the_judge() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();

    let remote = root.path().join("remote.git");
    git(root.path(), &home, &["init", "--bare", "remote.git"]);
    let work = root.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &home, &["init", "-b", "main"]);
    std::fs::write(work.join("file.txt"), "base\n").unwrap();
    git(&work, &home, &["add", "file.txt"]);
    git(&work, &home, &["commit", "-m", "base"]);
    let base_revision = revision(&work, &home);
    std::fs::write(work.join("file.txt"), "delivered\n").unwrap();
    std::fs::write(work.join("new.txt"), "new file\n").unwrap();
    let large = "0123456789abcdef".repeat(12_000);
    std::fs::write(work.join("large.txt"), &large).unwrap();
    git(&work, &home, &["add", "--all"]);
    git(&work, &home, &["commit", "-m", "delivered"]);
    let delivered_revision = revision(&work, &home);
    git(
        &work,
        &home,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(&work, &home, &["push", "origin", "main"]);

    let sample = root.path().join("sample");
    let request = CaptureRequest::new(
        &sample,
        remote.to_str().unwrap(),
        &base_revision,
        DeliveryReceipt {
            repository: "acme/project".to_owned(),
            mode: Some("push".to_owned()),
            outcome: Some("pushed".to_owned()),
            head_branch: Some("main".to_owned()),
            head_revision: Some(delivered_revision.clone()),
            ..DeliveryReceipt::default()
        },
    )
    .with_git_program("git");
    let captured = capture_patch(&request).await.unwrap();
    assert_eq!(captured.delivered_revision, delivered_revision);
    assert_eq!(captured.base_revision, base_revision);
    assert_eq!(captured.patch, std::path::PathBuf::from("patch.diff"));
    assert!(sample.join(&captured.patch).is_file());
    assert_eq!(
        captured.patch_bytes,
        std::fs::metadata(sample.join("patch.diff")).unwrap().len()
    );
    let patch = std::fs::read_to_string(sample.join(&captured.patch)).unwrap();
    assert!(patch.contains("-base"));
    assert!(patch.contains("+delivered"));
    assert!(patch.contains("new.txt"));
    assert!(captured.patch_bytes > 16 * 1_024);
    assert_eq!(u64::try_from(patch.len()).unwrap(), captured.patch_bytes);

    let judge = root.path().join("judge");
    let checkout = checkout_patch(&request, &judge).await.unwrap();
    assert_eq!(checkout.revision, delivered_revision);
    assert_eq!(checkout.base_revision, base_revision);
    assert_eq!(checkout.directory, std::path::PathBuf::from("judge"));
    assert_eq!(checkout.patch, std::path::PathBuf::from("patch.diff"));
    assert_eq!(
        std::fs::read_to_string(judge.join("file.txt")).unwrap(),
        "delivered\n"
    );
    assert_eq!(
        std::fs::read_to_string(judge.join("new.txt")).unwrap(),
        "new file\n"
    );
    assert_eq!(
        std::fs::read_to_string(judge.join("large.txt")).unwrap(),
        large
    );
}

#[tokio::test]
async fn capture_rejects_missing_revision_and_nonempty_destination() {
    let root = tempfile::tempdir().unwrap();
    let request = CaptureRequest::new(
        root.path().join("sample"),
        "https://example.invalid/acme/project.git",
        "a".repeat(40),
        DeliveryReceipt::default(),
    );
    let error = capture_patch(&request).await.unwrap_err();
    assert!(matches!(error, CaptureError::MissingRevision));

    let destination = root.path().join("judge");
    std::fs::create_dir_all(&destination).unwrap();
    std::fs::write(destination.join("existing"), "data").unwrap();
    let receipt = DeliveryReceipt {
        head_revision: Some("b".repeat(40)),
        ..DeliveryReceipt::default()
    };
    let request = CaptureRequest::new(
        root.path().join("sample"),
        "https://example.invalid/acme/project.git",
        "a".repeat(40),
        receipt,
    );
    let error = checkout_patch(&request, &destination).await.unwrap_err();
    assert!(matches!(error, CaptureError::DestinationNotEmpty(_)));
}

#[tokio::test]
async fn capture_accepts_a_revision_only_receipt() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let remote = root.path().join("remote.git");
    git(root.path(), &home, &["init", "--bare", "remote.git"]);
    let work = root.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &home, &["init", "-b", "main"]);
    std::fs::write(work.join("file.txt"), "base\n").unwrap();
    git(&work, &home, &["add", "file.txt"]);
    git(&work, &home, &["commit", "-m", "base"]);
    let base_revision = revision(&work, &home);
    std::fs::write(work.join("file.txt"), "delivered\n").unwrap();
    git(&work, &home, &["add", "--all"]);
    git(&work, &home, &["commit", "-m", "delivered"]);
    let delivered_revision = revision(&work, &home);
    git(
        &work,
        &home,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(&work, &home, &["push", "origin", "main"]);

    let receipt = DeliveryReceipt {
        head_revision: Some(delivered_revision.clone()),
        ..DeliveryReceipt::default()
    };
    let request = CaptureRequest::new(
        root.path().join("sample"),
        remote.to_str().unwrap(),
        &base_revision,
        receipt,
    )
    .with_git_program("git");
    let captured = capture_patch(&request).await.unwrap();
    assert_eq!(captured.delivered_revision, delivered_revision);
    assert_eq!(captured.delivered_branch, None);
}

#[tokio::test]
async fn run_sample_writes_sample_json_patch_and_judge_checkout_together() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let remote = root.path().join("remote.git");
    git(root.path(), &home, &["init", "--bare", "remote.git"]);
    let work = root.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &home, &["init", "-b", "main"]);
    std::fs::write(work.join("file.txt"), "base\n").unwrap();
    git(&work, &home, &["add", "file.txt"]);
    git(&work, &home, &["commit", "-m", "base"]);
    let base_revision = revision(&work, &home);
    std::fs::write(work.join("file.txt"), "delivered\n").unwrap();
    git(&work, &home, &["add", "--all"]);
    git(&work, &home, &["commit", "-m", "delivered"]);
    let delivered_revision = revision(&work, &home);
    git(
        &work,
        &home,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(&work, &home, &["push", "origin", "main"]);

    let sample = root.path().join("sample");
    std::fs::create_dir_all(sample.join("judge")).unwrap();
    std::fs::write(sample.join("judge").join("stale"), "partial checkout").unwrap();
    let receipt = DeliveryReceipt {
        repository: "acme/project".to_owned(),
        mode: Some("push".to_owned()),
        outcome: Some("pushed".to_owned()),
        head_branch: Some("main".to_owned()),
        head_revision: Some(delivered_revision.clone()),
        ..DeliveryReceipt::default()
    };
    let capture = CaptureRequest::new(
        &sample,
        remote.to_str().unwrap(),
        &base_revision,
        receipt.clone(),
    )
    .with_git_program("git");
    let runner = crate::native_v2_eval::FakeRunner::new();
    let artifact = crate::native_v2_eval::run_sample(
        &runner,
        crate::native_v2_eval::FakeSampleRequest {
            run_id: Some(openengine_cluster_protocol::RunId::new("run-capture")),
            delivery: Some(receipt),
            ..crate::native_v2_eval::FakeSampleRequest::succeeded("sample-capture")
        },
        &sample,
        |_, _| Some(capture),
    )
    .await
    .unwrap();
    assert!(
        artifact.capture_error.is_none(),
        "{:?}",
        artifact.capture_error
    );
    let captured = artifact.captured.unwrap();
    assert_eq!(captured.patch, PathBuf::from("patch.diff"));
    assert!(sample.join("patch.diff").is_file());
    let judge = captured.judge.unwrap();
    assert_eq!(judge.revision, delivered_revision);
    assert_eq!(judge.base_revision, base_revision);
    assert_eq!(judge.directory, PathBuf::from("judge"));
    assert_eq!(
        std::fs::read_to_string(sample.join("judge").join("file.txt")).unwrap(),
        "delivered\n"
    );
    assert!(sample.join("sample.json").is_file());
}

#[tokio::test]
async fn sha256_delivered_revisions_use_a_matching_object_format() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let remote = root.path().join("remote.git");
    git(
        root.path(),
        &home,
        &["init", "--bare", "--object-format=sha256", "remote.git"],
    );
    let work = root.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(
        &work,
        &home,
        &["init", "-b", "main", "--object-format=sha256"],
    );
    std::fs::write(work.join("file.txt"), "base\n").unwrap();
    git(&work, &home, &["add", "file.txt"]);
    git(&work, &home, &["commit", "-m", "base"]);
    let base_revision = revision(&work, &home);
    std::fs::write(work.join("file.txt"), "delivered\n").unwrap();
    git(&work, &home, &["add", "--all"]);
    git(&work, &home, &["commit", "-m", "delivered"]);
    let delivered_revision = revision(&work, &home);
    assert_eq!(base_revision.len(), 64);
    assert_eq!(delivered_revision.len(), 64);
    git(
        &work,
        &home,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(&work, &home, &["push", "origin", "main"]);

    let sample = root.path().join("sample");
    let request = CaptureRequest::new(
        &sample,
        remote.to_str().unwrap(),
        &base_revision,
        DeliveryReceipt {
            repository: "acme/project".to_owned(),
            mode: Some("push".to_owned()),
            outcome: Some("pushed".to_owned()),
            head_branch: Some("main".to_owned()),
            head_revision: Some(delivered_revision.clone()),
            ..DeliveryReceipt::default()
        },
    )
    .with_git_program("git");
    let captured = capture_patch(&request).await.unwrap();
    assert_eq!(captured.delivered_revision, delivered_revision);
    let judge = root.path().join("judge");
    checkout_patch(&request, &judge).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(judge.join("file.txt")).unwrap(),
        "delivered\n"
    );
}

#[tokio::test]
async fn failed_checkout_leaves_no_partial_destination_for_retry() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("judge");
    let request = CaptureRequest::new(
        root.path().join("sample"),
        root.path().join("missing.git").to_str().unwrap(),
        "a".repeat(40),
        DeliveryReceipt {
            repository: "acme/project".to_owned(),
            mode: Some("push".to_owned()),
            outcome: Some("pushed".to_owned()),
            head_branch: Some("main".to_owned()),
            head_revision: Some("b".repeat(40)),
            ..DeliveryReceipt::default()
        },
    )
    .with_git_program("git");
    assert!(checkout_patch(&request, &destination).await.is_err());
    assert!(!destination.exists());
}

#[tokio::test]
async fn missing_delivered_branch_falls_back_to_the_exact_revision() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let remote = root.path().join("remote.git");
    git(root.path(), &home, &["init", "--bare", "remote.git"]);
    let work = root.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &home, &["init", "-b", "main"]);
    std::fs::write(work.join("file.txt"), "base\n").unwrap();
    git(&work, &home, &["add", "file.txt"]);
    git(&work, &home, &["commit", "-m", "base"]);
    let base_revision = revision(&work, &home);
    std::fs::write(work.join("file.txt"), "delivered\n").unwrap();
    git(&work, &home, &["add", "--all"]);
    git(&work, &home, &["commit", "-m", "delivered"]);
    let delivered_revision = revision(&work, &home);
    git(
        &work,
        &home,
        &["remote", "add", "origin", remote.to_str().unwrap()],
    );
    git(
        &work,
        &home,
        &[
            "push",
            "origin",
            "HEAD:refs/heads/main",
            "HEAD:refs/heads/feature",
        ],
    );
    git(&work, &home, &["push", "origin", "--delete", "feature"]);

    let request = CaptureRequest::new(
        root.path().join("sample"),
        remote.to_str().unwrap(),
        &base_revision,
        DeliveryReceipt {
            repository: "acme/project".to_owned(),
            mode: Some("push".to_owned()),
            outcome: Some("pushed".to_owned()),
            target_branch: Some("main".to_owned()),
            head_branch: Some("feature".to_owned()),
            head_revision: Some(delivered_revision.clone()),
            ..DeliveryReceipt::default()
        },
    )
    .with_git_program("git");
    let captured = capture_patch(&request).await.unwrap();
    assert_eq!(captured.delivered_revision, delivered_revision);
}
