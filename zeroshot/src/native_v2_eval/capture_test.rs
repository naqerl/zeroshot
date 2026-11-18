use std::path::Path;
use std::process::Command;

use super::{CaptureRequest, DeliveryReceipt, capture_patch, checkout_patch};

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
    assert_eq!(receipt.delivered_revision(), Some("d".repeat(40)));
    assert_eq!(receipt.head_branch.as_deref(), Some("zeroshot/run"));
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
    assert!(captured.patch.is_file());
    let patch = std::fs::read_to_string(&captured.patch).unwrap();
    assert!(patch.contains("-base"));
    assert!(patch.contains("+delivered"));
    assert!(patch.contains("new.txt"));

    let judge = root.path().join("judge");
    let checkout = checkout_patch(&request, &judge).await.unwrap();
    assert_eq!(checkout.revision, base_revision);
    assert_eq!(
        std::fs::read_to_string(judge.join("file.txt")).unwrap(),
        "delivered\n"
    );
    assert_eq!(
        std::fs::read_to_string(judge.join("new.txt")).unwrap(),
        "new file\n"
    );
}
