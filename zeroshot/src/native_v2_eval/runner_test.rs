use openengine_cluster_protocol::{RunId, TokenCount, TokenUsage};
use serde_json::json;

use super::{
    SAMPLE_FILE, DeliveryReceipt, FakeRunner, FakeSampleRequest, SampleArtifact, SampleOutcome,
    SampleRunner, SampleStatus, run_sample,
};

fn usage() -> TokenUsage {
    TokenUsage {
        input_tokens: TokenCount::new(11).unwrap(),
        output_tokens: TokenCount::new(7).unwrap(),
        cache_read_input_tokens: None,
        cache_creation_input_tokens: None,
        complete: true,
    }
}

#[test]
fn delivery_receipt_is_found_in_a_wrapped_terminal_output() {
    let output = json!({
        "message": "published",
        "delivery": {
            "version": "v2",
            "mode": "push",
            "outcome": "pushed",
            "repository": "acme/project",
            "targetBranch": "main",
            "headBranch": "zeroshot/run",
            "headRevision": "a".repeat(40),
        }
    });
    let receipt = DeliveryReceipt::from_terminal_output(&output).unwrap();
    assert_eq!(receipt.repository, "acme/project");
    assert_eq!(receipt.head_branch.as_deref(), Some("zeroshot/run"));
    assert_eq!(receipt.delivered_revision(), Some("a".repeat(40)));
    assert_eq!(
        DeliveryReceipt::from_terminal_output(&json!({"message": "not a delivery"})),
        None
    );
}

#[tokio::test]
async fn fake_runner_reports_timings_tokens_and_delivery() {
    let runner = FakeRunner::new();
    let receipt = DeliveryReceipt {
        repository: "acme/project".to_owned(),
        mode: Some("push".to_owned()),
        outcome: Some("pushed".to_owned()),
        head_branch: Some("zeroshot/run".to_owned()),
        head_revision: Some("b".repeat(40)),
        ..DeliveryReceipt::default()
    };
    let run = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-1")),
            queue_wait_ms: 15,
            wall_clock_ms: 120,
            token_usage: Some(usage()),
            delivery: Some(receipt.clone()),
            ..FakeSampleRequest::succeeded("sample-1")
        })
        .await
        .unwrap();
    let outcome = runner.wait(&run).await.unwrap();
    assert_eq!(outcome.status, SampleStatus::Succeeded);
    assert_eq!(outcome.run_id.as_str(), "run-1");
    assert_eq!(outcome.timings.queue_wait_ms, 15);
    assert_eq!(outcome.timings.wall_clock_ms, 120);
    assert_eq!(
        outcome.timings.started_at_ms,
        Some(run.submitted_at_ms + 15)
    );
    assert_eq!(outcome.token_usage, Some(usage()));
    assert_eq!(outcome.delivered_revision, Some("b".repeat(40)));
    assert_eq!(outcome.delivered_branch.as_deref(), Some("zeroshot/run"));
    assert_eq!(outcome.delivery, Some(receipt));
}

#[tokio::test]
async fn fake_runner_reports_failure_and_stop() {
    let runner = FakeRunner::new();
    let run = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-2")),
            ..FakeSampleRequest::failed("sample-2", "worker_failed")
        })
        .await
        .unwrap();
    let outcome = runner.wait(&run).await.unwrap();
    assert_eq!(outcome.status, SampleStatus::Failed);
    assert_eq!(outcome.failure_reason.as_deref(), Some("worker_failed"));

    let stopped = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-3")),
            ..FakeSampleRequest::succeeded("sample-3")
        })
        .await
        .unwrap();
    runner.stop(&stopped).await.unwrap();
    assert_eq!(runner.stopped().await, vec![RunId::new("run-3")]);
    assert!(runner.wait(&stopped).await.is_err());
}

#[tokio::test]
async fn run_sample_writes_the_sample_artifact() {
    let runner = FakeRunner::new();
    let directory = tempfile::tempdir().unwrap();
    let artifact = run_sample(
        &runner,
        FakeSampleRequest {
            run_id: Some(RunId::new("run-direct")),
            ..FakeSampleRequest::succeeded("sample-direct")
        },
        directory.path(),
        |_, _| None,
    )
    .await
    .unwrap();
    assert_eq!(artifact.sample_id, "sample-direct");
    assert_eq!(artifact.outcome.run_id.as_str(), "run-direct");
    assert_eq!(SampleArtifact::read(directory.path()).unwrap(), artifact);
}

#[tokio::test]
async fn sample_artifact_round_trips_a_partial_eval_sample() {
    let runner = FakeRunner::new();
    let run = runner
        .submit(FakeSampleRequest {
            run_id: Some(RunId::new("run-4")),
            token_usage: Some(usage()),
            ..FakeSampleRequest::succeeded("sample-4")
        })
        .await
        .unwrap();
    let outcome: SampleOutcome = runner.wait(&run).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let artifact = SampleArtifact::new("sample-4", outcome);
    let path = artifact.write(directory.path()).unwrap();
    assert_eq!(path.file_name().unwrap(), SAMPLE_FILE);
    let stored = SampleArtifact::read(directory.path()).unwrap();
    assert_eq!(stored, artifact);
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(value["runId"], "run-4");
    assert_eq!(value["timings"]["wallClockMs"], 1);
    assert_eq!(value["tokenUsage"]["inputTokens"], 11);
}
