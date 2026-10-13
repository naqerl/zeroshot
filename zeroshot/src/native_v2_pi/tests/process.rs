//! Lifecycle tests: the real Pi adapter driving a scripted `pi` executable.
//!
//! Every other test in this module stops at argv and transcript construction, so these are the only
//! ones that prepare the private filesystem, open a provider process, stream the prompt into it,
//! and settle a turn from its output. A shell script stands in for the CLI, so no network, login,
//! or installed `pi` is needed.
//!
//! A child module of `native_v2_pi/tests.rs`, so the shared fixtures stay in one place and the
//! coverage totals keep excluding this file (`AGENTS.md`).

use std::collections::BTreeMap;
use std::time::Duration;

use openengine_cluster_protocol::WorkerOutcome;
use openengine_cluster_testkit::assertions::AssertValue;

use super::*;
use crate::native_v2_runner::test_support::control;
use crate::native_v2_runner::{LiveOutput, NodeDriver, NodeRunnerError};

/// A `pi --mode json` turn: the prompt is consumed and each line is framed as the CLI frames it.
fn script(lines: &[&str]) -> String {
    script_exiting(0, lines)
}

fn script_exiting(code: u8, lines: &[&str]) -> String {
    let mut body = String::from("#!/bin/sh\ncat > /dev/null\n");
    for line in lines {
        body.push_str(&format!("printf '%s\\n' '{line}'\n"));
    }
    body.push_str(&format!("exit {code}\n"));
    body
}

const NARRATION: &str = r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"working"}}"#;
const FAILED_TURN: &str = r#"{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"provider rejected the configured model","content":[]}}"#;
const COMPLETED_TURN: &str = r#"{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"{\"response\":null}"}]}}"#;

/// The anthropic lane reuses a local login, so this needs no declared credential and keeps the
/// neutral schema dialect for the completion case.
fn scripted_invocation() -> DriverInvocation {
    invocation(
        agent_binding("claude-sonnet-4-5", None, SessionScope::Execution, &[]),
        NodeRole::Worker,
        resolved(&[], &[]),
    )
}

/// A local adapter whose Pi executable is the supplied script, over real temporary directories.
fn scripted_adapter(directory: &TestDirectory, body: &str) -> PiAdapter {
    let executable = directory.write_executable("pi", body);
    adapter_for_executable(directory, executable.to_str().assert_value())
}

fn adapter_for_executable(directory: &TestDirectory, executable: &str) -> PiAdapter {
    let workspace = directory.child("workspace");
    let runtime_home = directory.child("runtime");
    std::fs::create_dir_all(&workspace).assert_value();
    std::fs::create_dir_all(&runtime_home).assert_value();
    let mut configuration =
        adapter_configuration(PiProvider::Anthropic, executable, BTreeMap::new());
    configuration.workspace = workspace;
    configuration.runtime_home = runtime_home;
    PiAdapter::new_local(configuration).assert_value()
}

/// Runs one turn, draining the durable output as it arrives so the bounded channel cannot block.
async fn run_turn(
    adapter: &PiAdapter,
    cancelled_first: bool,
) -> (Result<WorkerOutcome, NodeRunnerError>, Vec<LiveOutput>) {
    let (cancel, control, mut durable) = control();
    if cancelled_first {
        cancel.send(true).assert_value();
    }
    let drain = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Ok(output) = durable.recv_output().await {
            seen.push(output);
        }
        seen
    });
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        adapter.run(scripted_invocation(), control),
    )
    .await
    .assert_value_with("a scripted Pi turn must finish");
    drop(cancel);
    let seen = drain.await.assert_value();
    (outcome, seen)
}

#[tokio::test]
async fn a_scripted_turn_reports_the_failure_the_event_stream_carries() {
    // Pi exits zero even when its retries are exhausted, so only the stream carries the reason.
    let directory = TestDirectory::new("pi-scripted-failure");
    let adapter = scripted_adapter(&directory, &script(&[NARRATION, FAILED_TURN]));
    let (outcome, seen) = run_turn(&adapter, false).await;
    let Err(NodeRunnerError::DriverDetail(detail)) = outcome else {
        panic!("expected a provider failure detail, got {outcome:?}");
    };
    assert!(
        detail.contains("provider rejected the configured model"),
        "{detail}"
    );
    assert!(
        seen.iter().any(|output| output.text.contains("working")),
        "the streamed narration must reach the durable log: {seen:?}"
    );
}

#[tokio::test]
async fn a_scripted_turn_completes_from_its_final_assistant_message() {
    let directory = TestDirectory::new("pi-scripted-complete");
    let adapter = scripted_adapter(&directory, &script(&[NARRATION, COMPLETED_TURN]));
    let (outcome, seen) = run_turn(&adapter, false).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    assert!(
        seen.iter().any(|output| output.text.contains("working")),
        "{seen:?}"
    );
}

#[tokio::test]
async fn a_cancelled_turn_never_starts_the_provider() {
    let directory = TestDirectory::new("pi-scripted-cancelled");
    let adapter = scripted_adapter(&directory, &script(&[COMPLETED_TURN]));
    let (outcome, _seen) = run_turn(&adapter, true).await;
    assert!(
        matches!(outcome, Err(NodeRunnerError::Cancelled)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn an_unlaunchable_provider_reports_a_start_failure() {
    let directory = TestDirectory::new("pi-scripted-missing");
    let adapter = adapter_for_executable(&directory, "/nonexistent/pi-binary");
    let (outcome, _seen) = run_turn(&adapter, false).await;
    let Err(NodeRunnerError::DriverDetail(detail)) = outcome else {
        panic!("expected a launch failure detail, got {outcome:?}");
    };
    assert!(!detail.is_empty(), "{detail}");
}

#[tokio::test]
async fn a_provider_that_exits_nonzero_after_an_answer_still_fails_the_turn() {
    // A process that failed after producing an answer must not be treated as usable.
    let directory = TestDirectory::new("pi-scripted-late-exit");
    let adapter = scripted_adapter(&directory, &script_exiting(9, &[COMPLETED_TURN]));
    let (outcome, _seen) = run_turn(&adapter, false).await;
    let Err(NodeRunnerError::DriverDetail(detail)) = outcome else {
        panic!("expected a process failure, got {outcome:?}");
    };
    assert!(!detail.is_empty(), "{detail}");
}

#[tokio::test]
async fn a_provider_that_exits_nonzero_without_a_terminal_reports_the_exit_status() {
    // No terminal message and no provider error: the exit status is all the turn has, and reporting
    // it is what keeps the node from silently settling on nothing.
    let directory = TestDirectory::new("pi-scripted-no-terminal");
    let adapter = scripted_adapter(&directory, &script_exiting(4, &[]));
    let (outcome, _seen) = run_turn(&adapter, false).await;
    let Err(NodeRunnerError::DriverDetail(detail)) = outcome else {
        panic!("expected a provider failure, got {outcome:?}");
    };
    assert!(detail.contains("exited with status 4"), "{detail}");
}

#[tokio::test]
async fn a_turn_cancelled_while_the_provider_runs_settles_as_cancelled() {
    let directory = TestDirectory::new("pi-scripted-mid-turn-cancel");
    let adapter = scripted_adapter(
        &directory,
        &script(&["{\"type\":\"message_update\"}"]).replace("exit 0", "sleep 15\nexit 0"),
    );
    let (cancel, control, mut durable) = control();
    let drain = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Ok(output) = durable.recv_output().await {
            seen.push(output);
        }
        seen
    });
    let cancel_after_start = cancel.clone();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = cancel_after_start.send(true);
    });
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        adapter.run(scripted_invocation(), control),
    )
    .await
    .assert_value_with("a cancelled Pi turn must still finish");
    canceller.await.assert_value();
    drop(cancel);
    let _seen = drain.await.assert_value();
    assert!(
        matches!(outcome, Err(NodeRunnerError::Cancelled)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn records_that_are_not_events_do_not_hide_the_terminal_message() {
    let directory = TestDirectory::new("pi-scripted-malformed");
    let adapter = scripted_adapter(
        &directory,
        &script(&["not json at all", NARRATION, FAILED_TURN]),
    );
    let (outcome, _seen) = run_turn(&adapter, false).await;
    let Err(NodeRunnerError::DriverDetail(detail)) = outcome else {
        panic!("expected the streamed failure, got {outcome:?}");
    };
    assert!(
        detail.contains("provider rejected the configured model"),
        "{detail}"
    );
}
