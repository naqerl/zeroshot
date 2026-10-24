use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use openengine_cluster_protocol::{
    IdempotencyKey, NodeName, PositiveInteger, RunAttachParams, RunId, RunLogsParams, RunStatus,
    RunStatusParams, RunWatchEventNotification, RunWatchParams, Sha256Digest, TerminalResult,
    TokenCount, UnixTimestampMillis, WorkerOutcome,
};
use serde_json::Value;
use tokio::sync::{Barrier, Notify};

use super::*;
use crate::full_v1_reducer::{ExecutionVoidReason, StructuralOccurrence};
use crate::native_v2_contract::{
    AdmittedRun, ExecutionRef, NodeCompletion, NodeInvocation, TokenUsageDelta,
};
use crate::native_v2_runner::{
    DriverControl, DriverInvocation, LiveOutput, LiveOutputStream, NativeNodeRunner, NodeDriver,
    NodeRunRequest, NodeRunner, NodeRunnerError, NodeSession, ResolvedEnvironment, SessionFactory,
};
use crate::v2_run_ledger::fake::FakeRunLedger;
use crate::v2_run_ledger::{CreateRun, RunEvent, RunLedger, SafeLogLine};

fn admitted_run() -> AdmittedRun {
    crate::native_v2_runner::test_support::admitted()
}

async fn ledger_run(run: &str) -> (Arc<FakeRunLedger>, RunId) {
    let ledger = Arc::new(FakeRunLedger::new());
    let run_id = RunId::new(run);
    ledger
        .create_or_get(CreateRun {
            run_id: run_id.clone(),
            submission_key: IdempotencyKey::new(format!("submission-{run}")).assert_value(),
            submission_digest: Sha256Digest::new("a".repeat(64)).assert_value(),
            admitted: admitted_run(),
        })
        .await
        .assert_value();
    (ledger, run_id)
}

fn reference(run_id: &RunId, node: &str, execution: u64) -> ExecutionRef {
    ExecutionRef {
        run_id: run_id.clone(),
        node: NodeName::new(node).assert_value(),
        node_instance: crate::native_v2_contract::NodeInstanceId::new(execution).assert_value(),
        execution: ExecutionId::new(execution).assert_value(),
    }
}

fn public_reference(reference: &ExecutionRef) -> PublicExecutionRef {
    let result = opaque_execution(reference);
    assert!(
        result.is_ok(),
        "test execution reference must be projectable"
    );
    let mut values = result.ok().into_iter().collect::<Vec<_>>();
    values.swap_remove(0)
}

fn started(reference: &ExecutionRef) -> RunEvent {
    RunEvent::NodeStarted {
        reference: reference.clone(),
        occurrence: StructuralOccurrence {
            node: reference.node.clone(),
            map_indices: Vec::new(),
        },
        attempt: PositiveInteger::new(1).assert_value(),
        input: Value::Null,
        timestamp: None,
    }
}

fn completed(reference: &ExecutionRef, output: Value) -> RunEvent {
    RunEvent::NodeCompleted {
        completion: NodeCompletion {
            reference: reference.clone(),
            outcome: WorkerOutcome::Verified {
                output,
                artifacts: Vec::new(),
            },
        },
        timestamp: None,
    }
}

fn assert_watch_metadata(events: &[RunWatchEventNotification], admitted: &AdmittedRun) {
    for event in events {
        assert_eq!(&event.title, &admitted.title);
        assert_eq!(&event.source, &admitted.source);
        assert_eq!(event.size, admitted.runtime.size());
    }
}

struct LiveAttachFixture {
    ledger: Arc<FakeRunLedger>,
    run_id: RunId,
    reference: ExecutionRef,
    first_emitted: Arc<Notify>,
    release: Arc<Notify>,
    service: NativeV2Observability,
    handle: crate::native_v2_runner::NodeHandle,
    durable: crate::native_v2_runner::DurableOutput,
    registration: LiveExecutionRegistration,
}

async fn live_attach_fixture(run: &str) -> LiveAttachFixture {
    let (ledger, run_id) = ledger_run(run).await;
    let reference = reference(&run_id, "worker", 1);
    ledger
        .append(&run_id, vec![RunEvent::RunStarted, started(&reference)])
        .await
        .assert_value();
    let first_emitted = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let runner = NativeNodeRunner::new(
        &admitted_run(),
        Arc::new(ControlledDriver {
            first_emitted: first_emitted.clone(),
            release: release.clone(),
        }),
        Arc::new(FakeSessions),
    )
    .assert_value();
    let mut handle = runner
        .start(crate::native_v2_runner::test_support::request(
            reference.run_id.as_str(),
            reference.node.as_str(),
            (reference.node_instance.get(), reference.execution.get()),
        ))
        .await
        .assert_value();
    let service = NativeV2Observability::new(ledger.clone());
    let durable = handle.take_initial_output().assert_value();
    let registration = service
        .register_live_execution(&reference, handle.live_output_source().assert_value())
        .await
        .assert_value();
    LiveAttachFixture {
        ledger,
        run_id,
        reference,
        first_emitted,
        release,
        service,
        handle,
        durable,
        registration,
    }
}

async fn attach_working(fixture: &LiveAttachFixture) -> (RunAttachParams, RunAttachSubscription) {
    let params = RunAttachParams {
        run_id: fixture.run_id.clone(),
        execution: fixture.registration.public_execution().clone(),
    };
    let (_, mut attached) = fixture.service.attach(params.clone()).await.assert_value();
    assert!(matches!(
        attached.recv().await.assert_value().event,
        AgentAttachEvent::Working {}
    ));
    (params, attached)
}

async fn assert_attach_settled(attached: &mut RunAttachSubscription) {
    assert!(matches!(
        attached.recv().await.assert_value().event,
        AgentAttachEvent::Settled {}
    ));
}

#[path = "tests/attach.rs"]
mod attach;
#[path = "tests/errors.rs"]
mod errors;
#[path = "tests/logs.rs"]
mod logs;
#[path = "tests/parallel_attach.rs"]
mod parallel_attach;
#[path = "tests/replay.rs"]
mod replay;
#[path = "tests/status.rs"]
mod status;
#[path = "tests/watch.rs"]
mod watch;

use attach::{ControlledDriver, FakeSessions, ParallelVerifierDriver};
use errors::{attach_text, persist_output};
use status::cursor_fixture;

use openengine_cluster_testkit::assertions::{AssertValue};

#[tokio::test]
async fn history_definition_folds_node_lifecycle_and_counters() {
    let (ledger, run_id) = ledger_run("018f5e78-7f95-7c22-8d98-3f15af20c981").await;
    let worker = reference(&run_id, "worker", 7);
    let worker_retry = ExecutionRef {
        execution: crate::full_v1_reducer::ExecutionId::new(9).assert_value(),
        ..worker.clone()
    };
    let reviewer = reference(&run_id, "reviewer", 8);
    let timestamp = |value| UnixTimestampMillis::new(value).assert_value();
    ledger
        .append(
            &run_id,
            vec![
                RunEvent::RunStarted,
                RunEvent::NodeStarted {
                    reference: worker.clone(),
                    occurrence: StructuralOccurrence {
                        node: worker.node.clone(),
                        map_indices: Vec::new(),
                    },
                    attempt: PositiveInteger::new(1).assert_value(),
                    input: Value::Null,
                    timestamp: Some(timestamp(100)),
                },
                RunEvent::NodeStarted {
                    reference: worker_retry.clone(),
                    occurrence: StructuralOccurrence {
                        node: worker_retry.node.clone(),
                        map_indices: Vec::new(),
                    },
                    attempt: PositiveInteger::new(2).assert_value(),
                    input: Value::Null,
                    timestamp: Some(timestamp(150)),
                },
                RunEvent::NodeStarted {
                    reference: reviewer.clone(),
                    occurrence: StructuralOccurrence {
                        node: reviewer.node.clone(),
                        map_indices: Vec::new(),
                    },
                    attempt: PositiveInteger::new(1).assert_value(),
                    input: Value::Null,
                    timestamp: Some(timestamp(200)),
                },
                RunEvent::ExecutionVoided {
                    reference: reviewer,
                    reason: ExecutionVoidReason::ParallelJoin,
                    timestamp: Some(timestamp(220)),
                },
                RunEvent::NodeCompleted {
                    completion: NodeCompletion {
                        reference: worker_retry,
                        outcome: WorkerOutcome::Verified {
                            output: Value::Null,
                            artifacts: Vec::new(),
                        },
                    },
                    timestamp: Some(timestamp(300)),
                },
                RunEvent::SafeLog {
                    execution: None,
                    timestamp: timestamp(310),
                    stream: crate::v2_run_ledger::SafeLogStream::System,
                    line: SafeLogLine::new("Pi compacted the session context").assert_value(),
                    diagnostic: None,
                },
                RunEvent::SafeLog {
                    execution: None,
                    timestamp: timestamp(311),
                    stream: crate::v2_run_ledger::SafeLogStream::System,
                    line: SafeLogLine::new("final output rejected: malformed").assert_value(),
                    diagnostic: None,
                },
            ],
        )
        .await
        .assert_value();
    let service = crate::native_v2_observability::history::RunHistoryService::for_ledger(ledger);
    let definition = service.definition(&run_id).await.assert_value();
    assert_eq!(definition.compactions, 1);
    assert_eq!(definition.format_rejections, 1);
    let worker_row = definition
        .nodes
        .iter()
        .find(|node| node.name.as_str() == "worker")
        .assert_value();
    assert_eq!(worker_row.node_instance, "7");
    assert_eq!(worker_row.attempts, 2);
    assert_eq!(worker_row.started_at, Some(timestamp(100)));
    assert_eq!(worker_row.completed_at, Some(timestamp(300)));
    assert_eq!(worker_row.duration_ms, Some(200));
    assert!(!worker_row.voided);
    let reviewer_row = definition
        .nodes
        .iter()
        .find(|node| node.name.as_str() == "reviewer")
        .assert_value();
    assert_eq!(reviewer_row.attempts, 1);
    assert_eq!(reviewer_row.duration_ms, None);
    assert!(reviewer_row.voided);
    let report = crate::native_v2_observability::history::build_run_report(&definition, None);
    let value = serde_json::to_value(report).assert_value();
    assert_eq!(value["schema"], "zeroshot.run.report/v1");
    assert_eq!(value["totals"]["wallClockMs"], 200);
    assert_eq!(value["totals"]["nodeDurationMs"], 200);
    assert_eq!(value["totals"]["unattributedMs"], 0);
    assert_eq!(value["totals"]["compactions"], 1);
    assert_eq!(value["totals"]["formatRejections"], 1);

    let mut legacy = serde_json::to_value(&definition).assert_value();
    let fields = legacy.as_object_mut().assert_value();
    fields.remove("nodes");
    fields.remove("compactions");
    fields.remove("formatRejections");
    let decoded: crate::native_v2_observability::history::RunDefinition =
        serde_json::from_value(legacy).assert_value();
    assert!(decoded.nodes.is_empty());
    assert_eq!(decoded.compactions, 0);
    assert_eq!(decoded.format_rejections, 0);

    let mut untimed = definition;
    for node in &mut untimed.nodes {
        node.started_at = None;
        node.completed_at = None;
        node.duration_ms = None;
    }
    let undated = serde_json::to_value(crate::native_v2_observability::history::build_run_report(
        &untimed, None,
    ))
    .assert_value();
    assert_eq!(undated["totals"]["wallClockMs"], Value::Null);
    assert_eq!(undated["totals"]["nodeDurationMs"], 0);
    assert_eq!(undated["totals"]["unattributedMs"], Value::Null);
    let undated_nodes = undated["nodes"].as_array().assert_value();
    assert_eq!(undated_nodes.len(), 2);
    for node in undated_nodes {
        assert_eq!(node["startedAt"], Value::Null);
        assert_eq!(node["completedAt"], Value::Null);
        assert_eq!(node["durationMs"], Value::Null);
    }
}

async fn finish_worker_run(ledger: &dyn RunLedger, worker: &ExecutionRef) {
    ledger
        .append(
            &worker.run_id,
            vec![
                completed(worker, Value::Null),
                RunEvent::Terminal {
                    result: TerminalResult::Succeeded {
                        output: Value::Null,
                    },
                    timestamp: None,
                },
            ],
        )
        .await
        .assert_value();
}
