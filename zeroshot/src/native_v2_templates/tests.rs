use std::collections::BTreeSet;

use openengine_cluster_protocol::{IdempotencyKey, RunSubmission, RunTitle};
use openengine_cluster_server::admission::VerifiedGraph;
use openengine_cluster_testkit::assertions::{AssertError, AssertValue};
use serde_json::{Value, json};

use crate::native_v2_admission::{DeliveryPolicy, NativeV2Admission};
use crate::native_v2_contract::RunSubmissionIntent;
use crate::native_v2_delivery::{DELIVERY_MERGED_LABEL, DELIVERY_PUSHED_LABEL, DELIVERY_READY_LABEL};
use crate::full_v1_reducer::{
    Decision, DurableExecution, FullV1Reducer, Reduction, ReductionInput, TerminalProjection,
};

use super::*;

#[path = "tests/support.rs"]
mod support;
use support::*;

#[path = "tests/continuity.rs"]
mod continuity;

#[test]
fn catalog_and_template_inputs_are_closed() {
    assert_eq!(
        BuiltinGraphTemplate::all(),
        &[
            BuiltinGraphTemplate::SingleWorker,
            BuiltinGraphTemplate::SoftwareChange,
            BuiltinGraphTemplate::SoftwareChangeFast,
            BuiltinGraphTemplate::AutoResearch,
            BuiltinGraphTemplate::IssueReview,
            BuiltinGraphTemplate::PrReview,
        ]
    );
    assert_eq!(
        BuiltinGraphTemplate::parse("software-change"),
        Some(BuiltinGraphTemplate::SoftwareChange)
    );
    assert_eq!(
        BuiltinGraphTemplate::parse("software-change-fast"),
        Some(BuiltinGraphTemplate::SoftwareChangeFast)
    );
    assert_eq!(
        BuiltinGraphTemplate::parse("auto-research"),
        Some(BuiltinGraphTemplate::AutoResearch)
    );
    assert_eq!(BuiltinGraphTemplate::parse("unknown"), None);
    assert!(matches!(
        BuiltinGraphTemplate::SingleWorker.materialize(TemplateDelivery::Merge),
        Err(BuiltinTemplateError::UnsupportedDelivery { .. })
    ));

    let authored = json!({"task":"repair checkout"});
    let single = BuiltinGraphTemplate::SingleWorker
        .materialize(TemplateDelivery::None)
        .assert_value();
    single
        .initial_input
        .validate_value(&authored)
        .assert_value();
    let research = BuiltinGraphTemplate::AutoResearch
        .materialize(TemplateDelivery::None)
        .assert_value();
    research
        .initial_input
        .validate_value(&authored)
        .assert_value();
    research
        .initial_input
        .validate_value(&json!({"task":"repair checkout","options":{"iterations":200}}))
        .assert_value();
    assert!(
        research
            .initial_input
            .validate_value(&json!({"task":"repair checkout","options":{"iterations":"200"}}))
            .is_err()
    );
    assert!(
        research
            .initial_input
            .validate_value(&json!({"task":"repair checkout","iterations":10}))
            .is_err()
    );
    let software = BuiltinGraphTemplate::SoftwareChange
        .materialize(TemplateDelivery::None)
        .assert_value();
    software
        .initial_input
        .validate_value(&authored)
        .assert_value();
    assert!(
        software
            .initial_input
            .validate_value(&json!({"task":"repair checkout","acceptanceFeedback":""}))
            .is_err()
    );
    assert!(
        software
            .initial_input
            .validate_value(&json!({"task":"repair checkout","issueNumber":"208"}))
            .is_err()
    );
    let delivered = BuiltinGraphTemplate::SoftwareChange
        .materialize(TemplateDelivery::PullRequest)
        .assert_value();
    delivered
        .initial_input
        .validate_value(&authored)
        .assert_value();
    delivered
        .initial_input
        .validate_value(&json!({"task":"repair checkout","issueNumber":"208"}))
        .assert_value();
}

#[tokio::test]
async fn every_supported_materialization_is_admissible() {
    let cases = vec![
        (
            BuiltinGraphTemplate::SingleWorker,
            TemplateDelivery::None,
            vec!["worker"],
        ),
        (
            BuiltinGraphTemplate::SoftwareChange,
            TemplateDelivery::None,
            vec!["acceptance", "code", "worker"],
        ),
        (
            BuiltinGraphTemplate::SoftwareChange,
            TemplateDelivery::Push,
            vec!["acceptance", "code", "deliver", "worker"],
        ),
        (
            BuiltinGraphTemplate::SoftwareChange,
            TemplateDelivery::PullRequest,
            vec!["acceptance", "code", "deliver", "worker"],
        ),
        (
            BuiltinGraphTemplate::SoftwareChange,
            TemplateDelivery::Merge,
            vec!["acceptance", "code", "deliver", "worker"],
        ),
        (
            BuiltinGraphTemplate::AutoResearch,
            TemplateDelivery::None,
            auto_research_leaves(TemplateDelivery::None),
        ),
        (
            BuiltinGraphTemplate::AutoResearch,
            TemplateDelivery::Push,
            auto_research_leaves(TemplateDelivery::Push),
        ),
    ];

    for (template, delivery, expected) in cases {
        assert_admissible(template, delivery, &expected).await;
        let graph = template.materialize(delivery).assert_value();
        for node in all_nodes(&graph.root) {
            match node {
                GraphNode::Step(node) => assert!(node.timeout_ms.is_none()),
                GraphNode::Verifier(node) => assert!(node.timeout_ms.is_none()),
                _ => {}
            }
        }
    }
}

fn auto_research_leaves(delivery: TemplateDelivery) -> Vec<&'static str> {
    let mut leaves = vec![
        "explorer",
        "synthesizer",
        "challenger",
        "researcher",
        "reviewer",
    ];
    if delivery == TemplateDelivery::Push {
        leaves.push("checkpoint_delivery");
    }
    leaves
}

#[path = "auto_research_tests.rs"]
mod research_tests;

#[test]
fn software_change_has_one_global_ten_cycle_budget() {
    for delivery in [
        TemplateDelivery::None,
        TemplateDelivery::Push,
        TemplateDelivery::PullRequest,
        TemplateDelivery::Merge,
    ] {
        let graph = BuiltinGraphTemplate::SoftwareChange
            .materialize(delivery)
            .assert_value();
        let loops = all_nodes(&graph.root)
            .into_iter()
            .filter_map(|node| match node {
                GraphNode::Loop(loop_node) => Some(loop_node),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(loops.len(), 1, "software-change must contain one loop");
        let change_loop = loops.first().assert_value_with("software-change loop");
        assert_eq!(change_loop.name.as_str(), "change_loop");
        assert_eq!(change_loop.max_iterations.get(), 10);
        assert!(change_loop.until.is_none());
    }
}

fn fast_parameters() -> SoftwareChangeFastParameters {
    SoftwareChangeFastParameters {
        review_policy: ReviewPolicy::ByRadius,
        max_files: 3,
        max_lines: 80,
    }
}

fn fast_graph(delivery: TemplateDelivery) -> openengine_cluster_protocol::GraphSpec {
    BuiltinGraphTemplate::SoftwareChangeFast
        .materialize_with_parameters(delivery, fast_parameters())
        .assert_value()
}

fn named_node<'a>(root: &'a GraphNode, name: &str) -> &'a GraphNode {
    all_nodes(root)
        .into_iter()
        .find(|node| node.name().as_str() == name)
        .assert_value_with(name)
}

fn contains_node(root: &GraphNode, name: &str) -> bool {
    all_nodes(root)
        .into_iter()
        .any(|node| node.name().as_str() == name)
}

#[test]
fn native_v2_templates_gate_is_discoverable() {
    let baseline = BuiltinGraphTemplate::SoftwareChange
        .materialize(TemplateDelivery::None)
        .assert_value();
    let fast = BuiltinGraphTemplate::SoftwareChangeFast
        .materialize_with_parameters(TemplateDelivery::None, fast_parameters())
        .assert_value();
    assert_ne!(
        serde_json::to_value(&baseline).assert_value(),
        serde_json::to_value(&fast).assert_value(),
        "by_radius must produce the fast graph, not the baseline"
    );
}

#[test]
fn software_change_fast_always_is_the_software_change_baseline() {
    for delivery in [
        TemplateDelivery::None,
        TemplateDelivery::Push,
        TemplateDelivery::PullRequest,
        TemplateDelivery::Merge,
    ] {
        let baseline = BuiltinGraphTemplate::SoftwareChange
            .materialize(delivery)
            .assert_value();
        let fast = BuiltinGraphTemplate::SoftwareChangeFast
            .materialize_with_parameters(
                delivery,
                SoftwareChangeFastParameters {
                    review_policy: ReviewPolicy::Always,
                    ..fast_parameters()
                },
            )
            .assert_value();
        assert_eq!(
            serde_json::to_value(&baseline).assert_value(),
            serde_json::to_value(&fast).assert_value(),
            "software-change-fast with review_policy=always must serialize exactly like software-change"
        );
    }
}

#[test]
fn software_change_fast_by_radius_keeps_reviews_behind_the_large_branch() {
    for delivery in [
        TemplateDelivery::None,
        TemplateDelivery::Push,
        TemplateDelivery::PullRequest,
        TemplateDelivery::Merge,
    ] {
        let graph = fast_graph(delivery);
        let GraphNode::Choice(worker_result) = named_node(&graph.root, "worker_result") else {
            panic!("worker_result must be a choice");
        };
        let GraphNode::Seq(blast) = worker_result.otherwise.as_ref().assert_value().as_ref() else {
            panic!("worker_result otherwise must be the blast sequence");
        };
        assert_eq!(blast.name.as_str(), "blast");
        assert_eq!(
            blast
                .children
                .as_slice()
                .iter()
                .map(|node| node.name().as_str())
                .collect::<Vec<_>>(),
            ["jev", "blast_route"]
        );

        let GraphNode::Choice(blast_route) = named_node(&graph.root, "blast_route") else {
            panic!("blast_route must be a choice");
        };
        assert_eq!(blast_route.branches.as_slice().len(), 1);
        let small = &blast_route.branches.as_slice()[0];
        assert!(!contains_node(&small.node, "parallel_reviews"));
        assert!(contains_node(
            blast_route.otherwise.as_ref().assert_value(),
            "parallel_reviews"
        ));
        let small_accepts_delivery = if delivery == TemplateDelivery::None {
            contains_node(&small.node, "small_done")
        } else {
            contains_node(&small.node, "small_deliver")
        };
        assert!(small_accepts_delivery);
    }
}

#[test]
fn jev_output_schema_and_write_bindings_are_closed() {
    let graph = fast_graph(TemplateDelivery::None);
    let GraphNode::Step(jev) = named_node(&graph.root, "jev") else {
        panic!("jev must be a step");
    };
    assert_eq!(jev.worker.as_str(), "builtin.agent.jev@1");
    let PayloadType::Record { fields } = &jev.output else {
        panic!("jev output must be a record");
    };
    assert_eq!(
        fields
            .keys()
            .map(|name| name.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "radius",
            "confidence",
            "touches",
            "files",
            "lines",
            "checksReported",
            "reasons",
        ])
    );
    let written = jev
        .write_bindings
        .iter()
        .filter_map(|binding| binding.target.segments().last())
        .map(|name| name.as_str())
        .collect::<BTreeSet<_>>();
    for field in ["radius", "confidence", "files", "lines", "checksReported"] {
        assert!(written.contains(field), "missing write binding for {field}");
    }
}

async fn verified_fast_template(
    delivery: TemplateDelivery,
    parameters: SoftwareChangeFastParameters,
) -> (
    openengine_cluster_server::admission::VerifiedGraph,
    serde_json::Value,
) {
    let template = BuiltinGraphTemplate::SoftwareChangeFast;
    let graph = template
        .materialize_with_parameters(delivery, parameters)
        .assert_value();
    let runtime = runtime_for_parameters(
        template,
        delivery,
        &executable_leaves(&graph.root),
        parameters,
    );
    let initial_input = if delivery == TemplateDelivery::None {
        json!({"task":"repair checkout"})
    } else {
        json!({"task":"repair checkout","issueNumber":"208"})
    };
    let admitted = NativeV2Admission
        .admit(RunSubmission {
            environment: None,
            title: RunTitle::new("Fast template behavior").assert_value(),
            graph,
            initial_input: initial_input.clone(),
            runtime,
            source: Some(resolved_source()),
            submission_key: IdempotencyKey::new(format!(
                "fast-template-behavior-{}",
                delivery.name()
            ))
            .assert_value(),
        })
        .await
        .unwrap_or_else(|error| panic!("fast template was not admissible: {error:?}"));
    (
        openengine_cluster_server::admission::VerifiedGraph {
            compiled_ir: admitted.graph,
            diagnostics: Vec::new(),
        },
        initial_input,
    )
}

fn fast_worker_output(delivery: TemplateDelivery) -> serde_json::Value {
    if delivery == TemplateDelivery::None {
        json!({
            "checksReported": true,
            "acceptanceItems": [null, null],
            "codeItems": [null, null],
        })
    } else {
        json!({
            "checksReported": true,
            "acceptanceItems": [null, null],
            "codeItems": [null, null],
            "title": CHANGE_TITLE,
            "description": CHANGE_DESCRIPTION,
        })
    }
}

fn settled_fast_worker(delivery: TemplateDelivery) -> DurableExecution {
    settled_agent_with_output(
        SettledExecutionSpec {
            execution: 1,
            node_instance: 1,
            node: "worker",
            settled_at: 1,
            input: json!({
                "task":"repair checkout", "phase":{},
                "acceptanceFeedback":"", "codeFeedback":"", "deliveryFeedback":""
            }),
        },
        fast_worker_output(delivery),
    )
}

fn jev_output(
    radius: &str,
    confidence: &str,
    files: i64,
    lines: i64,
    checks: bool,
) -> serde_json::Value {
    json!({
        "radius": radius,
        "confidence": confidence,
        "touches": ["src/lib.rs"],
        "files": files,
        "lines": lines,
        "checksReported": checks,
        "reasons": "focused change",
    })
}

fn settled_fast_review(
    execution: u64,
    node: &str,
    verdict: &str,
    diagnostic: &str,
) -> DurableExecution {
    let marker = if verdict == ACCEPTED_LABEL {
        vec![serde_json::Value::Null, serde_json::Value::Null]
    } else {
        vec![serde_json::Value::Null]
    };
    let output = if node == "acceptance" {
        json!({ "acceptanceItems": marker })
    } else {
        json!({ "codeItems": marker })
    };
    settled_verifier(
        SettledExecutionSpec {
            execution,
            node_instance: execution,
            node,
            settled_at: execution,
            input: json!({"task":"repair checkout","deliveryFeedback":""}),
        },
        output,
        BTreeMap::from([(
            field_name(VERDICT_FIELD).assert_value(),
            enum_label(verdict).assert_value(),
        )]),
        json!({DIAGNOSTIC_MESSAGE_FIELD: diagnostic}),
    )
}

fn settled_jev(output: serde_json::Value) -> DurableExecution {
    settled_agent_with_output(
        SettledExecutionSpec {
            execution: 2,
            node_instance: 2,
            node: "jev",
            settled_at: 2,
            input: json!({"task":"repair checkout", "phase":{}, "checksReported": true}),
        },
        output,
    )
}

#[tokio::test]
async fn blast_route_sends_only_a_small_high_confidence_checked_change_to_delivery() {
    let (verified, initial_input) =
        verified_fast_template(TemplateDelivery::None, fast_parameters()).await;
    let history = [
        settled_fast_worker(TemplateDelivery::None),
        settled_jev(jev_output("small", "high", 1, 10, true)),
    ];
    let reduction = reduce(&verified, &initial_input, &history);
    assert_eq!(
        reduction.terminal,
        Some(TerminalProjection::Succeeded {
            output: serde_json::Value::Null,
        })
    );
    assert!(reduction.decisions.iter().all(|decision| !matches!(
        decision,
        Decision::Dispatch { occurrence, .. }
            if matches!(occurrence.node.as_str(), "acceptance" | "code")
    )));
}

#[tokio::test]
async fn blast_route_small_change_dispatches_delivery_with_worker_metadata() {
    let (verified, initial_input) =
        verified_fast_template(TemplateDelivery::PullRequest, fast_parameters()).await;
    let history = [
        settled_fast_worker(TemplateDelivery::PullRequest),
        settled_jev(jev_output("small", "high", 1, 10, true)),
    ];
    let reduction = reduce(&verified, &initial_input, &history);
    assert_dispatch(&reduction, SMALL_DELIVERY_NODE, &delivery_input());
}

#[tokio::test]
async fn blast_route_falls_back_to_reviews_for_each_failed_condition() {
    let (verified, initial_input) =
        verified_fast_template(TemplateDelivery::None, fast_parameters()).await;
    let cases = [
        ("large radius", jev_output("large", "high", 1, 10, true)),
        ("low confidence", jev_output("small", "low", 1, 10, true)),
        (
            "checks not reported",
            jev_output("small", "high", 1, 10, false),
        ),
        ("files over cap", jev_output("small", "high", 4, 10, true)),
        ("lines over cap", jev_output("small", "high", 1, 81, true)),
        ("negative files", jev_output("small", "high", -1, 10, true)),
        ("negative lines", jev_output("small", "high", 1, -1, true)),
    ];
    for (reason, output) in cases {
        let history = [
            settled_fast_worker(TemplateDelivery::None),
            settled_jev(output),
        ];
        let reduction = reduce(&verified, &initial_input, &history);
        assert!(
            reduction.terminal.is_none(),
            "{reason} must not complete without review"
        );
        assert_dispatched_together(&reduction, &["acceptance", "code"]);
    }
}

#[tokio::test]
async fn fast_template_admits_every_delivery() {
    for delivery in [
        TemplateDelivery::None,
        TemplateDelivery::Push,
        TemplateDelivery::PullRequest,
        TemplateDelivery::Merge,
    ] {
        let _ = verified_fast_template(delivery, fast_parameters()).await;
    }
}

#[tokio::test]
async fn accepted_large_reviews_reach_delivery_through_both_markers() {
    let (verified, initial_input) =
        verified_fast_template(TemplateDelivery::None, fast_parameters()).await;
    let history = [
        settled_fast_worker(TemplateDelivery::None),
        settled_jev(jev_output("large", "high", 4, 120, true)),
        settled_fast_review(3, "acceptance", ACCEPTED_LABEL, "requirements met"),
        settled_fast_review(4, "code", ACCEPTED_LABEL, "implementation sound"),
    ];
    let reduction = reduce(&verified, &initial_input, &history);
    assert_eq!(
        reduction.terminal,
        Some(TerminalProjection::Succeeded {
            output: serde_json::Value::Null,
        })
    );
    assert!(reduction.decisions.iter().all(|decision| !matches!(
        decision,
        Decision::Dispatch { occurrence, .. } if occurrence.node.as_str() == "worker"
    )));
}

#[tokio::test]
async fn rejected_reviews_clear_the_delivery_marker_and_dispatch_repair() {
    let (verified, initial_input) =
        verified_fast_template(TemplateDelivery::None, fast_parameters()).await;
    let history = [
        settled_fast_worker(TemplateDelivery::None),
        settled_jev(jev_output("large", "high", 4, 120, true)),
        settled_fast_review(
            3,
            "acceptance",
            REJECTED_LABEL,
            "missing requested behavior",
        ),
        settled_fast_review(4, "code", REJECTED_LABEL, "unsafe error handling"),
    ];
    let reduction = reduce(&verified, &initial_input, &history);
    assert!(
        reduction.terminal.is_none(),
        "terminal: {:?}; decisions: {:?}",
        reduction.terminal,
        reduction.decisions
    );
    assert!(
        reduction.decisions.iter().all(|decision| !matches!(
            decision,
            Decision::Dispatch { occurrence, .. } if occurrence.node.as_str() == DELIVERY_NODE
        )),
        "a rejected review must never reach delivery"
    );
    assert_dispatch(
        &reduction,
        "worker",
        &json!({
            "task":"repair checkout",
            "phase":{"after":"rejected"},
            "acceptanceFeedback":"missing requested behavior",
            "codeFeedback":"unsafe error handling",
            "deliveryFeedback":""
        }),
    );
}

#[tokio::test]
async fn rejected_parallel_reviews_dispatch_repair_with_both_diagnostics() {
    let (verified, initial_input) = verified_software_template(TemplateDelivery::None).await;
    let history = [
        settled_worker(),
        settled_review(
            2,
            "acceptance",
            REJECTED_LABEL,
            "missing requested behavior",
        ),
        settled_review(3, "code", REJECTED_LABEL, "unsafe error handling"),
    ];
    let reduction = reduce(&verified, &initial_input, &history);

    assert!(reduction.decisions.iter().any(|decision| matches!(
        decision,
        Decision::Dispatch { occurrence, input, .. }
            if occurrence.node.as_str() == "worker"
                && input == &json!({
                    "task":"repair checkout",
                    "phase":{"after":"rejected"},
                    "acceptanceFeedback":"missing requested behavior",
                    "codeFeedback":"unsafe error handling",
                    "deliveryFeedback":""
                })
    )));
}

fn crashed_parallel_review_history() -> Vec<DurableExecution> {
    let review_input = json!({"task":"repair checkout","deliveryFeedback":""});
    vec![
        settled_worker(),
        settled_review(
            2,
            "acceptance",
            REJECTED_LABEL,
            "missing requested behavior",
        ),
        settled_failure(
            SettledExecutionSpec {
                execution: 3,
                node_instance: 3,
                node: "code",
                settled_at: 3,
                input: review_input,
            },
            WorkerErrorCode::Crash,
        ),
    ]
}

#[tokio::test]
async fn crashed_parallel_review_retries_only_the_failed_verifier() {
    let (verified, initial_input) = verified_software_template(TemplateDelivery::None).await;
    let mut history = crashed_parallel_review_history();
    let reduction = reduce(&verified, &initial_input, &history);

    assert_eq!(
        reduction
            .decisions
            .iter()
            .filter(|decision| matches!(decision, Decision::Dispatch { .. }))
            .count(),
        1
    );
    assert!(reduction.decisions.iter().any(|decision| matches!(
        decision,
        Decision::Dispatch { occurrence, attempt, input, .. }
            if occurrence.node.as_str() == "code"
                && attempt.get() == MAX_AGENT_VERIFIER_ATTEMPTS
                && input == &json!({"task":"repair checkout","deliveryFeedback":""})
    )));
    assert!(reduction.terminal.is_none());

    let mut recovered = settled_review_execution(
        SettledExecutionSpec {
            execution: 4,
            node_instance: 3,
            node: "code",
            settled_at: 5,
            input: json!({"task":"repair checkout","deliveryFeedback":""}),
        },
        ACCEPTED_LABEL,
        "implementation sound",
    );
    recovered.attempt = PositiveInteger::new(MAX_AGENT_VERIFIER_ATTEMPTS).assert_value();
    history.push(recovered);
    let reviewed = reduce(&verified, &initial_input, &history);
    assert_dispatched_together(&reviewed, &["worker"]);
    let repair_input = json!({
        "task":"repair checkout",
        "phase":{"after":"rejected"},
        "acceptanceFeedback":"missing requested behavior",
        "codeFeedback":"implementation sound",
        "deliveryFeedback":""
    });
    assert!(reviewed.decisions.iter().any(|decision| matches!(
        decision, Decision::Dispatch { input, .. } if input == &repair_input
    )));
    history.push(settled_agent(SettledExecutionSpec {
        execution: 5,
        node_instance: 1,
        node: "worker",
        settled_at: 6,
        input: repair_input,
    }));
    let next_round = reduce(&verified, &initial_input, &history);
    assert_dispatched_together(&next_round, &["acceptance", "code"]);
    assert!(next_round.decisions.iter().all(|decision| match decision {
        Decision::Dispatch { attempt, .. } => attempt.get() == 1,
        _ => true,
    }));
}

#[tokio::test]
async fn malformed_reviewer_output_names_the_envelope_instead_of_review_failed() {
    let (verified, initial_input) = verified_software_template(TemplateDelivery::PullRequest).await;
    let history = [
        settled_worker(),
        settled_failure(
            SettledExecutionSpec {
                execution: 2,
                node_instance: 2,
                node: "acceptance",
                settled_at: 2,
                input: json!({"task":"repair checkout","deliveryFeedback":""}),
            },
            WorkerErrorCode::Malformed,
        ),
        settled_review(3, "code", ACCEPTED_LABEL, "implementation sound"),
    ];

    assert_eq!(
        reduce(&verified, &initial_input, &history).terminal,
        Some(TerminalProjection::Failed {
            context: None,
            reason: "review_output_invalid".parse().assert_value()
        })
    );
}

#[tokio::test]
async fn refused_reviewer_output_keeps_its_own_failure_reason() {
    let (verified, initial_input) = verified_software_template(TemplateDelivery::None).await;
    let history = [
        settled_worker(),
        settled_failure(
            SettledExecutionSpec {
                execution: 2,
                node_instance: 2,
                node: "acceptance",
                settled_at: 2,
                input: json!({"task":"repair checkout","deliveryFeedback":""}),
            },
            WorkerErrorCode::Refusal,
        ),
        settled_review(3, "code", REJECTED_LABEL, "unsafe error handling"),
    ];

    assert_eq!(
        reduce(&verified, &initial_input, &history).terminal,
        Some(TerminalProjection::Failed {
            context: None,
            reason: "review_refused".parse().assert_value()
        })
    );
}

#[tokio::test]
async fn repeatedly_crashed_parallel_review_fails_after_the_retry_limit() {
    let (verified, initial_input) = verified_software_template(TemplateDelivery::None).await;
    let review_input = json!({"task":"repair checkout","deliveryFeedback":""});
    let mut history = crashed_parallel_review_history();
    history.push(settled_failure_attempt(
        SettledExecutionSpec {
            execution: 4,
            node_instance: 3,
            node: "code",
            settled_at: 5,
            input: review_input,
        },
        WorkerErrorCode::Crash,
        MAX_AGENT_VERIFIER_ATTEMPTS,
    ));

    assert_eq!(
        reduce(&verified, &initial_input, &history).terminal,
        Some(TerminalProjection::Failed {
            context: None,
            reason: "review_failed".parse().assert_value()
        })
    );
}

#[tokio::test]
async fn accepted_second_review_round_ignores_stale_sibling_verdicts() {
    let (verified, initial_input) = verified_software_template(TemplateDelivery::None).await;
    let review_input = json!({"task":"repair checkout","deliveryFeedback":""});
    let mut history = vec![
        settled_worker(),
        settled_review(2, "acceptance", ACCEPTED_LABEL, "requirements met"),
        settled_review(3, "code", REJECTED_LABEL, "unsafe error handling"),
        settled_agent(SettledExecutionSpec {
            execution: 4,
            node_instance: 1,
            node: "worker",
            settled_at: 4,
            input: json!({
                "task":"repair checkout",
                "phase":{"after":"accepted"},
                "acceptanceFeedback":"requirements met",
                "codeFeedback":"unsafe error handling",
                "deliveryFeedback":""
            }),
        }),
    ];
    assert_dispatched_together(
        &reduce(&verified, &initial_input, &history),
        &["acceptance", "code"],
    );

    history.extend([
        settled_review_execution(
            SettledExecutionSpec {
                execution: 5,
                node_instance: 2,
                node: "acceptance",
                settled_at: 6,
                input: review_input.clone(),
            },
            ACCEPTED_LABEL,
            "repaired change meets requirements",
        ),
        settled_review_execution(
            SettledExecutionSpec {
                execution: 6,
                node_instance: 3,
                node: "code",
                settled_at: 5,
                input: review_input,
            },
            ACCEPTED_LABEL,
            "repaired implementation is sound",
        ),
    ]);

    assert_eq!(
        reduce(&verified, &initial_input, &history).terminal,
        Some(TerminalProjection::Succeeded {
            output: serde_json::Value::Null,
        })
    );
}

#[tokio::test]
async fn accepted_reviews_complete_or_dispatch_pull_request_delivery() {
    for delivery in [
        TemplateDelivery::None,
        TemplateDelivery::Push,
        TemplateDelivery::PullRequest,
    ] {
        let (verified, initial_input) = verified_software_template(delivery).await;
        let reviews = accepted_review_history(delivery);
        let worker_history = [settled_worker()];
        let after_worker = reduce(&verified, &initial_input, &worker_history);
        assert_dispatched_together(&after_worker, &["acceptance", "code"]);
        let reviewed = reduce(&verified, &initial_input, &reviews);

        if delivery == TemplateDelivery::None {
            assert_eq!(
                reviewed.terminal,
                Some(TerminalProjection::Succeeded {
                    output: serde_json::Value::Null,
                })
            );
            continue;
        }
        assert_dispatched_together(&reviewed, &[DELIVERY_NODE]);
        let (mode, outcome) = match delivery {
            TemplateDelivery::Push => (DeliveryMode::Push, DELIVERY_PUSHED_LABEL),
            TemplateDelivery::PullRequest => (DeliveryMode::PullRequestV2, DELIVERY_READY_LABEL),
            _ => unreachable!(),
        };
        let receipt = delivery_receipt(mode, outcome);
        let mut delivered = reviews;
        delivered.push(settled_delivery(
            SettledExecutionSpec {
                execution: 4,
                node_instance: 4,
                node: DELIVERY_NODE,
                settled_at: 4,
                input: delivery_input(),
            },
            mode,
            outcome,
        ));
        assert_eq!(
            reduce(&verified, &initial_input, &delivered).terminal,
            Some(TerminalProjection::Succeeded { output: receipt })
        );
    }
}

#[tokio::test]
async fn merge_delivery_repairs_recoverable_outcomes_then_returns_the_receipt() {
    for recoverable in [
        DELIVERY_CI_FAILED_LABEL,
        DELIVERY_CONFLICT_LABEL,
        DELIVERY_REPAIR_REQUIRED_LABEL,
    ] {
        assert_recoverable_delivery(recoverable).await;
    }
}

#[tokio::test]
async fn delivery_infrastructure_failure_never_dispatches_code_repair() {
    let (verified, initial_input) = verified_software_template(TemplateDelivery::Merge).await;
    let mut history = accepted_review_history(TemplateDelivery::Merge);
    history.push(settled_failure(
        SettledExecutionSpec {
            execution: 4,
            node_instance: 4,
            node: DELIVERY_NODE,
            settled_at: 4,
            input: delivery_input(),
        },
        WorkerErrorCode::Crash,
    ));

    let reduction = reduce(&verified, &initial_input, &history);
    assert_eq!(
        reduction.terminal,
        Some(TerminalProjection::Failed {
            context: None,
            reason: "delivery_failed".parse().assert_value()
        })
    );
    assert!(reduction.decisions.iter().all(|decision| !matches!(
        decision,
        Decision::Dispatch { occurrence, .. }
            if occurrence.node.as_str() == "worker"
    )));
}

async fn assert_recoverable_delivery(recoverable: &str) {
    let delivery_feedback = format!(
        "sourceRevision: {}\nreviewBaseRevision: {}\ntrusted delivery reported {recoverable}",
        "a".repeat(40),
        "b".repeat(40),
    );
    let (verified, initial_input) = verified_software_template(TemplateDelivery::Merge).await;
    let mut history = accepted_review_history(TemplateDelivery::Merge);
    let repair_input = json!({
        "task":"repair checkout",
        "phase":{"after":recoverable},
        "acceptanceFeedback":"Repair the checkout flow.",
        "codeFeedback":"implementation sound",
        "deliveryFeedback":delivery_feedback
    });
    history.push(settled_delivery_with_diagnostic(
        SettledExecutionSpec {
            execution: 4,
            node_instance: 4,
            node: DELIVERY_NODE,
            settled_at: 4,
            input: delivery_input(),
        },
        DeliveryMode::MergeV3,
        recoverable,
        &delivery_feedback,
    ));
    assert_dispatch(
        &reduce(&verified, &initial_input, &history),
        "worker",
        &repair_input,
    );
    history.push(settled_agent(SettledExecutionSpec {
        execution: 5,
        node_instance: 1,
        node: "worker",
        settled_at: 5,
        input: repair_input,
    }));
    assert_review_repair_preserves_delivery_feedback(
        &verified,
        &initial_input,
        &mut history,
        &delivery_feedback,
    );
    assert_repaired_reviews_then_merge(&verified, &initial_input, &mut history, &delivery_feedback);
}

fn assert_review_repair_preserves_delivery_feedback(
    verified: &VerifiedGraph,
    initial_input: &Value,
    history: &mut Vec<DurableExecution>,
    delivery_feedback: &str,
) {
    let review_input = json!({"task":"repair checkout","deliveryFeedback":delivery_feedback});
    let reviews = reduce(verified, initial_input, history);
    assert_dispatch(&reviews, "acceptance", &review_input);
    assert_dispatch(&reviews, "code", &review_input);
    history.extend([
        settled_review_execution_with_output(
            SettledExecutionSpec {
                execution: 6,
                node_instance: 2,
                node: "acceptance",
                settled_at: 6,
                input: review_input.clone(),
            },
            ACCEPTED_LABEL,
            REPAIRED_CHANGE_DESCRIPTION,
            repaired_change_manifest(),
        ),
        settled_review_execution(
            SettledExecutionSpec {
                execution: 7,
                node_instance: 3,
                node: "code",
                settled_at: 7,
                input: review_input,
            },
            REJECTED_LABEL,
            "repair an integration defect",
        ),
    ]);
    let repair_input = json!({
        "task":"repair checkout",
        "phase":{"after":"accepted"},
        "acceptanceFeedback":"Repair the checkout flow and stabilize its delivery checks.",
        "codeFeedback":"repair an integration defect",
        "deliveryFeedback":delivery_feedback,
    });
    assert_dispatch(
        &reduce(verified, initial_input, history),
        "worker",
        &repair_input,
    );
    history.push(settled_agent(SettledExecutionSpec {
        execution: 8,
        node_instance: 1,
        node: "worker",
        settled_at: 8,
        input: repair_input,
    }));
}

fn assert_repaired_reviews_then_merge(
    verified: &VerifiedGraph,
    initial_input: &Value,
    history: &mut Vec<DurableExecution>,
    delivery_feedback: &str,
) {
    let repaired = reduce(verified, initial_input, history);
    assert_dispatched_together(&repaired, &["acceptance", "code"]);
    let review_input = json!({
        "task":"repair checkout",
        "deliveryFeedback":delivery_feedback
    });
    assert_dispatch(&repaired, "acceptance", &review_input);
    assert_dispatch(&repaired, "code", &review_input);
    history.push(settled_review_execution_with_output(
        SettledExecutionSpec {
            execution: 9,
            node_instance: 2,
            node: "acceptance",
            settled_at: 9,
            input: review_input.clone(),
        },
        ACCEPTED_LABEL,
        REPAIRED_CHANGE_DESCRIPTION,
        repaired_change_manifest(),
    ));
    history.push(settled_review_execution(
        SettledExecutionSpec {
            execution: 10,
            node_instance: 3,
            node: "code",
            settled_at: 10,
            input: review_input,
        },
        ACCEPTED_LABEL,
        "CI repair is sound",
    ));
    assert_dispatch(
        &reduce(verified, initial_input, history),
        DELIVERY_NODE,
        &repaired_delivery_input(),
    );
    history.push(settled_delivery(
        SettledExecutionSpec {
            execution: 11,
            node_instance: 4,
            node: DELIVERY_NODE,
            settled_at: 11,
            input: repaired_delivery_input(),
        },
        DeliveryMode::MergeV3,
        DELIVERY_MERGED_LABEL,
    ));
    let receipt = delivery_receipt(DeliveryMode::MergeV3, DELIVERY_MERGED_LABEL);
    assert_eq!(
        reduce(verified, initial_input, history).terminal,
        Some(TerminalProjection::Succeeded { output: receipt })
    );
}

async fn assert_admissible(
    template: BuiltinGraphTemplate,
    delivery: TemplateDelivery,
    expected: &[&str],
) {
    let graph = template.materialize(delivery).assert_value();
    let leaves = executable_leaves(&graph.root);
    let names = leaves
        .iter()
        .map(|node| node.name().as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(names, expected.iter().copied().collect());
    assert_instruction_ownership(&leaves);

    let runtime = runtime_for(template, delivery, &leaves);
    let initial_input = json!({"task":"implement the requested change"});
    let policy = if delivery == TemplateDelivery::None {
        DeliveryPolicy::Optional
    } else {
        DeliveryPolicy::Required
    };
    let admitted = NativeV2Admission
        .validate_intent(
            &RunSubmissionIntent {
                environment: None,
                title: RunTitle::new("Built-in template admission").assert_value(),
                graph,
                initial_input,
                runtime,
                branch: None,
                submission_key: IdempotencyKey::new(format!(
                    "template-{}-{}",
                    template.name(),
                    delivery.name()
                ))
                .assert_value(),
            },
            policy,
        )
        .await;
    admitted.unwrap_or_else(|error| {
        panic!(
            "{} with {} delivery was not admissible: {error:?}",
            template.name(),
            delivery.name()
        )
    });
}

async fn verified_software_template(
    delivery: TemplateDelivery,
) -> (
    openengine_cluster_server::admission::VerifiedGraph,
    serde_json::Value,
) {
    let template = BuiltinGraphTemplate::SoftwareChange;
    let graph = template.materialize(delivery).assert_value();
    let runtime = runtime_for(template, delivery, &executable_leaves(&graph.root));
    let initial_input = if delivery == TemplateDelivery::None {
        json!({"task":"repair checkout"})
    } else {
        json!({"task":"repair checkout","issueNumber":"208"})
    };
    let admitted = NativeV2Admission
        .admit(RunSubmission {
            environment: None,
            title: RunTitle::new("Template behavior").assert_value(),
            graph,
            initial_input: initial_input.clone(),
            runtime,
            source: Some(resolved_source()),
            submission_key: IdempotencyKey::new(format!("template-behavior-{}", delivery.name()))
                .assert_value(),
        })
        .await
        .assert_value();
    (
        openengine_cluster_server::admission::VerifiedGraph {
            compiled_ir: admitted.graph,
            diagnostics: Vec::new(),
        },
        initial_input,
    )
}

fn reduce(
    verified: &openengine_cluster_server::admission::VerifiedGraph,
    initial_input: &serde_json::Value,
    history: &[DurableExecution],
) -> Reduction {
    let next_node_instance = history
        .iter()
        .map(|execution| execution.node_instance.get())
        .max()
        .unwrap_or(0)
        + 1;
    let next_execution = history
        .iter()
        .map(|execution| execution.execution.get())
        .max()
        .unwrap_or(0)
        + 1;
    FullV1Reducer::native_v2(verified)
        .reduce(ReductionInput {
            initial_input,
            executions: history,
            next_node_instance,
            next_execution,
        })
        .assert_value()
}

fn assert_dispatched_together(reduction: &Reduction, expected: &[&str]) {
    let dispatched = reduction
        .decisions
        .iter()
        .filter_map(|decision| match decision {
            Decision::Dispatch { occurrence, .. } => Some(occurrence.node.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(dispatched, expected.iter().copied().collect());
}

fn assert_dispatch(reduction: &Reduction, node: &str, expected_input: &serde_json::Value) {
    assert!(
        reduction.decisions.iter().any(|decision| matches!(
            decision,
            Decision::Dispatch { occurrence, input, .. }
                if occurrence.node.as_str() == node && input == expected_input
        )),
        "expected {node} input {expected_input}; decisions: {:?}",
        reduction.decisions
    );
}

fn all_nodes(root: &GraphNode) -> Vec<&GraphNode> {
    let mut nodes = Vec::new();
    let mut pending = vec![root];
    while let Some(node) = pending.pop() {
        nodes.push(node);
        pending.extend(openengine_cluster_server::graph_verifier::graph_node_children(node));
    }
    nodes
}

#[tokio::test]
async fn pull_request_delivery_routes_a_pre_review_git_failure_to_repair() {
    let (verified, initial_input) = verified_software_template(TemplateDelivery::PullRequest).await;
    let mut history = accepted_review_history(TemplateDelivery::PullRequest);
    history.push(settled_delivery_with_diagnostic(
        SettledExecutionSpec {
            execution: 4,
            node_instance: 4,
            node: DELIVERY_NODE,
            settled_at: 4,
            input: delivery_input(),
        },
        DeliveryMode::PullRequestV2,
        DELIVERY_REPAIR_REQUIRED_LABEL,
        "git push: unfamiliar failure",
    ));
    assert_dispatch(
        &reduce(&verified, &initial_input, &history),
        "worker",
        &json!({
            "task":"repair checkout", "phase":{"after":"repair_required"},
            "acceptanceFeedback":"Repair the checkout flow.",
            "codeFeedback":"implementation sound",
            "deliveryFeedback":"git push: unfamiliar failure"
        }),
    );
}
