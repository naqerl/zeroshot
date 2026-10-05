#![cfg(unix)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openengine_cluster_protocol::{
    NodeInstructions, NodeName, PayloadType, PullRequestFeedback, RunId, WorkerRef,
};
use openengine_cluster_testkit::assertions::AssertValue;
use serde_json::Value;

use super::command::{PiCommandInput, PiCommandRequest, pi_arguments};
use super::provider_document::{MODELS_FILE, gateway_base_url, write_models};
use super::session::{PiSession, identity};
use super::session_id::{observe_session, pi_session_id};
use super::{PiAdapter, PiAdapterConfigError, PiConfig, PiProcessEnvironment};
use crate::execution::process::{HostedProcessPool, HostedProcessScope};
use crate::execution::SessionScope;
use crate::native_v2_capsule::provider_process::{LocalHarnessEnvironment, ProviderExecutionFiles};
use crate::native_v2_candidate::test_support::{TestDirectory, environment_name};
use crate::native_v2_contract::{
    DeclaredConnections, DeclaredEnvironment, ExecutionId, ExecutionRef, NodeInstanceId,
    NodeInvocation, NodeRuntimeBinding, PiProvider,
};
use crate::native_v2_runner::{
    DriverInvocation, NodeResponseContract, NodeRole, NodeSession, ResolvedEnvironment,
};
use crate::worker_catalog::{ModelId, ReasoningEffort};

fn adapter_configuration(
    provider: PiProvider,
    executable: &str,
    native_environment: BTreeMap<String, String>,
) -> PiConfig {
    PiConfig {
        provider,
        executable: executable.to_owned(),
        prefix_arguments: Vec::new(),
        workspace: PathBuf::from("/workspace"),
        runtime_home: PathBuf::from("/runtime"),
        local_user_home: Some(PathBuf::from("/user")),
        native_environment: LocalHarnessEnvironment::new(native_environment),
        base_environment: PiProcessEnvironment::new(BTreeMap::from([(
            "PATH".to_owned(),
            "/usr/bin:/bin".to_owned(),
        )]))
        .assert_value(),
        process_pool: HostedProcessPool::new(10_003, 10_003, 20_000).assert_value(),
    }
}

fn agent_binding(
    model: &str,
    effort: Option<ReasoningEffort>,
    scope: SessionScope,
    environment: &[&str],
) -> NodeRuntimeBinding {
    let connections = if environment.is_empty() {
        DeclaredConnections::empty()
    } else {
        DeclaredConnections::single(
            "provider",
            DeclaredEnvironment::new(environment.iter().map(|name| environment_name(name)))
                .assert_value(),
        )
        .assert_value()
    };
    NodeRuntimeBinding::Agent {
        model: ModelId::new(model).assert_value(),
        effort,
        session_scope: scope,
        connections,
    }
}

fn delivery_binding() -> NodeRuntimeBinding {
    NodeRuntimeBinding::GitDelivery {
        connections: DeclaredConnections::empty(),
        pull_request_feedback: PullRequestFeedback::Consider,
    }
}

/// A resolved environment carrying exactly the supplied values, through the declared-connection path.
fn declared_names(fields: &[&str]) -> Vec<crate::native_v2_contract::EnvironmentVariableName> {
    fields.iter().map(|name| environment_name(name)).collect()
}

fn resolved(fields: &[&str], values: &[(&str, &str)]) -> ResolvedEnvironment {
    let binding = agent_binding("openai/gpt-5", None, SessionScope::Execution, fields);
    let mut map = BTreeMap::new();
    for (name, value) in values {
        map.insert(environment_name(name), (*value).to_owned());
    }
    ResolvedEnvironment::exact(&binding, map).assert_value_with("resolve declared environment")
}

/// A `ProviderExecutionFiles` whose home is a private directory under the test root, which is all
/// argv and session-directory construction reads.
fn files(directory: &TestDirectory) -> ProviderExecutionFiles {
    ProviderExecutionFiles::for_test(directory.path(), HostedProcessScope::WriterExecution(1))
        .assert_value_with("provider files")
}

/// argv construction is pure over the binding, role, and session inputs, so the remaining
/// invocation fields are never read and are left at their empty values.
fn invocation(
    binding: NodeRuntimeBinding,
    role: NodeRole,
    environment: ResolvedEnvironment,
) -> DriverInvocation {
    DriverInvocation {
        node: NodeInvocation {
            reference: execution_ref(),
            worker: WorkerRef::new("agent.pi@1").assert_value_with("worker"),
            instructions: Some(NodeInstructions::new("Exercise the Pi adapter.").assert_value()),
            input: Value::Null,
            binding,
        },
        role,
        response: NodeResponseContract::Worker {
            output: PayloadType::Null,
        },
        environment,
        session: test_session(),
        provider_session_slot: NodeInstanceId::new(1).assert_value_with("session slot"),
    }
}

fn execution_ref() -> ExecutionRef {
    ExecutionRef {
        run_id: RunId::new("run-pi"),
        node: NodeName::new("agent").assert_value_with("node"),
        node_instance: NodeInstanceId::new(1).assert_value_with("node instance"),
        execution: ExecutionId::new(1).assert_value_with("execution"),
    }
}

fn test_session() -> Arc<dyn NodeSession> {
    Arc::new(PiSession::for_test())
}

fn argv_for(provider: PiProvider, contained: bool) -> Vec<String> {
    pi_arguments(
        &[],
        &PiCommandInput {
            provider,
            model: "openai/gpt-5",
            effort: Some(ReasoningEffort::High),
            session_id: "zs-fixed-session-id",
            session_dir: Path::new("/run/pi-home/sessions"),
            contained,
        },
    )
}

fn value(argv: &[String], name: &str) -> Option<String> {
    argv.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

#[test]
fn coverage_contract_pi_argv_selects_json_mode_with_the_admitted_model_unchanged() {
    let argv = argv_for(PiProvider::OpenAi, false);
    assert_eq!(value(&argv, "--mode").as_deref(), Some("json"));
    // The model identifier is caller-owned and passed through unchanged.
    assert_eq!(value(&argv, "--model").as_deref(), Some("openai/gpt-5"));
    assert_eq!(value(&argv, "--provider").as_deref(), Some("openai"));
    assert_eq!(value(&argv, "--thinking").as_deref(), Some("high"));
    assert_eq!(
        value(&argv, "--session-id").as_deref(),
        Some("zs-fixed-session-id")
    );
    assert!(value(&argv, "--session-dir").is_some());
}

#[test]
fn coverage_contract_pi_argv_uses_pi_own_provider_names() {
    for (provider, expected) in [
        (PiProvider::Anthropic, "anthropic"),
        (PiProvider::OpenAi, "openai"),
        (PiProvider::OpenRouter, "openrouter"),
        (PiProvider::Bedrock, "amazon-bedrock"),
        (PiProvider::Gateway, "openai"),
    ] {
        assert_eq!(
            value(&argv_for(provider, false), "--provider").as_deref(),
            Some(expected),
            "unexpected Pi provider name for {provider:?}"
        );
    }
}

#[test]
fn coverage_contract_pi_contained_argv_pins_tools_and_refuses_project_resources() {
    let argv = argv_for(PiProvider::Anthropic, true);
    assert_eq!(
        value(&argv, "--tools").as_deref(),
        Some("read,bash,edit,write")
    );
    assert!(argv.iter().any(|item| item == "--no-extensions"));
    assert!(argv.iter().any(|item| item == "--no-approve"));
    // Context files stay enabled so repository-declared setup remains visible.
    assert!(!argv.iter().any(|item| item == "--no-context-files"));
}

#[test]
fn coverage_contract_pi_local_argv_keeps_ambient_extensions_active() {
    let argv = argv_for(PiProvider::Anthropic, false);
    assert!(!argv.iter().any(|item| item == "--no-extensions"));
    assert!(!argv.iter().any(|item| item == "--no-approve"));
    assert!(value(&argv, "--tools").is_none());
}

#[test]
fn coverage_contract_pi_never_appends_a_permission_argument() {
    // Pi exposes no approval or sandbox flag, so there is nothing to bypass. A contained run and
    // a local run must produce identical permission-free argv.
    for contained in [false, true] {
        let argv = argv_for(PiProvider::OpenAi, contained);
        for forbidden in [
            "--dangerously-bypass-approvals-and-sandbox",
            "--dangerously-skip-permissions",
            "--yolo",
            "--full-auto",
        ] {
            assert!(
                !argv.iter().any(|item| item == forbidden),
                "unexpected permission argument {forbidden}"
            );
        }
    }
}

#[test]
fn coverage_contract_pi_prefix_arguments_precede_the_adapter_contract() {
    let argv = pi_arguments(
        &["--offline".to_owned()],
        &PiCommandInput {
            provider: PiProvider::OpenAi,
            model: "openai/gpt-5",
            effort: None,
            session_id: "zs-fixed",
            session_dir: Path::new("/run/pi-home/sessions"),
            contained: false,
        },
    );
    assert_eq!(argv[0], "--offline");
    assert_eq!(value(&argv, "--mode").as_deref(), Some("json"));
}

#[test]
fn coverage_contract_pi_local_configuration_rejects_an_empty_executable() {
    assert!(matches!(
        PiAdapter::new_local(adapter_configuration(
            PiProvider::Anthropic,
            "",
            BTreeMap::new(),
        )),
        Err(PiAdapterConfigError::EmptyExecutable)
    ));
}

#[test]
fn coverage_contract_pi_process_environment_is_minimal_and_redacted_in_debug() {
    assert!(matches!(
        PiProcessEnvironment::new(BTreeMap::from([(
            "SECRET".to_owned(),
            "value".to_owned()
        )])),
        Err(PiAdapterConfigError::NonMinimalEnvironment(name)) if name == "SECRET"
    ));
    assert!(matches!(
        PiProcessEnvironment::new(BTreeMap::from([(
            "PATH".to_owned(),
            "bad\0value".to_owned()
        )])),
        Err(PiAdapterConfigError::InvalidEnvironment)
    ));
    let environment = PiProcessEnvironment::new(BTreeMap::from([(
        "PATH".to_owned(),
        "secret-search-path".to_owned(),
    )]))
    .assert_value();
    let debug = format!("{environment:?}");
    assert!(debug.contains("PATH"));
    assert!(!debug.contains("secret-search-path"));
}

#[test]
fn coverage_contract_pi_local_configuration_only_inherits_its_own_lane_credentials() {
    let native = BTreeMap::from([
        (
            "ANTHROPIC_API_KEY".to_owned(),
            "ambient-anthropic".to_owned(),
        ),
        ("OPENAI_API_KEY".to_owned(), "ambient-openai".to_owned()),
        ("HTTPS_PROXY".to_owned(), "https://proxy.invalid".to_owned()),
    ]);

    // A native-local lane keeps the ambient credentials Pi already resolved from its own store.
    let anthropic = PiAdapter::new_local(adapter_configuration(
        PiProvider::Anthropic,
        "pi",
        native.clone(),
    ))
    .assert_value();
    assert!(
        anthropic
            .local_environment
            .contains_key("ANTHROPIC_API_KEY")
    );

    let openai = PiAdapter::new_local(adapter_configuration(
        PiProvider::OpenAi,
        "pi",
        native.clone(),
    ))
    .assert_value();
    assert!(openai.local_environment.contains_key("OPENAI_API_KEY"));

    // A non-native lane must not inherit any lane credential from the invoking shell.
    let openrouter = PiAdapter::new_local(adapter_configuration(
        PiProvider::OpenRouter,
        "pi",
        native.clone(),
    ))
    .assert_value();
    assert!(
        !openrouter
            .local_environment
            .contains_key("ANTHROPIC_API_KEY")
    );
    assert!(!openrouter.local_environment.contains_key("OPENAI_API_KEY"));
    // Transport selectors are shared by every lane.
    assert!(openrouter.local_environment.contains_key("HTTPS_PROXY"));

    // A hosted adapter inherits nothing at all.
    let hosted =
        PiAdapter::new(adapter_configuration(PiProvider::Anthropic, "pi", native)).assert_value();
    assert!(hosted.local_environment.is_empty());
}

#[test]
fn coverage_contract_pi_reports_a_provider_identity_mismatch_as_a_failure() {
    assert!(observe_session(Some("zs-a"), "zs-a").is_ok());
    assert!(observe_session(Some("zs-b"), "zs-a").is_err());
    assert!(observe_session(None, "zs-a").is_ok());
}

#[test]
fn coverage_contract_pi_gateway_models_json_pins_responses_and_holds_no_secret() {
    let directory = TestDirectory::new("pi-gateway");
    let agent_dir = directory.child("agent");
    let fields = declared_names(&["GATEWAY_BASE_URL", "GATEWAY_API_KEY"]);
    let resolved = resolved(
        &["GATEWAY_BASE_URL", "GATEWAY_API_KEY"],
        &[
            ("GATEWAY_BASE_URL", "https://gateway.example/api/v1"),
            ("GATEWAY_API_KEY", "sk-gateway-secret"),
        ],
    );
    assert_eq!(fields.len(), 2);
    let path = write_models(
        &agent_dir,
        super::provider_document::GATEWAY,
        &gateway_base_url(&resolved).unwrap(),
    )
    .assert_value();
    assert_eq!(path, agent_dir.join(MODELS_FILE));
    let contents = directory.read("agent/models.json");
    let document: serde_json::Value = serde_json::from_str(&contents).unwrap();
    let provider = &document["providers"]["openai"];
    assert_eq!(provider["api"], "openai-responses");
    assert_eq!(provider["baseUrl"], "https://gateway.example/api/v1");
    // The secret stays in the declared connection, referenced through Pi's own interpolation.
    assert_eq!(provider["apiKey"], "$GATEWAY_API_KEY");
    assert!(!contents.contains("sk-gateway-secret"));
    // No model list is synthesized, so Pi keeps the built-in openai catalog.
    assert!(provider.get("models").is_none());
}

#[test]
fn coverage_contract_pi_gateway_connection_must_be_complete_before_launch() {
    let complete = resolved(
        &["GATEWAY_BASE_URL", "GATEWAY_API_KEY"],
        &[
            ("GATEWAY_BASE_URL", "https://gateway.example/api/v1"),
            ("GATEWAY_API_KEY", "sk-gateway"),
        ],
    );
    assert!(gateway_base_url(&complete).is_ok());

    // Only the base URL, only the key, an empty base URL, and nothing at all all fail closed.
    for (fields, values) in [
        (
            vec!["GATEWAY_BASE_URL"],
            vec![("GATEWAY_BASE_URL", "https://gateway.example")],
        ),
        (
            vec!["GATEWAY_API_KEY"],
            vec![("GATEWAY_API_KEY", "sk-gateway")],
        ),
        (
            vec!["GATEWAY_BASE_URL", "GATEWAY_API_KEY"],
            vec![("GATEWAY_BASE_URL", ""), ("GATEWAY_API_KEY", "k")],
        ),
        (vec![], vec![]),
    ] {
        assert!(
            gateway_base_url(&resolved(&fields, &values)).is_err(),
            "incomplete gateway connection must fail before launch"
        );
    }
}

/// A delivery binding is never valid for an agent turn.
#[test]
fn coverage_contract_pi_rejects_a_non_agent_binding() {
    let directory = TestDirectory::new("pi-binding");
    let files = files(&directory);
    let command = super::command::command(PiCommandRequest {
        provider: PiProvider::OpenAi,
        native_local: false,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(delivery_binding(), NodeRole::Worker, resolved(&[], &[])),
        files: &files,
        local_environment: &BTreeMap::new(),
        agent_dir: directory.path(),
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    });
    assert!(command.is_err());
}

#[test]
fn coverage_contract_pi_declared_connections_may_not_shadow_reserved_configuration() {
    // The adapter owns these names, so a declared connection colliding with one is a provider
    // failure rather than a silent override.
    for reserved in [
        "PI_CODING_AGENT_DIR",
        "PI_CODING_AGENT_SESSION_DIR",
        "PI_OFFLINE",
        "PI_TELEMETRY",
        "HOME",
        "TMPDIR",
    ] {
        let directory = TestDirectory::new("pi-reserved");
        let files = files(&directory);
        let command = super::command::command(PiCommandRequest {
            provider: PiProvider::Anthropic,
            native_local: false,
            contained: false,
            executable: "pi",
            prefix_arguments: &[],
            invocation: &invocation(
                agent_binding("openai/gpt-5", None, SessionScope::Execution, &[reserved]),
                NodeRole::Worker,
                resolved(&[reserved], &[(reserved, "authored")]),
            ),
            files: &files,
            local_environment: &BTreeMap::new(),
            agent_dir: directory.path(),
            session_dir: &directory.child("sessions"),
            session_id: "zs-fixed",
        });
        assert!(
            command.is_err(),
            "reserved name {reserved} must be rejected"
        );
    }
}

#[test]
fn coverage_contract_pi_local_shell_supplies_a_credential_when_none_is_declared() {
    // A local run with no declared connection relies on the invoking shell, and that value must
    // reach the Pi child: it is how a user who exports a key rather than storing one keeps it.
    let directory = TestDirectory::new("pi-ambient");
    let files = files(&directory);
    let ambient = BTreeMap::from([(String::from("ANTHROPIC_API_KEY"), String::from("ambient"))]);
    let command = super::command::command(PiCommandRequest {
        provider: PiProvider::Anthropic,
        native_local: false,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding(
                "anthropic/claude-sonnet-4-5",
                None,
                SessionScope::Execution,
                &[],
            ),
            NodeRole::Worker,
            resolved(&[], &[]),
        ),
        files: &files,
        local_environment: &ambient,
        agent_dir: directory.path(),
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    })
    .assert_value_with("ambient credential launch");
    assert_eq!(
        command
            .environment
            .get("ANTHROPIC_API_KEY")
            .map(String::as_str),
        Some("ambient")
    );
}

#[test]
fn coverage_contract_pi_declared_credential_beats_the_local_shell() {
    let directory = TestDirectory::new("pi-ambient-declared");
    let files = files(&directory);
    let ambient = BTreeMap::from([(String::from("ANTHROPIC_API_KEY"), String::from("ambient"))]);
    let command = super::command::command(PiCommandRequest {
        provider: PiProvider::Anthropic,
        native_local: false,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding(
                "anthropic/claude-sonnet-4-5",
                None,
                SessionScope::Execution,
                &["ANTHROPIC_API_KEY"],
            ),
            NodeRole::Worker,
            resolved(&["ANTHROPIC_API_KEY"], &[("ANTHROPIC_API_KEY", "declared")]),
        ),
        files: &files,
        local_environment: &ambient,
        agent_dir: directory.path(),
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    })
    .assert_value_with("declared credential wins");
    assert_eq!(
        command
            .environment
            .get("ANTHROPIC_API_KEY")
            .map(String::as_str),
        Some("declared")
    );
}

#[test]
fn coverage_contract_pi_gateway_refuses_a_foreign_ambient_credential() {
    // The ambient merge happens before credential validation, so a key belonging to another lane
    // is refused instead of riding along inside the gateway child.
    let directory = TestDirectory::new("pi-ambient-gateway");
    let files = files(&directory);
    let ambient = BTreeMap::from([(String::from("ANTHROPIC_API_KEY"), String::from("ambient"))]);
    let command = super::command::command(PiCommandRequest {
        provider: PiProvider::Gateway,
        native_local: false,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding("provider-model", None, SessionScope::Execution, &[]),
            NodeRole::Worker,
            resolved(
                &["GATEWAY_BASE_URL", "GATEWAY_API_KEY"],
                &[
                    ("GATEWAY_BASE_URL", "https://gateway.example/api/v1"),
                    ("GATEWAY_API_KEY", "declared"),
                ],
            ),
        ),
        files: &files,
        local_environment: &ambient,
        agent_dir: directory.path(),
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    });
    assert!(
        command.is_err(),
        "a foreign ambient key must not reach the gateway lane"
    );
}

#[test]
fn coverage_contract_pi_anthropic_lane_accepts_a_caller_owned_endpoint() {
    // Claude Code honours `ANTHROPIC_BASE_URL`; Pi does not, so the adapter converts a declared
    // value into a provider override that keeps Pi's Anthropic Messages protocol.
    let directory = TestDirectory::new("pi-anthropic-endpoint");
    let files = files(&directory);
    let agent_dir = directory.child("agent");
    let command = super::command::command(PiCommandRequest {
        provider: PiProvider::Anthropic,
        native_local: false,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding(
                "provider-owned-model",
                None,
                SessionScope::Execution,
                &["ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"],
            ),
            NodeRole::Worker,
            resolved(
                &["ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"],
                &[
                    ("ANTHROPIC_API_KEY", "declared"),
                    ("ANTHROPIC_BASE_URL", "https://messages.example"),
                ],
            ),
        ),
        files: &files,
        local_environment: &BTreeMap::new(),
        agent_dir: &agent_dir,
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    })
    .assert_value_with("caller-owned endpoint launch");
    assert_eq!(
        value(&command.argv, "--provider").as_deref(),
        Some("anthropic")
    );
    // A non-native lane receives a private agent directory so no ambient configuration is inherited.
    assert_eq!(
        command
            .environment
            .get("PI_CODING_AGENT_DIR")
            .map(String::as_str),
        Some(agent_dir.to_string_lossy().as_ref())
    );
}

#[test]
fn coverage_contract_pi_a_declared_endpoint_ends_native_login_reuse() {
    // The credential and the endpoint both come from the connection, so the run must not inherit
    // the user's own agent directory and send a stored login to a caller-owned host.
    let directory = TestDirectory::new("pi-endpoint-native");
    let files = files(&directory);
    // The agent directory is the provider session home itself.
    let agent_dir = files.home().to_path_buf();
    let adapter = PiAdapter::new_local(adapter_configuration(
        PiProvider::Anthropic,
        "pi",
        BTreeMap::new(),
    ))
    .assert_value();
    let invocation = invocation(
        agent_binding(
            "provider-owned-model",
            None,
            SessionScope::Execution,
            &["ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"],
        ),
        NodeRole::Worker,
        resolved(
            &["ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"],
            &[
                ("ANTHROPIC_API_KEY", "declared"),
                ("ANTHROPIC_BASE_URL", "https://messages.example"),
            ],
        ),
    );
    let command = adapter
        .command_for_test(&invocation, &files, "zs-fixed")
        .assert_value_with("declared endpoint launch");
    assert_eq!(
        command
            .environment
            .get("PI_CODING_AGENT_DIR")
            .map(String::as_str),
        Some(agent_dir.to_string_lossy().as_ref()),
        "a declared endpoint requires a private agent directory"
    );
    assert!(
        agent_dir.join(MODELS_FILE).is_file(),
        "a declared endpoint writes the provider document Pi reads"
    );
}

#[test]
fn coverage_contract_pi_native_local_anthropic_leaves_the_endpoint_to_pi() {
    // Without a declared endpoint and with a native-local lane, Pi keeps its own configuration and
    // login, so no provider document may be synthesized.
    let directory = TestDirectory::new("pi-anthropic-native");
    let files = files(&directory);
    let command = super::command::command(PiCommandRequest {
        provider: PiProvider::Anthropic,
        native_local: true,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding("claude-sonnet-4-5", None, SessionScope::Execution, &[]),
            NodeRole::Worker,
            resolved(&[], &[]),
        ),
        files: &files,
        local_environment: &BTreeMap::new(),
        agent_dir: directory.path(),
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    })
    .assert_value_with("native anthropic launch");
    assert!(!command.environment.contains_key("PI_CODING_AGENT_DIR"));
    assert!(!directory.path().join(MODELS_FILE).exists());
}

#[test]
fn coverage_contract_pi_launches_with_the_resolved_lane_credentials() {
    let directory = TestDirectory::new("pi-launch");
    let files = files(&directory);
    let sessions = directory.child("sessions");
    let command = super::command::command(PiCommandRequest {
        provider: PiProvider::Anthropic,
        native_local: false,
        contained: true,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding(
                "anthropic/claude-sonnet-4-5",
                None,
                SessionScope::Execution,
                &["ANTHROPIC_API_KEY"],
            ),
            NodeRole::Worker,
            resolved(
                &["ANTHROPIC_API_KEY"],
                &[("ANTHROPIC_API_KEY", "declared-key")],
            ),
        ),
        files: &files,
        local_environment: &BTreeMap::new(),
        agent_dir: directory.path(),
        session_dir: &sessions,
        session_id: "zs-fixed",
    })
    .assert_value_with("Pi launch");
    assert_eq!(command.program, "pi");
    assert_eq!(
        command
            .environment
            .get("ANTHROPIC_API_KEY")
            .map(String::as_str),
        Some("declared-key")
    );
    // A non-native lane gets a private agent directory, never the user's own configuration.
    assert_eq!(
        command
            .environment
            .get("PI_CODING_AGENT_DIR")
            .map(String::as_str),
        Some(directory.path().to_string_lossy().as_ref())
    );
    assert_eq!(
        command
            .environment
            .get("PI_CODING_AGENT_SESSION_DIR")
            .map(String::as_str),
        Some(sessions.to_string_lossy().as_ref())
    );
    // A contained run suppresses Pi's own startup traffic and telemetry.
    assert_eq!(
        command.environment.get("PI_OFFLINE").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        command.environment.get("PI_TELEMETRY").map(String::as_str),
        Some("0")
    );
}

#[test]
fn coverage_contract_pi_native_local_lane_leaves_the_agent_directory_unset() {
    let directory = TestDirectory::new("pi-native");
    let files = files(&directory);
    let command = super::command::command(PiCommandRequest {
        provider: PiProvider::Anthropic,
        native_local: true,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding(
                "anthropic/claude-sonnet-4-5",
                None,
                SessionScope::Execution,
                &[],
            ),
            NodeRole::Worker,
            resolved(&[], &[]),
        ),
        files: &files,
        local_environment: &BTreeMap::new(),
        agent_dir: directory.path(),
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    })
    .assert_value_with("Pi native launch");
    // The user's own Pi configuration supplies the stored login.
    assert!(!command.environment.contains_key("PI_CODING_AGENT_DIR"));
    assert!(!command.environment.contains_key("PI_OFFLINE"));
    // Session storage stays private even for a native lane.
    assert!(
        command
            .environment
            .contains_key("PI_CODING_AGENT_SESSION_DIR")
    );
}

#[test]
fn coverage_contract_pi_foreign_lane_credentials_are_refused() {
    // A credential from another lane must never authenticate the selected provider.
    for (provider, foreign) in [
        (PiProvider::Anthropic, "OPENAI_API_KEY"),
        (PiProvider::OpenAi, "ANTHROPIC_API_KEY"),
        (PiProvider::OpenRouter, "ANTHROPIC_API_KEY"),
    ] {
        let directory = TestDirectory::new("pi-foreign");
        let files = files(&directory);
        let command = super::command::command(PiCommandRequest {
            provider,
            native_local: false,
            contained: false,
            executable: "pi",
            prefix_arguments: &[],
            invocation: &invocation(
                agent_binding("provider/model", None, SessionScope::Execution, &[foreign]),
                NodeRole::Worker,
                resolved(&[foreign], &[(foreign, "smuggled")]),
            ),
            files: &files,
            local_environment: &BTreeMap::new(),
            agent_dir: directory.path(),
            session_dir: &directory.child("sessions"),
            session_id: "zs-fixed",
        });
        assert!(command.is_err(), "{foreign} must not satisfy {provider:?}");
    }
}

#[test]
fn coverage_contract_pi_present_but_empty_declared_credential_fails_closed() {
    for (provider, field) in [
        (PiProvider::Anthropic, "ANTHROPIC_API_KEY"),
        (PiProvider::OpenAi, "OPENAI_API_KEY"),
        (PiProvider::OpenRouter, "OPENROUTER_API_KEY"),
    ] {
        let directory = TestDirectory::new("pi-empty");
        let files = files(&directory);
        let command = super::command::command(PiCommandRequest {
            provider,
            native_local: false,
            contained: false,
            executable: "pi",
            prefix_arguments: &[],
            invocation: &invocation(
                agent_binding("provider/model", None, SessionScope::Execution, &[field]),
                NodeRole::Worker,
                resolved(&[field], &[(field, "")]),
            ),
            files: &files,
            local_environment: &BTreeMap::new(),
            agent_dir: directory.path(),
            session_dir: &directory.child("sessions"),
            session_id: "zs-fixed",
        });
        assert!(
            command.is_err(),
            "an empty declared {field} must not fall back to a stored login"
        );
    }
}

#[test]
fn coverage_contract_pi_bedrock_lane_requires_its_own_pair() {
    let directory = TestDirectory::new("pi-bedrock");
    let files = files(&directory);
    let complete = super::command::command(PiCommandRequest {
        provider: PiProvider::Bedrock,
        native_local: false,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding(
                "amazon-bedrock/anthropic.claude",
                None,
                SessionScope::Execution,
                &["AWS_BEARER_TOKEN_BEDROCK", "AWS_REGION"],
            ),
            NodeRole::Worker,
            resolved(
                &["AWS_BEARER_TOKEN_BEDROCK", "AWS_REGION"],
                &[
                    ("AWS_BEARER_TOKEN_BEDROCK", "bedrock-token"),
                    ("AWS_REGION", "us-east-1"),
                ],
            ),
        ),
        files: &files,
        local_environment: &BTreeMap::new(),
        agent_dir: directory.path(),
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    })
    .assert_value_with("bedrock launch");
    assert_eq!(
        value(&complete.argv, "--provider").as_deref(),
        Some("amazon-bedrock")
    );

    // Only one half of the pair is not enough.
    let partial = super::command::command(PiCommandRequest {
        provider: PiProvider::Bedrock,
        native_local: false,
        contained: false,
        executable: "pi",
        prefix_arguments: &[],
        invocation: &invocation(
            agent_binding(
                "amazon-bedrock/anthropic.claude",
                None,
                SessionScope::Execution,
                &["AWS_REGION"],
            ),
            NodeRole::Worker,
            resolved(&["AWS_REGION"], &[("AWS_REGION", "us-east-1")]),
        ),
        files: &files,
        local_environment: &BTreeMap::new(),
        agent_dir: directory.path(),
        session_dir: &directory.child("sessions"),
        session_id: "zs-fixed",
    });
    assert!(partial.is_err());
}

#[test]
fn coverage_contract_pi_session_identity_is_derived_per_scope() {
    let node_instance = invocation(
        agent_binding("provider/model", None, SessionScope::NodeInstance, &[]),
        NodeRole::Worker,
        resolved(&[], &[]),
    );
    let execution = invocation(
        agent_binding("provider/model", None, SessionScope::Execution, &[]),
        NodeRole::Worker,
        resolved(&[], &[]),
    );
    let derived = pi_session_id(&identity(&node_instance).unwrap()).unwrap();
    assert!(derived.starts_with("zs-"));
    // The two scopes must never share one provider session.
    assert_ne!(
        derived,
        pi_session_id(&identity(&execution).unwrap()).unwrap()
    );
    // A node instance keeps its identity across revisits of the same node.
    assert_eq!(
        derived,
        pi_session_id(&identity(&node_instance).unwrap()).unwrap()
    );
}

#[test]
fn coverage_contract_pi_provider_failures_carry_a_bounded_redacted_diagnostic() {
    let mut transcript = super::PiTranscript::new(vec!["sk-secret".to_owned()]);
    transcript.push(
        serde_json::json!({
            "type": "message_end",
            "message": {"role": "assistant", "content": [], "stopReason": "error",
                        "errorMessage": "rejected sk-secret"}
        })
        .to_string()
        .as_bytes(),
    );
    let super::PiAttempt::Failed(failure) = transcript.finish(None).unwrap() else {
        panic!("expected a failure");
    };
    assert!(
        failure.retryable,
        "a failed model response is retryable once"
    );
    assert!(!failure.diagnostic.contains("sk-secret"));
}

#[test]
fn coverage_contract_pi_tool_output_and_retries_surface_as_system_output() {
    let mut transcript = super::PiTranscript::new(Vec::new());
    let emissions = transcript.push(
        [
            r#"{"type":"tool_execution_start","toolName":"bash","args":{"command":"ls"}}"#,
            r#"{"type":"auto_retry_start","attempt":2,"delayMs":2000,"errorMessage":"529 overloaded"}"#,
            r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"ok"}],"stopReason":"stop"}}"#,
        ]
        .join("\n")
        .as_bytes(),
    );
    let system: String = emissions
        .iter()
        .filter(|emission| emission.stream == crate::native_v2_runner::LiveOutputStream::System)
        .map(|emission| emission.text.as_str())
        .collect();
    assert!(system.contains("retrying"));
    let super::PiAttempt::Complete(result) = transcript.finish(None).unwrap() else {
        panic!("expected a complete attempt");
    };
    assert_eq!(result.message, "ok");
}

#[test]
fn coverage_contract_pi_usage_is_recorded_for_the_turn() {
    let mut transcript = super::PiTranscript::new(Vec::new());
    transcript.push(
        br#"{"type":"message_update","usage":{"input":42,"output":7,"cacheRead":3,"cacheWrite":1,"totalTokens":53},"assistantMessageEvent":{"type":"start"}}
"#
            .as_slice(),
    );
    let usage = transcript.token_usage().expect("usage is recorded");
    assert_eq!(usage.input_tokens.get(), 42);
    assert_eq!(usage.output_tokens.get(), 7);
    assert_eq!(
        usage.cache_read_input_tokens.map(|count| count.get()),
        Some(3)
    );
    assert_eq!(
        usage.cache_creation_input_tokens.map(|count| count.get()),
        Some(1)
    );
}
#[test]
fn coverage_contract_pi_binding_effort_absent_omits_the_thinking_argument() {
    let argv = pi_arguments(
        &[],
        &PiCommandInput {
            provider: PiProvider::Anthropic,
            model: "anthropic/claude-sonnet-4-5",
            effort: None,
            session_id: "zs-fixed",
            session_dir: Path::new("/run/pi-home/sessions"),
            contained: false,
        },
    );
    assert!(value(&argv, "--thinking").is_none());
    assert_eq!(
        value(&argv, "--model").as_deref(),
        Some("anthropic/claude-sonnet-4-5")
    );
}

#[test]
fn coverage_contract_pi_homes_stay_inside_the_private_session_home() {
    let home = Path::new("/run/pi-home");
    assert_eq!(super::session_id::agent_directory(home), home);
    assert_eq!(
        super::session_id::session_directory(home),
        home.join("sessions")
    );
}
