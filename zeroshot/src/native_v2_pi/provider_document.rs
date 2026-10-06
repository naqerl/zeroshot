//! Synthesized `models.json` for Pi lanes that carry a caller-owned endpoint.
//!
//! Pi reads its provider catalog from this file. Two behaviors decide its shape, and both were
//! confirmed against the CLI rather than inferred. A *provider-level* `api` override does not
//! retag a built-in provider's existing models, though a model definition's own `api` always
//! applies. And a `models.json` `apiKey` is honored unless the agent directory holds a stored
//! credential for that provider, which outranks it on any provider, built-in or registered.
//! `baseUrl` is honored on both. That is why the `gateway` lane registers its own provider
//! instead of retargeting `openai` — it needs to choose the wire protocol — while the `anthropic`
//! lane retargets the endpoint and keeps the built-in protocol because it needs no `api`
//! override.
//!
//! The registered model entry carries the caller's own identifier, the chosen protocol, a display
//! name, and whether the caller's binding asked for reasoning. Capability metadata is deliberately
//! absent so Zeroshot never becomes a model catalog: Pi fills in its own defaults for context
//! window, output limit, and cost.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::native_v2_capsule::gateway;
use crate::native_v2_runner::{NodeRunnerError, ResolvedEnvironment};

/// Pi's provider file inside the private agent directory.
pub(super) const MODELS_FILE: &str = "models.json";

/// One provider override: which Pi provider to register, which protocol it speaks, and which
/// environment variable holds its key.
#[derive(Clone, Copy)]
pub(super) struct ProviderOverride {
    /// Pi provider name this file registers.
    pub(super) provider: &'static str,
    /// Environment field the API key is read from, referenced by Pi's `$NAME` interpolation.
    pub(super) api_key_field: &'static str,
}

/// The gateway lane registers its own provider so the caller owns the wire protocol.
pub(super) const GATEWAY: ProviderOverride = ProviderOverride {
    provider: "zeroshot-gateway",
    api_key_field: gateway::API_KEY,
};

/// The `anthropic` lane retargets Pi's built-in provider, because that lane's protocol is already
/// the one Pi speaks. No discriminant is written: a provider-level `api` override does not retag a
/// built-in provider's existing models.
pub(super) const ANTHROPIC: ProviderOverride = ProviderOverride {
    provider: "anthropic",
    api_key_field: "ANTHROPIC_API_KEY",
};

/// The `openai` lane retargets Pi's built-in OpenAI provider for the same reason. It keeps the
/// built-in Responses protocol, so a caller-owned endpoint for this lane must speak Responses; a
/// caller with another protocol uses the gateway lane, which can name it.
pub(super) const OPENAI: ProviderOverride = ProviderOverride {
    provider: "openai",
    api_key_field: "OPENAI_API_KEY",
};

/// Wire protocols Pi can be asked to speak. The caller names one; Zeroshot never guesses it from
/// the model identifier, because an identifier is opaque and provider-owned.
pub(super) const SUPPORTED_APIS: [&str; 3] = [
    "openai-responses",
    "openai-completions",
    "anthropic-messages",
];

#[derive(Debug, thiserror::Error)]
pub(super) enum PiProviderDocumentError {
    #[error("Pi provider configuration could not be serialized: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("Pi provider configuration could not be created: {0}")]
    Write(#[source] std::io::Error),
    #[error("Pi gateway requires a supported wire protocol in {field}")]
    UnsupportedApi { field: &'static str },
    #[error("Pi gateway requires a non-empty model identifier")]
    EmptyModel,
}

/// The endpoint fields a synthesized provider document is built from. A registered provider needs
/// a model entry; a built-in override does not.
struct EndpointDocument<'a> {
    base_url: &'a str,
    api: Option<&'a str>,
    model: Option<&'a str>,
    reasoning: bool,
}

/// A caller-owned gateway endpoint: the base URL, the wire protocol the caller named, the model
/// identifier the caller owns, and whether the binding asked for reasoning.
pub(super) struct GatewayDocument<'a> {
    pub(super) base_url: &'a str,
    pub(super) api: &'a str,
    pub(super) model: &'a str,
    pub(super) reasoning: bool,
}

/// Builds the provider document. The API key is read from the declared connection through Pi's
/// own `$NAME` interpolation, so the secret never reaches this file.
fn models_document(selection: ProviderOverride, endpoint: EndpointDocument<'_>) -> Value {
    let mut provider = serde_json::Map::new();
    provider.insert("baseUrl".to_owned(), json!(endpoint.base_url));
    if let Some(api) = endpoint.api {
        provider.insert("api".to_owned(), json!(api));
    }
    provider.insert(
        "apiKey".to_owned(),
        json!(format!("${}", selection.api_key_field)),
    );
    // A registered provider needs one model entry before it resolves any identifier. The entry
    // binds the caller's own identifier, and `reasoning` mirrors the binding's authored effort so
    // Pi does not silently discard it. Context window, output limit, and cost stay unset: Pi owns
    // those defaults rather than Zeroshot maintaining a catalog.
    if let Some(model) = endpoint.model {
        provider.insert(
            "models".to_owned(),
            json!([{
                "id": model,
                "name": model,
                "api": endpoint.api,
                "reasoning": endpoint.reasoning,
            }]),
        );
    }
    json!({ "providers": { selection.provider: Value::Object(provider) } })
}

/// Writes the provider document into the private agent directory for this turn.
///
/// The file is rewritten every turn so a resumed session cannot read a stale endpoint. It holds the
/// caller-owned base URL and an interpolation reference, never the secret itself.
pub(super) fn write_gateway(
    agent_dir: &Path,
    document: GatewayDocument<'_>,
) -> Result<PathBuf, PiProviderDocumentError> {
    validate_api(document.api)?;
    if document.model.trim().is_empty() {
        return Err(PiProviderDocumentError::EmptyModel);
    }
    write_document(
        agent_dir,
        models_document(
            GATEWAY,
            EndpointDocument {
                base_url: document.base_url,
                api: Some(document.api),
                model: Some(document.model),
                reasoning: document.reasoning,
            },
        ),
    )
}

/// Rejects an unsupported wire protocol before any child process starts. The same allowlist gates
/// the launch-time credential check, so the guarantee does not depend on which layer runs first.
pub(super) fn validate_api(api: &str) -> Result<(), PiProviderDocumentError> {
    if SUPPORTED_APIS.contains(&api) {
        Ok(())
    } else {
        Err(PiProviderDocumentError::UnsupportedApi {
            field: "GATEWAY_API",
        })
    }
}

/// Writes a built-in-provider endpoint override, which keeps that provider's own protocol and its
/// built-in model catalog.
pub(super) fn write_endpoint(
    agent_dir: &Path,
    selection: ProviderOverride,
    base_url: &str,
) -> Result<PathBuf, PiProviderDocumentError> {
    write_document(
        agent_dir,
        models_document(
            selection,
            EndpointDocument {
                base_url,
                api: None,
                model: None,
                reasoning: false,
            },
        ),
    )
}

fn write_document(agent_dir: &Path, document: Value) -> Result<PathBuf, PiProviderDocumentError> {
    let bytes = serde_json::to_vec_pretty(&document).map_err(PiProviderDocumentError::Serialize)?;
    let path = agent_dir.join(MODELS_FILE);
    std::fs::create_dir_all(agent_dir).map_err(PiProviderDocumentError::Write)?;
    // The agent directory is provider-private and mode 0700, so this caller-owned base URL is
    // visible only to that child. The file is created owner-only as well, matching the discipline
    // for every other provider-owned artifact.
    write_private(&path, &bytes).map_err(PiProviderDocumentError::Write)?;
    Ok(path)
}

/// Writes bytes with owner-only permissions on Unix. The agent directory already restricts access,
/// but the file holds a caller-owned endpoint and gets the same mode as other provider state.
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(bytes)
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, bytes)
    }
}

/// Resolves the gateway base URL from the declared connection, so a missing or invalid connection
/// fails before any child process starts.
pub(super) fn gateway_base_url(resolved: &ResolvedEnvironment) -> Result<String, NodeRunnerError> {
    gateway::connection(&declared(resolved)).map(|(base_url, _)| base_url.to_owned())
}

/// Resolves a caller-supplied endpoint override. An absent field means the provider's own endpoint
/// applies and no document is written.
pub(super) fn declared_base_url(resolved: &ResolvedEnvironment, field: &str) -> Option<String> {
    let value = resolved
        .iter()
        .find(|(name, _)| name.as_str() == field)?
        .1
        .trim();
    (!value.is_empty()).then(|| value.to_owned())
}

/// Reads the caller-owned wire protocol. A missing or unrecognized value is a usage failure rather
/// than a guess, because Pi has no way to report which protocol an opaque identifier needs.
pub(super) fn declared_api(
    resolved: &ResolvedEnvironment,
    field: &str,
) -> Result<String, NodeRunnerError> {
    let value = declared_base_url(resolved, field).ok_or_else(|| {
        NodeRunnerError::DriverDetail(format!(
            "Pi gateway requires {field} to name one of {}",
            SUPPORTED_APIS.join(", ")
        ))
    })?;
    if !SUPPORTED_APIS.contains(&value.as_str()) {
        return Err(NodeRunnerError::DriverDetail(format!(
            "Pi gateway does not support wire protocol {value}; expected one of {}",
            SUPPORTED_APIS.join(", ")
        )));
    }
    Ok(value)
}

fn declared(resolved: &ResolvedEnvironment) -> BTreeMap<String, String> {
    resolved
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), value.to_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        ANTHROPIC, EndpointDocument, GATEWAY, GatewayDocument, MODELS_FILE, OPENAI, SUPPORTED_APIS,
        declared_api, declared_base_url, gateway_base_url, models_document, write_endpoint,
        write_gateway,
    };
    use crate::native_v2_candidate::test_support::{TestDirectory, environment_name};
    use crate::native_v2_contract::{
        DeclaredConnections, DeclaredEnvironment, EnvironmentVariableName, NodeRuntimeBinding,
    };
    use crate::native_v2_runner::ResolvedEnvironment;
    use std::collections::BTreeMap;

    /// A resolved environment carrying exactly the supplied values, through the declared-connection
    /// path the runner uses.
    fn resolved(fields: &[&str], values: &[(&str, &str)]) -> ResolvedEnvironment {
        let names = fields
            .iter()
            .map(|name| environment_name(name))
            .collect::<Vec<_>>();
        let binding = NodeRuntimeBinding::Agent {
            model: crate::worker_catalog::ModelId::new("provider-model").unwrap(),
            effort: None,
            session_scope: crate::execution::SessionScope::Execution,
            connections: if names.is_empty() {
                DeclaredConnections::empty()
            } else {
                DeclaredConnections::single("provider", DeclaredEnvironment::new(names).unwrap())
                    .unwrap()
            },
        };
        let mut map: BTreeMap<EnvironmentVariableName, String> = BTreeMap::new();
        for (name, value) in values {
            map.insert(
                EnvironmentVariableName::new(*name).unwrap(),
                (*value).to_owned(),
            );
        }
        ResolvedEnvironment::exact(&binding, map).unwrap()
    }

    fn gateway_connection(api: Option<&str>) -> ResolvedEnvironment {
        let mut fields = vec!["GATEWAY_BASE_URL", "GATEWAY_API_KEY"];
        let mut values = vec![
            ("GATEWAY_BASE_URL", "https://gateway.example/api/v1"),
            ("GATEWAY_API_KEY", "sk-gateway"),
        ];
        if let Some(api) = api {
            fields.push("GATEWAY_API");
            values.push(("GATEWAY_API", api));
        }
        resolved(&fields, &values)
    }

    #[test]
    fn the_gateway_registers_its_own_provider_with_the_caller_protocol() {
        // Pi honors `api` on a registered provider and ignores it on a built-in one, so the lane
        // registers `zeroshot-gateway` rather than retargeting `openai`.
        let document = models_document(
            GATEWAY,
            EndpointDocument {
                base_url: "https://gateway.example/api/v1",
                api: Some("openai-completions"),
                model: Some("provider-model"),
                reasoning: true,
            },
        );
        let provider = &document["providers"][GATEWAY.provider];
        assert_eq!(provider["api"], "openai-completions");
        assert_eq!(provider["baseUrl"], "https://gateway.example/api/v1");
        // The key stays in the declared connection, never in the synthesized file.
        assert_eq!(provider["apiKey"], "$GATEWAY_API_KEY");
        // The model entry binds the caller's identifier and nothing else, so Zeroshot owns no
        // capability metadata and no catalog.
        let models = provider["models"].as_array().unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0]["id"], "provider-model");
        assert_eq!(models[0]["api"], "openai-completions");
        assert!(models[0].get("contextWindow").is_none());
        assert!(models[0].get("cost").is_none());
        // Reasoning mirrors the binding's authored effort so Pi cannot silently discard it.
        assert_eq!(models[0]["reasoning"], true);
    }

    #[test]
    fn the_written_provider_file_holds_no_secret() {
        // The only credential reference is an interpolation of a constant field name, so the
        // caller's key never reaches disk even though the base URL does.
        let directory = TestDirectory::new("pi-gateway-secret");
        let agent_dir = directory.child("agent");
        write_gateway(
            &agent_dir,
            GatewayDocument {
                base_url: "https://gateway.example/api/v1",
                api: "openai-responses",
                model: "provider-model",
                reasoning: false,
            },
        )
        .unwrap();
        let written = directory.read("agent/models.json");
        assert!(written.contains("$GATEWAY_API_KEY"));
        assert!(!written.contains("sk-gateway"));
    }

    #[test]
    fn every_supported_protocol_is_accepted_and_nothing_else_is() {
        for api in SUPPORTED_APIS {
            assert_eq!(
                declared_api(&gateway_connection(Some(api)), "GATEWAY_API").unwrap(),
                api
            );
        }
        // A missing or unrecognized protocol is a usage failure, never a guess.
        assert!(declared_api(&gateway_connection(None), "GATEWAY_API").is_err());
        assert!(declared_api(&gateway_connection(Some("grpc")), "GATEWAY_API").is_err());
        assert!(declared_api(&gateway_connection(Some("  ")), "GATEWAY_API").is_err());
    }

    #[test]
    fn the_gateway_document_is_written_and_validated() {
        let directory = TestDirectory::new("pi-gateway-doc");
        let agent_dir = directory.child("agent");
        let path = write_gateway(
            &agent_dir,
            GatewayDocument {
                base_url: "https://gateway.example/api/v1",
                api: "anthropic-messages",
                model: "provider-model",
                reasoning: true,
            },
        )
        .unwrap();
        assert_eq!(path, agent_dir.join(MODELS_FILE));
        assert!(
            directory
                .read("agent/models.json")
                .contains("anthropic-messages")
        );

        // An unsupported protocol and an empty identifier fail before any child process starts.
        assert!(
            write_gateway(
                &agent_dir,
                GatewayDocument {
                    base_url: "https://x.invalid",
                    api: "grpc",
                    model: "m",
                    reasoning: false,
                },
            )
            .is_err()
        );
        assert!(
            write_gateway(
                &agent_dir,
                GatewayDocument {
                    base_url: "https://x.invalid",
                    api: "openai-responses",
                    model: "  ",
                    reasoning: false,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn the_written_provider_file_is_owner_only() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let directory = TestDirectory::new("pi-gateway-mode");
            let agent_dir = directory.child("agent");
            let path = write_gateway(
                &agent_dir,
                GatewayDocument {
                    base_url: "https://gateway.example/api/v1",
                    api: "openai-responses",
                    model: "provider-model",
                    reasoning: false,
                },
            )
            .unwrap();
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "the provider file must be owner-only");
        }
    }

    #[test]
    fn the_anthropic_document_keeps_pis_own_messages_protocol() {
        // This is the escape hatch Claude Code offers through `ANTHROPIC_BASE_URL`: retarget the
        // endpoint without choosing a wire protocol, because overriding one is ignored anyway.
        let document = models_document(
            ANTHROPIC,
            EndpointDocument {
                base_url: "https://messages.example",
                api: None,
                model: None,
                reasoning: false,
            },
        );
        let provider = &document["providers"]["anthropic"];
        assert_eq!(provider["baseUrl"], "https://messages.example");
        assert_eq!(provider["apiKey"], "$ANTHROPIC_API_KEY");
        assert!(
            provider.get("api").is_none(),
            "the Anthropic Messages protocol must stay the built-in one"
        );
        assert!(provider.get("models").is_none());
    }

    #[test]
    fn the_openai_document_keeps_pis_own_responses_protocol() {
        // The OpenAI lane retargets the endpoint and keeps the built-in protocol, so an endpoint
        // for this lane must speak Responses. The gateway lane is the one that can name a protocol.
        let document = models_document(
            OPENAI,
            EndpointDocument {
                base_url: "https://openai-proxy.example/v1",
                api: None,
                model: None,
                reasoning: false,
            },
        );
        let provider = &document["providers"]["openai"];
        assert_eq!(provider["baseUrl"], "https://openai-proxy.example/v1");
        assert_eq!(provider["apiKey"], "$OPENAI_API_KEY");
        assert!(provider.get("api").is_none());
        assert!(provider.get("models").is_none());
    }

    #[test]
    fn the_provider_file_lands_in_the_agent_directory() {
        assert_eq!(MODELS_FILE, "models.json");
        let directory = TestDirectory::new("pi-anthropic-file");
        let agent_dir = directory.child("agent");
        let path = write_endpoint(&agent_dir, ANTHROPIC, "https://messages.example").unwrap();
        assert_eq!(path, agent_dir.join("models.json"));
    }

    #[test]
    fn a_declared_endpoint_override_is_read_only_when_present_and_nonempty() {
        assert_eq!(
            declared_base_url(
                &resolved(
                    &["ANTHROPIC_BASE_URL"],
                    &[("ANTHROPIC_BASE_URL", "https://messages.example")]
                ),
                "ANTHROPIC_BASE_URL"
            )
            .as_deref(),
            Some("https://messages.example")
        );
        // An absent field means the provider's own endpoint applies and nothing is written.
        assert!(declared_base_url(&resolved(&[], &[]), "ANTHROPIC_BASE_URL").is_none());
        assert!(
            declared_base_url(
                &resolved(&["ANTHROPIC_BASE_URL"], &[("ANTHROPIC_BASE_URL", "   ")]),
                "ANTHROPIC_BASE_URL"
            )
            .is_none()
        );
    }

    #[test]
    fn a_complete_gateway_connection_resolves_its_base_url() {
        assert_eq!(
            gateway_base_url(&gateway_connection(Some("openai-responses"))).unwrap(),
            "https://gateway.example/api/v1"
        );
    }

    #[test]
    fn an_incomplete_gateway_connection_fails_before_launch() {
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
}
