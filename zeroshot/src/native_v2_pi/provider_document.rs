//! Synthesized `models.json` for Pi lanes that carry a caller-owned endpoint.
//!
//! Pi reads its provider catalog from this file. Two behaviors decide its shape, and both were
//! confirmed against the CLI rather than inferred: Pi ignores `api`, `baseUrl`, and `apiKey`
//! overrides on a *built-in* provider, and it honours all three on a provider this file
//! registers. That is why the `gateway` lane registers its own provider instead of retargeting
//! `openai`, which is what lets a caller choose the wire protocol instead of being pinned to one.
//!
//! The registered model entry carries only the caller's own identifier, the chosen protocol, and a
//! display name. Capability metadata is deliberately absent so Zeroshot never becomes a model
//! catalog: Pi fills in its own defaults for context window, output limit, and cost.

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
/// the one Pi speaks. No discriminant is written: overriding one on a built-in provider is ignored.
pub(super) const ANTHROPIC: ProviderOverride = ProviderOverride {
    provider: "anthropic",
    api_key_field: "ANTHROPIC_API_KEY",
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

/// Builds the provider document. The API key is read from the declared connection through Pi's
/// own `$NAME` interpolation, so the secret never reaches this file.
fn models_document(
    selection: ProviderOverride,
    base_url: &str,
    api: Option<&str>,
    model: Option<&str>,
) -> Value {
    let mut provider = serde_json::Map::new();
    provider.insert("baseUrl".to_owned(), json!(base_url));
    if let Some(api) = api {
        provider.insert("api".to_owned(), json!(api));
    }
    provider.insert(
        "apiKey".to_owned(),
        json!(format!("${}", selection.api_key_field)),
    );
    // A registered provider needs one model entry before it resolves any identifier. The entry
    // binds the caller's own identifier and nothing else.
    if let Some(model) = model {
        provider.insert(
            "models".to_owned(),
            json!([{ "id": model, "name": model, "api": api }]),
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
    base_url: &str,
    api: &str,
    model: &str,
) -> Result<PathBuf, PiProviderDocumentError> {
    if !SUPPORTED_APIS.contains(&api) {
        return Err(PiProviderDocumentError::UnsupportedApi {
            field: "GATEWAY_API",
        });
    }
    if model.trim().is_empty() {
        return Err(PiProviderDocumentError::EmptyModel);
    }
    write_document(
        agent_dir,
        models_document(GATEWAY, base_url, Some(api), Some(model)),
    )
}

/// Writes the `anthropic` endpoint override, which keeps Pi's own Messages protocol.
pub(super) fn write_anthropic(
    agent_dir: &Path,
    base_url: &str,
) -> Result<PathBuf, PiProviderDocumentError> {
    write_document(agent_dir, models_document(ANTHROPIC, base_url, None, None))
}

fn write_document(agent_dir: &Path, document: Value) -> Result<PathBuf, PiProviderDocumentError> {
    let bytes = serde_json::to_vec_pretty(&document).map_err(PiProviderDocumentError::Serialize)?;
    let path = agent_dir.join(MODELS_FILE);
    std::fs::create_dir_all(agent_dir).map_err(PiProviderDocumentError::Write)?;
    // The agent directory is provider-private and mode 0700, so this caller-owned base URL is
    // visible only to that child.
    std::fs::write(&path, bytes).map_err(PiProviderDocumentError::Write)?;
    Ok(path)
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
        ANTHROPIC, GATEWAY, MODELS_FILE, SUPPORTED_APIS, declared_api, declared_base_url,
        gateway_base_url, models_document, write_anthropic, write_gateway,
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
            "https://gateway.example/api/v1",
            Some("openai-completions"),
            Some("provider-model"),
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
            "https://gateway.example/api/v1",
            "anthropic-messages",
            "provider-model",
        )
        .unwrap();
        assert_eq!(path, agent_dir.join(MODELS_FILE));
        assert!(
            directory
                .read("agent/models.json")
                .contains("anthropic-messages")
        );

        // An unsupported protocol and an empty identifier fail before any child process starts.
        assert!(write_gateway(&agent_dir, "https://x.invalid", "grpc", "m").is_err());
        assert!(write_gateway(&agent_dir, "https://x.invalid", "openai-responses", "  ").is_err());
    }

    #[test]
    fn the_anthropic_document_keeps_pis_own_messages_protocol() {
        // This is the escape hatch Claude Code offers through `ANTHROPIC_BASE_URL`: retarget the
        // endpoint without choosing a wire protocol, because overriding one is ignored anyway.
        let document = models_document(ANTHROPIC, "https://messages.example", None, None);
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
    fn the_provider_file_lands_in_the_agent_directory() {
        assert_eq!(MODELS_FILE, "models.json");
        let directory = TestDirectory::new("pi-anthropic-file");
        let agent_dir = directory.child("agent");
        let path = write_anthropic(&agent_dir, "https://messages.example").unwrap();
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
