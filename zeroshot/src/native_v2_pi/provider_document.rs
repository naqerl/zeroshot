//! Synthesized `models.json` for Pi lanes that carry a caller-owned endpoint.
//!
//! Pi resolves a compatible endpoint by overriding only `baseUrl`, `apiKey`, and optionally the
//! `api` discriminant of an existing provider, which leaves every built-in model of that provider
//! registered. That is what keeps the caller-owned model identifiers resolvable without Zeroshot
//! owning a model catalog: an unmatched identifier becomes a custom model id on that provider.
//!
//! This is Pi's equivalent of Claude Code's `ANTHROPIC_BASE_URL`. Pi does not read that variable
//! for endpoint selection, so a caller-owned Anthropic Messages endpoint has to arrive through this
//! file. The wire protocol is never chosen here: each lane keeps the built-in protocol of the Pi
//! provider it overrides, and only the `gateway` lane pins a discriminant, because that lane exists
//! to carry an OpenAI Responses endpoint the way `codex/gateway` does.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::native_v2_capsule::gateway;
use crate::native_v2_runner::{NodeRunnerError, ResolvedEnvironment};

/// Pi's provider file inside the private agent directory.
pub(super) const MODELS_FILE: &str = "models.json";

/// One provider override: which Pi provider to retarget, which endpoint it speaks, and which
/// environment variable holds its key.
#[derive(Clone, Copy)]
pub(super) struct ProviderOverride {
    /// Pi's own provider name, which is also the wire protocol it speaks.
    pub(super) provider: &'static str,
    /// Environment field the API key is read from, referenced by Pi's `$NAME` interpolation.
    pub(super) api_key_field: &'static str,
    /// Discriminant to pin. Omitted lanes keep the built-in protocol of the overridden provider.
    pub(super) pin_api: Option<&'static str>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PiProviderDocumentError {
    #[error("Pi provider configuration could not be serialized: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("Pi provider configuration could not be created: {0}")]
    Write(#[source] std::io::Error),
}

/// Builds the provider document. The API key is read from the declared connection through Pi's
/// own `$NAME` interpolation, so the secret never reaches this file.
fn models_document(selection: ProviderOverride, base_url: &str) -> Value {
    let mut provider = serde_json::Map::new();
    provider.insert("baseUrl".to_owned(), json!(base_url));
    if let Some(api) = selection.pin_api {
        provider.insert("api".to_owned(), json!(api));
    }
    provider.insert(
        "apiKey".to_owned(),
        json!(format!("${}", selection.api_key_field)),
    );
    json!({ "providers": { selection.provider: Value::Object(provider) } })
}

/// Writes the provider document into the private agent directory for this turn.
///
/// The file is rewritten every turn so a resumed session cannot read a stale endpoint. It holds the
/// caller-owned base URL and an interpolation reference, never the secret itself.
pub(super) fn write_models(
    agent_dir: &Path,
    selection: ProviderOverride,
    base_url: &str,
) -> Result<PathBuf, PiProviderDocumentError> {
    let bytes = serde_json::to_vec_pretty(&models_document(selection, base_url))
        .map_err(PiProviderDocumentError::Serialize)?;
    let path = agent_dir.join(MODELS_FILE);
    std::fs::create_dir_all(agent_dir).map_err(PiProviderDocumentError::Write)?;
    // The agent directory is provider-private and mode 0700, so this caller-owned base URL is
    // visible only to that child.
    std::fs::write(&path, bytes).map_err(PiProviderDocumentError::Write)?;
    Ok(path)
}

/// The `gateway` lane retargets Pi's built-in OpenAI provider and pins the Responses discriminant,
/// matching the lane `codex/gateway` already uses.
pub(super) const GATEWAY: ProviderOverride = ProviderOverride {
    provider: "openai",
    api_key_field: gateway::API_KEY,
    pin_api: Some("openai-responses"),
};

/// The `anthropic` lane keeps Pi's own Anthropic Messages protocol and only retargets the endpoint,
/// which is what a caller-owned Messages-compatible gateway needs.
pub(super) const ANTHROPIC: ProviderOverride = ProviderOverride {
    provider: "anthropic",
    api_key_field: "ANTHROPIC_API_KEY",
    pin_api: None,
};

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

fn declared(resolved: &ResolvedEnvironment) -> BTreeMap<String, String> {
    resolved
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), value.to_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        ANTHROPIC, GATEWAY, MODELS_FILE, declared_base_url, gateway_base_url, models_document,
        write_models,
    };
    use crate::native_v2_candidate::test_support::{TestDirectory, environment_name};
    use crate::native_v2_contract::{
        DeclaredConnections, DeclaredEnvironment, EnvironmentVariableName, NodeRuntimeBinding,
    };
    use crate::native_v2_runner::ResolvedEnvironment;
    use serde_json::json;
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

    #[test]
    fn the_gateway_document_pins_responses_and_interpolates_the_key() {
        let document = models_document(GATEWAY, "https://gateway.example/api/v1");
        let provider = &document["providers"]["openai"];
        assert_eq!(provider["api"], "openai-responses");
        assert_eq!(provider["baseUrl"], "https://gateway.example/api/v1");
        // The key stays in the declared connection, never in the synthesized file.
        assert_eq!(provider["apiKey"], "$GATEWAY_API_KEY");
        // No model list is synthesized: Pi keeps the built-in openai catalog.
        assert!(provider.get("models").is_none());
    }

    #[test]
    fn the_anthropic_document_keeps_pis_own_messages_protocol() {
        // This is the escape hatch Claude Code offers through `ANTHROPIC_BASE_URL`: retarget the
        // endpoint without choosing a wire protocol, because Pi ignores an `api` override on a
        // built-in provider anyway.
        let document = models_document(ANTHROPIC, "https://messages.example");
        let provider = &document["providers"]["anthropic"];
        assert_eq!(provider["baseUrl"], "https://messages.example");
        assert_eq!(provider["apiKey"], "$ANTHROPIC_API_KEY");
        assert!(
            provider.get("api").is_none(),
            "the Anthropic Messages protocol must stay the built-in one"
        );
    }

    #[test]
    fn the_provider_file_lands_in_the_agent_directory() {
        assert_eq!(MODELS_FILE, "models.json");
        let directory = TestDirectory::new("pi-provider-file");
        let agent_dir = directory.child("agent");
        let path = write_models(&agent_dir, GATEWAY, "https://gateway.example/api/v1").unwrap();
        assert_eq!(path, agent_dir.join("models.json"));
        assert!(
            directory
                .read("agent/models.json")
                .contains("openai-responses")
        );
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
        let complete = resolved(
            &["GATEWAY_BASE_URL", "GATEWAY_API_KEY"],
            &[
                ("GATEWAY_BASE_URL", "https://gateway.example/api/v1"),
                ("GATEWAY_API_KEY", "sk-gateway"),
            ],
        );
        assert_eq!(
            gateway_base_url(&complete).unwrap(),
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

    #[test]
    fn a_gateway_document_is_valid_json_for_pi() {
        let document = models_document(GATEWAY, "https://gateway.example/api/v1");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&document.to_string()).unwrap(),
            json!({"providers": {"openai": {
                "baseUrl": "https://gateway.example/api/v1",
                "api": "openai-responses",
                "apiKey": "$GATEWAY_API_KEY"
            }}})
        );
    }
}
