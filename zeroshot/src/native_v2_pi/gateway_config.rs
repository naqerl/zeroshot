//! Synthesized `models.json` for the Pi gateway lane.
//!
//! Pi resolves a compatible endpoint by overriding only `baseUrl`, `apiKey`, and the `api`
//! discriminant of an existing provider, which leaves every built-in model of that provider
//! registered. That is what keeps the caller's model identifiers resolvable without Zeroshot
//! owning a model catalog: an unmatched identifier becomes a custom model id on that provider.
//!
//! The file is written with Pi's `openai-responses` discriminant because that is the lane
//! `codex/gateway` already uses. Pinning it is an adapter-owned choice, never protocol detection.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::native_v2_capsule::gateway;
use crate::native_v2_runner::{NodeRunnerError, ResolvedEnvironment};

/// Pi's provider file inside the private agent directory.
pub(super) const MODELS_FILE: &str = "models.json";

#[derive(Debug, thiserror::Error)]
pub(super) enum PiGatewayConfigError {
    #[error("Pi gateway provider configuration could not be serialized: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("Pi gateway provider configuration could not be created: {0}")]
    Write(#[source] std::io::Error),
}

/// Builds the provider document. The API key is read from the declared connection through Pi's
/// own `$NAME` interpolation, so the secret never reaches this file.
fn models_document(base_url: &str) -> Value {
    json!({
        "providers": {
            "openai": {
                "baseUrl": base_url,
                "api": "openai-responses",
                "apiKey": "$GATEWAY_API_KEY"
            }
        }
    })
}

/// Writes the provider document into the private agent directory for this turn.
///
/// The file is rewritten every turn so a resumed session cannot read a stale endpoint. It holds the
/// caller-owned base URL and an interpolation reference, never the secret itself.
pub(super) fn write_models(
    agent_dir: &Path,
    base_url: &str,
) -> Result<PathBuf, PiGatewayConfigError> {
    let bytes = serde_json::to_vec_pretty(&models_document(base_url))
        .map_err(PiGatewayConfigError::Serialize)?;
    let path = agent_dir.join(MODELS_FILE);
    std::fs::create_dir_all(agent_dir).map_err(PiGatewayConfigError::Write)?;
    // The agent directory is provider-private and mode 0700, so this caller-owned base URL is
    // visible only to that child.
    std::fs::write(&path, bytes).map_err(PiGatewayConfigError::Write)?;
    Ok(path)
}

/// Resolves the gateway base URL from the declared connection, so a missing or invalid connection
/// fails before any child process starts.
pub(super) fn base_url(resolved: &ResolvedEnvironment) -> Result<String, NodeRunnerError> {
    gateway::connection(&declared(resolved)).map(|(base_url, _)| base_url.to_owned())
}

fn declared(resolved: &ResolvedEnvironment) -> BTreeMap<String, String> {
    resolved
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), value.to_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{MODELS_FILE, base_url, models_document, write_models};
    use crate::native_v2_candidate::test_support::{TestDirectory, environment_name};
    use crate::native_v2_contract::{
        DeclaredConnections, DeclaredEnvironment, EnvironmentVariableName, NodeRuntimeBinding,
    };
    use crate::native_v2_runner::ResolvedEnvironment;
    use serde_json::json;
    use std::collections::BTreeMap;

    /// A resolved environment carrying exactly the supplied gateway fields, through the same
    /// declared-connection path the runner uses.
    fn environment(values: &[(&str, &str)]) -> ResolvedEnvironment {
        let names = values
            .iter()
            .map(|(name, _)| environment_name(name))
            .collect::<Vec<_>>();
        let binding = NodeRuntimeBinding::Agent {
            model: crate::worker_catalog::ModelId::new("openai/gpt-5").unwrap(),
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
    fn the_document_pins_the_openai_responses_discriminant_and_interpolates_the_key() {
        let document = models_document("https://gateway.example/api/v1");
        let provider = &document["providers"]["openai"];
        assert_eq!(provider["api"], "openai-responses");
        assert_eq!(provider["baseUrl"], "https://gateway.example/api/v1");
        // The key must stay in the declared connection, never in the synthesized file.
        assert_eq!(provider["apiKey"], "$GATEWAY_API_KEY");
        // No model list is synthesized: Pi keeps the built-in openai catalog.
        assert!(provider.get("models").is_none());
    }

    #[test]
    fn the_provider_file_lands_in_the_agent_directory() {
        assert_eq!(MODELS_FILE, "models.json");
        let directory = TestDirectory::new("pi-gateway-file");
        let agent_dir = directory.child("agent");
        let path = write_models(&agent_dir, "https://gateway.example/api/v1").unwrap();
        assert_eq!(path, agent_dir.join("models.json"));
        assert!(
            directory
                .read("agent/models.json")
                .contains("openai-responses")
        );
    }

    #[test]
    fn a_complete_gateway_connection_resolves_its_base_url() {
        let resolved = environment(&[
            ("GATEWAY_BASE_URL", "https://gateway.example/api/v1"),
            ("GATEWAY_API_KEY", "sk-gateway"),
        ]);
        assert_eq!(
            base_url(&resolved).unwrap(),
            "https://gateway.example/api/v1"
        );
    }

    #[test]
    fn an_incomplete_gateway_connection_fails_before_launch() {
        assert!(
            base_url(&environment(&[(
                "GATEWAY_BASE_URL",
                "https://gateway.example"
            )]))
            .is_err()
        );
        assert!(base_url(&environment(&[("GATEWAY_API_KEY", "sk-gateway")])).is_err());
        assert!(
            base_url(&environment(&[
                ("GATEWAY_BASE_URL", ""),
                ("GATEWAY_API_KEY", "k")
            ]))
            .is_err()
        );
        assert!(base_url(&environment(&[])).is_err());
    }

    #[test]
    fn a_gateway_document_is_valid_json_for_pi() {
        let document = models_document("https://gateway.example/api/v1");
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
