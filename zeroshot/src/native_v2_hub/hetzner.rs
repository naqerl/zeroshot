//! Hetzner Cloud [`Provisioner`] over the public API (`https://api.hetzner.cloud/v1`).
//!
//! The API token comes from `--hetzner-token-file` or `HCLOUD_TOKEN`; it is sent only in the
//! `Authorization` header, never logged, labelled, put in user data or stored in hub state.
//! Every response's `RateLimit-Remaining` and `RateLimit-Reset` headers are kept, and no request
//! is sent while the hourly budget (3600 requests) is spent.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::error::HubError;
use super::pool::{Resource, ResourceKind};
use super::provision::{Created, Progress, Provisioner, ServerRequest};

pub(crate) const API: &str = "https://api.hetzner.cloud/v1";
/// Environment variable holding the token when no token file is configured.
pub(crate) const TOKEN_ENV: &str = "HCLOUD_TOKEN";
const PAGE_SIZE: u32 = 50;

/// A provider token that never prints.
pub(crate) struct Token(String);

impl std::fmt::Debug for Token {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Token(<redacted>)")
    }
}

impl Token {
    /// The token from `file`, or else from `HCLOUD_TOKEN`; `None` when neither is set.
    pub(crate) fn load(file: Option<&Path>) -> Result<Option<Self>, HubError> {
        if let Some(file) = file {
            let text = std::fs::read_to_string(file).map_err(|error| {
                HubError::storage(&format!("{} could not be read", file.display()), error)
            })?;
            return Self::parse(&text).map(Some);
        }
        match std::env::var(TOKEN_ENV) {
            Ok(text) if !text.trim().is_empty() => Self::parse(&text).map(Some),
            _ => Ok(None),
        }
    }

    pub(crate) fn parse(text: &str) -> Result<Self, HubError> {
        let token = text.trim();
        if token.is_empty() || !token.chars().all(|character| character.is_ascii_graphic()) {
            return Err(HubError::Usage(
                "the Hetzner token must be one non-empty line".to_owned(),
            ));
        }
        Ok(Self(token.to_owned()))
    }
}

/// Last rate-limit headers seen.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RateLimit {
    pub remaining: Option<u64>,
    /// Unix second the budget refills.
    pub reset: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct Hetzner {
    base: String,
    token: Token,
    client: reqwest::Client,
    limit: Mutex<RateLimit>,
}

impl Hetzner {
    pub(crate) fn new(base: &str, token: Token) -> Result<Self, HubError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("zeroshot-hub/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| HubError::storage("HTTP client could not be initialized", error))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_owned(),
            token,
            client,
            limit: Mutex::new(RateLimit::default()),
        })
    }

    pub(crate) fn rate_limit(&self) -> RateLimit {
        *self
            .limit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// One API call. `Ok(None)` is a 404; every other non-success is a typed error.
    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Option<Value>, HubError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        let limit = self.rate_limit();
        if limit.remaining == Some(0) && limit.reset.is_some_and(|reset| reset > now) {
            return Err(HubError::Provider(format!(
                "Hetzner rate limit is spent until Unix second {}",
                limit.reset.unwrap_or_default()
            )));
        }
        let mut request = self
            .client
            .request(method.clone(), format!("{}{path}", self.base))
            .bearer_auth(&self.token.0);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|error| {
            HubError::Provider(format!(
                "Hetzner {method} {path} was not answered: {}",
                error.without_url()
            ))
        })?;
        self.remember(response.headers());
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status == StatusCode::NO_CONTENT {
            return Ok(Some(Value::Null));
        }
        let document = response.json::<Value>().await.map_err(|error| {
            HubError::Provider(format!(
                "Hetzner {method} {path} returned HTTP {} without JSON: {}",
                status.as_u16(),
                error.without_url()
            ))
        })?;
        if !status.is_success() {
            let code = document
                .pointer("/error/code")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let message = document
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("");
            return Err(HubError::Provider(format!(
                "Hetzner {method} {path} failed with HTTP {} {code}: {message}",
                status.as_u16()
            )));
        }
        Ok(Some(document))
    }

    fn remember(&self, headers: &reqwest::header::HeaderMap) {
        let number = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok())
        };
        let mut limit = self
            .limit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(remaining) = number("ratelimit-remaining") {
            limit.remaining = Some(remaining);
        }
        if let Some(reset) = number("ratelimit-reset") {
            limit.reset = Some(reset);
        }
    }

    async fn list(&self, kind: ResourceKind, selector: &str) -> Result<Vec<Resource>, HubError> {
        let collection = collection(kind);
        let mut found = Vec::new();
        let mut page = 1_u64;
        loop {
            let path = format!(
                "/{collection}?label_selector={}&page={page}&per_page={PAGE_SIZE}",
                encode(selector)
            );
            let document = self
                .call(Method::GET, &path, None)
                .await?
                .ok_or_else(|| HubError::Provider(format!("Hetzner GET /{collection} is 404")))?;
            for item in document
                .get(collection)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                found.push(resource(kind, item)?);
            }
            match document
                .pointer("/meta/pagination/next_page")
                .and_then(Value::as_u64)
            {
                Some(next) if next > page => page = next,
                _ => return Ok(found),
            }
        }
    }
}

const fn collection(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Server => "servers",
        ResourceKind::PrimaryIp => "primary_ips",
        ResourceKind::Firewall => "firewalls",
    }
}

fn resource(kind: ResourceKind, item: &Value) -> Result<Resource, HubError> {
    let id = item
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| HubError::Provider(format!("a Hetzner {} has no ID", collection(kind))))?;
    let labels = item
        .get("labels")
        .and_then(Value::as_object)
        .map(|labels| {
            labels
                .iter()
                .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_owned())))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    Ok(Resource {
        kind,
        id,
        name: item
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        labels,
    })
}

/// Percent-encode a label selector for a query string.
fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            byte => format!("%{byte:02X}"),
        })
        .collect()
}

fn action_progress(document: &Value) -> Progress {
    match document.pointer("/action/status").and_then(Value::as_str) {
        Some("success") => Progress::Done(None),
        Some("error") => Progress::Failed(
            document
                .pointer("/action/error/message")
                .and_then(Value::as_str)
                .unwrap_or("the action failed")
                .to_owned(),
        ),
        _ => Progress::Pending,
    }
}

#[async_trait]
impl Provisioner for Hetzner {
    async fn create(&self, request: &ServerRequest) -> Result<Created, HubError> {
        let firewall = self
            .call(
                Method::POST,
                "/firewalls",
                Some(&json!({
                    "name": request.name,
                    "labels": request.labels,
                    "rules": [{
                        "direction": "in",
                        "protocol": "tcp",
                        "port": "22",
                        "source_ips": request.ssh_from,
                        "description": "SSH from the zeroshot hub only",
                    }],
                })),
            )
            .await?
            .and_then(|document| document.pointer("/firewall/id").and_then(Value::as_u64))
            .ok_or_else(|| HubError::Provider("Hetzner created no firewall".to_owned()))?;
        let created = self
            .call(
                Method::POST,
                "/servers",
                Some(&json!({
                    "name": request.name,
                    "server_type": request.server_type,
                    "location": request.location,
                    "image": request.image,
                    "user_data": request.user_data,
                    "labels": request.labels,
                    "firewalls": [{ "firewall": firewall }],
                    "start_after_create": true,
                    "public_net": { "enable_ipv4": true, "enable_ipv6": true },
                })),
            )
            .await;
        // The response may carry a root password; only IDs are read from it.
        let document = match created {
            Ok(Some(document)) => document,
            outcome => {
                // Best effort: a firewall still attached to a server that was created after
                // all is refused here and removed with its lease or by the reaper.
                let _ = self
                    .call(Method::DELETE, &format!("/firewalls/{firewall}"), None)
                    .await;
                return Err(outcome.err().unwrap_or_else(|| {
                    HubError::Provider("Hetzner POST /servers is 404".to_owned())
                }));
            }
        };
        let server_id = document
            .pointer("/server/id")
            .and_then(Value::as_u64)
            .ok_or_else(|| HubError::Provider("Hetzner created no server ID".to_owned()))?;
        Ok(Created {
            server_id,
            firewall_id: Some(firewall),
            action_id: document.pointer("/action/id").and_then(Value::as_u64),
        })
    }

    async fn wait_ready(
        &self,
        server_id: u64,
        action_id: Option<u64>,
    ) -> Result<Progress, HubError> {
        if let Some(action_id) = action_id {
            match self.action(action_id).await? {
                Progress::Done(_) => {}
                other => return Ok(other),
            }
        }
        let Some(document) = self
            .call(Method::GET, &format!("/servers/{server_id}"), None)
            .await?
        else {
            return Ok(Progress::Failed(format!(
                "server {server_id} no longer exists"
            )));
        };
        if document.pointer("/server/status").and_then(Value::as_str) != Some("running") {
            return Ok(Progress::Pending);
        }
        Ok(Progress::Done(
            document
                .pointer("/server/public_net/ipv4/ip")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ))
    }

    async fn destroy(&self, resource: &Resource) -> Result<Option<u64>, HubError> {
        let path = format!("/{}/{}", collection(resource.kind), resource.id);
        Ok(self
            .call(Method::DELETE, &path, None)
            .await?
            .and_then(|document| document.pointer("/action/id").and_then(Value::as_u64)))
    }

    async fn action(&self, action_id: u64) -> Result<Progress, HubError> {
        self.call(Method::GET, &format!("/actions/{action_id}"), None)
            .await?
            .map_or_else(
                || Ok(Progress::Failed(format!("action {action_id} is unknown"))),
                |document| Ok(action_progress(&document)),
            )
    }

    async fn list_by_label(&self, selector: &str) -> Result<Vec<Resource>, HubError> {
        let mut found = Vec::new();
        for kind in [
            ResourceKind::Server,
            ResourceKind::PrimaryIp,
            ResourceKind::Firewall,
        ] {
            found.extend(self.list(kind, selector).await?);
        }
        Ok(found)
    }
}
