//! Hetzner Cloud [`Provisioner`] over the public API (`https://api.hetzner.cloud/v1`).
//!
//! The API token comes from `--hetzner-token-file` or `HCLOUD_TOKEN`; it is sent only in the
//! `Authorization` header, never logged, labelled, put in user data or stored in hub state.
//! Every response's `RateLimit-Remaining` and `RateLimit-Reset` headers are kept, and no request
//! is sent while the hourly budget (3600 requests) is spent.
//!
//! Every failed call is classified ([`classify`]) so the pool can tell a capacity shortage
//! (try another location or type, then wait) from an account limit or a bad request (stop)
//! and from a transient failure (retry).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::error::HubError;
use super::pool::{Resource, ResourceKind};
use super::provision::{
    Architecture, Catalog, Created, Failure, Location, Progress, Provisioner, Refusal,
    ServerRequest, ServerType,
};

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

    /// One API call. `Ok(None)` is a 404; every other non-success is a classified failure.
    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Option<Value>, Failure> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        let limit = self.rate_limit();
        if limit.remaining == Some(0) && limit.reset.is_some_and(|reset| reset > now) {
            return Err(Failure::new(
                Refusal::RateLimited,
                format!(
                    "Hetzner rate limit is spent until Unix second {}",
                    limit.reset.unwrap_or_default()
                ),
            ));
        }
        let mut request = self
            .client
            .request(method.clone(), format!("{}{path}", self.base))
            .bearer_auth(&self.token.0);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|error| {
            Failure::new(
                Refusal::Transient,
                format!(
                    "Hetzner {method} {path} was not answered: {}",
                    error.without_url()
                ),
            )
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
            Failure::new(
                classify(status.as_u16(), "", ""),
                format!(
                    "Hetzner {method} {path} returned HTTP {} without JSON: {}",
                    status.as_u16(),
                    error.without_url()
                ),
            )
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
            return Err(Failure::new(
                classify(status.as_u16(), code, message),
                format!(
                    "Hetzner {method} {path} failed with HTTP {} {code}: {message}",
                    status.as_u16()
                ),
            ));
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
        let query = format!("label_selector={}&", encode(selector));
        self.pages(collection, &query)
            .await?
            .iter()
            .map(|item| resource(kind, item))
            .collect()
    }

    /// Every item of a paginated collection; `query` is empty or ends with `&`.
    async fn pages(&self, collection: &str, query: &str) -> Result<Vec<Value>, HubError> {
        let mut found = Vec::new();
        let mut page = 1_u64;
        loop {
            let path = format!("/{collection}?{query}page={page}&per_page={PAGE_SIZE}");
            let document = self
                .call(Method::GET, &path, None)
                .await?
                .ok_or_else(|| HubError::Provider(format!("Hetzner GET /{collection} is 404")))?;
            found.extend(
                document
                    .get(collection)
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .cloned(),
            );
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

/// What a failed call means, from its HTTP status and Hetzner error code. A server type the
/// location does not offer, or no longer has room for, is a capacity shortage like
/// `resource_unavailable` and `placement_error`, so the next location or type is tried.
pub(crate) fn classify(status: u16, code: &str, message: &str) -> Refusal {
    let lowered = message.to_ascii_lowercase();
    let not_offered_here = [
        "not available",
        "unavailable",
        "unsupported location",
        "sold out",
    ]
    .iter()
    .any(|phrase| lowered.contains(phrase));
    match code {
        "resource_unavailable" | "placement_error" | "unavailable" | "maintenance" => {
            Refusal::Capacity
        }
        "invalid_input" | "invalid_server_type" | "unsupported_error" if not_offered_here => {
            Refusal::Capacity
        }
        "resource_limit_exceeded"
        | "server_limit_exceeded"
        | "limit_exceeded"
        | "forbidden"
        | "unauthorized"
        | "token_readonly" => Refusal::Quota,
        "rate_limit_exceeded" => Refusal::RateLimited,
        "invalid_input"
        | "invalid_server_type"
        | "unsupported_error"
        | "json_error"
        | "not_found"
        | "deprecated_api_endpoint" => Refusal::Invalid,
        _ => match status {
            429 => Refusal::RateLimited,
            401 | 403 => Refusal::Quota,
            400..=499 if !matches!(status, 408 | 409 | 423) => Refusal::Invalid,
            _ => Refusal::Transient,
        },
    }
}

fn server_type(item: &Value) -> Option<(u64, ServerType)> {
    let number = |key: &str| item.get(key).and_then(Value::as_u64);
    let hourly = item
        .get("prices")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|price| {
            Some((
                price.get("location")?.as_str()?.to_owned(),
                price.pointer("/price_hourly/gross")?.as_str()?.to_owned(),
            ))
        })
        .collect();
    let memory_gb = item.get("memory").and_then(Value::as_f64).unwrap_or(0.0);
    let deprecated = item.get("deprecated").and_then(Value::as_bool) == Some(true)
        || item
            .get("deprecation")
            .is_some_and(|value| !value.is_null());
    Some((
        number("id")?,
        ServerType {
            name: item.get("name")?.as_str()?.to_owned(),
            cores: u32::try_from(number("cores")?).ok()?,
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            memory_mb: (memory_gb * 1024.0).round().max(0.0) as u64,
            disk_gb: number("disk").unwrap_or(0),
            architecture: if item.get("architecture").and_then(Value::as_str) == Some("arm") {
                Architecture::Arm
            } else {
                Architecture::X86
            },
            deprecated,
            hourly,
        },
    ))
}

/// Locations from datacenters: a location offers, and has room for, the union of its
/// datacenters' server types.
fn locations(datacenters: &[Value], names: &BTreeMap<u64, String>) -> Vec<Location> {
    let mut found: Vec<Location> = Vec::new();
    for datacenter in datacenters {
        let Some(name) = datacenter.pointer("/location/name").and_then(Value::as_str) else {
            continue;
        };
        let types = |key: &str| {
            datacenter
                .pointer(&format!("/server_types/{key}"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|id| names.get(&id.as_u64()?).cloned())
                .collect::<BTreeSet<_>>()
        };
        let index = found
            .iter()
            .position(|location| location.name == name)
            .unwrap_or_else(|| {
                found.push(Location {
                    name: name.to_owned(),
                    supported: BTreeSet::new(),
                    available: BTreeSet::new(),
                });
                found.len() - 1
            });
        let location = &mut found[index];
        location.supported.extend(types("supported"));
        location.available.extend(types("available"));
    }
    found
}

impl Hetzner {
    /// An unformatted Volume beside the server, carrying the lease's labels: the boot script
    /// formats it btrfs.
    async fn create_volume(&self, request: &ServerRequest, size: u32) -> Result<u64, Failure> {
        self.call(
            Method::POST,
            "/volumes",
            Some(&json!({
                "name": request.name,
                "size": size,
                "location": request.location,
                "labels": request.labels,
                "automount": false,
            })),
        )
        .await?
        .and_then(|document| document.pointer("/volume/id").and_then(Value::as_u64))
        .ok_or_else(|| Failure::new(Refusal::Transient, "Hetzner created no volume"))
    }

    /// Delete what a failed create made, the Volume before the firewall. Anything still
    /// attached to a server that was created after all is refused here; it carries the lease's
    /// labels, so the lease's cleanup or the reaper removes it.
    async fn undo(&self, mut failure: Failure, volume: Option<u64>, firewall: u64) -> Failure {
        let made = volume
            .map(|id| ("volume", "volumes", id))
            .into_iter()
            .chain([("firewall", "firewalls", firewall)]);
        for (kind, collection, id) in made {
            if let Err(left) = self
                .call(Method::DELETE, &format!("/{collection}/{id}"), None)
                .await
            {
                failure.message =
                    format!("{}; {kind} {id} left for cleanup: {left}", failure.message);
            }
        }
        failure
    }
}

const fn collection(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Server => "servers",
        ResourceKind::Volume => "volumes",
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
    async fn create(&self, request: &ServerRequest) -> Result<Created, Failure> {
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
            .ok_or_else(|| Failure::new(Refusal::Transient, "Hetzner created no firewall"))?;
        let volume = match request.volume_gb {
            Some(size) => match self.create_volume(request, size).await {
                Ok(volume) => Some(volume),
                Err(failure) => return Err(self.undo(failure, None, firewall).await),
            },
            None => None,
        };
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
                    "volumes": volume.into_iter().collect::<Vec<_>>(),
                    "automount": false,
                    "start_after_create": true,
                    "public_net": { "enable_ipv4": true, "enable_ipv6": true },
                })),
            )
            .await;
        // The response may carry a root password; only IDs are read from it.
        let document = match created {
            Ok(Some(document)) => document,
            outcome => {
                let failure = outcome.err().unwrap_or_else(|| {
                    Failure::new(Refusal::Invalid, "Hetzner POST /servers is 404")
                });
                return Err(self.undo(failure, volume, firewall).await);
            }
        };
        let server_id = document
            .pointer("/server/id")
            .and_then(Value::as_u64)
            .ok_or_else(|| Failure::new(Refusal::Transient, "Hetzner created no server ID"))?;
        Ok(Created {
            server_id,
            firewall_id: Some(firewall),
            volume_id: volume,
            action_id: document.pointer("/action/id").and_then(Value::as_u64),
        })
    }

    async fn catalog(&self) -> Result<Catalog, HubError> {
        let mut names = BTreeMap::new();
        let mut types = Vec::new();
        for item in self.pages("server_types", "").await? {
            if let Some((id, server_type)) = server_type(&item) {
                names.insert(id, server_type.name.clone());
                types.push(server_type);
            }
        }
        let datacenters = self.pages("datacenters", "").await?;
        Ok(Catalog {
            types,
            locations: locations(&datacenters, &names),
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
            ResourceKind::Volume,
            ResourceKind::PrimaryIp,
            ResourceKind::Firewall,
        ] {
            found.extend(self.list(kind, selector).await?);
        }
        Ok(found)
    }
}
