//! Cloud servers for pool leases: the [`Provisioner`] contract, implemented by
//! [`super::hetzner::Hetzner`] and by a fake in the tests, and the cloud-init document every
//! server boots with.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::data;
use super::error::HubError;
use super::pool::{Lease, Pool, Resource};

/// Hetzner's limit on `user_data`.
pub(crate) const USER_DATA_LIMIT: usize = 32 * 1024;
/// Port the target listens on inside the server, published on its loopback only.
pub(crate) const TARGET_PORT: u16 = 8080;
/// Server folder bound to the target's `/var/lib/zeroshot` volume.
pub(crate) const DATA_FOLDER: &str = "/mnt/zs";

/// One server to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServerRequest {
    pub name: String,
    pub server_type: String,
    pub location: String,
    pub image: String,
    pub user_data: String,
    /// The pool's SSH public key, registered with the provider so its servers admit the hub.
    pub public_key: String,
    pub labels: BTreeMap<String, String>,
    /// Source CIDRs admitted on SSH; every other inbound port is closed.
    pub ssh_from: Vec<String>,
}

/// A created server; `action_id` is its creation action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Created {
    pub server_id: u64,
    pub firewall_id: Option<u64>,
    pub action_id: Option<u64>,
}

/// Progress of a created server or of a provider action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Progress {
    /// Running; carries the server's public IPv4 when known.
    Done(Option<String>),
    Pending,
    Failed(String),
}

/// What a refused or unanswered provider call means for the pool.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Refusal {
    /// No capacity for this server type in this location right now, or the type is not offered
    /// there: try the next location, then the next type.
    Capacity,
    /// An account limit or a permission: retrying cannot help, so the pool stops creating
    /// servers until `hub pool resume`.
    Quota,
    /// The provider rejected the request itself; the pool stops as for [`Self::Quota`].
    Invalid,
    /// The provider's rate limit, or the local request budget, is spent: retry with backoff.
    RateLimited,
    /// A server error, a timeout or no answer: retry with backoff.
    Transient,
}

/// A failed provider call with its classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    pub refusal: Refusal,
    pub message: String,
    /// The failure happened while registering the pool's SSH key, before any server existed.
    pub key_registration: bool,
}

impl Failure {
    pub(crate) fn new(refusal: Refusal, message: impl Into<String>) -> Self {
        Self {
            refusal,
            message: message.into(),
            key_registration: false,
        }
    }

    pub(crate) fn during_key_registration(mut self) -> Self {
        self.key_registration = true;
        self
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl From<Failure> for HubError {
    fn from(failure: Failure) -> Self {
        Self::Provider(failure.message)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Architecture {
    X86,
    Arm,
}

/// One server type the provider offers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServerType {
    pub name: String,
    pub cores: u32,
    pub memory_mb: u64,
    pub disk_gb: u64,
    pub architecture: Architecture,
    pub deprecated: bool,
    /// Gross hourly price by location name, as the provider writes it.
    pub hourly: BTreeMap<String, String>,
}

/// One location and the server types it offers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Location {
    pub name: String,
    /// Types the location offers at all.
    pub supported: BTreeSet<String>,
    /// Types the location can create right now.
    pub available: BTreeSet<String>,
}

/// The provider's server types and locations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Catalog {
    pub types: Vec<ServerType>,
    pub locations: Vec<Location>,
}

impl Catalog {
    pub(crate) fn server_type(&self, name: &str) -> Option<&ServerType> {
        self.types
            .iter()
            .find(|server_type| server_type.name == name)
    }

    pub(crate) fn location(&self, name: &str) -> Option<&Location> {
        self.locations.iter().find(|location| location.name == name)
    }
}

#[async_trait]
pub(crate) trait Provisioner: Send + Sync {
    /// Create a firewall admitting only SSH from `ssh_from`, then the server behind it. A
    /// failure leaves nothing behind that the provider confirmed; anything it could not remove
    /// carries the lease's labels for the caller's cleanup and the reaper.
    async fn create(&self, request: &ServerRequest) -> Result<Created, Failure>;
    /// Server types, with prices, and locations, with the types each can create now.
    async fn catalog(&self) -> Result<Catalog, HubError>;
    /// One readiness check: the creation action succeeded and the server is running.
    async fn wait_ready(
        &self,
        server_id: u64,
        action_id: Option<u64>,
    ) -> Result<Progress, HubError>;
    /// Start deleting a resource. Returns the action to await, or `None` when the provider
    /// confirms it is already gone or deleted it synchronously.
    async fn destroy(&self, resource: &Resource) -> Result<Option<u64>, HubError>;
    /// One check of a provider action.
    async fn action(&self, action_id: u64) -> Result<Progress, HubError>;
    /// Servers, Volumes, primary IPs and firewalls matching a label selector.
    async fn list_by_label(&self, selector: &str) -> Result<Vec<Resource>, HubError>;
}

/// The cloud-init document of a lease's server. It runs the boot script of
/// [`data::boot_script`]: Docker, a btrfs data folder, and the pinned target image bound to the
/// server's loopback with its public origin set to the hub's end of the tunnel. The hub alone
/// decides when the server is deleted, so no clock shuts it down. It carries the hub's public
/// key only: no provider token, API token or credential.
pub(crate) fn user_data(pool: &Pool, lease: &Lease, public_key: &str) -> Result<String, HubError> {
    let key = public_key.trim();
    if key.is_empty() || key.contains(['\n', '\r', '"']) {
        return Err(HubError::Usage(
            "the pool's SSH public key must be one line".to_owned(),
        ));
    }
    let origin = format!("http://127.0.0.1:{}", lease.local_port);
    let script = data::boot_script(&data::Boot {
        target_image: &pool.target_image,
        public_origin: &origin,
        data_folder: DATA_FOLDER,
        target_port: TARGET_PORT,
    })?;
    let mut document = format!(
        "#cloud-config\n\
         ssh_pwauth: false\n\
         disable_root: false\n\
         ssh_authorized_keys:\n  - \"{key}\"\n\
         chpasswd: {{ expire: false }}\n\
         write_files:\n\
         \x20 - path: {path}\n\
         \x20   owner: root:root\n\
         \x20   permissions: '0700'\n\
         \x20   content: |\n",
        path = data::BOOT_SCRIPT,
    );
    for line in script.lines() {
        if !line.is_empty() {
            document.push_str("      ");
            document.push_str(line);
        }
        document.push('\n');
    }
    document.push_str(&format!(
        "runcmd:\n\
         \x20 - [sh, -c, \"chage -M 99999 -E -1 -I -1 -d $(date +%F) root >/dev/null 2>&1 || true\"]\n\
         \x20 - [{path}]\n",
        path = data::BOOT_SCRIPT,
    ));
    if document.len() > USER_DATA_LIMIT {
        return Err(HubError::Usage(format!(
            "cloud-init user data is {} bytes; the provider accepts at most {USER_DATA_LIMIT}",
            document.len()
        )));
    }
    Ok(document)
}

/// Whether an image reference is pinned: a digest, or a tag other than `latest`.
pub(crate) fn pinned(image: &str) -> bool {
    if image.contains("@sha256:") {
        return true;
    }
    let name = image.rsplit('/').next().unwrap_or(image);
    name.split_once(':')
        .is_some_and(|(_, tag)| !tag.is_empty() && tag != "latest")
}
