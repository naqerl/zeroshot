//! Cloud servers for pool leases: the [`Provisioner`] contract, implemented by
//! [`super::hetzner::Hetzner`] and by a fake in the tests, and the cloud-init document every
//! server boots with.

use std::collections::BTreeMap;

use async_trait::async_trait;

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

#[async_trait]
pub(crate) trait Provisioner: Send + Sync {
    /// Create a firewall admitting only SSH from `ssh_from`, then the server behind it.
    async fn create(&self, request: &ServerRequest) -> Result<Created, HubError>;
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
    /// Servers, primary IPs and firewalls matching a label selector.
    async fn list_by_label(&self, selector: &str) -> Result<Vec<Resource>, HubError>;
}

/// The cloud-init document of a lease's server. It schedules the backstop shutdown first, so a
/// server whose hub vanished still stops at the end of its TTL plus grace, installs Docker when
/// the image lacks it, and runs the pinned target image bound to the server's loopback, with
/// its public origin set to the hub's end of the tunnel. It carries the hub's public key only:
/// no provider token, API token or credential.
pub(crate) fn user_data(pool: &Pool, lease: &Lease, public_key: &str) -> Result<String, HubError> {
    let backstop_minutes = lease
        .hard_deadline_ms()
        .saturating_sub(lease.created_at_ms)
        .div_ceil(60_000)
        .max(1);
    let key = public_key.trim();
    if key.is_empty() || key.contains(['\n', '\r', '"']) {
        return Err(HubError::Usage(
            "the pool's SSH public key must be one line".to_owned(),
        ));
    }
    let image = quoted(&pool.target_image)?;
    let origin = format!("http://127.0.0.1:{}", lease.local_port);
    let document = format!(
        "#cloud-config\n\
         ssh_pwauth: false\n\
         disable_root: false\n\
         ssh_authorized_keys:\n  - \"{key}\"\n\
         runcmd:\n\
         \x20 - [shutdown, -h, \"+{backstop_minutes}\"]\n\
         \x20 - [sh, -c, \"command -v docker >/dev/null 2>&1 || (curl -fsSL https://get.docker.com | sh)\"]\n\
         \x20 - [mkdir, -p, \"{DATA_FOLDER}\"]\n\
         \x20 - [docker, run, --detach, --restart, unless-stopped, --name, zeroshot-target, \
         --publish, \"127.0.0.1:{TARGET_PORT}:{TARGET_PORT}\", --volume, \
         \"{DATA_FOLDER}:/var/lib/zeroshot\", {image}, target, serve, --listen, \
         \"0.0.0.0:{TARGET_PORT}\", --public-origin, \"{origin}\", --storage, \
         /var/lib/zeroshot/native-v2]\n"
    );
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

fn quoted(value: &str) -> Result<String, HubError> {
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_graphic() && !matches!(character, '"' | '\\'))
    {
        return Err(HubError::Usage(format!(
            "image reference {value:?} has characters cloud-init cannot carry"
        )));
    }
    Ok(format!("\"{value}\""))
}
