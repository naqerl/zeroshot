//! SSH tunnels from the hub to pool servers: `ssh -N -L 127.0.0.1:PORT:127.0.0.1:8080`, so a
//! pool target is reached through the hub's loopback only and never listens publicly.
//!
//! Host keys are trusted on first use (`StrictHostKeyChecking=accept-new`) into the pool's own
//! `known_hosts`, since a fresh server's key is unknown until it boots. Each server's key is then
//! pinned for its lifetime; the window is its first connection.

use std::net::TcpListener;
use std::path::PathBuf;

use super::error::HubError;

/// One tunnel to open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TunnelSpec {
    pub name: String,
    pub host: String,
    pub local_port: u16,
    pub key: PathBuf,
    pub known_hosts: PathBuf,
}

pub(crate) trait Tunnels: Send + Sync {
    /// Start the tunnel, or restart it when its process exited.
    fn open(&self, spec: &TunnelSpec) -> Result<(), HubError>;
    fn is_open(&self, name: &str) -> bool;
    fn close(&self, name: &str);
}

/// A free loopback port for a tunnel's local end.
pub(crate) fn free_port() -> Result<u16, HubError> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| HubError::storage("no free loopback port for a tunnel", error))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| HubError::storage("no free loopback port for a tunnel", error))
}

/// The `ssh` arguments of a tunnel.
pub(crate) fn ssh_arguments(spec: &TunnelSpec) -> Vec<String> {
    let mut arguments = vec!["-N".to_owned()];
    for option in [
        "ExitOnForwardFailure=yes".to_owned(),
        "ServerAliveInterval=15".to_owned(),
        "ServerAliveCountMax=3".to_owned(),
        "BatchMode=yes".to_owned(),
        "IdentitiesOnly=yes".to_owned(),
        "StrictHostKeyChecking=accept-new".to_owned(),
        format!("UserKnownHostsFile={}", spec.known_hosts.display()),
    ] {
        arguments.push("-o".to_owned());
        arguments.push(option);
    }
    arguments.extend([
        "-i".to_owned(),
        spec.key.display().to_string(),
        "-L".to_owned(),
        format!(
            "127.0.0.1:{}:127.0.0.1:{}",
            spec.local_port,
            super::provision::TARGET_PORT
        ),
        format!("root@{}", spec.host),
    ]);
    arguments
}

/// Tunnels as `ssh` child processes of the daemon.
pub(crate) struct SshTunnels {
    #[cfg(unix)]
    program: PathBuf,
    #[cfg(unix)]
    children: std::sync::Mutex<std::collections::BTreeMap<String, std::process::Child>>,
}

impl Default for SshTunnels {
    fn default() -> Self {
        Self {
            #[cfg(unix)]
            program: PathBuf::from("ssh"),
            #[cfg(unix)]
            children: std::sync::Mutex::default(),
        }
    }
}

#[cfg(unix)]
impl SshTunnels {
    /// Tunnels started with `program` in place of `ssh`, for tests.
    #[cfg(test)]
    pub(crate) fn with_program(program: PathBuf) -> Self {
        Self {
            program,
            children: std::sync::Mutex::default(),
        }
    }
}

#[cfg(unix)]
impl Tunnels for SshTunnels {
    fn open(&self, spec: &TunnelSpec) -> Result<(), HubError> {
        let mut children = self
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(child) = children.get_mut(&spec.name)
            && matches!(child.try_wait(), Ok(None))
        {
            return Ok(());
        }
        if let Some(parent) = spec.known_hosts.parent() {
            crate::private_file::create_directory(parent).map_err(|error| {
                HubError::storage(&format!("{} could not be created", parent.display()), error)
            })?;
        }
        let child = std::process::Command::new(&self.program)
            .args(ssh_arguments(spec))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|error| HubError::storage("ssh could not be started", error))?;
        children.insert(spec.name.clone(), child);
        Ok(())
    }

    fn is_open(&self, name: &str) -> bool {
        self.children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(name)
            .is_some_and(|child| matches!(child.try_wait(), Ok(None)))
    }

    fn close(&self, name: &str) {
        let child = self
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
        if let Some(mut child) = child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(not(unix))]
impl Tunnels for SshTunnels {
    fn open(&self, _spec: &TunnelSpec) -> Result<(), HubError> {
        Err(HubError::Usage(
            "hub pools need a Unix hub host for their SSH tunnels".to_owned(),
        ))
    }

    fn is_open(&self, _name: &str) -> bool {
        false
    }

    fn close(&self, _name: &str) {}
}

#[cfg(unix)]
impl Drop for SshTunnels {
    fn drop(&mut self) {
        let names = self
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in names {
            self.close(&name);
        }
    }
}
