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
    /// Run a shell script as root on the tunnel's server and return what it printed.
    fn run(&self, spec: &TunnelSpec, script: &str) -> Result<String, HubError>;
}

/// How long a script run over SSH may take.
pub(crate) const RUN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// A free loopback port for a tunnel's local end.
pub(crate) fn free_port() -> Result<u16, HubError> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| HubError::storage("no free loopback port for a tunnel", error))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| HubError::storage("no free loopback port for a tunnel", error))
}

/// The `ssh` options shared by tunnels and script runs.
fn ssh_options(spec: &TunnelSpec) -> Vec<String> {
    let mut arguments = Vec::new();
    for option in [
        "ExitOnForwardFailure=yes".to_owned(),
        "ServerAliveInterval=15".to_owned(),
        "ServerAliveCountMax=3".to_owned(),
        "ConnectTimeout=30".to_owned(),
        "BatchMode=yes".to_owned(),
        "IdentitiesOnly=yes".to_owned(),
        "StrictHostKeyChecking=accept-new".to_owned(),
        format!("UserKnownHostsFile={}", spec.known_hosts.display()),
    ] {
        arguments.push("-o".to_owned());
        arguments.push(option);
    }
    arguments.extend(["-i".to_owned(), spec.key.display().to_string()]);
    arguments
}

/// The `ssh` arguments of a tunnel.
pub(crate) fn ssh_arguments(spec: &TunnelSpec) -> Vec<String> {
    let mut arguments = vec!["-N".to_owned()];
    arguments.extend(ssh_options(spec));
    arguments.extend([
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

/// The `ssh` arguments of a script run: the script comes on standard input.
pub(crate) fn run_arguments(spec: &TunnelSpec) -> Vec<String> {
    let mut arguments = ssh_options(spec);
    arguments.extend([
        format!("root@{}", spec.host),
        "sh".to_owned(),
        "-s".to_owned(),
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

    fn run(&self, spec: &TunnelSpec, script: &str) -> Result<String, HubError> {
        run_script(&self.program, &run_arguments(spec), script, RUN_TIMEOUT)
    }
}

/// Run `program` with the script on standard input, killed after `timeout`.
#[cfg(unix)]
pub(crate) fn run_script(
    program: &std::path::Path,
    arguments: &[String],
    script: &str,
    timeout: std::time::Duration,
) -> Result<String, HubError> {
    use std::io::{Read, Write};
    let mut child = std::process::Command::new(program)
        .args(arguments)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| HubError::storage("ssh could not be started", error))?;
    let mut stdout = child.stdout.take();
    let reader = std::thread::spawn(move || {
        let mut output = String::new();
        if let Some(stdout) = stdout.as_mut() {
            let _ = stdout.read_to_string(&mut output);
        }
        output
    });
    if let Some(mut stdin) = child.stdin.take() {
        // A server that closes early shows in the exit status, not here.
        let _ = stdin.write_all(script.as_bytes());
    }
    let started = std::time::Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < timeout => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(HubError::Unanswered(format!(
                    "ssh did not finish within {} s",
                    timeout.as_secs()
                )));
            }
        }
    };
    let output = reader.join().unwrap_or_default();
    if status.success() {
        return Ok(output);
    }
    let mut error = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut error);
    }
    let error = error.trim();
    Err(HubError::Unanswered(format!(
        "ssh exited with {status}{}{error}",
        if error.is_empty() { "" } else { ": " }
    )))
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

    fn run(&self, _spec: &TunnelSpec, _script: &str) -> Result<String, HubError> {
        Err(HubError::Usage(
            "hub pools need a Unix hub host for their SSH tunnels".to_owned(),
        ))
    }
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
