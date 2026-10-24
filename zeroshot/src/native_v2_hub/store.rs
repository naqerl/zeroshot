use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use fs2::FileExt;

use super::error::HubError;
use super::model::HubState;
use super::service::create_private_directory;

const STATE_FILE: &str = "state.json";
const LOCK_FILE: &str = "hub.lock";
const DAEMON_LOCK_FILE: &str = "daemon.lock";
const HEARTBEAT_FILE: &str = "daemon.json";

/// Private hub directory holding members and jobs, guarded by one advisory lock.
#[derive(Clone, Debug)]
pub(crate) struct HubStore {
    directory: PathBuf,
}

/// Held advisory lock; released when dropped.
pub(crate) struct HubLock {
    _file: File,
}

impl HubStore {
    pub(crate) const fn new(directory: PathBuf) -> Self {
        Self { directory }
    }

    /// `ZEROSHOT_HUB_DIR`, or `hub/` beside the target registry.
    pub(crate) fn default_directory(registry_path: &Path) -> PathBuf {
        if let Some(directory) =
            std::env::var_os("ZEROSHOT_HUB_DIR").filter(|value| !value.is_empty())
        {
            return PathBuf::from(directory);
        }
        registry_path
            .parent()
            .map_or_else(|| PathBuf::from("hub"), |parent| parent.join("hub"))
    }

    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    /// Claim the daemon role for this hub folder; `None` when another daemon holds it.
    pub(crate) fn try_daemon_lock(&self) -> Result<Option<HubLock>, HubError> {
        create_private_directory(&self.directory)?;
        let file = private_options()
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.directory.join(DAEMON_LOCK_FILE))
            .map_err(|error| HubError::storage("daemon lock could not be opened", error))?;
        Ok(file
            .try_lock_exclusive()
            .ok()
            .map(|()| HubLock { _file: file }))
    }

    /// Replace the daemon heartbeat document.
    pub(crate) fn write_heartbeat(&self, heartbeat: &serde_json::Value) -> Result<(), HubError> {
        let bytes = serde_json::to_vec_pretty(heartbeat)
            .map_err(|error| HubError::storage("heartbeat could not be encoded", error))?;
        self.replace(HEARTBEAT_FILE, &bytes)
    }

    /// The last daemon heartbeat, if any daemon ever ran on this hub folder.
    pub(crate) fn heartbeat(&self) -> Option<serde_json::Value> {
        let bytes = std::fs::read(self.directory.join(HEARTBEAT_FILE)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    pub(crate) fn lock(&self) -> Result<HubLock, HubError> {
        create_private_directory(&self.directory)?;
        let file = private_options()
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.directory.join(LOCK_FILE))
            .map_err(|error| HubError::storage("lock could not be opened", error))?;
        file.lock_exclusive()
            .map_err(|error| HubError::storage("lock could not be acquired", error))?;
        Ok(HubLock { _file: file })
    }

    pub(crate) fn load(&self, _lock: &HubLock) -> Result<HubState, HubError> {
        let path = self.directory.join(STATE_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| {
                HubError::storage(&format!("{} is malformed", path.display()), error)
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(HubState::default()),
            Err(error) => Err(HubError::storage("state could not be read", error)),
        }
    }

    pub(crate) fn save(&self, _lock: &HubLock, state: &HubState) -> Result<(), HubError> {
        let bytes = serde_json::to_vec_pretty(state)
            .map_err(|error| HubError::storage("state could not be encoded", error))?;
        self.replace(STATE_FILE, &bytes)
    }

    fn replace(&self, name: &str, bytes: &[u8]) -> Result<(), HubError> {
        create_private_directory(&self.directory)?;
        crate::private_file::replace(&self.directory.join(name), bytes)
            .map_err(|error| HubError::storage("state could not be replaced", error))
    }
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.create(true);
    crate::private_file::set_private_file_mode(&mut options);
    options
}
