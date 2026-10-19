use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use super::error::HubError;
use super::model::HubState;

const STATE_FILE: &str = "state.json";
const LOCK_FILE: &str = "hub.lock";

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
        let staged = self.directory.join(format!("{STATE_FILE}.tmp"));
        let mut file = private_options()
            .write(true)
            .truncate(true)
            .open(&staged)
            .map_err(|error| HubError::storage("state could not be staged", error))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| HubError::storage("state could not be written", error))?;
        std::fs::rename(&staged, self.directory.join(STATE_FILE))
            .map_err(|error| HubError::storage("state could not be replaced", error))
    }
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn create_private_directory(path: &Path) -> Result<(), HubError> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|error| HubError::storage("directory could not be created", error))
}
