//! The data folder of pool servers: zeroshot keeps run workspaces, checkpoint staging and the
//! Restic repository under the target's storage root and copies workspaces with reflinks
//! (`FICLONE`), which need a copy-on-write filesystem. Hetzner root disks are ext4, so every pool
//! server formats its data folder as btrfs at boot, on a file on its local disk or on an
//! attached Volume, and the hub checks it before the server joins.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::error::HubError;

/// Filesystem of a pool server's data folder.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DataFs {
    #[default]
    Btrfs,
}

impl DataFs {
    /// The filesystem a `--data-fs` value names.
    pub(crate) fn parse(value: &str) -> Result<Self, HubError> {
        match value {
            "btrfs" => Ok(Self::Btrfs),
            other => Err(HubError::Usage(format!(
                "--data-fs {other:?} is not supported; pool data folders are btrfs"
            ))),
        }
    }
}

/// Smallest btrfs data file or Volume, in GB.
pub(crate) const MIN_DATA_GB: u32 = 5;
/// Smallest and largest Hetzner Volume, in GB.
pub(crate) const VOLUME_GB: std::ops::RangeInclusive<u32> = 10..=10_240;

/// Where a pool server keeps its data: a btrfs file on the local disk, sized by `size_gb` or
/// the free disk after the target image less a headroom for Docker, or a btrfs Volume of
/// `volume_gb` attached to the server and deleted with it.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct DataSettings {
    #[serde(default)]
    pub fs: DataFs,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_gb: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_gb: Option<u32>,
}

/// How a data folder is backed.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DataMode {
    /// A loop-mounted file on the server's local disk: no extra resource, fastest to boot.
    File,
    /// An attached Hetzner Volume.
    Volume,
}

impl DataSettings {
    pub(crate) const fn mode(&self) -> DataMode {
        if self.volume_gb.is_some() {
            DataMode::Volume
        } else {
            DataMode::File
        }
    }

    /// Check `--data-size` and `--data-volume`.
    pub(crate) fn validate(&self) -> Result<(), HubError> {
        let usage = |message: String| Err(HubError::Usage(message));
        if self.size_gb.is_some() && self.volume_gb.is_some() {
            return usage("--data-size and --data-volume exclude each other".to_owned());
        }
        if let Some(size) = self.size_gb
            && size < MIN_DATA_GB
        {
            return usage(format!("--data-size must be at least {MIN_DATA_GB} GB"));
        }
        if let Some(size) = self.volume_gb
            && !VOLUME_GB.contains(&size)
        {
            return usage(format!(
                "--data-volume must be {} to {} GB",
                VOLUME_GB.start(),
                VOLUME_GB.end()
            ));
        }
        Ok(())
    }

    pub(crate) fn view(&self) -> Value {
        json!({
            "fs": self.fs,
            "mode": self.mode(),
            "sizeGb": self.size_gb,
            "volumeGb": self.volume_gb,
        })
    }
}
