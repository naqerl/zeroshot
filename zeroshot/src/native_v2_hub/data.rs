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
/// the free disk after the target image less [`HEADROOM_GB`], or a btrfs Volume of
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

impl DataMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Volume => "volume",
        }
    }
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

/// Local disk kept free beside the data file for Docker layers, container writes and logs, in
/// GB.
pub(crate) const HEADROOM_GB: u32 = 8;
/// Where the boot script installs itself on the server.
pub(crate) const BOOT_SCRIPT: &str = "/usr/local/sbin/zeroshot-boot";
/// The boot script's folder: its log, and `ok` or `failed` with the reason.
pub(crate) const BOOT_STATUS: &str = "/var/lib/zeroshot-boot";
/// The btrfs data file in file mode.
pub(crate) const DATA_FILE: &str = "/var/lib/zeroshot-data.btrfs";
/// btrfs mount options of the data folder.
pub(crate) const MOUNT_OPTIONS: &str = "noatime,compress=zstd:1";

/// What the boot script needs to know about one server.
pub(crate) struct Boot<'a> {
    pub data: &'a DataSettings,
    pub target_image: &'a str,
    pub public_origin: &'a str,
    pub data_folder: &'a str,
    pub target_port: u16,
}

/// The server's boot script, run once by cloud-init and safe to rerun: it installs Docker and
/// btrfs-progs when missing, pulls the target image, formats and mounts the btrfs data folder
/// (a file sized to the free disk, or the attached Volume), adds it to `/etc/fstab`, proves a
/// subvolume snapshot and a reflink copy on it, and only then starts the target with the data
/// folder as its `/var/lib/zeroshot`. Any failure stops the script before the target starts and
/// leaves the reason in `BOOT_STATUS/failed`, which the hub's health check reports. Existing
/// data is never reformatted.
pub(crate) fn boot_script(boot: &Boot<'_>) -> Result<String, HubError> {
    let image = shell_word(boot.target_image)?;
    let origin = shell_word(boot.public_origin)?;
    let mode = boot.data.mode().as_str();
    let size = boot
        .data
        .size_gb
        .map(|size| size.to_string())
        .unwrap_or_default();
    let data = boot.data_folder;
    let port = boot.target_port;
    Ok(format!(
        r#"#!/bin/sh
# zeroshot pool server boot, written by the hub: a btrfs data folder, then the pinned target.
# Safe to rerun. The target never starts unless its data folder is btrfs.
set -eu
DATA_MODE='{mode}'
DATA_SIZE_GB='{size}'
HEADROOM_GB={HEADROOM_GB}
MIN_DATA_GB={MIN_DATA_GB}
IMAGE='{image}'
ORIGIN='{origin}'
DATA='{data}'
DATA_FILE='{DATA_FILE}'
STATUS='{BOOT_STATUS}'
OPTIONS='{MOUNT_OPTIONS}'
GIB=1073741824
mkdir -p "$STATUS"
exec >>"$STATUS/log" 2>&1
rm -f "$STATUS/ok" "$STATUS/failed"
step='starting'
fail() {{
  printf '%s\n' "$*" >"$STATUS/failed"
  echo "zeroshot boot failed: $*"
  exit 1
}}
trap 'code=$?; if [ "$code" -ne 0 ] && [ ! -e "$STATUS/failed" ]; then printf "%s failed with exit code %s\n" "$step" "$code" >"$STATUS/failed"; fi' EXIT

step='installing Docker'
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
  command -v docker >/dev/null 2>&1 || fail 'Docker did not install'
fi

step='installing btrfs-progs'
if ! command -v mkfs.btrfs >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get -o DPkg::Lock::Timeout=300 update -q
    DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y -q btrfs-progs
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y -q btrfs-progs
  else
    fail 'btrfs-progs is missing and there is no apt-get or dnf to install it'
  fi
fi
grep -qw btrfs /proc/filesystems || modprobe btrfs || true
grep -qw btrfs /proc/filesystems || fail 'the kernel has no btrfs support'

step='pulling the target image'
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null

step='preparing the data folder'
mkdir -p "$DATA"
if ! mountpoint -q "$DATA"; then
  # Unmounted, the folder stays read-only, so nothing writes the root disk in its place.
  chattr +i "$DATA" 2>/dev/null || true
  if [ "$DATA_MODE" = volume ]; then
    step='waiting for the data volume'
    device=''
    tries=0
    while [ -z "$device" ]; do
      for candidate in /dev/disk/by-id/scsi-0HC_Volume_*; do
        if [ -b "$candidate" ]; then
          device=$candidate
          break
        fi
      done
      if [ -z "$device" ]; then
        tries=$((tries + 1))
        [ "$tries" -le 90 ] || fail 'the data volume did not appear under /dev/disk/by-id'
        sleep 2
      fi
    done
    options=$OPTIONS
    fs=$(blkid -o value -s TYPE "$device" 2>/dev/null || true)
    if [ -z "$fs" ]; then
      step='formatting the data volume'
      mkfs.btrfs -q -L zeroshot-data "$device"
    elif [ "$fs" != btrfs ]; then
      fail "the data volume $device holds $fs, not btrfs; it is left as it is"
    fi
  else
    device=$DATA_FILE
    options="loop,$OPTIONS"
    if [ ! -e "$DATA_FILE" ]; then
      step='allocating the data file'
      rm -f "$DATA_FILE.part"
      free=$(df -B1 --output=avail "$(dirname "$DATA_FILE")" | tail -n 1 | tr -d ' ')
      room=$((free - HEADROOM_GB * GIB))
      if [ -n "$DATA_SIZE_GB" ]; then
        size=$((DATA_SIZE_GB * GIB))
        [ "$size" -le "$room" ] || fail "--data-size $DATA_SIZE_GB GB does not fit: $((free / GIB)) GiB free, $HEADROOM_GB GiB kept for Docker"
      else
        size=$((room / GIB * GIB))
      fi
      [ "$size" -ge $((MIN_DATA_GB * GIB)) ] || fail "only $((free / GIB)) GiB free: the data file needs $MIN_DATA_GB GiB beside $HEADROOM_GB GiB for Docker; use a bigger server type or --data-volume"
      fallocate -l "$size" "$DATA_FILE.part"
      chmod 600 "$DATA_FILE.part"
      step='formatting the data file'
      mkfs.btrfs -q -L zeroshot-data "$DATA_FILE.part"
      mv "$DATA_FILE.part" "$DATA_FILE"
    fi
    fs=$(blkid -o value -s TYPE "$DATA_FILE" 2>/dev/null || true)
    [ "$fs" = btrfs ] || fail "$DATA_FILE holds ${{fs:-no filesystem}}, not btrfs; it is left as it is"
  fi
  step='mounting the data folder'
  if ! grep -qs "^[^#]* $DATA btrfs " /etc/fstab; then
    printf '%s %s btrfs %s,nofail 0 0\n' "$device" "$DATA" "$options" >>/etc/fstab
  fi
  mount "$DATA"
fi

step='checking the data folder'
fs=$(stat -f -c %T "$DATA")
[ "$fs" = btrfs ] || fail "$DATA is $fs, not btrfs; the target is not started on it"
step='testing a btrfs snapshot and a reflink'
probe="$DATA/.zeroshot-boot-probe"
for old in "$probe.snapshot" "$probe"; do
  if [ -e "$old" ]; then btrfs subvolume delete "$old" >/dev/null; fi
done
btrfs subvolume create "$probe" >/dev/null
printf 'zeroshot\n' >"$probe/file"
btrfs subvolume snapshot -r "$probe" "$probe.snapshot" >/dev/null
cp --reflink=always "$probe/file" "$probe/clone"
btrfs subvolume delete "$probe.snapshot" >/dev/null
btrfs subvolume delete "$probe" >/dev/null

step='starting the target'
if docker container inspect zeroshot-target >/dev/null 2>&1; then
  docker start zeroshot-target >/dev/null
else
  docker run --detach --restart unless-stopped --name zeroshot-target \
    --publish "127.0.0.1:{port}:{port}" --volume "$DATA:/var/lib/zeroshot" \
    "$IMAGE" target serve --listen "0.0.0.0:{port}" --public-origin "$ORIGIN" \
    --storage /var/lib/zeroshot/native-v2 >/dev/null
fi
touch "$STATUS/ok"
echo 'zeroshot boot finished'
"#
    ))
}

/// A value for a single-quoted shell word: image references and URLs only.
fn shell_word(value: &str) -> Result<&str, HubError> {
    if value.is_empty()
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '.' | '_' | '-' | ':' | '/' | '@')
        })
    {
        return Err(HubError::Usage(format!(
            "{value:?} has characters the server's boot script cannot carry"
        )));
    }
    Ok(value)
}
