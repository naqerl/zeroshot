//! The data health check of a pool server, run over SSH once its tunnel is open and before it
//! joins as a member: the boot script finished, the data folder is btrfs, a subvolume snapshot
//! works on it, and the running target can reflink-copy inside its own storage mount. A failed
//! check is a failed boot.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::clock::rfc3339;
use super::data::{BOOT_STATUS, DataMode};
use super::error::HubError;

/// The data folder a pool member last passed the health check with.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct DataHealth {
    pub fs: String,
    pub mode: DataMode,
    pub size_bytes: u64,
    pub checked_at_ms: u64,
}

impl DataHealth {
    pub(crate) fn view(&self) -> Value {
        json!({
            "fs": self.fs,
            "mode": self.mode,
            "sizeBytes": self.size_bytes,
            "checkedAt": rfc3339(self.checked_at_ms),
        })
    }
}

/// What the health check found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Health {
    /// The boot script still runs; check again later.
    Pending,
    /// The boot or the data folder failed, with the reason.
    Failed(String),
    /// btrfs, with the data folder's size.
    Healthy { size_bytes: u64 },
}

/// The health check, a POSIX sh script for `ssh root@server sh -s`. It always exits 0 and
/// prints one line: `pending`, `failed <reason>` or `ok btrfs <size bytes>`; anything else,
/// such as an SSH failure, is retried.
pub(crate) fn health_script(data_folder: &str) -> String {
    format!(
        r#"set -u
STATUS='{BOOT_STATUS}'
DATA='{data_folder}'
if [ -s "$STATUS/failed" ]; then
  printf 'failed boot: %s\n' "$(head -n 1 "$STATUS/failed")"
  exit 0
fi
if [ ! -e "$STATUS/ok" ]; then
  echo pending
  exit 0
fi
fs=$(stat -f -c %T "$DATA" 2>/dev/null || echo missing)
if [ "$fs" != btrfs ]; then
  echo "failed $DATA is $fs, not btrfs"
  exit 0
fi
probe="$DATA/.zeroshot-health"
clean() {{
  for old in "$probe.snapshot" "$probe"; do
    if [ -e "$old" ]; then btrfs subvolume delete "$old" >/dev/null 2>&1 || true; fi
  done
}}
check() {{
  if ! "$@" >/dev/null 2>&1; then
    clean
    echo "failed $DATA: $1 $2 did not work"
    exit 0
  fi
}}
clean
check btrfs subvolume create "$probe"
check sh -c "printf 'zeroshot\n' >'$probe/file'"
check btrfs subvolume snapshot -r "$probe" "$probe.snapshot"
check docker exec zeroshot-target cp --reflink=always /var/lib/zeroshot/.zeroshot-health/file /var/lib/zeroshot/.zeroshot-health/clone
clean
size=$(df -B1 --output=size "$DATA" | tail -n 1 | tr -d ' ')
echo "ok btrfs $size"
"#
    )
}

/// The health check's output line.
pub(crate) fn parse(output: &str) -> Result<Health, HubError> {
    let line = output.lines().map(str::trim).rfind(|line| !line.is_empty());
    let unexpected = || {
        HubError::Unanswered(format!(
            "the data health check printed {:?}",
            line.unwrap_or_default()
        ))
    };
    let line = line.ok_or_else(unexpected)?;
    if line == "pending" {
        return Ok(Health::Pending);
    }
    if let Some(reason) = line.strip_prefix("failed ") {
        return Ok(Health::Failed(reason.to_owned()));
    }
    let mut words = line.split(' ');
    match (words.next(), words.next(), words.next(), words.next()) {
        (Some("ok"), Some("btrfs"), Some(size), None) => size
            .parse()
            .map(|size_bytes| Health::Healthy { size_bytes })
            .map_err(|_| unexpected()),
        _ => Err(unexpected()),
    }
}
