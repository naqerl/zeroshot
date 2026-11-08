//! The data folder of pool servers: zeroshot keeps run workspaces, checkpoint staging and the
//! Restic repository under the target's storage root and copies workspaces with reflinks
//! (`FICLONE`), which need a copy-on-write filesystem. Hetzner root disks are ext4, so every pool
//! server formats a btrfs data file on its local disk at boot, and the hub checks it before the
//! server joins.

use super::error::HubError;
use super::pool::Transport;

/// Bytes in a GiB, shared by the data-size bounds and the boot script's shell arithmetic.
const GIB: u64 = 1 << 30;

/// Local disk kept free beside the data file for Docker layers, container writes and logs, in
/// GB.
pub(crate) const HEADROOM_GB: u32 = 8;
/// Smallest data file the boot script creates, in GB.
pub(crate) const MIN_DATA_GB: u32 = 5;
/// Largest data file the boot script creates, in GB. The root disk is ext4 and shared with
/// Docker's writable layer, so a data file sized from the whole free disk leaves the build no
/// room and wedges the box when the layer grows. The cap also bounds the target's own run
/// ledger and checkpoints: filling that 24 GiB mid-run still surfaces as the opaque SQLite
/// database-is-full failure, because the boot floor only runs once at boot.
pub(crate) const DATA_MAX_GB: u32 = 24;
/// Free root space below which the boot fails as `disk_full`, in GB.
pub(crate) const DISK_FLOOR_GB: u32 = 5;
/// Free data-folder space below which the boot fails as `disk_full`, in GB. Lower than the root
/// floor because a freshly formatted 5 GiB btrfs reports slightly less than 5 GiB available.
pub(crate) const DATA_FLOOR_GB: u32 = 3;
/// Where the boot script installs itself on the server.
pub(crate) const BOOT_SCRIPT: &str = "/usr/local/sbin/zeroshot-boot";
/// The boot script's folder: its log, and `ok` or `failed` with the reason.
pub(crate) const BOOT_STATUS: &str = "/var/lib/zeroshot-boot";

/// The data file's size for a root disk with `free` bytes: `free - headroom` clamped to the
/// minimum and maximum data file, rounded down to whole GiB. `None` when the disk cannot hold
/// the headroom and the minimum file, so the boot script fails instead of formatting a sliver.
pub(crate) fn data_size_bytes(
    free: u64,
    headroom_gb: u32,
    min_gb: u32,
    max_gb: u32,
) -> Option<u64> {
    let room = free.checked_sub(u64::from(headroom_gb) * GIB)?;
    let min = u64::from(min_gb) * GIB;
    if room < min {
        return None;
    }
    let max = u64::from(max_gb) * GIB;
    Some((room / GIB * GIB).min(max))
}
/// The btrfs data file on the server's local disk.
pub(crate) const DATA_FILE: &str = "/var/lib/zeroshot-data.btrfs";
/// btrfs mount options of the data folder. `discard` returns deleted extents to the loop file
/// and therefore to the ext4 root disk instead of growing the file until the disk is full.
pub(crate) const MOUNT_OPTIONS: &str = "noatime,compress=zstd:1,discard";
/// Pinned tailcat CLI the boot script installs for a `tailcat` pool.
pub(crate) const TAILCAT_VERSION: &str = "0.7.0";
/// Checksums from that release's `checksums.txt`, for the architectures the target supports.
pub(crate) const TAILCAT_AMD64_SHA256: &str =
    "23c0b1887a5ec422f0d18a9c52b4f5357815febdaae738a1eb54036d10bd9ee6";
pub(crate) const TAILCAT_ARM64_SHA256: &str =
    "bbb1ab50f24f00effe1e1fd86d0501803fb80793a90785a2a16ff3428f03d8ef";
/// Persistent tailcat server key, so a restarted server keeps its address.
pub(crate) const TAILCAT_KEY: &str = "/var/lib/zeroshot-tailcat/tailcat.private.json";

/// What the boot script needs to know about one server.
pub(crate) struct Boot<'a> {
    pub target_image: &'a str,
    pub public_origin: &'a str,
    pub data_folder: &'a str,
    pub target_port: u16,
    pub transport: Transport,
}

/// The server's boot script, run once by cloud-init and safe to rerun: it installs Docker and
/// btrfs-progs when missing, pulls the target image, formats and mounts the btrfs data file on
/// the free local disk, adds it to `/etc/fstab`, proves a subvolume snapshot and a reflink copy
/// on it, and only then starts the target on the server's loopback with the data folder as its
/// `/var/lib/zeroshot`. The target shares the server's network namespace, so the hub's SSH tunnel
/// reaches it directly without Docker's published-port proxy. Any
/// failure stops the script before the target starts and leaves the reason in
/// `BOOT_STATUS/failed`, which the hub's health check reports. Existing data is never
/// reformatted.
pub(crate) fn boot_script(boot: &Boot<'_>) -> Result<String, HubError> {
    let image = shell_word(boot.target_image)?;
    let origin = shell_word(boot.public_origin)?;
    let data = boot.data_folder;
    let port = boot.target_port;
    // The bounds are the pure function's answer, so the shell never invents its own clamp.
    let min_data_bytes = data_size_bytes(
        u64::from(HEADROOM_GB + MIN_DATA_GB) * GIB,
        HEADROOM_GB,
        MIN_DATA_GB,
        DATA_MAX_GB,
    )
    .unwrap_or(0);
    let max_data_bytes =
        data_size_bytes(u64::MAX, HEADROOM_GB, MIN_DATA_GB, DATA_MAX_GB).unwrap_or(u64::MAX);
    let tailcat = match boot.transport {
        Transport::Ssh => String::new(),
        Transport::Tailcat => tailcat_boot(boot.target_port),
    };
    Ok(format!(
        r#"#!/bin/sh
# zeroshot pool server boot, written by the hub: a btrfs data folder, then the pinned target.
# Safe to rerun. The target never starts unless its data folder is btrfs.
set -eu
HEADROOM_GB={HEADROOM_GB}
MIN_DATA_GB={MIN_DATA_GB}
MIN_DATA_BYTES={min_data_bytes}
MAX_DATA_BYTES={max_data_bytes}
DISK_FLOOR_GB={DISK_FLOOR_GB}
DATA_FLOOR_GB={DATA_FLOOR_GB}
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
warn() {{ echo "zeroshot boot warning: $*"; }}
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
  device=$DATA_FILE
  options="loop,$OPTIONS"
  if [ ! -e "$DATA_FILE" ]; then
    step='allocating the data file'
    rm -f "$DATA_FILE.part"
    free=$(df -B1 --output=avail "$(dirname "$DATA_FILE")" | tail -n 1 | tr -d ' ')
    room=$((free - HEADROOM_GB * GIB))
    size=$((room / GIB * GIB))
    [ "$size" -ge "$MIN_DATA_BYTES" ] || fail "only $((free / GIB)) GiB free: the data file needs $MIN_DATA_GB GiB beside $HEADROOM_GB GiB for Docker; use a bigger server type"
    # Bound the file: the root disk is ext4 and Docker's writable layer lives beside it.
    [ "$size" -le "$MAX_DATA_BYTES" ] || size=$MAX_DATA_BYTES
    fallocate -l "$size" "$DATA_FILE.part"
    chmod 600 "$DATA_FILE.part"
    step='formatting the data file'
    mkfs.btrfs -q -L zeroshot-data "$DATA_FILE.part"
    mv "$DATA_FILE.part" "$DATA_FILE"
  fi
  fs=$(blkid -o value -s TYPE "$DATA_FILE" 2>/dev/null || true)
  [ "$fs" = btrfs ] || fail "$DATA_FILE holds ${{fs:-no filesystem}}, not btrfs; it is left as it is"
  step='mounting the data folder'
  if ! grep -qs "^[^#]* $DATA btrfs " /etc/fstab; then
    printf '%s %s btrfs %s,nofail 0 0\n' "$device" "$DATA" "$options" >>/etc/fstab
  fi
  mount "$DATA"
fi

step='returning freed space to ext4'
fstrim -av >/dev/null 2>&1 || warn 'fstrim failed; deleted blocks stay allocated until the next trim'

step='checking free space'
root_avail=$(df -B1 --output=avail / | tail -n 1 | tr -d ' ')
if [ "$root_avail" -lt $((DISK_FLOOR_GB * GIB)) ]; then
  fail "disk_full: the root disk has $((root_avail / GIB)) GiB free, below the $DISK_FLOOR_GB GiB floor; use a bigger server type"
fi
data_avail=$(df -B1 --output=avail "$DATA" | tail -n 1 | tr -d ' ')
if [ "$data_avail" -lt $((DATA_FLOOR_GB * GIB)) ]; then
  fail "disk_full: the data folder has $((data_avail / GIB)) GiB free, below the $DATA_FLOOR_GB GiB floor"
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

step='preparing the build folders'
# Keep the build cache and the fallback temp folder on btrfs, where `discard` gives it back.
mkdir -p "$DATA/.cargo" "$DATA/.cargo-target" "$DATA/tmp"

step='starting the target'
if docker container inspect zeroshot-target >/dev/null 2>&1; then
  docker start zeroshot-target >/dev/null
else
  docker run --detach --restart unless-stopped --name zeroshot-target \
    --network host --volume "$DATA:/var/lib/zeroshot" \
    --env CARGO_HOME=/var/lib/zeroshot/.cargo \
    --env CARGO_TARGET_DIR=/var/lib/zeroshot/.cargo-target \
    --env TMPDIR=/var/lib/zeroshot/tmp \
    "$IMAGE" target serve --listen "127.0.0.1:{port}" --public-origin "$ORIGIN" \
    --storage /var/lib/zeroshot/native-v2 >/dev/null
fi
{tailcat}touch "$STATUS/ok"
echo 'zeroshot boot finished'
"#
    ))
}

/// The boot steps that install and start the pinned tailcat server beside the target. Every
/// failure is a warning: the hub then finds no address and uses its SSH tunnel, so a tailcat
/// problem never fails a lease that SSH can still serve. The server key lives on the root disk
/// and its region is pinned once, so a restarted server keeps the same address.
fn tailcat_boot(port: u16) -> String {
    let version = TAILCAT_VERSION;
    let addr = super::tunnel::TAILCAT_ADDR_FILE;
    let key = TAILCAT_KEY;
    format!(
        r#"step='installing tailcat'
mkdir -p /run/zeroshot /var/lib/zeroshot-tailcat
TAILCAT=''
case "$(uname -m)" in
  x86_64|amd64) asset="tailcat_{version}_linux_amd64.tar.gz"; sum='{amd64}' ;;
  aarch64|arm64) asset="tailcat_{version}_linux_arm64.tar.gz"; sum='{arm64}' ;;
  *) asset=''; sum='' ;;
esac
if [ -z "$asset" ]; then
  warn "tailcat has no build for $(uname -m); the hub will use the ssh tunnel"
elif [ -x /usr/local/bin/tailcat ]; then
  TAILCAT=/usr/local/bin/tailcat
else
  url="https://github.com/tailscale/tailcat/releases/download/v{version}/$asset"
  if ! command -v curl >/dev/null 2>&1; then
    warn 'curl is missing; the hub will use the ssh tunnel'
  elif curl -fsSL "$url" -o /run/zeroshot/tailcat.tar.gz; then
    if echo "$sum  /run/zeroshot/tailcat.tar.gz" | sha256sum -c - >/dev/null 2>&1; then
      if tar -xzf /run/zeroshot/tailcat.tar.gz -C /usr/local/bin tailcat; then
        chmod 0755 /usr/local/bin/tailcat
        TAILCAT=/usr/local/bin/tailcat
      else
        warn 'tailcat could not be unpacked; the hub will use the ssh tunnel'
      fi
    else
      warn 'tailcat failed its pinned checksum; the hub will use the ssh tunnel'
    fi
    rm -f /run/zeroshot/tailcat.tar.gz
  else
    warn 'tailcat could not be downloaded; the hub will use the ssh tunnel'
  fi
fi
step='starting tailcat'
if [ -n "$TAILCAT" ] && [ ! -s '{key}' ]; then
  if ! "$TAILCAT" genkey --key='{key}' --fixed-region >/dev/null 2>&1; then
    warn 'tailcat could not generate a server key; the hub will use the ssh tunnel'
    TAILCAT=''
  fi
fi
if [ -n "$TAILCAT" ]; then
  docker rm -f zeroshot-tailcat >/dev/null 2>&1 || true
  if ! docker run --detach --restart unless-stopped --name zeroshot-tailcat \
    --network host \
    --volume "$TAILCAT:/usr/local/bin/tailcat:ro" \
    --volume /var/lib/zeroshot-tailcat:/var/lib/zeroshot-tailcat:ro \
    --volume /run/zeroshot:/run/zeroshot \
    --env TAILCAT_ADDR_FILE={addr} \
    --entrypoint /usr/local/bin/tailcat \
    "$IMAGE" --key='{key}' serve {port} >/dev/null; then
    warn 'tailcat did not start; the hub will use the ssh tunnel'
  fi
  # The hub reads the address as soon as the health check passes, so do not report `ok`
  # before the server has published it.
  tries=0
  while [ ! -s /run/zeroshot/tailcat.addr ] && [ "$tries" -lt 20 ]; do
    sleep 1
    tries=$((tries + 1))
  done
  if [ ! -s /run/zeroshot/tailcat.addr ]; then
    warn 'tailcat did not publish its address; the hub will use the ssh tunnel'
  fi
fi
"#,
        amd64 = TAILCAT_AMD64_SHA256,
        arm64 = TAILCAT_ARM64_SHA256,
    )
}

/// The remote cleanup before a lease's server is deleted: stop the target container so its
/// writable layer is reclaimable, remove unused images and volumes, then return the freed blocks
/// to the provider's storage. Best effort: the hub logs a failure and deletes the server anyway.
pub(crate) fn teardown_script() -> String {
    "set -u\n\
     status=0\n\
     docker rm -f zeroshot-target >/dev/null 2>&1 || true\n\
     docker system prune -af --volumes || status=$?\n\
     fstrim_status=0\n\
     fstrim -av || fstrim_status=$?\n\
     [ \"$status\" -ne 0 ] || status=$fstrim_status\n\
     exit \"$status\"\n"
        .to_owned()
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
