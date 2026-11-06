//! The btrfs data folder of pool servers: the boot script's strict, idempotent steps only.

use super::super::data;
use super::super::pool::{Lease, Transport};
use super::super::provision;
use super::pool::pool_fixture;

/// The boot script as the server will run it: the `write_files` content block, unindented.
fn embedded_script(document: &str) -> String {
    let start = document.find("    content: |\n").expect("script block") + "    content: |\n".len();
    let end = document.find("\nruncmd:\n").expect("runcmd") + 1;
    document[start..end]
        .lines()
        .map(|line| line.strip_prefix("      ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("{needle:?} missing from\n{script}"))
}

#[test]
fn user_data_installs_a_strict_idempotent_btrfs_boot_script() {
    let pool = pool_fixture();
    let lease = Lease::new(&pool, "hj_one", 40123, 0);
    let document = provision::user_data(&pool, &lease, "ssh-ed25519 AAAA hub").expect("user data");
    assert!(document.contains(&format!("  - path: {}\n", data::BOOT_SCRIPT)));
    assert!(document.contains("    permissions: '0700'\n"));
    assert!(
        document.ends_with(&format!("  - [{}]\n", data::BOOT_SCRIPT)),
        "{document}"
    );
    let script = embedded_script(&document);
    let expected = data::boot_script(&data::Boot {
        target_image: &pool.target_image,
        public_origin: "http://127.0.0.1:40123",
        data_folder: provision::DATA_FOLDER,
        target_port: provision::TARGET_PORT,
        transport: super::super::pool::Transport::Ssh,
    })
    .expect("script");
    assert_eq!(script, expected);
    assert!(script.starts_with("#!/bin/sh\n"));
    assert!(script.contains("\nset -eu\n"));
    assert!(!script.contains("DATA_MODE"));
    assert!(!script.contains("DATA_SIZE_GB"));
    assert!(!script.contains("/dev/disk/by-id/scsi-0HC_Volume_"));
    assert!(script.contains("OPTIONS='noatime,compress=zstd:1'"));
    assert!(!script.contains("tailcat"));
    assert!(script.contains("--network host"));
    assert!(script.contains("--listen \"127.0.0.1:8080\""));
    assert!(!script.contains("--publish"));
    // The steps run in order and the target starts only after the btrfs checks.
    let order = [
        "command -v docker",
        "install -y -q btrfs-progs",
        "modprobe btrfs",
        "docker pull",
        "chattr +i",
        "fallocate -l",
        "mkfs.btrfs -q -L zeroshot-data \"$DATA_FILE.part\"",
        ">>/etc/fstab",
        "mount \"$DATA\"",
        "stat -f -c %T \"$DATA\"",
        "[ \"$fs\" = btrfs ] || fail \"$DATA is $fs, not btrfs; the target is not started on it\"",
        "btrfs subvolume create \"$probe\"",
        "btrfs subvolume snapshot -r",
        "cp --reflink=always",
        "btrfs subvolume delete \"$probe\"",
        "docker run --detach",
        "touch \"$STATUS/ok\"",
    ];
    for pair in order.windows(2) {
        assert!(
            position(&script, pair[0]) < position(&script, pair[1]),
            "{:?} must come before {:?}",
            pair[0],
            pair[1]
        );
    }
    // A rerun keeps what the first run made.
    for guard in [
        "if ! command -v mkfs.btrfs",
        "docker image inspect \"$IMAGE\" >/dev/null 2>&1 || docker pull",
        "if ! mountpoint -q \"$DATA\"; then",
        "if [ ! -e \"$DATA_FILE\" ]; then",
        "if ! grep -qs \"^[^#]* $DATA btrfs \" /etc/fstab; then",
        "if docker container inspect zeroshot-target",
        "rm -f \"$STATUS/ok\" \"$STATUS/failed\"",
    ] {
        position(&script, guard);
    }
    // Failures are written for the health check and never reformat existing data.
    for strict in [
        "printf '%s\\n' \"$*\" >\"$STATUS/failed\"",
        "trap 'code=$?;",
        "not btrfs; it is left as it is",
        "the kernel has no btrfs support",
        "fail \"only $((free / GIB)) GiB free",
    ] {
        position(&script, strict);
    }
    assert!(!script.contains("mkfs.btrfs -f"));
    assert!(!script.contains("--force"));
}

#[test]
fn the_boot_script_rejects_unsafe_image_and_origin_words() {
    let boot = |image: &'static str, origin: &'static str| {
        data::boot_script(&data::Boot {
            target_image: image,
            public_origin: origin,
            data_folder: provision::DATA_FOLDER,
            target_port: provision::TARGET_PORT,
            transport: super::super::pool::Transport::Ssh,
        })
    };
    let document =
        boot("ghcr.io/x/target@sha256:abc", "http://127.0.0.1:40123").expect("pinned image");
    assert!(document.contains("IMAGE='ghcr.io/x/target@sha256:abc'"));
    for unsafe_image in ["ghcr.io/x/target:1.2'; reboot", "a b", "", "x$(id)"] {
        let error = boot(unsafe_image, "http://127.0.0.1:40123")
            .expect_err(unsafe_image)
            .to_string();
        assert!(error.contains("boot script cannot carry"), "{error}");
    }
    for unsafe_origin in ["http://127.0.0.1:1'x", "a b", "", "x$(id)"] {
        let error = boot("ghcr.io/x/target:1.2", unsafe_origin)
            .expect_err(unsafe_origin)
            .to_string();
        assert!(error.contains("boot script cannot carry"), "{error}");
    }
}

#[cfg(unix)]
#[test]
fn the_rendered_boot_script_is_valid_posix_shell() {
    let pool = pool_fixture();
    let lease = Lease::new(&pool, "hj_one", 40123, 0);
    let document = provision::user_data(&pool, &lease, "ssh-ed25519 AAAA hub").expect("user data");
    assert!(document.len() <= provision::USER_DATA_LIMIT);
    let script = tempfile::NamedTempFile::new().expect("file");
    std::fs::write(script.path(), embedded_script(&document)).expect("write");
    let checked = std::process::Command::new("sh")
        .arg("-n")
        .arg(script.path())
        .output()
        .expect("sh");
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
}

#[cfg(unix)]
#[test]
fn a_tailcat_pool_installs_the_pinned_server_and_publishes_its_address() {
    let mut pool = pool_fixture();
    pool.transport = Transport::Tailcat;
    let lease = Lease::new(&pool, "hj_one", 40123, 0);
    let document = provision::user_data(&pool, &lease, "ssh-ed25519 AAAA hub").expect("user data");
    assert!(document.len() <= provision::USER_DATA_LIMIT);
    let script = embedded_script(&document);
    for needle in [
        "https://github.com/tailscale/tailcat/releases/download/v0.7.0/",
        data::TAILCAT_AMD64_SHA256,
        data::TAILCAT_ARM64_SHA256,
        "sha256sum -c -",
        "TAILCAT_ADDR_FILE=/run/zeroshot/tailcat.addr",
        "genkey --key='/var/lib/zeroshot-tailcat/tailcat.private.json' --fixed-region",
        "serve 8080",
        "warn 'tailcat failed its pinned checksum",
        "warn 'tailcat did not start; the hub will use the ssh tunnel",
        "while [ ! -s /run/zeroshot/tailcat.addr ]",
        "warn 'tailcat did not publish its address; the hub will use the ssh tunnel",
    ] {
        assert!(script.contains(needle), "{needle:?} missing from\n{script}");
    }
    assert!(!script.contains("latest"));
    // The tailcat steps are warnings, not boot failures: SSH must still be able to serve the
    // lease when tailcat cannot start.
    assert!(script.contains("warn() { echo \"zeroshot boot warning: $*\"; }"));
    let file = tempfile::NamedTempFile::new().expect("file");
    std::fs::write(file.path(), &script).expect("write");
    let checked = std::process::Command::new("sh")
        .arg("-n")
        .arg(file.path())
        .output()
        .expect("sh");
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
}
