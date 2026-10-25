//! The btrfs data folder of pool servers, against the fake provider and fake tunnels only.

use serde_json::json;

use super::super::data::{self, DataFs, DataMode, DataSettings};
use super::super::pool::Lease;
use super::super::provision;
use super::super::model::HubState;
use super::super::store::migrate;
use super::pool::{add_request, pool_fixture, rig};

#[tokio::test]
async fn data_settings_default_to_a_btrfs_file_and_are_validated_and_listed() {
    let rig = rig();
    let folder = rig.directory.path();
    type Case = (&'static str, DataSettings);
    let cases: [Case; 4] = [
        (
            "exclude each other",
            DataSettings {
                size_gb: Some(50),
                volume_gb: Some(50),
                ..DataSettings::default()
            },
        ),
        (
            "--data-size must be at least 5 GB",
            DataSettings {
                size_gb: Some(4),
                ..DataSettings::default()
            },
        ),
        (
            "--data-volume must be 10 to 10240 GB",
            DataSettings {
                volume_gb: Some(9),
                ..DataSettings::default()
            },
        ),
        (
            "--data-volume must be 10 to 10240 GB",
            DataSettings {
                volume_gb: Some(10_241),
                ..DataSettings::default()
            },
        ),
    ];
    for (message, data) in cases {
        let mut request = add_request(folder, 1);
        request.data = data;
        let error = rig.service.add_pool(request).await.expect_err(message);
        assert!(error.to_string().contains(message), "{message}: {error}");
    }
    rig.service
        .add_pool(add_request(folder, 1))
        .await
        .expect("default data");
    let listed = rig.service.list_pools().await;
    assert_eq!(
        listed["pools"][0]["data"],
        json!({"fs": "btrfs", "mode": "file", "sizeGb": null, "volumeGb": null})
    );
    let volume = DataSettings {
        volume_gb: Some(100),
        ..DataSettings::default()
    };
    assert_eq!(volume.mode(), DataMode::Volume);
    assert_eq!(volume.view()["mode"], json!("volume"));
    assert_eq!(volume.view()["volumeGb"], json!(100));
    assert_eq!(DataFs::parse("btrfs").expect("btrfs"), DataFs::Btrfs);
    assert!(
        DataFs::parse("ext4")
            .expect_err("ext4")
            .to_string()
            .contains("not supported")
    );
}

#[tokio::test]
async fn a_v2_state_without_data_settings_gets_the_btrfs_default() {
    let rig = rig();
    rig.service
        .add_pool(add_request(rig.directory.path(), 1))
        .await
        .expect("pool");
    let state: HubState = rig.service.snapshot().await;
    let mut document = serde_json::to_value(&state).expect("state");
    let pool = document["pools"][0].as_object_mut().expect("pool");
    assert!(pool.remove("data").is_some(), "data is stored");
    let migrated = migrate(document.to_string().as_bytes()).expect("migrates");
    assert_eq!(migrated.pools[0].data, DataSettings::default());
    assert_eq!(migrated.pools[0].data.fs, DataFs::Btrfs);
}

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
        data: &pool.data,
        target_image: &pool.target_image,
        public_origin: "http://127.0.0.1:40123",
        data_folder: provision::DATA_FOLDER,
        target_port: provision::TARGET_PORT,
    })
    .expect("script");
    assert_eq!(script, expected);
    assert!(script.starts_with("#!/bin/sh\n"));
    assert!(script.contains("\nset -eu\n"));
    assert!(script.contains("DATA_MODE='file'\nDATA_SIZE_GB=''\n"));
    assert!(script.contains("OPTIONS='noatime,compress=zstd:1'"));
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
fn the_boot_script_follows_the_data_mode_and_rejects_unsafe_words() {
    let mut pool = pool_fixture();
    pool.data.size_gb = Some(60);
    let boot = |pool: &super::super::pool::Pool, image: &'static str| {
        data::boot_script(&data::Boot {
            data: &pool.data,
            target_image: image,
            public_origin: "http://127.0.0.1:40123",
            data_folder: provision::DATA_FOLDER,
            target_port: provision::TARGET_PORT,
        })
    };
    let sized = boot(&pool, "ghcr.io/x/target:1.2").expect("sized");
    assert!(sized.contains("DATA_MODE='file'\nDATA_SIZE_GB='60'\n"));
    pool.data = DataSettings {
        volume_gb: Some(200),
        ..DataSettings::default()
    };
    let volume = boot(&pool, "ghcr.io/x/target@sha256:abc").expect("volume");
    assert!(volume.contains("DATA_MODE='volume'\n"));
    position(&volume, "/dev/disk/by-id/scsi-0HC_Volume_*");
    position(&volume, "if [ -z \"$fs\" ]; then");
    for unsafe_image in ["ghcr.io/x/target:1.2'; reboot", "a b", "", "x$(id)"] {
        let error = boot(&pool, unsafe_image)
            .expect_err(unsafe_image)
            .to_string();
        assert!(error.contains("boot script cannot carry"), "{error}");
    }
}

#[cfg(unix)]
#[test]
fn the_rendered_boot_script_is_valid_posix_shell() {
    for settings in [
        DataSettings::default(),
        DataSettings {
            size_gb: Some(60),
            ..DataSettings::default()
        },
        DataSettings {
            volume_gb: Some(200),
            ..DataSettings::default()
        },
    ] {
        let mut pool = pool_fixture();
        pool.data = settings;
        let lease = Lease::new(&pool, "hj_one", 40123, 0);
        let document =
            provision::user_data(&pool, &lease, "ssh-ed25519 AAAA hub").expect("user data");
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
}
