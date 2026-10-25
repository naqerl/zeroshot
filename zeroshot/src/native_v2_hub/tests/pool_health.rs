//! The btrfs data health check of pool servers, against the fake provider and fake tunnels,
//! and the SSH script runner against a fake `ssh`.

use super::super::data::DataMode;
use super::super::health::{self, Health};
use super::super::model::MemberState;
use super::super::pool::{LeasePhase, ResourceKind, TeardownReason};
use super::super::provision::DATA_FOLDER;
use super::pool::{first_error, rig};
use crate::native_v2_target::TargetRegistry;

#[test]
fn the_health_check_output_is_parsed_strictly() {
    assert_eq!(
        health::parse("pending\n").expect("pending"),
        Health::Pending
    );
    assert_eq!(
        health::parse("noise\nfailed boot: /mnt/zs is ext4\n").expect("failed"),
        Health::Failed("boot: /mnt/zs is ext4".to_owned())
    );
    assert_eq!(
        health::parse("ok btrfs 1073741824\n\n").expect("ok"),
        Health::Healthy {
            size_bytes: 1_073_741_824
        }
    );
    for garbage in [
        "",
        "\n",
        "ok ext4 5",
        "ok btrfs many",
        "ok btrfs 5 more",
        "hello",
    ] {
        let error = health::parse(garbage).expect_err(garbage);
        assert!(
            error.to_string().contains("the data health check printed"),
            "{error}"
        );
    }
}

#[test]
fn the_health_check_script_checks_status_btrfs_snapshot_and_reflink_in_order() {
    let script = health::health_script(DATA_FOLDER);
    let at = |needle: &str| {
        script
            .find(needle)
            .unwrap_or_else(|| panic!("{needle:?} missing from\n{script}"))
    };
    assert!(script.contains("DATA='/mnt/zs'"));
    assert!(at("$STATUS/failed") < at("$STATUS/ok"));
    assert!(at("$STATUS/ok") < at("stat -f -c %T"));
    assert!(at("stat -f -c %T") < at("btrfs subvolume create"));
    assert!(at("btrfs subvolume create") < at("btrfs subvolume snapshot -r"));
    assert!(
        at("btrfs subvolume snapshot -r") < at("docker exec zeroshot-target cp --reflink=always")
    );
    assert!(at("docker exec zeroshot-target") < at("echo \"ok btrfs $size\""));
    #[cfg(unix)]
    {
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
}

#[tokio::test]
async fn a_member_joins_only_after_its_data_folder_passes_the_check() {
    let rig = rig();
    rig.add_pool(1).await;
    *rig.tunnels.health.lock().expect("lock") = Some(Ok("pending\n".to_owned()));
    rig.enqueue("hj_one").await;
    for _ in 0..4 {
        rig.pass().await;
    }
    let lease = rig.lease().await;
    assert_eq!(lease.phase, LeasePhase::Connecting, "{lease:?}");
    assert_eq!(
        lease.error.as_deref(),
        Some("the boot script is still running")
    );
    let state = rig.state().await;
    assert!(state.members.is_empty());
    assert!(rig.service.registry().get(&lease.name).is_err());
    let scripts = rig.tunnels.scripts.lock().expect("lock").clone();
    assert!(!scripts.is_empty());
    assert!(
        scripts
            .iter()
            .all(|(name, script)| name == &lease.name
                && script == &health::health_script(DATA_FOLDER))
    );

    *rig.tunnels.health.lock().expect("lock") = None;
    rig.pass().await;
    rig.pass().await;
    let state = rig.state().await;
    let member = state.member(&lease.name).expect("member");
    let data = member.data.as_ref().expect("data health");
    assert_eq!(
        (data.fs.as_str(), data.mode, data.size_bytes),
        ("btrfs", DataMode::File, 1_073_741_824)
    );
    let view = member.view();
    assert_eq!(view["data"]["fs"], "btrfs");
    assert_eq!(view["data"]["mode"], "file");
    assert_eq!(view["data"]["sizeBytes"], 1_073_741_824_u64);
    assert!(view["data"]["checkedAt"].is_string());
    assert!(rig.service.registry().get(&lease.name).is_ok());
}

#[tokio::test]
async fn an_ssh_failure_retries_the_check_without_failing_the_boot() {
    let rig = rig();
    rig.add_pool(1).await;
    *rig.tunnels.health.lock().expect("lock") =
        Some(Err("ssh exited with 255: connection refused".to_owned()));
    rig.enqueue("hj_one").await;
    let mut last = serde_json::Value::Null;
    for _ in 0..4 {
        last = rig.pass().await;
    }
    let lease = rig.lease().await;
    assert_eq!(lease.phase, LeasePhase::Connecting, "{lease:?}");
    assert!(lease.reason.is_none());
    assert!(first_error(&last).contains("connection refused"), "{last}");
    assert!(rig.state().await.members.is_empty());
}

#[tokio::test]
async fn a_failed_data_check_fails_the_boot_and_deletes_the_server() {
    let rig = rig();
    rig.add_pool(1).await;
    *rig.tunnels.health.lock().expect("lock") = Some(Ok(
        "failed boot: /mnt/zs is ext2/ext3, not btrfs; the target is not started on it\n"
            .to_owned(),
    ));
    rig.enqueue("hj_one").await;
    for _ in 0..3 {
        rig.pass().await;
    }
    let lease = rig.lease().await;
    assert_eq!(lease.reason, Some(TeardownReason::BootFailed), "{lease:?}");
    assert!(
        matches!(lease.phase, LeasePhase::Draining | LeasePhase::Deleting),
        "{lease:?}"
    );
    assert_eq!(
        lease.error.as_deref(),
        Some(
            "data health check: boot: /mnt/zs is ext2/ext3, not btrfs; the target is not \
             started on it"
        )
    );
    assert!(
        rig.state()
            .await
            .members
            .iter()
            .all(|member| member.state != MemberState::Active)
    );
    for _ in 0..4 {
        rig.pass().await;
        if rig
            .state()
            .await
            .leases
            .iter()
            .all(|slot| slot.name != lease.name)
        {
            break;
        }
    }
    let state = rig.state().await;
    assert!(state.leases.iter().all(|slot| slot.name != lease.name));
    assert!(state.members.is_empty());
    assert_eq!(state.pools[0].boot_failures, 1);
    let server = lease.server_id.expect("server");
    assert!(
        rig.cloud
            .with(|cloud| cloud.destroyed.contains(&(ResourceKind::Server, server)))
    );
}

#[cfg(unix)]
#[test]
fn ssh_runs_a_script_on_standard_input_and_reports_failures() {
    use super::super::tunnel::{
        RUN_TIMEOUT, SshTunnels, TunnelSpec, Tunnels, run_arguments, run_script,
    };
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().expect("tempdir");
    let program = |name: &str, body: &str| {
        let path = directory.path().join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("fake ssh");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).expect("mode");
        path
    };
    let arguments = directory.path().join("arguments");
    let fake = program(
        "fake-ssh",
        &format!(
            "printf '%s\\n' \"$@\" >'{}'\nexec sh -s",
            arguments.display()
        ),
    );
    let spec = TunnelSpec {
        name: "zs-one".to_owned(),
        host: "203.0.113.10".to_owned(),
        local_port: 40123,
        key: directory.path().join("key"),
        known_hosts: directory.path().join("pool/known_hosts"),
    };
    let tunnels = SshTunnels::with_program(fake);
    let output = tunnels
        .run(&spec, "echo first\necho \"ok btrfs $((2 * 512))\"\n")
        .expect("ran");
    assert_eq!(output, "first\nok btrfs 1024\n");
    let passed = std::fs::read_to_string(&arguments).expect("arguments");
    assert_eq!(passed.lines().collect::<Vec<_>>(), run_arguments(&spec));
    assert!(!passed.contains("-N") && !passed.contains("-L"), "{passed}");
    assert!(passed.ends_with("root@203.0.113.10\nsh\n-s\n"), "{passed}");
    assert!(passed.contains("BatchMode=yes"));
    assert!(RUN_TIMEOUT.as_secs() >= 60);

    let denied = program(
        "denied-ssh",
        "echo 'Permission denied (publickey).' >&2\nexit 255",
    );
    let error = SshTunnels::with_program(denied)
        .run(&spec, "echo never\n")
        .expect_err("denied");
    assert!(error.to_string().contains("exit status: 255"), "{error}");
    assert!(error.to_string().contains("Permission denied"), "{error}");

    let quiet = program("quiet-ssh", "exit 3");
    let error = run_script(&quiet, &[], "", RUN_TIMEOUT).expect_err("failed");
    assert!(error.to_string().ends_with("exit status: 3"), "{error}");

    let slow = program("slow-ssh", "exec sleep 30");
    let started = std::time::Instant::now();
    let error =
        run_script(&slow, &[], "", std::time::Duration::from_millis(200)).expect_err("timed out");
    assert!(error.to_string().contains("did not finish"), "{error}");
    assert!(started.elapsed() < std::time::Duration::from_secs(10));

    let missing = SshTunnels::with_program(directory.path().join("absent"));
    let error = missing.run(&spec, "true").expect_err("no program");
    assert!(
        error.to_string().contains("ssh could not be started"),
        "{error}"
    );
}
