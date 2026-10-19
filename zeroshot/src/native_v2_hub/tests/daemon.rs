use std::path::PathBuf;

use serde_json::json;
use zeroshot_engine::native_v2_cli::hub::{HubCommand, HubManager, HubStats, HubUninstall};

use super::super::HubContext;
use super::super::daemon::{DaemonProbe, Detection, StaticProbe, guard, pass, requires_daemon};
use super::super::error::{EXIT_DAEMON_CONFLICT, EXIT_DAEMON_NOT_RUNNING, HubError};
use super::super::install::{
    InstallEnv, Step, docker_install_plan, docker_run, docker_uninstall_plan, systemd_install_plan,
    systemd_quote, systemd_uninstall_plan, systemd_unit,
};
use super::super::model::HubState;
use super::super::store::HubStore;
use super::{FakeDriver, job, member};
use crate::native_v2_target::FileTargetRegistry;

fn env() -> InstallEnv {
    InstallEnv {
        executable: PathBuf::from("/opt/zero shot/bin/zeroshot"),
        home: PathBuf::from("/home/andrey"),
        user: "andrey".to_owned(),
        uid: 1000,
        gid: 1000,
        path: "/home/andrey/.local/bin:/usr/bin".to_owned(),
        config_dir: PathBuf::from("/home/andrey/.config/zeroshot"),
        hub_dir: PathBuf::from("/home/andrey/.config/zeroshot/hub"),
        state_dir: PathBuf::from("/home/andrey/.local/state/zeroshot"),
        user_unit_dir: PathBuf::from("/home/andrey/.config/systemd/user"),
        credential_store: Some("file".to_owned()),
    }
}

async fn verdict(probe: StaticProbe) -> Result<&'static str, HubError> {
    guard(&probe).await
}

#[tokio::test]
async fn guard_requires_exactly_one_active_daemon() {
    assert_eq!(
        verdict(StaticProbe {
            systemd: true,
            docker: false
        })
        .await
        .ok(),
        Some("systemd")
    );
    assert_eq!(
        verdict(StaticProbe {
            systemd: false,
            docker: true
        })
        .await
        .ok(),
        Some("docker")
    );

    let missing = verdict(StaticProbe::default())
        .await
        .expect_err("no daemon");
    assert_eq!(missing.exit_code(), EXIT_DAEMON_NOT_RUNNING);
    let diagnostic = missing.diagnostic();
    assert_eq!(diagnostic["schema"], json!("zeroshot.error/v1"));
    assert_eq!(diagnostic["code"], json!("hub.daemon_not_running"));
    assert_eq!(
        diagnostic["details"]["daemon"]["systemd"]["active"],
        json!(false)
    );
    assert!(
        missing
            .to_string()
            .contains("zeroshot hub install --systemd")
    );

    let both = verdict(StaticProbe {
        systemd: true,
        docker: true,
    })
    .await
    .expect_err("conflict");
    assert_eq!(both.exit_code(), EXIT_DAEMON_CONFLICT);
    assert_eq!(both.diagnostic()["code"], json!("hub.daemon_conflict"));
}

#[tokio::test]
async fn probe_override_lists_active_managers() {
    assert_eq!(StaticProbe::parse("none"), StaticProbe::default());
    assert_eq!(
        StaticProbe::parse("docker, systemd"),
        StaticProbe {
            systemd: true,
            docker: true
        }
    );
    let probe = StaticProbe::parse("docker");
    let detection = Detection::probe(&probe).await;
    assert!(detection.docker.active && !detection.systemd.active);
    assert!(
        probe
            .systemd()
            .await
            .detail
            .contains("ZEROSHOT_HUB_DAEMON_PROBE")
    );
}

#[test]
fn only_daemon_and_service_commands_skip_the_guard() {
    let uninstall = HubCommand::Uninstall(HubUninstall {
        manager: HubManager::Docker,
        system: false,
        dry_run: true,
    });
    for exempt in [
        HubCommand::Daemon {
            interval: "10s".to_owned(),
        },
        HubCommand::DaemonStatus,
        uninstall,
    ] {
        assert!(!requires_daemon(&exempt), "{exempt:?}");
    }
    let stats = HubCommand::Stats(HubStats {
        by: Vec::new(),
        since: None,
        until: None,
        prices: None,
    });
    for guarded in [
        HubCommand::Dispatch,
        HubCommand::List {
            tags: Vec::new(),
            probe: false,
        },
        HubCommand::Remove {
            name: "a".to_owned(),
        },
        HubCommand::Dequeue {
            job: "hj_1".to_owned(),
        },
        HubCommand::Status {
            job: "hj_1".to_owned(),
        },
        HubCommand::ForceStop {
            job: "hj_1".to_owned(),
        },
        stats,
    ] {
        assert!(requires_daemon(&guarded), "{guarded:?}");
    }
}

#[test]
fn systemd_user_unit_pins_this_shells_hub_and_restarts() {
    let unit = systemd_unit(&env(), false, "15s");
    assert!(unit.contains(
        "ExecStart=\"/opt/zero shot/bin/zeroshot\" \"hub\" \"daemon\" \"run\" \"--interval\" \"15s\"\n"
    ));
    for line in [
        "Environment=\"ZEROSHOT_HUB_DAEMON_MANAGER=systemd\"",
        "Environment=\"ZEROSHOT_CONFIG_DIR=/home/andrey/.config/zeroshot\"",
        "Environment=\"ZEROSHOT_HUB_DIR=/home/andrey/.config/zeroshot/hub\"",
        "Environment=\"ZEROSHOT_STATE_DIR=/home/andrey/.local/state/zeroshot\"",
        "Environment=\"ZEROSHOT_CREDENTIAL_STORE=file\"",
        "Environment=\"PATH=/home/andrey/.local/bin:/usr/bin\"",
        "Restart=on-failure",
        "WantedBy=default.target",
    ] {
        assert!(
            unit.lines().any(|candidate| candidate == line),
            "missing {line}\n{unit}"
        );
    }
    assert!(
        !unit.contains("User="),
        "user units run as the invoking user"
    );

    let plan = systemd_install_plan(&env(), false, "15s");
    assert_eq!(plan.manager, "systemd");
    let Step::Write { path, .. } = &plan.steps[0] else {
        panic!("first step writes the unit");
    };
    assert_eq!(
        path,
        &PathBuf::from("/home/andrey/.config/systemd/user/zeroshot-hub.service")
    );
    let commands = plan
        .steps
        .iter()
        .filter_map(|step| match step {
            Step::Run { argv, .. } => Some(argv.join(" ")),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        commands,
        [
            "systemctl --user daemon-reload",
            "systemctl --user enable zeroshot-hub.service",
            "systemctl --user restart zeroshot-hub.service",
        ]
    );
    assert!(
        plan.notes
            .iter()
            .any(|note| note.contains("loginctl enable-linger andrey"))
    );
}

#[test]
fn systemd_system_unit_runs_as_the_installing_user() {
    let unit = systemd_unit(&env(), true, "10s");
    assert!(unit.contains("User=1000\nGroup=1000\n"));
    assert!(unit.contains("WantedBy=multi-user.target"));
    let plan = systemd_install_plan(&env(), true, "10s");
    assert!(matches!(
        &plan.steps[0],
        Step::Write { path, .. } if path == &PathBuf::from("/etc/systemd/system/zeroshot-hub.service")
    ));
    assert!(
        matches!(&plan.steps[1], Step::Run { argv, .. } if argv == &["systemctl", "daemon-reload"])
    );
    let uninstall = systemd_uninstall_plan(&env(), true);
    assert!(matches!(
        &uninstall.steps[0],
        Step::Run { argv, may_fail: true } if argv.join(" ") == "systemctl disable --now zeroshot-hub.service"
    ));
    assert!(matches!(&uninstall.steps[1], Step::Remove { .. }));
    assert_eq!(systemd_quote("50% \"a\\b\""), "\"50%% \\\"a\\\\b\\\"\"");
}

#[test]
fn docker_container_restarts_and_mounts_hub_folders_once() {
    let volumes = [
        PathBuf::from("/home/andrey/src"),
        PathBuf::from("/home/andrey/src/app"),
    ];
    let argv = docker_run(&env(), "zeroshot-hub:dev", &volumes, "10s");
    let joined = argv.join(" ");
    for fragment in [
        "docker run --detach --name zeroshot-hub --restart unless-stopped --network host",
        "--user 1000:1000",
        "--env ZEROSHOT_HUB_DAEMON_MANAGER=docker",
        "--env HOME=/home/andrey",
        "--env ZEROSHOT_CONFIG_DIR=/home/andrey/.config/zeroshot",
        "--volume /home/andrey/.config/zeroshot:/home/andrey/.config/zeroshot",
        "--volume /home/andrey/.local/state/zeroshot:/home/andrey/.local/state/zeroshot",
        "--volume /home/andrey/src:/home/andrey/src",
        "zeroshot-hub:dev hub daemon run --interval 10s",
    ] {
        assert!(joined.contains(fragment), "missing {fragment}\n{joined}");
    }
    let mounts = argv.iter().filter(|value| *value == "--volume").count();
    assert_eq!(
        mounts, 3,
        "nested hub and app folders reuse their parent mount: {joined}"
    );

    let plan = docker_install_plan(&env(), "zeroshot-hub:dev", &[], "10s");
    assert!(matches!(
        &plan.steps[0],
        Step::Run { argv, may_fail: true } if argv.join(" ") == "docker rm --force zeroshot-hub"
    ));
    assert_eq!(docker_uninstall_plan().steps.len(), 1);
}

#[tokio::test]
async fn daemon_pass_fires_places_and_saves_under_the_lock() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    {
        let lock = store.lock().expect("lock");
        let state = HubState {
            members: vec![member("local", &[], 1, 0)],
            jobs: vec![job("j1", 0)],
            ..HubState::default()
        };
        store.save(&lock, &state).expect("seed");
    }
    let context = HubContext {
        store: store.clone(),
        driver: Box::new(FakeDriver::default()),
        registry: FileTargetRegistry::new(directory.path().join("targets.json")),
    };
    let (report, view) = pass(&context).await.expect("pass");
    assert!(!report.is_idle());
    assert_eq!(view["schema"], json!("zeroshot.hub.tick/v1"));
    assert_eq!(view["running"], json!(1));
    let lock = store.lock().expect("lock");
    assert_eq!(
        store.load(&lock).expect("state").jobs[0].target.as_deref(),
        Some("local")
    );
    drop(lock);
    let (report, _) = pass(&context).await.expect("idle pass");
    assert!(report.is_idle(), "a pass with nothing to do is idle");
}

#[test]
fn one_daemon_owns_a_hub_folder() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    let first = store.try_daemon_lock().expect("lock file");
    assert!(first.is_some());
    assert!(store.try_daemon_lock().expect("lock file").is_none());
    drop(first);
    assert!(store.try_daemon_lock().expect("lock file").is_some());
    assert!(store.heartbeat().is_none());
    store
        .write_heartbeat(&json!({"pid": 1}))
        .expect("heartbeat");
    assert_eq!(store.heartbeat(), Some(json!({"pid": 1})));
}
