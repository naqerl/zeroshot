use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use super::super::daemon::{client_url, load_or_create_token};
use super::super::install::{
    InstallEnv, Step, daemon_arguments, docker_install_plan, docker_run, docker_uninstall_plan,
    refuse_other_owner, systemd_install_plan, systemd_quote, systemd_uninstall_plan, systemd_unit,
};
use super::super::model::HubState;
use super::super::service::HubService;
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

fn daemon(interval: &str) -> Vec<String> {
    daemon_arguments(interval, None, None)
}

#[test]
fn installed_daemons_pass_listen_and_token_file_through() {
    let arguments = daemon_arguments(
        "5s",
        Some("0.0.0.0:7383"),
        Some(std::path::Path::new("/srv/hub/token")),
    );
    assert_eq!(
        arguments.join(" "),
        "hub daemon run --interval 5s --listen 0.0.0.0:7383 --token-file /srv/hub/token"
    );
    let unit = systemd_unit(&env(), false, &arguments);
    assert!(unit.contains("\"--listen\" \"0.0.0.0:7383\" \"--token-file\" \"/srv/hub/token\""));
    let argv = docker_run(&env(), "zeroshot-hub:dev", &[], &arguments).join(" ");
    assert!(
        argv.contains("--volume /srv/hub:/srv/hub"),
        "token folder is mounted: {argv}"
    );
    assert!(argv.ends_with("zeroshot-hub:dev hub daemon run --interval 5s --listen 0.0.0.0:7383 --token-file /srv/hub/token"));
}

#[test]
fn systemd_user_unit_pins_this_shells_hub_and_restarts() {
    let unit = systemd_unit(&env(), false, &daemon("15s"));
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

    let plan = systemd_install_plan(&env(), false, &daemon("15s"));
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
    let unit = systemd_unit(&env(), true, &daemon("10s"));
    assert!(unit.contains("User=1000\nGroup=1000\n"));
    assert!(unit.contains("WantedBy=multi-user.target"));
    let plan = systemd_install_plan(&env(), true, &daemon("10s"));
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
    let argv = docker_run(&env(), "zeroshot-hub:dev", &volumes, &daemon("10s"));
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

    let plan = docker_install_plan(&env(), "zeroshot-hub:dev", &[], &daemon("10s"));
    assert!(matches!(
        &plan.steps[0],
        Step::Run { argv, may_fail: true } if argv.join(" ") == "docker rm --force zeroshot-hub"
    ));
    assert_eq!(docker_uninstall_plan().steps.len(), 1);
}

fn service(store: &HubStore, driver: FakeDriver, directory: &std::path::Path) -> HubService {
    HubService::open(
        store.clone(),
        Arc::new(driver),
        FileTargetRegistry::new(directory.join("targets.json")),
    )
    .expect("service")
}

#[tokio::test]
async fn daemon_pass_fires_places_and_persists() {
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
    let service = service(&store, FakeDriver::default(), directory.path());
    let (report, view) = service.pass().await.expect("pass");
    assert!(!report.is_idle());
    assert_eq!(view["schema"], json!("zeroshot.hub.tick/v1"));
    assert_eq!(view["running"], json!(1));
    let lock = store.lock().expect("lock");
    assert_eq!(
        store.load(&lock).expect("state").jobs[0].target.as_deref(),
        Some("local")
    );
    drop(lock);
    let (report, _) = service.pass().await.expect("idle pass");
    assert!(report.is_idle(), "a pass with nothing to do is idle");
}

#[tokio::test]
async fn reads_do_not_wait_for_a_pass_blocked_on_a_slow_target() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    {
        let lock = store.lock().expect("lock");
        let mut running = job("j1", 0);
        running.state = super::super::model::JobState::Running;
        running.target = Some("local".to_owned());
        running.run_id = Some("local-run-0".to_owned());
        let state = HubState {
            members: vec![member("local", &[], 1, 0)],
            jobs: vec![running],
            ..HubState::default()
        };
        store.save(&lock, &state).expect("seed");
    }
    let driver = FakeDriver {
        status_delay: Some(std::time::Duration::from_secs(30)),
        ..FakeDriver::default()
    };
    let service = Arc::new(service(&store, driver, directory.path()));
    let pass = tokio::spawn({
        let service = service.clone();
        async move { service.pass().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let query = super::super::wire::JobQuery {
        state: "all".to_owned(),
        target: None,
        labels: Vec::new(),
        limit: None,
    };
    let listed = tokio::time::timeout(std::time::Duration::from_secs(2), service.ps(query))
        .await
        .expect("ps answers while the pass waits on the target")
        .expect("ps");
    assert_eq!(listed["jobs"][0]["jobId"], json!("j1"));
    let dequeued = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        service.dequeue("missing"),
    )
    .await
    .expect("writes do not wait either");
    assert!(dequeued.is_err());
    pass.abort();
}

#[test]
fn interrupted_placements_return_to_the_queue_on_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    {
        let lock = store.lock().expect("lock");
        let mut placing = job("j1", 0);
        placing.state = super::super::model::JobState::Running;
        placing.target = Some("local".to_owned());
        let state = HubState {
            jobs: vec![placing],
            ..HubState::default()
        };
        store.save(&lock, &state).expect("seed");
    }
    drop(service(&store, FakeDriver::default(), directory.path()));
    let lock = store.lock().expect("lock");
    let state = store.load(&lock).expect("state");
    assert_eq!(state.jobs[0].state, super::super::model::JobState::Queued);
    assert_eq!(state.jobs[0].target, None);
}

#[test]
fn api_token_is_created_private_once_and_reused() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("hub").join("api-token");
    let token = load_or_create_token(&path).expect("token");
    assert_eq!(token.len(), 64);
    assert!(token.chars().all(|character| character.is_ascii_hexdigit()));
    assert_eq!(load_or_create_token(&path).expect("reuse"), token);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).expect("meta").permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    std::fs::write(&path, "\n").expect("empty");
    assert!(
        load_or_create_token(&path).is_err(),
        "an empty token is refused"
    );
}

#[test]
fn client_urls_reach_wildcard_listeners_through_loopback() {
    assert_eq!(
        client_url("0.0.0.0:7383".parse().expect("addr")),
        "http://127.0.0.1:7383"
    );
    assert_eq!(
        client_url("10.0.0.5:9000".parse().expect("addr")),
        "http://10.0.0.5:9000"
    );
    assert_eq!(
        client_url("[::]:7383".parse().expect("addr")),
        "http://[::1]:7383"
    );
}

#[test]
fn install_refuses_a_folder_owned_by_another_supervisor() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    refuse_other_owner(&store, "systemd").expect("no daemon owns the folder");
    let held = store.try_daemon_lock().expect("lock").expect("held");
    store
        .write_heartbeat(&json!({"manager": "docker"}))
        .expect("heartbeat");
    let error = refuse_other_owner(&store, "systemd").expect_err("docker owns it");
    assert_eq!(error.diagnostic()["code"], json!("hub.daemon_running"));
    assert!(error.to_string().contains("uninstall --docker"), "{error}");
    refuse_other_owner(&store, "docker").expect("reinstalling under docker replaces it");
    drop(held);
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
