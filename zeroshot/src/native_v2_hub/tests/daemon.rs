use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;
use zeroshot_engine::native_v2_cli::hub::HubApiOptions;

use super::super::daemon::{client_url, load_or_create_token};
use super::super::install::{
    InstallEnv, Step, daemon_arguments, docker_install_plan, docker_run, docker_uninstall_plan,
    refuse_other_owner, systemd_install_plan, systemd_quote, systemd_uninstall_plan, systemd_unit,
};
use super::super::model::{HubState, JobErrorKind, Schedule, ScheduleState};
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
    daemon_arguments(interval, &HubApiOptions::default())
}

#[test]
fn installed_daemons_pass_listen_and_token_file_through() {
    let arguments = daemon_arguments(
        "5s",
        &HubApiOptions {
            listen: Some("0.0.0.0:7383".to_owned()),
            token_file: Some("/srv/hub/token".into()),
            ..HubApiOptions::default()
        },
    );
    assert_eq!(
        arguments.join(" "),
        "hub daemon run --interval 5s --listen 0.0.0.0:7383 --token-file /srv/hub/token"
    );
    let with_metrics = daemon_arguments(
        "5s",
        &HubApiOptions {
            metrics_listen: Some("127.0.0.1:9383".to_owned()),
            metrics_labels: vec!["task".to_owned(), "eval.variant".to_owned()],
            ..HubApiOptions::default()
        },
    );
    assert_eq!(
        with_metrics.join(" "),
        "hub daemon run --interval 5s --metrics-listen 127.0.0.1:9383 --metrics-label task \
         --metrics-label eval.variant"
    );
    let limited = daemon_arguments(
        "5s",
        &HubApiOptions {
            unconfirmed_limit: Some(5),
            ..HubApiOptions::default()
        },
    );
    assert_eq!(
        limited.join(" "),
        "hub daemon run --interval 5s --unconfirmed-limit 5"
    );
    let stalled = daemon_arguments(
        "5s",
        &HubApiOptions {
            max_stall: Some("30m".to_owned()),
            ..HubApiOptions::default()
        },
    );
    assert_eq!(
        stalled.join(" "),
        "hub daemon run --interval 5s --max-stall 30m"
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

#[tokio::test]
async fn daemon_passes_confirm_unanswered_submissions_with_the_same_key() {
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
    let driver = FakeDriver {
        unanswered: std::sync::Mutex::new(1),
        ..FakeDriver::default()
    };
    let service = service(&store, driver, directory.path());
    let (_, view) = service.pass().await.expect("pass");
    assert_eq!(view["dispatchUnconfirmed"][0]["jobId"], json!("j1"));
    assert_eq!(view["dispatchFailed"], json!([]));
    let (_, view) = service.pass().await.expect("retry");
    assert_eq!(view["dispatched"][0]["runId"], json!("local-run-0"));
    let lock = store.lock().expect("lock");
    let state = store.load(&lock).expect("state");
    assert_eq!(state.jobs[0].attempts, 1);
    assert_eq!(state.jobs[0].error, None);
}

#[tokio::test]
async fn unconfirmed_submission_counts_survive_a_restart_up_to_the_limit() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    {
        let lock = store.lock().expect("lock");
        let mut placing = job("j1", 0);
        placing.state = super::super::model::JobState::Running;
        placing.target = Some("local".to_owned());
        placing.unconfirmed_submissions = 2;
        let state = HubState {
            members: vec![member("local", &[], 1, 0)],
            jobs: vec![placing],
            ..HubState::default()
        };
        store.save(&lock, &state).expect("seed");
    }
    let driver = FakeDriver {
        unanswered: std::sync::Mutex::new(1),
        ..FakeDriver::default()
    };
    let service = service(&store, driver, directory.path()).with_unconfirmed_limit(Some(3));
    let (_, view) = service.pass().await.expect("pass");
    assert_eq!(view["dispatchFailed"][0]["jobId"], json!("j1"));
    let lock = store.lock().expect("lock");
    let state = store.load(&lock).expect("state");
    assert_eq!(state.jobs[0].state, super::super::model::JobState::Failed);
    assert_eq!(state.jobs[0].unconfirmed_submissions, 3);
    assert_eq!(
        state.jobs[0].error.as_ref().map(|error| error.kind),
        Some(super::super::model::JobErrorKind::UnconfirmedLimit)
    );
}

#[tokio::test]
async fn a_job_dequeued_mid_submission_records_the_run_without_adopting_it() {
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
    let driver = FakeDriver {
        submit_delay: Some(std::time::Duration::from_secs(2)),
        ..FakeDriver::default()
    };
    let service = Arc::new(service(&store, driver, directory.path()));
    let pass = tokio::spawn({
        let service = service.clone();
        async move { service.pass().await }
    });
    let mut dequeued = None;
    for _ in 0..150 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        if service.snapshot().await.jobs[0].is_placing() {
            dequeued = Some(service.dequeue("j1").await.expect("dequeue"));
            break;
        }
    }
    let dequeued = dequeued.expect("the job is placing while its submission is in flight");
    assert_eq!(dequeued["job"]["state"], json!("dequeued"));
    pass.await.expect("join").expect("pass");
    let lock = store.lock().expect("lock");
    let state = store.load(&lock).expect("state");
    let job = &state.jobs[0];
    assert_eq!(job.state, super::super::model::JobState::Dequeued);
    assert_eq!(job.run_id, None, "the accepted run is not adopted");
    let error = job.error.as_ref().expect("error");
    assert_eq!(
        error.kind,
        super::super::model::JobErrorKind::DequeuedWhilePlacing
    );
    assert!(
        error
            .message
            .ends_with("local then accepted run local-run-0, which the hub does not follow"),
        "{}",
        error.message
    );
}

#[test]
fn interrupted_placements_stay_on_their_member_unconfirmed_on_open() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    {
        let lock = store.lock().expect("lock");
        let mut placing = job("j1", 0);
        placing.state = super::super::model::JobState::Running;
        placing.target = Some("local".to_owned());
        let mut unplaced = job("j2", 0);
        unplaced.state = super::super::model::JobState::Running;
        let state = HubState {
            jobs: vec![placing, unplaced],
            ..HubState::default()
        };
        store.save(&lock, &state).expect("seed");
    }
    drop(service(&store, FakeDriver::default(), directory.path()));
    let lock = store.lock().expect("lock");
    let state = store.load(&lock).expect("state");
    assert_eq!(state.jobs[0].state, super::super::model::JobState::Running);
    assert_eq!(state.jobs[0].target.as_deref(), Some("local"));
    assert_eq!(
        state.jobs[0].error.as_ref().map(|error| error.kind),
        Some(super::super::model::JobErrorKind::SubmissionUnconfirmed)
    );
    assert_eq!(
        state.jobs[0].attempts, 0,
        "the same submission key is reused"
    );
    assert_eq!(state.jobs[1].state, super::super::model::JobState::Queued);
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

#[tokio::test]
async fn the_daemon_serves_the_api_dispatches_and_stops_on_shutdown() {
    use zeroshot_engine::native_v2_cli::hub::{HubDaemonRun, HubSubmit};
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    let token_file = directory.path().join("secrets").join("token");
    let options = HubDaemonRun {
        interval: "1s".to_owned(),
        api: HubApiOptions {
            listen: Some("127.0.0.1:0".to_owned()),
            token_file: Some(token_file.clone()),
            metrics_listen: Some("127.0.0.1:0".to_owned()),
            metrics_labels: vec!["team".to_owned()],
            unconfirmed_limit: None,
            max_stall: None,
        },
    };
    let registry = directory.path().join("targets.json");
    let driver = Arc::new(FakeDriver::default());
    let fake = driver.clone();
    let open = |store| HubService::open(store, driver, FileTargetRegistry::new(registry.clone()));
    let observer = HubStore::new(directory.path().join("hub"));
    let folder = directory.path().to_path_buf();
    let client_token = token_file.clone();
    let drive = async move {
        let heartbeat = loop {
            if let Some(heartbeat) = observer.heartbeat() {
                break heartbeat;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        let url = heartbeat["url"].as_str().expect("url").to_owned();
        let client = super::super::client::HubClient::new(&super::super::context::Resolved {
            name: "local".to_owned(),
            url: url.clone(),
            token: super::super::context::TokenRef::File(client_token),
            source: "flag",
        })
        .expect("client");
        let mut output = Vec::new();
        super::super::client::execute(
            &client,
            zeroshot_engine::native_v2_cli::hub::HubCommand::Add(
                zeroshot_engine::native_v2_cli::hub::HubMemberAdd {
                    name: "local".to_owned(),
                    tags: Vec::new(),
                    slots: 1,
                    priority: 0,
                },
            ),
            &mut output,
        )
        .await
        .expect("add");
        // Queue more work than fits; the daemon's next pass places the queued job.
        for title in ["first", "second"] {
            super::super::client::execute(
                &client,
                zeroshot_engine::native_v2_cli::hub::HubCommand::Submit(Box::new(HubSubmit {
                    job: super::flows::spec(&folder, title, None),
                    detach: true,
                    wait: None,
                    queue: true,
                })),
                &mut output,
            )
            .await
            .expect("submit");
        }
        let metrics = heartbeat["metricsListen"].as_str().expect("metrics listen");
        let scraped = reqwest::get(format!("http://{metrics}/metrics"))
            .await
            .expect("metrics")
            .text()
            .await
            .expect("text");
        assert!(scraped.contains("zeroshot_hub_"), "{scraped}");
        // The first run finishing frees its slot; a later pass settles it and places the next.
        fake.finish_all();
        let passes = || {
            observer
                .heartbeat()
                .and_then(|heartbeat| heartbeat["passes"].as_u64())
                .unwrap_or(0)
        };
        let target = passes() + 2;
        loop {
            if passes() >= target {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        heartbeat
    };
    let mut output = Vec::new();
    let (result, heartbeat) = {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let daemon = super::super::daemon::Daemon {
            store: store.clone(),
            options: &options,
            output: &mut output,
        };
        let run = super::super::daemon::run_until(daemon, open, async {
            let _ = receiver.await;
        });
        let drive = async move {
            let heartbeat = drive.await;
            let _ = sender.send(());
            heartbeat
        };
        tokio::join!(run, drive)
    };
    result.expect("daemon");
    assert_eq!(heartbeat["manager"], json!("foreground"));
    assert_eq!(heartbeat["tokenFile"], json!(token_file));
    assert_eq!(
        heartbeat["mcp"],
        json!(format!("{}/mcp", heartbeat["url"].as_str().expect("url")))
    );
    let events = String::from_utf8(output).expect("utf8");
    let lines = events
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json line"))
        .collect::<Vec<_>>();
    assert_eq!(lines.first().expect("started")["event"], json!("started"));
    assert_eq!(lines.last().expect("stopped")["event"], json!("stopped"));
    assert!(
        lines.len() > 2,
        "a pass that placed work is reported: {events}"
    );
    let stopped = store.heartbeat().expect("heartbeat");
    assert!(stopped["stoppedAt"].is_string());
    let token =
        std::fs::read_to_string(directory.path().join("secrets").join("token")).expect("token");
    assert_eq!(token.trim().len(), 64);
}

#[tokio::test]
async fn the_daemon_refuses_bad_options_before_serving() {
    use zeroshot_engine::native_v2_cli::hub::HubDaemonRun;
    let directory = tempfile::tempdir().expect("tempdir");
    let registry = directory.path().join("targets.json");
    let open = |store| {
        HubService::open(
            store,
            Arc::new(FakeDriver::default()),
            FileTargetRegistry::new(registry.clone()),
        )
    };
    let options = |interval: &str, listen: &str, metrics: Option<&str>| HubDaemonRun {
        interval: interval.to_owned(),
        api: HubApiOptions {
            listen: Some(listen.to_owned()),
            token_file: Some(directory.path().join("token")),
            metrics_listen: metrics.map(str::to_owned),
            metrics_labels: Vec::new(),
            unconfirmed_limit: None,
            max_stall: None,
        },
    };
    // Each refusal gets its own hub folder: a just-released daemon lock can stay held for a
    // moment by a child another test is forking.
    let runs = std::sync::atomic::AtomicUsize::new(0);
    let run = |options: HubDaemonRun| {
        let next = runs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let store = HubStore::new(directory.path().join(format!("hub-{next}")));
        async move {
            let daemon = super::super::daemon::Daemon {
                store,
                options: &options,
                output: &mut Vec::new(),
            };
            super::super::daemon::run_until(daemon, open, async {}).await
        }
    };
    assert!(matches!(
        run(options("soon", "127.0.0.1:0", None)).await,
        Err(super::super::error::HubError::Usage(_))
    ));
    let bad_listen = run(options("1s", "256.0.0.1:0", None))
        .await
        .expect_err("bad address");
    assert!(
        bad_listen.to_string().contains("could not listen"),
        "{bad_listen}"
    );
    let public = run(options("1s", "127.0.0.1:0", Some("0.0.0.0:0")))
        .await
        .expect_err("public metrics");
    assert!(
        public.to_string().contains("not a loopback address"),
        "{public}"
    );
    std::fs::write(directory.path().join("token"), "\n").expect("empty token");
    let empty = run(options("1s", "127.0.0.1:0", None))
        .await
        .expect_err("empty token");
    assert!(empty.to_string().contains("is empty"), "{empty}");
    let held = HubStore::new(directory.path().join("hub-4"))
        .try_daemon_lock()
        .expect("lock")
        .expect("held");
    let owned = run(options("1s", "127.0.0.1:0", None))
        .await
        .expect_err("owned");
    assert_eq!(owned.diagnostic()["code"], json!("hub.daemon_running"));
    drop(held);
    std::fs::write(directory.path().join("token"), "a-token\n").expect("token");
    let metrics = run(options("1s", "127.0.0.1:0", Some("256.0.0.1:0")))
        .await
        .expect_err("bad metrics address");
    assert!(
        metrics.to_string().contains("metrics could not listen"),
        "{metrics}"
    );
}

/// Service installs read `HOME` and the user, which Windows does not provide.
#[cfg(unix)]
#[test]
fn install_env_reads_this_shells_facts() {
    let directory = tempfile::tempdir().expect("tempdir");
    let registry = directory.path().join("config").join("targets.json");
    let env = InstallEnv::detect(&registry, &directory.path().join("hub")).expect("detect");
    assert!(env.executable.is_absolute());
    assert_eq!(env.config_dir, directory.path().join("config"));
    assert_eq!(env.hub_dir, directory.path().join("hub"));
    assert!(env.user_unit_dir.ends_with("systemd/user"));
    assert!(!env.user.is_empty());
    assert!(InstallEnv::detect(std::path::Path::new(""), directory.path()).is_err());
}

#[tokio::test]
async fn install_and_uninstall_print_plans_on_dry_runs() {
    use super::super::install::{install, uninstall};
    use zeroshot_engine::native_v2_cli::hub::{HubInstall, HubManager, HubUninstall};
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    let request = |manager, dry_run| HubInstall {
        manager,
        system: false,
        image: None,
        volumes: vec![PathBuf::from("/srv/repos")],
        interval: "5s".to_owned(),
        api: HubApiOptions {
            listen: None,
            token_file: Some(PathBuf::from("relative-token")),
            metrics_listen: None,
            metrics_labels: Vec::new(),
            unconfirmed_limit: None,
            max_stall: None,
        },
        dry_run,
    };
    for (manager, name) in [
        (HubManager::Systemd, "systemd"),
        (HubManager::Docker, "docker"),
    ] {
        let mut output = Vec::new();
        install(&env(), request(manager, true), &store, &mut output)
            .await
            .expect("dry run");
        let document: serde_json::Value = serde_json::from_slice(&output).expect("json");
        assert_eq!(document["action"], json!("install"));
        assert_eq!(document["manager"], json!(name));
        assert_eq!(document["dryRun"], json!(true));
        let steps = document["steps"].to_string();
        // The relative token file is resolved against the invoking folder. Windows paths are
        // escaped once more inside unit files, so compare the unescaped form only on Unix.
        let token = std::env::current_dir().expect("cwd").join("relative-token");
        if cfg!(unix) {
            assert!(steps.contains(&*token.to_string_lossy()), "{steps}");
        }
        assert!(steps.contains("relative-token"), "{steps}");
        assert!(document.get("results").is_none());

        let mut output = Vec::new();
        uninstall(
            &env(),
            HubUninstall {
                manager,
                system: true,
                dry_run: true,
            },
            &mut output,
        )
        .await
        .expect("dry uninstall");
        let document: serde_json::Value = serde_json::from_slice(&output).expect("json");
        assert_eq!(document["action"], json!("uninstall"));
        assert!(
            document["steps"]
                .as_array()
                .is_some_and(|steps| !steps.is_empty())
        );
    }

    // A real install never starts while a daemon under another manager owns the folder.
    let held = store.try_daemon_lock().expect("lock").expect("held");
    store
        .write_heartbeat(&json!({"manager": "foreground"}))
        .expect("heartbeat");
    let refused = install(
        &env(),
        request(HubManager::Docker, false),
        &store,
        &mut Vec::new(),
    )
    .await
    .expect_err("owned");
    assert_eq!(refused.diagnostic()["code"], json!("hub.daemon_running"));
    drop(held);
}

#[tokio::test]
async fn install_steps_apply_in_order_and_stop_at_a_required_failure() {
    use super::super::install::{apply_steps, require_program};
    let directory = tempfile::tempdir().expect("tempdir");
    let unit = directory.path().join("units").join("zeroshot-hub.service");
    let missing = require_program("zeroshot-no-such-manager", "systemd")
        .await
        .expect_err("absent");
    assert!(missing.to_string().contains("--dry-run"), "{missing}");
    let shell = if cfg!(windows) { "cmd" } else { "sh" };
    let (ok, fail) = if cfg!(windows) {
        (["/C", "exit 0"], ["/C", "exit 1"])
    } else {
        (["-c", "exit 0"], ["-c", "exit 1"])
    };
    let run = |argv: [&str; 2], may_fail: bool| Step::Run {
        argv: std::iter::once(shell)
            .chain(argv)
            .map(str::to_owned)
            .collect(),
        may_fail,
    };
    let results = apply_steps(&[
        Step::Write {
            path: unit.clone(),
            content: "[Unit]\n".to_owned(),
        },
        run(ok, false),
        run(fail, true),
        Step::Run {
            argv: vec!["zeroshot-no-such-program".to_owned()],
            may_fail: true,
        },
        Step::Remove { path: unit.clone() },
        Step::Remove { path: unit.clone() },
    ])
    .await
    .expect("apply");
    assert_eq!(results.len(), 6);
    assert_eq!(results[1]["ok"], json!(true));
    assert_eq!(results[2]["ok"], json!(false));
    assert!(
        results[3]["stderr"]
            .as_str()
            .is_some_and(|stderr| stderr.contains("could not start"))
    );
    assert!(!unit.exists());
    let failed = apply_steps(&[run(fail, false), Step::Remove { path: unit }])
        .await
        .expect_err("required step fails");
    let super::super::error::HubError::Target { diagnostic, .. } = &failed else {
        panic!("{failed:?}");
    };
    assert_eq!(
        diagnostic.as_ref().expect("steps")["steps"]
            .as_array()
            .map(Vec::len),
        Some(1),
        "later steps never run"
    );
}

#[test]
fn the_store_refuses_malformed_state_and_defaults_beside_the_registry() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    let lock = store.lock().expect("lock");
    assert!(
        store.load(&lock).expect("absent state").jobs.is_empty(),
        "a fresh hub folder starts empty"
    );
    std::fs::write(
        directory.path().join("hub").join("state.json"),
        b"{not json",
    )
    .expect("write");
    let error = store.load(&lock).expect_err("malformed state").to_string();
    assert!(error.contains("state.json is malformed"), "{error}");
    std::fs::write(directory.path().join("hub").join("daemon.json"), b"[").expect("write");
    assert!(
        store.heartbeat().is_none(),
        "a torn heartbeat reads as none"
    );
    drop(lock);

    if std::env::var_os("ZEROSHOT_HUB_DIR").is_none() {
        assert_eq!(
            HubStore::default_directory(Path::new("/srv/zeroshot/targets.json")),
            Path::new("/srv/zeroshot/hub")
        );
        assert_eq!(HubStore::default_directory(Path::new("")), Path::new("hub"));
    }
}

#[test]
fn legacy_lifetimes_load_and_are_dropped_on_the_next_write() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    let state = HubState {
        members: vec![member("box", &[], 1, 0)],
        jobs: vec![job("hj_legacy", 0)],
        schedules: vec![Schedule {
            schedule_id: "hs_legacy".to_owned(),
            state: ScheduleState::Active,
            cron: "* * * * *".to_owned(),
            timezone: "UTC".to_owned(),
            template: job("hs_legacy", 0),
            repin_from: None,
            submission_key: None,
            created_at_ms: 0,
            next_fire_ms: None,
            last_fired_ms: None,
            last_job_id: None,
            fired: 0,
            skipped: 0,
            removed_at_ms: None,
            error: None,
        }],
        ..HubState::default()
    };
    let mut legacy = serde_json::to_value(&state).expect("state json");
    legacy["members"][0]["expiresAt"] = json!("2026-01-01T00:00:00Z");
    legacy["members"][0]["ttlMs"] = json!(3_600_000);
    legacy["jobs"][0]["expiresAt"] = json!("2026-01-01T00:00:00Z");
    legacy["jobs"][0]["error"] = json!({
        "kind": "run_expired",
        "message": "the job reached its deadline",
        "deadline": "2026-01-01T00:00:00Z"
    });
    legacy["schedules"][0]["expiresAfterMs"] = json!(7_200_000);
    legacy["retiredField"] = json!(true);
    std::fs::create_dir_all(directory.path().join("hub")).expect("hub folder");
    std::fs::write(
        directory.path().join("hub").join("state.json"),
        serde_json::to_vec_pretty(&legacy).expect("legacy bytes"),
    )
    .expect("seed legacy state");

    let lock = store.lock().expect("lock");
    let loaded = store.load(&lock).expect("legacy state loads");
    assert!(matches!(
        loaded.jobs[0].error.as_ref().map(|error| error.kind),
        Some(JobErrorKind::Unclassified)
    ));
    store.save(&lock, &loaded).expect("rewrite state");
    let rewritten: serde_json::Value = serde_json::from_slice(
        &std::fs::read(directory.path().join("hub").join("state.json")).expect("rewritten bytes"),
    )
    .expect("rewritten json");
    for pointer in [
        "/members/0/expiresAt",
        "/members/0/ttlMs",
        "/jobs/0/expiresAt",
        "/schedules/0/expiresAfterMs",
        "/retiredField",
    ] {
        assert!(
            rewritten.pointer(pointer).is_none(),
            "{pointer} was dropped on write"
        );
    }
    assert_eq!(
        rewritten.pointer("/jobs/0/error/kind"),
        Some(&json!("unclassified"))
    );
}

#[tokio::test]
async fn the_daemon_reports_failing_passes_and_keeps_serving() {
    use zeroshot_engine::native_v2_cli::hub::HubDaemonRun;
    let directory = tempfile::tempdir().expect("tempdir");
    let store = HubStore::new(directory.path().join("hub"));
    let registry = directory.path().join("targets.json");
    let state_file = directory.path().join("hub").join("state.json");
    // A due cron schedule makes the first pass change, and so save, the state.
    let seeded = HubState {
        schedules: vec![Schedule {
            schedule_id: "hs_due".to_owned(),
            state: ScheduleState::Active,
            cron: "* * * * *".to_owned(),
            timezone: "UTC".to_owned(),
            template: job("hs_due", 0),
            repin_from: None,
            submission_key: None,
            created_at_ms: 0,
            next_fire_ms: Some(0),
            last_fired_ms: None,
            last_job_id: None,
            fired: 0,
            skipped: 0,
            removed_at_ms: None,
            error: None,
        }],
        ..HubState::default()
    };
    store
        .save(&store.lock().expect("lock"), &seeded)
        .expect("seed");
    let open = |store| {
        let service = HubService::open(
            store,
            Arc::new(FakeDriver::default()),
            FileTargetRegistry::new(registry.clone()),
        )?;
        // Every later save fails: the state file it replaces is a non-empty folder.
        std::fs::remove_file(&state_file).expect("unseed");
        std::fs::create_dir_all(state_file.join("inside")).expect("block replacing");
        Ok(service)
    };
    let options = HubDaemonRun {
        interval: "1s".to_owned(),
        api: HubApiOptions {
            listen: Some("127.0.0.1:0".to_owned()),
            token_file: Some(directory.path().join("token")),
            metrics_listen: None,
            metrics_labels: Vec::new(),
            unconfirmed_limit: None,
            max_stall: Some("1h".to_owned()),
        },
    };
    let mut output = Vec::new();
    let daemon = super::super::daemon::Daemon {
        store: store.clone(),
        options: &options,
        output: &mut output,
    };
    super::super::daemon::run_until(daemon, open, async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    })
    .await
    .expect("a failing pass does not stop the daemon");
    let lines = String::from_utf8(output)
        .expect("utf8")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json line"))
        .collect::<Vec<_>>();
    assert_eq!(lines.first().expect("started")["event"], json!("started"));
    assert!(lines.first().expect("started")["metricsListen"].is_null());
    assert_eq!(
        lines.first().expect("started")["maxStallMs"],
        json!(3_600_000)
    );
    let failures = lines
        .iter()
        .filter(|line| line.to_string().contains("state could not be replaced"))
        .count();
    assert_eq!(
        failures, 2,
        "the dispatch and the eval pass both report: {lines:?}"
    );
    let stopped = lines.last().expect("stopped");
    assert_eq!(stopped["event"], json!("stopped"));
    assert!(
        stopped["lastError"]
            .as_str()
            .is_some_and(|error| error.contains("could not be replaced")),
        "{stopped}"
    );
}

#[test]
fn an_unreadable_token_file_is_a_storage_error() {
    let directory = tempfile::tempdir().expect("tempdir");
    let error = load_or_create_token(directory.path()).expect_err("a folder is no token file");
    assert!(error.to_string().contains("could not be read"), "{error}");
}

#[test]
fn the_store_reports_each_file_it_cannot_use() {
    let directory = tempfile::tempdir().expect("tempdir");
    let message = |error: super::super::error::HubError| error.to_string();

    // The hub folder itself is a file.
    let blocked = directory.path().join("blocked");
    std::fs::write(&blocked, b"").expect("file");
    let store = HubStore::new(blocked);
    assert!(message(store.lock().err().expect("lock")).contains("could not be created"));
    assert!(
        message(store.try_daemon_lock().err().expect("daemon lock"))
            .contains("could not be created")
    );

    // Each lock file, state file, and staging file is a folder.
    let hub = directory.path().join("hub");
    let store = HubStore::new(hub.clone());
    for name in ["hub.lock", "daemon.lock"] {
        std::fs::create_dir_all(hub.join(name)).expect("folder");
    }
    assert!(message(store.lock().err().expect("lock")).contains("lock could not be opened"));
    assert!(
        message(store.try_daemon_lock().err().expect("daemon lock"))
            .contains("daemon lock could not be opened")
    );
    std::fs::remove_dir(hub.join("hub.lock")).expect("unblock");
    let lock = store.lock().expect("lock");
    std::fs::create_dir_all(hub.join("state.json").join("inside")).expect("folder");
    assert!(message(store.load(&lock).expect_err("read")).contains("state could not be read"));
    assert!(
        message(
            store
                .save(&lock, &HubState::default())
                .expect_err("replace")
        )
        .contains("state could not be replaced")
    );
    std::fs::create_dir_all(hub.join("daemon.json").join("inside")).expect("folder");
    assert!(
        message(store.write_heartbeat(&json!({})).expect_err("replace"))
            .contains("state could not be replaced")
    );
}
