//! End to end through the hub API: the CLI client, the HTTP routes, and the service.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
#[cfg(unix)]
use zeroshot_engine::native_v2_cli::hub::HubLogs;
use zeroshot_engine::native_v2_cli::hub::{
    HubApiOptions, HubCommand, HubContextCommand, HubDaemonRun, HubEnqueue, HubInstall, HubJobSpec,
    HubManager, HubMemberAdd, HubMemberState, HubMemberUpdate, HubPs, HubStats, HubStrategy,
    HubSubmit, HubUninstall,
};

use super::super::api::{ApiState, metrics_router, router};
use super::super::client::{HubClient, execute, job_files, schedule};
use super::super::context::{Resolved, TokenRef};
use super::super::error::HubError;
#[cfg(unix)]
use super::super::model::JobState;
use super::super::service::HubService;
use super::super::store::HubStore;
use super::FakeDriver;
use crate::native_v2_target::{FileTargetRegistry, TargetAccess, TargetRecord, TargetRegistry};

const TOKEN: &str = "fedcba9876543210fedcba9876543210";

pub(super) struct Hub {
    pub url: String,
    pub service: Arc<HubService>,
    pub driver: Arc<FakeDriver>,
    pub client: HubClient,
    pub directory: tempfile::TempDir,
}

/// A hub served over HTTP with `driver`; `box` is registered as a direct target.
pub(super) async fn hub_with(driver: FakeDriver, executable: Option<PathBuf>) -> Hub {
    let directory = tempfile::tempdir().expect("tempdir");
    let registry = FileTargetRegistry::new(directory.path().join("targets.json"));
    registry
        .insert(TargetRecord {
            id: "00000000-0000-4000-8000-0000000000b0".to_owned(),
            name: "box".to_owned(),
            origin: "http://127.0.0.1:9".to_owned(),
            access: TargetAccess::Direct,
        })
        .expect("register box");
    let driver = Arc::new(driver);
    let service = Arc::new(
        HubService::open(
            HubStore::new(directory.path().join("hub")),
            driver.clone(),
            registry,
        )
        .expect("service"),
    );
    let state = Arc::new(ApiState {
        service: service.clone(),
        token: TOKEN.to_owned(),
        heartbeat: Arc::new(Mutex::new(json!({"schema": "zeroshot.hub.daemon/v1"}))),
        executable,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, router(state)).await });
    let token = directory.path().join("token");
    std::fs::write(&token, TOKEN).expect("token");
    let client = HubClient::new(&Resolved {
        name: "lab".to_owned(),
        url: url.clone(),
        token: TokenRef::File(token),
        source: "flag",
    })
    .expect("client");
    Hub {
        url,
        service,
        driver,
        client,
        directory,
    }
}

async fn run(client: &HubClient, command: HubCommand) -> Result<Value, HubError> {
    let mut output = Vec::new();
    execute(client, command, &mut output).await?;
    Ok(serde_json::from_slice(&output).expect("json output"))
}

async fn expect_code(client: &HubClient, command: HubCommand, code: &str) -> HubError {
    let error = run(client, command).await.expect_err(code);
    assert_eq!(error.diagnostic()["code"], json!(code), "{error:?}");
    error
}

pub(super) fn spec(folder: &Path, title: &str, target: Option<&str>) -> HubJobSpec {
    let input = folder.join(format!("{title}.json"));
    std::fs::write(&input, "{\"task\":\"flows\"}").expect("input");
    HubJobSpec {
        title: title.to_owned(),
        run_args: vec![
            "--title".to_owned(),
            title.to_owned(),
            "--input".to_owned(),
            input.display().to_string(),
        ],
        target: target.map(str::to_owned),
        tags: Vec::new(),
        strategy: HubStrategy::FirstFree,
        labels: vec![("team".to_owned(), "flows".to_owned())],
        runtime_files: Vec::new(),
        repository: None,
        branch: None,
        revision: None,
        source_connection: None,
        environment: false,
        cwd: Some(folder.to_path_buf()),
        submission_key: None,
    }
}

fn submit(job: HubJobSpec) -> HubCommand {
    HubCommand::Submit(Box::new(HubSubmit { job, detach: true }))
}

fn add(name: &str, slots: u32) -> HubCommand {
    HubCommand::Add(HubMemberAdd {
        name: name.to_owned(),
        tags: vec!["linux".to_owned()],
        slots,
        priority: 0,
    })
}

fn ps(state: &str) -> HubCommand {
    HubCommand::Ps(HubPs {
        state: state.to_owned(),
        target: None,
        labels: Vec::new(),
        limit: None,
    })
}

#[tokio::test]
async fn members_are_added_updated_listed_and_removed_through_the_api() {
    let hub = hub_with(FakeDriver::default(), None).await;
    let client = &hub.client;
    let local = run(client, add("local", 2)).await.expect("add local");
    assert_eq!(local["member"]["name"], json!("local"));
    let boxed = run(client, add("box", 0)).await.expect("add box");
    assert_eq!(boxed["member"]["slots"], json!(1), "slots are at least one");
    expect_code(client, add("box", 1), "hub.conflict").await;
    let missing = expect_code(client, add("ghost", 1), "hub.member_not_found").await;
    assert!(missing.to_string().contains("zeroshot target add ghost"));

    // A queued job shows up as matching the members that could take it.
    run(
        client,
        HubCommand::Enqueue(Box::new(HubEnqueue {
            job: spec(hub.directory.path(), "later", None),
            at: Some("+1h".to_owned()),
            cron: None,
            cron_timezone: None,
            priority: 0,
            expires: None,
        })),
    )
    .await
    .expect("enqueue");

    let updated = run(
        client,
        HubCommand::Update(HubMemberUpdate {
            name: "box".to_owned(),
            add_tags: vec!["gpu".to_owned()],
            remove_tags: vec!["linux".to_owned()],
            slots: Some(0),
            priority: Some(7),
            state: Some(HubMemberState::Drain),
        }),
    )
    .await
    .expect("update");
    assert_eq!(updated["member"]["tags"], json!(["gpu"]));
    assert_eq!(updated["member"]["slots"], json!(1));
    assert_eq!(updated["member"]["priority"], json!(7));
    assert_eq!(updated["member"]["state"], json!("drain"));
    expect_code(
        client,
        HubCommand::Update(HubMemberUpdate {
            name: "ghost".to_owned(),
            add_tags: Vec::new(),
            remove_tags: Vec::new(),
            slots: None,
            priority: None,
            state: None,
        }),
        "hub.member_not_found",
    )
    .await;

    let listed = run(
        client,
        HubCommand::List {
            tags: Vec::new(),
            probe: false,
        },
    )
    .await
    .expect("list");
    let targets = listed["targets"].as_array().expect("targets");
    assert_eq!(targets.len(), 2);
    let boxed = targets
        .iter()
        .find(|target| target["name"] == json!("box"))
        .expect("box");
    assert_eq!(boxed["kind"], json!("direct"));
    assert_eq!(boxed["origin"], json!("http://127.0.0.1:9"));
    assert!(boxed.get("health").is_none(), "no probe, no health");
    let local = targets
        .iter()
        .find(|target| target["name"] == json!("local"))
        .expect("local");
    assert_eq!(local["kind"], json!("local"));
    assert_eq!(boxed["queuedMatching"], json!(0), "{boxed}");
    assert_eq!(local["queuedMatching"], json!(1), "{local}");

    let probed = run(
        client,
        HubCommand::List {
            tags: vec!["gpu".to_owned()],
            probe: true,
        },
    )
    .await
    .expect("probe");
    let targets = probed["targets"].as_array().expect("targets");
    assert_eq!(targets.len(), 1, "{probed}");
    assert_eq!(targets[0]["health"], json!("ready"));
    assert_eq!(targets[0]["free"], json!(1));
    assert_eq!(targets[0]["hubRunning"], json!(0));

    let removed = run(
        client,
        HubCommand::Remove {
            name: "box".to_owned(),
        },
    )
    .await
    .expect("remove");
    assert_eq!(removed["removed"], json!("box"));
    expect_code(
        client,
        HubCommand::Remove {
            name: "box".to_owned(),
        },
        "hub.member_not_found",
    )
    .await;
}

#[tokio::test]
async fn unreachable_members_probe_as_unreachable() {
    let driver = FakeDriver {
        unreachable: vec!["box".to_owned()],
        ..FakeDriver::default()
    };
    let hub = hub_with(driver, None).await;
    run(&hub.client, add("box", 1)).await.expect("add");
    let probed = run(
        &hub.client,
        HubCommand::List {
            tags: Vec::new(),
            probe: true,
        },
    )
    .await
    .expect("probe");
    assert_eq!(probed["targets"][0]["health"], json!("unreachable"));
    assert!(probed["targets"][0]["probeError"].is_string(), "{probed}");
}

#[tokio::test]
async fn submissions_place_and_refuse_through_the_api() {
    let hub = hub_with(FakeDriver::default(), None).await;
    let client = &hub.client;
    let folder = hub.directory.path();
    run(client, add("local", 1)).await.expect("add");

    let mut keyed = spec(folder, "first", None);
    keyed.submission_key = Some("first-key".to_owned());
    let placed = run(client, submit(keyed.clone())).await.expect("submit");
    assert_eq!(placed["job"]["state"], json!("running"));
    assert_eq!(placed["job"]["target"], json!("local"));
    let job_id = placed["job"]["jobId"].as_str().expect("job id").to_owned();
    let again = run(client, submit(keyed)).await.expect("same key");
    assert_eq!(
        again["job"]["jobId"],
        json!(job_id),
        "submission keys dedupe"
    );
    assert_eq!(hub.driver.submissions.lock().expect("lock").len(), 1);

    // The only slot is taken: submit cannot place it now.
    let busy = expect_code(
        client,
        submit(spec(folder, "second", None)),
        "hub.no_capacity",
    )
    .await;
    assert_eq!(busy.exit_code(), super::super::error::EXIT_NO_CAPACITY);
    expect_code(
        client,
        submit(spec(folder, "elsewhere", Some("box"))),
        "hub.no_eligible_target",
    )
    .await;

    // Status reads the run from its target; a finished run settles the job.
    let status = run(
        client,
        HubCommand::Status {
            job: job_id.clone(),
        },
    )
    .await
    .expect("status");
    assert_eq!(status["run"]["status"]["phase"], json!("running"));
    let refused = expect_code(
        client,
        HubCommand::Dequeue {
            job: job_id.clone(),
        },
        "hub.invalid_state",
    )
    .await;
    assert!(refused.to_string().contains("force-stop"), "{refused}");
    hub.driver.finish_all();
    let finished = run(
        client,
        HubCommand::Status {
            job: job_id.clone(),
        },
    )
    .await
    .expect("finished");
    assert_eq!(finished["job"]["state"], json!("succeeded"), "{finished}");
    let dispatched = run(client, HubCommand::Dispatch).await.expect("dispatch");
    assert!(dispatched.is_object(), "{dispatched}");
    let all = run(client, ps("all")).await.expect("ps all");
    assert!(!all["jobs"].as_array().expect("jobs").is_empty(), "{all}");
    let limited = run(
        client,
        HubCommand::Ps(HubPs {
            state: "all".to_owned(),
            target: Some("local".to_owned()),
            labels: vec![("team".to_owned(), "flows".to_owned())],
            limit: Some(1),
        }),
    )
    .await
    .expect("ps limit");
    assert_eq!(limited["jobs"].as_array().expect("jobs").len(), 1);
    let none = run(
        client,
        HubCommand::Ps(HubPs {
            state: "all".to_owned(),
            target: None,
            labels: Vec::new(),
            limit: Some(0),
        }),
    )
    .await
    .expect("ps zero");
    assert_eq!(none["jobs"], json!([]));
    expect_code(client, ps("sideways"), "hub.invalid_request").await;

    let stats = run(
        client,
        HubCommand::Stats(HubStats {
            by: vec!["target".to_owned()],
            since: Some("7d".to_owned()),
            until: None,
            prices: None,
        }),
    )
    .await
    .expect("stats");
    assert!(stats.is_object(), "{stats}");
}

#[tokio::test]
async fn recurring_schedules_are_created_listed_and_removed() {
    let hub = hub_with(FakeDriver::default(), None).await;
    let client = &hub.client;
    run(client, add("local", 1)).await.expect("add");
    let mut job = spec(hub.directory.path(), "nightly", None);
    job.submission_key = Some("nightly-key".to_owned());
    let enqueue = |job: HubJobSpec, expires: Option<&str>| {
        HubCommand::Enqueue(Box::new(HubEnqueue {
            job,
            at: None,
            cron: Some("0 3 * * *".to_owned()),
            cron_timezone: Some("Europe/Belgrade".to_owned()),
            priority: 2,
            expires: expires.map(str::to_owned),
        }))
    };
    let created = run(client, enqueue(job.clone(), Some("2h")))
        .await
        .expect("schedule");
    let schedule_id = created["schedule"]["scheduleId"]
        .as_str()
        .expect("schedule id")
        .to_owned();
    assert!(schedule_id.starts_with("hs_"), "{created}");
    assert_eq!(created["schedule"]["timezone"], json!("Europe/Belgrade"));
    let again = run(client, enqueue(job.clone(), Some("2h")))
        .await
        .expect("same key");
    assert_eq!(again["schedule"]["scheduleId"], json!(schedule_id));
    expect_code(
        client,
        enqueue(job.clone(), Some("tomorrow")),
        "hub.invalid_request",
    )
    .await;
    expect_code(
        client,
        enqueue(spec(hub.directory.path(), "nowhere", Some("box")), None),
        "hub.no_eligible_target",
    )
    .await;

    let recurring = run(client, ps("recurring")).await.expect("ps recurring");
    assert_eq!(
        recurring["schedules"][0]["scheduleId"],
        json!(schedule_id),
        "{recurring}"
    );
    let removed = run(
        client,
        HubCommand::Dequeue {
            job: schedule_id.clone(),
        },
    )
    .await
    .expect("remove schedule");
    assert_eq!(removed["schedule"]["state"], json!("removed"));
    expect_code(
        client,
        HubCommand::Dequeue { job: schedule_id },
        "hub.invalid_state",
    )
    .await;
    let active = run(client, ps("recurring")).await.expect("ps");
    assert_eq!(active["schedules"], json!([]));
}

#[test]
fn schedules_are_built_from_enqueue_flags() {
    let directory = tempfile::tempdir().expect("tempdir");
    let request = |cron: &str, zone: Option<&str>| HubEnqueue {
        job: spec(directory.path(), "built", None),
        at: None,
        cron: Some(cron.to_owned()),
        cron_timezone: zone.map(str::to_owned),
        priority: 5,
        expires: None,
    };
    let (built, files) = schedule(request("*/5 * * * *", None)).expect("schedule");
    assert!(!built.timezone.is_empty(), "the local zone is the default");
    assert_eq!(built.template.priority, 5);
    assert_eq!(built.template.job_id, built.schedule_id);
    assert!(built.next_fire_ms.is_some());
    assert_eq!(files.len(), 1, "the input file is uploaded");
    assert!(schedule(request("not a cron", None)).is_err());
    assert!(schedule(request("0 3 * * *", Some("Mars/Olympus"))).is_err());
}

#[tokio::test]
async fn stats_validate_dimensions_and_price_tables_on_the_client() {
    let hub = hub_with(FakeDriver::default(), None).await;
    let client = &hub.client;
    let stats = |by: &str, prices: Option<PathBuf>, until: Option<&str>| {
        HubCommand::Stats(HubStats {
            by: vec![by.to_owned()],
            since: None,
            until: until.map(str::to_owned),
            prices,
        })
    };
    let prices = hub.directory.path().join("prices.json");
    std::fs::write(
        &prices,
        r#"{"big": {"input": 3, "output": 15, "cacheRead": 0.3, "cacheCreation": 3.75}}"#,
    )
    .expect("prices");
    let priced = run(client, stats("model", Some(prices), Some("1h")))
        .await
        .expect("priced");
    assert!(priced.is_object());
    assert!(matches!(
        run(client, stats("colour", None, None)).await,
        Err(HubError::Usage(_))
    ));
    let missing = run(
        client,
        stats("model", Some(hub.directory.path().join("none.json")), None),
    )
    .await
    .expect_err("missing prices");
    assert!(
        missing.to_string().contains("could not be read"),
        "{missing}"
    );
    let garbage = hub.directory.path().join("garbage.json");
    std::fs::write(&garbage, "not json").expect("garbage");
    let invalid = run(client, stats("model", Some(garbage), None))
        .await
        .expect_err("invalid prices");
    assert!(invalid.to_string().contains("price table"), "{invalid}");
    let wrong_shape = hub.directory.path().join("shape.json");
    std::fs::write(&wrong_shape, "[1, 2]").expect("shape");
    expect_code(
        client,
        stats("model", Some(wrong_shape), None),
        "hub.invalid_request",
    )
    .await;
}

#[tokio::test]
async fn machine_local_commands_are_refused_through_a_context() {
    let hub = hub_with(FakeDriver::default(), None).await;
    let api = HubApiOptions {
        hetzner_token_file: None,
        listen: None,
        token_file: None,
        metrics_listen: None,
        metrics_labels: Vec::new(),
        unconfirmed_limit: None,
    };
    for command in [
        HubCommand::Daemon(HubDaemonRun {
            interval: "5s".to_owned(),
            api: api.clone(),
        }),
        HubCommand::Install(HubInstall {
            manager: HubManager::Docker,
            system: false,
            image: None,
            volumes: Vec::new(),
            interval: "5s".to_owned(),
            api,
            dry_run: true,
        }),
        HubCommand::Uninstall(HubUninstall {
            manager: HubManager::Systemd,
            system: false,
            dry_run: true,
        }),
        HubCommand::Context(HubContextCommand::List),
    ] {
        let error = run(&hub.client, command).await.expect_err("local only");
        assert!(matches!(error, HubError::Usage(_)), "{error:?}");
    }
}

#[test]
fn job_files_refuse_missing_and_oversized_inputs() {
    let directory = tempfile::tempdir().expect("tempdir");
    let mut job = super::job("hj_files", 0);
    job.cwd = directory.path().to_path_buf();
    job.run_args = ["--input", "missing.json"].map(str::to_owned).to_vec();
    let missing = job_files(&job).expect_err("missing");
    assert!(
        missing.to_string().contains("could not be read"),
        "{missing}"
    );
    let large = directory.path().join("large.json");
    let file = std::fs::File::create(&large).expect("create");
    file.set_len(8 * 1024 * 1024 + 1).expect("grow");
    job.run_args = ["--input", "large.json"].map(str::to_owned).to_vec();
    let oversized = job_files(&job).expect_err("too large");
    assert!(
        oversized.to_string().contains("larger than 8 MiB"),
        "{oversized}"
    );
    std::fs::write(directory.path().join("binary.json"), [0xff, 0xfe, 0x00]).expect("binary");
    job.run_args = ["--input", "binary.json", "--input", "binary.json"]
        .map(str::to_owned)
        .to_vec();
    assert!(job_files(&job).is_err(), "non-UTF-8 files are refused");
}

#[tokio::test]
async fn uploads_must_name_a_file_argument_of_the_run() {
    let hub = hub_with(FakeDriver::default(), None).await;
    run(&hub.client, add("local", 1)).await.expect("add");
    let mut job = super::job("hj_0123456789abcdef0123456789abcdef", 1);
    job.cwd = hub.directory.path().to_path_buf();
    let body = json!({
        "job": job,
        "files": [{"argument": "elsewhere.json", "content": "{}"}],
    });
    let response = reqwest::Client::new()
        .post(format!("{}/v1/hub/jobs", hub.url))
        .bearer_auth(TOKEN)
        .json(&body)
        .send()
        .await
        .expect("submit");
    assert_eq!(response.status(), 400);
    let document: Value = response.json().await.expect("json");
    assert!(
        document["message"]
            .as_str()
            .is_some_and(|message| message.contains("not a file argument")),
        "{document}"
    );
    let unknown = reqwest::Client::new()
        .get(format!("{}/v1/hub/nowhere", hub.url))
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("unknown route");
    assert_eq!(unknown.status(), 400);
}

#[tokio::test]
async fn the_metrics_listener_serves_only_metrics() {
    let hub = hub_with(FakeDriver::default(), None).await;
    let state = Arc::new(ApiState {
        service: hub.service.clone(),
        token: TOKEN.to_owned(),
        heartbeat: Arc::new(Mutex::new(json!({}))),
        executable: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, metrics_router(state)).await });
    let http = reqwest::Client::new();
    let metrics = http
        .get(format!("{url}/metrics"))
        .send()
        .await
        .expect("metrics");
    assert_eq!(metrics.status(), 200);
    assert!(
        metrics
            .text()
            .await
            .expect("text")
            .contains("zeroshot_hub_")
    );
    let other = http
        .get(format!("{url}/v1/hub/daemon"))
        .send()
        .await
        .expect("other");
    assert_eq!(other.status(), 400, "nothing but /metrics is served");
}

/// A stand-in for `zeroshot` that echoes its arguments as JSON and exits 3.
#[cfg(unix)]
fn echo_executable(folder: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = folder.join("zeroshot-echo");
    std::fs::write(
        &path,
        "#!/bin/sh\necho \"{\\\"args\\\": \\\"$*\\\"}\"\necho warning >&2\nexit 3\n",
    )
    .expect("echo");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    path
}

#[cfg(unix)]
#[tokio::test]
async fn observations_stream_the_target_command_and_its_exit_code() {
    let folder = tempfile::tempdir().expect("tempdir");
    let hub = hub_with(FakeDriver::default(), Some(echo_executable(folder.path()))).await;
    let client = &hub.client;
    run(client, add("local", 2)).await.expect("add");
    let placed = run(client, submit(spec(hub.directory.path(), "observed", None)))
        .await
        .expect("submit");
    let job = placed["job"]["jobId"].as_str().expect("job").to_owned();
    for command in [
        HubCommand::Watch {
            job: job.clone(),
            after: Some("c1".to_owned()),
        },
        HubCommand::Logs(HubLogs {
            job: job.clone(),
            after: Some("c2".to_owned()),
            execution: Some("ex1".to_owned()),
        }),
        HubCommand::Attach {
            job: job.clone(),
            execution: "ex1".to_owned(),
        },
        HubCommand::ForceStop { job: job.clone() },
    ] {
        let code = execute(client, command, &mut Vec::new())
            .await
            .expect("stream");
        assert_eq!(code, 3, "the remote exit code is the command's");
    }

    // Submitting without --detach follows the placed job.
    let mut follow = spec(hub.directory.path(), "followed", None);
    follow.submission_key = Some("followed".to_owned());
    let mut output = Vec::new();
    let code = execute(
        client,
        HubCommand::Submit(Box::new(HubSubmit {
            job: follow,
            detach: false,
        })),
        &mut output,
    )
    .await
    .expect("submit and follow");
    assert_eq!(code, 3);
    let document: Value = serde_json::from_slice(&output).expect("json");
    assert_eq!(document["job"]["state"], json!("running"));

    // A job that never placed has nothing to observe.
    run(client, add("box", 1)).await.expect("add box");
    let state = hub.service.snapshot().await;
    assert!(
        state.jobs.iter().all(|job| job.state == JobState::Running),
        "both jobs placed"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn an_unstartable_observer_reports_exit_1() {
    let hub = hub_with(
        FakeDriver::default(),
        Some(PathBuf::from("/nonexistent/zeroshot")),
    )
    .await;
    run(&hub.client, add("local", 1)).await.expect("add");
    let placed = run(
        &hub.client,
        submit(spec(hub.directory.path(), "lost", None)),
    )
    .await
    .expect("submit");
    let job = placed["job"]["jobId"].as_str().expect("job").to_owned();
    let code = execute(
        &hub.client,
        HubCommand::Watch { job, after: None },
        &mut Vec::new(),
    )
    .await
    .expect("stream");
    assert_eq!(code, 1);
}

#[tokio::test]
async fn non_hub_servers_are_reported_as_unreachable() {
    use axum::routing::get;
    let app = axum::Router::new()
        .route(
            "/v1/hub/daemon",
            get(|| async { (axum::http::StatusCode::BAD_GATEWAY, "upstream down") }),
        )
        .route("/v1/hub/jobs/{id}", get(|| async { "not json" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, app).await });
    let directory = tempfile::tempdir().expect("tempdir");
    let token = directory.path().join("token");
    std::fs::write(&token, TOKEN).expect("token");
    let client = HubClient::new(&Resolved {
        name: "proxy".to_owned(),
        url,
        token: TokenRef::File(token),
        source: "flag",
    })
    .expect("client");
    let gateway = run(&client, HubCommand::DaemonStatus)
        .await
        .expect_err("not a hub");
    assert!(
        gateway.to_string().contains("is this a zeroshot hub?"),
        "{gateway}"
    );
    assert_eq!(gateway.exit_code(), 5);
    let invalid = run(
        &client,
        HubCommand::Status {
            job: "hj_x".to_owned(),
        },
    )
    .await
    .expect_err("invalid json");
    assert!(invalid.to_string().contains("invalid JSON"), "{invalid}");
    let stream = execute(
        &client,
        HubCommand::Watch {
            job: "hj_x".to_owned(),
            after: None,
        },
        &mut Vec::new(),
    )
    .await
    .expect_err("no stream route");
    assert_eq!(stream.exit_code(), 5, "{stream:?}");
}

#[tokio::test]
async fn placing_jobs_can_be_dequeued_through_the_api_and_are_never_adopted() {
    let driver = FakeDriver {
        unanswered: Mutex::new(1),
        ..FakeDriver::default()
    };
    let hub = hub_with(driver, None).await;
    let client = &hub.client;
    run(client, add("local", 1)).await.expect("add");
    let placing = run(
        client,
        submit(spec(hub.directory.path(), "unanswered", None)),
    )
    .await
    .expect("an unconfirmed submission is not an error");
    assert_eq!(placing["job"]["state"], json!("running"));
    assert_eq!(placing["job"]["runId"], Value::Null);
    assert_eq!(placing["job"]["errorKind"], json!("submission_unconfirmed"));
    let job_id = placing["job"]["jobId"].as_str().expect("job id").to_owned();

    let dequeued = run(
        client,
        HubCommand::Dequeue {
            job: job_id.clone(),
        },
    )
    .await
    .expect("dequeue a placing job");
    assert_eq!(dequeued["job"]["state"], json!("dequeued"));
    assert_eq!(
        dequeued["job"]["errorKind"],
        json!("dequeued_while_placing")
    );
    let message = dequeued["job"]["error"].as_str().expect("error");
    assert!(
        message.contains(&format!("submission key hub-{job_id}-1 may exist there")),
        "{message}"
    );

    hub.service.pass().await.expect("pass");
    assert_eq!(
        hub.driver.submissions.lock().expect("lock").len(),
        1,
        "a dequeued job is not submitted again"
    );
    let status = run(client, HubCommand::Status { job: job_id })
        .await
        .expect("status");
    assert_eq!(status["job"]["state"], json!("dequeued"));
    assert_eq!(
        status["job"]["runId"],
        Value::Null,
        "the run is not adopted"
    );
}
