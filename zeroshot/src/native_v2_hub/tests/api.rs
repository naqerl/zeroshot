use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::{
    HubCommand, HubContextCommand, HubEnqueue, HubJobSpec, HubMemberAdd, HubPoolCommand, HubPs,
    HubStrategy, HubTokenSource,
};

use super::super::api::{ApiState, EXIT_CODE_HEADER, router, token_matches};
use super::super::client::{HubClient, execute};
use super::super::context::{ContextFile, Contexts, Resolved, TokenRef, select};
use super::super::error::HubError;
use super::super::service::HubService;
use super::super::store::HubStore;
use super::FakeDriver;
use crate::native_v2_target::FileTargetRegistry;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

struct Hub {
    url: String,
    service: Arc<HubService>,
    directory: tempfile::TempDir,
}

async fn hub() -> Hub {
    let directory = tempfile::tempdir().expect("tempdir");
    let service = Arc::new(
        HubService::open(
            HubStore::new(directory.path().join("hub")),
            Arc::new(FakeDriver::default()),
            FileTargetRegistry::new(directory.path().join("targets.json")),
        )
        .expect("service"),
    );
    let state = Arc::new(ApiState {
        service: service.clone(),
        token: TOKEN.to_owned(),
        heartbeat: Arc::new(Mutex::new(json!({"schema": "zeroshot.hub.daemon/v1"}))),
        executable: None,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, router(state)).await });
    Hub {
        url,
        service,
        directory,
    }
}

fn client(hub: &Hub, token: &str) -> HubClient {
    let path = hub.directory.path().join(format!("token-{}", token.len()));
    std::fs::write(&path, format!("{token}\n")).expect("token file");
    HubClient::new(&Resolved {
        name: "lab".to_owned(),
        url: hub.url.clone(),
        token: TokenRef::File(path),
        source: "flag",
    })
    .expect("client")
}

async fn run(client: &HubClient, command: HubCommand) -> Result<Value, HubError> {
    let mut output = Vec::new();
    execute(client, command, &mut output).await?;
    Ok(serde_json::from_slice(&output).expect("json output"))
}

fn spec(input: &Path, target: Option<&str>) -> HubJobSpec {
    HubJobSpec {
        title: "Later".to_owned(),
        run_args: vec![
            "--title".to_owned(),
            "Later".to_owned(),
            "--input".to_owned(),
            input.display().to_string(),
        ],
        target: target.map(str::to_owned),
        tags: Vec::new(),
        strategy: HubStrategy::FirstFree,
        labels: vec![("task".to_owned(), "api".to_owned())],
        runtime_files: Vec::new(),
        repository: None,
        branch: None,
        revision: None,
        source_connection: None,
        environment: false,
        cwd: None,
        submission_key: None,
    }
}

#[test]
fn context_selection_prefers_flag_then_environment_then_current_then_local() {
    let file = ContextFile {
        current: Some("lab".to_owned()),
        contexts: Vec::new(),
    };
    assert_eq!(
        select(Some("prod"), Some("ci"), &file),
        ("prod".to_owned(), "flag")
    );
    assert_eq!(select(None, Some("ci"), &file), ("ci".to_owned(), "env"));
    assert_eq!(
        select(Some(""), Some(""), &file),
        ("lab".to_owned(), "current")
    );
    assert_eq!(
        select(None, None, &ContextFile::default()),
        ("local".to_owned(), "default")
    );
}

#[test]
fn contexts_store_token_sources_not_tokens() {
    let directory = tempfile::tempdir().expect("tempdir");
    let hub = HubStore::new(directory.path().join("hub"));
    let contexts = Contexts::new(directory.path(), hub.clone());
    let token_file = directory.path().join("lab.token");
    std::fs::write(&token_file, "secret-token-value\n").expect("token");
    let mut output = Vec::new();
    for command in [
        HubContextCommand::Add {
            name: "lab".to_owned(),
            url: "http://lab-box:7383/".to_owned(),
            token: HubTokenSource::File(token_file.clone()),
        },
        HubContextCommand::Add {
            name: "ci".to_owned(),
            url: "https://hub.example".to_owned(),
            token: HubTokenSource::Env("CI_HUB_TOKEN".to_owned()),
        },
        HubContextCommand::Use {
            name: "lab".to_owned(),
        },
    ] {
        contexts
            .execute(None, command, &mut output)
            .expect("context");
    }
    let stored = std::fs::read_to_string(directory.path().join("hub-contexts.json")).expect("file");
    assert!(!stored.contains("secret-token-value"), "{stored}");
    let resolved = contexts.resolve(None).expect("current");
    assert_eq!(
        (
            resolved.name.as_str(),
            resolved.url.as_str(),
            resolved.source
        ),
        ("lab", "http://lab-box:7383", "current")
    );
    assert_eq!(resolved.token, TokenRef::File(token_file));
    assert_eq!(
        contexts.resolve(Some("ci")).expect("flag").token,
        TokenRef::Env("CI_HUB_TOKEN".to_owned())
    );

    // `local` follows this machine's daemon heartbeat, and falls back to the defaults.
    assert_eq!(
        contexts.resolve(Some("local")).expect("local").url,
        "http://127.0.0.1:7383"
    );
    hub.write_heartbeat(&json!({"url": "http://127.0.0.1:9999", "tokenFile": "/srv/t"}))
        .expect("heartbeat");
    let local = contexts.resolve(Some("local")).expect("local");
    assert_eq!(local.url, "http://127.0.0.1:9999");
    assert_eq!(local.token, TokenRef::File("/srv/t".into()));

    let mut listing = Vec::new();
    contexts
        .execute(None, HubContextCommand::List, &mut listing)
        .expect("list");
    let listing: Value = serde_json::from_slice(&listing).expect("json");
    assert_eq!(listing["current"], json!("lab"));
    assert_eq!(listing["contexts"].as_array().map(Vec::len), Some(3));
    assert!(!listing.to_string().contains("secret-token-value"));

    for refused in [
        HubContextCommand::Remove {
            name: "local".to_owned(),
        },
        HubContextCommand::Add {
            name: "local".to_owned(),
            url: "http://x:1".to_owned(),
            token: HubTokenSource::Env("X".to_owned()),
        },
        HubContextCommand::Add {
            name: "bad".to_owned(),
            url: "http://user:pass@x:1".to_owned(),
            token: HubTokenSource::Env("X".to_owned()),
        },
        HubContextCommand::Use {
            name: "missing".to_owned(),
        },
    ] {
        assert!(
            contexts
                .execute(None, refused.clone(), &mut output)
                .is_err(),
            "{refused:?}"
        );
    }
    contexts
        .execute(
            None,
            HubContextCommand::Remove {
                name: "lab".to_owned(),
            },
            &mut output,
        )
        .expect("remove");
    assert_eq!(contexts.resolve(None).expect("default").name, "local");
}

#[test]
fn token_comparison_requires_an_exact_match() {
    assert!(token_matches(TOKEN, TOKEN));
    assert!(!token_matches(TOKEN, &TOKEN[1..]));
    assert!(!token_matches(TOKEN, &TOKEN.replace('0', "1")));
    assert!(!token_matches(TOKEN, ""));
}

#[tokio::test]
async fn api_rejects_missing_and_wrong_bearer_tokens() {
    let hub = hub().await;
    let http = reqwest::Client::new();
    let health = http
        .get(format!("{}/v1/hub/health", hub.url))
        .send()
        .await
        .expect("health");
    assert_eq!(health.status(), 200, "health needs no token");
    for token in [None, Some("wrong-token")] {
        let mut request = http.get(format!("{}/v1/hub/daemon", hub.url));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.expect("daemon");
        assert_eq!(response.status(), 401);
        assert_eq!(
            response
                .headers()
                .get(EXIT_CODE_HEADER)
                .map(|value| value.as_bytes()),
            Some(&b"1"[..])
        );
        let body: Value = response.json().await.expect("json");
        assert_eq!(body["code"], json!("hub.unauthorized"));
    }
    let error = run(&client(&hub, "wrong"), HubCommand::DaemonStatus)
        .await
        .expect_err("wrong token");
    assert!(matches!(error, HubError::Unauthorized { .. }), "{error:?}");
    assert_eq!(error.diagnostic()["details"]["context"], json!("lab"));
    assert!(
        !error.to_string().contains("wrong"),
        "the token is never echoed: {error}"
    );
}

#[tokio::test]
async fn pool_list_adds_history_only_for_an_accepted_event_count() {
    let hub = hub().await;
    let client = client(&hub, TOKEN);
    let listed = run(
        &client,
        HubCommand::Pool(HubPoolCommand::List { event: None }),
    )
    .await
    .expect("pools");
    assert!(
        listed.get("events").is_none(),
        "history is not state: {listed}"
    );

    let listed = run(
        &client,
        HubCommand::Pool(HubPoolCommand::List { event: Some(1) }),
    )
    .await
    .expect("events");
    assert_eq!(listed["events"], json!([]));

    let error = run(
        &client,
        HubCommand::Pool(HubPoolCommand::List { event: Some(0) }),
    )
    .await
    .expect_err("zero");
    assert_eq!(error.diagnostic()["code"], json!("hub.invalid_request"));
    assert!(error.to_string().contains("between 1 and 1024"), "{error}");

    let error = client
        .get_query(&["v1", "hub", "pools"], &[("event", "many".to_owned())])
        .await
        .expect_err("not an integer");
    assert_eq!(error.diagnostic()["code"], json!("hub.invalid_request"));
    assert!(
        error.to_string().contains("integer between 1 and 1024"),
        "{error}"
    );
}

#[tokio::test]
async fn remote_client_drives_the_hub_and_replays_errors() {
    let hub = hub().await;
    let client = client(&hub, TOKEN);
    let status = run(&client, HubCommand::DaemonStatus)
        .await
        .expect("status");
    assert_eq!(status["context"], json!("lab"));
    assert_eq!(status["reachable"], json!(true));
    let added = run(
        &client,
        HubCommand::Add(HubMemberAdd {
            name: "local".to_owned(),
            tags: vec!["laptop".to_owned()],
            slots: 2,
            priority: 0,
        }),
    )
    .await
    .expect("add");
    assert_eq!(added["member"]["slots"], json!(2));

    let input = hub.directory.path().join("client-input.json");
    std::fs::write(&input, "{\"task\":\"remote\"}").expect("input");
    let enqueue = |target: Option<&str>| {
        HubCommand::Enqueue(Box::new(HubEnqueue {
            job: spec(&input, target),
            at: Some("+1h".to_owned()),
            cron: None,
            cron_timezone: None,
            priority: 3,
        }))
    };
    let queued = run(&client, enqueue(None)).await.expect("enqueue");
    let job_id = queued["job"]["jobId"].as_str().expect("job id").to_owned();
    assert_eq!(queued["job"]["state"], json!("scheduled"));

    // The client's input file was uploaded and the run now reads the hub's copy.
    let state = hub.service.snapshot().await;
    let spooled = &state.jobs[0].run_args[3];
    let spool = hub.directory.path().join("hub").join("spool");
    assert!(
        std::path::Path::new(spooled).starts_with(&spool),
        "{spooled}"
    );
    assert_eq!(
        std::fs::read_to_string(spooled).expect("spooled input"),
        "{\"task\":\"remote\"}"
    );

    let listed = run(
        &client,
        HubCommand::Ps(HubPs {
            state: "active".to_owned(),
            target: None,
            labels: vec![("task".to_owned(), "api".to_owned())],
            limit: None,
        }),
    )
    .await
    .expect("ps");
    assert_eq!(listed["jobs"][0]["jobId"], json!(job_id));

    let no_eligible = run(&client, enqueue(Some("missing-box")))
        .await
        .expect_err("no member matches");
    assert_eq!(no_eligible.exit_code(), 4, "remote exit codes are kept");
    assert_eq!(
        no_eligible.diagnostic()["code"],
        json!("hub.no_eligible_target")
    );

    let early = execute(
        &client,
        HubCommand::Logs(zeroshot_engine::native_v2_cli::hub::HubLogs {
            job: job_id.clone(),
            after: None,
            execution: None,
        }),
        &mut Vec::new(),
    )
    .await
    .expect_err("no run yet");
    assert_eq!(early.diagnostic()["code"], json!("hub.invalid_state"));

    let dequeued = run(
        &client,
        HubCommand::Dequeue {
            job: job_id.clone(),
        },
    )
    .await
    .expect("dequeue");
    assert_eq!(dequeued["job"]["state"], json!("dequeued"));
    let missing = run(
        &client,
        HubCommand::Status {
            job: "hj_missing".to_owned(),
        },
    )
    .await
    .expect_err("missing job");
    assert_eq!(missing.diagnostic()["code"], json!("hub.job_not_found"));
    assert_eq!(missing.exit_code(), 1);
}

#[tokio::test]
async fn an_unreachable_hub_fails_with_exit_5_naming_context_and_url() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    drop(listener);
    let directory = tempfile::tempdir().expect("tempdir");
    let token = directory.path().join("token");
    std::fs::write(&token, TOKEN).expect("token");
    let client = HubClient::new(&Resolved {
        name: "lab".to_owned(),
        url: url.clone(),
        token: TokenRef::File(token),
        source: "flag",
    })
    .expect("client");
    let error = run(&client, HubCommand::Dispatch)
        .await
        .expect_err("nothing listens");
    assert_eq!(error.exit_code(), 5);
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic["code"], json!("hub.unreachable"));
    assert_eq!(
        diagnostic["details"],
        json!({ "context": "lab", "url": url })
    );

    // The local context before any daemon ever started is unreachable too, not a usage error.
    let local = TokenRef::File(directory.path().join("hub/api-token"))
        .read("local", "http://127.0.0.1:7383")
        .expect_err("no token yet");
    assert_eq!(local.exit_code(), 5);
}

#[cfg(unix)]
#[tokio::test]
async fn proxied_commands_stream_both_outputs_then_the_exit_code() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    super::super::api::proxy(
        "/bin/sh".into(),
        vec![
            "-c".to_owned(),
            "echo '{\"event\":1}'; echo warn >&2; exit 3".to_owned(),
        ],
        sender,
    )
    .await;
    let mut frames = Vec::new();
    while let Some(frame) = receiver.recv().await {
        frames.push(serde_json::from_slice::<Value>(&frame).expect("frame"));
    }
    assert!(
        frames.contains(&json!({"stdout": "{\"event\":1}"})),
        "{frames:?}"
    );
    assert!(frames.contains(&json!({"stderr": "warn"})), "{frames:?}");
    assert_eq!(frames.last(), Some(&json!({"exit": 3})));
}

#[test]
fn token_sources_fail_with_their_name_not_their_content() {
    let directory = tempfile::tempdir().expect("tempdir");
    let missing = TokenRef::File(directory.path().join("missing.token"));
    let local = missing
        .read("local", "http://127.0.0.1:7383")
        .expect_err("no local daemon yet");
    assert!(
        matches!(local, HubError::Unreachable { .. }),
        "the local daemon creates its token on first start: {local}"
    );
    let named = missing
        .read("lab", "http://lab:7383")
        .expect_err("missing token file");
    assert!(named.to_string().contains("could not be read"), "{named}");
    let blank = directory.path().join("blank.token");
    std::fs::write(&blank, " \n").expect("blank");
    let empty = TokenRef::File(blank)
        .read("lab", "http://lab:7383")
        .expect_err("empty token");
    assert!(
        empty.to_string().contains("bearer token is empty"),
        "{empty}"
    );
    let unset = TokenRef::Env("ZEROSHOT_HUB_TEST_TOKEN_NEVER_SET".to_owned())
        .read("ci", "https://hub.example")
        .expect_err("unset variable");
    assert!(
        unset
            .to_string()
            .contains("ZEROSHOT_HUB_TEST_TOKEN_NEVER_SET is not set"),
        "{unset}"
    );
}

#[test]
fn context_files_and_commands_reject_bad_records() {
    let directory = tempfile::tempdir().expect("tempdir");
    let hub = HubStore::new(directory.path().join("hub"));
    let contexts = Contexts::new(directory.path(), hub.clone());
    let file = directory.path().join("hub-contexts.json");
    let mut output = Vec::new();
    let add = |name: &str, url: &str| HubContextCommand::Add {
        name: name.to_owned(),
        url: url.to_owned(),
        token: HubTokenSource::Env("LAB_TOKEN".to_owned()),
    };
    contexts
        .execute(None, add("lab", "http://lab:7383"), &mut output)
        .expect("add");
    let duplicate = contexts
        .execute(None, add("lab", "http://other:7383"), &mut output)
        .expect_err("duplicate");
    assert!(matches!(duplicate, HubError::Conflict(_)), "{duplicate}");
    for url in [
        "ftp://lab:21",
        "not a url",
        "http://lab:7383/?token=x",
        "file:///tmp/hub",
    ] {
        let error = contexts
            .execute(None, add("other", url), &mut output)
            .expect_err("invalid url");
        assert!(
            error.to_string().contains("invalid hub URL"),
            "{url}: {error}"
        );
    }
    let missing = contexts
        .execute(
            None,
            HubContextCommand::Remove {
                name: "absent".to_owned(),
            },
            &mut output,
        )
        .expect_err("remove absent");
    assert!(missing.to_string().contains("does not exist"), "{missing}");

    std::fs::write(
        &file,
        r#"{"current":"bare","contexts":[{"name":"bare","url":"http://bare:7383"}]}"#,
    )
    .expect("tokenless record");
    let bare = contexts.resolve(None).expect_err("no token source");
    assert!(bare.to_string().contains("has no token source"), "{bare}");

    std::fs::write(&file, b"{\"contexts\": 3}").expect("malformed");
    let malformed = contexts.resolve(None).expect_err("malformed");
    assert!(
        malformed.to_string().contains("is malformed"),
        "{malformed}"
    );

    std::fs::remove_file(&file).expect("remove");
    std::fs::create_dir(&file).expect("folder in the way");
    let unreadable = contexts.resolve(None).expect_err("unreadable");
    assert!(
        unreadable.to_string().contains("could not be read"),
        "{unreadable}"
    );
}
