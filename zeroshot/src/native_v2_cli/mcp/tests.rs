use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use super::super::{CliTestBackend, CliTestCall, NeverDetach, RunWatchCommand};
use super::{McpFailure, McpHost, McpServer, WatchStream, serve_http_listener};
use crate::native_v2_cli::hub::{HubCommand, HubPoolCommand, HubRequest};
use crate::native_v2_cli::{NativeV2CliCommand, execute_native_v2_cli};

struct ToolHost {
    backend: CliTestBackend,
}

#[async_trait(?Send)]
impl McpHost for ToolHost {
    async fn execute(&self, command: NativeV2CliCommand) -> Result<String, McpFailure> {
        let mut output = Vec::new();
        let mut signal = NeverDetach;
        execute_native_v2_cli(command, &self.backend, &mut signal, &mut output)
            .await
            .map(|_outcome| String::from_utf8_lossy(&output).into_owned())
            .map_err(|error| McpFailure::from_cli(&error))
    }

    async fn open_watch(&self, _command: &RunWatchCommand) -> Result<WatchStream, McpFailure> {
        Err(McpFailure::invalid("this host does not open watches"))
    }
}

struct ScriptHost;

#[async_trait(?Send)]
impl McpHost for ScriptHost {
    async fn execute(&self, _command: NativeV2CliCommand) -> Result<String, McpFailure> {
        Err(McpFailure::invalid("scripted host has no command executor"))
    }

    async fn open_watch(&self, command: &RunWatchCommand) -> Result<WatchStream, McpFailure> {
        let cursors = match command.after.as_ref().map(|cursor| cursor.as_str()) {
            None => vec!["v2:1", "v2:2"],
            Some("v2:1") => vec!["v2:2"],
            Some("v2:2") => vec!["v2:5"],
            _ => Vec::new(),
        };
        let run_id = command.run.run_id.as_str();
        let (sender, receiver) = mpsc::channel(8);
        for cursor in cursors {
            let event = json!({
                "subscriptionId": "watch-1",
                "runId": run_id,
                "title": "Repair checkout",
                "size": "medium",
                "cursor": cursor,
                "status": {"phase": "running", "activeExecutions": []}
            });
            sender
                .try_send(Ok(event))
                .expect("scripted watch event fits the buffer");
        }
        drop(sender);
        let task = tokio::spawn(async {});
        Ok(WatchStream::new(receiver, task))
    }
}

/// Serves a scripted durable watch. Events in `replay` are available while subscribing;
/// `live` events arrive after `live_delay` so they exercise the live path.
struct EventHost {
    replay: Vec<Value>,
    live: Vec<Value>,
    live_delay: Duration,
}

impl EventHost {
    fn new(events: Vec<Value>) -> Self {
        Self {
            replay: events,
            live: Vec::new(),
            live_delay: Duration::ZERO,
        }
    }
}

#[async_trait(?Send)]
impl McpHost for EventHost {
    async fn execute(&self, _command: NativeV2CliCommand) -> Result<String, McpFailure> {
        Err(McpFailure::invalid("event host has no command executor"))
    }

    async fn open_watch(&self, command: &RunWatchCommand) -> Result<WatchStream, McpFailure> {
        let after = command
            .after
            .as_ref()
            .and_then(|cursor| sequence_from_cursor(cursor.as_str()))
            .unwrap_or(0);
        let replay = self
            .replay
            .iter()
            .filter(|event| sequence(event).is_some_and(|cursor| cursor > after))
            .cloned()
            .collect::<Vec<_>>();
        let live = self
            .live
            .iter()
            .filter(|event| sequence(event).is_some_and(|cursor| cursor > after))
            .cloned()
            .collect::<Vec<_>>();
        let delay = self.live_delay;
        let (sender, receiver) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            for event in replay {
                if sender.send(Ok(event)).await.is_err() {
                    return;
                }
            }
            if !live.is_empty() {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                for event in live {
                    if sender.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(WatchStream::new(receiver, task))
    }
}

/// A hub host that serves one running job, one recorded pool transition, and the job's run
/// watch.
struct HubFeedHost {
    jobs: Vec<Value>,
    members: Vec<Value>,
    pool_events: Vec<Value>,
    events: Vec<Value>,
}

#[async_trait(?Send)]
impl McpHost for HubFeedHost {
    async fn execute(&self, command: NativeV2CliCommand) -> Result<String, McpFailure> {
        match command {
            NativeV2CliCommand::Hub(HubRequest {
                command: HubCommand::Ps(_),
                ..
            }) => Ok(json!({"schema": "zeroshot.hub.jobs/v1", "jobs": self.jobs}).to_string()),
            NativeV2CliCommand::Hub(HubRequest {
                command: HubCommand::List { .. },
                ..
            }) => Ok(
                json!({"schema": "zeroshot.hub.targets/v1", "targets": self.members}).to_string(),
            ),
            NativeV2CliCommand::Hub(HubRequest {
                command: HubCommand::Pool(HubPoolCommand::List),
                ..
            }) => Ok(
                json!({"schema": "zeroshot.hub.pools/v1", "pools": [], "events": self.pool_events})
                    .to_string(),
            ),
            _ => Err(McpFailure::invalid(
                "hub feed host only lists jobs, members and pools",
            )),
        }
    }

    async fn open_watch(&self, command: &RunWatchCommand) -> Result<WatchStream, McpFailure> {
        let run_id = command.run.run_id.as_str();
        let (sender, receiver) = mpsc::channel(32);
        for event in self.events.clone() {
            let mut event = event;
            event["runId"] = json!(run_id);
            sender
                .try_send(Ok(event))
                .expect("scripted run event fits the buffer");
        }
        drop(sender);
        let task = tokio::spawn(async {});
        Ok(WatchStream::new(receiver, task))
    }
}

fn hub_feed_host() -> HubFeedHost {
    HubFeedHost {
        jobs: vec![json!({
            "jobId": "hj_feed",
            "state": "running",
            "title": "Repair checkout",
            "target": "local",
            "runId": "run-public",
            "submittedAt": "2026-01-01T00:00:00Z",
            "dispatchedAt": "2026-01-01T00:00:01Z"
        })],
        members: vec![json!({
            "name": "local",
            "state": "active",
            "addedAt": "2026-01-01T00:00:00Z"
        })],
        pool_events: vec![json!({
            "sequence": 1,
            "kind": "lease",
            "pool": "burst",
            "leaseId": "zs-feed",
            "serverId": 11,
            "memberId": "builder",
            "machine": {
                "cores": 2,
                "memoryMb": 4096,
                "location": "fsn1",
                "serverType": "cx23"
            },
            "phase": "ready",
            "previousPhase": "preparing",
            "at": "2026-01-01T00:00:02Z"
        })],
        events: vec![watch_event("run-public", "v2:1", running_status("worker"))],
    }
}

/// A hub host whose job snapshot advances through `jobs` on each `hub ps` read and
/// whose runs replay scripted watch records after an optional delay.
struct HubLifecycleHost {
    jobs: Vec<Vec<Value>>,
    reads: AtomicUsize,
    members: Vec<Value>,
    run_events: HashMap<String, Vec<Value>>,
    watch_delay: Duration,
    watched_targets: Arc<Mutex<Vec<Option<String>>>>,
}

impl HubLifecycleHost {
    fn new(
        jobs: Vec<Vec<Value>>,
        members: Vec<Value>,
        run_events: HashMap<String, Vec<Value>>,
    ) -> Self {
        assert!(!jobs.is_empty(), "a lifecycle host needs one job snapshot");
        Self {
            jobs,
            reads: AtomicUsize::new(0),
            members,
            run_events,
            watch_delay: Duration::ZERO,
            watched_targets: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn with_watch_delay(mut self, delay: Duration) -> Self {
        self.watch_delay = delay;
        self
    }
}

#[async_trait(?Send)]
impl McpHost for HubLifecycleHost {
    async fn execute(&self, command: NativeV2CliCommand) -> Result<String, McpFailure> {
        match command {
            NativeV2CliCommand::Hub(HubRequest {
                command: HubCommand::Ps(_),
                ..
            }) => {
                let read = self.reads.fetch_add(1, Ordering::SeqCst);
                let index = read.min(self.jobs.len() - 1);
                Ok(
                    json!({ "schema": "zeroshot.hub.jobs/v1", "jobs": self.jobs[index] })
                        .to_string(),
                )
            }
            NativeV2CliCommand::Hub(HubRequest {
                command: HubCommand::List { .. },
                ..
            }) => Ok(
                json!({"schema": "zeroshot.hub.targets/v1", "targets": self.members}).to_string(),
            ),
            NativeV2CliCommand::Hub(HubRequest {
                command: HubCommand::Pool(HubPoolCommand::List),
                ..
            }) => Ok(
                json!({"schema": "zeroshot.hub.pools/v1", "pools": [], "events": []}).to_string(),
            ),
            _ => Err(McpFailure::invalid(
                "lifecycle host only lists jobs, members and pools",
            )),
        }
    }

    async fn open_watch(&self, command: &RunWatchCommand) -> Result<WatchStream, McpFailure> {
        self.watched_targets
            .lock()
            .expect("watched targets")
            .push(command.run.target.clone());
        let run_id = command.run.run_id.as_str().to_owned();
        let events = self.run_events.get(&run_id).cloned().unwrap_or_default();
        let delay = self.watch_delay;
        let (sender, receiver) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            if !events.is_empty() && !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            for mut event in events {
                event["runId"] = json!(run_id);
                if sender.send(Ok(event)).await.is_err() {
                    return;
                }
            }
        });
        Ok(WatchStream::new(receiver, task))
    }
}

fn hub_lifecycle_job(job_id: &str, state: &str, run_id: Option<&str>) -> Value {
    let mut job = json!({
        "jobId": job_id,
        "title": "Lifecycle job",
        "state": state,
        "target": "builder",
        "project": "acme/repo",
        "submittedAt": "2026-01-01T00:00:00Z",
    });
    if let Some(run_id) = run_id {
        job["runId"] = json!(run_id);
        job["dispatchedAt"] = json!("2026-01-01T00:00:01Z");
    }
    job
}

fn hub_member(name: &str) -> Value {
    json!({"name": name, "state": "active", "addedAt": "2026-01-01T00:00:00Z"})
}

fn sequence_from_cursor(cursor: &str) -> Option<u64> {
    cursor.rsplit_once(':')?.1.parse().ok()
}

fn sequence(event: &Value) -> Option<u64> {
    let cursor = event
        .get("cursor")
        .or_else(|| event.pointer("/data/cursor"))?;
    sequence_from_cursor(cursor.as_str()?)
}

fn source() -> Value {
    json!({"repository": "acme/repo", "branch": "main", "revision": "abc123"})
}

fn watch_event(run_id: &str, cursor: &str, status: Value) -> Value {
    json!({
        "subscriptionId": "v2-watch-455",
        "runId": run_id,
        "title": "Repair checkout",
        "size": "medium",
        "source": source(),
        "cursor": cursor,
        "status": status
    })
}

fn running_status(node: &str) -> Value {
    json!({
        "phase": "running",
        "activeExecutions": [{"execution": "exec-1", "node": node}],
        "metadata": {"tokenUsage": {"inputTokens": 10, "outputTokens": 2, "complete": true}}
    })
}

fn finished_status() -> Value {
    json!({
        "phase": "finished",
        "terminalResult": {
            "status": "failed",
            "reason": "worker_failed",
            "context": {
                "primary": {"message": "primary failure"},
                "recorded": [
                    {"message": "first error", "cursor": "v2:1"},
                    {"message": "last error", "cursor": "v2:3"}
                ],
                "incomplete": false
            }
        },
        "metadata": {"tokenUsage": {"inputTokens": 20, "outputTokens": 3, "complete": true}}
    })
}

/// Drives the real HTTP transport on an ephemeral port. The host is `!Send`, so the
/// server owns its own thread and current-thread runtime.
struct Session {
    client: reqwest::Client,
    url: String,
    frames: std::collections::VecDeque<Value>,
    session: Option<String>,
}

impl Session {
    fn spawn(host: impl McpHost + 'static) -> Self {
        let (address_sender, address_receiver) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("mcp-test-server".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("mcp test runtime");
                let (router, server) = McpServer::spawn(host).expect("start MCP worker");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind MCP test listener");
                    let address = listener.local_addr().expect("MCP test address");
                    address_sender
                        .send(address)
                        .expect("report MCP test address");
                    let _ = axum::serve(listener, router).await;
                    server.shutdown().await;
                });
            })
            .expect("spawn MCP test server");
        let address = address_receiver.recv().expect("MCP test address");
        Self {
            client: reqwest::Client::new(),
            url: format!("http://{address}/mcp"),
            frames: std::collections::VecDeque::new(),
            session: None,
        }
    }

    async fn request(&mut self, id: i64, method: &str, params: Value) -> Value {
        self.send(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await;
        self.read().await
    }

    /// Posts one message. Server-sent events are queued for `read`.
    async fn send(&mut self, message: &Value) -> Option<String> {
        self.post(message, self.session.clone()).await
    }

    /// Posts one message without a session header, ignoring any stored session.
    async fn send_without_session(&mut self, message: &Value) -> Option<String> {
        self.post(message, None).await
    }

    /// Starts a stream without waiting for its body, so a long-lived subscription can be
    /// inspected and then dropped.
    async fn start_stream(&mut self, message: &Value) -> reqwest::Response {
        let mut request = self.client.post(&self.url).json(message);
        if let Some(session) = &self.session {
            request = request.header("mcp-session-id", session);
        }
        let response = request.send().await.expect("start MCP stream");
        if let Some(session) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
        {
            self.session = Some(session);
        }
        response
    }

    async fn post(&mut self, message: &Value, session: Option<String>) -> Option<String> {
        let mut request = self.client.post(&self.url).json(message);
        if let Some(session) = session {
            request = request.header("mcp-session-id", session);
        }
        let response = request.send().await.expect("post MCP message");
        let session = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if let Some(id) = &session {
            self.session = Some(id.clone());
        }
        if response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .starts_with("text/event-stream")
        {
            let body = tokio::time::timeout(Duration::from_secs(5), response.text())
                .await
                .expect("timed out reading the MCP event stream")
                .expect("MCP event stream body");
            for line in body.lines() {
                let Some(data) = line.strip_prefix("data: ") else {
                    continue;
                };
                self.frames
                    .push_back(serde_json::from_str(data).expect("event stream data is JSON"));
            }
            return session;
        }
        if response.status() == reqwest::StatusCode::ACCEPTED {
            return session;
        }
        let value: Value = response.json().await.expect("MCP response JSON");
        self.frames.push_back(value);
        session
    }

    async fn read(&mut self) -> Value {
        if self.frames.is_empty() {
            panic!("no MCP frame was produced");
        }
        self.frames.pop_front().expect("queued MCP frame")
    }
}

/// Drains server-sent event frames until `done` accepts what has been read or the deadline
/// passes. A hub stream is intentionally long-lived, so a closed reader is the only local stop.
async fn collect_stream(
    response: reqwest::Response,
    deadline: Duration,
    mut done: impl FnMut(&[Value]) -> bool,
) -> Vec<Value> {
    let mut body = response.bytes_stream();
    let mut pending = String::new();
    let mut frames = Vec::new();
    let end = tokio::time::Instant::now() + deadline;
    while !done(&frames) {
        let chunk = match tokio::time::timeout_at(end, body.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            _ => break,
        };
        pending.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(index) = pending.find("\n\n") {
            let block: String = pending.drain(..index + 2).collect();
            for line in block.lines() {
                let Some(data) = line.strip_prefix("data: ") else {
                    continue;
                };
                frames.push(serde_json::from_str(data).expect("stream data is JSON"));
            }
        }
    }
    frames
}

fn stream_notifications(frames: &[Value]) -> Vec<&Value> {
    frames
        .iter()
        .filter(|frame| {
            frame
                .get("method")
                .and_then(Value::as_str)
                .is_some_and(|method| method.starts_with("notifications/events/"))
        })
        .collect()
}

fn hub_event_states(frames: &[Value], job_id: &str) -> Vec<String> {
    stream_notifications(frames)
        .into_iter()
        .filter(|frame| frame["params"]["name"] == json!("hub.job"))
        .filter(|frame| frame["params"]["data"]["jobId"] == json!(job_id))
        .filter_map(|frame| frame["params"]["data"]["state"].as_str().map(str::to_owned))
        .collect()
}

fn has_complete_frame(frames: &[Value]) -> bool {
    frames.iter().any(|frame| {
        frame
            .get("result")
            .and_then(|result| result.get("resultType"))
            == Some(&json!("complete"))
    })
}

async fn initialize(session: &mut Session) {
    let response = session
        .request(
            1,
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"}
            }),
        )
        .await;
    assert_eq!(response["id"], json!(1));
    assert_eq!(response["result"]["protocolVersion"], json!("2025-06-18"));
    assert_eq!(response["result"]["serverInfo"]["name"], json!("zeroshot"));
    assert!(
        response["result"]["capabilities"]["extensions"]
            .get("io.modelcontextprotocol/events")
            .is_some()
    );
}

fn initialize_message(id: i64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0"}
        }
    })
}

fn assert_session_id(id: &str) {
    assert_eq!(id.len(), 32, "session id is 32 characters");
    assert!(
        id.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "session id is lowercase hex: {id}"
    );
}

#[tokio::test]
async fn initialize_mints_a_fresh_session_for_each_headerless_client() {
    let mut session = Session::spawn(ScriptHost);
    let first = session
        .send_without_session(&initialize_message(1))
        .await
        .expect("first session id");
    assert_session_id(&first);
    assert_eq!(
        session.read().await["result"]["serverInfo"]["name"],
        json!("zeroshot")
    );
    let second = session
        .send_without_session(&initialize_message(2))
        .await
        .expect("second session id");
    assert_session_id(&second);
    assert_eq!(
        session.read().await["result"]["serverInfo"]["name"],
        json!("zeroshot")
    );
    assert_ne!(first, second);
}

#[tokio::test]
async fn requests_without_a_session_are_rejected() {
    let mut session = Session::spawn(ScriptHost);
    let header = session
        .send_without_session(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {}
        }))
        .await;
    assert!(header.is_none());
    let response = session.read().await;
    assert_eq!(response["id"], json!(1));
    assert_eq!(response["error"]["code"], json!(-32600));
    assert_eq!(
        response["error"]["data"]["code"],
        json!("mcp.session_required")
    );
}

#[tokio::test]
async fn initialize_with_an_existing_session_resets_it() {
    let mut session = Session::spawn(ScriptHost);
    initialize(&mut session).await;
    let existing = session.session.clone().expect("initial session id");
    let returned = session.send(&initialize_message(2)).await;
    assert_eq!(returned.as_deref(), Some(existing.as_str()));
    let response = session.read().await;
    assert_eq!(response["id"], json!(2));
    assert!(response.get("error").is_none());
    assert_eq!(response["result"]["serverInfo"]["name"], json!("zeroshot"));
    let listed = session.request(3, "tools/list", json!({})).await;
    assert!(
        listed["result"]["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .any(|tool| tool["name"] == "run")
    );
}

#[tokio::test]
async fn handshake_lists_cli_tools() {
    let mut session = Session::spawn(ToolHost {
        backend: CliTestBackend::default(),
    });
    initialize(&mut session).await;
    let listed = session.request(2, "tools/list", json!({})).await;
    let tools = listed["result"]["tools"].as_array().expect("tools array");
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "run",
            "status",
            "logs",
            "watch",
            "list",
            "resume",
            "force-stop",
            "report",
            "hub_list",
            "hub_ps",
            "hub_status",
            "hub_submit",
            "hub_enqueue",
            "hub_stats",
            "hub_logs",
            "hub_daemon_status",
            "hub_dequeue",
            "hub_add",
            "hub_update",
            "hub_remove",
            "hub_dispatch",
            "hub_force_stop",
            "hub_watch",
        ]
    );
    for tool in tools {
        assert!(!tool["description"].as_str().unwrap_or_default().is_empty());
        assert_eq!(tool["inputSchema"]["type"], json!("object"));
        assert_eq!(tool["inputSchema"]["additionalProperties"], json!(false));
    }
    let status = tools.iter().find(|tool| tool["name"] == "status").unwrap();
    assert!(
        status["description"]
            .as_str()
            .unwrap_or_default()
            .contains("Read a run's current status as JSON")
    );
    assert_eq!(status["inputSchema"]["required"], json!(["runId"]));
}

#[test]
fn hub_tools_are_listed_and_map_arguments() {
    let listed = super::tools::list(&Value::Null).expect("list tools");
    let names = listed["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|tool| tool["name"].as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    for name in [
        "hub_dequeue",
        "hub_add",
        "hub_update",
        "hub_remove",
        "hub_dispatch",
        "hub_force_stop",
        "hub_watch",
    ] {
        assert!(names.contains(&name), "{name} is listed");
    }

    let arguments = json!({"job": "hj_1"})
        .as_object()
        .expect("arguments object")
        .clone();
    assert_eq!(
        super::tools::argv_for_test("hub_dequeue", &arguments).expect("hub_dequeue argv"),
        ["hub", "dequeue", "hj_1"]
    );

    let arguments = json!({"title": "Review", "taskFrom": "https://github.com/acme/repo/pull/1"})
        .as_object()
        .expect("arguments object")
        .clone();
    assert_eq!(
        super::tools::argv_for_test("run", &arguments).expect("run argv"),
        [
            "run",
            "--title",
            "Review",
            "--task-from",
            "https://github.com/acme/repo/pull/1"
        ]
    );
}

#[test]
fn every_advertised_tool_property_is_accepted() {
    let listed = super::tools::list(&Value::Null).expect("list tools");
    for tool in listed["tools"].as_array().expect("tools array") {
        let name = tool["name"].as_str().expect("tool name");
        let properties = tool["inputSchema"]["properties"]
            .as_object()
            .expect("tool properties");
        for (property, schema) in properties {
            let value = match schema.get("type") {
                Some(Value::String(kind)) => match kind.as_str() {
                    "string" => json!("value"),
                    "boolean" => json!(true),
                    "integer" => json!(0),
                    "array" => json!(["value"]),
                    "object" => json!({}),
                    other => panic!("{name}.{property}: unhandled type {other}"),
                },
                Some(other) => panic!("{name}.{property}: unhandled schema {other}"),
                None if schema.get("oneOf").is_some() => json!("10m"),
                None => json!({}),
            };
            let mut arguments = Map::new();
            arguments.insert(property.clone(), value);
            super::tools::argv_for_test(name, &arguments).unwrap_or_else(|failure| {
                panic!("{name}.{property} is advertised but rejected: {failure:?}")
            });
        }
    }
}

#[tokio::test]
async fn hub_submit_schema_has_no_wait_or_queue() {
    let mut session = Session::spawn(ToolHost {
        backend: CliTestBackend::default(),
    });
    initialize(&mut session).await;
    let listed = session.request(2, "tools/list", json!({})).await;
    let tools = listed["result"]["tools"].as_array().expect("tools array");
    let submit = tools
        .iter()
        .find(|tool| tool["name"] == "hub_submit")
        .expect("hub_submit tool");
    let properties = submit["inputSchema"]["properties"]
        .as_object()
        .expect("hub_submit properties");
    for removed in ["wait", "queue"] {
        assert!(
            !properties.contains_key(removed),
            "{removed} is still in the hub_submit schema: {submit}"
        );
    }
    assert!(properties.contains_key("tag"), "{submit}");
}

#[tokio::test]
async fn status_tool_uses_the_cli_backend() {
    let backend = CliTestBackend::default();
    let mut session = Session::spawn(ToolHost {
        backend: backend.clone(),
    });
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "tools/call",
            json!({"name": "status", "arguments": {"runId": "run-public"}}),
        )
        .await;
    assert_eq!(response["result"]["isError"], json!(false));
    assert_eq!(
        response["result"]["structuredContent"]["runId"],
        json!("run-public")
    );
    assert_eq!(
        response["result"]["structuredContent"]["status"]["phase"],
        json!("admitted")
    );
    assert!(backend.calls().iter().any(|call| matches!(
        call,
        CliTestCall::Status { target: None, run_id } if run_id == "run-public"
    )));
}

#[tokio::test]
async fn failing_status_is_a_tool_error_with_the_cli_code() {
    let mut session = Session::spawn(ToolHost {
        backend: CliTestBackend::default(),
    });
    initialize(&mut session).await;
    let response = session
        .request(2, "tools/call", json!({"name": "status", "arguments": {}}))
        .await;
    assert_eq!(response["result"]["isError"], json!(true));
    assert_eq!(
        response["result"]["structuredContent"]["code"],
        json!("request.invalid")
    );
    assert!(response.get("error").is_none());
}

#[tokio::test]
async fn events_list_describes_one_hub_type_and_poll_and_push() {
    let mut session = Session::spawn(ScriptHost);
    initialize(&mut session).await;
    let listed = session.request(2, "events/list", json!({})).await;
    assert_eq!(listed["result"]["resultType"], json!("complete"));
    let events = listed["result"]["events"]
        .as_array()
        .expect("event descriptions");
    assert_eq!(events.len(), 1, "one subscription covers the whole hub");
    let event = &events[0];
    assert_eq!(event["name"], json!("hub"));
    assert_eq!(event["delivery"], json!(["poll", "push"]));
    let description = event["description"].as_str().unwrap_or_default();
    assert!(
        description.contains("everything in the hub"),
        "{description}"
    );
    for name in ["hub.job", "hub.run", "hub.pool", "hub.warning"] {
        assert!(description.contains(name), "{name} is described");
        assert!(
            event["names"]
                .as_array()
                .is_some_and(|names| names.contains(&json!(name))),
            "{name} is listed as a carried name"
        );
    }
    assert!(event["inputSchema"].get("oneOf").is_none());
    for property in ["runId", "jobId", "target"] {
        assert!(
            event["inputSchema"]["properties"].get(property).is_some(),
            "{property} is documented"
        );
    }
    assert_eq!(
        event["payloadSchema"]["required"],
        json!(["eventId", "name", "timestamp", "data", "_meta"])
    );
    assert_eq!(
        event["payloadSchema"]["properties"]["name"]["enum"],
        json!(["hub.job", "hub.run", "hub.pool", "hub.warning"])
    );
    assert!(
        event["payloadSchema"]["properties"]["data"].is_object(),
        "payload documents data"
    );
    assert!(
        event["payloadSchema"]["properties"].get("cursor").is_none(),
        "the hub cursor stays inside data"
    );
    assert!(
        event["payloadSchema"]["properties"]
            .get("truncated")
            .is_none(),
        "truncation is reported by the poll and active views, not the event envelope"
    );
    for filter in ["kinds", "phases", "nodes", "terminalOnly", "verbose"] {
        assert!(
            event["inputSchema"]["properties"].get(filter).is_some(),
            "{filter} is documented"
        );
    }
    assert!(
        event["inputSchema"]["description"]
            .as_str()
            .unwrap_or_default()
            .contains("maxAgeMs")
    );
    let next = session
        .request(3, "events/list", json!({"cursor": "1"}))
        .await;
    assert_eq!(next["result"]["events"], json!([]));

    let unknown = session
        .request(
            4,
            "events/poll",
            json!({"name": "hub.unknown", "arguments": {}}),
        )
        .await;
    assert_eq!(unknown["error"]["code"], json!(-32600), "{unknown}");
    assert_eq!(unknown["error"]["data"]["kind"], json!("event"));

    let subscribe = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "events/subscribe",
            "params": {}
        }))
        .await;
    assert_eq!(subscribe.status(), reqwest::StatusCode::OK);
    drop(subscribe);
}

#[tokio::test]
async fn hub_events_multiplex_a_whole_hub_on_one_cursor() {
    let mut session = Session::spawn(hub_feed_host());
    initialize(&mut session).await;
    let first = session.request(2, "events/poll", json!({})).await;
    assert_eq!(first["result"]["resultType"], json!("complete"));
    let mut cursor = first["result"]["cursor"].clone();
    let mut events = first["result"]["events"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for id in 3..20 {
        if ["hub.job", "hub.run", "hub.pool"]
            .iter()
            .all(|name| events.iter().any(|event| event["name"] == json!(name)))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let response = session
            .request(
                id,
                "events/poll",
                json!({
                    "name": "hub.event",
                    "arguments": {},
                    "cursor": cursor
                }),
            )
            .await;
        if let Some(batch) = response["result"]["events"].as_array() {
            events.extend(batch.iter().cloned());
        }
        cursor = response["result"]["cursor"].clone();
    }
    for name in ["hub.job", "hub.run", "hub.pool"] {
        assert!(
            events.iter().any(|event| event["name"] == json!(name)),
            "{name} arrives on the hub stream: {events:?}"
        );
    }
    let mut sequences = Vec::new();
    for event in &events {
        let event_id = event["eventId"].as_str().expect("hub event id");
        let sequence = event_id
            .strip_prefix("hub:")
            .and_then(|value| value.parse::<u64>().ok())
            .expect("hub cursor shape");
        sequences.push(sequence);
        let envelope = event.as_object().expect("hub event object");
        assert_eq!(
            envelope.len(),
            5,
            "hub occurrences keep the flattened lifetimes envelope"
        );
        for key in ["eventId", "name", "timestamp", "data", "_meta"] {
            assert!(envelope.contains_key(key), "{key} is in the envelope");
        }
        assert_eq!(event["data"]["cursor"], json!(event_id));
    }
    assert!(sequences.windows(2).all(|pair| pair[0] <= pair[1]));
    let run = events
        .iter()
        .find(|event| event["name"] == json!("hub.run"))
        .expect("run event");
    assert_eq!(run["data"]["runId"], json!("run-public"));
    assert_eq!(run["data"]["runCursor"], json!("v2:1"));
    assert_eq!(run["data"]["summary"]["phase"], json!("running"));
    assert_eq!(run["data"]["replayed"], json!(true));

    let after = session
        .request(
            20,
            "events/poll",
            json!({
                "name": "hub.event",
                "arguments": {},
                "cursor": cursor
            }),
        )
        .await;
    assert_eq!(after["result"]["events"], json!([]));
    assert_eq!(after["result"]["cursor"], cursor);
}

#[tokio::test]
async fn hub_events_filter_run_and_pool_kinds_on_replay() {
    let mut session = Session::spawn(hub_feed_host());
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "events/poll",
            json!({"name": "hub.event", "arguments": {}}),
        )
        .await;
    let cursor = response["result"]["cursor"].clone();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let runs = session
        .request(
            3,
            "events/poll",
            json!({
                "name": "hub.run",
                "arguments": {"runId": "run-public", "phases": ["running"]},
                "cursor": "hub:0"
            }),
        )
        .await;
    let runs = runs["result"]["events"]
        .as_array()
        .expect("filtered events");
    assert!(!runs.is_empty());
    assert!(runs.iter().all(|event| event["name"] == json!("hub.run")));
    assert!(
        runs.iter()
            .all(|event| event["data"]["summary"]["phase"] == json!("running"))
    );
    assert_ne!(cursor, json!(null));
}

#[tokio::test]
async fn events_poll_advances_the_cursor() {
    let mut session = Session::spawn(ScriptHost);
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public"},
                "cursor": "v2:1"
            }),
        )
        .await;
    assert_eq!(response["result"]["resultType"], json!("complete"));
    assert_eq!(response["result"]["cursor"], json!("v2:2"));
    assert_eq!(response["result"]["truncated"], json!(false));
    assert_eq!(response["result"]["hasMore"], json!(false));
    assert_eq!(response["result"]["nextPollMs"], json!(1000));
    assert_eq!(
        response["result"]["events"][0]["eventId"],
        json!("run-public:v2:2")
    );
    assert_eq!(
        response["result"]["events"][0]["data"]["cursor"],
        json!("v2:2")
    );
    assert!(response["result"]["events"][0].get("cursor").is_none());
}

#[tokio::test]
async fn events_poll_does_not_report_sparse_cursor_gaps() {
    let mut session = Session::spawn(ScriptHost);
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public"},
                "cursor": "v2:2"
            }),
        )
        .await;
    assert_eq!(response["result"]["truncated"], json!(false));
    assert_eq!(response["result"]["cursor"], json!("v2:5"));
    assert_eq!(response["result"]["hasMore"], json!(false));
    assert_eq!(
        response["result"]["events"][0]["data"]["cursor"],
        json!("v2:5")
    );
}

#[tokio::test]
async fn events_stream_pushes_a_hub_run_notification() {
    let mut session = Session::spawn(hub_feed_host());
    initialize(&mut session).await;
    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "events/stream",
            "params": {"name": "hub.run", "arguments": {}}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_secs(2), |frames| {
        stream_notifications(frames)
            .iter()
            .any(|frame| frame["params"]["name"] == json!("hub.run"))
    })
    .await;
    let event = stream_notifications(&frames)
        .into_iter()
        .find(|frame| frame["params"]["name"] == json!("hub.run"))
        .expect("a hub.run event");
    assert_eq!(event["params"]["data"]["runId"], json!("run-public"));
    assert_eq!(event["params"]["data"]["replayed"], json!(false));
    assert_eq!(
        event["params"]["data"]["summary"]["phase"],
        json!("running")
    );
    assert!(
        event["params"]["eventId"]
            .as_str()
            .is_some_and(|event_id| event_id.starts_with("hub:"))
    );
    assert_eq!(
        event["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
        json!(7)
    );
    assert!(!has_complete_frame(&frames));
}

#[tokio::test]
async fn events_job_selector_filters_the_hub_stream() {
    let mut session = Session::spawn(hub_feed_host());
    initialize(&mut session).await;
    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "events/stream",
            "params": {"name": "hub.event", "arguments": {"jobId": "hj_feed"}}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_secs(2), |frames| {
        let names = stream_notifications(frames)
            .into_iter()
            .filter_map(|frame| frame["params"]["name"].as_str())
            .collect::<Vec<_>>();
        names.contains(&"hub.job") && names.contains(&"hub.run")
    })
    .await;
    assert!(stream_notifications(&frames).iter().all(|frame| {
        frame["params"]
            .get("data")
            .and_then(|data| data.get("jobId"))
            .is_none_or(|job_id| *job_id == json!("hj_feed"))
    }));
    assert!(!has_complete_frame(&frames));
}

#[tokio::test]
async fn events_poll_marks_replayed_and_live_hub_events() {
    let host = HubLifecycleHost::new(
        vec![vec![hub_lifecycle_job(
            "hj_replayed",
            "running",
            Some("run-replayed"),
        )]],
        vec![hub_member("builder")],
        HashMap::new(),
    );
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let mut replayed = Value::Null;
    for _ in 0..40 {
        replayed = session
            .request(
                2,
                "events/poll",
                json!({"arguments": {}, "cursor": "hub:0", "maxEvents": 100}),
            )
            .await;
        if replayed["result"]["events"]
            .as_array()
            .is_some_and(|events| !events.is_empty())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let events = replayed["result"]["events"]
        .as_array()
        .expect("hub poll events");
    assert!(!events.is_empty());
    assert!(
        events
            .iter()
            .all(|event| event["data"]["replayed"] == json!(true))
    );

    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "events/stream",
            "params": {"arguments": {}}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_secs(2), |frames| {
        stream_notifications(frames)
            .iter()
            .any(|frame| frame["params"]["data"]["replayed"] == json!(true))
    })
    .await;
    let event = stream_notifications(&frames)
        .into_iter()
        .find(|frame| frame["params"]["data"]["replayed"] == json!(true))
        .expect("a replayed hub event");
    assert!(
        event["params"]["eventId"]
            .as_str()
            .is_some_and(|event_id| event_id.starts_with("hub:"))
    );
}

#[tokio::test]
async fn hub_subscription_opens_before_any_job_and_follows_its_lifecycle() {
    let host = HubLifecycleHost::new(
        vec![
            Vec::new(),
            Vec::new(),
            vec![hub_lifecycle_job("hj_life", "running", Some("run-life"))],
            vec![hub_lifecycle_job("hj_life", "succeeded", Some("run-life"))],
            vec![hub_lifecycle_job("hj_life", "succeeded", Some("run-life"))],
        ],
        vec![hub_member("builder")],
        HashMap::new(),
    );
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "events/stream",
            "params": {"arguments": {}}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_secs(4), |frames| {
        let states = hub_event_states(frames, "hj_life");
        ["submitted", "running", "succeeded"]
            .iter()
            .all(|state| states.iter().any(|seen| seen == state))
    })
    .await;
    let states = hub_event_states(&frames, "hj_life");
    assert!(
        states.iter().any(|state| state == "submitted"),
        "{states:?}"
    );
    assert!(states.iter().any(|state| state == "running"), "{states:?}");
    assert!(
        states.iter().any(|state| state == "succeeded"),
        "{states:?}"
    );
    assert!(!has_complete_frame(&frames));
    let job_events = stream_notifications(&frames)
        .into_iter()
        .filter(|frame| frame["params"]["name"] == json!("hub.job"))
        .collect::<Vec<_>>();
    assert!(
        job_events
            .iter()
            .all(|frame| frame["params"]["data"]["replayed"] == json!(false))
    );
    assert!(
        job_events
            .iter()
            .all(|frame| frame["params"]["data"]["at"].is_string())
    );
    let mut sequences = job_events
        .iter()
        .filter_map(|frame| frame["params"]["eventId"].as_str())
        .map(|event_id| sequence_from_cursor(event_id).expect("hub sequence"))
        .collect::<Vec<_>>();
    let sorted = {
        let mut sorted = sequences.clone();
        sorted.sort_unstable();
        sorted
    };
    assert_eq!(sequences, sorted);
    sequences.dedup();
    assert_eq!(sequences.len(), job_events.len());
}

#[tokio::test]
async fn hub_stream_survives_a_terminal_run_and_carries_the_next_job() {
    let mut run_events = HashMap::new();
    run_events.insert(
        "run-one".to_owned(),
        vec![watch_event("run-one", "v2:2", finished_status())],
    );
    run_events.insert(
        "run-two".to_owned(),
        vec![watch_event("run-two", "v2:1", running_status("worker"))],
    );
    let host = HubLifecycleHost::new(
        vec![
            vec![hub_lifecycle_job("hj_one", "running", Some("run-one"))],
            vec![hub_lifecycle_job("hj_one", "running", Some("run-one"))],
            vec![hub_lifecycle_job("hj_one", "succeeded", Some("run-one"))],
            vec![
                hub_lifecycle_job("hj_one", "succeeded", Some("run-one")),
                hub_lifecycle_job("hj_two", "running", Some("run-two")),
            ],
            vec![
                hub_lifecycle_job("hj_one", "succeeded", Some("run-one")),
                hub_lifecycle_job("hj_two", "running", Some("run-two")),
            ],
        ],
        vec![hub_member("builder")],
        run_events,
    )
    .with_watch_delay(Duration::from_millis(300));
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "events/stream",
            "params": {"arguments": {}}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_secs(4), |frames| {
        let terminal = stream_notifications(frames).iter().any(|frame| {
            frame["params"]["name"] == json!("hub.run")
                && frame["params"]["data"]["summary"]["terminal"] == json!(true)
        });
        let second = hub_event_states(frames, "hj_two")
            .iter()
            .any(|state| state == "running");
        terminal && second
    })
    .await;
    assert!(
        stream_notifications(&frames).iter().any(|frame| {
            frame["params"]["name"] == json!("hub.run")
                && frame["params"]["data"]["summary"]["terminal"] == json!(true)
        }),
        "{frames:#?}"
    );
    assert!(
        hub_event_states(&frames, "hj_two")
            .iter()
            .any(|state| state == "running"),
        "{frames:#?}"
    );
    assert!(!has_complete_frame(&frames));
}

#[tokio::test]
async fn hub_poll_resumes_from_a_cursor_captured_before_a_terminal_event() {
    let mut run_events = HashMap::new();
    run_events.insert(
        "run-one".to_owned(),
        vec![watch_event("run-one", "v2:2", finished_status())],
    );
    run_events.insert(
        "run-two".to_owned(),
        vec![watch_event("run-two", "v2:1", running_status("worker"))],
    );
    let host = HubLifecycleHost::new(
        vec![
            vec![hub_lifecycle_job("hj_one", "running", Some("run-one"))],
            vec![hub_lifecycle_job("hj_one", "running", Some("run-one"))],
            vec![
                hub_lifecycle_job("hj_one", "succeeded", Some("run-one")),
                hub_lifecycle_job("hj_two", "running", Some("run-two")),
            ],
            vec![
                hub_lifecycle_job("hj_one", "succeeded", Some("run-one")),
                hub_lifecycle_job("hj_two", "running", Some("run-two")),
            ],
        ],
        vec![hub_member("builder")],
        run_events,
    )
    .with_watch_delay(Duration::from_millis(400));
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let mut first = Value::Null;
    for _ in 0..40 {
        first = session
            .request(
                2,
                "events/poll",
                json!({"arguments": {}, "cursor": "hub:0", "maxEvents": 100}),
            )
            .await;
        if first["result"]["events"]
            .as_array()
            .is_some_and(|events| !events.is_empty())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let cursor = first["result"]["cursor"]
        .as_str()
        .expect("first hub cursor")
        .to_owned();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let later = session
        .request(
            3,
            "events/poll",
            json!({"arguments": {}, "cursor": cursor, "maxEvents": 100}),
        )
        .await;
    assert_eq!(later["result"]["truncated"], json!(false), "{later}");
    let events = later["result"]["events"]
        .as_array()
        .expect("resumed events");
    assert!(!events.is_empty(), "{later}");
    assert!(events.iter().all(|event| {
        sequence_from_cursor(event["eventId"].as_str().unwrap_or_default()).unwrap_or_default()
            > sequence_from_cursor(&cursor).unwrap_or_default()
    }));
    assert!(
        events.iter().any(|event| {
            event["name"] == json!("hub.run") && event["data"]["summary"]["terminal"] == json!(true)
        }),
        "{later}"
    );
    assert!(
        events.iter().any(|event| {
            event["name"] == json!("hub.job")
                && event["data"]["jobId"] == json!("hj_two")
                && event["data"]["state"] == json!("running")
        }),
        "{later}"
    );
}

#[tokio::test]
async fn a_nameless_subscription_streams_jobs_runs_and_pool_phases() {
    let host = hub_feed_host();
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let listed = session.request(2, "events/list", json!({})).await;
    assert_eq!(listed["result"]["events"][0]["name"], json!("hub"));

    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "events/subscribe",
            "params": {}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_secs(5), |frames| {
        let names = stream_notifications(frames)
            .into_iter()
            .filter_map(|frame| frame["params"]["name"].as_str())
            .collect::<std::collections::HashSet<_>>();
        ["hub.job", "hub.run", "hub.pool"]
            .iter()
            .all(|name| names.contains(name))
    })
    .await;
    let notifications = stream_notifications(&frames);
    let names = notifications
        .iter()
        .filter_map(|frame| frame["params"]["name"].as_str())
        .collect::<Vec<_>>();
    for name in ["hub.job", "hub.run", "hub.pool"] {
        assert!(
            names.contains(&name),
            "{name} arrives on one stream: {names:?}"
        );
    }
    assert!(
        names.iter().collect::<std::collections::HashSet<_>>().len() >= 3,
        "the subscription carries distinct names: {names:?}"
    );
    let pool = notifications
        .iter()
        .find(|frame| frame["params"]["name"] == json!("hub.pool"))
        .expect("pool phase");
    assert_eq!(pool["params"]["data"]["phase"], json!("ready"));
    assert_eq!(pool["params"]["data"]["pool"], json!("burst"));
    assert_eq!(pool["params"]["data"]["leaseId"], json!("zs-feed"));
    assert_eq!(
        pool["params"]["data"]["machine"]["serverType"],
        json!("cx23")
    );
    assert_eq!(pool["params"]["data"]["previousPhase"], json!("preparing"));

    let mut sequences = Vec::new();
    for frame in notifications
        .iter()
        .filter(|frame| frame["params"]["eventId"].is_string())
    {
        let event_id = frame["params"]["eventId"].as_str().expect("hub event id");
        sequences.push(
            event_id
                .strip_prefix("hub:")
                .and_then(|value| value.parse::<u64>().ok())
                .expect("hub cursor shape"),
        );
    }
    assert!(
        sequences.windows(2).all(|pair| pair[0] < pair[1]),
        "cursors strictly increase on one stream: {sequences:?}"
    );
}

#[tokio::test]
async fn hub_run_selector_narrows_the_stream_without_ending_it() {
    let mut run_events = HashMap::new();
    run_events.insert(
        "run-a".to_owned(),
        vec![watch_event("run-a", "v2:1", running_status("worker"))],
    );
    run_events.insert(
        "run-b".to_owned(),
        vec![watch_event("run-b", "v2:1", running_status("worker"))],
    );
    let host = HubLifecycleHost::new(
        vec![vec![
            hub_lifecycle_job("hj_a", "running", Some("run-a")),
            hub_lifecycle_job("hj_b", "running", Some("run-b")),
        ]],
        vec![hub_member("builder")],
        run_events,
    );
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "events/stream",
            "params": {"arguments": {"runId": "run-a"}}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_millis(1500), |_| false).await;
    let notifications = stream_notifications(&frames);
    assert!(
        notifications.iter().any(|frame| {
            frame["params"]["name"] == json!("hub.run")
                && frame["params"]["data"]["runId"] == json!("run-a")
        }),
        "{frames:#?}"
    );
    assert!(
        notifications
            .iter()
            .all(|frame| frame["params"]["data"]["runId"] != json!("run-b")),
        "{frames:#?}"
    );
    assert!(!has_complete_frame(&frames));
}

#[tokio::test]
async fn hub_stream_sends_heartbeats_while_idle() {
    let host = HubLifecycleHost::new(vec![Vec::new()], Vec::new(), HashMap::new());
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 12,
            "method": "events/stream",
            "params": {"arguments": {}}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_secs(2), |frames| {
        stream_notifications(frames)
            .iter()
            .any(|frame| frame["method"] == json!("notifications/events/heartbeat"))
    })
    .await;
    let heartbeat = stream_notifications(&frames)
        .into_iter()
        .find(|frame| frame["method"] == json!("notifications/events/heartbeat"))
        .expect("an idle heartbeat");
    assert!(
        heartbeat["params"]["cursor"]
            .as_str()
            .is_some_and(|cursor| cursor.starts_with("hub:"))
    );
    assert!(!has_complete_frame(&frames));
}

#[tokio::test]
async fn hub_poll_clipping_is_has_more_without_reporting_truncation() {
    let host = HubLifecycleHost::new(
        vec![vec![hub_lifecycle_job(
            "hj_clip",
            "running",
            Some("run-clip"),
        )]],
        vec![hub_member("builder")],
        HashMap::new(),
    );
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    for _ in 0..40 {
        let response = session
            .request(
                2,
                "events/poll",
                json!({"arguments": {}, "cursor": "hub:0", "maxEvents": 100}),
            )
            .await;
        if response["result"]["events"]
            .as_array()
            .is_some_and(|events| events.len() > 1)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let clipped = session
        .request(
            3,
            "events/poll",
            json!({"arguments": {}, "cursor": "hub:0", "maxEvents": 1}),
        )
        .await;
    assert_eq!(clipped["result"]["hasMore"], json!(true), "{clipped}");
    assert_eq!(
        clipped["result"]["truncated"],
        json!(false),
        "a resumable maxEvents clip is hasMore, not permanent loss: {clipped}"
    );
    let cursor = clipped["result"]["cursor"]
        .as_str()
        .expect("clipped cursor")
        .to_owned();
    let first = clipped["result"]["events"][0].clone();
    let resumed = session
        .request(
            4,
            "events/poll",
            json!({"arguments": {}, "cursor": cursor, "maxEvents": 1}),
        )
        .await;
    let second = resumed["result"]["events"][0].clone();
    assert!(
        sequence(&second).unwrap() > sequence(&first).unwrap(),
        "{resumed}"
    );
    assert_eq!(
        second["data"]["cursor"], second["eventId"],
        "the resumed batch starts at the next unread hub event: {resumed}"
    );
}

#[tokio::test]
async fn hub_replay_reports_the_exact_evicted_range_and_starts_at_the_front() {
    let jobs = (0..2050)
        .map(|index| hub_lifecycle_job(&format!("hj_{index}"), "running", None))
        .collect::<Vec<_>>();
    let host = HubLifecycleHost::new(vec![jobs], Vec::new(), HashMap::new());
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let mut first = Value::Null;
    for _ in 0..80 {
        let response = session
            .request(
                2,
                "events/poll",
                json!({"arguments": {}, "cursor": "hub:0", "maxEvents": 50}),
            )
            .await;
        if response["result"]["truncated"].is_object()
            && response["result"]["events"]
                .as_array()
                .is_some_and(|events| events.len() == 50)
        {
            first = response;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(first["result"]["truncated"].is_object(), "{first}");
    let first_event = first["result"]["events"][0].clone();
    let front = sequence(&first_event).expect("front sequence");
    assert!(front > 1, "{first}");
    assert_eq!(
        first["result"]["truncated"],
        json!({"from": "hub:1", "to": format!("hub:{}", front - 1)}),
        "{first}"
    );
    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "events/stream",
            "params": {"arguments": {}, "cursor": "hub:0"}
        }))
        .await;
    let frames = collect_stream(response, Duration::from_secs(2), |frames| {
        stream_notifications(frames)
            .iter()
            .any(|frame| frame["method"] == json!("notifications/events/event"))
    })
    .await;
    let active = stream_notifications(&frames)
        .into_iter()
        .find(|frame| frame["method"] == json!("notifications/events/active"))
        .expect("active frame");
    assert_eq!(
        active["params"]["truncated"],
        json!({"from": "hub:1", "to": format!("hub:{}", front - 1)}),
        "{active}"
    );
    let streamed = stream_notifications(&frames)
        .into_iter()
        .find(|frame| frame["method"] == json!("notifications/events/event"))
        .expect("replayed event");
    assert_eq!(
        sequence_from_cursor(streamed["params"]["eventId"].as_str().unwrap_or_default()),
        Some(front),
        "stream replay starts at the retained log front"
    );
}

#[tokio::test]
async fn event_subscriptions_reject_two_selectors_and_accept_the_whole_hub() {
    let mut session = Session::spawn(ScriptHost);
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "events/stream",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public", "jobId": "hj_1"}
            }),
        )
        .await;
    assert_eq!(response["error"]["code"], json!(-32602), "{response}");
    assert_eq!(
        response["error"]["data"]["code"],
        json!("request.invalid"),
        "{response}"
    );

    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "events/stream",
            "params": {"name": "hub.job", "arguments": {}}
        }))
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .starts_with("text/event-stream")
    );
    drop(response);
}

#[tokio::test]
async fn events_unsubscribe_stops_a_stream_by_request_id() {
    let host = HubLifecycleHost::new(vec![Vec::new()], Vec::new(), HashMap::new());
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    let response = session
        .start_stream(&json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "events/subscribe",
            "params": {}
        }))
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let stopped = session
        .request(7, "events/unsubscribe", json!({"requestId": 6}))
        .await;
    assert_eq!(stopped["result"]["unsubscribed"], json!(true), "{stopped}");
    let body = tokio::time::timeout(Duration::from_secs(2), response.text())
        .await
        .expect("unsubscribe closes the stream")
        .expect("stream body");
    assert!(!body.contains("\"resultType\":\"complete\""), "{body}");
    let missing = session
        .request(8, "events/unsubscribe", json!({"requestId": 6}))
        .await;
    assert_eq!(missing["result"]["unsubscribed"], json!(false), "{missing}");
}

#[tokio::test]
async fn events_trim_run_constants_and_collapse_terminal_errors() {
    let mut session = Session::spawn(EventHost::new(vec![
        watch_event("run-public", "v2:1", running_status("worker")),
        watch_event("run-public", "v2:2", finished_status()),
    ]));
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public"},
                "cursor": "v2:0"
            }),
        )
        .await;
    let events = response["result"]["events"]
        .as_array()
        .expect("poll events");
    assert_eq!(events.len(), 2);

    let running = &events[0];
    assert_eq!(running["data"]["cursor"], json!("v2:1"));
    assert!(running["data"].get("title").is_none());
    assert!(running["data"].get("size").is_none());
    assert!(running["data"].get("source").is_none());
    assert!(running["data"].get("subscriptionId").is_none());
    assert!(running["data"]["status"].get("metadata").is_none());
    assert_eq!(
        running["data"]["summary"],
        json!({"phase": "running", "node": "worker", "terminal": false, "reason": null})
    );

    let finished = &events[1];
    assert_eq!(finished["data"]["cursor"], json!("v2:2"));
    assert!(finished["data"].get("title").is_none());
    assert!(finished["data"].get("size").is_none());
    assert!(finished["data"].get("source").is_none());
    assert!(finished["data"].get("subscriptionId").is_none());
    assert!(finished["data"]["status"].get("metadata").is_some());
    assert_eq!(
        finished["data"]["status"]["terminalResult"]["context"]["recorded"],
        json!({"reason": "worker_failed", "lastError": "last error", "errorCount": 2})
    );
    assert_eq!(
        finished["data"]["summary"],
        json!({
            "phase": "finished",
            "node": null,
            "terminal": true,
            "reason": "worker_failed"
        })
    );
}

#[tokio::test]
async fn events_verbose_keeps_the_recorded_failure_array() {
    let mut session = Session::spawn(EventHost::new(vec![watch_event(
        "run-public",
        "v2:2",
        finished_status(),
    )]));
    initialize(&mut session).await;
    let collapsed = session
        .request(
            2,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public"},
                "cursor": "v2:1"
            }),
        )
        .await;
    assert_eq!(
        collapsed["result"]["events"][0]["data"]["status"]["terminalResult"]["context"]["recorded"],
        json!({"reason": "worker_failed", "lastError": "last error", "errorCount": 2})
    );

    let verbose = session
        .request(
            3,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public", "verbose": true},
                "cursor": "v2:1"
            }),
        )
        .await;
    let recorded =
        verbose["result"]["events"][0]["data"]["status"]["terminalResult"]["context"]["recorded"]
            .as_array()
            .expect("verbose keeps the recorded array");
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[1]["message"], json!("last error"));
}

#[tokio::test]
async fn events_terminal_only_skips_nonterminal_events_and_advances_the_cursor() {
    let mut session = Session::spawn(EventHost::new(vec![
        watch_event("run-public", "v2:2", running_status("worker")),
        watch_event("run-public", "v2:3", finished_status()),
    ]));
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public", "terminalOnly": true},
                "cursor": "v2:1"
            }),
        )
        .await;
    assert_eq!(response["result"]["events"].as_array().unwrap().len(), 1);
    assert_eq!(
        response["result"]["events"][0]["eventId"],
        json!("run-public:v2:3")
    );
    assert_eq!(response["result"]["cursor"], json!("v2:3"));
    assert_eq!(response["result"]["truncated"], json!(false));
    assert_eq!(
        response["result"]["events"][0]["data"]["replayed"],
        json!(false)
    );
    assert_eq!(
        response["result"]["events"][0]["data"]["summary"]["terminal"],
        json!(true)
    );
}

#[tokio::test]
async fn events_node_filter_keeps_the_terminal_event() {
    let mut session = Session::spawn(EventHost::new(vec![
        watch_event("run-public", "v2:2", running_status("worker")),
        watch_event("run-public", "v2:3", running_status("acceptance")),
        watch_event("run-public", "v2:4", finished_status()),
    ]));
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public", "nodes": ["worker"]},
                "cursor": "v2:1"
            }),
        )
        .await;
    let events = response["result"]["events"]
        .as_array()
        .expect("events array");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["data"]["cursor"], json!("v2:2"));
    assert_eq!(events[1]["data"]["cursor"], json!("v2:4"));
    assert_eq!(events[1]["data"]["summary"]["terminal"], json!(true));
    assert_eq!(response["result"]["cursor"], json!("v2:4"));

    let phased = session
        .request(
            3,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {
                    "runId": "run-public",
                    "nodes": ["worker"],
                    "phases": ["running"]
                },
                "cursor": "v2:1"
            }),
        )
        .await;
    let events = phased["result"]["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["data"]["cursor"], json!("v2:2"));
    assert!(
        events
            .iter()
            .all(|event| event["data"]["summary"]["terminal"] != json!(true))
    );
}

#[tokio::test]
async fn events_phase_filter_drops_events_without_truncation() {
    let mut session = Session::spawn(EventHost::new(vec![
        watch_event("run-public", "v2:2", running_status("worker")),
        watch_event("run-public", "v2:3", json!({"phase": "admitted"})),
        watch_event("run-public", "v2:4", finished_status()),
    ]));
    initialize(&mut session).await;
    let response = session
        .request(
            2,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public", "phases": ["running"]},
                "cursor": "v2:1"
            }),
        )
        .await;
    assert_eq!(response["result"]["events"].as_array().unwrap().len(), 1);
    assert_eq!(
        response["result"]["events"][0]["data"]["cursor"],
        json!("v2:2")
    );
    assert_eq!(response["result"]["cursor"], json!("v2:4"));
    assert_eq!(response["result"]["truncated"], json!(false));

    let invalid = session
        .request(
            3,
            "events/poll",
            json!({
                "name": "run.event",
                "arguments": {"runId": "run-public", "phases": ["unknown"]},
                "cursor": "v2:1"
            }),
        )
        .await;
    assert_eq!(invalid["error"]["code"], json!(-32602));
}

#[test]
fn inline_tool_data_is_materialized_for_the_command() {
    let original = json!({
        "input": {"task": "Fix the parser"},
        "graph": {"profile": "openengine.graph.full/v1"},
        "runtimeConfig": {"agents": {}},
        "uniformRuntimeConfig": {"harness": "codex"},
        "environment": {"variables": {"MODE": "test"}},
    });
    let mut arguments: Map<String, Value> = original
        .as_object()
        .expect("inline fixture is an object")
        .clone();
    let directory = super::tools::materialize_inline(&mut arguments)
        .expect("inline data materializes")
        .expect("inline data creates a temporary directory");
    let root = directory.path().to_path_buf();
    for key in [
        "input",
        "graph",
        "runtimeConfig",
        "uniformRuntimeConfig",
        "environment",
    ] {
        let path = arguments[key].as_str().expect("argument became a path");
        let stored: Value =
            serde_json::from_slice(&std::fs::read(path).expect("temporary command input exists"))
                .expect("temporary command input is JSON");
        assert_eq!(stored, original[key], "{key} round trips");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(path)
                .expect("temporary command input metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "{key} is private");
        }
    }
    drop(directory);
    assert!(!root.exists(), "temporary command inputs are removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_transport_handshakes_and_answers_notifications() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind MCP HTTP test listener");
    let address = listener.local_addr().expect("MCP HTTP test address");
    let server = tokio::spawn(serve_http_listener(
        ToolHost {
            backend: CliTestBackend::default(),
        },
        listener,
    ));
    let url = format!("http://{address}/mcp");
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"}
            }
        }))
        .send()
        .await
        .expect("initialize over HTTP");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let session = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .expect("initialize returns a session id")
        .to_owned();
    assert!(!session.is_empty());
    let initialized: Value = response.json().await.expect("initialize response JSON");
    assert_eq!(
        initialized["result"]["serverInfo"]["name"],
        json!("zeroshot")
    );

    let response = client
        .post(&url)
        .header("mcp-session-id", &session)
        .json(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        .send()
        .await
        .expect("notification over HTTP");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    assert!(
        response
            .bytes()
            .await
            .expect("notification body")
            .is_empty()
    );

    let response = client
        .post(&url)
        .header("mcp-session-id", &session)
        .json(&json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
        .send()
        .await
        .expect("tools/list over HTTP");
    let listed: Value = response.json().await.expect("tools/list response JSON");
    assert_eq!(listed["id"], json!(2));
    assert!(
        listed["result"]["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .any(|tool| tool["name"] == "run")
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_transport_streams_events_as_server_sent_events() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind MCP HTTP test listener");
    let address = listener.local_addr().expect("MCP HTTP test address");
    let server = tokio::spawn(serve_http_listener(hub_feed_host(), listener));
    let url = format!("http://{address}/mcp");
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"}
            }
        }))
        .send()
        .await
        .expect("initialize over HTTP");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let session = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .expect("initialize returns a session id")
        .to_owned();

    let response = client
        .post(&url)
        .header("mcp-session-id", &session)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "events/stream",
            "params": {"name": "hub.run", "arguments": {}}
        }))
        .send()
        .await
        .expect("events/stream over HTTP");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .starts_with("text/event-stream")
    );
    let frames = collect_stream(response, Duration::from_secs(2), |frames| {
        stream_notifications(frames)
            .iter()
            .any(|frame| frame["params"]["name"] == json!("hub.run"))
    })
    .await;
    assert!(
        frames
            .iter()
            .any(|frame| frame["method"] == "notifications/events/active")
    );
    assert!(
        stream_notifications(&frames)
            .iter()
            .any(|frame| frame["method"] == "notifications/events/event")
    );
    assert!(!has_complete_frame(&frames));
    server.abort();
}
