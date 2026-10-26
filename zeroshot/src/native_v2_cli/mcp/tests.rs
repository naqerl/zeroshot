use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use super::super::{CliTestBackend, CliTestCall, NeverDetach, RunWatchCommand};
use super::{McpFailure, McpHost, McpServer, WatchStream, serve_http_listener};
use crate::native_v2_cli::hub::{HubCommand, HubRequest};
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

    fn streaming(replay: Vec<Value>, live: Vec<Value>, live_delay: Duration) -> Self {
        Self {
            replay,
            live,
            live_delay,
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

/// A hub host whose job status advances through `statuses` on each read and whose
/// placed run exposes the scripted watch events.
struct JobHost {
    statuses: Vec<Value>,
    reads: AtomicUsize,
    events: Vec<Value>,
    watched_targets: Arc<Mutex<Vec<Option<String>>>>,
}

impl JobHost {
    fn new(statuses: Vec<Value>, events: Vec<Value>) -> Self {
        assert!(!statuses.is_empty(), "a job host needs at least one status");
        Self {
            statuses,
            reads: AtomicUsize::new(0),
            events,
            watched_targets: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn watched_targets(&self) -> Arc<Mutex<Vec<Option<String>>>> {
        Arc::clone(&self.watched_targets)
    }
}

#[async_trait(?Send)]
impl McpHost for JobHost {
    async fn execute(&self, command: NativeV2CliCommand) -> Result<String, McpFailure> {
        match command {
            NativeV2CliCommand::Hub(HubRequest {
                command: HubCommand::Status { job },
                ..
            }) => {
                let read = self.reads.fetch_add(1, Ordering::SeqCst);
                let index = read.min(self.statuses.len() - 1);
                Ok(json!({
                    "schema": "zeroshot.hub.job/v1",
                    "job": self.statuses[index],
                    "requestedJob": job,
                })
                .to_string())
            }
            _ => Err(McpFailure::invalid("job host only answers hub status")),
        }
    }

    async fn open_watch(&self, command: &RunWatchCommand) -> Result<WatchStream, McpFailure> {
        self.watched_targets
            .lock()
            .expect("watched targets")
            .push(command.run.target.clone());
        let run_id = command.run.run_id.as_str();
        let (sender, receiver) = mpsc::channel(32);
        for event in self.events.clone() {
            let mut event = event;
            event["runId"] = json!(run_id);
            sender
                .try_send(Ok(event))
                .expect("scripted job run event fits the buffer");
        }
        drop(sender);
        let task = tokio::spawn(async {});
        Ok(WatchStream::new(receiver, task))
    }
}

fn sequence_from_cursor(cursor: &str) -> Option<u64> {
    cursor.strip_prefix("v2:")?.parse().ok()
}

fn sequence(event: &Value) -> Option<u64> {
    sequence_from_cursor(event.get("cursor")?.as_str()?)
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

    fn drain_frames(&mut self) -> Vec<Value> {
        self.frames.drain(..).collect()
    }
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
async fn events_list_describes_poll_and_push_only() {
    let mut session = Session::spawn(ScriptHost);
    initialize(&mut session).await;
    let listed = session.request(2, "events/list", json!({})).await;
    assert_eq!(listed["result"]["resultType"], json!("complete"));
    let event = &listed["result"]["events"][0];
    assert_eq!(event["name"], json!("run.event"));
    assert_eq!(event["delivery"], json!(["poll", "push"]));
    assert!(
        event["description"]
            .as_str()
            .unwrap_or_default()
            .contains("unsupported")
    );
    assert_eq!(
        event["inputSchema"]["oneOf"],
        json!([{"required": ["runId"]}, {"required": ["jobId"]}])
    );
    assert!(event["inputSchema"]["properties"].get("runId").is_some());
    assert!(event["inputSchema"]["properties"].get("jobId").is_some());
    assert!(event["inputSchema"]["properties"].get("target").is_some());
    assert_eq!(
        event["payloadSchema"]["required"],
        json!([
            "eventId",
            "name",
            "timestamp",
            "data",
            "summary",
            "replayed",
            "_meta"
        ])
    );
    assert!(
        event["payloadSchema"]["properties"]["data"]["oneOf"][0]
            .get("properties")
            .is_some()
    );
    assert!(
        event["payloadSchema"]["properties"]["jobId"].is_object(),
        "payload documents jobId"
    );
    for filter in ["phases", "nodes", "terminalOnly", "verbose"] {
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
    let subscribe = session
        .request(4, "events/subscribe", json!({"name": "run.event"}))
        .await;
    assert_eq!(subscribe["error"]["code"], json!(-32026));
    assert_eq!(subscribe["error"]["data"]["feature"], json!("deliveryMode"));
    assert_eq!(subscribe["error"]["data"]["value"], json!("webhook"));
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
async fn events_poll_reports_a_skipped_window() {
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
    assert_eq!(response["result"]["truncated"], json!(true));
    assert_eq!(response["result"]["cursor"], json!("v2:5"));
    assert_eq!(
        response["result"]["events"][0]["data"]["cursor"],
        json!("v2:5")
    );
}

#[tokio::test]
async fn events_stream_pushes_a_notification() {
    let mut session = Session::spawn(ScriptHost);
    initialize(&mut session).await;
    session
        .send(&json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "events/stream",
            "params": {
                "name": "run.event",
                "arguments": {"runId": "run-public"},
                "cursor": "v2:1"
            }
        }))
        .await;
    let mut saw_event = false;
    for _ in 0..8 {
        let message = session.read().await;
        if message["method"] == "notifications/events/active" {
            assert_eq!(
                message["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
                json!(7)
            );
        }
        if message["method"] == "notifications/events/event" {
            assert_eq!(message["params"]["cursor"], json!("v2:2"));
            assert_eq!(message["params"]["data"]["cursor"], json!("v2:2"));
            assert_eq!(message["params"]["name"], json!("run.event"));
            assert_eq!(
                message["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
                json!(7)
            );
            saw_event = true;
            break;
        }
    }
    assert!(saw_event);
}

#[tokio::test]
async fn events_job_with_a_run_delivers_that_runs_events() {
    let mut session = Session::spawn(JobHost::new(
        vec![json!({
            "jobId": "hj_placed",
            "state": "running",
            "target": "builder",
            "runId": "run-public"
        })],
        vec![watch_event("ignored", "v2:1", running_status("worker"))],
    ));
    initialize(&mut session).await;
    session
        .send(&json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "events/stream",
            "params": {
                "name": "run.event",
                "arguments": {"jobId": "hj_placed"}
            }
        }))
        .await;
    let frames = session.drain_frames();
    let event = frames
        .iter()
        .find(|frame| frame["method"] == "notifications/events/event")
        .expect("the placed job's run event");
    assert_eq!(event["params"]["eventId"], json!("run-public:v2:1"));
    assert_eq!(event["params"]["data"]["runId"], json!("run-public"));
    assert_eq!(
        event["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
        json!(8)
    );
    assert_eq!(
        frames.last().unwrap()["result"]["resultType"],
        json!("complete")
    );
}

#[tokio::test]
async fn events_local_job_opens_the_local_watch() {
    let host = JobHost::new(
        vec![json!({
            "jobId": "hj_local",
            "state": "running",
            "target": "local",
            "runId": "run-local"
        })],
        vec![watch_event("ignored", "v2:1", running_status("worker"))],
    );
    let watched_targets = host.watched_targets();
    let mut session = Session::spawn(host);
    initialize(&mut session).await;
    session
        .send(&json!({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "events/stream",
            "params": {
                "name": "run.event",
                "arguments": {"jobId": "hj_local"}
            }
        }))
        .await;
    let frames = session.drain_frames();
    assert!(
        frames
            .iter()
            .any(|frame| frame["method"] == "notifications/events/event")
    );
    assert_eq!(
        watched_targets.lock().expect("watched targets").as_slice(),
        [None]
    );
}

#[tokio::test]
async fn events_follow_a_queued_job_until_its_run_appears() {
    let mut session = Session::spawn(JobHost::new(
        vec![
            json!({"jobId": "hj_queued", "state": "queued", "runId": null}),
            json!({
                "jobId": "hj_queued",
                "state": "running",
                "target": "builder",
                "runId": "run-public"
            }),
        ],
        vec![watch_event("ignored", "v2:1", running_status("worker"))],
    ));
    initialize(&mut session).await;
    session
        .send(&json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "events/stream",
            "params": {
                "name": "run.event",
                "arguments": {"jobId": "hj_queued"}
            }
        }))
        .await;
    let frames = session.drain_frames();
    let queued = frames
        .iter()
        .find(|frame| {
            frame["method"] == "notifications/events/active"
                && frame["params"]["state"] == json!("queued")
        })
        .expect("active notification for the queued job");
    assert_eq!(queued["params"]["run"], Value::Null);
    assert_eq!(queued["params"]["job"]["jobId"], json!("hj_queued"));
    assert_eq!(queued["params"]["job"]["state"], json!("queued"));
    assert!(
        queued["params"]["eventId"]
            .as_str()
            .is_some_and(|event_id| event_id.starts_with("hj_queued:"))
    );
    let event = frames
        .iter()
        .find(|frame| frame["method"] == "notifications/events/event")
        .expect("run events after the job was placed");
    assert_eq!(event["params"]["data"]["runId"], json!("run-public"));
    assert_eq!(
        frames.last().unwrap()["result"]["resultType"],
        json!("complete")
    );
}

#[tokio::test]
async fn events_end_a_job_that_fails_before_it_runs() {
    let mut session = Session::spawn(JobHost::new(
        vec![json!({
            "jobId": "hj_rejected",
            "state": "failed",
            "runId": null,
            "error": "the target refused the submission",
            "errorKind": "submission_rejected"
        })],
        Vec::new(),
    ));
    initialize(&mut session).await;
    session
        .send(&json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "events/stream",
            "params": {
                "name": "run.event",
                "arguments": {"jobId": "hj_rejected"}
            }
        }))
        .await;
    let frames = session.drain_frames();
    let terminal = frames
        .iter()
        .find(|frame| {
            frame["method"] == "notifications/events/event"
                && frame["params"]["state"] == json!("failed")
        })
        .expect("terminal job notification");
    assert_eq!(
        terminal["params"]["error"],
        json!("the target refused the submission")
    );
    assert_eq!(
        terminal["params"]["errorKind"],
        json!("submission_rejected")
    );
    assert!(
        terminal["params"]["eventId"]
            .as_str()
            .is_some_and(|event_id| event_id.starts_with("hj_rejected:"))
    );
    assert_eq!(
        frames.last().unwrap()["result"]["resultType"],
        json!("complete")
    );
}

#[tokio::test]
async fn event_subscriptions_require_exactly_one_selector() {
    let mut session = Session::spawn(ScriptHost);
    initialize(&mut session).await;
    for (id, arguments) in [
        (2, json!({"runId": "run-public", "jobId": "hj_1"})),
        (3, json!({})),
    ] {
        let response = session
            .request(
                id,
                "events/stream",
                json!({"name": "run.event", "arguments": arguments}),
            )
            .await;
        assert_eq!(response["error"]["code"], json!(-32602), "{response}");
        assert_eq!(
            response["error"]["data"]["code"],
            json!("request.invalid"),
            "{response}"
        );
    }
}

#[tokio::test]
async fn events_trim_run_constants_and_collapse_terminal_errors() {
    let mut session = Session::spawn(EventHost::new(vec![
        watch_event("run-public", "v2:1", running_status("worker")),
        watch_event("run-public", "v2:2", finished_status()),
    ]));
    initialize(&mut session).await;
    session
        .send(&json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "events/stream",
            "params": {
                "name": "run.event",
                "arguments": {"runId": "run-public"}
            }
        }))
        .await;
    let frames = session.drain_frames();
    let active = frames
        .iter()
        .find(|frame| frame["method"] == "notifications/events/active")
        .expect("active notification");
    assert_eq!(
        active["params"]["run"],
        json!({
            "runId": "run-public",
            "title": "Repair checkout",
            "size": "medium",
            "source": source()
        })
    );
    let events = frames
        .iter()
        .filter(|frame| frame["method"] == "notifications/events/event")
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);

    let running = &events[0]["params"];
    assert_eq!(running["data"]["cursor"], json!("v2:1"));
    assert!(running["data"].get("title").is_none());
    assert!(running["data"].get("size").is_none());
    assert!(running["data"].get("source").is_none());
    assert!(running["data"].get("subscriptionId").is_none());
    assert!(running["data"]["status"].get("metadata").is_none());
    assert_eq!(
        running["summary"],
        json!({"phase": "running", "node": "worker", "terminal": false, "reason": null})
    );

    let finished = &events[1]["params"];
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
        finished["summary"],
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
    assert_eq!(response["result"]["events"][0]["replayed"], json!(false));
    assert_eq!(
        response["result"]["events"][0]["summary"]["terminal"],
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
    assert_eq!(events[1]["summary"]["terminal"], json!(true));
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
            .all(|event| event["summary"]["terminal"] != json!(true))
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

#[tokio::test]
async fn events_mark_replayed_and_live_events() {
    let mut session = Session::spawn(EventHost::streaming(
        vec![watch_event("run-public", "v2:1", running_status("worker"))],
        vec![watch_event("run-public", "v2:2", running_status("worker"))],
        Duration::from_millis(500),
    ));
    initialize(&mut session).await;
    session
        .send(&json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "events/stream",
            "params": {
                "name": "run.event",
                "arguments": {"runId": "run-public"}
            }
        }))
        .await;
    let frames = session.drain_frames();
    let events = frames
        .iter()
        .filter(|frame| frame["method"] == "notifications/events/event")
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["params"]["data"]["cursor"], json!("v2:1"));
    assert_eq!(events[0]["params"]["replayed"], json!(true));
    assert_eq!(events[1]["params"]["data"]["cursor"], json!("v2:2"));
    assert_eq!(events[1]["params"]["replayed"], json!(false));
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
    let server = tokio::spawn(serve_http_listener(ScriptHost, listener));
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
            "params": {
                "name": "run.event",
                "arguments": {"runId": "run-public"},
                "cursor": "v2:1"
            }
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
    let body = tokio::time::timeout(Duration::from_secs(5), response.text())
        .await
        .expect("events/stream ends")
        .expect("events/stream body");
    assert!(body.contains("notifications/events/active"));
    assert!(body.contains("notifications/events/event"));
    assert!(body.contains("\"resultType\":\"complete\""));
    server.abort();
}
