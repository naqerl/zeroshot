use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use super::super::{CliTestBackend, CliTestCall, NeverDetach, RunWatchCommand};
use super::{serve, serve_http_listener, McpFailure, McpHost, WatchStream};
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

struct Session {
    input: tokio::io::DuplexStream,
    output: BufReader<tokio::io::DuplexStream>,
}

impl Session {
    fn spawn(host: impl McpHost + 'static) -> Self {
        let (client_input, server_input) = tokio::io::duplex(1 << 20);
        let (server_output, client_output) = tokio::io::duplex(1 << 20);
        // The CLI executor future is not Send, so the server runs on its own
        // current-thread runtime.
        let _thread = std::thread::Builder::new()
            .name("mcp-test-server".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("mcp test runtime");
                runtime.block_on(async move {
                    let reader = BufReader::new(server_input);
                    let _ = serve(host, reader, server_output).await;
                });
            })
            .expect("spawn mcp test server");
        Self {
            input: client_input,
            output: BufReader::new(client_output),
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

    async fn send(&mut self, message: &Value) {
        let mut bytes = serde_json::to_vec(message).expect("request json");
        bytes.push(b'\n');
        self.input.write_all(&bytes).await.expect("write request");
    }

    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut self.output))
            .await
            .expect("timed out waiting for MCP output")
            .expect("read MCP output");
        serde_json::from_str(&line).expect("MCP output is JSON")
    }
}

async fn read_frame(output: &mut BufReader<tokio::io::DuplexStream>) -> std::io::Result<String> {
    let mut line = String::new();
    let read = output.read_line(&mut line).await?;
    if read == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "closed",
        ));
    }
    Ok(line)
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
    assert_eq!(event["inputSchema"]["required"], json!(["runId"]));
    assert!(event["inputSchema"]["properties"].get("target").is_some());
    assert_eq!(
        event["payloadSchema"]["required"],
        json!([
            "subscriptionId",
            "runId",
            "title",
            "size",
            "cursor",
            "status"
        ])
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
    assert_eq!(
        response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok()),
        Some("default")
    );
    let initialized: Value = response.json().await.expect("initialize response JSON");
    assert_eq!(
        initialized["result"]["serverInfo"]["name"],
        json!("zeroshot")
    );

    let response = client
        .post(&url)
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

    let response = client
        .post(&url)
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
