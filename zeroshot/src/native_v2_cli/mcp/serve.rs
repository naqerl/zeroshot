use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;

use super::events::{self, Prepared, EXTENSION_ID};
use super::host::{McpFailure, McpHost};
use super::tools;

const KNOWN_VERSIONS: &[&str] = &[
    "2024-11-05",
    "2025-03-26",
    "2025-06-18",
    "2025-11-25",
    "2026-07-28",
];

struct StreamCancel {
    flag: Arc<AtomicBool>,
    handle: AbortHandle,
}

pub(super) enum Action {
    Reply(Value),
    Fail {
        code: i64,
        message: &'static str,
        data: Option<Value>,
    },
    Stream(Prepared),
}

/// Per-session MCP state held by the HTTP transport worker.
#[derive(Default)]
pub(super) struct SessionState {
    initialized: bool,
    cancellations: HashMap<String, StreamCancel>,
}

impl SessionState {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn initialized(&self) -> bool {
        self.initialized
    }

    pub(super) fn mark_initialized(&mut self) {
        self.initialized = true;
    }

    pub(super) fn reset_for_initialize(&mut self) {
        self.shutdown();
        self.initialized = false;
    }

    pub(super) fn handle_notification(&mut self, message: &Value) {
        handle_notification(message, &mut self.cancellations);
    }

    pub(super) fn spawn_stream<W>(&mut self, prepared: Prepared, id: Value, writer: &Arc<Mutex<W>>)
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        spawn_stream(prepared, id, writer, &mut self.cancellations);
    }

    pub(super) fn shutdown(&mut self) {
        for cancel in self.cancellations.values() {
            cancel.flag.store(true, Ordering::Relaxed);
            cancel.handle.abort();
        }
        self.cancellations.clear();
    }
}

/// Handles one complete JSON-RPC message without transport-specific I/O.
/// Writes one newline-delimited JSON-RPC message. The HTTP transport turns those lines
/// into server-sent event data frames.
pub(super) async fn write_message<W>(writer: &Mutex<W>, message: &Value) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = serde_json::to_vec(message).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    let mut writer = writer.lock().await;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

fn spawn_stream<W>(
    prepared: Prepared,
    id: Value,
    writer: &Arc<Mutex<W>>,
    cancellations: &mut HashMap<String, StreamCancel>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let flag = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn(events::drive(
        prepared,
        id.clone(),
        Arc::clone(writer),
        Arc::clone(&flag),
    ));
    let cancel = StreamCancel {
        flag,
        handle: task.abort_handle(),
    };
    if let Some(previous) = cancellations.insert(id_key(&id), cancel) {
        previous.flag.store(true, Ordering::Relaxed);
        previous.handle.abort();
    }
}

pub(super) async fn dispatch<H>(host: &H, message: &Value, initialized: bool) -> Action
where
    H: McpHost + 'static,
{
    if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Action::Fail {
            code: -32600,
            message: "Invalid Request",
            data: None,
        };
    }
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return Action::Fail {
            code: -32600,
            message: "Invalid Request",
            data: None,
        };
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    if !params.is_null() && !params.is_object() {
        return Action::Fail {
            code: -32602,
            message: "Invalid params",
            data: None,
        };
    }
    if method == "initialize" {
        return match negotiate(&params) {
            Ok(version) => Action::Reply(initialize_result(version)),
            Err(failure) => invalid_params(failure),
        };
    }
    if !initialized {
        return Action::Fail {
            code: -32600,
            message: "Invalid Request",
            data: None,
        };
    }
    match method {
        "ping" => Action::Reply(json!({})),
        "tools/list" => match tools::list(&params) {
            Ok(result) => Action::Reply(result),
            Err(failure) => invalid_params(failure),
        },
        "tools/call" => match tools::call(host, &params).await {
            Ok(result) => Action::Reply(result),
            Err(failure) => invalid_params(failure),
        },
        "events/list" => match events::list(&params) {
            Ok(result) => Action::Reply(result),
            Err(error) => event_failure(error),
        },
        "events/poll" => match events::poll(host, &params).await {
            Ok(result) => Action::Reply(result),
            Err(error) => event_failure(error),
        },
        "events/stream" => match events::prepare(host, &params).await {
            Ok(prepared) => Action::Stream(prepared),
            Err(error) => event_failure(error),
        },
        "events/subscribe" | "events/unsubscribe" => event_failure(events::unsupported_webhook()),
        _ => Action::Fail {
            code: -32601,
            message: "Method not found",
            data: None,
        },
    }
}

fn handle_notification(message: &Value, cancellations: &mut HashMap<String, StreamCancel>) {
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };
    if method != "notifications/cancelled" {
        return;
    }
    let Some(request_id) = message.pointer("/params/requestId") else {
        return;
    };
    if let Some(cancel) = cancellations.remove(&id_key(request_id)) {
        cancel.flag.store(true, Ordering::Relaxed);
        cancel.handle.abort();
    }
}

fn invalid_params(failure: McpFailure) -> Action {
    Action::Fail {
        code: -32602,
        message: "Invalid params",
        data: Some(json!({"code": failure.code, "message": failure.message})),
    }
}

fn event_failure(error: events::EventError) -> Action {
    let (code, message, data) = events::rpc_error(error);
    Action::Fail {
        code,
        message,
        data: Some(data),
    }
}

fn negotiate(params: &Value) -> Result<String, McpFailure> {
    match params.get("protocolVersion") {
        None | Some(Value::Null) => Ok("2025-06-18".to_owned()),
        Some(Value::String(version)) if KNOWN_VERSIONS.contains(&version.as_str()) => {
            Ok(version.clone())
        }
        Some(Value::String(_)) => Ok("2025-06-18".to_owned()),
        Some(_) => Err(McpFailure::invalid("protocolVersion must be a string")),
    }
}

fn initialize_result(protocol_version: String) -> Value {
    let mut result = json!({
        "protocolVersion": protocol_version,
        "capabilities": {
            "tools": {"listChanged": false},
            "extensions": {}
        },
        "serverInfo": {
            "name": "zeroshot",
            "version": env!("CARGO_PKG_VERSION")
        },
        "instructions": "Zeroshot tools run the same commands as the CLI. Run events use the \
            io.modelcontextprotocol/events extension and the durable watch stream."
    });
    result["capabilities"]["extensions"][EXTENSION_ID] = json!({});
    result
}

pub(super) fn valid_id(id: &Value) -> bool {
    id.is_number() || id.is_string()
}

fn id_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_default()
}

pub(super) fn ok_message(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

pub(super) fn error_message(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = json!({"code": code, "message": message});
    if let Some(data) = data {
        error["data"] = data;
    }
    json!({"jsonrpc": "2.0", "id": id, "error": error})
}
