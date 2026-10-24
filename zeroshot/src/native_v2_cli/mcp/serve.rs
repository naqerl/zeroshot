use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;

use super::events::{self, Prepared, EXTENSION_ID};
use super::host::{McpFailure, McpHost};
use super::tools;

const MAX_LINE: usize = 16 * 1024 * 1024;
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

enum Action {
    Reply(Value),
    Fail {
        code: i64,
        message: &'static str,
        data: Option<Value>,
    },
    Stream(Prepared),
    Ignore,
}

/// Serve newline-delimited JSON-RPC MCP on `reader` and `writer` until stdin closes.
pub async fn serve<H, R, W>(host: H, mut reader: R, writer: W) -> io::Result<()>
where
    H: McpHost + 'static,
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let host = Arc::new(host);
    let writer = Arc::new(Mutex::new(writer));
    let mut cancellations: HashMap<String, StreamCancel> = HashMap::new();
    let mut initialized = false;
    loop {
        let Some(line) = read_line(&mut reader).await? else {
            break;
        };
        if line.is_empty() {
            continue;
        }
        let message = match serde_json::from_str::<Value>(&line) {
            Ok(message) => message,
            Err(_) => {
                write_message(
                    &writer,
                    &error_message(Value::Null, -32700, "Parse error", None),
                )
                .await?;
                continue;
            }
        };
        if message.get("id").is_none() {
            handle_notification(&message, &mut cancellations);
            continue;
        }
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        if !valid_id(&id) {
            write_message(
                &writer,
                &error_message(Value::Null, -32600, "Invalid Request", None),
            )
            .await?;
            continue;
        }
        match dispatch(&host, &message, initialized).await {
            Action::Reply(result) => {
                if message.get("method").and_then(Value::as_str) == Some("initialize") {
                    initialized = true;
                }
                write_message(&writer, &ok_message(id, result)).await?;
            }
            Action::Fail {
                code,
                message: text,
                data,
            } => {
                write_message(&writer, &error_message(id, code, text, data)).await?;
            }
            Action::Stream(prepared) => {
                spawn_stream(prepared, id, &writer, &mut cancellations);
            }
            Action::Ignore => {}
        }
    }
    for cancel in cancellations.values() {
        cancel.flag.store(true, Ordering::Relaxed);
        cancel.handle.abort();
    }
    Ok(())
}

pub(super) async fn write_message<W>(writer: &Mutex<W>, message: &Value) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = serde_json::to_vec(message).map_err(io::Error::other)?;
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

async fn dispatch<H>(host: &Arc<H>, message: &Value, initialized: bool) -> Action
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
        if initialized {
            return Action::Fail {
                code: -32600,
                message: "Invalid Request",
                data: None,
            };
        }
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
        "tools/call" => match tools::call(host.as_ref(), &params).await {
            Ok(result) => Action::Reply(result),
            Err(failure) => invalid_params(failure),
        },
        "events/list" => match events::list(&params) {
            Ok(result) => Action::Reply(result),
            Err(error) => event_failure(error),
        },
        "events/poll" => match events::poll(host.as_ref(), &params).await {
            Ok(result) => Action::Reply(result),
            Err(error) => event_failure(error),
        },
        "events/stream" => match events::prepare(host.as_ref(), &params).await {
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

fn valid_id(id: &Value) -> bool {
    id.is_number() || id.is_string()
}

fn id_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_default()
}

fn ok_message(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_message(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = json!({"code": code, "message": message});
    if let Some(data) = data {
        error["data"] = data;
    }
    json!({"jsonrpc": "2.0", "id": id, "error": error})
}

async fn read_line<R>(reader: &mut R) -> io::Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unterminated JSON-RPC line",
            ));
        }
        if let Some(position) = available.iter().position(|byte| *byte == b'\n') {
            if line.len().saturating_add(position) > MAX_LINE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "JSON-RPC line exceeds 16 MiB",
                ));
            }
            line.extend_from_slice(&available[..=position]);
            reader.consume(position + 1);
            break;
        }
        if line.len().saturating_add(available.len()) > MAX_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "JSON-RPC line exceeds 16 MiB",
            ));
        }
        let consumed = available.len();
        line.extend_from_slice(available);
        reader.consume(consumed);
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    String::from_utf8(line)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
