use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
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
    alive: Arc<AtomicBool>,
    handle: AbortHandle,
}

struct JobCancel {
    requests: Vec<String>,
    handle: AbortHandle,
}

struct StreamLease {
    active: Rc<Cell<usize>>,
    alive: Arc<AtomicBool>,
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        self.active.set(self.active.get().saturating_sub(1));
        self.alive.store(false, Ordering::Relaxed);
    }
}

pub(super) enum Action {
    Reply(Value),
    Fail {
        code: i64,
        message: &'static str,
        data: Option<Value>,
    },
    Stream(Box<Prepared>),
}

pub(super) struct StreamRequest<W> {
    pub(super) prepared: Box<Prepared>,
    pub(super) id: Value,
    pub(super) writer: Arc<Mutex<W>>,
}

/// Per-session MCP state held by the HTTP transport worker.
#[derive(Default)]
pub(super) struct SessionState {
    initialized: bool,
    cancellations: HashMap<String, StreamCancel>,
    jobs: HashMap<u64, JobCancel>,
    active_streams: Rc<Cell<usize>>,
    /// Monotone per-session occurrence counter shared by `events/poll` calls, so a client
    /// polling the same MCP session sees `sequence: 1, 2, 3...` across responses.
    event_sequence: Rc<Cell<u64>>,
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
        // The initialize request is itself an in-flight job; resetting a session cancels its
        // streams but must never abort the job that is answering the reset.
        self.shutdown_streams();
        self.initialized = false;
    }

    pub(super) fn handle_notification(&mut self, message: &Value) {
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return;
        };
        if method != "notifications/cancelled" {
            return;
        }
        let Some(request_id) = message.pointer("/params/requestId") else {
            return;
        };
        self.cancel_request(request_id);
    }

    pub(super) fn unsubscribe(&mut self, request_id: &Value) -> bool {
        self.cancel_request(request_id)
    }

    /// Cancel the stream or in-flight job answering this request id.
    fn cancel_request(&mut self, request_id: &Value) -> bool {
        let key = id_key(request_id);
        let mut stopped = false;
        if let Some(cancel) = self.cancellations.remove(&key) {
            stopped = cancel.alive.swap(false, Ordering::Relaxed);
            cancel.flag.store(true, Ordering::Relaxed);
            cancel.handle.abort();
        }
        let jobs = self
            .jobs
            .iter()
            .filter(|(_, job)| job.requests.iter().any(|request| request == &key))
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in jobs {
            if let Some(job) = self.jobs.remove(&id) {
                if !job.handle.is_finished() {
                    stopped = true;
                }
                job.handle.abort();
            }
        }
        stopped
    }

    pub(super) fn track_job(&mut self, id: u64, requests: Vec<String>, handle: AbortHandle) {
        // Completed jobs are harmless but would accumulate for a long-lived session. Prune them
        // whenever a new job is tracked; in-flight jobs remain cancellable for DELETE,
        // notifications/cancelled, and events/unsubscribe.
        self.jobs.retain(|_, job| !job.handle.is_finished());
        self.jobs.insert(id, JobCancel { requests, handle });
    }

    pub(super) fn spawn_stream<W>(&mut self, request: StreamRequest<W>)
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        spawn_stream(
            request,
            &mut self.cancellations,
            Rc::clone(&self.active_streams),
        );
    }

    pub(super) fn has_active_streams(&self) -> bool {
        self.active_streams.get() > 0
    }

    pub(super) fn event_sequence(&self) -> Rc<Cell<u64>> {
        Rc::clone(&self.event_sequence)
    }

    fn shutdown_streams(&mut self) {
        for cancel in self.cancellations.values() {
            cancel.flag.store(true, Ordering::Relaxed);
            cancel.handle.abort();
        }
        self.cancellations.clear();
    }

    pub(super) fn shutdown(&mut self) {
        self.shutdown_streams();
        for job in self.jobs.values() {
            job.handle.abort();
        }
        self.jobs.clear();
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
    request: StreamRequest<W>,
    cancellations: &mut HashMap<String, StreamCancel>,
    active: Rc<Cell<usize>>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let StreamRequest {
        prepared,
        id,
        writer,
    } = request;
    let flag = Arc::new(AtomicBool::new(false));
    let alive = Arc::new(AtomicBool::new(true));
    let output = events::StreamOutput::new(id.clone(), writer, Arc::clone(&flag));
    active.set(active.get().saturating_add(1));
    let lease = StreamLease {
        active,
        alive: Arc::clone(&alive),
    };
    let task = tokio::task::spawn_local(async move {
        let _lease = lease;
        let _ = events::drive(*prepared, output).await;
    });
    let cancel = StreamCancel {
        flag,
        alive,
        handle: task.abort_handle(),
    };
    if let Some(previous) = cancellations.insert(id_key(&id), cancel) {
        previous.flag.store(true, Ordering::Relaxed);
        previous.handle.abort();
    }
}

pub(super) async fn dispatch<H>(
    host: &Rc<H>,
    feed: &Rc<events::HubFeed>,
    session: &Rc<std::cell::RefCell<SessionState>>,
    message: &Value,
) -> Action
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
    if !session.borrow().initialized() {
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
        "events/poll" => {
            let sequence = session.borrow().event_sequence();
            match events::poll(host, feed, &params, sequence.as_ref()).await {
                Ok(result) => Action::Reply(result),
                Err(error) => event_failure(error),
            }
        }
        "events/stream" => match events::prepare(host, feed, &params).await {
            Ok(prepared) => Action::Stream(Box::new(prepared)),
            Err(error) => event_failure(error),
        },
        "events/subscribe" => match events::prepare(host, feed, &params).await {
            Ok(prepared) => Action::Stream(Box::new(prepared)),
            Err(error) => event_failure(error),
        },
        "events/unsubscribe" => match events::unsubscribe(&params) {
            Ok(request_id) => {
                let removed = session.borrow_mut().unsubscribe(&request_id);
                Action::Reply(json!({
                    "resultType": "complete",
                    "unsubscribed": removed,
                }))
            }
            Err(error) => event_failure(error),
        },
        _ => Action::Fail {
            code: -32601,
            message: "Method not found",
            data: None,
        },
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

pub(super) fn id_key(id: &Value) -> String {
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
