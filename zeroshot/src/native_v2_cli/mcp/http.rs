use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use futures_util::stream;
use serde_json::{Value, json};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::LocalSet;

use super::host::McpHost;
use super::serve::{self, Action, SessionState};

const MAX_BODY: usize = 16 * 1024 * 1024;
const SESSION_HEADER: HeaderName = HeaderName::from_static("mcp-session-id");
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

struct Job {
    session: Option<String>,
    message: Value,
    reply: oneshot::Sender<JobReply>,
}

struct JobReply {
    session: Option<String>,
    reply: WorkerReply,
}

enum WorkerReply {
    None,
    Message(Value),
    Stream(mpsc::UnboundedReceiver<String>),
}

#[derive(Clone)]
struct HttpState {
    jobs: mpsc::UnboundedSender<Job>,
}

/// The mounted MCP router plus the worker that answers its requests.
///
/// The host is `!Send`, so [`McpServer::spawn`] owns a dedicated thread with a current-thread
/// runtime and `LocalSet`; callers only keep this handle and merge the router into their own
/// listener.
pub struct McpServer {
    stop: Option<oneshot::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl McpServer {
    /// Start the MCP worker and return its router (`POST /mcp`) and shutdown handle.
    pub fn spawn<H>(host: H) -> io::Result<(Router, Self)>
    where
        H: McpHost + 'static,
    {
        let (jobs, stop, worker) = spawn_worker(host)?;
        let router = Router::new()
            .route(
                "/mcp",
                post(mcp_handler).layer(DefaultBodyLimit::max(MAX_BODY)),
            )
            .with_state(HttpState { jobs });
        Ok((
            router,
            Self {
                stop: Some(stop),
                worker: Some(worker),
            },
        ))
    }

    /// Stop the worker and wait for its sessions to shut down.
    pub async fn shutdown(mut self) {
        self.stop_worker();
        if let Some(worker) = self.worker.take() {
            let _ = tokio::task::spawn_blocking(move || worker.join()).await;
        }
    }

    fn stop_worker(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

impl Drop for McpServer {
    fn drop(&mut self) {
        self.stop_worker();
    }
}

/// Serve Streamable HTTP MCP on `POST /mcp` until the process is stopped.
pub async fn serve_http<H>(host: H, listen: SocketAddr) -> io::Result<()>
where
    H: McpHost + 'static,
{
    let listener = TcpListener::bind(listen).await?;
    serve_http_listener(host, listener).await
}

/// Serve Streamable HTTP MCP on an already-bound listener.
pub async fn serve_http_listener<H>(host: H, listener: TcpListener) -> io::Result<()>
where
    H: McpHost + 'static,
{
    let address = listener.local_addr()?;
    let (router, server) = McpServer::spawn(host)?;
    eprintln!("Zeroshot MCP: http://{address}/mcp");
    let result = tokio::select! {
        result = axum::serve(listener, router) => result,
        () = shutdown_signal() => Ok(()),
    };
    server.shutdown().await;
    result
}

async fn mcp_handler(State(state): State<HttpState>, headers: HeaderMap, body: Bytes) -> Response {
    let session = session_id(&headers);
    let message = match serde_json::from_slice::<Value>(&body) {
        Ok(message) => message,
        Err(_) => {
            return json_response(
                serve::error_message(Value::Null, -32700, "Parse error", None),
                session.as_deref(),
            );
        }
    };
    if let Value::Array(items) = &message
        && items.is_empty()
    {
        return json_response(
            serve::error_message(Value::Null, -32600, "Invalid Request", None),
            session.as_deref(),
        );
    }
    let reply = match request_reply(&state, session.clone(), message).await {
        Ok(reply) => reply,
        Err(()) => {
            return json_response(
                serve::error_message(Value::Null, -32603, "Internal error", None),
                session.as_deref(),
            );
        }
    };
    let session = reply.session.as_deref();
    match reply.reply {
        WorkerReply::None => accepted_response(session),
        WorkerReply::Message(value) => json_response(value, session),
        WorkerReply::Stream(receiver) => sse_response(receiver, session),
    }
}

async fn request_reply(
    state: &HttpState,
    session: Option<String>,
    message: Value,
) -> Result<JobReply, ()> {
    let (reply, response) = oneshot::channel();
    state
        .jobs
        .send(Job {
            session,
            message,
            reply,
        })
        .map_err(|_| ())?;
    response.await.map_err(|_| ())
}

fn json_response(value: Value, session: Option<&str>) -> Response {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    set_session_header(&mut response, session);
    response
}

fn accepted_response(session: Option<&str>) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::ACCEPTED;
    set_session_header(&mut response, session);
    response
}

fn sse_response(receiver: mpsc::UnboundedReceiver<String>, session: Option<&str>) -> Response {
    let stream = stream::unfold(receiver, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|line| (Ok::<_, Infallible>(Event::default().data(line)), receiver))
    });
    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response();
    set_session_header(&mut response, session);
    response
}

fn set_session_header(response: &mut Response, session: Option<&str>) {
    let Some(session) = session else {
        return;
    };
    if let Ok(value) = HeaderValue::from_str(session) {
        response.headers_mut().insert(SESSION_HEADER, value);
    }
}

fn session_id(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get(&SESSION_HEADER)
        .and_then(|value| value.to_str().ok())?;
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return None;
    }
    Some(value.to_owned())
}

fn spawn_worker<H>(
    host: H,
) -> io::Result<(
    mpsc::UnboundedSender<Job>,
    oneshot::Sender<()>,
    std::thread::JoinHandle<()>,
)>
where
    H: McpHost + 'static,
{
    let (jobs, mut receiver) = mpsc::unbounded_channel();
    let (stop, mut stopped) = oneshot::channel();
    let (ready, started) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::Builder::new()
        .name("zeroshot-mcp-http".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready.send(Err(error.to_string()));
                    return;
                }
            };
            runtime.block_on(async move {
                let local = LocalSet::new();
                local
                    .run_until(async move {
                        let host = Rc::new(host);
                        let sessions: Sessions = Rc::new(RefCell::new(HashMap::new()));
                        let _ = ready.send(Ok(()));
                        loop {
                            tokio::select! {
                                _ = &mut stopped => break,
                                job = receiver.recv() => {
                                    let Some(job) = job else {
                                        break;
                                    };
                                    let host = Rc::clone(&host);
                                    let sessions = Rc::clone(&sessions);
                                    tokio::task::spawn_local(async move {
                                        process_job(&host, &sessions, job).await;
                                    });
                                }
                            }
                        }
                        for (_, session) in sessions.borrow_mut().drain() {
                            session.state.borrow_mut().shutdown();
                        }
                    })
                    .await;
            });
        })?;
    match started.recv() {
        Ok(Ok(())) => Ok((jobs, stop, worker)),
        Ok(Err(error)) => Err(io::Error::other(error)),
        Err(_) => Err(io::Error::other("MCP HTTP worker stopped during startup")),
    }
}

struct SessionEntry {
    state: Rc<RefCell<SessionState>>,
    last_seen: Instant,
}

type Sessions = Rc<RefCell<HashMap<String, SessionEntry>>>;

struct ResolvedSession {
    id: Option<String>,
    state: Rc<RefCell<SessionState>>,
}

enum SessionError {
    Required,
    Unavailable,
}

fn collect_idle_sessions(sessions: &mut HashMap<String, SessionEntry>) {
    let now = Instant::now();
    sessions.retain(|_, entry| {
        if now.duration_since(entry.last_seen) > SESSION_IDLE_TIMEOUT {
            entry.state.borrow_mut().shutdown();
            false
        } else {
            true
        }
    });
}

fn new_session_id() -> Result<String, ()> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| ())?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn is_initialize(message: &Value) -> bool {
    message.get("method").and_then(Value::as_str) == Some("initialize")
}

fn request_is_initialize(message: &Value) -> bool {
    match message {
        Value::Array(items) => items.iter().any(is_initialize),
        _ => is_initialize(message),
    }
}

fn request_needs_session(message: &Value) -> bool {
    match message {
        Value::Array(items) => items
            .iter()
            .any(|item| item.get("id").is_some() || is_initialize(item)),
        _ => message.get("id").is_some() || is_initialize(message),
    }
}

fn resolve_session(
    sessions: &Sessions,
    requested: Option<String>,
    message: &Value,
) -> Result<ResolvedSession, SessionError> {
    let mut sessions = sessions.borrow_mut();
    collect_idle_sessions(&mut sessions);
    if let Some(id) = requested {
        let Some(entry) = sessions.get_mut(&id) else {
            return Err(SessionError::Required);
        };
        entry.last_seen = Instant::now();
        return Ok(ResolvedSession {
            id: Some(id),
            state: Rc::clone(&entry.state),
        });
    }
    if request_is_initialize(message) {
        let id = new_session_id().map_err(|()| SessionError::Unavailable)?;
        let state = Rc::new(RefCell::new(SessionState::new()));
        sessions.insert(
            id.clone(),
            SessionEntry {
                state: Rc::clone(&state),
                last_seen: Instant::now(),
            },
        );
        return Ok(ResolvedSession {
            id: Some(id),
            state,
        });
    }
    if request_needs_session(message) {
        return Err(SessionError::Required);
    }
    Ok(ResolvedSession {
        id: None,
        state: Rc::new(RefCell::new(SessionState::new())),
    })
}

fn session_required_reply(message: &Value) -> WorkerReply {
    let data = Some(json!({"code": "mcp.session_required"}));
    if let Value::Array(items) = message {
        let mut replies = Vec::new();
        for item in items {
            if !item.is_object() {
                replies.push(serve::error_message(
                    Value::Null,
                    -32600,
                    "Invalid Request",
                    None,
                ));
                continue;
            }
            if item.get("id").is_none() && !is_initialize(item) {
                continue;
            }
            let id = item
                .get("id")
                .cloned()
                .filter(serve::valid_id)
                .unwrap_or(Value::Null);
            replies.push(serve::error_message(
                id,
                -32600,
                "Invalid Request",
                data.clone(),
            ));
        }
        return WorkerReply::Message(Value::Array(replies));
    }
    let id = message
        .get("id")
        .cloned()
        .filter(serve::valid_id)
        .unwrap_or(Value::Null);
    WorkerReply::Message(serve::error_message(id, -32600, "Invalid Request", data))
}

async fn process_job<H>(host: &Rc<H>, sessions: &Sessions, job: Job)
where
    H: McpHost + 'static,
{
    let resolved = match resolve_session(sessions, job.session, &job.message) {
        Ok(resolved) => resolved,
        Err(SessionError::Required) => {
            let _ = job.reply.send(JobReply {
                session: None,
                reply: session_required_reply(&job.message),
            });
            return;
        }
        Err(SessionError::Unavailable) => {
            let _ = job.reply.send(JobReply {
                session: None,
                reply: WorkerReply::Message(serve::error_message(
                    Value::Null,
                    -32603,
                    "Internal error",
                    None,
                )),
            });
            return;
        }
    };
    let reply = process_request(host, &resolved.state, &job.message).await;
    let _ = job.reply.send(JobReply {
        session: resolved.id,
        reply,
    });
}

async fn process_request<H>(
    host: &Rc<H>,
    session: &Rc<RefCell<SessionState>>,
    message: &Value,
) -> WorkerReply
where
    H: McpHost + 'static,
{
    let Value::Array(items) = message else {
        return process_message(host, session, message).await;
    };
    if items.is_empty() {
        return WorkerReply::Message(serve::error_message(
            Value::Null,
            -32600,
            "Invalid Request",
            None,
        ));
    }
    let mut replies = Vec::new();
    for item in items {
        if !item.is_object() {
            replies.push(serve::error_message(
                Value::Null,
                -32600,
                "Invalid Request",
                None,
            ));
            continue;
        }
        match process_message(host, session, item).await {
            WorkerReply::None => {}
            WorkerReply::Message(value) => replies.push(value),
            WorkerReply::Stream(_) => {
                return WorkerReply::Message(serve::error_message(
                    Value::Null,
                    -32600,
                    "Streaming is not available in a JSON-RPC batch",
                    None,
                ));
            }
        }
    }
    WorkerReply::Message(Value::Array(replies))
}

async fn process_message<H>(
    host: &Rc<H>,
    session: &Rc<RefCell<SessionState>>,
    message: &Value,
) -> WorkerReply
where
    H: McpHost + 'static,
{
    if message.get("id").is_none() {
        session.borrow_mut().handle_notification(message);
        return WorkerReply::None;
    }
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    if !serve::valid_id(&id) {
        return WorkerReply::Message(serve::error_message(
            Value::Null,
            -32600,
            "Invalid Request",
            None,
        ));
    }
    let method = message.get("method").and_then(Value::as_str);
    if method == Some("initialize") {
        session.borrow_mut().reset_for_initialize();
    }
    let initialized = session.borrow().initialized();
    match serve::dispatch(host.as_ref(), message, initialized).await {
        Action::Reply(result) => {
            if method == Some("initialize") {
                session.borrow_mut().mark_initialized();
            }
            WorkerReply::Message(serve::ok_message(id, result))
        }
        Action::Fail {
            code,
            message: text,
            data,
        } => WorkerReply::Message(serve::error_message(id, code, text, data)),
        Action::Stream(prepared) => {
            let (sender, receiver) = mpsc::unbounded_channel();
            let writer = Arc::new(Mutex::new(LineWriter::new(sender)));
            session.borrow_mut().spawn_stream(
                serve::StreamRequest {
                    prepared,
                    id,
                    writer,
                },
                host,
            );
            WorkerReply::Stream(receiver)
        }
    }
}

/// Turns newline-delimited JSON-RPC output into individual SSE data frames.
struct LineWriter {
    sender: mpsc::UnboundedSender<String>,
    buffer: Vec<u8>,
    failed: bool,
}

impl LineWriter {
    fn new(sender: mpsc::UnboundedSender<String>) -> Self {
        Self {
            sender,
            buffer: Vec::new(),
            failed: false,
        }
    }

    fn drain_lines(&mut self) {
        while !self.failed {
            let Some(position) = self.buffer.iter().position(|byte| *byte == b'\n') else {
                if self.buffer.len() > MAX_BODY {
                    self.failed = true;
                }
                return;
            };
            let mut line: Vec<u8> = self.buffer.drain(..=position).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.is_empty() {
                continue;
            }
            let Ok(text) = String::from_utf8(line) else {
                self.failed = true;
                return;
            };
            if self.sender.send(text).is_err() {
                self.failed = true;
            }
        }
    }
}

impl AsyncWrite for LineWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.failed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "MCP event stream closed",
            )));
        }
        self.buffer.extend_from_slice(bytes);
        self.drain_lines();
        if self.failed {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "MCP event stream closed",
            )))
        } else {
            Poll::Ready(Ok(bytes.len()))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn shutdown_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("Zeroshot MCP: could not listen for Ctrl-C: {error}");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => eprintln!("Zeroshot MCP: could not listen for SIGTERM: {error}"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}
