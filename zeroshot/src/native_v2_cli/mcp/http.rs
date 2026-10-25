use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use futures_util::stream;
use serde_json::Value;
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::LocalSet;

use super::host::McpHost;
use super::serve::{self, Action, SessionState};

const MAX_BODY: usize = 16 * 1024 * 1024;
const DEFAULT_SESSION: &str = "default";
const SESSION_HEADER: HeaderName = HeaderName::from_static("mcp-session-id");

struct Job {
    session: String,
    message: Value,
    reply: oneshot::Sender<WorkerReply>,
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
                &session,
            );
        }
    };
    if let Value::Array(items) = message {
        if items.is_empty() {
            return json_response(
                serve::error_message(Value::Null, -32600, "Invalid Request", None),
                &session,
            );
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
            let reply = match request_reply(&state, &session, item).await {
                Ok(reply) => reply,
                Err(()) => {
                    return json_response(
                        serve::error_message(Value::Null, -32603, "Internal error", None),
                        &session,
                    );
                }
            };
            match reply {
                WorkerReply::None => {}
                WorkerReply::Message(value) => replies.push(value),
                WorkerReply::Stream(_) => {
                    return json_response(
                        serve::error_message(
                            Value::Null,
                            -32600,
                            "Streaming is not available in a JSON-RPC batch",
                            None,
                        ),
                        &session,
                    );
                }
            }
        }
        return json_response(Value::Array(replies), &session);
    }
    let reply = match request_reply(&state, &session, message).await {
        Ok(reply) => reply,
        Err(()) => {
            return json_response(
                serve::error_message(Value::Null, -32603, "Internal error", None),
                &session,
            );
        }
    };
    match reply {
        WorkerReply::None => accepted_response(&session),
        WorkerReply::Message(value) => json_response(value, &session),
        WorkerReply::Stream(receiver) => sse_response(receiver, &session),
    }
}

async fn request_reply(
    state: &HttpState,
    session: &str,
    message: Value,
) -> Result<WorkerReply, ()> {
    let (reply, response) = oneshot::channel();
    state
        .jobs
        .send(Job {
            session: session.to_owned(),
            message,
            reply,
        })
        .map_err(|_| ())?;
    response.await.map_err(|_| ())
}

fn json_response(value: Value, session: &str) -> Response {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    set_session_header(&mut response, session);
    response
}

fn accepted_response(session: &str) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::ACCEPTED;
    set_session_header(&mut response, session);
    response
}

fn sse_response(receiver: mpsc::UnboundedReceiver<String>, session: &str) -> Response {
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

fn set_session_header(response: &mut Response, session: &str) {
    if let Ok(value) = HeaderValue::from_str(session) {
        response.headers_mut().insert(SESSION_HEADER, value);
    }
}

fn session_id(headers: &HeaderMap) -> String {
    let Some(value) = headers
        .get(&SESSION_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return DEFAULT_SESSION.to_owned();
    };
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return DEFAULT_SESSION.to_owned();
    }
    value.to_owned()
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
                            session.borrow_mut().shutdown();
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

type Sessions = Rc<RefCell<HashMap<String, Rc<RefCell<SessionState>>>>>;

async fn process_job<H>(host: &Rc<H>, sessions: &Sessions, job: Job)
where
    H: McpHost + 'static,
{
    let session = {
        let mut sessions = sessions.borrow_mut();
        Rc::clone(
            sessions
                .entry(job.session)
                .or_insert_with(|| Rc::new(RefCell::new(SessionState::new()))),
        )
    };
    let reply = process_message(host.as_ref(), &session, &job.message).await;
    let _ = job.reply.send(reply);
}

async fn process_message<H>(
    host: &H,
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
    let initialized = session.borrow().initialized();
    match serve::dispatch(host, message, initialized).await {
        Action::Reply(result) => {
            if message.get("method").and_then(Value::as_str) == Some("initialize") {
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
            session.borrow_mut().spawn_stream(prepared, id, &writer);
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
