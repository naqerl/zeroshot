use std::ffi::OsString;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, LocalSet};

use super::super::{NativeV2CliCommand, NativeV2CliError, RunWatchCommand};

/// JSON-RPC/tool failure code for a command that exceeded the MCP command budget.
pub(super) const TIMEOUT_CODE: &str = "mcp.timeout";

/// Failure carried into an MCP tool result or an events JSON-RPC error.
///
/// `code` and `message` are the CLI diagnostic fields (`zeroshot.error/v1`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpFailure {
    pub code: String,
    pub message: String,
}

impl McpFailure {
    #[must_use]
    pub fn from_cli(error: &NativeV2CliError) -> Self {
        let value = serde_json::to_value(error.diagnostic()).unwrap_or(Value::Null);
        let fallback = error.to_string();
        Self {
            code: value
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("target.unavailable")
                .to_owned(),
            message: value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or(&fallback)
                .to_owned(),
        }
    }

    #[must_use]
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::from_cli(&NativeV2CliError::Usage(message.into()))
    }

    #[must_use]
    pub fn timeout(timeout: Duration) -> Self {
        Self {
            code: TIMEOUT_CODE.to_owned(),
            message: format!(
                "the MCP command exceeded its {} ms budget",
                timeout.as_millis()
            ),
        }
    }
}

/// One durable watch subscription, forwarded as JSON objects.
///
/// Dropping the stream aborts the forwarder, which drops the watch subscription. A stream
/// acquired through `OffloadedHost` also stops the runtime thread that owns its forwarder.
pub struct WatchStream {
    receiver: mpsc::Receiver<Result<Value, McpFailure>>,
    task: JoinHandle<()>,
    stop: Option<oneshot::Sender<()>>,
}

impl WatchStream {
    #[must_use]
    pub fn new(receiver: mpsc::Receiver<Result<Value, McpFailure>>, task: JoinHandle<()>) -> Self {
        Self {
            receiver,
            task,
            stop: None,
        }
    }

    #[must_use]
    pub fn with_stop(mut self, stop: oneshot::Sender<()>) -> Self {
        self.stop = Some(stop);
        self
    }

    pub async fn recv(&mut self) -> Option<Result<Value, McpFailure>> {
        self.receiver.recv().await
    }
}

impl Drop for WatchStream {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// Runs CLI command values and opens the watch stream those commands already use.
///
/// Futures are not `Send`. They await the CLI executor, which holds non-`Send` state.
#[async_trait::async_trait(?Send)]
pub trait McpHost: Send + Sync {
    async fn execute(&self, command: NativeV2CliCommand) -> Result<String, McpFailure>;

    async fn open_watch(&self, command: &RunWatchCommand) -> Result<WatchStream, McpFailure>;
}

pub(super) fn parse_cli(argv: Vec<String>) -> Result<NativeV2CliCommand, McpFailure> {
    let command = super::super::parse_native_v2_args(argv.into_iter().map(OsString::from))
        .map_err(|error| McpFailure::from_cli(&error))?;
    match command {
        NativeV2CliCommand::Help(text) => Err(McpFailure::invalid(text)),
        NativeV2CliCommand::Version => Err(McpFailure::invalid("version is not a tool result")),
        command => Ok(command),
    }
}

/// Runs each non-`Send` host operation on its own current-thread runtime.
///
/// The HTTP worker must never poll a CLI future itself: a command can wait for a run lifecycle
/// transition for minutes, and on a single-threaded worker that would stop every later request
/// from being dispatched. Offloading also gives every operation a bounded budget, and dropping
/// the returned future cancels the command it was serving.
pub(super) struct OffloadedHost<H> {
    inner: Arc<H>,
    timeout: Duration,
}

impl<H> OffloadedHost<H>
where
    H: McpHost + 'static,
{
    pub(super) fn new(inner: H, timeout: Duration) -> Self {
        Self {
            inner: Arc::new(inner),
            timeout,
        }
    }
}

struct CancelOnDrop {
    sender: Option<oneshot::Sender<()>>,
}

impl CancelOnDrop {
    fn new(sender: oneshot::Sender<()>) -> Self {
        Self {
            sender: Some(sender),
        }
    }

    fn disarm(&mut self) {
        self.sender = None;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, McpFailure> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| McpFailure::invalid(format!("the MCP command runtime failed: {error}")))
}

#[async_trait::async_trait(?Send)]
impl<H> McpHost for OffloadedHost<H>
where
    H: McpHost + 'static,
{
    async fn execute(&self, command: NativeV2CliCommand) -> Result<String, McpFailure> {
        let inner = Arc::clone(&self.inner);
        let (result, result_rx) = oneshot::channel();
        let (cancel, cancelled) = oneshot::channel();
        std::thread::Builder::new()
            .name("zeroshot-mcp-command".to_owned())
            .spawn(move || {
                let outcome = runtime().and_then(|runtime| {
                    let local = LocalSet::new();
                    runtime.block_on(local.run_until(async move {
                        tokio::select! {
                            result = inner.execute(command) => result,
                            _ = cancelled => Err(McpFailure::invalid("the MCP command was cancelled")),
                        }
                    }))
                });
                let _ = result.send(outcome);
            })
            .map_err(|error| {
                McpFailure::invalid(format!("the MCP command worker could not start: {error}"))
            })?;
        let mut guard = CancelOnDrop::new(cancel);
        match tokio::time::timeout(self.timeout, result_rx).await {
            Ok(Ok(outcome)) => {
                guard.disarm();
                outcome
            }
            Ok(Err(_)) => Err(McpFailure::invalid("the MCP command worker stopped")),
            Err(_) => Err(McpFailure::timeout(self.timeout)),
        }
    }

    async fn open_watch(&self, command: &RunWatchCommand) -> Result<WatchStream, McpFailure> {
        let inner = Arc::clone(&self.inner);
        let command = command.clone();
        let (result, result_rx) = oneshot::channel();
        let (cancel, cancelled) = oneshot::channel();
        let (stop, stopped) = oneshot::channel();
        std::thread::Builder::new()
            .name("zeroshot-mcp-watch".to_owned())
            .spawn(move || {
                let runtime = match runtime() {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = result.send(Err(error));
                        return;
                    }
                };
                let local = LocalSet::new();
                let _ = runtime.block_on(local.run_until(async move {
                    let stream = tokio::select! {
                        result = inner.open_watch(&command) => result,
                        _ = cancelled => return,
                    };
                    match stream {
                        Err(error) => {
                            let _ = result.send(Err(error));
                        }
                        Ok(stream) => {
                            if result.send(Ok(stream.with_stop(stop))).is_err() {
                                return;
                            }
                            let _ = stopped.await;
                        }
                    }
                }));
            })
            .map_err(|error| {
                McpFailure::invalid(format!("the MCP watch worker could not start: {error}"))
            })?;
        let mut guard = CancelOnDrop::new(cancel);
        match tokio::time::timeout(self.timeout, result_rx).await {
            Ok(Ok(Ok(stream))) => {
                guard.disarm();
                Ok(stream)
            }
            Ok(Ok(Err(error))) => {
                guard.disarm();
                Err(error)
            }
            Ok(Err(_)) => Err(McpFailure::invalid("the MCP watch worker stopped")),
            Err(_) => Err(McpFailure::timeout(self.timeout)),
        }
    }
}
