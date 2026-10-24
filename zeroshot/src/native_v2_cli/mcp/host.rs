use std::ffi::OsString;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::super::{NativeV2CliCommand, NativeV2CliError, RunWatchCommand};

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
        let value = serde_json::to_value(error.diagnostic()).unwrap_or_else(|_| Value::Null);
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
}

/// One durable watch subscription, forwarded as JSON objects.
///
/// Dropping the stream aborts the forwarder, which drops the watch subscription.
pub struct WatchStream {
    receiver: mpsc::Receiver<Result<Value, McpFailure>>,
    task: JoinHandle<()>,
}

impl WatchStream {
    #[must_use]
    pub fn new(receiver: mpsc::Receiver<Result<Value, McpFailure>>, task: JoinHandle<()>) -> Self {
        Self { receiver, task }
    }

    pub async fn recv(&mut self) -> Option<Result<Value, McpFailure>> {
        self.receiver.recv().await
    }
}

impl Drop for WatchStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Runs CLI command values and opens the watch stream those commands already use.
#[async_trait::async_trait]
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
