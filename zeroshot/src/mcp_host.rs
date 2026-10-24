//! Process host for `zeroshot mcp serve`.
//!
//! Tools run through the same public and hub executors as the CLI. Events read the same
//! `run_watch` subscription `zeroshot watch` opens.

use std::time::Duration;

use async_trait::async_trait;
use openengine_cluster_protocol::{RunWatchParams, SubscriptionCloseReason};
use serde_json::Value;
use tokio::sync::mpsc;
use zeroshot_engine::native_v2_cli::local::LocalCliBackend;
use zeroshot_engine::native_v2_cli::mcp::{McpFailure, McpHost, WatchStream, serve as serve_session};
use zeroshot_engine::native_v2_cli::oecp::NamedTargetCliBackend;
use zeroshot_engine::native_v2_cli::{
    CliRunWatchEventNotification, CliSubscription, CliSubscriptionItem, NativeV2CliBackend,
    NativeV2CliCommand, NativeV2CliError, RunWatchCommand,
};

use crate::native_v2_hub::HubError;
use crate::native_v2_target::{
    FileTargetRegistry, NativeV2TargetConnector, TargetConnectorError, TargetHttpControlAuthority,
    TargetOecpWebSocketDialer, default_target_registry_path,
};

pub(super) async fn serve() -> Result<(), super::ProcessError> {
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    // The watch pump is not Send. A LocalSet runs it beside this stdio loop.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(serve_session(ProductionHost, stdin, stdout))
        .await
        .map_err(super::ProcessError::from)
}

struct ProductionHost;

#[async_trait(?Send)]
impl McpHost for ProductionHost {
    async fn execute(&self, command: NativeV2CliCommand) -> Result<String, McpFailure> {
        match command {
            NativeV2CliCommand::Hub(request) => execute_hub_command(request).await,
            command => execute_cli_command(command).await,
        }
    }

    async fn open_watch(&self, command: &RunWatchCommand) -> Result<WatchStream, McpFailure> {
        let params = RunWatchParams {
            run_id: command.run.run_id.clone(),
            from_cursor: command.after.clone(),
        };
        let (sender, receiver) = mpsc::channel(64);
        // Backend watch futures are not Send. `serve` polls this task on its LocalSet.
        let task = if command.run.target.is_none() {
            let backend =
                LocalCliBackend::production().map_err(|error| McpFailure::from_cli(&error))?;
            let subscription = backend
                .run_watch(None, params.clone())
                .await
                .map_err(|error| McpFailure::from_cli(&error))?;
            tokio::task::spawn_local(
                Pump {
                    backend,
                    target: None,
                    params,
                    sender,
                }
                .run(subscription),
            )
        } else {
            let backend = named_backend()?;
            let target = command.run.target.clone();
            let subscription = backend
                .run_watch(target.as_deref(), params.clone())
                .await
                .map_err(|error| McpFailure::from_cli(&error))?;
            tokio::task::spawn_local(
                Pump {
                    backend,
                    target,
                    params,
                    sender,
                }
                .run(subscription),
            )
        };
        Ok(WatchStream::new(receiver, task))
    }
}

fn named_backend() -> Result<
    NamedTargetCliBackend<
        NativeV2TargetConnector<
            FileTargetRegistry,
            TargetHttpControlAuthority,
            TargetOecpWebSocketDialer,
        >,
    >,
    McpFailure,
> {
    let registry = FileTargetRegistry::new(
        default_target_registry_path().map_err(|error| connector_failure(&error))?,
    );
    let authority = TargetHttpControlAuthority::production()
        .map_err(|error| connector_failure(&TargetConnectorError::Authority(error)))?;
    let connector = NativeV2TargetConnector::new(registry, authority, TargetOecpWebSocketDialer);
    Ok(NamedTargetCliBackend::new(connector))
}

fn connector_failure(error: &TargetConnectorError) -> McpFailure {
    McpFailure::from_cli(&NativeV2CliError::Target(error.to_string()))
}

async fn execute_cli_command(command: NativeV2CliCommand) -> Result<String, McpFailure> {
    let mut output = Vec::new();
    super::execute_public_command(command, &mut output)
        .await
        .map_err(|error| failure_from_process(&error))?;
    utf8_output(output)
}

async fn execute_hub_command(
    request: zeroshot_engine::native_v2_cli::hub::HubRequest,
) -> Result<String, McpFailure> {
    let mut output = Vec::new();
    match crate::native_v2_hub::execute_hub(request, &mut output).await {
        Ok(0) => utf8_output(output),
        Ok(code) => Err(hub_exit(code, &output)),
        Err(error) => Err(failure_from_hub(&error)),
    }
}

fn failure_from_process(error: &super::ProcessError) -> McpFailure {
    match error {
        super::ProcessError::Cli(error) => McpFailure::from_cli(error),
        super::ProcessError::Hub(error) => failure_from_hub(error),
        other => McpFailure::from_cli(&NativeV2CliError::Target(other.to_string())),
    }
}

fn failure_from_hub(error: &HubError) -> McpFailure {
    let value = error.diagnostic();
    let fallback = error.to_string();
    McpFailure {
        code: value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("hub.exit")
            .to_owned(),
        message: value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or(&fallback)
            .to_owned(),
    }
}

fn hub_exit(code: i32, output: &[u8]) -> McpFailure {
    if let Ok(text) = std::str::from_utf8(output) {
        if let Some(failure) = diagnostic_text(text) {
            return failure;
        }
    }
    McpFailure {
        code: "hub.exit".to_owned(),
        message: format!("hub command exited {code}"),
    }
}

fn diagnostic_text(text: &str) -> Option<McpFailure> {
    let value: Value = serde_json::from_str(text.trim()).ok()?;
    let code = value.get("code")?.as_str()?.to_owned();
    let message = value.get("message")?.as_str()?.to_owned();
    if code.is_empty() || message.is_empty() {
        return None;
    }
    Some(McpFailure { code, message })
}

fn utf8_output(output: Vec<u8>) -> Result<String, McpFailure> {
    String::from_utf8(output).map_err(|error| {
        McpFailure::from_cli(&NativeV2CliError::Protocol(format!(
            "command output is not UTF-8: {error}"
        )))
    })
}

struct Pump<B: NativeV2CliBackend> {
    backend: B,
    target: Option<String>,
    params: RunWatchParams,
    sender: mpsc::Sender<Result<Value, McpFailure>>,
}

impl<B> Pump<B>
where
    B: NativeV2CliBackend + 'static,
    B::Watch: 'static,
{
    async fn run(mut self, mut subscription: B::Watch) {
        loop {
            match forward(&mut subscription, &mut self.params, &self.sender).await {
                Forward::Stop => return,
                Forward::Reopen => {}
            }
            if self.sender.is_closed() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            if self.sender.is_closed() {
                return;
            }
            match self
                .backend
                .run_watch(self.target.as_deref(), self.params.clone())
                .await
            {
                Ok(next) => subscription = next,
                Err(error) if retryable(&error) => continue,
                Err(error) => {
                    let _ = self.sender.send(Err(McpFailure::from_cli(&error))).await;
                    return;
                }
            }
        }
    }
}

enum Forward {
    Reopen,
    Stop,
}

async fn forward<S>(
    subscription: &mut S,
    params: &mut RunWatchParams,
    sender: &mpsc::Sender<Result<Value, McpFailure>>,
) -> Forward
where
    S: CliSubscription<CliRunWatchEventNotification>,
{
    loop {
        if sender.is_closed() {
            return Forward::Stop;
        }
        match subscription.next().await {
            Ok(Some(CliSubscriptionItem::Event(event))) => {
                if params.from_cursor.as_ref() == Some(&event.cursor) {
                    continue;
                }
                params.from_cursor = Some(event.cursor.clone());
                let value = match serde_json::to_value(&event) {
                    Ok(value) => value,
                    Err(error) => {
                        let failure = McpFailure::from_cli(&NativeV2CliError::OutputJson(error));
                        let _ = sender.send(Err(failure)).await;
                        return Forward::Stop;
                    }
                };
                if sender.send(Ok(value)).await.is_err() {
                    return Forward::Stop;
                }
            }
            Ok(Some(CliSubscriptionItem::Closed {
                reason: SubscriptionCloseReason::Done,
            })) => return Forward::Stop,
            Ok(Some(CliSubscriptionItem::Closed {
                reason: SubscriptionCloseReason::SlowConsumer,
            }))
            | Ok(None) => return Forward::Reopen,
            Ok(Some(CliSubscriptionItem::Closed {
                reason: SubscriptionCloseReason::SourceUnavailable,
            })) => {
                let failure = McpFailure::from_cli(&NativeV2CliError::Protocol(
                    "observation source is unavailable; the stream is incomplete".to_owned(),
                ));
                let _ = sender.send(Err(failure)).await;
                return Forward::Stop;
            }
            Err(error) if retryable(&error) => return Forward::Reopen,
            Err(error) => {
                let _ = sender.send(Err(McpFailure::from_cli(&error))).await;
                return Forward::Stop;
            }
        }
    }
}

fn retryable(error: &NativeV2CliError) -> bool {
    matches!(
        error,
        NativeV2CliError::Disconnected | NativeV2CliError::TargetTransport { .. }
    )
}
