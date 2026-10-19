//! The hub daemon: detection of the supervising service, the guard every hub command passes,
//! and the foreground dispatch loop the services run.

use std::io::Write;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::process::Command;
use zeroshot_engine::native_v2_cli::hub::HubCommand;

use super::clock::{now_ms, parse_duration_ms, rfc3339};
use super::error::HubError;
use super::schedule::{self, TickReport};
use super::{HubContext, write_json};

/// systemd unit and Docker container name of the installed daemon.
pub(crate) const SERVICE_NAME: &str = "zeroshot-hub";
/// Set by installers so the daemon reports who supervises it.
pub(crate) const MANAGER_ENV: &str = "ZEROSHOT_HUB_DAEMON_MANAGER";
/// Test and demo hook: a comma-separated list of managers to report as active
/// (`systemd`, `docker`), or `none`, instead of asking systemctl and docker.
pub(crate) const PROBE_ENV: &str = "ZEROSHOT_HUB_DAEMON_PROBE";
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// What one service manager reports about the daemon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManagerState {
    pub active: bool,
    /// `user` or `system` for systemd units.
    pub scope: Option<&'static str>,
    pub detail: String,
}

impl ManagerState {
    fn view(&self) -> Value {
        json!({ "active": self.active, "scope": self.scope, "detail": self.detail })
    }
}

/// Asks systemd and Docker whether they run the hub daemon. Injectable for tests.
#[async_trait]
pub(crate) trait DaemonProbe: Send + Sync {
    async fn systemd(&self) -> ManagerState;
    async fn docker(&self) -> ManagerState;
}

/// Both managers' answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Detection {
    pub systemd: ManagerState,
    pub docker: ManagerState,
}

impl Detection {
    pub(crate) async fn probe(probe: &dyn DaemonProbe) -> Self {
        let (systemd, docker) = tokio::join!(probe.systemd(), probe.docker());
        Self { systemd, docker }
    }

    pub(crate) fn view(&self) -> Value {
        json!({ "systemd": self.systemd.view(), "docker": self.docker.view() })
    }

    /// The single active manager, or the guard error.
    pub(crate) fn verdict(&self) -> Result<&'static str, HubError> {
        match (self.systemd.active, self.docker.active) {
            (true, false) => Ok("systemd"),
            (false, true) => Ok("docker"),
            (false, false) => Err(HubError::DaemonNotRunning {
                message: format!(
                    "no hub daemon is running (checked systemd unit {SERVICE_NAME}.service and \
                     Docker container {SERVICE_NAME}); start one with `zeroshot hub install \
                     --systemd` or `zeroshot hub install --docker`"
                ),
                detection: self.view(),
            }),
            (true, true) => Err(HubError::DaemonConflict {
                message: "hub daemons are active under both systemd and Docker; keep one with \
                          `zeroshot hub uninstall --docker` or `zeroshot hub uninstall --systemd`"
                    .to_owned(),
                detection: self.view(),
            }),
        }
    }
}

/// Commands that run without an active daemon: the daemon itself and its service management.
pub(crate) const fn requires_daemon(command: &HubCommand) -> bool {
    !matches!(
        command,
        HubCommand::Daemon { .. }
            | HubCommand::DaemonStatus
            | HubCommand::Install(_)
            | HubCommand::Uninstall(_)
    )
}

/// Fail unless exactly one daemon is active under systemd or Docker.
pub(crate) async fn guard(probe: &dyn DaemonProbe) -> Result<&'static str, HubError> {
    Detection::probe(probe).await.verdict()
}

/// The probe for this process: `ZEROSHOT_HUB_DAEMON_PROBE` when set, otherwise systemctl and
/// docker.
pub(crate) fn production_probe() -> Box<dyn DaemonProbe> {
    match std::env::var(PROBE_ENV) {
        Ok(value) if !value.is_empty() => Box::new(StaticProbe::parse(&value)),
        _ => Box::new(ServiceProbe),
    }
}

/// Fixed answers, from `ZEROSHOT_HUB_DAEMON_PROBE` or tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct StaticProbe {
    pub systemd: bool,
    pub docker: bool,
}

impl StaticProbe {
    pub(crate) fn parse(value: &str) -> Self {
        let managers = value.split(',').map(str::trim).collect::<Vec<_>>();
        Self {
            systemd: managers.contains(&"systemd"),
            docker: managers.contains(&"docker"),
        }
    }

    fn state(active: bool) -> ManagerState {
        ManagerState {
            active,
            scope: None,
            detail: format!("reported by {PROBE_ENV}"),
        }
    }
}

#[async_trait]
impl DaemonProbe for StaticProbe {
    async fn systemd(&self) -> ManagerState {
        Self::state(self.systemd)
    }

    async fn docker(&self) -> ManagerState {
        Self::state(self.docker)
    }
}

/// Asks `systemctl` (user, then system scope) and `docker inspect`.
struct ServiceProbe;

#[async_trait]
impl DaemonProbe for ServiceProbe {
    async fn systemd(&self) -> ManagerState {
        let unit = format!("{SERVICE_NAME}.service");
        let mut details = Vec::new();
        for (scope, arguments) in [
            ("user", vec!["--user", "is-active", unit.as_str()]),
            ("system", vec!["is-active", unit.as_str()]),
        ] {
            match probe_command("systemctl", &arguments).await {
                Ok(state) if state == "active" => {
                    return ManagerState {
                        active: true,
                        scope: Some(scope),
                        detail: format!("{scope} unit {unit} is active"),
                    };
                }
                Ok(state) => details.push(format!(
                    "{scope} unit {}",
                    if state.is_empty() { "unknown" } else { &state }
                )),
                Err(error) => details.push(format!("{scope}: {error}")),
            }
        }
        ManagerState {
            active: false,
            scope: None,
            detail: details.join("; "),
        }
    }

    async fn docker(&self) -> ManagerState {
        let result = probe_command(
            "docker",
            &["inspect", "--format", "{{.State.Running}}", SERVICE_NAME],
        )
        .await;
        match result {
            Ok(running) if running == "true" => ManagerState {
                active: true,
                scope: None,
                detail: format!("container {SERVICE_NAME} is running"),
            },
            Ok(running) if running == "false" => ManagerState {
                active: false,
                scope: None,
                detail: format!("container {SERVICE_NAME} is stopped"),
            },
            Ok(_) => ManagerState {
                active: false,
                scope: None,
                detail: format!("no container {SERVICE_NAME}"),
            },
            Err(error) => ManagerState {
                active: false,
                scope: None,
                detail: error,
            },
        }
    }
}

/// First stdout line of a short command, whatever its exit status; `Err` when it cannot run.
async fn probe_command(program: &str, arguments: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .output();
    match tokio::time::timeout(PROBE_TIMEOUT, output).await {
        Ok(Ok(output)) => Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_owned()),
        Ok(Err(error)) => Err(format!("{program} unavailable: {error}")),
        Err(_) => Err(format!("{program} timed out")),
    }
}

/// One dispatcher pass under the hub lock.
pub(crate) async fn pass(context: &HubContext) -> Result<(TickReport, Value), HubError> {
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    let now = now_ms();
    let report = schedule::tick(&mut state, context.driver.as_ref(), now).await;
    context.store.save(&lock, &state)?;
    let view = report.view(&state, now);
    Ok((report, view))
}

impl TickReport {
    pub(crate) fn is_idle(&self) -> bool {
        self.fired.is_empty()
            && self.dispatched.is_empty()
            && self.finished.is_empty()
            && self.expired.is_empty()
            && self.failed.is_empty()
    }
}

/// `hub daemon run`: dispatch every `interval` until SIGTERM or Ctrl-C.
pub(crate) async fn run(
    context: &HubContext,
    interval: &str,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let pause = Duration::from_millis(parse_duration_ms(interval)?);
    let Some(_instance) = context.store.try_daemon_lock()? else {
        return Err(HubError::DaemonConflict {
            message: format!(
                "another hub daemon already owns {}",
                context.store.directory().display()
            ),
            detection: Value::Null,
        });
    };
    let manager = std::env::var(MANAGER_ENV).unwrap_or_else(|_| "foreground".to_owned());
    let started = now_ms();
    let mut heartbeat = json!({
        "schema": "zeroshot.hub.daemon/v1",
        "pid": std::process::id(),
        "manager": manager,
        "version": env!("CARGO_PKG_VERSION"),
        "hubDirectory": context.store.directory(),
        "intervalMs": pause.as_millis(),
        "startedAt": rfc3339(started),
        "lastPassAt": null,
        "lastError": null,
        "passes": 0,
    });
    let mut event = heartbeat.clone();
    event["event"] = json!("started");
    write_json(output, &event)?;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut passes = 0_u64;
    loop {
        passes += 1;
        match pass(context).await {
            Ok((report, view)) => {
                heartbeat["lastError"] = Value::Null;
                if !report.is_idle() {
                    write_json(output, &view)?;
                }
            }
            Err(error) => {
                heartbeat["lastError"] = json!(error.to_string());
                write_json(output, &error.diagnostic())?;
            }
        }
        heartbeat["passes"] = json!(passes);
        heartbeat["lastPassAt"] = json!(rfc3339(now_ms()));
        context.store.write_heartbeat(&heartbeat)?;
        tokio::select! {
            () = tokio::time::sleep(pause) => {}
            () = &mut shutdown => break,
        }
    }
    heartbeat["stoppedAt"] = json!(rfc3339(now_ms()));
    context.store.write_heartbeat(&heartbeat)?;
    let mut event = heartbeat;
    event["event"] = json!("stopped");
    write_json(output, &event)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = terminate.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

/// `hub daemon status`: detection plus the last heartbeat. Exit 0, 5, or 6 like the guard.
pub(crate) async fn status(
    context: &HubContext,
    probe: &dyn DaemonProbe,
    output: &mut impl Write,
) -> Result<i32, HubError> {
    let detection = Detection::probe(probe).await;
    let verdict = detection.verdict();
    let heartbeat = context.store.heartbeat();
    let age = heartbeat
        .as_ref()
        .and_then(|heartbeat| heartbeat.get("lastPassAt"))
        .and_then(Value::as_str)
        .and_then(super::clock::parse_rfc3339_ms)
        .map(|at| now_ms().saturating_sub(at));
    write_json(
        output,
        &json!({
            "schema": "zeroshot.hub.daemon-status/v1",
            "active": verdict.as_ref().ok(),
            "error": verdict.as_ref().err().map(|error| error.diagnostic()["code"].clone()),
            "detection": detection.view(),
            "heartbeat": heartbeat,
            "heartbeatAgeMs": age,
        }),
    )?;
    Ok(verdict.map_or_else(|error| error.exit_code(), |_| 0))
}
