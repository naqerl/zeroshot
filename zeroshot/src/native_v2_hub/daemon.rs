//! `hub daemon run`: the single owner of a hub folder. It serves the hub API and runs a
//! dispatcher pass every interval until SIGTERM or Ctrl-C.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::HubDaemonRun;

use super::api::{ApiState, metrics_router, router};
use super::clock::{now_ms, parse_duration_ms, rfc3339};
use super::error::HubError;
use super::service::{HubService, write_private};
use super::store::HubStore;
use super::write_json;

/// systemd unit and Docker container name of the installed daemon.
pub(crate) const SERVICE_NAME: &str = "zeroshot-hub";
/// Set by installers so the daemon reports who supervises it.
pub(crate) const MANAGER_ENV: &str = "ZEROSHOT_HUB_DAEMON_MANAGER";
/// Default hub API address: loopback only.
pub(crate) const DEFAULT_LISTEN: &str = "127.0.0.1:7383";
/// Default bearer token file inside the hub folder.
pub(crate) const TOKEN_FILE: &str = "api-token";
/// How long shutdown waits for open API streams before closing them.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Read the API token, creating the file with a random 256-bit token when it is missing.
pub(crate) fn load_or_create_token(path: &Path) -> Result<String, HubError> {
    match std::fs::read_to_string(path) {
        Ok(token) if !token.trim().is_empty() => return Ok(token.trim().to_owned()),
        Ok(_) => {
            return Err(HubError::Usage(format!(
                "hub API token file {} is empty",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(HubError::storage(
                &format!("{} could not be read", path.display()),
                error,
            ));
        }
    }
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| HubError::storage("a random API token could not be generated", error))?;
    let token = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if let Some(parent) = path.parent() {
        super::service::create_private_directory(parent)?;
    }
    write_private(path, format!("{token}\n").as_bytes())?;
    Ok(token)
}

/// URL clients use for a bound address: unspecified addresses are reached through loopback.
pub(crate) fn client_url(address: std::net::SocketAddr) -> String {
    let ip = match address.ip() {
        std::net::IpAddr::V4(ip) if ip.is_unspecified() => {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        }
        std::net::IpAddr::V6(ip) if ip.is_unspecified() => {
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        }
        ip => ip,
    };
    format!("http://{}", std::net::SocketAddr::new(ip, address.port()))
}

/// `hub daemon run`.
pub(crate) async fn run(
    store: HubStore,
    open: impl FnOnce(HubStore) -> Result<HubService, HubError>,
    options: &HubDaemonRun,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let pause = Duration::from_millis(parse_duration_ms(&options.interval)?);
    let Some(_instance) = store.try_daemon_lock()? else {
        return Err(HubError::DaemonRunning(format!(
            "another hub daemon already owns {}; check it with `zeroshot hub daemon status`",
            store.directory().display()
        )));
    };
    let token_file = options
        .api
        .token_file
        .clone()
        .unwrap_or_else(|| store.directory().join(TOKEN_FILE));
    let token_file = absolute(token_file);
    let token = load_or_create_token(&token_file)?;
    let listen = options.api.listen.as_deref().unwrap_or(DEFAULT_LISTEN);
    let listener = bind(listen, "hub API").await?;
    let address = listener
        .local_addr()
        .map_err(|error| HubError::storage("hub API address is unavailable", error))?;
    let metrics_listener = match options.api.metrics_listen.as_deref() {
        Some(listen) => Some(bind_loopback_metrics(listen).await?),
        None => None,
    };
    let service = Arc::new(open(store)?.with_metric_labels(options.api.metrics_labels.clone()));
    let manager = std::env::var(MANAGER_ENV).unwrap_or_else(|_| "foreground".to_owned());
    let mut heartbeat = json!({
        "schema": "zeroshot.hub.daemon/v1",
        "pid": std::process::id(),
        "manager": manager,
        "version": env!("CARGO_PKG_VERSION"),
        "hubDirectory": service.store().directory(),
        "listen": address.to_string(),
        "url": client_url(address),
        "tokenFile": token_file,
        "metricsListen": metrics_listener
            .as_ref()
            .and_then(|listener| listener.local_addr().ok())
            .map(|address| address.to_string()),
        "intervalMs": pause.as_millis(),
        "startedAt": rfc3339(now_ms()),
        "lastPassAt": null,
        "lastError": null,
        "passes": 0,
    });
    let shared = Arc::new(Mutex::new(heartbeat.clone()));
    service.store().write_heartbeat(&heartbeat)?;
    let api = Arc::new(ApiState {
        service: service.clone(),
        token,
        heartbeat: shared.clone(),
    });
    let (stop, stopped) = tokio::sync::watch::channel(false);
    if let Some(metrics_listener) = metrics_listener {
        let mut stopped = stopped.clone();
        let metrics = metrics_router(api.clone());
        tokio::spawn(async move {
            axum::serve(metrics_listener, metrics)
                .with_graceful_shutdown(async move {
                    let _ = stopped.changed().await;
                })
                .await
        });
    }
    let server = tokio::spawn(async move {
        let mut stopped = stopped;
        axum::serve(listener, router(api))
            .with_graceful_shutdown(async move {
                let _ = stopped.changed().await;
            })
            .await
    });
    let mut event = heartbeat.clone();
    event["event"] = json!("started");
    write_json(output, &event)?;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut passes = 0_u64;
    loop {
        passes += 1;
        match service.pass().await {
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
        // Eval work (measuring finished samples, checks, judges) runs beside the dispatcher:
        // a slow check or judge never delays placement.
        match service.eval_claim().await {
            Ok(work) => {
                for item in work {
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) = service.eval_execute(item).await {
                            eprintln!("zeroshot hub eval: {error}");
                        }
                    });
                }
            }
            Err(error) => {
                heartbeat["lastError"] = json!(error.to_string());
                write_json(output, &error.diagnostic())?;
            }
        }
        heartbeat["passes"] = json!(passes);
        heartbeat["lastPassAt"] = json!(rfc3339(now_ms()));
        publish(&service, &shared, &heartbeat)?;
        tokio::select! {
            () = tokio::time::sleep(pause) => {}
            () = &mut shutdown => break,
        }
    }
    let _ = stop.send(true);
    let _ = tokio::time::timeout(SHUTDOWN_GRACE, server).await;
    heartbeat["stoppedAt"] = json!(rfc3339(now_ms()));
    publish(&service, &shared, &heartbeat)?;
    let mut event = heartbeat;
    event["event"] = json!("stopped");
    write_json(output, &event)
}

async fn bind(listen: &str, what: &str) -> Result<tokio::net::TcpListener, HubError> {
    tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| HubError::Usage(format!("{what} could not listen on {listen}: {error}")))
}

/// Bind the unauthenticated metrics listener, refusing any address that is not loopback:
/// remote scrapers use `/metrics` on the API address with the bearer token.
pub(crate) async fn bind_loopback_metrics(
    listen: &str,
) -> Result<tokio::net::TcpListener, HubError> {
    let listener = bind(listen, "metrics").await?;
    let loopback = listener
        .local_addr()
        .is_ok_and(|address| address.ip().is_loopback());
    if !loopback {
        return Err(HubError::Usage(format!(
            "--metrics-listen {listen} is not a loopback address; it serves /metrics without a \
             token, so remote Prometheus servers should scrape /metrics on the API address with \
             the bearer token instead"
        )));
    }
    Ok(listener)
}

fn publish(service: &HubService, shared: &Mutex<Value>, heartbeat: &Value) -> Result<(), HubError> {
    if let Ok(mut current) = shared.lock() {
        current.clone_from(heartbeat);
    }
    service.store().write_heartbeat(heartbeat)
}

fn absolute(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    std::env::current_dir().map_or(path.clone(), |folder| folder.join(path))
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

impl super::schedule::TickReport {
    pub(crate) fn is_idle(&self) -> bool {
        self.fired.is_empty()
            && self.dispatched.is_empty()
            && self.finished.is_empty()
            && self.expired.is_empty()
            && self.failed.is_empty()
    }
}
