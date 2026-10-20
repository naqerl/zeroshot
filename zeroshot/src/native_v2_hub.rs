//! `zeroshot hub`: a pool of registered targets with placement, a durable queue, recurring
//! schedules, and usage accounting.
//!
//! One daemon per hub folder (`hub daemon run`, supervised by systemd or Docker through
//! `hub install`) owns the hub: it is the only writer of `state.json`, dispatches continuously,
//! and serves the hub over an HTTP API with bearer-token auth. Every other hub command is a
//! client of the hub selected by `--hub`, `ZEROSHOT_HUB`, or the current hub context, so one
//! machine can drive a hub that runs on another. The hub never changes run contracts: it
//! chooses one member per job and delegates every target operation to the same CLI commands a
//! user would run (`run --detach`, `list`, `status`, `watch`, `logs`, `attach`, `force-stop`),
//! so registry, authentication, admission, and observation behave exactly as without the hub.

use std::io::Write;
use std::sync::Arc;

use serde_json::Value;
use zeroshot_engine::native_v2_cli::hub::{HubCommand, HubRequest};

use crate::native_v2_target::{FileTargetRegistry, default_target_registry_path};

mod api;
mod client;
mod clock;
mod context;
mod cron;
mod daemon;
mod driver;
mod error;
mod install;
mod jobs;
mod metrics;
mod model;
mod placement;
mod schedule;
mod service;
mod stats;
mod store;
mod wire;

#[cfg(test)]
#[path = "native_v2_hub/tests.rs"]
mod tests;

pub(crate) use error::HubError;
use store::HubStore;

/// Execute one hub command. Returns the process exit code for successful commands; a
/// proxied observation command reports its remote child's exit code.
pub(crate) async fn execute_hub(
    request: HubRequest,
    output: &mut impl Write,
) -> Result<i32, HubError> {
    let registry_path = default_target_registry_path()
        .map_err(|error| HubError::storage("target registry path is unavailable", error))?;
    let store = HubStore::new(HubStore::default_directory(&registry_path));
    let config_dir = registry_path.parent().map_or_else(
        || std::path::PathBuf::from("."),
        std::path::Path::to_path_buf,
    );
    match request.command {
        HubCommand::Daemon(options) => {
            let open = |store| {
                let driver = driver::ProcessDriver::production(registry_path.clone())?;
                service::HubService::open(
                    store,
                    Arc::new(driver),
                    FileTargetRegistry::new(registry_path.clone()),
                )
            };
            daemon::run(store, open, &options, output).await.map(|()| 0)
        }
        HubCommand::Install(install) => {
            let env = install::InstallEnv::detect(&registry_path, store.directory())?;
            install::install(&env, install, &store, output)
                .await
                .map(|()| 0)
        }
        HubCommand::Uninstall(uninstall) => {
            let env = install::InstallEnv::detect(&registry_path, store.directory())?;
            install::uninstall(&env, uninstall, output)
                .await
                .map(|()| 0)
        }
        HubCommand::Context(command) => context::Contexts::new(&config_dir, store)
            .execute(request.context.as_deref(), command, output)
            .map(|()| 0),
        command => {
            let resolved =
                context::Contexts::new(&config_dir, store).resolve(request.context.as_deref())?;
            let client = client::HubClient::new(&resolved)?;
            client::execute(&client, command, output).await
        }
    }
}

pub(crate) fn write_json(output: &mut impl Write, value: &Value) -> Result<(), HubError> {
    serde_json::to_writer(&mut *output, value)
        .map_err(std::io::Error::from)
        .and_then(|()| output.write_all(b"\n"))
        .and_then(|()| output.flush())
        .map_err(|error| HubError::storage("output could not be written", error))
}
