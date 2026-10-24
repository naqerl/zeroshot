//! `hub install` / `hub uninstall`: generate a systemd unit or a Docker container command that
//! runs `zeroshot hub daemon run`, then apply it step by step. Generation is pure so it is
//! tested without systemd or Docker; `--dry-run` prints the same plan.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use serde_json::{Value, json};
use tokio::process::Command;
use zeroshot_engine::native_v2_cli::hub::{HubApiOptions, HubInstall, HubManager, HubUninstall};

use super::daemon::{MANAGER_ENV, SERVICE_NAME};
use super::error::HubError;
use super::paths::{absolute, absolute_from};
use super::store::HubStore;
use super::write_json;

/// Image used by `--docker` without `--image`.
pub(crate) const DEFAULT_IMAGE: &str = "ghcr.io/the-open-engine/zeroshot-target:latest";

/// Host facts the generated service pins, so the daemon sees this shell's targets and queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InstallEnv {
    pub executable: PathBuf,
    pub home: PathBuf,
    pub user: String,
    pub uid: u32,
    pub gid: u32,
    /// `PATH` for harnesses and git started by local runs.
    pub path: String,
    pub config_dir: PathBuf,
    pub hub_dir: PathBuf,
    pub state_dir: PathBuf,
    pub user_unit_dir: PathBuf,
    pub credential_store: Option<String>,
}

/// `hub daemon run` arguments the service starts with: the interval plus the API options
/// given to `hub install` (--listen, --token-file, --metrics-listen, --metrics-label,
/// --unconfirmed-limit).
pub(crate) fn daemon_arguments(interval: &str, api: &HubApiOptions) -> Vec<String> {
    let mut arguments = ["hub", "daemon", "run", "--interval", interval]
        .map(str::to_owned)
        .to_vec();
    if let Some(listen) = &api.listen {
        arguments.extend(["--listen".to_owned(), listen.clone()]);
    }
    if let Some(token_file) = &api.token_file {
        arguments.extend(["--token-file".to_owned(), display(token_file)]);
    }
    if let Some(listen) = &api.metrics_listen {
        arguments.extend(["--metrics-listen".to_owned(), listen.clone()]);
    }
    for key in &api.metrics_labels {
        arguments.extend(["--metrics-label".to_owned(), key.clone()]);
    }
    if let Some(limit) = api.unconfirmed_limit {
        arguments.extend(["--unconfirmed-limit".to_owned(), limit.to_string()]);
    }
    arguments
}

/// One install or uninstall step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    Write { path: PathBuf, content: String },
    Run { argv: Vec<String>, may_fail: bool },
    Remove { path: PathBuf },
}

impl Step {
    fn run(argv: &[&str]) -> Self {
        Self::Run {
            argv: argv.iter().map(|value| (*value).to_owned()).collect(),
            may_fail: false,
        }
    }

    fn run_may_fail(argv: &[&str]) -> Self {
        Self::Run {
            argv: argv.iter().map(|value| (*value).to_owned()).collect(),
            may_fail: true,
        }
    }

    fn view(&self) -> Value {
        match self {
            Self::Write { path, content } => json!({ "write": path, "content": content }),
            Self::Run { argv, may_fail } => json!({ "run": argv, "mayFail": may_fail }),
            Self::Remove { path } => json!({ "remove": path }),
        }
    }
}

/// Ordered steps plus notes for the operator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    pub manager: &'static str,
    pub steps: Vec<Step>,
    pub notes: Vec<String>,
}

impl Plan {
    fn manager_program(&self) -> &'static str {
        if self.manager == "docker" {
            "docker"
        } else {
            "systemctl"
        }
    }
}

impl InstallEnv {
    /// Facts of the invoking shell.
    pub(crate) fn detect(registry_path: &Path, hub_dir: &Path) -> Result<Self, HubError> {
        let unavailable = |what: &str| HubError::Usage(format!("{what} is unavailable"));
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| unavailable("HOME"))?;
        // A unit or container runs later from another folder, so every pinned folder is resolved
        // now: home shorthand against this shell's home, relative paths against this folder.
        let pin = |path: &Path| absolute_from(path, Some(&home), std::env::current_dir());
        let executable = std::env::current_exe()
            .and_then(std::fs::canonicalize)
            .map_err(|error| HubError::storage("executable path is unavailable", error))?;
        let config_dir = registry_path
            .parent()
            .map(pin)
            .ok_or_else(|| unavailable("the target registry folder"))??;
        let state_dir = std::env::var_os("ZEROSHOT_STATE_DIR")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("XDG_STATE_HOME")
                    .filter(|value| !value.is_empty())
                    .map(|root| PathBuf::from(root).join("zeroshot"))
            })
            .unwrap_or_else(|| home.join(".local").join("state").join("zeroshot"));
        let state_dir = pin(&state_dir)?;
        let user_unit_dir = std::env::var_os("XDG_CONFIG_HOME")
            .filter(|value| !value.is_empty())
            .map_or_else(|| home.join(".config"), PathBuf::from)
            .join("systemd")
            .join("user");
        let user_unit_dir = pin(&user_unit_dir)?;
        let hub_dir = pin(hub_dir)?;
        let (uid, gid) = ids();
        Ok(Self {
            executable,
            user: std::env::var("USER")
                .ok()
                .filter(|user| !user.is_empty())
                .unwrap_or_else(|| uid.to_string()),
            home,
            uid,
            gid,
            path: std::env::var("PATH").unwrap_or_else(|_| "/usr/local/bin:/usr/bin:/bin".into()),
            config_dir,
            hub_dir,
            state_dir,
            user_unit_dir,
            credential_store: std::env::var("ZEROSHOT_CREDENTIAL_STORE")
                .ok()
                .filter(|value| !value.is_empty()),
        })
    }

    pub(crate) fn pinned_environment(&self, manager: &str) -> Vec<(String, String)> {
        let mut environment = vec![
            (MANAGER_ENV.to_owned(), manager.to_owned()),
            ("HOME".to_owned(), display(&self.home)),
            ("ZEROSHOT_CONFIG_DIR".to_owned(), display(&self.config_dir)),
            ("ZEROSHOT_HUB_DIR".to_owned(), display(&self.hub_dir)),
            ("ZEROSHOT_STATE_DIR".to_owned(), display(&self.state_dir)),
        ];
        if let Some(store) = &self.credential_store {
            environment.push(("ZEROSHOT_CREDENTIAL_STORE".to_owned(), store.clone()));
        }
        environment
    }
}

#[cfg(unix)]
fn ids() -> (u32, u32) {
    // SAFETY: getuid and getgid have no preconditions and cannot fail.
    unsafe { (libc::getuid(), libc::getgid()) }
}

#[cfg(not(unix))]
const fn ids() -> (u32, u32) {
    (0, 0)
}

fn display(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn unit_name() -> String {
    format!("{SERVICE_NAME}.service")
}

fn unit_path(env: &InstallEnv, system: bool) -> PathBuf {
    if system {
        PathBuf::from("/etc/systemd/system").join(unit_name())
    } else {
        env.user_unit_dir.join(unit_name())
    }
}

/// Quote one systemd unit value: double quotes, escaped backslashes and quotes, `%` doubled.
pub(crate) fn systemd_quote(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%");
    format!("\"{escaped}\"")
}

/// The generated unit file.
pub(crate) fn systemd_unit(env: &InstallEnv, system: bool, daemon: &[String]) -> String {
    let exec = std::iter::once(display(&env.executable))
        .chain(daemon.iter().cloned())
        .collect::<Vec<_>>();
    let exec = exec
        .iter()
        .map(|part| systemd_quote(part))
        .collect::<Vec<_>>()
        .join(" ");
    let mut environment = env.pinned_environment("systemd");
    environment.push(("PATH".to_owned(), env.path.clone()));
    let mut unit = String::from(
        "# Generated by `zeroshot hub install --systemd`; remove with `zeroshot hub uninstall --systemd`.\n\
         [Unit]\n\
         Description=Zeroshot hub daemon\n\
         Documentation=https://the-open-engine.github.io/zeroshot/\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n",
    );
    unit.push_str(&format!("ExecStart={exec}\n"));
    for (key, value) in environment {
        unit.push_str(&format!(
            "Environment={}\n",
            systemd_quote(&format!("{key}={value}"))
        ));
    }
    // `WorkingDirectory=` takes a bare path: systemd keeps the quotes of a quoted value and
    // then rejects the unit as a non-absolute path, so it must not be systemd_quote'd.
    unit.push_str(&format!("WorkingDirectory={}\n", display(&env.home)));
    if system {
        unit.push_str(&format!("User={}\nGroup={}\n", env.uid, env.gid));
    }
    unit.push_str(&format!(
        "Restart=on-failure\nRestartSec=5\nKillSignal=SIGTERM\n\n[Install]\nWantedBy={}\n",
        if system {
            "multi-user.target"
        } else {
            "default.target"
        }
    ));
    unit
}

fn systemctl(system: bool, arguments: &[&str]) -> Vec<String> {
    let mut argv = vec!["systemctl".to_owned()];
    if !system {
        argv.push("--user".to_owned());
    }
    argv.extend(arguments.iter().map(|argument| (*argument).to_owned()));
    argv
}

pub(crate) fn systemd_install_plan(env: &InstallEnv, system: bool, daemon: &[String]) -> Plan {
    let unit = unit_name();
    let mut notes = vec![format!(
        "logs: journalctl {}-u {unit}",
        if system { "" } else { "--user " }
    )];
    if !system {
        notes.push(format!(
            "user units stop at logout unless lingering is enabled: loginctl enable-linger {}",
            env.user
        ));
    }
    Plan {
        manager: "systemd",
        steps: vec![
            Step::Write {
                path: unit_path(env, system),
                content: systemd_unit(env, system, daemon),
            },
            Step::Run {
                argv: systemctl(system, &["daemon-reload"]),
                may_fail: false,
            },
            Step::Run {
                argv: systemctl(system, &["enable", &unit]),
                may_fail: false,
            },
            Step::Run {
                argv: systemctl(system, &["restart", &unit]),
                may_fail: false,
            },
        ],
        notes,
    }
}

pub(crate) fn systemd_uninstall_plan(env: &InstallEnv, system: bool) -> Plan {
    let unit = unit_name();
    Plan {
        manager: "systemd",
        steps: vec![
            Step::Run {
                argv: systemctl(system, &["disable", "--now", &unit]),
                may_fail: true,
            },
            Step::Remove {
                path: unit_path(env, system),
            },
            Step::Run {
                argv: systemctl(system, &["daemon-reload"]),
                may_fail: false,
            },
        ],
        notes: vec!["hub state is kept".to_owned()],
    }
}

/// `docker run` arguments for the daemon container.
pub(crate) fn docker_run(
    env: &InstallEnv,
    image: &str,
    volumes: &[PathBuf],
    daemon: &[String],
) -> Vec<String> {
    let mut argv = [
        "docker",
        "run",
        "--detach",
        "--name",
        SERVICE_NAME,
        "--restart",
        "unless-stopped",
        "--network",
        "host",
        "--label",
        "org.openengine.zeroshot.hub=v1",
    ]
    .map(str::to_owned)
    .to_vec();
    argv.extend(["--user".to_owned(), format!("{}:{}", env.uid, env.gid)]);
    for (key, value) in env.pinned_environment("docker") {
        argv.extend(["--env".to_owned(), format!("{key}={value}")]);
    }
    let token_folder = daemon
        .iter()
        .position(|argument| argument == "--token-file")
        .and_then(|index| daemon.get(index + 1))
        .and_then(|file| Path::new(file).parent().map(Path::to_path_buf));
    let mut mounts: Vec<&PathBuf> = vec![&env.config_dir, &env.hub_dir, &env.state_dir];
    mounts.extend(volumes);
    mounts.extend(token_folder.as_ref());
    let mut seen: Vec<&PathBuf> = Vec::new();
    for mount in mounts {
        if seen.iter().any(|parent| mount.starts_with(parent)) {
            continue;
        }
        seen.retain(|child| !child.starts_with(mount));
        seen.push(mount);
    }
    for mount in seen {
        let path = display(mount);
        argv.extend(["--volume".to_owned(), format!("{path}:{path}")]);
    }
    argv.push(image.to_owned());
    argv.extend(daemon.iter().cloned());
    argv
}

pub(crate) fn docker_install_plan(
    env: &InstallEnv,
    image: &str,
    volumes: &[PathBuf],
    daemon: &[String],
) -> Plan {
    Plan {
        manager: "docker",
        steps: vec![
            Step::run_may_fail(&["docker", "rm", "--force", SERVICE_NAME]),
            Step::Run {
                argv: docker_run(env, image, volumes, daemon),
                may_fail: false,
            },
        ],
        notes: vec![
            format!("logs: docker logs --follow {SERVICE_NAME}"),
            "the container uses host networking so loopback target origins keep working".to_owned(),
            "jobs placed on `local` run inside the container; mount their working folders with \
             --volume, or prefer named targets"
                .to_owned(),
            format!(
                "the image must contain a zeroshot build with `hub`; until a release ships it, \
                 build one with `docker build -f docker/zeroshot-target/Dockerfile -t \
                 zeroshot-hub:dev .` and pass --image zeroshot-hub:dev (default {DEFAULT_IMAGE})"
            ),
        ],
    }
}

pub(crate) fn docker_uninstall_plan() -> Plan {
    Plan {
        manager: "docker",
        steps: vec![Step::run(&["docker", "rm", "--force", SERVICE_NAME])],
        notes: vec!["hub state is kept".to_owned()],
    }
}

fn plan_document(plan: &Plan, action: &str, dry_run: bool) -> Value {
    json!({
        "schema": "zeroshot.hub.install/v1",
        "action": action,
        "manager": plan.manager,
        "dryRun": dry_run,
        "steps": plan.steps.iter().map(Step::view).collect::<Vec<_>>(),
        "notes": plan.notes,
    })
}

/// Apply steps in order once the manager is available; stop at the first failing step that
/// may not fail.
async fn apply(plan: &Plan) -> Result<Vec<Value>, HubError> {
    require_program(plan.manager_program(), plan.manager).await?;
    apply_steps(&plan.steps).await
}

/// Refuse to change anything when `program --version` does not succeed.
pub(crate) async fn require_program(program: &str, manager: &str) -> Result<(), HubError> {
    let available = Command::new(program)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success());
    if !available {
        return Err(HubError::Usage(format!(
            "{program} is not available on this machine; nothing was changed (use --dry-run to \
             print the {manager} definition)"
        )));
    }
    Ok(())
}

/// Apply `steps` in order; stop at the first failing step that may not fail.
pub(crate) async fn apply_steps(steps: &[Step]) -> Result<Vec<Value>, HubError> {
    let mut results = Vec::new();
    for step in steps {
        match step {
            Step::Write { path, content } => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|error| {
                        HubError::storage(
                            &format!("{} could not be created", parent.display()),
                            error,
                        )
                    })?;
                }
                super::service::replace_private(path, content.as_bytes())?;
                results.push(json!({ "write": path, "ok": true }));
            }
            Step::Remove { path } => {
                match std::fs::remove_file(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(HubError::storage(
                            &format!("{} could not be removed", path.display()),
                            error,
                        ));
                    }
                }
                results.push(json!({ "remove": path, "ok": true }));
            }
            Step::Run { argv, may_fail } => {
                let output = Command::new(&argv[0])
                    .args(&argv[1..])
                    .stdin(Stdio::null())
                    .output()
                    .await;
                let (ok, detail) = match output {
                    Ok(output) => (
                        output.status.success(),
                        String::from_utf8_lossy(&output.stderr).trim().to_owned(),
                    ),
                    Err(error) => (false, format!("{} could not start: {error}", argv[0])),
                };
                results.push(json!({ "run": argv, "ok": ok, "stderr": detail }));
                if !ok && !may_fail {
                    return Err(HubError::Target {
                        message: format!("`{}` failed: {detail}", argv.join(" ")),
                        diagnostic: Some(json!({ "steps": results })),
                    });
                }
            }
        }
    }
    Ok(results)
}

pub(crate) async fn install(
    env: &InstallEnv,
    request: HubInstall,
    store: &HubStore,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let api = HubApiOptions {
        token_file: request
            .api
            .token_file
            .as_deref()
            .map(absolute)
            .transpose()?,
        ..request.api.clone()
    };
    let daemon = daemon_arguments(&request.interval, &api);
    let plan = match request.manager {
        HubManager::Systemd => systemd_install_plan(env, request.system, &daemon),
        HubManager::Docker => docker_install_plan(
            env,
            request.image.as_deref().unwrap_or(DEFAULT_IMAGE),
            &request.volumes,
            &daemon,
        ),
    };
    let mut document = plan_document(&plan, "install", request.dry_run);
    if request.dry_run {
        return write_json(output, &document);
    }
    refuse_other_owner(store, plan.manager)?;
    document["results"] = json!(apply(&plan).await?);
    write_json(output, &document)
}

/// Refuse to install when a daemon supervised by something else (another manager, or a
/// foreground `hub daemon run`) owns the hub folder: the new service could never take it.
/// Reinstalling under the same manager replaces its daemon.
pub(crate) fn refuse_other_owner(store: &HubStore, manager: &str) -> Result<(), HubError> {
    if store.try_daemon_lock()?.is_some() {
        return Ok(());
    }
    let owner = store
        .heartbeat()
        .and_then(|heartbeat| {
            heartbeat
                .get("manager")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "unknown".to_owned());
    if owner == manager {
        return Ok(());
    }
    Err(HubError::DaemonRunning(format!(
        "a hub daemon run by {owner} already owns {}; stop it first (for an installed one, \
         `zeroshot hub uninstall --{}`)",
        store.directory().display(),
        if owner == "docker" {
            "docker"
        } else {
            "systemd"
        }
    )))
}

pub(crate) async fn uninstall(
    env: &InstallEnv,
    request: HubUninstall,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let plan = match request.manager {
        HubManager::Systemd => systemd_uninstall_plan(env, request.system),
        HubManager::Docker => docker_uninstall_plan(),
    };
    let mut document = plan_document(&plan, "uninstall", request.dry_run);
    if !request.dry_run {
        document["results"] = json!(apply(&plan).await?);
    }
    write_json(output, &document)
}
