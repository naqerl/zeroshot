//! `hub install` / `hub uninstall`: generate a systemd unit or a Docker container command that
//! runs `zeroshot hub daemon run`, then apply it step by step. Generation is pure so it is
//! tested without systemd or Docker; `--dry-run` prints the same plan.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use serde_json::{Value, json};
use tokio::process::Command;
use zeroshot_engine::native_v2_cli::hub::{HubInstall, HubManager, HubUninstall};

use super::daemon::{DaemonProbe, Detection, MANAGER_ENV, SERVICE_NAME};
use super::error::HubError;
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
        let executable = std::env::current_exe()
            .and_then(std::fs::canonicalize)
            .map_err(|error| HubError::storage("executable path is unavailable", error))?;
        let config_dir = registry_path
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| unavailable("the target registry folder"))?;
        let state_dir = std::env::var_os("ZEROSHOT_STATE_DIR")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("XDG_STATE_HOME")
                    .filter(|value| !value.is_empty())
                    .map(|root| PathBuf::from(root).join("zeroshot"))
            })
            .unwrap_or_else(|| home.join(".local").join("state").join("zeroshot"));
        let user_unit_dir = std::env::var_os("XDG_CONFIG_HOME")
            .filter(|value| !value.is_empty())
            .map_or_else(|| home.join(".config"), PathBuf::from)
            .join("systemd")
            .join("user");
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
            hub_dir: hub_dir.to_path_buf(),
            state_dir,
            user_unit_dir,
            credential_store: std::env::var("ZEROSHOT_CREDENTIAL_STORE")
                .ok()
                .filter(|value| !value.is_empty()),
        })
    }

    fn pinned_environment(&self, manager: &str) -> Vec<(String, String)> {
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
pub(crate) fn systemd_unit(env: &InstallEnv, system: bool, interval: &str) -> String {
    let exec = [
        display(&env.executable),
        "hub".to_owned(),
        "daemon".to_owned(),
        "run".to_owned(),
        "--interval".to_owned(),
        interval.to_owned(),
    ]
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
    unit.push_str(&format!(
        "WorkingDirectory={}\n",
        systemd_quote(&display(&env.home))
    ));
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

pub(crate) fn systemd_install_plan(env: &InstallEnv, system: bool, interval: &str) -> Plan {
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
                content: systemd_unit(env, system, interval),
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
    interval: &str,
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
    let mut mounts: Vec<&PathBuf> = vec![&env.config_dir, &env.hub_dir, &env.state_dir];
    mounts.extend(volumes);
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
    argv.extend([image, "hub", "daemon", "run", "--interval", interval].map(str::to_owned));
    argv
}

pub(crate) fn docker_install_plan(
    env: &InstallEnv,
    image: &str,
    volumes: &[PathBuf],
    interval: &str,
) -> Plan {
    Plan {
        manager: "docker",
        steps: vec![
            Step::run_may_fail(&["docker", "rm", "--force", SERVICE_NAME]),
            Step::Run {
                argv: docker_run(env, image, volumes, interval),
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

/// Apply steps in order; stop at the first failing step that may not fail.
async fn apply(plan: &Plan) -> Result<Vec<Value>, HubError> {
    let program = plan.manager_program();
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
             print the {} definition)",
            plan.manager
        )));
    }
    let mut results = Vec::new();
    for step in &plan.steps {
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
                std::fs::write(path, content).map_err(|error| {
                    HubError::storage(&format!("{} could not be written", path.display()), error)
                })?;
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
    probe: &dyn DaemonProbe,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let plan = match request.manager {
        HubManager::Systemd => systemd_install_plan(env, request.system, &request.interval),
        HubManager::Docker => docker_install_plan(
            env,
            request.image.as_deref().unwrap_or(DEFAULT_IMAGE),
            &request.volumes,
            &request.interval,
        ),
    };
    let mut document = plan_document(&plan, "install", request.dry_run);
    if request.dry_run {
        return write_json(output, &document);
    }
    let detection = Detection::probe(probe).await;
    let other = match request.manager {
        HubManager::Systemd => detection.docker.active.then_some("docker"),
        HubManager::Docker => detection.systemd.active.then_some("systemd"),
    };
    if let Some(other) = other {
        return Err(HubError::DaemonConflict {
            message: format!(
                "a hub daemon is already active under {other}; run `zeroshot hub uninstall \
                 --{other}` first"
            ),
            detection: detection.view(),
        });
    }
    document["results"] = json!(apply(&plan).await?);
    write_json(output, &document)
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
