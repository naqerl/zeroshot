//! Parsed `zeroshot hub` requests.
//!
//! The hub pools registered targets behind one queue. A daemon (`hub daemon run`, installed with
//! `hub install`) owns the hub state and serves it over an authenticated HTTP API; every other
//! hub command is a client of the hub selected by `--hub`, `ZEROSHOT_HUB`, or the current hub
//! context. Parsing only checks syntax and keeps the run arguments in the exact `zeroshot run`
//! form; the shipped binary owns placement, queue state, and the delegated target calls.

use std::path::PathBuf;

/// A hub subcommand plus the hub context it addresses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubRequest {
    /// `--hub NAME`; `None` falls back to `ZEROSHOT_HUB`, then the current context.
    pub context: Option<String>,
    pub command: HubCommand,
}

/// One parsed hub subcommand.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HubCommand {
    Add(HubMemberAdd),
    Update(HubMemberUpdate),
    Remove {
        name: String,
    },
    List {
        tags: Vec<String>,
        probe: bool,
    },
    Submit(Box<HubSubmit>),
    Enqueue(Box<HubEnqueue>),
    Ps(HubPs),
    Status {
        job: String,
    },
    Watch {
        job: String,
        after: Option<String>,
    },
    Logs(HubLogs),
    Attach {
        job: String,
        execution: String,
    },
    ForceStop {
        job: String,
    },
    /// Remove a queued job (`hj_…`) or a recurring schedule (`hs_…`).
    Dequeue {
        job: String,
    },
    /// One dispatcher pass, for debugging; the daemon runs these continuously.
    Dispatch,
    Stats(HubStats),
    /// Foreground daemon: serve the hub API and dispatch every `interval` until SIGTERM or
    /// Ctrl-C.
    Daemon(HubDaemonRun),
    /// Ask the selected hub's daemon for its heartbeat.
    DaemonStatus,
    /// Manage the hub contexts this machine can address.
    Context(HubContextCommand),
    Install(HubInstall),
    Uninstall(HubUninstall),
}

/// `hub daemon run` options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubDaemonRun {
    pub interval: String,
    /// `ADDR:PORT` for the hub API; `None` uses the loopback default.
    pub listen: Option<String>,
    /// Bearer token file; `None` uses `api-token` in the hub folder, created when missing.
    pub token_file: Option<PathBuf>,
}

/// `hub context`: named hub endpoints, docker-context style.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HubContextCommand {
    Add {
        name: String,
        url: String,
        token: HubTokenSource,
    },
    List,
    Use {
        name: String,
    },
    Remove {
        name: String,
    },
}

/// Where a context's bearer token is read from at request time; the token itself is never
/// stored in the context file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HubTokenSource {
    File(PathBuf),
    Env(String),
}

/// Service manager that supervises the hub daemon.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HubManager {
    Systemd,
    Docker,
}

/// `hub install`: generate and apply a service definition for the daemon.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubInstall {
    pub manager: HubManager,
    /// systemd only: a system unit instead of a user unit.
    pub system: bool,
    /// docker only: image containing a zeroshot binary with `hub`.
    pub image: Option<String>,
    /// docker only: extra host folders mounted at the same path, such as job working folders.
    pub volumes: Vec<PathBuf>,
    pub interval: String,
    /// Passed to `hub daemon run --listen`.
    pub listen: Option<String>,
    /// Passed to `hub daemon run --token-file`.
    pub token_file: Option<PathBuf>,
    pub dry_run: bool,
}

/// `hub uninstall`: stop and remove the daemon service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubUninstall {
    pub manager: HubManager,
    pub system: bool,
    pub dry_run: bool,
}

/// Registration of an existing named target, or the built-in `local` controller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubMemberAdd {
    pub name: String,
    pub tags: Vec<String>,
    pub slots: u32,
    pub priority: i32,
}

/// Partial update of one hub member.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HubMemberUpdate {
    pub name: String,
    pub add_tags: Vec<String>,
    pub remove_tags: Vec<String>,
    pub slots: Option<u32>,
    pub priority: Option<i32>,
    pub state: Option<HubMemberState>,
}

/// Placement eligibility of a hub member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HubMemberState {
    /// Accepts new placements.
    Active,
    /// Keeps running jobs but accepts no new placements.
    Drain,
    /// Ignored by placement and probes.
    Disabled,
}

/// Choice among free eligible members.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HubStrategy {
    /// Highest priority, then registration order.
    #[default]
    FirstFree,
    /// Lowest active-to-slots ratio.
    LeastLoaded,
}

/// A run request shared by `submit` and `enqueue`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubJobSpec {
    pub title: String,
    /// Arguments after `zeroshot run`, without target, submission key, or detach.
    pub run_args: Vec<String>,
    pub target: Option<String>,
    pub tags: Vec<String>,
    pub strategy: HubStrategy,
    pub labels: Vec<(String, String)>,
    /// Runtime files used to attribute token usage to a model.
    pub runtime_files: Vec<PathBuf>,
    pub repository: Option<String>,
    pub branch: Option<String>,
    pub revision: Option<String>,
    pub source_connection: Option<String>,
    /// An `--environment` file was given; local runs reject environment hooks.
    pub environment: bool,
    pub cwd: Option<PathBuf>,
    pub submission_key: Option<String>,
}

/// Immediate placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubSubmit {
    pub job: HubJobSpec,
    pub detach: bool,
    pub wait: Option<Option<String>>,
    pub queue: bool,
}

/// Deferred placement: once (`at`) or recurring (`cron`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubEnqueue {
    pub job: HubJobSpec,
    pub at: Option<String>,
    /// Five-field cron expression; each fire queues one job.
    pub cron: Option<String>,
    /// IANA time zone for `cron`; `None` uses the enqueuing machine's zone.
    pub cron_timezone: Option<String>,
    pub priority: i32,
    pub expires: Option<String>,
}

/// Hub job listing filters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubPs {
    pub state: String,
    pub target: Option<String>,
    pub labels: Vec<(String, String)>,
    pub limit: Option<usize>,
}

/// Log passthrough for one hub job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubLogs {
    pub job: String,
    pub after: Option<String>,
    pub execution: Option<String>,
}

/// Token and outcome aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubStats {
    pub by: Vec<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub prices: Option<PathBuf>,
}
