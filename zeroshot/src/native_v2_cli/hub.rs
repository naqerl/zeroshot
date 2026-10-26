//! Parsed `zeroshot hub` requests.
//!
//! The hub pools registered targets behind one queue. A daemon (`hub daemon run`, installed with
//! `hub install`) owns the hub state and serves it over an authenticated HTTP API; every other
//! hub command is a client of the hub selected by `--hub`, `ZEROSHOT_HUB`, or the current hub
//! context. Parsing only checks syntax and keeps the run arguments in the exact `zeroshot run`
//! form; the shipped binary owns placement, queue state, and the delegated target calls.

use std::path::PathBuf;

/// Whether `name` can name an eval suite, case, variant, or check: 1 to 64 characters of
/// ASCII letters, digits, `.`, `_`, and `-`. The parser and the hub daemon share this rule.
#[must_use]
pub fn valid_eval_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
}

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
    /// Evaluation suites, cases, runs, and reports.
    Eval(HubEvalCommand),
    /// On-demand cloud server pools.
    Pool(HubPoolCommand),
}

/// `hub pool`: on-demand cloud server pools whose servers become hub members.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HubPoolCommand {
    Add(Box<HubPoolAdd>),
    List,
    Remove {
        name: String,
    },
    /// List the provider's server types with prices.
    Types,
    /// Clear a blocked, capacity-waiting or boot-paused pool so it starts servers again.
    Resume {
        name: String,
    },
    /// Set the machines a pool creates for jobs with a tag.
    TagSet(Box<HubPoolTag>),
    /// List a pool's tag profiles.
    TagList {
        name: String,
    },
    /// Remove a tag profile; jobs with the tag fall back to the pool's types.
    TagRemove {
        name: String,
        tag: String,
    },
}

/// `hub pool tag set`: the machines a pool creates for jobs carrying `tag`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HubPoolTag {
    pub pool: String,
    pub tag: String,
    /// Server types, best first; empty means the pool's types that meet the minimum.
    pub server_types: Vec<String>,
    /// Locations in order; empty means the pool's locations.
    pub locations: Vec<String>,
    pub min_cores: Option<u32>,
    pub min_memory_gb: Option<u32>,
    pub slots: Option<u32>,
    pub priority: u32,
}

/// `hub pool add`. Durations stay text until the client parses them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubPoolAdd {
    pub name: String,
    pub provider: String,
    /// Server types, best first.
    pub server_types: Vec<String>,
    /// Locations in order; empty means every location offering one of the types.
    pub locations: Vec<String>,
    pub multi_arch: bool,
    pub wait_max: Option<String>,
    pub image: String,
    pub target_image: String,
    pub max: u32,
    pub slots: u32,
    pub idle: String,
    pub tags: Vec<String>,
    pub ssh_key: PathBuf,
    pub ssh_from: Vec<String>,
    /// Filesystem of the servers' data folder; only `btrfs`.
    pub data_fs: String,
    /// Size of the btrfs data file on the local disk, in GB; `None` sizes it to the free disk.
    pub data_size_gb: Option<u32>,
    /// Keep the data on an attached Volume of this many GB instead of the local disk.
    pub data_volume_gb: Option<u32>,
}

/// `hub daemon run` options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubDaemonRun {
    pub interval: String,
    pub api: HubApiOptions,
}

/// Hub API listener options of `hub daemon run`, passed through by `hub install`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HubApiOptions {
    /// `ADDR:PORT` for the hub API; `None` uses the loopback default.
    pub listen: Option<String>,
    /// Bearer token file; `None` uses `api-token` in the hub folder, created when missing.
    pub token_file: Option<PathBuf>,
    /// Loopback `ADDR:PORT` serving only `/metrics` without a token.
    pub metrics_listen: Option<String>,
    /// Job label keys whose values are exported with token counters.
    pub metrics_labels: Vec<String>,
    /// Submissions a target may leave unconfirmed before the job fails; `None` uses the
    /// daemon's default.
    pub unconfirmed_limit: Option<u32>,
    /// Hetzner Cloud API token file for pool servers; `None` reads `HCLOUD_TOKEN`.
    pub hetzner_token_file: Option<PathBuf>,
    /// Stop a running job whose watch cursor does not advance for this long; `None` disables it.
    pub max_stall: Option<String>,
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
    /// Passed through to `hub daemon run`.
    pub api: HubApiOptions,
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
    pub expires: Option<String>,
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

/// `hub eval`: capture cases, rerun them under variants, measure, judge, and compare.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HubEvalCommand {
    /// Create a suite, or update its description.
    SuiteAdd {
        name: String,
        description: Option<String>,
    },
    SuiteList,
    CaseAdd(Box<HubEvalCaseAdd>),
    CaseList {
        suite: Option<String>,
    },
    CaseShow {
        case: String,
    },
    Run(Box<HubEvalRun>),
    List {
        suite: Option<String>,
        limit: Option<usize>,
    },
    Status {
        run: String,
    },
    Report(HubEvalReport),
}

/// `hub eval case add`: snapshot a task from a hub job or from run flags.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubEvalCaseAdd {
    /// Suites the case joins; each must exist.
    pub suites: Vec<String>,
    pub name: Option<String>,
    /// Extra labels for a case captured `--from-job`; flag cases carry theirs in the spec.
    pub labels: Vec<(String, String)>,
    pub source: HubEvalCaseSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HubEvalCaseSource {
    /// Copy an existing hub job's run request and source pin.
    FromJob(String),
    /// Build the case from `zeroshot run` flags, like `hub enqueue`.
    Flags(Box<HubJobSpec>),
}

/// One `--variant NAME=KEY=VALUE[,KEY=VALUE...]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubEvalVariant {
    pub name: String,
    /// Ordered `(key, value)` overrides; keys are checked by the parser.
    pub overrides: Vec<(String, String)>,
}

/// How the judge scores a sample.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HubJudgeMode {
    /// Score each sample on its own, 0..=1.
    #[default]
    Absolute,
    /// Compare each variant sample with the baseline sample of the same case and repeat,
    /// -1..=1 (positive means the variant is better).
    Pairwise,
}

/// `hub eval run`: enqueue every case x variant x repeat through hub placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubEvalRun {
    pub suite: Option<String>,
    pub cases: Vec<String>,
    /// A variant name (default `baseline`, the case as captured), a hub job ID whose result is
    /// the baseline sample, or `none`.
    pub baseline: Option<String>,
    pub variants: Vec<HubEvalVariant>,
    pub repeat: u32,
    pub target: Option<String>,
    pub tags: Vec<String>,
    pub priority: i32,
    /// `cmd:SCRIPT` or `zeroshot:GRAPH`.
    pub judge: Option<String>,
    pub judge_mode: HubJudgeMode,
    /// Uniform runtime config for a `zeroshot:` judge.
    pub judge_runtime: Option<PathBuf>,
    /// `(name, shell command)` checks run against each finished sample.
    pub checks: Vec<(String, String)>,
    pub prices: Option<PathBuf>,
    /// Idempotency key: the same key and request return the same eval run.
    pub key: Option<String>,
}

/// `hub eval report`: per-case and aggregate deltas against a baseline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubEvalReport {
    pub run: String,
    /// A variant of this run, or `er_ID[:VARIANT]` of another run.
    pub baseline: Option<String>,
    /// `(metric, value)` overrides of the improvement/regression thresholds.
    pub thresholds: Vec<(String, String)>,
    pub prices: Option<PathBuf>,
    pub fail_on_regression: bool,
    pub wait: Option<Option<String>>,
    pub table: bool,
}
