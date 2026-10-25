use clap::{Args, Subcommand, ValueEnum};

use super::RunArgs;

#[path = "hub/eval.rs"]
mod eval;
use crate::native_v2_cli::NativeV2CliError;
use crate::native_v2_cli::hub::{
    HubApiOptions, HubCommand, HubContextCommand, HubDaemonRun, HubEnqueue, HubInstall, HubJobSpec,
    HubLogs, HubManager, HubMemberAdd, HubMemberState, HubMemberUpdate, HubPs, HubStats,
    HubPoolAdd, HubPoolCommand, HubStrategy, HubSubmit, HubTokenSource, HubUninstall,
};

#[derive(Debug, Subcommand)]
#[command(after_long_help = r#"HUB
A hub pools targets registered with `zeroshot target add`, plus the built-in `local` controller.
It picks a target for each run and keeps queued, scheduled, and recurring runs in shared state
beside the target registry. Every placed job is one ordinary run on exactly one target, so
`zeroshot status RUN_ID --target NAME` keeps working.

DAEMON AND API
A hub daemon owns the hub: it is the only writer of its state, serves the hub over an HTTP API
with bearer-token auth (default 127.0.0.1:7383, token in the hub folder's api-token file), and
dispatches continuously: it fires cron schedules, places due jobs, polls running jobs to
completion, and records token usage. Install it once with `zeroshot hub install --systemd`
(user unit; --system for a system unit) or `--docker` (restarting container); `hub daemon run`
is the foreground entrypoint the services start. One daemon owns a hub folder at a time.

CONTEXTS
Every other hub command is a client of one hub, chosen by --hub NAME, then ZEROSHOT_HUB, then
`hub context use`, then the built-in `local` context (this machine's daemon, found through the
hub folder). Add another machine's hub with `zeroshot hub context add NAME URL --token-file
FILE` (or --token-env VAR); the token is read at request time and never stored or printed. A
hub that does not answer fails with hub.unreachable (exit 5) naming the context and URL.

Placement: --target pins one member; each --tag must match (AND). Members must be active, and
a probe must reach them. Members must also support the run: --repository, --branch, --revision,
and --environment hooks exclude `local`; named targets need a pushed revision, and generic Git
transport or per-node runtime lanes need the matching discovery marker
(openengine.git-transport/v1, openengine.node-runtime-lanes/v1). `hub list` shows each member's
discovered capabilities and the queued jobs it rejects. Free members (active runs < slots) are
ranked by --strategy. Hub documents carry a `schema` field.

EXIT CODES
  0  success
  1  error (ZEROSHOT_ERROR_FORMAT=json writes a zeroshot.error/v1 diagnostic)
  3  no capacity: matching members are busy or unreachable, or --wait timed out (retryable)
  4  no eligible target: no active member matches the placement, or none supports the run
     (not retryable; details.rejected lists each member's reason)
  5  hub.unreachable: the selected hub context's daemon does not answer

EXAMPLES
  zeroshot hub install --systemd
  zeroshot hub context add lab http://lab-box:7383 --token-file ~/.config/zeroshot/lab.token
  zeroshot hub context use lab
  zeroshot hub add box-1 --tag gpu --slots 2
  zeroshot hub list
  zeroshot hub submit --tag gpu --title Fix --template single-worker --input in.json -d
  zeroshot hub enqueue --at +2h --priority 5 --title Later --profile nightly --input in.json
  zeroshot hub enqueue --cron "0 3 * * *" --cron-tz Europe/Belgrade --title Nightly \
    --profile nightly --input in.json
  zeroshot hub ps
  zeroshot hub stats --by target --by model --since 7d"#)]
pub(super) enum HubCliCommand {
    /// Add a registered target, or `local`, to the hub.
    Add(HubAddArgs),

    /// Change a member's tags, slots, priority, or placement state.
    Update(HubUpdateArgs),

    /// Remove a member from the hub. Its target registration is unchanged.
    #[command(visible_alias = "rm")]
    Remove(HubNameArgs),

    /// List members with health and load as JSON.
    #[command(visible_alias = "ls")]
    List(HubListArgs),

    /// Place a run on a free member now.
    ///
    /// Fails with exit code 3 when every matching member is busy, unless --wait or --queue is
    /// given. Without --detach, follows the placed run's events as NDJSON.
    Submit(Box<HubSubmitArgs>),

    /// Accept a run into the hub queue for later placement, once or on a cron schedule.
    ///
    /// --cron creates a recurring schedule (`hs_…`); the hub daemon queues one job per fire.
    /// Cron fields use the --cron-tz zone, stored with the schedule, so daylight-saving changes
    /// follow that zone wherever the daemon runs. Missed fires collapse into one catch-up fire,
    /// and a fire is skipped while the schedule's previous job is still queued.
    Enqueue(Box<HubEnqueueArgs>),

    /// List hub jobs as JSON.
    #[command(name = "ps")]
    Ps(HubPsArgs),

    /// Read one hub job with its run status as JSON.
    Status(HubJobArgs),

    /// Follow a placed job's run events as NDJSON.
    Watch(HubWatchArgs),

    /// Follow a placed job's run logs as NDJSON.
    Logs(HubLogsArgs),

    /// Attach to an execution of a placed job as NDJSON.
    Attach(HubAttachArgs),

    /// Force a placed job's run to stop.
    ForceStop(HubJobArgs),

    /// Remove a queued or scheduled job before it is placed, a placing job its target has not
    /// confirmed, or a recurring schedule (`hs_…`).
    ///
    /// A placing job's target may already hold a run under its submission key
    /// (`hub-JOB_ID-ATTEMPT`); the hub records that in the job's error and never follows or
    /// adopts that run. Removing a schedule keeps the jobs it already queued.
    Dequeue(HubJobArgs),

    /// Ask the hub daemon to run one dispatcher pass now and print its summary.
    ///
    /// The daemon runs the same pass every --interval; a requested pass is serialized with it.
    Dispatch,

    /// Aggregate job outcomes and provider-reported tokens as JSON.
    ///
    /// A job is attributed to one model only when its runtime files name exactly one model;
    /// otherwise its model is `mixed` or `unknown`. Costs appear only with --prices.
    Stats(HubStatsArgs),

    /// Run or inspect the hub daemon.
    #[command(subcommand)]
    Daemon(HubDaemonCommand),

    /// Manage named hub endpoints this machine can address.
    #[command(subcommand)]
    Context(HubContextCliCommand),

    /// Install the hub daemon as a systemd unit or a Docker container and start it.
    ///
    /// The service runs `zeroshot hub daemon run` with this shell's config, hub, and state
    /// folders pinned, so it sees the same targets and queue; --listen and --token-file are
    /// passed through. --dry-run prints the generated unit or container command and the steps
    /// without changing anything.
    Install(HubInstallArgs),

    /// Stop and remove the hub daemon service. Hub state is kept.
    Uninstall(HubUninstallArgs),

    /// Capture tasks as eval cases, rerun them under variants, judge, and compare.
    #[command(subcommand)]
    Eval(eval::HubEvalCliCommand),

    /// Manage on-demand cloud server pools whose servers join the hub as members.
    ///
    /// A pool with no live server still counts as eligible, so a matching job queues and the
    /// daemon creates a server for it (up to --max). Each server runs the pinned target image
    /// behind an SSH tunnel from the hub, is archived before it is deleted, and is torn down
    /// after --idle without jobs or at --ttl. Servers are billed while they exist.
    #[command(subcommand)]
    Pool(HubPoolCliCommand),
}

#[derive(Debug, Subcommand)]
pub(super) enum HubPoolCliCommand {
    /// Add a pool. The daemon needs a provider token (--hetzner-token-file or HCLOUD_TOKEN).
    Add(Box<HubPoolAddArgs>),

    /// List pools and their servers as JSON.
    #[command(visible_alias = "ls")]
    List,

    /// Remove a pool that has no servers left.
    #[command(visible_alias = "rm")]
    Remove(HubPoolNameArgs),

    /// List the provider's current server types as JSON: vCPUs, memory, disk, architecture,
    /// gross price per hour by location, and the locations that can create each one now.
    ///
    /// Asks the hub daemon, which reads the provider's catalog with its token.
    Types,

    /// Let a pool start servers again after it stopped on an account limit or a rejected
    /// request, and retry a capacity shortage or a failed-boot pause now.
    Resume(HubPoolNameArgs),
}

#[derive(Debug, Args)]
pub(super) struct HubPoolNameArgs {
    /// Pool name.
    #[arg(value_name = "NAME")]
    name: String,
}

#[derive(Debug, Args)]
pub(super) struct HubPoolAddArgs {
    /// Pool name; servers are named zs-<job>.
    #[arg(value_name = "NAME")]
    name: String,

    /// Cloud provider.
    #[arg(long, value_name = "PROVIDER", default_value = "hetzner")]
    provider: String,

    /// Server types to create, best first, comma-separated. A job gets the first type that
    /// fits its size request (`hub submit --server-type/--min-cpu/--min-ram-gb`). Each must be
    /// a current type from `hub pool types`; ARM types need --multi-arch.
    #[arg(long = "type", value_name = "TYPE,...", required = true,
          action = clap::ArgAction::Append, value_delimiter = ',')]
    server_types: Vec<String>,

    /// Locations to try, in order, for each type, comma-separated. When a location has no
    /// capacity the next one is tried, then the next type. Default: every location offering
    /// one of the types.
    #[arg(long = "location", value_name = "LOCATION,...",
          action = clap::ArgAction::Append, value_delimiter = ',')]
    locations: Vec<String>,

    /// The target image runs on arm64 as well as amd64, so ARM server types are allowed.
    #[arg(long)]
    multi_arch: bool,

    /// Expire a queued job this pool serves once it has waited this long for a server, such as
    /// 30m; its error kind is pool_wait_timeout. Default: wait until the job's own expiry.
    #[arg(long, value_name = "DURATION")]
    wait_max: Option<String>,

    /// Server image: a snapshot id or a system image such as docker-ce.
    #[arg(long, value_name = "IMAGE")]
    image: String,

    /// Pinned zeroshot target container image (a digest or a tag other than latest).
    #[arg(long, value_name = "IMAGE")]
    target_image: String,

    /// Most servers alive at once.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..=100))]
    max: u32,

    /// Concurrent runs per server.
    #[arg(long, value_name = "N", default_value_t = 1,
          value_parser = clap::value_parser!(u32).range(1..=1024))]
    slots: u32,

    /// Tear a server down after this long without jobs.
    #[arg(long, value_name = "DURATION", default_value = "10m")]
    idle: String,

    /// Tear a server down this long after creation, even while busy; it also shuts itself down
    /// shortly after as a backstop.
    #[arg(long, value_name = "DURATION", default_value = "2h")]
    ttl: String,

    /// Placement tag servers join with. Repeat for several.
    #[arg(long = "tag", visible_alias = "tags", value_name = "TAG",
          action = clap::ArgAction::Append, value_delimiter = ',')]
    tags: Vec<String>,

    /// Private SSH key on the hub host; its .pub sibling is installed on servers.
    #[arg(long, value_name = "FILE")]
    ssh_key: std::path::PathBuf,

    /// CIDR allowed to reach servers over SSH (the hub's public address). Repeat for several.
    #[arg(long, value_name = "CIDR", required = true, action = clap::ArgAction::Append)]
    ssh_from: Vec<String>,
}

impl HubPoolCliCommand {
    fn into_command(self) -> Result<HubPoolCommand, NativeV2CliError> {
        Ok(match self {
            Self::Add(args) => {
                let args = *args;
                HubPoolCommand::Add(Box::new(HubPoolAdd {
                    name: member_name(args.name)?,
                    provider: args.provider,
                    server_types: args.server_types,
                    locations: args.locations,
                    multi_arch: args.multi_arch,
                    wait_max: args.wait_max,
                    image: args.image,
                    target_image: args.target_image,
                    max: args.max,
                    slots: args.slots,
                    idle: args.idle,
                    ttl: args.ttl,
                    tags: tags(args.tags)?,
                    ssh_key: args.ssh_key,
                    ssh_from: args.ssh_from,
                }))
            }
            Self::List => HubPoolCommand::List,
            Self::Remove(args) => HubPoolCommand::Remove {
                name: member_name(args.name)?,
            },
            Self::Types => HubPoolCommand::Types,
            Self::Resume(args) => HubPoolCommand::Resume {
                name: member_name(args.name)?,
            },
        })
    }
}

#[derive(Debug, Subcommand)]
pub(super) enum HubDaemonCommand {
    /// Serve the hub API and dispatch in the foreground until SIGTERM or Ctrl-C; the entrypoint
    /// of installed services.
    ///
    /// Each pass fires due cron schedules, refreshes capabilities and running jobs, expires stale
    /// jobs, and places due ones; target calls run outside the state lock, so API reads never
    /// wait for a slow target. Passes with activity are written as NDJSON. Only one daemon may
    /// own a hub folder at a time.
    Run(HubDaemonRunArgs),

    /// Ask the selected hub's daemon for its heartbeat as JSON.
    ///
    /// Exits 0 when the daemon answers and 5 (hub.unreachable) when it does not.
    Status,
}

#[derive(Debug, Args)]
pub(super) struct HubDaemonRunArgs {
    /// Pause between dispatcher passes.
    #[arg(long, value_name = "DURATION", default_value = "10s")]
    interval: String,

    #[command(flatten)]
    api: HubApiArgs,
}

#[derive(Debug, Args)]
pub(super) struct HubApiArgs {
    /// Address for the hub API [default: 127.0.0.1:7383]. Use a non-loopback address only on
    /// a trusted network or behind a TLS proxy; the API is plain HTTP.
    #[arg(long, value_name = "ADDR:PORT")]
    listen: Option<String>,

    /// Bearer token file for the hub API [default: api-token in the hub folder, created with
    /// a random token when missing].
    #[arg(long, value_name = "FILE")]
    token_file: Option<std::path::PathBuf>,

    /// Also serve only GET /metrics on this loopback ADDR:PORT without a token, for a local
    /// Prometheus or agent. /metrics on the API address always needs the bearer token.
    #[arg(long, value_name = "ADDR:PORT")]
    metrics_listen: Option<String>,

    /// Export token counters by this job label key, such as task. Repeat for several; values
    /// beyond the first 50 collapse into _other.
    #[arg(long = "metrics-label", value_name = "KEY", action = clap::ArgAction::Append)]
    metrics_labels: Vec<String>,

    /// Fail a job after this many submissions its target did not confirm (timeouts, lost
    /// receipts) [default: 30]. Each is retried on the same member with the same key.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
    unconfirmed_limit: Option<u32>,

    /// Hetzner Cloud API token file for pool servers [default: the HCLOUD_TOKEN environment
    /// variable]. The token is never logged or stored in hub state.
    #[arg(long, value_name = "FILE")]
    hetzner_token_file: Option<std::path::PathBuf>,
}

impl HubApiArgs {
    fn into_options(self) -> Result<HubApiOptions, NativeV2CliError> {
        for key in &self.metrics_labels {
            if key.is_empty() || key.contains(char::is_whitespace) {
                return Err(usage(format!(
                    "invalid --metrics-label {key:?}: use a job label key"
                )));
            }
        }
        Ok(HubApiOptions {
            listen: self.listen,
            token_file: self.token_file,
            metrics_listen: self.metrics_listen,
            metrics_labels: self.metrics_labels,
            unconfirmed_limit: self.unconfirmed_limit,
            hetzner_token_file: self.hetzner_token_file,
        })
    }
}

#[derive(Debug, Subcommand)]
pub(super) enum HubContextCliCommand {
    /// Add a hub endpoint. The token is read from FILE or VAR at each request; it is never
    /// copied into the context file.
    Add(HubContextAddArgs),

    /// List hub contexts as JSON, marking the current and selected ones.
    #[command(visible_alias = "ls")]
    List,

    /// Make a context the default for hub commands on this machine.
    Use(HubContextNameArgs),

    /// Remove a context. The built-in `local` context cannot be removed.
    #[command(visible_alias = "rm")]
    Remove(HubContextNameArgs),
}

#[derive(Debug, Args)]
#[command(group(clap::ArgGroup::new("token").required(true).args(["token_file", "token_env"])))]
pub(super) struct HubContextAddArgs {
    /// Context name.
    #[arg(value_name = "NAME")]
    name: String,

    /// Hub API base URL, such as http://lab-box:7383.
    #[arg(value_name = "URL")]
    url: String,

    /// File holding the bearer token, such as a copy of the hub's api-token.
    #[arg(long, value_name = "FILE")]
    token_file: Option<std::path::PathBuf>,

    /// Environment variable holding the bearer token.
    #[arg(long, value_name = "VAR")]
    token_env: Option<String>,
}

#[derive(Debug, Args)]
pub(super) struct HubContextNameArgs {
    /// Context name.
    #[arg(value_name = "NAME")]
    name: String,
}

#[derive(Debug, Args)]
#[command(group(clap::ArgGroup::new("manager").required(true).args(["systemd", "docker"])))]
pub(super) struct HubInstallArgs {
    /// Install a systemd unit (user unit unless --system) and enable it now.
    #[arg(long)]
    systemd: bool,

    /// Run a Docker container with a restart policy and the hub folders mounted.
    #[arg(long)]
    docker: bool,

    /// systemd: install a system unit running as this user (needs root).
    #[arg(long, conflicts_with = "docker")]
    system: bool,

    /// docker: image with a zeroshot binary that includes `hub`.
    #[arg(long, value_name = "IMAGE", conflicts_with = "systemd")]
    image: Option<String>,

    /// docker: also mount this host folder at the same path, such as a job's working folder.
    #[arg(long, value_name = "PATH", conflicts_with = "systemd", action = clap::ArgAction::Append)]
    volume: Vec<std::path::PathBuf>,

    /// Pause between dispatcher passes.
    #[arg(long, value_name = "DURATION", default_value = "10s")]
    interval: String,

    #[command(flatten)]
    api: HubApiArgs,

    /// Print the generated service definition and steps without applying them.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
#[command(group(clap::ArgGroup::new("manager").required(true).args(["systemd", "docker"])))]
pub(super) struct HubUninstallArgs {
    /// Remove the systemd unit.
    #[arg(long)]
    systemd: bool,

    /// Remove the Docker container.
    #[arg(long)]
    docker: bool,

    /// systemd: remove the system unit instead of the user unit.
    #[arg(long, conflicts_with = "docker")]
    system: bool,

    /// Print the steps without applying them.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum HubStrategyArg {
    /// Highest member priority, then registration order.
    FirstFree,
    /// Lowest active-runs-to-slots ratio.
    LeastLoaded,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum HubStateArg {
    Active,
    Drain,
    Disabled,
}

#[derive(Debug, Args)]
pub(super) struct HubNameArgs {
    /// Member name: a registered target name or `local`.
    #[arg(value_name = "NAME")]
    name: String,
}

#[derive(Debug, Args)]
pub(super) struct HubAddArgs {
    /// Registered target name, or `local` for the local controller.
    #[arg(value_name = "NAME")]
    name: String,

    /// Placement tag, such as gpu or project=citydiff. Repeat for several.
    #[arg(long, value_name = "TAG", action = clap::ArgAction::Append)]
    tag: Vec<String>,

    /// Concurrent runs the hub places on this member.
    #[arg(long, value_name = "N", default_value_t = 1,
          value_parser = clap::value_parser!(u32).range(1..=1024))]
    slots: u32,

    /// Higher priority members are preferred by first-free placement.
    #[arg(
        long,
        value_name = "N",
        default_value_t = 0,
        allow_negative_numbers = true
    )]
    priority: i32,
}

#[derive(Debug, Args)]
pub(super) struct HubUpdateArgs {
    /// Member name.
    #[arg(value_name = "NAME")]
    name: String,

    /// Add this tag. Repeat for several.
    #[arg(long, value_name = "TAG", action = clap::ArgAction::Append)]
    add_tag: Vec<String>,

    /// Remove this tag. Repeat for several.
    #[arg(long, value_name = "TAG", action = clap::ArgAction::Append)]
    remove_tag: Vec<String>,

    /// Concurrent runs the hub places on this member.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..=1024))]
    slots: Option<u32>,

    /// Placement priority.
    #[arg(long, value_name = "N", allow_negative_numbers = true)]
    priority: Option<i32>,

    /// active accepts placements; drain keeps running jobs but takes no new ones; disabled is ignored.
    #[arg(long, value_enum, value_name = "STATE")]
    state: Option<HubStateArg>,
}

#[derive(Debug, Args)]
pub(super) struct HubListArgs {
    /// Only list members carrying this tag. Repeat for several.
    #[arg(long, value_name = "TAG", action = clap::ArgAction::Append)]
    tag: Vec<String>,

    /// Report stored members without contacting targets.
    #[arg(long)]
    no_probe: bool,
}

#[derive(Debug, Args)]
pub(super) struct HubPlacementArgs {
    /// Require this member tag. Repeat for several; all must match.
    #[arg(long, value_name = "TAG", action = clap::ArgAction::Append)]
    tag: Vec<String>,

    /// Pool jobs: run on this server type (one the pool lists). Same as --tag server-type=TYPE.
    #[arg(long, value_name = "TYPE")]
    server_type: Option<String>,

    /// Pool jobs: run on a server with at least N vCPUs. Same as --tag min-cpu=N.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
    min_cpu: Option<u32>,

    /// Pool jobs: run on a server with at least N GB of RAM. Same as --tag min-ram-gb=N.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u32).range(1..))]
    min_ram_gb: Option<u32>,

    /// Choice among free matching members.
    #[arg(
        long,
        value_enum,
        value_name = "STRATEGY",
        default_value = "first-free"
    )]
    strategy: HubStrategyArg,

    /// Job label for ps filters and stats, such as task=parser. Repeat for several.
    #[arg(long, value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
    label: Vec<String>,
}

#[derive(Debug, Args)]
#[command(
    mut_arg("target", |arg| arg.help("Pin placement to this hub member")),
    mut_arg("validate_only", |arg| arg.hide(true))
)]
pub(super) struct HubSubmitArgs {
    #[command(flatten)]
    placement: HubPlacementArgs,

    /// Wait for a free member, optionally up to DURATION such as 10m.
    #[arg(long, value_name = "DURATION", num_args = 0..=1, conflicts_with = "queue")]
    wait: Option<Option<String>>,

    /// Queue the job when every matching member is busy instead of failing.
    #[arg(long)]
    queue: bool,

    #[command(flatten)]
    run: RunArgs,
}

#[derive(Debug, Args)]
#[command(
    mut_arg("target", |arg| arg.help("Pin placement to this hub member")),
    mut_arg("validate_only", |arg| arg.hide(true)),
    mut_arg("detach", |arg| arg.hide(true))
)]
pub(super) struct HubEnqueueArgs {
    #[command(flatten)]
    placement: HubPlacementArgs,

    /// Not before this RFC 3339 time or relative +DURATION, such as +2h.
    #[arg(long, value_name = "TIME", conflicts_with = "cron")]
    at: Option<String>,

    /// Recur on this five-field cron expression (minute hour day-of-month month day-of-week),
    /// such as "0 3 * * 1-5". Each fire queues one job.
    #[arg(long, value_name = "EXPR")]
    cron: Option<String>,

    /// IANA time zone for --cron, such as Europe/Belgrade or UTC [default: this machine's zone].
    #[arg(long, value_name = "ZONE", requires = "cron")]
    cron_tz: Option<String>,

    /// Higher priority jobs are placed first.
    #[arg(
        long,
        value_name = "N",
        default_value_t = 0,
        allow_negative_numbers = true
    )]
    priority: i32,

    /// Expire the job if it is still queued after DURATION or at an RFC 3339 time. With --cron,
    /// each fired job expires DURATION after its fire time.
    #[arg(long, value_name = "DURATION|TIME")]
    expires: Option<String>,

    #[command(flatten)]
    run: RunArgs,
}

#[derive(Debug, Args)]
pub(super) struct HubPsArgs {
    /// Job state filter: active (queued, scheduled, running), queued, scheduled, running,
    /// succeeded, failed, dequeued, expired, recurring (schedules only), or all. Active
    /// schedules are listed with active and recurring; all schedules with all.
    #[arg(long, value_name = "STATE", default_value = "active")]
    state: String,

    /// Only jobs placed on this member.
    #[arg(long, value_name = "NAME")]
    target: Option<String>,

    /// Only jobs carrying this label. Repeat for several.
    #[arg(long, value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
    label: Vec<String>,

    /// Return at most this many of the newest matching jobs.
    #[arg(long, value_name = "N")]
    limit: Option<usize>,
}

#[derive(Debug, Args)]
pub(super) struct HubJobArgs {
    /// Hub job ID.
    #[arg(value_name = "JOB_ID")]
    job: String,
}

#[derive(Debug, Args)]
pub(super) struct HubWatchArgs {
    /// Hub job ID.
    #[arg(value_name = "JOB_ID")]
    job: String,

    /// Resume strictly after this durable cursor.
    #[arg(long, value_name = "CURSOR")]
    after: Option<String>,
}

#[derive(Debug, Args)]
pub(super) struct HubLogsArgs {
    /// Hub job ID.
    #[arg(value_name = "JOB_ID")]
    job: String,

    /// Resume strictly after this durable cursor.
    #[arg(long, value_name = "CURSOR")]
    after: Option<String>,

    /// Return records only for this opaque execution selector.
    #[arg(long, value_name = "EXECUTION_REF")]
    execution: Option<String>,
}

#[derive(Debug, Args)]
pub(super) struct HubAttachArgs {
    /// Hub job ID.
    #[arg(value_name = "JOB_ID")]
    job: String,

    /// Execution reference emitted by the run.
    #[arg(value_name = "EXECUTION_REF")]
    execution: String,
}

#[derive(Debug, Args)]
pub(super) struct HubStatsArgs {
    /// Group by target, model, project, day, or label:KEY. Repeat to combine.
    #[arg(long, value_name = "DIMENSION", action = clap::ArgAction::Append)]
    by: Vec<String>,

    /// Only jobs submitted at or after this RFC 3339 time or DURATION ago, such as 7d.
    #[arg(long, value_name = "TIME")]
    since: Option<String>,

    /// Only jobs submitted before this RFC 3339 time or DURATION ago.
    #[arg(long, value_name = "TIME")]
    until: Option<String>,

    /// JSON price table: {"MODEL": {"input": USD, "output": USD, "cacheRead": USD,
    /// "cacheCreation": USD}} per million tokens.
    #[arg(long, value_name = "FILE")]
    prices: Option<std::path::PathBuf>,
}

impl HubCliCommand {
    pub(super) fn into_command(self) -> Result<HubCommand, NativeV2CliError> {
        match self {
            Self::Add(args) => add_command(args),
            Self::Update(args) => update_command(args),
            Self::Remove(args) => Ok(HubCommand::Remove {
                name: member_name(args.name)?,
            }),
            Self::List(args) => Ok(HubCommand::List {
                tags: tags(args.tag)?,
                probe: !args.no_probe,
            }),
            Self::Submit(args) => Ok(HubCommand::Submit(Box::new(args.into_submit()?))),
            Self::Enqueue(args) => Ok(HubCommand::Enqueue(Box::new(args.into_enqueue()?))),
            Self::Ps(args) => ps_command(args),
            Self::Status(args) => Ok(HubCommand::Status { job: args.job }),
            Self::Watch(args) => Ok(HubCommand::Watch {
                job: args.job,
                after: args.after,
            }),
            Self::Logs(args) => Ok(HubCommand::Logs(HubLogs {
                job: args.job,
                after: args.after,
                execution: args.execution,
            })),
            Self::Attach(args) => Ok(HubCommand::Attach {
                job: args.job,
                execution: args.execution,
            }),
            Self::ForceStop(args) => Ok(HubCommand::ForceStop { job: args.job }),
            Self::Dequeue(args) => Ok(HubCommand::Dequeue { job: args.job }),
            Self::Dispatch => Ok(HubCommand::Dispatch),
            Self::Stats(args) => Ok(stats_command(args)),
            Self::Daemon(command) => daemon_command(command),
            Self::Context(command) => Ok(HubCommand::Context(command.into_command()?)),
            Self::Install(args) => install_command(args),
            Self::Uninstall(args) => Ok(HubCommand::Uninstall(HubUninstall {
                manager: manager(args.systemd),
                system: args.system,
                dry_run: args.dry_run,
            })),
            Self::Eval(command) => Ok(HubCommand::Eval(command.into_command()?)),
            Self::Pool(command) => Ok(HubCommand::Pool(command.into_command()?)),
        }
    }
}

fn add_command(args: HubAddArgs) -> Result<HubCommand, NativeV2CliError> {
    Ok(HubCommand::Add(HubMemberAdd {
        name: member_name(args.name)?,
        tags: tags(args.tag)?,
        slots: args.slots,
        priority: args.priority,
    }))
}

fn update_command(args: HubUpdateArgs) -> Result<HubCommand, NativeV2CliError> {
    Ok(HubCommand::Update(HubMemberUpdate {
        name: member_name(args.name)?,
        add_tags: tags(args.add_tag)?,
        remove_tags: tags(args.remove_tag)?,
        slots: args.slots,
        priority: args.priority,
        state: args.state.map(HubStateArg::into_state),
    }))
}

fn ps_command(args: HubPsArgs) -> Result<HubCommand, NativeV2CliError> {
    Ok(HubCommand::Ps(HubPs {
        state: args.state,
        target: args.target,
        labels: labels(args.label)?,
        limit: args.limit,
    }))
}

fn stats_command(args: HubStatsArgs) -> HubCommand {
    HubCommand::Stats(HubStats {
        by: args.by,
        since: args.since,
        until: args.until,
        prices: args.prices,
    })
}

fn daemon_command(command: HubDaemonCommand) -> Result<HubCommand, NativeV2CliError> {
    Ok(match command {
        HubDaemonCommand::Run(args) => HubCommand::Daemon(HubDaemonRun {
            interval: args.interval,
            api: args.api.into_options()?,
        }),
        HubDaemonCommand::Status => HubCommand::DaemonStatus,
    })
}

fn install_command(args: HubInstallArgs) -> Result<HubCommand, NativeV2CliError> {
    Ok(HubCommand::Install(HubInstall {
        manager: manager(args.systemd),
        system: args.system,
        image: args.image,
        volumes: args.volume,
        interval: args.interval,
        api: args.api.into_options()?,
        dry_run: args.dry_run,
    }))
}

impl HubContextCliCommand {
    fn into_command(self) -> Result<HubContextCommand, NativeV2CliError> {
        Ok(match self {
            Self::Add(args) => HubContextCommand::Add {
                name: context_name(args.name)?,
                url: args.url,
                token: match (args.token_file, args.token_env) {
                    (Some(file), _) => HubTokenSource::File(file),
                    (None, Some(variable)) => HubTokenSource::Env(variable),
                    (None, None) => return Err(usage("--token-file or --token-env is required")),
                },
            },
            Self::List => HubContextCommand::List,
            Self::Use(args) => HubContextCommand::Use {
                name: context_name(args.name)?,
            },
            Self::Remove(args) => HubContextCommand::Remove {
                name: context_name(args.name)?,
            },
        })
    }
}

fn context_name(name: String) -> Result<String, NativeV2CliError> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "-_.".contains(character))
    {
        return Err(usage(
            "hub context name must be 1-64 ASCII letters, digits, '-', '_', or '.'",
        ));
    }
    Ok(name)
}

const fn manager(systemd: bool) -> HubManager {
    if systemd {
        HubManager::Systemd
    } else {
        HubManager::Docker
    }
}

impl HubStateArg {
    const fn into_state(self) -> HubMemberState {
        match self {
            Self::Active => HubMemberState::Active,
            Self::Drain => HubMemberState::Drain,
            Self::Disabled => HubMemberState::Disabled,
        }
    }
}

impl HubSubmitArgs {
    fn into_submit(self) -> Result<HubSubmit, NativeV2CliError> {
        let detach = self.run.detach;
        Ok(HubSubmit {
            job: job_spec(self.run, self.placement)?,
            detach,
            wait: self.wait,
            queue: self.queue,
        })
    }
}

impl HubEnqueueArgs {
    fn into_enqueue(self) -> Result<HubEnqueue, NativeV2CliError> {
        if self.run.detach {
            return Err(usage(
                "hub enqueue always returns after acceptance; omit --detach",
            ));
        }
        Ok(HubEnqueue {
            job: job_spec(self.run, self.placement)?,
            at: self.at,
            cron: self.cron,
            cron_timezone: self.cron_tz,
            priority: self.priority,
            expires: self.expires,
        })
    }
}

fn job_spec(run: RunArgs, placement: HubPlacementArgs) -> Result<HubJobSpec, NativeV2CliError> {
    if run.validate_only {
        return Err(usage(
            "hub jobs cannot use --validate-only; validate with `zeroshot run --validate-only`",
        ));
    }
    let target = run.target.clone().map(member_name).transpose()?;
    let runtime_files = [&run.runtime_config, &run.uniform_runtime_config]
        .into_iter()
        .flatten()
        .cloned()
        .collect();
    Ok(HubJobSpec {
        title: run.title.clone(),
        run_args: run_arguments(&run)?,
        target,
        tags: tags(placement_tags(&placement))?,
        strategy: match placement.strategy {
            HubStrategyArg::FirstFree => HubStrategy::FirstFree,
            HubStrategyArg::LeastLoaded => HubStrategy::LeastLoaded,
        },
        labels: labels(placement.label)?,
        runtime_files,
        repository: run.repository.clone(),
        branch: run.branch.clone(),
        revision: run.revision.clone(),
        source_connection: run.source_connection.clone(),
        environment: run.environment.is_some(),
        cwd: run.cwd.clone(),
        submission_key: run.submission_key.clone(),
    })
}

/// The job's tags, with size requests for pool servers added as size tags.
fn placement_tags(placement: &HubPlacementArgs) -> Vec<String> {
    let mut tags = placement.tag.clone();
    tags.extend(
        placement
            .server_type
            .iter()
            .map(|name| format!("server-type={name}")),
    );
    tags.extend(placement.min_cpu.map(|cores| format!("min-cpu={cores}")));
    tags.extend(
        placement
            .min_ram_gb
            .map(|memory| format!("min-ram-gb={memory}")),
    );
    tags
}

/// Rebuild the `zeroshot run` arguments that do not depend on placement.
fn run_arguments(run: &RunArgs) -> Result<Vec<String>, NativeV2CliError> {
    let mut arguments = vec!["--title".to_owned(), run.title.clone()];
    arguments.extend(path_argument("--input", Some(&run.input))?);
    for (flag, path) in [
        ("--graph", &run.graph),
        ("--runtime-config", &run.runtime_config),
        ("--uniform-runtime-config", &run.uniform_runtime_config),
        ("--environment", &run.environment),
        ("--cwd", &run.cwd),
    ] {
        arguments.extend(path_argument(flag, path.as_ref())?);
    }
    if let Some(template) = run
        .template
        .and_then(|template| template.to_possible_value())
    {
        arguments.extend(["--template".to_owned(), template.get_name().to_owned()]);
    }
    for (flag, value) in [
        ("--profile", &run.profile),
        ("--source-connection", &run.source_connection),
        ("--repository", &run.repository),
        ("--branch", &run.branch),
        ("--revision", &run.revision),
        ("--delivery", &run.delivery.delivery),
    ] {
        if let Some(value) = value {
            arguments.extend([flag.to_owned(), value.clone()]);
        }
    }
    for (flag, enabled) in [
        ("--no-environment", run.no_environment),
        ("--push", run.delivery.push),
        ("--pr", run.delivery.pr),
        ("--ship", run.delivery.ship),
        ("--no-pr-feedback", run.delivery.no_pr_feedback),
    ] {
        if enabled {
            arguments.push(flag.to_owned());
        }
    }
    Ok(arguments)
}

fn path_argument(
    flag: &str,
    path: Option<&std::path::PathBuf>,
) -> Result<Vec<String>, NativeV2CliError> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let text = path
        .to_str()
        .ok_or_else(|| usage(format!("{flag} must be valid UTF-8 for hub jobs")))?;
    Ok(vec![flag.to_owned(), text.to_owned()])
}

fn member_name(name: String) -> Result<String, NativeV2CliError> {
    if name.is_empty() || name.chars().count() > 256 || name.chars().any(char::is_control) {
        return Err(usage(
            "hub member name must be 1..=256 non-control characters",
        ));
    }
    Ok(name)
}

fn tags(values: Vec<String>) -> Result<Vec<String>, NativeV2CliError> {
    for value in &values {
        if value.is_empty()
            || value.len() > 128
            || value
                .chars()
                .any(|character| character.is_whitespace() || character == ',')
        {
            return Err(usage(format!(
                "invalid tag {value:?}: use 1-128 characters without whitespace or commas"
            )));
        }
    }
    let mut unique = values;
    unique.sort();
    unique.dedup();
    Ok(unique)
}

fn labels(values: Vec<String>) -> Result<Vec<(String, String)>, NativeV2CliError> {
    values
        .into_iter()
        .map(|value| match value.split_once('=') {
            Some((key, label)) if !key.is_empty() && !key.contains(char::is_whitespace) => {
                Ok((key.to_owned(), label.to_owned()))
            }
            _ => Err(usage(format!("invalid label {value:?}: use KEY=VALUE"))),
        })
        .collect()
}

fn usage(message: impl Into<String>) -> NativeV2CliError {
    NativeV2CliError::Usage(message.into())
}
