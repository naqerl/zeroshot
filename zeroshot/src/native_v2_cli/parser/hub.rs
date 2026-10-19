use clap::{Args, Subcommand, ValueEnum};

use super::RunArgs;
use crate::native_v2_cli::NativeV2CliError;
use crate::native_v2_cli::hub::{
    HubCommand, HubEnqueue, HubInstall, HubJobSpec, HubLogs, HubManager, HubMemberAdd,
    HubMemberState, HubMemberUpdate, HubPs, HubStats, HubStrategy, HubSubmit, HubUninstall,
};

#[derive(Debug, Subcommand)]
#[command(after_long_help = r#"HUB
A hub pools targets registered with `zeroshot target add`, plus the built-in `local` controller.
It picks a target for each run and keeps queued, scheduled, and recurring runs in shared state
beside the target registry. Every placed job is one ordinary run on exactly one target, so
`zeroshot status RUN_ID --target NAME` keeps working.

DAEMON
A hub daemon dispatches continuously: it fires cron schedules, places due jobs, polls running
jobs to completion, and records token usage. Install it once with `zeroshot hub install
--systemd` (user unit; --system for a system unit) or `--docker` (restarting container). Every
other hub command first checks that exactly one daemon is active under systemd or Docker, and
fails with hub.daemon_not_running or hub.daemon_conflict otherwise. `hub daemon status` shows
what was detected; `hub daemon run` is the foreground entrypoint the services start.

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
  5  hub.daemon_not_running: no hub daemon is active under systemd or Docker
  6  hub.daemon_conflict: hub daemons are active under both systemd and Docker

EXAMPLES
  zeroshot hub install --systemd
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

    /// Remove a queued or scheduled job before it is placed, or a recurring schedule (`hs_…`).
    ///
    /// Removing a schedule keeps the jobs it already queued.
    Dequeue(HubJobArgs),

    /// Run one dispatcher pass now and print its summary, for debugging.
    ///
    /// The daemon runs the same pass continuously; a manual pass is safe beside it because both
    /// hold the hub lock.
    Dispatch,

    /// Aggregate job outcomes and provider-reported tokens as JSON.
    ///
    /// A job is attributed to one model only when its runtime files name exactly one model;
    /// otherwise its model is `mixed` or `unknown`. Costs appear only with --prices.
    Stats(HubStatsArgs),

    /// Run or inspect the hub daemon.
    #[command(subcommand)]
    Daemon(HubDaemonCommand),

    /// Install the hub daemon as a systemd unit or a Docker container and start it.
    ///
    /// The service runs `zeroshot hub daemon run` with this shell's config, hub, and state
    /// folders pinned, so it sees the same targets and queue. --dry-run prints the generated
    /// unit or container command and the steps without changing anything.
    Install(HubInstallArgs),

    /// Stop and remove the hub daemon service. Hub state is kept.
    Uninstall(HubUninstallArgs),
}

#[derive(Debug, Subcommand)]
pub(super) enum HubDaemonCommand {
    /// Dispatch in the foreground until SIGTERM or Ctrl-C; the entrypoint of installed services.
    ///
    /// Each pass fires due cron schedules, refreshes capabilities and running jobs, expires stale
    /// jobs, and places due ones. Passes with activity are written as NDJSON. Only one daemon may
    /// own a hub folder at a time.
    Run(HubDaemonRunArgs),

    /// Report the detected systemd and Docker daemons and the last heartbeat as JSON.
    ///
    /// Exits 0 when exactly one daemon is active, 5 when none is, and 6 when both are.
    Status,
}

#[derive(Debug, Args)]
pub(super) struct HubDaemonRunArgs {
    /// Pause between dispatcher passes.
    #[arg(long, value_name = "DURATION", default_value = "10s")]
    interval: String,
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
    #[arg(long, requires = "systemd")]
    system: bool,

    /// docker: image with a zeroshot binary that includes `hub`.
    #[arg(long, value_name = "IMAGE", requires = "docker")]
    image: Option<String>,

    /// docker: also mount this host folder at the same path, such as a job's working folder.
    #[arg(long, value_name = "PATH", requires = "docker", action = clap::ArgAction::Append)]
    volume: Vec<std::path::PathBuf>,

    /// Pause between dispatcher passes.
    #[arg(long, value_name = "DURATION", default_value = "10s")]
    interval: String,

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
    #[arg(long, requires = "systemd")]
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
        Ok(match self {
            Self::Add(args) => HubCommand::Add(HubMemberAdd {
                name: member_name(args.name)?,
                tags: tags(args.tag)?,
                slots: args.slots,
                priority: args.priority,
            }),
            Self::Update(args) => HubCommand::Update(HubMemberUpdate {
                name: member_name(args.name)?,
                add_tags: tags(args.add_tag)?,
                remove_tags: tags(args.remove_tag)?,
                slots: args.slots,
                priority: args.priority,
                state: args.state.map(HubStateArg::into_state),
            }),
            Self::Remove(args) => HubCommand::Remove {
                name: member_name(args.name)?,
            },
            Self::List(args) => HubCommand::List {
                tags: tags(args.tag)?,
                probe: !args.no_probe,
            },
            Self::Submit(args) => HubCommand::Submit(Box::new(args.into_submit()?)),
            Self::Enqueue(args) => HubCommand::Enqueue(Box::new(args.into_enqueue()?)),
            Self::Ps(args) => HubCommand::Ps(HubPs {
                state: args.state,
                target: args.target,
                labels: labels(args.label)?,
                limit: args.limit,
            }),
            Self::Status(args) => HubCommand::Status { job: args.job },
            Self::Watch(args) => HubCommand::Watch {
                job: args.job,
                after: args.after,
            },
            Self::Logs(args) => HubCommand::Logs(HubLogs {
                job: args.job,
                after: args.after,
                execution: args.execution,
            }),
            Self::Attach(args) => HubCommand::Attach {
                job: args.job,
                execution: args.execution,
            },
            Self::ForceStop(args) => HubCommand::ForceStop { job: args.job },
            Self::Dequeue(args) => HubCommand::Dequeue { job: args.job },
            Self::Dispatch => HubCommand::Dispatch,
            Self::Stats(args) => HubCommand::Stats(HubStats {
                by: args.by,
                since: args.since,
                until: args.until,
                prices: args.prices,
            }),
            Self::Daemon(HubDaemonCommand::Run(args)) => HubCommand::Daemon {
                interval: args.interval,
            },
            Self::Daemon(HubDaemonCommand::Status) => HubCommand::DaemonStatus,
            Self::Install(args) => HubCommand::Install(HubInstall {
                manager: manager(args.systemd),
                system: args.system,
                image: args.image,
                volumes: args.volume,
                interval: args.interval,
                dry_run: args.dry_run,
            }),
            Self::Uninstall(args) => HubCommand::Uninstall(HubUninstall {
                manager: manager(args.systemd),
                system: args.system,
                dry_run: args.dry_run,
            }),
        })
    }
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
        tags: tags(placement.tag)?,
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
