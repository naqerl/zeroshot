//! `zeroshot hub eval`: capture tasks as cases, rerun them under variants through hub
//! placement, measure, judge, and compare.

use crate::native_v2_cli::hub::valid_eval_name;
use clap::{Args, Subcommand, ValueEnum};

use super::super::RunArgs;
use super::{HubPlacementArgs, job_spec, labels, member_name, tags, usage};
use crate::native_v2_cli::NativeV2CliError;
use crate::native_v2_cli::hub::{
    HubEvalCaseAdd, HubEvalCaseSource, HubEvalCommand, HubEvalReport, HubEvalRun, HubEvalVariant,
    HubJudgeMode,
};

/// Variant override keys and whether their value is a file uploaded with the run.
pub(crate) const VARIANT_KEYS: [(&str, bool); 10] = [
    ("graph", true),
    ("template", false),
    ("profile", false),
    ("runtime-config", true),
    ("uniform-runtime-config", true),
    ("environment", true),
    ("runtime-patch", true),
    ("model", false),
    ("harness", false),
    ("provider", false),
];

/// Report metrics whose improvement/regression threshold `--threshold` can change.
pub(crate) const THRESHOLD_METRICS: [&str; 5] =
    ["passRate", "judgeScore", "tokens", "costUsd", "durationMs"];

#[derive(Debug, Subcommand)]
#[command(after_long_help = r#"EVALS
A case snapshots one task: its run request (input, graph or template, runtime config, profile,
environment), the pinned source revision, placement, and labels. File contents are stored on the
hub by SHA-256, so a case ID (ec_...) is the hash of the snapshot and adding the same task twice
returns the same case. Suites group cases by name.

`hub eval run` enqueues every case x variant x repeat as an ordinary hub job, labeled eval.run,
eval.case, eval.variant, and eval.repeat, so placement, metrics, ps, and logs work unchanged. A
variant overrides parts of the case:
  --variant NAME=KEY=VALUE[,KEY=VALUE...]
  graph=FILE  template=NAME  profile=NAME  runtime-config=FILE  uniform-runtime-config=FILE
  environment=FILE  model=MODEL  harness=NAME  provider=NAME  runtime-patch=FILE
File overrides replace the case's file (graph and template replace each other, as do the two
runtime flags); model, harness, provider, and runtime-patch (a JSON merge patch) edit the
case's runtime file. The baseline is the variant named by --baseline: by default `baseline`,
the case exactly as captured. Use --baseline hj_JOB to reuse an existing job's result, or none.
`NAME=` with no overrides replays the case unchanged (an A/A control for noise).

The hub daemon measures each finished sample (outcome, duration, tokens, estimated cost),
runs --check commands, and calls the judge. Checks run with `sh -c` on the hub host in the
sample's work folder (status.json, job.json, case.json, outcome.json) with ZEROSHOT_EVAL_RUN,
_CASE, _VARIANT, _REPEAT, _JOB, _OUTCOME, _TARGET, _RUN_ID, _STATUS, _INPUTS, and _WORKDIR set;
exit 0 passes. Target workspaces stay on their targets: a check that needs the result checks
out the delivered branch or calls `zeroshot` (ZEROSHOT_HUB_EXECUTABLE) itself. Judges:
  cmd:SCRIPT      SCRIPT is uploaded and run on the hub host. stdin: a
                  zeroshot.hub.eval.judge-request/v1 document; stdout: {"score": N,
                  "verdict": "...", "rationale": "..."}. Absolute scores are 0..1; pairwise
                  scores are -1..1, positive when the variant beats the baseline.
  zeroshot:GRAPH  the judge itself runs as a hub job with GRAPH and the request as --input;
                  its terminal output must carry the same fields.

`hub eval report` compares each variant with the baseline per case and in aggregate: pass rate,
judge score mean with a 95% normal-approximation interval, tokens, cost, and duration. Deltas
past a threshold are flagged as improvements or regressions.

EXIT CODES (report)
  0  report written; no regression, or --fail-on-regression not given
  3  --fail-on-regression and the run is not finished yet (retryable)
  6  --fail-on-regression and a variant regressed

EXAMPLES
  zeroshot hub eval suite add parser --description "Parser fixes"
  zeroshot hub eval case add --suite parser --from-job hj_0123
  zeroshot hub eval run --suite parser --repeat 3 --variant fast=model=gpt-5-mini \
    --judge cmd:judge.py --check tests="cargo test" --prices prices.json --key nightly-42
  zeroshot hub eval report er_0123 --fail-on-regression --table"#)]
pub(in crate::native_v2_cli::parser) enum HubEvalCliCommand {
    /// Create or describe an eval suite.
    #[command(subcommand)]
    Suite(HubEvalSuiteCommand),

    /// Capture, list, or show eval cases.
    #[command(subcommand)]
    Case(HubEvalCaseCommand),

    /// Enqueue an eval run: every case x variant x repeat through hub placement.
    Run(Box<HubEvalRunArgs>),

    /// List eval runs as JSON.
    #[command(visible_alias = "ls")]
    List(HubEvalListArgs),

    /// Show one eval run with every sample's measurement and verdict as JSON.
    Status(HubEvalRunIdArgs),

    /// Compare variants with the baseline per case and in aggregate.
    Report(HubEvalReportArgs),
}

#[derive(Debug, Subcommand)]
pub(in crate::native_v2_cli::parser) enum HubEvalSuiteCommand {
    /// Create a suite, or change its description.
    Add(HubEvalSuiteAddArgs),
    /// List suites with their case counts as JSON.
    #[command(visible_alias = "ls")]
    List,
}

#[derive(Debug, Args)]
pub(in crate::native_v2_cli::parser) struct HubEvalSuiteAddArgs {
    /// Suite name: 1-64 ASCII letters, digits, '-', '_', or '.'.
    #[arg(value_name = "NAME")]
    name: String,

    /// Free-form description.
    #[arg(long, value_name = "TEXT")]
    description: Option<String>,
}

#[derive(Debug, Subcommand)]
pub(in crate::native_v2_cli::parser) enum HubEvalCaseCommand {
    /// Snapshot a task as a case, from a hub job (--from-job) or from run flags.
    Add(Box<HubEvalCaseAddArgs>),
    /// List cases as JSON.
    #[command(visible_alias = "ls")]
    List(HubEvalSuiteFilterArgs),
    /// Show one case with its run request and stored files as JSON.
    Show(HubEvalCaseIdArgs),
}

#[derive(Debug, Args)]
#[command(
    mut_arg("target", |arg| arg.help("Default placement: pin to this hub member")),
    mut_arg("validate_only", |arg| arg.hide(true)),
    mut_arg("detach", |arg| arg.hide(true)),
    mut_arg("title", |arg| arg.required(false).required_unless_present("from_job")),
    mut_arg("input", |arg| arg.required(false).required_unless_present("from_job")),
    mut_arg("task_from", |arg| {
        arg.required(false).required_unless_present("from_job")
    })
)]
pub(in crate::native_v2_cli::parser) struct HubEvalCaseAddArgs {
    // Declared first: clap hands fields their matches in order, and the run arguments must
    // still see --from-job to know they are absent.
    #[command(flatten)]
    run: CaseRunArgs,

    /// Hub job whose run request, files, source pin, placement, and labels become the case.
    #[arg(long, value_name = "JOB_ID", conflicts_with_all = RUN_ARGUMENT_IDS)]
    from_job: Option<String>,

    /// Add the case to this suite. Repeat for several.
    #[arg(long, value_name = "NAME", action = clap::ArgAction::Append)]
    suite: Vec<String>,

    /// Short case name for reports.
    #[arg(long, value_name = "NAME")]
    name: Option<String>,

    #[command(flatten)]
    placement: HubPlacementArgs,
}

/// `zeroshot run` argument IDs that describe a task, so they conflict with --from-job.
const RUN_ARGUMENT_IDS: [&str; 22] = [
    "title",
    "task_from",
    "graph",
    "template",
    "input",
    "runtime_config",
    "uniform_runtime_config",
    "environment",
    "no_environment",
    "profile",
    "cwd",
    "source_connection",
    "repository",
    "branch",
    "revision",
    "submission_key",
    "detach",
    "delivery",
    "push",
    "pr",
    "ship",
    "no_pr_feedback",
];

/// The run flags of `case add`, absent with --from-job. (`Option<RunArgs>` cannot be
/// flattened: clap mishandles optional groups that contain nested flattened groups.)
#[derive(Debug)]
struct CaseRunArgs(Option<RunArgs>);

impl clap::FromArgMatches for CaseRunArgs {
    fn from_arg_matches(matches: &clap::ArgMatches) -> Result<Self, clap::Error> {
        if matches.get_one::<String>("from_job").is_some() {
            return Ok(Self(None));
        }
        RunArgs::from_arg_matches(matches).map(|run| Self(Some(run)))
    }

    fn update_from_arg_matches(&mut self, matches: &clap::ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

impl Args for CaseRunArgs {
    fn augment_args(command: clap::Command) -> clap::Command {
        RunArgs::augment_args(command)
    }

    fn augment_args_for_update(command: clap::Command) -> clap::Command {
        RunArgs::augment_args_for_update(command)
    }
}

#[derive(Debug, Args)]
pub(in crate::native_v2_cli::parser) struct HubEvalSuiteFilterArgs {
    /// Only cases in this suite.
    #[arg(long, value_name = "NAME")]
    suite: Option<String>,
}

#[derive(Debug, Args)]
pub(in crate::native_v2_cli::parser) struct HubEvalCaseIdArgs {
    /// Case ID (ec_...).
    #[arg(value_name = "CASE_ID")]
    case: String,
}

#[derive(Debug, Args)]
pub(in crate::native_v2_cli::parser) struct HubEvalRunIdArgs {
    /// Eval run ID (er_...).
    #[arg(value_name = "RUN_ID")]
    run: String,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum HubJudgeModeArg {
    /// Score each sample on its own, 0..1.
    Absolute,
    /// Score each variant sample against the baseline sample of the same case and repeat.
    Pairwise,
}

#[derive(Debug, Args)]
#[command(group(clap::ArgGroup::new("cases").required(true).multiple(true).args(["suite", "case"])))]
pub(in crate::native_v2_cli::parser) struct HubEvalRunArgs {
    /// Run every case of this suite.
    #[arg(long, value_name = "NAME")]
    suite: Option<String>,

    /// Run this case. Repeat for several; combines with --suite.
    #[arg(long, value_name = "CASE_ID", action = clap::ArgAction::Append)]
    case: Vec<String>,

    /// Baseline: a variant name [default: baseline, the case as captured], a hub job ID whose
    /// result is the baseline sample, or none.
    #[arg(long, value_name = "VARIANT|JOB_ID|none")]
    baseline: Option<String>,

    /// Variant as NAME=KEY=VALUE[,KEY=VALUE...]; see EVALS. Repeat for several.
    #[arg(long, value_name = "NAME=OVERRIDES", action = clap::ArgAction::Append)]
    variant: Vec<String>,

    /// Samples per case and variant.
    #[arg(long, value_name = "N", default_value_t = 1,
          value_parser = clap::value_parser!(u32).range(1..=100))]
    repeat: u32,

    /// Pin every sample to this hub member instead of the case's placement.
    #[arg(long, value_name = "NAME")]
    target: Option<String>,

    /// Require this member tag instead of the case's tags. Repeat for several.
    #[arg(long, value_name = "TAG", action = clap::ArgAction::Append)]
    tag: Vec<String>,

    /// Queue priority of the samples.
    #[arg(
        long,
        value_name = "N",
        default_value_t = 0,
        allow_negative_numbers = true
    )]
    priority: i32,

    /// Judge: cmd:SCRIPT or zeroshot:GRAPH.
    #[arg(long, value_name = "cmd:SCRIPT|zeroshot:GRAPH")]
    judge: Option<String>,

    /// Absolute scores each sample; pairwise compares each variant sample with the baseline.
    #[arg(long, value_enum, value_name = "MODE", default_value = "absolute")]
    judge_mode: HubJudgeModeArg,

    /// Uniform runtime config of a zeroshot:GRAPH judge.
    #[arg(long, value_name = "FILE")]
    judge_runtime: Option<std::path::PathBuf>,

    /// Check NAME=COMMAND: a shell command run on the hub host for each finished sample; exit
    /// 0 passes. Repeat for several.
    #[arg(long, value_name = "NAME=COMMAND", action = clap::ArgAction::Append)]
    check: Vec<String>,

    /// JSON price table (as for `hub stats`) stored with the run for cost estimates.
    #[arg(long, value_name = "FILE")]
    prices: Option<std::path::PathBuf>,

    /// Idempotency key: repeating a request with the same key returns the same eval run.
    #[arg(long, value_name = "KEY")]
    key: Option<String>,
}

#[derive(Debug, Args)]
pub(in crate::native_v2_cli::parser) struct HubEvalListArgs {
    /// Only runs of this suite.
    #[arg(long, value_name = "NAME")]
    suite: Option<String>,

    /// Return at most this many of the newest runs.
    #[arg(long, value_name = "N")]
    limit: Option<usize>,
}

#[derive(Debug, Args)]
pub(in crate::native_v2_cli::parser) struct HubEvalReportArgs {
    /// Eval run ID (er_...).
    #[arg(value_name = "RUN_ID")]
    run: String,

    /// Compare with this variant of the run, or with er_ID[:VARIANT] of another run
    /// [default: the run's baseline].
    #[arg(long, value_name = "VARIANT|RUN_ID[:VARIANT]")]
    baseline: Option<String>,

    /// METRIC=VALUE threshold for flags, or METRIC=off. passRate and judgeScore are absolute
    /// differences [defaults 0.05]; tokens, costUsd, and durationMs are relative changes
    /// [defaults 0.10, 0.10, 0.25].
    #[arg(long, value_name = "METRIC=VALUE", action = clap::ArgAction::Append)]
    threshold: Vec<String>,

    /// JSON price table for cost estimates [default: the run's --prices].
    #[arg(long, value_name = "FILE")]
    prices: Option<std::path::PathBuf>,

    /// Exit 6 when a variant regressed, and 3 while the run is unfinished.
    #[arg(long)]
    fail_on_regression: bool,

    /// Wait until every sample is measured and judged, optionally up to DURATION.
    #[arg(long, value_name = "DURATION", num_args = 0..=1)]
    wait: Option<Option<String>>,

    /// Print a human-readable table instead of JSON.
    #[arg(long)]
    table: bool,
}

impl HubEvalCliCommand {
    pub(in crate::native_v2_cli::parser) fn into_command(
        self,
    ) -> Result<HubEvalCommand, NativeV2CliError> {
        Ok(match self {
            Self::Suite(HubEvalSuiteCommand::Add(args)) => HubEvalCommand::SuiteAdd {
                name: eval_name("suite", args.name)?,
                description: args.description,
            },
            Self::Suite(HubEvalSuiteCommand::List) => HubEvalCommand::SuiteList,
            Self::Case(HubEvalCaseCommand::Add(args)) => {
                HubEvalCommand::CaseAdd(Box::new(args.into_case()?))
            }
            Self::Case(HubEvalCaseCommand::List(args)) => HubEvalCommand::CaseList {
                suite: args
                    .suite
                    .map(|suite| eval_name("suite", suite))
                    .transpose()?,
            },
            Self::Case(HubEvalCaseCommand::Show(args)) => {
                HubEvalCommand::CaseShow { case: args.case }
            }
            Self::Run(args) => HubEvalCommand::Run(Box::new(args.into_run()?)),
            Self::List(args) => HubEvalCommand::List {
                suite: args.suite,
                limit: args.limit,
            },
            Self::Status(args) => HubEvalCommand::Status { run: args.run },
            Self::Report(args) => HubEvalCommand::Report(args.into_report()?),
        })
    }
}

impl HubEvalCaseAddArgs {
    fn into_case(self) -> Result<HubEvalCaseAdd, NativeV2CliError> {
        let suites = self
            .suite
            .into_iter()
            .map(|suite| eval_name("suite", suite))
            .collect::<Result<Vec<_>, _>>()?;
        let mut extra_labels = Vec::new();
        let source = match (self.from_job, self.run.0) {
            (Some(job), None) => {
                extra_labels = labels(self.placement.label)?;
                if !self.placement.tag.is_empty() {
                    return Err(usage(
                        "--tag with --from-job: the case keeps the job's placement; override it \
                         per eval run with --tag",
                    ));
                }
                HubEvalCaseSource::FromJob(job)
            }
            (None, Some(run)) => {
                if run.detach {
                    return Err(usage(
                        "hub eval case add never runs the case; omit --detach",
                    ));
                }
                HubEvalCaseSource::Flags(Box::new(job_spec(run, self.placement)?))
            }
            _ => {
                return Err(usage(
                    "hub eval case add needs --from-job JOB_ID or run flags (--title, --input, ...)",
                ));
            }
        };
        Ok(HubEvalCaseAdd {
            suites,
            name: self.name,
            labels: extra_labels,
            source,
        })
    }
}

impl HubEvalRunArgs {
    fn into_run(self) -> Result<HubEvalRun, NativeV2CliError> {
        let variants = self
            .variant
            .iter()
            .map(|variant| parse_variant(variant))
            .collect::<Result<Vec<_>, _>>()?;
        for (index, variant) in variants.iter().enumerate() {
            if variants[..index]
                .iter()
                .any(|other| other.name == variant.name)
            {
                return Err(usage(format!(
                    "variant {:?} is given more than once",
                    variant.name
                )));
            }
        }
        if let Some(judge) = &self.judge {
            if !(judge.starts_with("cmd:") || judge.starts_with("zeroshot:"))
                || judge
                    .split_once(':')
                    .is_some_and(|(_, path)| path.is_empty())
            {
                return Err(usage(format!(
                    "invalid --judge {judge:?}: use cmd:SCRIPT or zeroshot:GRAPH"
                )));
            }
        }
        if self.judge_runtime.is_some()
            && !self
                .judge
                .as_deref()
                .is_some_and(|judge| judge.starts_with("zeroshot:"))
        {
            return Err(usage("--judge-runtime needs --judge zeroshot:GRAPH"));
        }
        let judge_mode = match self.judge_mode {
            HubJudgeModeArg::Absolute => HubJudgeMode::Absolute,
            HubJudgeModeArg::Pairwise => HubJudgeMode::Pairwise,
        };
        if judge_mode == HubJudgeMode::Pairwise && self.baseline.as_deref() == Some("none") {
            return Err(usage("--judge-mode pairwise needs a baseline"));
        }
        let checks = self
            .check
            .into_iter()
            .map(|check| match check.split_once('=') {
                Some((name, command)) if valid_eval_name(name) && !command.trim().is_empty() => {
                    Ok((name.to_owned(), command.to_owned()))
                }
                _ => Err(usage(format!(
                    "invalid --check {check:?}: use NAME=COMMAND with a 1-64 character name"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(HubEvalRun {
            suite: self
                .suite
                .map(|suite| eval_name("suite", suite))
                .transpose()?,
            cases: self.case,
            baseline: self.baseline,
            variants,
            repeat: self.repeat,
            target: self.target.map(member_name).transpose()?,
            tags: tags(self.tag)?,
            priority: self.priority,
            judge: self.judge,
            judge_mode,
            judge_runtime: self.judge_runtime,
            checks,
            prices: self.prices,
            key: self.key,
        })
    }
}

impl HubEvalReportArgs {
    fn into_report(self) -> Result<HubEvalReport, NativeV2CliError> {
        let thresholds = labels(self.threshold)
            .map_err(|_| usage("invalid --threshold: use METRIC=VALUE or METRIC=off"))?;
        for (metric, value) in &thresholds {
            if !THRESHOLD_METRICS.contains(&metric.as_str()) {
                return Err(usage(format!(
                    "invalid --threshold metric {metric:?}: use one of {}",
                    THRESHOLD_METRICS.join(", ")
                )));
            }
            if value != "off" && value.parse::<f64>().map_or(true, |value| value < 0.0) {
                return Err(usage(format!(
                    "invalid --threshold {metric}={value}: use a non-negative number or off"
                )));
            }
        }
        Ok(HubEvalReport {
            run: self.run,
            baseline: self.baseline,
            thresholds,
            prices: self.prices,
            fail_on_regression: self.fail_on_regression,
            wait: self.wait,
            table: self.table,
        })
    }
}

/// `NAME=KEY=VALUE[,KEY=VALUE...]`; `NAME=` is a variant without overrides.
pub(crate) fn parse_variant(text: &str) -> Result<HubEvalVariant, NativeV2CliError> {
    let invalid = |reason: &str| {
        usage(format!(
            "invalid --variant {text:?}: {reason}; use NAME=KEY=VALUE[,KEY=VALUE...]"
        ))
    };
    let (name, overrides) = text.split_once('=').ok_or_else(|| invalid("missing '='"))?;
    if !valid_eval_name(name) || name == "none" || name.starts_with("hj_") {
        return Err(invalid(
            "the name must be 1-64 ASCII letters, digits, '-', '_', or '.', and not none or hj_*",
        ));
    }
    let mut parsed = Vec::new();
    for pair in overrides.split(',').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| invalid("each override is KEY=VALUE"))?;
        if !VARIANT_KEYS.iter().any(|(known, _)| *known == key) {
            return Err(invalid(&format!(
                "unknown key {key:?} (use {})",
                VARIANT_KEYS
                    .iter()
                    .map(|(key, _)| *key)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if value.is_empty() {
            return Err(invalid(&format!("{key} needs a value")));
        }
        parsed.push((key.to_owned(), value.to_owned()));
    }
    Ok(HubEvalVariant {
        name: name.to_owned(),
        overrides: parsed,
    })
}

fn eval_name(kind: &str, name: String) -> Result<String, NativeV2CliError> {
    if valid_eval_name(&name) {
        Ok(name)
    } else {
        Err(usage(format!(
            "{kind} name must be 1-64 ASCII letters, digits, '-', '_', or '.'"
        )))
    }
}
