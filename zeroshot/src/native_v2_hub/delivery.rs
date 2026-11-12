//! Did a terminal run's Git delivery actually produce a branch to merge?
//!
//! A run can end `succeeded` even though its delivery node was skipped or its managed branch
//! still points at the pinned revision. The hub therefore inspects every successful terminal
//! result whose job requested Git delivery (or whose receipt reports one) before calling the job
//! `succeeded`: a delivered branch must carry at least one commit that is not reachable from the
//! pin, and its head must not equal the pin.
//!
//! A real new commit can still leave no source change to review: an empty commit, or a commit
//! that only moves recorded dependency resolutions in a lockfile. That is not a failed delivery,
//! so the job stays `succeeded` and its terminal output carries `emptyDiff: true`. Everything
//! else is `delivery_empty`: the hub names the reason and records both revisions instead of
//! pretending the branch has something to merge.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

use super::model::Job;

const DELIVERY_EMPTY_REASON: &str = "delivery_empty";

/// Lockfiles whose only change is a dependency-resolution update, not a source change.
const LOCKFILES: &[&str] = &[
    "Cargo.lock",
    "Cargo.lock.msrv",
    "package-lock.json",
    "npm-shrinkwrap.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "bun.lock",
    "bun.lockb",
    "poetry.lock",
    "Pipfile.lock",
    "uv.lock",
    "Gemfile.lock",
    "composer.lock",
    "go.sum",
    "flake.lock",
    "deno.lock",
    "packages.lock.json",
    "Package.resolved",
    ".terraform.lock.hcl",
    "mix.lock",
    "pubspec.lock",
    "Podfile.lock",
    "gradle.lockfile",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryMode {
    Push,
    PullRequest,
    Merge,
}

impl DeliveryMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "push" => Some(Self::Push),
            "pull_request" | "pr" => Some(Self::PullRequest),
            "merge" | "ship" => Some(Self::Merge),
            _ => None,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Push => "push",
            Self::PullRequest => "pr",
            Self::Merge => "merge",
        }
    }
}

/// The delivery facts the hub publishes as a terminal job output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeliveryOutcome {
    pub mode: DeliveryMode,
    pub head_revision: Option<String>,
    pub pinned_revision: Option<String>,
    pub empty_diff: bool,
}

impl DeliveryOutcome {
    /// The terminal output a caller sees for a delivery job. `emptyDiff` is meaningful only on a
    /// successful delivery; a delivery that produced nothing keeps it false.
    pub(crate) fn view(&self) -> Value {
        json!({
            "mode": self.mode.name(),
            "headRevision": self.head_revision,
            "pinnedRevision": self.pinned_revision,
            "emptyDiff": self.empty_diff,
        })
    }

    pub(crate) fn failure_message(&self) -> String {
        match (&self.head_revision, &self.pinned_revision) {
            (Some(head), Some(pin)) if head == pin => format!(
                "the run succeeded but its delivery produced no commit: headRevision {head} == \
                 pinnedRevision {pin}"
            ),
            (Some(head), Some(pin)) => format!(
                "the run succeeded but its delivery produced no commit not reachable from the \
                 pin: headRevision {head}, pinnedRevision {pin}"
            ),
            (None, Some(pin)) => format!(
                "the run succeeded but its delivery produced no branch head (pinnedRevision {pin})"
            ),
            (Some(head), None) => format!(
                "the run succeeded but its delivery produced no verifiable new commit \
                 (headRevision {head})"
            ),
            (None, None) => {
                "the run succeeded but its delivery produced no branch head or pinned revision"
                    .to_owned()
            }
        }
    }
}

/// What the hub can say about a terminal run's delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Inspection {
    /// The run did not request Git delivery, or reported none.
    Absent,
    /// The delivered branch carries a new commit and may be handed to a caller.
    Landed(DeliveryOutcome),
    /// The delivery cannot be shown to have produced anything beyond the pin.
    Empty(DeliveryOutcome),
}

impl Inspection {
    pub(crate) fn outcome(&self) -> Option<&DeliveryOutcome> {
        match self {
            Self::Absent => None,
            Self::Landed(outcome) | Self::Empty(outcome) => Some(outcome),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty(_))
    }

    pub(crate) const fn reason(&self) -> Option<&'static str> {
        match self {
            Self::Empty(_) => Some(DELIVERY_EMPTY_REASON),
            Self::Absent | Self::Landed(_) => None,
        }
    }
}

/// Inspect a successful terminal result against the job's pinned revision.
///
/// The mode comes from the run's delivery receipt when it has one, otherwise from the run
/// arguments that asked for delivery. Only push, pull-request, and merge deliveries are checked;
/// comment delivery and runs without delivery keep their existing terminal outcome.
pub(crate) fn inspect(job: &Job, status: &Value) -> Inspection {
    let output = status.pointer("/status/terminalResult/output");
    // The supervisor accepts the delivery verifier's exact receipt anywhere in the graph's
    // terminal output, commonly under `{"delivery": receipt}`. Find that receipt object rather
    // than requiring the graph's top-level succeed bindings to flatten it, so a real delivery is
    // not mistaken for an absent one.
    let receipt = output.and_then(find_delivery_receipt);
    let reported = receipt
        .map(|(mode, _)| mode)
        .or_else(|| output.and_then(mode_field));
    let Some(mode) = reported.or_else(|| requested_mode(&job.run_args)) else {
        return Inspection::Absent;
    };
    let pin = pinned_revision(job);
    let head = receipt
        .and_then(|(_, receipt)| receipt.get("headRevision"))
        .or_else(|| output.and_then(|output| output.get("headRevision")))
        .and_then(Value::as_str)
        .filter(|head| !head.is_empty())
        .map(str::to_owned);
    let mut outcome = DeliveryOutcome {
        mode,
        head_revision: head.clone(),
        pinned_revision: pin.clone(),
        empty_diff: false,
    };
    let (Some(head), Some(pin)) = (head, pin) else {
        return if outcome.head_revision.is_none() {
            Inspection::Empty(outcome)
        } else {
            Inspection::Landed(outcome)
        };
    };
    if head == pin {
        return Inspection::Empty(outcome);
    }
    let workspace = working_folder(job);
    if commits_ahead(&workspace, &pin, &head) == Some(0) {
        return Inspection::Empty(outcome);
    }
    outcome.empty_diff = empty_diff(&workspace, &pin, &head)
        .or_else(|| {
            receipt
                .and_then(|(_, receipt)| receipt.get("emptyDiff"))
                .or_else(|| output.and_then(|output| output.get("emptyDiff")))
                .and_then(Value::as_bool)
        })
        .unwrap_or(false);
    Inspection::Landed(outcome)
}

/// The first object in a terminal output that is a Git delivery receipt: it carries a supported
/// `mode` and a `headRevision`. Arrays and nested records are searched in document order. This
/// mirrors the supervisor's exact-receipt tolerance without trusting an unrelated `headRevision`
/// from a review scope, which has no delivery `mode` beside it.
fn find_delivery_receipt(value: &Value) -> Option<(DeliveryMode, &Value)> {
    match value {
        Value::Object(fields) => {
            if let Some(mode) = mode_field(value)
                && fields.contains_key("headRevision")
            {
                return Some((mode, value));
            }
            fields.values().find_map(find_delivery_receipt)
        }
        Value::Array(values) => values.iter().find_map(find_delivery_receipt),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => None,
    }
}

fn mode_field(value: &Value) -> Option<DeliveryMode> {
    value
        .get("mode")
        .and_then(Value::as_str)
        .and_then(DeliveryMode::parse)
}

/// The delivery mode a `zeroshot run` invocation asked for, in its stored argument form.
fn requested_mode(arguments: &[String]) -> Option<DeliveryMode> {
    let mut mode = None;
    let mut cursor = 0;
    while let Some(argument) = arguments.get(cursor) {
        mode = match argument.as_str() {
            "--delivery" => arguments
                .get(cursor + 1)
                .and_then(|value| DeliveryMode::parse(value))
                .or(mode),
            "--push" => Some(DeliveryMode::Push),
            "--pr" => Some(DeliveryMode::PullRequest),
            "--ship" => Some(DeliveryMode::Merge),
            _ => mode,
        };
        cursor += 1;
    }
    mode
}

/// The revision the job was pinned to. An explicit `--revision` is not copied into
/// `pinnedRevision` when the job was created, so read it from the stored run arguments too.
fn pinned_revision(job: &Job) -> Option<String> {
    job.pinned_revision.clone().or_else(|| {
        let mut cursor = 0;
        while let Some(argument) = job.run_args.get(cursor) {
            if argument == "--revision" {
                return job
                    .run_args
                    .get(cursor + 1)
                    .filter(|revision| !revision.is_empty())
                    .cloned();
            }
            cursor += 1;
        }
        None
    })
}

/// The local checkout a job was submitted from. Named targets run their own copy, so commits
/// produced there may not exist here; the ancestry probe then reports `None` and the distinct
/// head remains the only evidence the hub has.
fn working_folder(job: &Job) -> PathBuf {
    let mut folder = job.cwd.clone();
    let mut cursor = 0;
    while let Some(argument) = job.run_args.get(cursor) {
        if argument == "--cwd"
            && let Some(path) = job.run_args.get(cursor + 1)
        {
            let path = Path::new(path);
            folder = if path.is_absolute() {
                path.to_path_buf()
            } else {
                job.cwd.join(path)
            };
        }
        cursor += 1;
    }
    folder
}

/// Commits reachable from `head` but not from `pin`. `None` means the hub cannot answer: the
/// checkout is unavailable, or the target produced a commit this hub has never fetched.
fn commits_ahead(workspace: &Path, pin: &str, head: &str) -> Option<u64> {
    let output = git(
        workspace,
        &["rev-list", "--count", &format!("{pin}..{head}")],
    )?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

/// Whether the delivered range changes no source file: no changed paths at all, or only
/// lockfiles. This is the rule that keeps a dependency-resolution commit a legitimate success
/// while a branch identical to the pin remains `delivery_empty`.
fn empty_diff(workspace: &Path, pin: &str, head: &str) -> Option<bool> {
    let output = git(
        workspace,
        &[
            "diff",
            "--name-only",
            "--no-renames",
            &format!("{pin}..{head}"),
        ],
    )?;
    if !output.status.success() {
        return None;
    }
    let names = String::from_utf8(output.stdout).ok()?;
    Some(names.lines().all(is_lockfile_or_empty))
}

fn is_lockfile_or_empty(path: &str) -> bool {
    let path = path.trim();
    if path.is_empty() {
        return true;
    }
    let name = path.rsplit('/').next().unwrap_or(path);
    LOCKFILES.contains(&name) || name.ends_with(".lock") || name.ends_with(".lockb")
}

fn git(workspace: &Path, arguments: &[&str]) -> Option<std::process::Output> {
    let mut command = Command::new("git");
    command.arg("-C").arg(workspace).args(arguments);
    // Repository-selection controls must never redirect the inspection away from the checkout
    // the job was submitted from. This mirrors the delivery transport's own environment cleanup.
    for variable in [
        "ZEROSHOT_API_KEY",
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_CEILING_DIRECTORIES",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
        "GIT_NAMESPACE",
    ] {
        command.env_remove(variable);
    }
    command.output().ok()
}
