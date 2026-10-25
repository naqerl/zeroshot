use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::{HubMemberState, HubStrategy};

use super::capacity::Machine;
use super::clock::rfc3339;
use super::eval::EvalState;
use super::pool::{Lease, Pool};
use super::usage::JobUsage;

/// Reserved member name for the local controller.
pub(crate) const LOCAL_MEMBER: &str = "local";
/// Version of `state.json` this hub writes. Version 2 added pools, leases and job archives;
/// [`super::store`] upgrades version 1 files on load and refuses newer ones.
pub(crate) const STATE_VERSION: u32 = 2;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct HubState {
    pub version: u32,
    #[serde(default)]
    pub members: Vec<Member>,
    #[serde(default)]
    pub jobs: Vec<Job>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schedules: Vec<Schedule>,
    #[serde(default, skip_serializing_if = "EvalState::is_empty")]
    pub evals: EvalState,
    /// Random ID labelling every cloud resource this hub creates; set with the first pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hub_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pools: Vec<Pool>,
    /// Pool servers from creation until their deletion is confirmed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub leases: Vec<Lease>,
}

impl Default for HubState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            members: Vec::new(),
            jobs: Vec::new(),
            schedules: Vec::new(),
            evals: EvalState::default(),
            hub_id: None,
            pools: Vec::new(),
            leases: Vec::new(),
        }
    }
}

impl HubState {
    pub(crate) fn member(&self, name: &str) -> Option<&Member> {
        self.members.iter().find(|member| member.name == name)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Member {
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub slots: u32,
    #[serde(default)]
    pub priority: i32,
    pub state: MemberState,
    pub added_at_ms: u64,
    /// Discovery capability kinds last advertised by the target; `None` until first discovered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_error: Option<String>,
    /// Pool that created this member's server; `None` for registered members.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// Provider server ID of a pool member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_id: Option<u64>,
    /// When a pool member last became idle; `None` while it runs hub jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_since_ms: Option<u64>,
    /// When a pool member's lease expires and it is torn down.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    /// Server type, location and size of a pool member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<Machine>,
}

impl Member {
    /// An active, untagged, single-slot member; callers set tags, slots, and priority.
    pub(crate) const fn new(name: String, now_ms: u64) -> Self {
        Self {
            name,
            tags: Vec::new(),
            slots: 1,
            priority: 0,
            state: MemberState::Active,
            added_at_ms: now_ms,
            capabilities: None,
            capabilities_at_ms: None,
            capability_error: None,
            pool: None,
            server_id: None,
            idle_since_ms: None,
            expires_at_ms: None,
            machine: None,
        }
    }

    pub(crate) fn view(&self) -> Value {
        json!({
            "name": self.name,
            "tags": self.tags,
            "slots": self.slots,
            "priority": self.priority,
            "state": self.state,
            "addedAt": rfc3339(self.added_at_ms),
            "capabilities": self.capabilities,
            "capabilitiesAt": self.capabilities_at_ms.map(rfc3339),
            "capabilityError": self.capability_error,
            "pool": self.pool,
            "serverId": self.server_id,
            "idleSince": self.idle_since_ms.map(rfc3339),
            "expiresAt": self.expires_at_ms.map(rfc3339),
            "machine": self.machine.as_ref().map(Machine::view),
        })
    }

    /// Whether the member carries a tag, or for a size tag, runs on a big enough server.
    pub(crate) fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|own| own == tag)
            || self
                .machine
                .as_ref()
                .is_some_and(|machine| machine.meets(tag))
    }
}

/// A recurring `enqueue --cron`: each fire queues a copy of `template`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Schedule {
    pub schedule_id: String,
    pub state: ScheduleState,
    pub cron: String,
    /// IANA zone the cron fields are evaluated in.
    pub timezone: String,
    pub template: Job,
    /// Folder whose `HEAD` is re-pinned at each fire, when no explicit source was given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repin_from: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_after_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_key: Option<String>,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fired_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_job_id: Option<String>,
    #[serde(default)]
    pub fired: u64,
    #[serde(default)]
    pub skipped: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScheduleState {
    Active,
    Removed,
}

/// What a job needs from the member that runs it, resolved once at acceptance.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Requirements {
    /// Discovery capability kinds a named target must advertise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    /// Why the local controller cannot run this job, if it cannot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_unsupported: Option<String>,
    /// Why named targets cannot run this job, if they cannot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_unsupported: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemberState {
    Active,
    Drain,
    Disabled,
}

impl From<HubMemberState> for MemberState {
    fn from(state: HubMemberState) -> Self {
        match state {
            HubMemberState::Active => Self::Active,
            HubMemberState::Drain => Self::Drain,
            HubMemberState::Disabled => Self::Disabled,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Strategy {
    #[default]
    FirstFree,
    LeastLoaded,
}

impl From<HubStrategy> for Strategy {
    fn from(strategy: HubStrategy) -> Self {
        match strategy {
            HubStrategy::FirstFree => Self::FirstFree,
            HubStrategy::LeastLoaded => Self::LeastLoaded,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct PlacementSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub strategy: Strategy,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Dequeued,
    Expired,
}

impl JobState {
    pub(crate) const fn is_terminal(self) -> bool {
        !matches!(self, Self::Queued | Self::Running)
    }
}

/// What kind of problem the hub had with a job.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobErrorKind {
    /// The target refused the submission; the job failed without a run.
    SubmissionRejected,
    /// The target did not confirm the submission; it is retried with the same key.
    SubmissionUnconfirmed,
    /// The last status read of the job's run failed; the next pass reads it again.
    StatusUnavailable,
    /// The target left too many submissions unconfirmed; the job failed, though a run with
    /// its last submission key may still exist there.
    UnconfirmedLimit,
    /// Dequeued while placing. A run with its submission key may exist on the member; the hub
    /// does not follow or adopt it.
    DequeuedWhilePlacing,
    /// Waited longer than its pool's `--wait-max` for a server; the job expired.
    PoolWaitTimeout,
    /// Recorded as plain text by an earlier hub.
    Unclassified,
}

/// A hub-side problem with a job. Earlier hubs stored only the message; such records still
/// load, as [`JobErrorKind::Unclassified`].
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(from = "StoredJobError")]
pub(crate) struct JobError {
    pub kind: JobErrorKind,
    pub message: String,
}

impl JobError {
    pub(crate) fn new(kind: JobErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredJobError {
    Message(String),
    Typed { kind: JobErrorKind, message: String },
}

impl From<StoredJobError> for JobError {
    fn from(stored: StoredJobError) -> Self {
        match stored {
            StoredJobError::Message(message) => Self::new(JobErrorKind::Unclassified, message),
            StoredJobError::Typed { kind, message } => Self::new(kind, message),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Job {
    pub job_id: String,
    pub state: JobState,
    pub title: String,
    pub run_args: Vec<String>,
    /// Invoking folder; relative run arguments resolve from here.
    pub cwd: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_revision: Option<String>,
    pub placement: PlacementSpec,
    #[serde(default)]
    pub requirements: Requirements,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_key: Option<String>,
    pub submitted_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    #[serde(default)]
    pub attempts: u32,
    /// Submissions of the current attempt its target did not confirm; apart from `attempts`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub unconfirmed_submissions: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<JobUsage>,
    /// The hub's own problem with this job, apart from its run's `failure_reason`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JobError>,
    /// Recurring schedule that created this job, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_id: Option<String>,
    /// Terminal result output, kept only for eval judge jobs (`eval.role=judge`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Folder holding the run's report, status, watch replay and logs, saved before the pool
    /// server that ran it was deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive: Option<PathBuf>,
    /// `authoredGraphHash` from the archived run report, when the target reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_hash: Option<String>,
    /// Why a queued job its pool serves has no server yet, such as a capacity shortage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_wait: Option<String>,
}

impl Job {
    /// Placed on a member that has not confirmed a run yet: stored as running without a run
    /// ID, so state files of earlier hubs read the same.
    pub(crate) fn is_placing(&self) -> bool {
        self.state == JobState::Running && self.run_id.is_none()
    }

    /// Public state, distinguishing future queued jobs as `scheduled`.
    pub(crate) fn display_state(&self, now_ms: u64) -> &'static str {
        match self.state {
            JobState::Queued if self.not_before_ms.is_some_and(|at| at > now_ms) => "scheduled",
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Dequeued => "dequeued",
            JobState::Expired => "expired",
        }
    }

    /// Model attribution for stats: one named model, `mixed`, or `unknown`.
    pub(crate) fn model_key(&self) -> String {
        match self.models.as_slice() {
            [] => "unknown".to_owned(),
            [model] => model.clone(),
            _ => "mixed".to_owned(),
        }
    }

    pub(crate) fn view(&self, now_ms: u64) -> Value {
        let time = |value: Option<u64>| value.map(rfc3339);
        json!({
            "jobId": self.job_id,
            "state": self.display_state(now_ms),
            "title": self.title,
            "target": self.target,
            "runId": self.run_id,
            "placement": self.placement,
            "requirements": self.requirements,
            "scheduleId": self.schedule_id,
            "priority": self.priority,
            "labels": self.labels,
            "project": self.project,
            "models": self.models,
            "pinnedRevision": self.pinned_revision,
            "attempts": self.attempts,
            "unconfirmedSubmissions": self.unconfirmed_submissions,
            "submittedAt": rfc3339(self.submitted_at_ms),
            "notBefore": time(self.not_before_ms),
            "expiresAt": time(self.expires_at_ms),
            "dispatchedAt": time(self.dispatched_at_ms),
            "finishedAt": time(self.finished_at_ms),
            "failureReason": self.failure_reason,
            "tokenUsage": self.token_usage,
            "error": self.error.as_ref().map(|error| &error.message),
            "errorKind": self.error.as_ref().map(|error| error.kind),
            "archive": self.archive,
            "graphHash": self.graph_hash,
            "waiting": self.pool_wait,
        })
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u32) -> bool {
    *value == 0
}
