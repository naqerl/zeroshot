use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::future::join_all;
use serde_json::{Value, json};

use super::driver::{HubDriver, SubmitRequest};
use super::error::HubError;
use super::model::{
    HubState, Job, JobActivity, JobError, JobErrorKind, JobState, LOCAL_MEMBER, Member, MemberState,
    QuotaWait,
};
use super::usage::JobUsage;
use super::placement::{Load, Placement, Reachability, place};
use super::pool;
use super::quota;
#[cfg(test)]
use super::pool::PoolPhase;

const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
/// Consecutive *decisive* unreachable member probes, one per dispatcher interval, after which a
/// job placed on that member is considered for `target_lost`. A single failed probe, an
/// inconclusive probe, or a member that flaps below this threshold keeps the job running.
pub(crate) const UNREACHABLE_REAP_THRESHOLD: u32 = 3;
/// Discovered capabilities are refreshed after this age by placement passes.
pub(crate) const CAPABILITY_TTL_MS: u64 = 5 * 60 * 1000;
/// A failed discovery backs off from one minute, doubling to half an hour, so a dead member is
/// not probed on every pass while it stays registered.
const CAPABILITY_RETRY_BASE_MS: u64 = 60 * 1000;
const CAPABILITY_RETRY_MAX_MS: u64 = 30 * 60 * 1000;

/// Outcome of one dispatcher pass.
#[derive(Debug, Default)]
pub(crate) struct TickReport {
    pub fired: Vec<Value>,
    pub dispatched: Vec<Value>,
    pub finished: Vec<Value>,
    pub stalled: Vec<Value>,
    pub target_lost: Vec<Value>,
    pub failed: Vec<Value>,
    pub unconfirmed: Vec<Value>,
    pub resumed: Vec<Value>,
    pub loads: Option<BTreeMap<String, Load>>,
}

impl TickReport {
    pub(crate) fn view(&self, state: &HubState, now_ms: u64) -> Value {
        let queued = state
            .jobs
            .iter()
            .filter(|job| job.state == JobState::Queued)
            .count();
        let running = state
            .jobs
            .iter()
            .filter(|job| job.state == JobState::Running)
            .count();
        let schedules = state
            .schedules
            .iter()
            .filter(|schedule| schedule.state == super::model::ScheduleState::Active)
            .count();
        json!({
            "schema": "zeroshot.hub.tick/v1",
            "schedules": schedules,
            "at": super::clock::rfc3339(now_ms),
            "fired": self.fired,
            "dispatched": self.dispatched,
            "finished": self.finished,
            "stalled": self.stalled,
            "targetLost": self.target_lost,
            "dispatchFailed": self.failed,
            "dispatchUnconfirmed": self.unconfirmed,
            "resumed": self.resumed,
            "queued": queued,
            "running": running,
        })
    }
}

/// Count nonterminal runs from a `zeroshot list` result.
pub(crate) fn active_runs(runs: &[Value]) -> u32 {
    let active = runs
        .iter()
        .filter(|run| {
            run.pointer("/status/phase")
                .and_then(Value::as_str)
                .is_some_and(|phase| phase != "finished")
        })
        .count();
    u32::try_from(active).unwrap_or(u32::MAX)
}

/// Probe members in parallel. Load is the larger of the observed active runs and the hub's own
/// running jobs on that member, so a target that lags behind admission is not oversubscribed.
pub(crate) async fn probe(
    members: &[&Member],
    jobs: &[Job],
    driver: &dyn HubDriver,
) -> BTreeMap<String, Load> {
    let probes = members.iter().map(|member| async move {
        let result = tokio::time::timeout(PROBE_TIMEOUT, driver.list_runs(&member.name)).await;
        let load = match result {
            Ok(Ok(runs)) => Load {
                reachability: Reachability::Reachable,
                active: active_runs(&runs),
                error: None,
            },
            Ok(Err(error)) => Load {
                reachability: classify_probe_error(&error),
                active: 0,
                error: Some(error.to_string()),
            },
            Err(_) => Load {
                reachability: Reachability::Inconclusive,
                active: 0,
                error: Some("probe timed out".to_owned()),
            },
        };
        (member.name.clone(), load)
    });
    let mut loads = join_all(probes)
        .await
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    for job in jobs
        .iter()
        .filter(|job| matches!(job.state, JobState::Running | JobState::WaitingQuota))
    {
        if let Some(load) = job.target.as_ref().and_then(|target| loads.get_mut(target)) {
            let own = u32::try_from(
                jobs.iter()
                    .filter(|other| {
                        matches!(other.state, JobState::Running | JobState::WaitingQuota)
                            && other.target == job.target
                    })
                    .count(),
            )
            .unwrap_or(u32::MAX);
            load.active = load.active.max(own);
        }
    }
    loads
}

/// Classify a failed probe. Only decisive transport evidence may count toward a member's
/// failure streak; a timeout, rate limit, busy target, or rejected credential is inconclusive,
/// since it says nothing about whether the member is still there.
pub(crate) fn classify_probe_error(error: &HubError) -> Reachability {
    let text = error.to_string().to_ascii_lowercase();
    if [
        "timed out",
        "timeout",
        "rate limit",
        "rate_limit",
        "rate_limited",
        "too many requests",
        " 429",
        "busy",
        "connection reset",
        "connection aborted",
        "temporarily unavailable",
        "status_unavailable",
        "authentication",
        "unauthorized",
        "forbidden",
        "permission denied",
    ]
    .iter()
    .any(|needle| text.contains(needle))
    {
        return Reachability::Inconclusive;
    }
    if [
        "connection refused",
        "no route to host",
        "unknown host",
        "name or service not known",
        "nodename nor servname",
    ]
    .iter()
    .any(|needle| text.contains(needle))
    {
        return Reachability::Unreachable;
    }
    // Anything else is not proof: keep the job running until a decisive signal, the provider,
    // or a direct endpoint check confirms the member is gone.
    Reachability::Inconclusive
}

/// Record load probes on members and on the jobs placed there. A successful probe resets the
/// member's failure streak and refreshes the last success on the member and its running jobs;
/// only decisive failures extend the streak.
pub(crate) fn record_health(
    members: &mut [Member],
    jobs: &mut [Job],
    loads: &BTreeMap<String, Load>,
    now_ms: u64,
) {
    for member in members {
        let Some(load) = loads.get(&member.name) else {
            continue;
        };
        match load.reachability {
            Reachability::Reachable => {
                member.unreachable_probes = 0;
                member.last_probe_ok_at_ms = Some(now_ms);
            }
            Reachability::Unreachable => {
                member.unreachable_probes = member.unreachable_probes.saturating_add(1);
            }
            Reachability::Inconclusive => {}
        }
    }
    for job in jobs
        .iter_mut()
        .filter(|job| matches!(job.state, JobState::Running | JobState::WaitingQuota))
    {
        let Some(target) = job.target.as_deref() else {
            continue;
        };
        if loads.get(target).is_some_and(Load::reachable) {
            job.target_last_ok_at_ms = Some(now_ms);
            // A successful load probe says nothing about a failed status read. Keep that
            // retryable warning until a later status read succeeds and clears it.
            if !job
                .error
                .as_ref()
                .is_some_and(|error| error.kind == JobErrorKind::StatusUnavailable)
            {
                job.reaper = None;
            }
        }
    }
}

/// A completed `status` read is direct proof that the member is alive and still serving this
/// job, so it must clear the same failure streak a successful load probe clears. Without this,
/// a member that answers status but not `list` is reaped while the hub is actively observing it.
pub(crate) fn record_target_health(state: &mut HubState, target: &str, now_ms: u64) {
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.name == target)
    {
        member.unreachable_probes = 0;
        member.last_probe_ok_at_ms = Some(now_ms);
    }
    for job in state.jobs.iter_mut().filter(|job| {
        matches!(job.state, JobState::Running | JobState::WaitingQuota)
            && job.target.as_deref() == Some(target)
    }) {
        job.target_last_ok_at_ms = Some(now_ms);
        if !job
            .error
            .as_ref()
            .is_some_and(|error| error.kind == JobErrorKind::StatusUnavailable)
        {
            job.reaper = None;
        }
    }
}

/// A running job whose member left the hub or failed too many decisive probes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LostTarget {
    pub job_id: String,
    pub member: String,
    /// Consecutive decisive failures; `None` when the member is no longer a hub member.
    pub unreachable_probes: Option<u32>,
    pub last_probe_ok_at_ms: Option<u64>,
    /// The deciding confirmation, such as a provider saying the pool server is gone.
    pub evidence: Option<String>,
    /// Which observation failed the job: `member_absent`, `provider_confirmed_absent`,
    /// `endpoint_refused`, or `probe_unreachable`. Set with `evidence`, and recorded on the job
    /// so its terminal record says what the hub actually saw.
    pub classification: Option<&'static str>,
}

/// The decisive observations that fail a lost-target job by themselves.
pub(crate) const CLASSIFIED_MEMBER_ABSENT: &str = "member_absent";
pub(crate) const CLASSIFIED_PROVIDER_ABSENT: &str = "provider_confirmed_absent";
pub(crate) const CLASSIFIED_ENDPOINT_REFUSED: &str = "endpoint_refused";
/// A bounded wait retired a lease whose provider server still existed.
pub(crate) const CLASSIFIED_LEASE_RETIRED: &str = "lease_retired";
pub(crate) const CLASSIFIED_STALLED: &str = "stalled";

/// A lost-target candidate the reaper kept running, with the reason it withheld action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WithheldTarget {
    pub job_id: String,
    pub member: String,
    pub reason: String,
}

/// Running or quota-paused jobs that can no longer be observed because their member is gone or
/// unreachable. A quota-paused job cannot resume against a member that no longer exists, so it
/// must settle as lost rather than wait forever.
pub(crate) fn lost_targets(state: &HubState) -> Vec<LostTarget> {
    state
        .jobs
        .iter()
        .filter(|job| matches!(job.state, JobState::Running | JobState::WaitingQuota))
        .filter_map(|job| {
            let name = job.target.as_deref()?;
            match state.member(name) {
                None => Some(LostTarget {
                    job_id: job.job_id.clone(),
                    member: name.to_owned(),
                    unreachable_probes: None,
                    last_probe_ok_at_ms: job.target_last_ok_at_ms,
                    evidence: None,
                    classification: Some(CLASSIFIED_MEMBER_ABSENT),
                }),
                Some(member) if member.unreachable_probes >= UNREACHABLE_REAP_THRESHOLD => {
                    Some(LostTarget {
                        job_id: job.job_id.clone(),
                        member: member.name.clone(),
                        unreachable_probes: Some(member.unreachable_probes),
                        last_probe_ok_at_ms: member
                            .last_probe_ok_at_ms
                            .or(job.target_last_ok_at_ms),
                        evidence: None,
                        classification: None,
                    })
                }
                Some(_) => None,
            }
        })
        .collect()
}

/// Fail lost-target jobs, naming the member and the last successful probe. Returns summaries.
pub(crate) fn settle_lost_targets(
    jobs: &mut [Job],
    now_ms: u64,
    lost: &[LostTarget],
) -> Vec<Value> {
    let mut failed = Vec::new();
    for target in lost {
        let Some(job) = jobs.iter_mut().find(|job| job.job_id == target.job_id) else {
            continue;
        };
        if !matches!(job.state, JobState::Running | JobState::WaitingQuota)
            || job.target.as_deref() != Some(target.member.as_str())
        {
            continue;
        }
        let last = target.last_probe_ok_at_ms.map_or_else(
            || "no successful probe was recorded".to_owned(),
            |at| format!("last successful probe at {}", super::clock::rfc3339(at)),
        );
        let detail = match target.unreachable_probes {
            Some(count) => format!(
                "{} has been unreachable for {count} consecutive probes ({last})",
                target.member
            ),
            None => format!("{} is no longer a hub member ({last})", target.member),
        };
        let detail = match &target.evidence {
            Some(evidence) => format!("{detail}; {evidence}"),
            None => detail,
        };
        failed.push(fail_job(
            job,
            now_ms,
            target.classification.unwrap_or("unclassified"),
            detail,
        ));
    }
    failed
}

/// A lease the hub is giving up and the observation that authorizes failing its runs.
pub(crate) struct LeaseRetirement<'a> {
    pub member: &'a str,
    pub reason: &'a str,
    pub classification: &'static str,
}

/// Fail the runs still placing on a lease that the hub gave up. The classification names the
/// actual observation: a provider-confirmed absent server for an orphaned lease, or a bounded
/// wait that retired a live server.
pub(crate) fn settle_retired_lease(
    jobs: &mut [Job],
    now_ms: u64,
    retirement: LeaseRetirement<'_>,
) -> Vec<Value> {
    let mut failed = Vec::new();
    for job in jobs.iter_mut() {
        if !matches!(job.state, JobState::Running | JobState::WaitingQuota)
            || job.target.as_deref() != Some(retirement.member)
        {
            continue;
        }
        failed.push(fail_job(
            job,
            now_ms,
            retirement.classification,
            format!("{}; {} is retired", retirement.reason, retirement.member),
        ));
    }
    failed
}

/// Fail the runs still placing on a lease whose pool was removed and whose provider server is
/// confirmed gone.
pub(crate) fn settle_orphaned_lease(
    jobs: &mut [Job],
    now_ms: u64,
    member: &str,
    evidence: &str,
) -> Vec<Value> {
    settle_retired_lease(
        jobs,
        now_ms,
        LeaseRetirement {
            member,
            reason: evidence,
            classification: CLASSIFIED_PROVIDER_ABSENT,
        },
    )
}

/// Record one target-lost failure, naming the observation that decided it. `detail` explains
/// what the hub saw; `classification` says which observation it was.
fn fail_job(job: &mut Job, now_ms: u64, classification: &str, detail: String) -> Value {
    job.reaper = None;
    job.state = JobState::Failed;
    job.quota = None;
    job.quota_attempts = 0;
    job.finished_at_ms = Some(now_ms);
    job.failure_reason = Some("target_lost".to_owned());
    job.failure_classification = Some(classification.to_owned());
    job.error = Some(JobError::new(
        JobErrorKind::TargetLost,
        format!("{detail}; the hub cannot observe or stop its run ({classification})"),
    ));
    json!({
        "jobId": job.job_id,
        "state": job.display_state(now_ms),
        "target": job.target,
        "runId": job.run_id,
        "reason": "target_lost",
        "classification": job.failure_classification,
    })
}

/// The deciding observation when only a failed load probe proves a member is gone.
#[cfg(test)]
pub(crate) const CLASSIFIED_PROBE_UNREACHABLE: &str = "probe_unreachable";

/// Keep only candidates whose current probe is decisive, or whose member already left. All
/// others stay running with a reaper note naming why no action was taken.
///
/// The test-only [`tick`] uses this probe-only policy. Production `service::pass` uses the
/// provider or direct-TCP confirmation of `service::confirm_lost_targets` instead.
#[cfg(test)]
pub(crate) fn confirm_lost_by_probe(
    lost: Vec<LostTarget>,
    loads: &BTreeMap<String, Load>,
) -> (Vec<LostTarget>, Vec<WithheldTarget>) {
    let mut confirmed = Vec::new();
    let mut withheld = Vec::new();
    for mut target in lost {
        let Some(count) = target.unreachable_probes else {
            confirmed.push(target);
            continue;
        };
        match loads.get(&target.member) {
            Some(load) if matches!(load.reachability, Reachability::Unreachable) => {
                target.evidence = Some(format!(
                    "the load probe could not connect to the member: {}",
                    load.error.as_deref().unwrap_or("connection failed")
                ));
                target.classification = Some(CLASSIFIED_PROBE_UNREACHABLE);
                confirmed.push(target);
            }
            Some(load) => withheld.push(WithheldTarget {
                job_id: target.job_id,
                member: target.member.clone(),
                reason: format!(
                    "{} probe after {count} failures was {}: inconclusive, job kept running",
                    target.member,
                    match load.reachability {
                        Reachability::Reachable => "successful",
                        Reachability::Unreachable | Reachability::Inconclusive => "inconclusive",
                    }
                ),
            }),
            None => withheld.push(WithheldTarget {
                job_id: target.job_id,
                member: target.member.clone(),
                reason: format!(
                    "{} was not probed after {count} failures: inconclusive, job kept running",
                    target.member
                ),
            }),
        }
    }
    (confirmed, withheld)
}

/// Record reaper notes on running or quota-paused jobs; `hub.warning` surfaces them.
pub(crate) fn note_withheld(jobs: &mut [Job], withheld: &[WithheldTarget]) {
    for note in withheld {
        if let Some(job) = jobs.iter_mut().find(|job| job.job_id == note.job_id) {
            if matches!(job.state, JobState::Running | JobState::WaitingQuota)
                && job.target.as_deref() == Some(note.member.as_str())
            {
                job.reaper = Some(note.reason.clone());
            }
        }
    }
}

/// Refresh discovery capabilities of named, non-disabled members older than `max_age_ms`.
/// A failed discovery keeps the last known capabilities and records the error.
pub(crate) async fn discover(
    members: &mut [Member],
    driver: &dyn HubDriver,
    now_ms: u64,
    max_age_ms: u64,
) {
    let stale = members
        .iter()
        .enumerate()
        .filter(|(_, member)| member.name != LOCAL_MEMBER && member.state != MemberState::Disabled)
        .filter(|(_, member)| {
            let retry_ready = member.capability_retry_at_ms.is_none_or(|at| now_ms >= at);
            retry_ready
                && member
                    .capabilities_at_ms
                    .is_none_or(|at| now_ms.saturating_sub(at) >= max_age_ms)
        })
        .map(|(index, member)| (index, member.name.clone()))
        .collect::<Vec<_>>();
    let results = join_all(stale.into_iter().map(|(index, name)| async move {
        let result = tokio::time::timeout(PROBE_TIMEOUT, driver.capabilities(&name)).await;
        (index, result)
    }))
    .await;
    for (index, result) in results {
        let member = &mut members[index];
        match result {
            Ok(Ok(capabilities)) => {
                member.capabilities = Some(capabilities);
                member.capabilities_at_ms = Some(now_ms);
                member.capability_error = None;
                member.capability_failures = 0;
                member.capability_retry_at_ms = None;
            }
            Ok(Err(error)) => back_off_discovery(member, now_ms, error.to_string()),
            Err(_) => back_off_discovery(member, now_ms, "discovery timed out".to_owned()),
        }
    }
}

fn back_off_discovery(member: &mut Member, now_ms: u64, error: String) {
    member.capability_error = Some(error);
    member.capability_failures = member.capability_failures.saturating_add(1);
    let shift = member.capability_failures.saturating_sub(1).min(5);
    let delay = CAPABILITY_RETRY_BASE_MS
        .saturating_mul(1u64 << shift)
        .min(CAPABILITY_RETRY_MAX_MS);
    member.capability_retry_at_ms = Some(now_ms.saturating_add(delay));
}

/// Unconfirmed submissions a job may have before it fails, unless the daemon sets another
/// limit (`hub daemon run --unconfirmed-limit`).
pub(crate) const DEFAULT_UNCONFIRMED_LIMIT: u32 = 30;

/// How one submission attempt ended.
#[derive(Debug)]
pub(crate) enum Dispatched {
    /// The target confirmed the run.
    Placed,
    /// The target gave no usable answer: the job stays placing on the member, and the next
    /// pass submits it again with the same submission key, so a run the target did accept is
    /// not duplicated.
    Unconfirmed(HubError),
    /// The submission was refused: the job failed without a run.
    Rejected(HubError),
}

impl Dispatched {
    /// The `outcome` label of the placement counter.
    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::Placed => "placed",
            Self::Unconfirmed(_) => "unconfirmed",
            Self::Rejected(_) => "dispatch_failed",
        }
    }
}

/// Place one job on `member`. Only a refused submission fails the job and counts an attempt;
/// an unconfirmed one keeps it placing (see [`Dispatched::Unconfirmed`]) until `limit`
/// unconfirmed submissions, when the job fails. `now_ms` stamps the outcome.
pub(crate) async fn dispatch(
    job: &mut Job,
    member: &str,
    driver: &dyn HubDriver,
    (now_ms, limit): (u64, u32),
) -> Dispatched {
    let attempt = job.attempts + 1;
    let submission_key = format!("hub-{}-{attempt}", job.job_id);
    let request = SubmitRequest {
        member,
        job,
        submission_key: submission_key.clone(),
    };
    let submitted = driver.submit(request).await;
    job.target = Some(member.to_owned());
    match submitted {
        Ok(run_id) => {
            job.attempts = attempt;
            job.state = JobState::Running;
            job.run_id = Some(run_id);
            job.dispatched_at_ms = Some(now_ms);
            job.error = None;
            job.failure_reason = None;
            Dispatched::Placed
        }
        Err(error @ HubError::Unanswered(_)) => {
            job.unconfirmed_submissions = job.unconfirmed_submissions.saturating_add(1);
            if job.unconfirmed_submissions >= limit {
                job.state = JobState::Failed;
                job.finished_at_ms = Some(now_ms);
                job.failure_reason = None;
                job.error = Some(JobError::new(
                    JobErrorKind::UnconfirmedLimit,
                    format!(
                        "{member} did not confirm {} submissions (last: {error}); a run with \
                         submission key {submission_key} may still exist there",
                        job.unconfirmed_submissions
                    ),
                ));
                return Dispatched::Rejected(error);
            }
            job.state = JobState::Running;
            job.failure_reason = None;
            job.error = Some(JobError::new(
                JobErrorKind::SubmissionUnconfirmed,
                error.to_string(),
            ));
            Dispatched::Unconfirmed(error)
        }
        Err(error) => {
            job.attempts = attempt;
            job.state = JobState::Failed;
            job.finished_at_ms = Some(now_ms);
            job.failure_reason = None;
            job.error = Some(JobError::new(
                JobErrorKind::SubmissionRejected,
                error.to_string(),
            ));
            Dispatched::Rejected(error)
        }
    }
}

/// Record the status document's watch cursor and newest activity heartbeat on a running job.
/// An unchanged cursor keeps the time it last advanced; a new cursor resets it. Activity is
/// ordered by the producer timestamp, so repeated polls of the same heartbeat are not new
/// evidence and cannot keep a dead run alive.
pub(crate) fn record_cursor(job: &mut Job, status: &Value, now_ms: u64) {
    match status
        .get("atCursor")
        .or_else(|| status.pointer("/status/atCursor"))
        .and_then(Value::as_str)
    {
        None => {
            // A successful status read without a cursor still proves the run was observed, so it
            // resets the stall clock without inventing a cursor value.
            job.last_cursor_at_ms = Some(now_ms);
        }
        Some(cursor) if job.last_cursor.as_deref() == Some(cursor) => {
            if job.last_cursor_at_ms.is_none() {
                job.last_cursor_at_ms = Some(now_ms);
            }
        }
        Some(cursor) => {
            job.last_cursor = Some(cursor.to_owned());
            job.last_cursor_at_ms = Some(now_ms);
        }
    }
    if let Some((reported_at_ms, activity)) = status_activity(status) {
        // Order by the producer timestamp so a repeated poll is not new evidence, but keep the
        // stall clock on this hub's clock so target clock skew cannot extend the window. Older
        // status documents carry no timestamp, so a changed activity is then the only evidence.
        let new_evidence = match (reported_at_ms, job.last_activity_reported_at_ms) {
            (Some(reported), Some(current)) => reported > current,
            (Some(_), None) => true,
            (None, _) => job.last_activity.as_ref() != Some(&activity),
        };
        if new_evidence {
            job.last_activity_reported_at_ms = reported_at_ms.or(job.last_activity_reported_at_ms);
            job.last_activity_at_ms = Some(now_ms);
            job.last_activity = Some(activity);
        }
    }
}

fn status_activity(status: &Value) -> Option<(Option<u64>, JobActivity)> {
    let activity = status
        .pointer("/status/activity")
        .or_else(|| status.get("activity"))?;
    Some((
        activity.get("timestamp").and_then(Value::as_u64),
        JobActivity {
            node: activity.get("node").and_then(Value::as_str)?.to_owned(),
            command: activity.get("command").and_then(Value::as_str)?.to_owned(),
            elapsed_ms: activity
                .get("elapsedMs")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        },
    ))
}

/// Apply a run status projection to a running job. Returns true when the job finished.
///
/// A provider quota refusal is not a failed run for the hub: the job enters [`JobState::WaitingQuota`],
/// keeps its machine held, and resumes the same node after the retry deadline. This is why a
/// reviewer whose provider call was refused is never reported as `review_failed`.
pub(crate) fn apply_status(job: &mut Job, status: &Value, now_ms: u64) -> bool {
    let Some(phase) = status.pointer("/status/phase").and_then(Value::as_str) else {
        return false;
    };
    if phase != "finished" {
        return false;
    }
    let result = status
        .pointer("/status/terminalResult/status")
        .and_then(Value::as_str);
    if result != Some("succeeded")
        && let Some(failure) = quota::detect(status, now_ms)
    {
        let attempts = job.quota_attempts.saturating_add(1);
        job.quota_attempts = attempts;
        let backoff = quota::backoff_ms(attempts, &job.job_id);
        let until_ms = failure
            .retry_at_ms
            .unwrap_or_else(|| now_ms.saturating_add(backoff))
            .max(now_ms.saturating_add(backoff));
        job.quota = Some(QuotaWait {
            limit: failure.limit.clone(),
            retry_at_ms: until_ms,
            attempts,
        });
        job.state = JobState::WaitingQuota;
        job.failure_reason = None;
        job.failure_classification = None;
        job.finished_at_ms = None;
        job.token_usage = JobUsage::from_status(status);
        job.error = Some(JobError::new(
            JobErrorKind::ProviderQuota,
            quota::wait_message(&failure, until_ms),
        ));
        return false;
    }
    job.state = if result == Some("succeeded") {
        JobState::Succeeded
    } else {
        JobState::Failed
    };
    job.quota = None;
    job.quota_attempts = 0;
    job.failure_reason = status
        .pointer("/status/terminalResult/reason")
        .and_then(Value::as_str)
        .map(str::to_owned);
    job.token_usage = JobUsage::from_status(status);
    if job.labels.contains_key(super::eval::ROLE_LABEL) {
        job.output = status.pointer("/status/terminalResult/output").cloned();
    }
    job.finished_at_ms = Some(now_ms);
    job.error = None;
    true
}

/// Apply one status read to a running job; returns the status when it was read.
pub(crate) fn apply_refresh(
    job: &mut Job,
    result: Result<Value, HubError>,
    now_ms: u64,
) -> Option<Value> {
    match result {
        Ok(status) => {
            job.reaper = None;
            if job
                .error
                .as_ref()
                .is_some_and(|error| error.kind == JobErrorKind::StatusUnavailable)
            {
                job.error = None;
            }
            record_cursor(job, &status, now_ms);
            apply_status(job, &status, now_ms);
            Some(status)
        }
        Err(error) => {
            job.reaper = Some(format!(
                "status probe was unavailable ({error}): job kept running"
            ));
            job.error = Some(JobError::new(
                JobErrorKind::StatusUnavailable,
                error.to_string(),
            ));
            None
        }
    }
}

/// A running job whose run is polled by a dispatcher pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Poll {
    pub job_id: String,
    pub target: String,
    pub run_id: String,
}

/// Running jobs that have a run to poll. Jobs still being placed have no run yet.
pub(crate) fn polls(state: &HubState) -> Vec<Poll> {
    state
        .jobs
        .iter()
        .filter(|job| job.state == JobState::Running)
        .filter_map(|job| {
            Some(Poll {
                job_id: job.job_id.clone(),
                target: job.target.clone()?,
                run_id: job.run_id.clone()?,
            })
        })
        .collect()
}

/// Read every polled run's status concurrently, without touching hub state.
pub(crate) async fn poll(
    polls: Vec<Poll>,
    driver: &dyn HubDriver,
) -> Vec<(Poll, Result<Value, HubError>)> {
    join_all(polls.into_iter().map(|poll| async move {
        let result =
            match tokio::time::timeout(PROBE_TIMEOUT, driver.status(&poll.target, &poll.run_id))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(HubError::Target {
                    message: format!("zeroshot status {} timed out", poll.run_id),
                    diagnostic: None,
                }),
            };
        (poll, result)
    }))
    .await
}

/// Apply polled statuses to jobs that still run the polled run. Returns finished summaries.
pub(crate) fn apply_polls(
    state: &mut HubState,
    results: Vec<(Poll, Result<Value, HubError>)>,
    now_ms: u64,
) -> Vec<Value> {
    let mut finished = Vec::new();
    for (poll, result) in results {
        if result.is_ok() {
            record_target_health(state, &poll.target, now_ms);
        }
        let Some(job) = state.jobs.iter_mut().find(|job| {
            job.job_id == poll.job_id
                && job.state == JobState::Running
                && job.run_id.as_deref() == Some(poll.run_id.as_str())
        }) else {
            continue;
        };
        apply_refresh(job, result, now_ms);
        if job.state.is_terminal() {
            finished.push(json!({
                "jobId": job.job_id,
                "state": job.display_state(now_ms),
                "target": job.target,
                "runId": job.run_id,
            }));
        }
    }
    // A quota-paused job keeps its machine through the derived automatic hold, separate from
    // any operator `hub stop` hold on the same lease.
    pool::sync_quota_holds(state);
    finished
}

/// A quota-paused job whose retry time has arrived and whose run can be resumed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QuotaResume {
    pub job_id: String,
    pub target: String,
    pub run_id: String,
}

/// Quota-paused jobs that may be resumed now. Only these wait; every other job is left alone.
pub(crate) fn due_quota_waits(state: &HubState, now_ms: u64) -> Vec<QuotaResume> {
    state
        .jobs
        .iter()
        .filter(|job| job.state == JobState::WaitingQuota)
        .filter(|job| {
            job.quota
                .as_ref()
                .is_some_and(|quota| quota.retry_at_ms <= now_ms)
        })
        .filter_map(|job| {
            Some(QuotaResume {
                job_id: job.job_id.clone(),
                target: job.target.clone()?,
                run_id: job.run_id.clone()?,
            })
        })
        .collect()
}

/// Record that a quota-paused run was resumed. The successor continues from the failed run's
/// latest checkpoint, so the executing node is retried instead of the graph restarting.
pub(crate) fn settle_quota_resumed(
    jobs: &mut [Job],
    resume: &QuotaResume,
    successor_run_id: &str,
    now_ms: u64,
) -> Option<Value> {
    let job = jobs.iter_mut().find(|job| {
        job.job_id == resume.job_id
            && job.state == JobState::WaitingQuota
            && job.run_id.as_deref() == Some(resume.run_id.as_str())
    })?;
    job.state = JobState::Running;
    job.run_id = Some(successor_run_id.to_owned());
    job.dispatched_at_ms = Some(now_ms);
    job.last_cursor = None;
    job.last_cursor_at_ms = None;
    job.target_last_ok_at_ms = Some(now_ms);
    job.finished_at_ms = None;
    job.failure_reason = None;
    job.failure_classification = None;
    job.reaper = None;
    job.error = None;
    Some(json!({
        "jobId": job.job_id,
        "target": job.target,
        "runId": successor_run_id,
        "resumedFrom": resume.run_id,
    }))
}

/// Keep a quota-paused job waiting when its resume could not be submitted. The pause is extended
/// with the next bounded backoff; a transport problem never turns the job into a failure.
pub(crate) fn reschedule_quota_wait(
    jobs: &mut [Job],
    resume: &QuotaResume,
    error: &str,
    now_ms: u64,
) {
    let Some(job) = jobs.iter_mut().find(|job| {
        job.job_id == resume.job_id
            && job.state == JobState::WaitingQuota
            && job.run_id.as_deref() == Some(resume.run_id.as_str())
    }) else {
        return;
    };
    let attempts = job.quota_attempts.saturating_add(1);
    job.quota_attempts = attempts;
    let limit = job
        .quota
        .as_ref()
        .map_or_else(|| "provider".to_owned(), |quota| quota.limit.clone());
    let until_ms = now_ms.saturating_add(quota::backoff_ms(attempts, &job.job_id));
    job.quota = Some(QuotaWait {
        limit: limit.clone(),
        retry_at_ms: until_ms,
        attempts,
    });
    job.error = Some(JobError::new(
        JobErrorKind::ProviderQuota,
        format!(
            "provider quota {limit} is exhausted; resuming the held node failed ({error}); retrying at {}",
            super::clock::rfc3339(until_ms)
        ),
    ));
}

/// Copy discovery results from `discovered` into the members of the same name.
pub(crate) fn merge_capabilities(members: &mut [Member], discovered: &[Member]) {
    for member in members {
        if let Some(found) = discovered.iter().find(|found| found.name == member.name) {
            member.capabilities.clone_from(&found.capabilities);
            member.capabilities_at_ms = found.capabilities_at_ms;
            member.capability_error.clone_from(&found.capability_error);
            member.capability_failures = found.capability_failures;
            member.capability_retry_at_ms = found.capability_retry_at_ms;
        }
    }
}

/// Choose a member for each due job that fits now, highest priority first. Chosen jobs are
/// marked running on their member without a run, so no other pass or request places them
/// again while their submission is in flight.
#[cfg(test)]
pub(crate) fn choose(
    state: &mut HubState,
    now_ms: u64,
    loads: &mut BTreeMap<String, Load>,
) -> Vec<(String, String)> {
    choose_with_quota(state, now_ms, loads, None)
}

/// Like [`choose`], with a provider-exhaustion refusal supplied by the owning hub service.
pub(crate) fn choose_with_quota(
    state: &mut HubState,
    now_ms: u64,
    loads: &mut BTreeMap<String, Load>,
    quota_refusal: Option<&str>,
) -> Vec<(String, String)> {
    // A provider exhaustion owned by the hub blocks new placements until its retry time.
    // Queued jobs stay parked and their `waiting` field says why; no target is asked to start
    // work that the gateway would refuse.
    if let Some(refusal) = quota_refusal {
        for index in due_jobs(&state.jobs, now_ms) {
            state.jobs[index].pool_wait = Some(refusal.to_owned());
        }
        return Vec::new();
    }
    for index in due_jobs(&state.jobs, now_ms) {
        if state.jobs[index]
            .pool_wait
            .as_deref()
            .is_some_and(|waiting| waiting.starts_with("provider quota"))
        {
            state.jobs[index].pool_wait = None;
        }
    }
    let mut chosen = Vec::new();
    for index in due_jobs(&state.jobs, now_ms) {
        let job = &state.jobs[index];
        let Placement::Place(member) =
            place(&state.members, &job.placement, &job.requirements, loads)
        else {
            continue;
        };
        if let Some(load) = loads.get_mut(&member) {
            load.active = load.active.saturating_add(1);
        }
        let job = &mut state.jobs[index];
        job.state = JobState::Running;
        job.target = Some(member.clone());
        chosen.push((job.job_id.clone(), member));
    }
    chosen
}

/// Record one dispatch outcome in a pass report.
pub(crate) fn record_dispatch(report: &mut TickReport, job: &Job, dispatched: &Dispatched) {
    let mut entry = json!({ "jobId": job.job_id, "target": job.target });
    match dispatched {
        Dispatched::Placed => {
            entry["runId"] = json!(job.run_id);
            report.dispatched.push(entry);
        }
        Dispatched::Unconfirmed(error) => {
            entry["error"] = json!(error.to_string());
            report.unconfirmed.push(entry);
        }
        Dispatched::Rejected(error) => {
            entry["error"] = json!(error.to_string());
            report.failed.push(entry);
        }
    }
}

/// Jobs placing on a member, as (job, member): each pass submits them again there. Placement
/// is serialized, so none of them has a submission in flight when a pass reads them.
pub(crate) fn placing(state: &HubState) -> Vec<(String, String)> {
    state
        .jobs
        .iter()
        .filter(|job| job.is_placing())
        .filter_map(|job| Some((job.job_id.clone(), job.target.clone()?)))
        .collect()
}

/// Mark jobs that were mid-placement when a daemon stopped as unconfirmed on their member, so
/// the next pass submits them there again with the same key and a run the target already
/// accepted is not duplicated. A placing job without a member returns to the queue.
pub(crate) fn recover_placing(state: &mut HubState) -> usize {
    let mut recovered = 0;
    for job in state.jobs.iter_mut().filter(|job| job.is_placing()) {
        if job.target.is_some() {
            job.error = Some(JobError::new(
                JobErrorKind::SubmissionUnconfirmed,
                "the hub stopped before the target confirmed the submission",
            ));
        } else {
            job.state = JobState::Queued;
        }
        recovered += 1;
    }
    recovered
}

/// Where a job's effective stall limit came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StallLimitSource {
    /// The job's own `maxStallMs`.
    Job,
    /// The hub daemon's `--max-stall`.
    Hub,
    /// A built-in fallback, when one is configured.
    Default,
}

impl StallLimitSource {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Hub => "hub",
            Self::Default => "default",
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Hub => "hub default",
            Self::Default => "default",
        }
    }
}

/// The stall limit that applies to one job and where it was configured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StallLimit {
    pub max_stall_ms: u64,
    pub source: StallLimitSource,
}

impl StallLimit {
    /// Job `maxStallMs` first, then the hub daemon's `--max-stall`.
    pub(crate) fn for_job(job: &Job, hub_max_stall_ms: Option<u64>) -> Option<Self> {
        Self::resolve(job, hub_max_stall_ms, None)
    }

    fn resolve(
        job: &Job,
        hub_max_stall_ms: Option<u64>,
        built_in_default: Option<u64>,
    ) -> Option<Self> {
        job.max_stall_ms
            .map(|max_stall_ms| Self {
                max_stall_ms,
                source: StallLimitSource::Job,
            })
            .or_else(|| {
                hub_max_stall_ms.map(|max_stall_ms| Self {
                    max_stall_ms,
                    source: StallLimitSource::Hub,
                })
            })
            .or_else(|| {
                built_in_default.map(|max_stall_ms| Self {
                    max_stall_ms,
                    source: StallLimitSource::Default,
                })
            })
    }
}

/// A running job whose watch cursor stayed unchanged past its effective stall limit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StalledRun {
    pub job_id: String,
    pub target: String,
    pub run_id: String,
    pub cursor: String,
    pub still_ms: u64,
    pub max_stall_ms: u64,
    pub limit_source: StallLimitSource,
    pub activity: Option<JobActivity>,
}

/// Running jobs whose cursor has stayed unchanged for at least their own effective limit.
///
/// `limit_for` resolves the job override before the hub-wide limit, so one job may wait longer
/// than the rest of the sweep. A run that has not produced a cursor yet is still stalled once it
/// has sat at dispatch for the same budget; otherwise the limit silently does nothing for a run
/// that never starts watching. Missing identity fields stay empty rather than excluding the job
/// from the check.
pub(crate) fn stalled_runs(
    jobs: &[Job],
    now_ms: u64,
    limit_for: impl Fn(&Job) -> Option<StallLimit>,
) -> Vec<StalledRun> {
    jobs.iter()
        .filter(|job| job.state == JobState::Running)
        .filter_map(|job| {
            let limit = limit_for(job)?;
            // The newest of the cursor clock, an in-flight heartbeat, and the last successful
            // contact (a status read without `atCursor` or a load probe), and only then
            // placement. A reachable member whose status omits its cursor is observed work, not
            // a silent run.
            let at = match (job.last_cursor_at_ms, job.last_activity_at_ms) {
                (Some(cursor), Some(activity)) => Some(cursor.max(activity)),
                (cursor, activity) => cursor.or(activity),
            }
            .or(job.target_last_ok_at_ms)
            .or(job.dispatched_at_ms)?;
            let still_ms = now_ms.saturating_sub(at);
            (still_ms >= limit.max_stall_ms).then_some(StalledRun {
                job_id: job.job_id.clone(),
                target: job.target.clone().unwrap_or_default(),
                run_id: job.run_id.clone().unwrap_or_default(),
                cursor: job.last_cursor.clone().unwrap_or_default(),
                still_ms,
                max_stall_ms: limit.max_stall_ms,
                limit_source: limit.source,
                activity: job.last_activity.clone(),
            })
        })
        .collect()
}

/// The daemon policy and evidence for settling stalled jobs in one pass.
pub(crate) struct StallSettlement<'a> {
    pub now_ms: u64,
    pub stalled: &'a [StalledRun],
    pub stop_errors: &'a BTreeMap<String, String>,
}

/// Fail stalled jobs after their runs were stopped, naming the limit, its source, the cursor, and
/// how long the cursor was still.
pub(crate) fn settle_stalled(jobs: &mut [Job], settlement: StallSettlement<'_>) -> Vec<Value> {
    let StallSettlement {
        now_ms,
        stalled,
        stop_errors,
    } = settlement;
    let mut failed = Vec::new();
    for run in stalled {
        let Some(job) = jobs.iter_mut().find(|job| job.job_id == run.job_id) else {
            continue;
        };
        if job.state != JobState::Running
            || job.run_id.as_deref().unwrap_or_default() != run.run_id
            || job.last_cursor.as_deref().unwrap_or_default() != run.cursor
        {
            continue;
        }
        let run_label = if run.run_id.is_empty() {
            "the run".to_owned()
        } else {
            format!("run {}", run.run_id)
        };
        let cursor_label = if run.cursor.is_empty() {
            "no cursor".to_owned()
        } else {
            format!("cursor {}", run.cursor)
        };
        let activity = run.activity.as_ref().map_or_else(String::new, |activity| {
            format!(
                "; last activity node '{}' ran '{}' for {}s",
                activity.node,
                activity.command,
                activity.elapsed_ms / 1000
            )
        });
        let silence = if run.activity.is_some() {
            "no activity for"
        } else {
            "no cursor movement for"
        };
        let detail = format!(
            "{silence} {}ms (limit {}ms from {}); {run_label} on {} at {cursor_label}{activity}",
            run.still_ms,
            run.max_stall_ms,
            run.limit_source.description(),
            run.target,
        );
        if let Some(error) = stop_errors.get(&run.job_id) {
            job.error = Some(JobError::new(
                JobErrorKind::Stalled,
                format!("stalled: {detail}; stopping it failed: {error}; the hub will retry"),
            ));
            continue;
        }
        job.state = JobState::Failed;
        job.finished_at_ms = Some(now_ms);
        job.failure_reason = Some("stalled".to_owned());
        job.failure_classification = Some(CLASSIFIED_STALLED.to_owned());
        job.error = Some(JobError::new(
            JobErrorKind::Stalled,
            format!("stalled: {detail}; the hub stopped it"),
        ));
        failed.push(json!({
            "jobId": job.job_id,
            "state": job.display_state(now_ms),
            "target": job.target,
            "runId": job.run_id,
            "cursor": run.cursor,
            "stillMs": run.still_ms,
            "maxStallMs": run.max_stall_ms,
            "maxStallSource": run.limit_source.as_str(),
            "lastActivity": run.activity,
        }));
    }
    failed
}

/// Indexes of queued jobs whose start time has come: higher priority first, then FIFO.
pub(crate) fn due_jobs(jobs: &[Job], now_ms: u64) -> Vec<usize> {
    let mut due = jobs
        .iter()
        .enumerate()
        .filter(|(_, job)| {
            job.state == JobState::Queued && job.not_before_ms.is_none_or(|at| at <= now_ms)
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    due.sort_by(|left, right| {
        let (left, right) = (&jobs[*left], &jobs[*right]);
        right
            .priority
            .cmp(&left.priority)
            .then(left.submitted_at_ms.cmp(&right.submitted_at_ms))
            .then(left.job_id.cmp(&right.job_id))
    });
    due
}

/// One dispatcher pass on an exclusively held state: fire due cron schedules, refresh running
/// jobs, then place due jobs. The daemon runs the same phases with target
/// calls outside its state lock (see `service::HubService::pass`).
#[cfg(test)]
pub(crate) async fn tick(state: &mut HubState, driver: &dyn HubDriver, now_ms: u64) -> TickReport {
    let mut report = TickReport {
        fired: super::cron::fire_due(state, now_ms),
        ..TickReport::default()
    };
    let results = poll(polls(state), driver).await;
    report.finished = apply_polls(state, results, now_ms);
    for resume in due_quota_waits(state, now_ms) {
        match driver.resume(&resume.target, &resume.run_id).await {
            Ok(successor) => {
                if let Some(record) =
                    settle_quota_resumed(&mut state.jobs, &resume, &successor, now_ms)
                {
                    report.resumed.push(record);
                }
            }
            Err(error) => {
                reschedule_quota_wait(&mut state.jobs, &resume, &error.to_string(), now_ms);
            }
        }
    }
    pool::sync_quota_holds(state);
    // Probe every member on every pass, so a placed job's member is observed even when no
    // queued job needs placement. Health failures accumulate across passes.
    let members = state.members.iter().collect::<Vec<_>>();
    let mut loads = probe(&members, &state.jobs, driver).await;
    record_health(&mut state.members, &mut state.jobs, &loads, now_ms);
    let (lost, withheld) = confirm_lost_by_probe(lost_targets(state), &loads);
    note_withheld(&mut state.jobs, &withheld);
    report.target_lost = settle_lost_targets(&mut state.jobs, now_ms, &lost);
    pool::sync_quota_holds(state);
    for (job_id, member) in placing(state) {
        if let Some(job) = state.jobs.iter_mut().find(|job| job.job_id == job_id) {
            let dispatched =
                dispatch(job, &member, driver, (now_ms, DEFAULT_UNCONFIRMED_LIMIT)).await;
            let placed = matches!(dispatched, Dispatched::Placed);
            let job = job.clone();
            record_dispatch(&mut report, &job, &dispatched);
            if placed {
                if let Some(member) = job.target.as_deref() {
                    pool::record_member_phase(state, member, PoolPhase::InUse, now_ms);
                }
            }
        }
    }
    if due_jobs(&state.jobs, now_ms).is_empty() {
        return report;
    }
    discover(&mut state.members, driver, now_ms, CAPABILITY_TTL_MS).await;
    for (job_id, member) in choose(state, now_ms, &mut loads) {
        let Some(job) = state.jobs.iter_mut().find(|job| job.job_id == job_id) else {
            continue;
        };
        let dispatched = dispatch(job, &member, driver, (now_ms, DEFAULT_UNCONFIRMED_LIMIT)).await;
        let placed = matches!(dispatched, Dispatched::Placed);
        let job = job.clone();
        record_dispatch(&mut report, &job, &dispatched);
        if placed {
            if let Some(member) = job.target.as_deref() {
                pool::record_member_phase(state, member, PoolPhase::InUse, now_ms);
            }
        }
    }
    report.loads = Some(loads);
    report
}
