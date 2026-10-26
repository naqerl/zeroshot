use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::future::join_all;
use serde_json::{Value, json};

use super::driver::{HubDriver, SubmitRequest};
use super::error::HubError;
use super::model::{HubState, Job, JobError, JobErrorKind, JobState, LOCAL_MEMBER, Member, MemberState};
use super::usage::JobUsage;
use super::placement::{Load, Placement, place};

const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
/// Consecutive failed member probes, one per dispatcher interval, after which a job placed on
/// that member is failed as `target_lost`. A single failed probe, or a member that flaps below
/// this threshold, keeps the job running.
pub(crate) const UNREACHABLE_REAP_THRESHOLD: u32 = 3;
/// Discovered capabilities are refreshed after this age by placement passes.
pub(crate) const CAPABILITY_TTL_MS: u64 = 5 * 60 * 1000;

/// Outcome of one dispatcher pass.
#[derive(Debug, Default)]
pub(crate) struct TickReport {
    pub fired: Vec<Value>,
    pub dispatched: Vec<Value>,
    pub finished: Vec<Value>,
    pub expired: Vec<String>,
    pub stalled: Vec<Value>,
    pub target_lost: Vec<Value>,
    pub failed: Vec<Value>,
    pub unconfirmed: Vec<Value>,
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
            "expired": self.expired,
            "stalled": self.stalled,
            "targetLost": self.target_lost,
            "dispatchFailed": self.failed,
            "dispatchUnconfirmed": self.unconfirmed,
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
                reachable: true,
                active: active_runs(&runs),
                error: None,
            },
            Ok(Err(error)) => Load {
                reachable: false,
                active: 0,
                error: Some(error.to_string()),
            },
            Err(_) => Load {
                reachable: false,
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
    for job in jobs.iter().filter(|job| job.state == JobState::Running) {
        if let Some(load) = job.target.as_ref().and_then(|target| loads.get_mut(target)) {
            let own = u32::try_from(
                jobs.iter()
                    .filter(|other| other.state == JobState::Running && other.target == job.target)
                    .count(),
            )
            .unwrap_or(u32::MAX);
            load.active = load.active.max(own);
        }
    }
    loads
}

/// Record load probes on members and on the jobs placed there. A successful probe resets the
/// member's failure streak and refreshes the last success on the member and its running jobs.
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
        if load.reachable {
            member.unreachable_probes = 0;
            member.last_probe_ok_at_ms = Some(now_ms);
        } else {
            member.unreachable_probes = member.unreachable_probes.saturating_add(1);
        }
    }
    for job in jobs.iter_mut().filter(|job| job.state == JobState::Running) {
        let Some(target) = job.target.as_deref() else {
            continue;
        };
        if loads.get(target).is_some_and(|load| load.reachable) {
            job.target_last_ok_at_ms = Some(now_ms);
        }
    }
}

/// A running job whose member left the hub or stayed unreachable for too many probes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LostTarget {
    pub job_id: String,
    pub member: String,
    /// Consecutive failed probes; `None` when the member is no longer a hub member.
    pub unreachable_probes: Option<u32>,
    pub last_probe_ok_at_ms: Option<u64>,
}

/// Running jobs that can no longer be observed because their member is gone or unreachable.
pub(crate) fn lost_targets(state: &HubState) -> Vec<LostTarget> {
    state
        .jobs
        .iter()
        .filter(|job| job.state == JobState::Running)
        .filter_map(|job| {
            let name = job.target.as_deref()?;
            match state.member(name) {
                None => Some(LostTarget {
                    job_id: job.job_id.clone(),
                    member: name.to_owned(),
                    unreachable_probes: None,
                    last_probe_ok_at_ms: job.target_last_ok_at_ms,
                }),
                Some(member) if member.unreachable_probes >= UNREACHABLE_REAP_THRESHOLD => {
                    Some(LostTarget {
                        job_id: job.job_id.clone(),
                        member: member.name.clone(),
                        unreachable_probes: Some(member.unreachable_probes),
                        last_probe_ok_at_ms: member
                            .last_probe_ok_at_ms
                            .or(job.target_last_ok_at_ms),
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
        if job.state != JobState::Running || job.target.as_deref() != Some(target.member.as_str()) {
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
        job.state = JobState::Failed;
        job.finished_at_ms = Some(now_ms);
        job.failure_reason = Some("target_lost".to_owned());
        job.error = Some(JobError::new(
            JobErrorKind::TargetLost,
            format!("{detail}; the hub cannot observe or stop its run"),
        ));
        failed.push(json!({
            "jobId": job.job_id,
            "state": job.display_state(now_ms),
            "target": job.target,
            "runId": job.run_id,
            "reason": "target_lost",
        }));
    }
    failed
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
            member
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
            }
            Ok(Err(error)) => member.capability_error = Some(error.to_string()),
            Err(_) => member.capability_error = Some("discovery timed out".to_owned()),
        }
    }
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
            Dispatched::Placed
        }
        Err(error @ HubError::Unanswered(_)) => {
            job.unconfirmed_submissions = job.unconfirmed_submissions.saturating_add(1);
            if job.unconfirmed_submissions >= limit {
                job.state = JobState::Failed;
                job.finished_at_ms = Some(now_ms);
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
            job.error = Some(JobError::new(
                JobErrorKind::SubmissionRejected,
                error.to_string(),
            ));
            Dispatched::Rejected(error)
        }
    }
}

/// Record the status document's watch cursor on a running job. An unchanged cursor keeps the
/// time it last advanced; a new cursor resets both fields.
pub(crate) fn record_cursor(job: &mut Job, status: &Value, now_ms: u64) {
    let cursor = status
        .get("atCursor")
        .or_else(|| status.pointer("/status/atCursor"))
        .and_then(Value::as_str);
    let Some(cursor) = cursor else {
        return;
    };
    if job.last_cursor.as_deref() == Some(cursor) {
        if job.last_cursor_at_ms.is_none() {
            job.last_cursor_at_ms = Some(now_ms);
        }
        return;
    }
    job.last_cursor = Some(cursor.to_owned());
    job.last_cursor_at_ms = Some(now_ms);
}

/// Apply a run status projection to a running job. Returns true when the job finished.
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
    job.state = if result == Some("succeeded") {
        JobState::Succeeded
    } else {
        JobState::Failed
    };
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
            record_cursor(job, &status, now_ms);
            apply_status(job, &status, now_ms);
            Some(status)
        }
        Err(error) => {
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
    finished
}

/// Copy discovery results from `discovered` into the members of the same name.
pub(crate) fn merge_capabilities(members: &mut [Member], discovered: &[Member]) {
    for member in members {
        if let Some(found) = discovered.iter().find(|found| found.name == member.name) {
            member.capabilities.clone_from(&found.capabilities);
            member.capabilities_at_ms = found.capabilities_at_ms;
            member.capability_error.clone_from(&found.capability_error);
        }
    }
}

/// Choose a member for each due job that fits now, highest priority first. Chosen jobs are
/// marked running on their member without a run, so no other pass or request places them
/// again while their submission is in flight.
pub(crate) fn choose(
    state: &mut HubState,
    now_ms: u64,
    loads: &mut BTreeMap<String, Load>,
) -> Vec<(String, String)> {
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

/// Mark any job past its expiry, including a placed one. Returns their IDs.
pub(crate) fn expire(jobs: &mut [Job], now_ms: u64) -> Vec<String> {
    expire_matching(jobs, now_ms, true)
}

/// Mark queued jobs and placed jobs whose target has not confirmed a run past their expiry. A
/// confirmed run is settled by [`settle_expiring`] only after its stop succeeds.
pub(crate) fn expire_unconfirmed(jobs: &mut [Job], now_ms: u64) -> Vec<String> {
    expire_matching(jobs, now_ms, false)
}

fn expire_matching(jobs: &mut [Job], now_ms: u64, confirmed: bool) -> Vec<String> {
    jobs.iter_mut()
        .filter(|job| {
            let placed = job.state == JobState::Running;
            (job.state == JobState::Queued || (placed && (confirmed || job.run_id.is_none())))
                && job.expires_at_ms.is_some_and(|at| at <= now_ms)
        })
        .map(|job| {
            let placed = job.state == JobState::Running;
            let has_run = job.run_id.is_some();
            job.state = JobState::Expired;
            job.finished_at_ms = Some(now_ms);
            job.error = Some(JobError::new(
                JobErrorKind::RunExpired,
                format!(
                    "deadline passed at {}{}",
                    super::clock::rfc3339(job.expires_at_ms.unwrap_or(now_ms)),
                    if !placed {
                        " before the job was placed"
                    } else if has_run {
                        " before the run could be stopped"
                    } else {
                        " before the target confirmed a run"
                    }
                ),
            ));
            job.job_id.clone()
        })
        .collect()
}

/// A placed job whose deadline passed and whose run must be stopped before it is settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExpiringRun {
    pub job_id: String,
    pub target: Option<String>,
    pub run_id: Option<String>,
    pub submission_key: String,
}

/// Running jobs (including jobs still placing) whose deadline has passed.
pub(crate) fn expiring_runs(jobs: &[Job], now_ms: u64) -> Vec<ExpiringRun> {
    jobs.iter()
        .filter(|job| {
            job.state == JobState::Running && job.expires_at_ms.is_some_and(|at| at <= now_ms)
        })
        .map(|job| ExpiringRun {
            job_id: job.job_id.clone(),
            target: job.target.clone(),
            run_id: job.run_id.clone(),
            submission_key: format!("hub-{}-{}", job.job_id, job.attempts + 1),
        })
        .collect()
}

/// Settle placed jobs past their deadline. `stop_errors` holds a failed stop by job ID: the job
/// stays running, records the failure, and a later pass retries the stop. A job without a
/// confirmed run names the submission key a target may still hold instead.
pub(crate) fn settle_expiring(
    jobs: &mut [Job],
    now_ms: u64,
    expiring: &[ExpiringRun],
    stop_errors: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut expired = Vec::new();
    for run in expiring {
        let Some(job) = jobs.iter_mut().find(|job| job.job_id == run.job_id) else {
            continue;
        };
        if job.state != JobState::Running
            || job.run_id != run.run_id
            || !job.expires_at_ms.is_some_and(|at| at <= now_ms)
        {
            continue;
        }
        let deadline = super::clock::rfc3339(job.expires_at_ms.unwrap_or(now_ms));
        let target = run.target.as_deref().unwrap_or("its member");
        if let Some(run_id) = &run.run_id {
            if let Some(error) = stop_errors.get(&run.job_id) {
                job.error = Some(JobError::new(
                    JobErrorKind::RunExpired,
                    format!(
                        "deadline passed at {deadline}; run {run_id} on {target} could not be \
                         stopped: {error}; the hub will retry"
                    ),
                ));
                continue;
            }
        }
        let detail = match &run.run_id {
            Some(run_id) => format!("the hub stopped run {run_id} on {target}"),
            None => format!(
                "{target} had not confirmed a run; a run with submission key {} may still exist there",
                run.submission_key
            ),
        };
        job.state = JobState::Expired;
        job.finished_at_ms = Some(now_ms);
        job.error = Some(JobError::new(
            JobErrorKind::RunExpired,
            format!("deadline passed at {deadline}; {detail}"),
        ));
        expired.push(job.job_id.clone());
    }
    expired
}

/// A running job whose watch cursor stayed unchanged for the daemon's `--max-stall`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StalledRun {
    pub job_id: String,
    pub target: String,
    pub run_id: String,
    pub cursor: String,
    pub still_ms: u64,
}

/// Running jobs whose cursor has stayed unchanged for at least `max_stall_ms`.
pub(crate) fn stalled_runs(jobs: &[Job], now_ms: u64, max_stall_ms: u64) -> Vec<StalledRun> {
    jobs.iter()
        .filter(|job| job.state == JobState::Running)
        .filter_map(|job| {
            let target = job.target.clone()?;
            let run_id = job.run_id.clone()?;
            let cursor = job.last_cursor.clone()?;
            let at = job.last_cursor_at_ms?;
            let still_ms = now_ms.saturating_sub(at);
            (still_ms >= max_stall_ms).then_some(StalledRun {
                job_id: job.job_id.clone(),
                target,
                run_id,
                cursor,
                still_ms,
            })
        })
        .collect()
}

/// The daemon policy and evidence for settling stalled jobs in one pass.
pub(crate) struct StallSettlement<'a> {
    pub now_ms: u64,
    pub max_stall_ms: u64,
    pub stalled: &'a [StalledRun],
    pub stop_errors: &'a BTreeMap<String, String>,
}

/// Fail stalled jobs after their runs were stopped, naming the cursor and how long it was still.
pub(crate) fn settle_stalled(jobs: &mut [Job], settlement: StallSettlement<'_>) -> Vec<Value> {
    let StallSettlement {
        now_ms,
        max_stall_ms,
        stalled,
        stop_errors,
    } = settlement;
    let mut failed = Vec::new();
    for run in stalled {
        let Some(job) = jobs.iter_mut().find(|job| job.job_id == run.job_id) else {
            continue;
        };
        if job.state != JobState::Running
            || job.run_id.as_deref() != Some(run.run_id.as_str())
            || job.last_cursor.as_deref() != Some(run.cursor.as_str())
        {
            continue;
        }
        if let Some(error) = stop_errors.get(&run.job_id) {
            job.error = Some(JobError::new(
                JobErrorKind::Stalled,
                format!(
                    "run {} on {} stayed at cursor {} for {}ms (max stall {}ms); stopping it \
                     failed: {error}; the hub will retry",
                    run.run_id, run.target, run.cursor, run.still_ms, max_stall_ms
                ),
            ));
            continue;
        }
        job.state = JobState::Failed;
        job.finished_at_ms = Some(now_ms);
        job.failure_reason = Some("stalled".to_owned());
        job.error = Some(JobError::new(
            JobErrorKind::Stalled,
            format!(
                "run {} on {} stayed at cursor {} for {}ms (max stall {}ms); the hub stopped it",
                run.run_id, run.target, run.cursor, run.still_ms, max_stall_ms
            ),
        ));
        failed.push(json!({
            "jobId": job.job_id,
            "state": job.display_state(now_ms),
            "target": job.target,
            "runId": job.run_id,
            "cursor": run.cursor,
            "stillMs": run.still_ms,
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
/// jobs, expire stale ones, then place due jobs. The daemon runs the same phases with target
/// calls outside its state lock (see `service::HubService::pass`).
#[cfg(test)]
pub(crate) async fn tick(state: &mut HubState, driver: &dyn HubDriver, now_ms: u64) -> TickReport {
    let mut report = TickReport {
        fired: super::cron::fire_due(state, now_ms),
        ..TickReport::default()
    };
    let results = poll(polls(state), driver).await;
    report.finished = apply_polls(state, results, now_ms);
    let expiring = expiring_runs(&state.jobs, now_ms);
    let mut stop_errors = BTreeMap::new();
    for run in &expiring {
        if let Some(run_id) = &run.run_id {
            let stopped = match &run.target {
                Some(target) => driver.stop(target, run_id).await,
                None => Err(HubError::Target {
                    message: format!("run {run_id} has no member to stop it on"),
                    diagnostic: None,
                }),
            };
            if let Err(error) = stopped {
                stop_errors.insert(run.job_id.clone(), error.to_string());
            }
        }
    }
    report.expired = settle_expiring(&mut state.jobs, now_ms, &expiring, &stop_errors);
    // With every requested stop confirmed, any remaining expired job is queued or placing, so the
    // broad sweep is safe. A failed stop keeps its confirmed run for the next pass.
    report.expired.extend(if stop_errors.is_empty() {
        expire(&mut state.jobs, now_ms)
    } else {
        expire_unconfirmed(&mut state.jobs, now_ms)
    });
    // Probe every member on every pass, so a placed job's member is observed even when no
    // queued job needs placement. Health failures accumulate across passes.
    let members = state.members.iter().collect::<Vec<_>>();
    let mut loads = probe(&members, &state.jobs, driver).await;
    record_health(&mut state.members, &mut state.jobs, &loads, now_ms);
    let lost = lost_targets(state);
    report.target_lost = settle_lost_targets(&mut state.jobs, now_ms, &lost);
    for (job_id, member) in placing(state) {
        if let Some(job) = state.jobs.iter_mut().find(|job| job.job_id == job_id) {
            let dispatched =
                dispatch(job, &member, driver, (now_ms, DEFAULT_UNCONFIRMED_LIMIT)).await;
            let job = job.clone();
            record_dispatch(&mut report, &job, &dispatched);
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
        let job = job.clone();
        record_dispatch(&mut report, &job, &dispatched);
    }
    report.loads = Some(loads);
    report
}
