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
/// Discovered capabilities are refreshed after this age by placement passes.
pub(crate) const CAPABILITY_TTL_MS: u64 = 5 * 60 * 1000;

/// Outcome of one dispatcher pass.
#[derive(Debug, Default)]
pub(crate) struct TickReport {
    pub fired: Vec<Value>,
    pub dispatched: Vec<Value>,
    pub finished: Vec<Value>,
    pub expired: Vec<String>,
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
/// an unconfirmed one keeps it placing (see [`Dispatched::Unconfirmed`]).
pub(crate) async fn dispatch(
    job: &mut Job,
    member: &str,
    driver: &dyn HubDriver,
    now_ms: u64,
) -> Dispatched {
    let attempt = job.attempts + 1;
    let request = SubmitRequest {
        member,
        job,
        submission_key: format!("hub-{}-{attempt}", job.job_id),
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

/// Mark queued jobs past their expiry. Returns their IDs.
pub(crate) fn expire(jobs: &mut [Job], now_ms: u64) -> Vec<String> {
    jobs.iter_mut()
        .filter(|job| {
            job.state == JobState::Queued && job.expires_at_ms.is_some_and(|at| at <= now_ms)
        })
        .map(|job| {
            job.state = JobState::Expired;
            job.finished_at_ms = Some(now_ms);
            job.job_id.clone()
        })
        .collect()
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
    report.expired = expire(&mut state.jobs, now_ms);
    for (job_id, member) in placing(state) {
        if let Some(job) = state.jobs.iter_mut().find(|job| job.job_id == job_id) {
            let dispatched = dispatch(job, &member, driver, now_ms).await;
            let job = job.clone();
            record_dispatch(&mut report, &job, &dispatched);
        }
    }
    if due_jobs(&state.jobs, now_ms).is_empty() {
        return report;
    }
    discover(&mut state.members, driver, now_ms, CAPABILITY_TTL_MS).await;
    let active = state
        .members
        .iter()
        .filter(|member| member.state == MemberState::Active)
        .collect::<Vec<_>>();
    let mut loads = probe(&active, &state.jobs, driver).await;
    for (job_id, member) in choose(state, now_ms, &mut loads) {
        let Some(job) = state.jobs.iter_mut().find(|job| job.job_id == job_id) else {
            continue;
        };
        let dispatched = dispatch(job, &member, driver, now_ms).await;
        let job = job.clone();
        record_dispatch(&mut report, &job, &dispatched);
    }
    report.loads = Some(loads);
    report
}
