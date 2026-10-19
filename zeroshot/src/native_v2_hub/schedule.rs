use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::future::join_all;
use serde_json::{Value, json};

use super::driver::{HubDriver, SubmitRequest};
use super::error::HubError;
use super::model::{HubState, Job, JobState, LOCAL_MEMBER, Member, MemberState, TokenUsage};
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

/// Place one job on `member`. A failed submission marks the job failed and returns the error.
pub(crate) async fn dispatch(
    job: &mut Job,
    member: &str,
    driver: &dyn HubDriver,
    now_ms: u64,
) -> Result<(), HubError> {
    let attempt = job.attempts + 1;
    let request = SubmitRequest {
        member,
        job,
        submission_key: format!("hub-{}-{attempt}", job.job_id),
    };
    match driver.submit(request).await {
        Ok(run_id) => {
            job.attempts = attempt;
            job.state = JobState::Running;
            job.target = Some(member.to_owned());
            job.run_id = Some(run_id);
            job.dispatched_at_ms = Some(now_ms);
            job.error = None;
            Ok(())
        }
        Err(error) => {
            job.attempts = attempt;
            job.state = JobState::Failed;
            job.target = Some(member.to_owned());
            job.finished_at_ms = Some(now_ms);
            job.error = Some(error.to_string());
            Err(error)
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
    job.token_usage = status
        .pointer("/status/metadata/tokenUsage")
        .and_then(|usage| serde_json::from_value::<TokenUsage>(usage.clone()).ok());
    job.finished_at_ms = Some(now_ms);
    job.error = None;
    true
}

/// Refresh one running job from its target.
pub(crate) async fn refresh(job: &mut Job, driver: &dyn HubDriver, now_ms: u64) -> Option<Value> {
    let (Some(target), Some(run_id)) = (job.target.clone(), job.run_id.clone()) else {
        return None;
    };
    match driver.status(&target, &run_id).await {
        Ok(status) => {
            apply_status(job, &status, now_ms);
            Some(status)
        }
        Err(error) => {
            job.error = Some(error.to_string());
            None
        }
    }
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

/// One dispatcher pass: fire due cron schedules, refresh running jobs, expire stale ones, then
/// place due jobs. Jobs that cannot be placed stay queued; a lower-priority job may still use a
/// member the head cannot.
pub(crate) async fn tick(state: &mut HubState, driver: &dyn HubDriver, now_ms: u64) -> TickReport {
    let mut report = TickReport {
        fired: super::cron::fire_due(state, now_ms),
        ..TickReport::default()
    };
    for job in state
        .jobs
        .iter_mut()
        .filter(|job| job.state == JobState::Running)
    {
        refresh(job, driver, now_ms).await;
        if job.state.is_terminal() {
            report.finished.push(json!({
                "jobId": job.job_id,
                "state": job.display_state(now_ms),
                "target": job.target,
                "runId": job.run_id,
            }));
        }
    }
    report.expired = expire(&mut state.jobs, now_ms);
    let due = due_jobs(&state.jobs, now_ms);
    if due.is_empty() {
        return report;
    }
    discover(&mut state.members, driver, now_ms, CAPABILITY_TTL_MS).await;
    let active = state
        .members
        .iter()
        .filter(|member| member.state == MemberState::Active)
        .collect::<Vec<_>>();
    let mut loads = probe(&active, &state.jobs, driver).await;
    for index in due {
        let job = &state.jobs[index];
        let Placement::Place(member) =
            place(&state.members, &job.placement, &job.requirements, &loads)
        else {
            continue;
        };
        let job = &mut state.jobs[index];
        let entry = json!({ "jobId": job.job_id, "target": member });
        match dispatch(job, &member, driver, now_ms).await {
            Ok(()) => {
                if let Some(load) = loads.get_mut(&member) {
                    load.active = load.active.saturating_add(1);
                }
                let mut entry = entry;
                entry["runId"] = json!(job.run_id);
                report.dispatched.push(entry);
            }
            Err(error) => {
                let mut entry = entry;
                entry["error"] = json!(error.to_string());
                report.failed.push(entry);
            }
        }
    }
    report.loads = Some(loads);
    report
}
