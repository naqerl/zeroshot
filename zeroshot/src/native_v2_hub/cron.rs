//! Recurring schedules: five-field cron expressions evaluated in an explicit IANA zone.

use std::path::Path;

use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};
use jiff::Timestamp;
use jiff::tz::TimeZone;
use serde_json::{Value, json};

use super::clock::rfc3339;
use super::error::HubError;
use super::jobs::{SourceFacts, remote_reason, source_facts};
use super::model::{HubState, Job, JobState, Schedule, ScheduleState};

/// Parse a five-field cron expression (or an `@daily`-style nickname). POSIX semantics: when
/// both day-of-month and day-of-week are restricted, either may match.
pub(crate) fn parse(expression: &str) -> Result<Cron, HubError> {
    let fields = expression.split_whitespace().count();
    if !(fields == 5 || (fields == 1 && expression.trim_start().starts_with('@'))) {
        return Err(HubError::Usage(format!(
            "invalid --cron {expression:?}: use five fields (minute hour day-of-month month \
             day-of-week) or a nickname such as @daily"
        )));
    }
    CronParser::builder()
        .seconds(Seconds::Disallowed)
        .year(Year::Disallowed)
        .build()
        .parse(expression)
        .map_err(|error| HubError::Usage(format!("invalid --cron {expression:?}: {error}")))
}

/// Resolve an IANA zone name, or the enqueuing machine's zone when `None`.
pub(crate) fn timezone(name: Option<&str>) -> Result<(String, TimeZone), HubError> {
    match name {
        Some(name) => TimeZone::get(name)
            .map(|zone| (name.to_owned(), zone))
            .map_err(|error| HubError::Usage(format!("invalid --cron-tz {name:?}: {error}"))),
        None => {
            let system = TimeZone::system();
            match system.iana_name().map(str::to_owned) {
                Some(name) => Ok((name, system)),
                None => Ok(("UTC".to_owned(), TimeZone::UTC)),
            }
        }
    }
}

/// First fire strictly after `after_ms`, in Unix milliseconds.
pub(crate) fn next_fire(cron: &Cron, zone: &TimeZone, after_ms: u64) -> Result<u64, HubError> {
    let invalid = |error: String| HubError::Usage(format!("cron schedule cannot fire: {error}"));
    let start = Timestamp::from_millisecond(i64::try_from(after_ms).unwrap_or(i64::MAX))
        .map_err(|error| invalid(error.to_string()))?
        .to_zoned(zone.clone());
    let next = cron
        .find_next_occurrence(&start, false)
        .map_err(|error| invalid(error.to_string()))?;
    u64::try_from(next.timestamp().as_millisecond()).map_err(|error| invalid(error.to_string()))
}

/// `next_fire` for a stored schedule.
pub(crate) fn schedule_next(schedule: &Schedule, after_ms: u64) -> Result<u64, HubError> {
    let cron = parse(&schedule.cron)?;
    let (_, zone) = timezone(Some(&schedule.timezone))?;
    next_fire(&cron, &zone, after_ms)
}

/// Wall-clock rendering of an instant in the schedule's zone.
pub(crate) fn local_time(timezone: &str, at_ms: u64) -> Option<String> {
    let zone = TimeZone::get(timezone).ok()?;
    let at = Timestamp::from_millisecond(i64::try_from(at_ms).ok()?).ok()?;
    Some(
        at.to_zoned(zone)
            .strftime("%Y-%m-%dT%H:%M:%S%:z[%Q]")
            .to_string(),
    )
}

/// Queue one job for every active schedule whose fire time has come. Missed fires collapse
/// into one; a fire is skipped while the schedule's previous job is still queued.
pub(crate) fn fire_due(state: &mut HubState, now_ms: u64) -> Vec<Value> {
    let mut fired = Vec::new();
    for index in 0..state.schedules.len() {
        let schedule = &state.schedules[index];
        if schedule.state != ScheduleState::Active
            || schedule.next_fire_ms.is_none_or(|at| at > now_ms)
        {
            continue;
        }
        let previous_queued = schedule.last_job_id.as_ref().is_some_and(|last| {
            state
                .jobs
                .iter()
                .any(|job| &job.job_id == last && job.state == JobState::Queued)
        });
        let job = (!previous_queued).then(|| fire_job(schedule, now_ms));
        let schedule = &mut state.schedules[index];
        let next = schedule_next(schedule, now_ms);
        schedule.next_fire_ms = next.as_ref().ok().copied();
        schedule.error = next.err().map(|error| error.to_string());
        match job {
            Some(job) => {
                schedule.fired += 1;
                schedule.last_fired_ms = Some(now_ms);
                schedule.last_job_id = Some(job.job_id.clone());
                fired.push(json!({ "scheduleId": schedule.schedule_id, "jobId": job.job_id }));
                state.jobs.push(job);
            }
            None => {
                schedule.skipped += 1;
                fired.push(json!({
                    "scheduleId": schedule.schedule_id,
                    "skipped": "previous job is still queued",
                }));
            }
        }
    }
    fired
}

fn fire_job(schedule: &Schedule, now_ms: u64) -> Job {
    let mut job = schedule.template.clone();
    job.job_id = format!("hj_{}", uuid::Uuid::now_v7().simple());
    job.state = JobState::Queued;
    job.schedule_id = Some(schedule.schedule_id.clone());
    job.submission_key = None;
    job.submitted_at_ms = now_ms;
    job.not_before_ms = None;
    job.expires_at_ms = schedule
        .expires_after_ms
        .map(|after| now_ms.saturating_add(after));
    if let Some(folder) = &schedule.repin_from {
        repin(&mut job, folder);
    }
    job
}

/// Re-resolve the working folder's `HEAD` so each fire runs the current commit.
fn repin(job: &mut Job, folder: &Path) {
    let (revision, unpushed) = source_facts(folder);
    let facts = SourceFacts {
        pinned_revision: revision.as_deref(),
        unpushed: unpushed.as_deref(),
        lanes: false,
    };
    job.requirements.remote_unsupported = remote_reason(&facts);
    job.pinned_revision = revision;
}

impl Schedule {
    pub(crate) fn view(&self) -> Value {
        let time = |value: Option<u64>| value.map(rfc3339);
        json!({
            "scheduleId": self.schedule_id,
            "state": self.state,
            "cron": self.cron,
            "timezone": self.timezone,
            "title": self.template.title,
            "placement": self.template.placement,
            "requirements": self.template.requirements,
            "priority": self.template.priority,
            "labels": self.template.labels,
            "createdAt": rfc3339(self.created_at_ms),
            "nextFireAt": time(self.next_fire_ms),
            "nextFireLocal": self.next_fire_ms.and_then(|at| local_time(&self.timezone, at)),
            "lastFiredAt": time(self.last_fired_ms),
            "lastJobId": self.last_job_id,
            "fired": self.fired,
            "skipped": self.skipped,
            "expiresAfterMs": self.expires_after_ms,
            "removedAt": time(self.removed_at_ms),
            "error": self.error,
        })
    }
}
