//! `zeroshot hub`: a pool of registered targets with placement, a durable queue, recurring
//! schedules, and usage accounting.
//!
//! One daemon per hub folder (`hub daemon run`, supervised by systemd or Docker through
//! `hub install`) dispatches continuously. Every other command checks that exactly one such
//! daemon is active, then reads or changes `state.json` under the same advisory lock the
//! daemon holds for each pass, so no IPC protocol is needed. The hub never changes run contracts. It chooses one member per job and delegates every
//! target operation to the same CLI commands a user would run (`run --detach`, `list`,
//! `status`, `watch`, `logs`, `attach`, `force-stop`), so registry, authentication, admission,
//! and observation behave exactly as they do without the hub.

use std::io::Write;
use std::time::Duration;

use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::{
    HubCommand, HubEnqueue, HubMemberAdd, HubMemberUpdate, HubPs, HubStats, HubSubmit,
};

use crate::native_v2_target::{
    FileTargetRegistry, TargetAccess, TargetRegistry, default_target_registry_path,
};

mod clock;
mod cron;
mod daemon;
mod driver;
mod error;
mod install;
mod jobs;
mod model;
mod placement;
mod schedule;
mod stats;
mod store;

#[cfg(test)]
#[path = "native_v2_hub/tests.rs"]
mod tests;

use clock::{now_ms, parse_duration_ms, parse_future_ms, parse_past_ms};
use driver::{HubDriver, ProcessDriver, passthrough, route};
pub(crate) use error::HubError;
use model::{HubState, Job, JobState, LOCAL_MEMBER, Member, MemberState, Schedule, ScheduleState};
use placement::{Fit, Placement, describe, fit, matching_members, place, rejections};
use store::HubStore;

const WAIT_POLL: Duration = Duration::from_secs(5);

struct HubContext {
    store: HubStore,
    driver: Box<dyn HubDriver>,
    registry: FileTargetRegistry,
}

/// Execute one hub command. Returns the process exit code for successful commands; a
/// passthrough command reports its child's exit code.
pub(crate) async fn execute_hub(
    command: HubCommand,
    output: &mut impl Write,
) -> Result<i32, HubError> {
    let probe = daemon::production_probe();
    if daemon::requires_daemon(&command) {
        daemon::guard(probe.as_ref()).await?;
    }
    let registry_path = default_target_registry_path()
        .map_err(|error| HubError::storage("target registry path is unavailable", error))?;
    let context = HubContext {
        store: HubStore::new(HubStore::default_directory(&registry_path)),
        driver: Box::new(ProcessDriver::production(registry_path.clone())?),
        registry: FileTargetRegistry::new(registry_path.clone()),
    };
    match command {
        HubCommand::Daemon { interval } => {
            daemon::run(&context, &interval, output).await.map(|()| 0)
        }
        HubCommand::DaemonStatus => daemon::status(&context, probe.as_ref(), output).await,
        HubCommand::Install(request) => {
            let env = install::InstallEnv::detect(&registry_path, context.store.directory())?;
            install::install(&env, request, probe.as_ref(), output)
                .await
                .map(|()| 0)
        }
        HubCommand::Uninstall(request) => {
            let env = install::InstallEnv::detect(&registry_path, context.store.directory())?;
            install::uninstall(&env, request, output).await.map(|()| 0)
        }
        HubCommand::Add(request) => add(&context, request, output).await.map(|()| 0),
        HubCommand::Update(request) => update(&context, request, output).map(|()| 0),
        HubCommand::Remove { name } => remove(&context, &name, output).map(|()| 0),
        HubCommand::List { tags, probe } => list(&context, &tags, probe, output).await.map(|()| 0),
        HubCommand::Submit(request) => submit(&context, *request, output).await,
        HubCommand::Enqueue(request) => enqueue(&context, *request, output).map(|()| 0),
        HubCommand::Ps(request) => ps(&context, request, output).await.map(|()| 0),
        HubCommand::Status { job } => status(&context, &job, output).await.map(|()| 0),
        HubCommand::Dequeue { job } => dequeue(&context, &job, output).map(|()| 0),
        HubCommand::Dispatch => {
            let (_, view) = daemon::pass(&context).await?;
            write_json(output, &view).map(|()| 0)
        }
        HubCommand::Stats(request) => stats(&context, request, output).map(|()| 0),
        observation => observe(&context, observation).await,
    }
}

fn write_json(output: &mut impl Write, value: &Value) -> Result<(), HubError> {
    serde_json::to_writer(&mut *output, value)
        .map_err(std::io::Error::from)
        .and_then(|()| output.write_all(b"\n"))
        .and_then(|()| output.flush())
        .map_err(|error| HubError::storage("output could not be written", error))
}

fn member_document(member: &Member) -> Value {
    json!({ "schema": "zeroshot.hub.member/v1", "member": member.view() })
}

fn job_document(job: &Job, now: u64) -> Value {
    json!({ "schema": "zeroshot.hub.job/v1", "job": job.view(now) })
}

async fn add(
    context: &HubContext,
    request: HubMemberAdd,
    output: &mut impl Write,
) -> Result<(), HubError> {
    if request.name != LOCAL_MEMBER {
        context.registry.get(&request.name).map_err(|error| {
            HubError::MemberNotFound(format!(
                "{error}; register it first with `zeroshot target add {} --url ORIGIN`",
                request.name
            ))
        })?;
    }
    let mut member = Member {
        tags: request.tags,
        slots: request.slots,
        priority: request.priority,
        ..Member::new(request.name, now_ms())
    };
    schedule::discover(
        std::slice::from_mut(&mut member),
        context.driver.as_ref(),
        now_ms(),
        0,
    )
    .await;
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    if state.member(&member.name).is_some() {
        return Err(HubError::Conflict(format!(
            "{} is already a hub member; use `zeroshot hub update`",
            member.name
        )));
    }
    state.members.push(member.clone());
    context.store.save(&lock, &state)?;
    write_json(output, &member_document(&member))
}

fn update(
    context: &HubContext,
    request: HubMemberUpdate,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    let member = state
        .members
        .iter_mut()
        .find(|member| member.name == request.name)
        .ok_or_else(|| HubError::MemberNotFound(format!("{} is not a hub member", request.name)))?;
    member.tags.retain(|tag| !request.remove_tags.contains(tag));
    for tag in request.add_tags {
        if !member.tags.contains(&tag) {
            member.tags.push(tag);
        }
    }
    member.tags.sort();
    if let Some(slots) = request.slots {
        member.slots = slots;
    }
    if let Some(priority) = request.priority {
        member.priority = priority;
    }
    if let Some(member_state) = request.state {
        member.state = member_state.into();
    }
    let document = member_document(member);
    context.store.save(&lock, &state)?;
    write_json(output, &document)
}

fn remove(context: &HubContext, name: &str, output: &mut impl Write) -> Result<(), HubError> {
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    let before = state.members.len();
    state.members.retain(|member| member.name != name);
    if state.members.len() == before {
        return Err(HubError::MemberNotFound(format!(
            "{name} is not a hub member"
        )));
    }
    context.store.save(&lock, &state)?;
    write_json(
        output,
        &json!({ "schema": "zeroshot.hub.member/v1", "removed": name }),
    )
}

fn member_kind(context: &HubContext, name: &str) -> (Value, Value) {
    if name == LOCAL_MEMBER {
        return (json!("local"), Value::Null);
    }
    match context.registry.get(name) {
        Ok(record) => {
            let kind = match record.access {
                TargetAccess::Direct => "direct",
                TargetAccess::Hosted { .. } => "hosted",
            };
            (json!(kind), json!(record.origin))
        }
        Err(_) => (json!("unregistered"), Value::Null),
    }
}

async fn list(
    context: &HubContext,
    tags: &[String],
    probe: bool,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let state = {
        let lock = context.store.lock()?;
        let mut state = context.store.load(&lock)?;
        if probe {
            schedule::discover(&mut state.members, context.driver.as_ref(), now_ms(), 0).await;
            context.store.save(&lock, &state)?;
        }
        state
    };
    let members = state
        .members
        .iter()
        .filter(|member| tags.iter().all(|tag| member.tags.contains(tag)))
        .collect::<Vec<_>>();
    let probed = members
        .iter()
        .copied()
        .filter(|member| member.state != MemberState::Disabled)
        .collect::<Vec<_>>();
    let loads = if probe {
        Some(schedule::probe(&probed, &state.jobs, context.driver.as_ref()).await)
    } else {
        None
    };
    let now = now_ms();
    let targets = members
        .iter()
        .map(|member| {
            let mut view = member.view();
            let (kind, origin) = member_kind(context, &member.name);
            view["kind"] = kind;
            view["origin"] = origin;
            view["hubRunning"] = json!(count_jobs(&state, |job| job.state == JobState::Running
                && job.target.as_deref() == Some(member.name.as_str())));
            let queued = state.jobs.iter().filter(|job| {
                job.state == JobState::Queued
                    && matching_members(std::slice::from_ref(member), &job.placement).len() == 1
            });
            let (mut matching, mut rejects) = (0, Vec::new());
            for job in queued {
                match fit(member, &job.requirements) {
                    Fit::Unsupported(reason) => {
                        rejects.push(json!({ "jobId": job.job_id, "reason": reason }));
                    }
                    Fit::Supported | Fit::Unknown => matching += 1,
                }
            }
            view["queuedMatching"] = json!(matching);
            view["rejects"] = json!(rejects);
            if let Some(load) = loads.as_ref().and_then(|loads| loads.get(&member.name)) {
                view["health"] = json!(if load.reachable {
                    "ready"
                } else {
                    "unreachable"
                });
                view["active"] = json!(load.active);
                view["free"] = json!(member.slots.saturating_sub(load.active));
                view["probeError"] = json!(load.error);
                view["probedAt"] = json!(clock::rfc3339(now));
            }
            view
        })
        .collect::<Vec<_>>();
    write_json(
        output,
        &json!({ "schema": "zeroshot.hub.targets/v1", "targets": targets }),
    )
}

fn count_jobs(state: &HubState, predicate: impl Fn(&Job) -> bool) -> usize {
    state.jobs.iter().filter(|job| predicate(job)).count()
}

/// Return an earlier job accepted with the same hub submission key, if any.
fn idempotent<'a>(state: &'a HubState, job: &Job) -> Option<&'a Job> {
    let key = job.submission_key.as_ref()?;
    state
        .jobs
        .iter()
        .find(|existing| existing.submission_key.as_ref() == Some(key))
}

async fn submit(
    context: &HubContext,
    request: HubSubmit,
    output: &mut impl Write,
) -> Result<i32, HubError> {
    let started = now_ms();
    let mut job = jobs::new_job(&request.job, started)?;
    let deadline = match &request.wait {
        None => None,
        Some(None) => Some(u64::MAX),
        Some(Some(duration)) => Some(started.saturating_add(parse_duration_ms(duration)?)),
    };
    loop {
        let lock = context.store.lock()?;
        let mut state = context.store.load(&lock)?;
        if let Some(existing) = idempotent(&state, &job) {
            write_json(output, &job_document(existing, now_ms()))?;
            return Ok(0);
        }
        let now = now_ms();
        schedule::discover(
            &mut state.members,
            context.driver.as_ref(),
            now,
            schedule::CAPABILITY_TTL_MS,
        )
        .await;
        if let Some(rejected) = rejections(&state.members, &job.placement, &job.requirements) {
            context.store.save(&lock, &state)?;
            return Err(HubError::no_eligible(describe(&job.placement), rejected));
        }
        let report = schedule::tick(&mut state, context.driver.as_ref(), now).await;
        let loads = match report.loads {
            Some(loads) => loads,
            None => {
                let members = matching_members(&state.members, &job.placement);
                schedule::probe(&members, &state.jobs, context.driver.as_ref()).await
            }
        };
        match place(&state.members, &job.placement, &job.requirements, &loads) {
            Placement::Place(member) => {
                let placed =
                    schedule::dispatch(&mut job, &member, context.driver.as_ref(), now).await;
                state.jobs.push(job.clone());
                context.store.save(&lock, &state)?;
                drop(lock);
                placed?;
                write_json(output, &job_document(&job, now))?;
                if request.detach {
                    return Ok(0);
                }
                return follow(&job).await;
            }
            Placement::NoEligible { rejected } => {
                context.store.save(&lock, &state)?;
                return Err(HubError::no_eligible(describe(&job.placement), rejected));
            }
            Placement::Busy { .. } if request.queue => {
                state.jobs.push(job.clone());
                context.store.save(&lock, &state)?;
                write_json(output, &job_document(&job, now))?;
                return Ok(0);
            }
            Placement::Busy { .. } => {
                context.store.save(&lock, &state)?;
                drop(lock);
                match deadline {
                    None => return Err(HubError::NoCapacity(describe(&job.placement))),
                    Some(deadline) if now_ms() >= deadline => {
                        return Err(HubError::WaitTimeout(describe(&job.placement)));
                    }
                    Some(deadline) => {
                        let remaining = Duration::from_millis(deadline.saturating_sub(now_ms()));
                        tokio::time::sleep(remaining.min(WAIT_POLL)).await;
                    }
                }
            }
        }
    }
}

async fn follow(job: &Job) -> Result<i32, HubError> {
    let (Some(target), Some(run_id)) = (&job.target, &job.run_id) else {
        return Ok(0);
    };
    let mut arguments = vec!["watch".to_owned(), run_id.clone()];
    arguments.extend(route(target));
    passthrough(arguments).await
}

fn enqueue(
    context: &HubContext,
    request: HubEnqueue,
    output: &mut impl Write,
) -> Result<(), HubError> {
    if request.cron.is_some() {
        return enqueue_schedule(context, request, output);
    }
    let now = now_ms();
    let mut job = jobs::new_job(&request.job, now)?;
    job.priority = request.priority;
    job.not_before_ms = request
        .at
        .as_deref()
        .map(|at| parse_future_ms(at, now))
        .transpose()?;
    job.expires_at_ms = request
        .expires
        .as_deref()
        .map(|expires| parse_future_ms(expires, now))
        .transpose()?;
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    if let Some(existing) = idempotent(&state, &job) {
        return write_json(output, &job_document(existing, now));
    }
    if let Some(rejected) = rejections(&state.members, &job.placement, &job.requirements) {
        return Err(HubError::no_eligible(describe(&job.placement), rejected));
    }
    let document = job_document(&job, now);
    state.jobs.push(job);
    context.store.save(&lock, &state)?;
    write_json(output, &document)
}

fn schedule_document(schedule: &Schedule) -> Value {
    json!({ "schema": "zeroshot.hub.schedule/v1", "schedule": schedule.view() })
}

/// Store a recurring schedule; the daemon queues one job per fire.
fn enqueue_schedule(
    context: &HubContext,
    request: HubEnqueue,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let now = now_ms();
    let expression = request.cron.unwrap_or_default();
    let cron = cron::parse(&expression)?;
    let (timezone, zone) = cron::timezone(request.cron_timezone.as_deref())?;
    let next_fire_ms = cron::next_fire(&cron, &zone, now)?;
    let expires_after_ms = request
        .expires
        .as_deref()
        .map(|expires| {
            parse_duration_ms(expires).map_err(|_| {
                HubError::Usage(format!(
                    "--expires {expires:?} with --cron must be a DURATION such as 2h"
                ))
            })
        })
        .transpose()?;
    let mut template = jobs::new_job(&request.job, now)?;
    template.priority = request.priority;
    let repin_from = (request.job.repository.is_none() && request.job.revision.is_none())
        .then(|| jobs::working_folder(&request.job))
        .transpose()?;
    let schedule_id = format!("hs_{}", uuid::Uuid::now_v7().simple());
    template.job_id.clone_from(&schedule_id);
    let schedule = Schedule {
        schedule_id,
        state: ScheduleState::Active,
        cron: expression,
        timezone,
        submission_key: template.submission_key.take(),
        template,
        repin_from,
        expires_after_ms,
        created_at_ms: now,
        next_fire_ms: Some(next_fire_ms),
        last_fired_ms: None,
        last_job_id: None,
        fired: 0,
        skipped: 0,
        removed_at_ms: None,
        error: None,
    };
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    if let Some(existing) = schedule.submission_key.as_ref().and_then(|key| {
        state
            .schedules
            .iter()
            .find(|existing| existing.submission_key.as_ref() == Some(key))
    }) {
        return write_json(output, &schedule_document(existing));
    }
    let template = &schedule.template;
    if let Some(rejected) = rejections(&state.members, &template.placement, &template.requirements)
    {
        return Err(HubError::no_eligible(
            describe(&template.placement),
            rejected,
        ));
    }
    let document = schedule_document(&schedule);
    state.schedules.push(schedule);
    context.store.save(&lock, &state)?;
    write_json(output, &document)
}

fn state_matches(job: &Job, filter: &str, now: u64) -> bool {
    let state = job.display_state(now);
    match filter {
        "all" => true,
        "active" => matches!(state, "queued" | "scheduled" | "running"),
        filter => state == filter,
    }
}

async fn ps(context: &HubContext, request: HubPs, output: &mut impl Write) -> Result<(), HubError> {
    const STATES: [&str; 10] = [
        "active",
        "all",
        "recurring",
        "queued",
        "scheduled",
        "running",
        "succeeded",
        "failed",
        "dequeued",
        "expired",
    ];
    if !STATES.contains(&request.state.as_str()) {
        return Err(HubError::Usage(format!(
            "invalid --state {:?}: use one of {}",
            request.state,
            STATES.join(", ")
        )));
    }
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    let now = now_ms();
    for job in state
        .jobs
        .iter_mut()
        .filter(|job| job.state == JobState::Running)
    {
        schedule::refresh(job, context.driver.as_ref(), now).await;
    }
    context.store.save(&lock, &state)?;
    drop(lock);
    let mut selected = state
        .jobs
        .iter()
        .filter(|job| state_matches(job, &request.state, now))
        .filter(|job| request.target.is_none() || job.target == request.target)
        .filter(|job| {
            request
                .labels
                .iter()
                .all(|(key, value)| job.labels.get(key) == Some(value))
        })
        .collect::<Vec<_>>();
    if let Some(limit) = request.limit {
        let skip = selected.len().saturating_sub(limit);
        selected.drain(..skip);
    }
    if request.state == "recurring" {
        selected.clear();
    }
    let views = selected.iter().map(|job| job.view(now)).collect::<Vec<_>>();
    let schedules = state
        .schedules
        .iter()
        .filter(|schedule| match request.state.as_str() {
            "all" => true,
            "active" | "recurring" => schedule.state == ScheduleState::Active,
            _ => false,
        })
        .filter(|schedule| {
            request.target.is_none() || schedule.template.placement.target == request.target
        })
        .filter(|schedule| {
            request
                .labels
                .iter()
                .all(|(key, value)| schedule.template.labels.get(key) == Some(value))
        })
        .map(Schedule::view)
        .collect::<Vec<_>>();
    write_json(
        output,
        &json!({ "schema": "zeroshot.hub.jobs/v1", "jobs": views, "schedules": schedules }),
    )
}

async fn status(
    context: &HubContext,
    job_id: &str,
    output: &mut impl Write,
) -> Result<(), HubError> {
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    let index = state
        .job_index(job_id)
        .ok_or_else(|| HubError::JobNotFound(job_id.to_owned()))?;
    let now = now_ms();
    let run = if state.jobs[index].run_id.is_some() {
        let job = &mut state.jobs[index];
        let refreshed = if job.state == JobState::Running {
            schedule::refresh(job, context.driver.as_ref(), now).await
        } else {
            None
        };
        match (refreshed, &job.target, &job.run_id) {
            (Some(status), _, _) => Some(status),
            (None, Some(target), Some(run_id)) => context.driver.status(target, run_id).await.ok(),
            _ => None,
        }
    } else {
        None
    };
    context.store.save(&lock, &state)?;
    let mut document = job_document(&state.jobs[index], now);
    document["run"] = run.unwrap_or(Value::Null);
    write_json(output, &document)
}

fn dequeue(context: &HubContext, job_id: &str, output: &mut impl Write) -> Result<(), HubError> {
    let lock = context.store.lock()?;
    let mut state = context.store.load(&lock)?;
    if let Some(schedule) = state
        .schedules
        .iter_mut()
        .find(|schedule| schedule.schedule_id == job_id)
    {
        if schedule.state == ScheduleState::Removed {
            return Err(HubError::InvalidState(format!(
                "hub schedule {job_id} is already removed"
            )));
        }
        schedule.state = ScheduleState::Removed;
        schedule.removed_at_ms = Some(now_ms());
        schedule.next_fire_ms = None;
        let document = schedule_document(schedule);
        context.store.save(&lock, &state)?;
        return write_json(output, &document);
    }
    let index = state
        .job_index(job_id)
        .ok_or_else(|| HubError::JobNotFound(job_id.to_owned()))?;
    let now = now_ms();
    let job = &mut state.jobs[index];
    if job.state != JobState::Queued {
        return Err(HubError::InvalidState(format!(
            "hub job {job_id} is {}; only queued or scheduled jobs can be dequeued{}",
            job.display_state(now),
            if job.state == JobState::Running {
                "; use `zeroshot hub force-stop` to stop its run"
            } else {
                ""
            }
        )));
    }
    job.state = JobState::Dequeued;
    job.finished_at_ms = Some(now);
    let document = job_document(job, now);
    context.store.save(&lock, &state)?;
    write_json(output, &document)
}

fn stats(context: &HubContext, request: HubStats, output: &mut impl Write) -> Result<(), HubError> {
    let now = now_ms();
    let by = request
        .by
        .iter()
        .map(|dimension| stats::Dimension::parse(dimension))
        .collect::<Result<Vec<_>, _>>()?;
    let since_ms = request
        .since
        .as_deref()
        .map(|since| parse_past_ms(since, now))
        .transpose()?;
    let until_ms = request
        .until
        .as_deref()
        .map(|until| parse_past_ms(until, now))
        .transpose()?;
    let prices = request
        .prices
        .as_ref()
        .map(|path| {
            let bytes = std::fs::read(path).map_err(|error| {
                HubError::Usage(format!(
                    "--prices {} could not be read: {error}",
                    path.display()
                ))
            })?;
            serde_json::from_slice::<stats::Prices>(&bytes).map_err(|error| {
                HubError::Usage(format!(
                    "--prices {} is not a price table: {error}",
                    path.display()
                ))
            })
        })
        .transpose()?;
    let state = {
        let lock = context.store.lock()?;
        context.store.load(&lock)?
    };
    let query = stats::StatsQuery {
        by: &by,
        since_ms,
        until_ms,
        prices: prices.as_ref(),
    };
    write_json(output, &stats::aggregate(&state.jobs, &query))
}

/// Resolve a hub job to its run and delegate an observation or stop command.
async fn observe(context: &HubContext, command: HubCommand) -> Result<i32, HubError> {
    let job_id = match &command {
        HubCommand::Watch { job, .. }
        | HubCommand::Attach { job, .. }
        | HubCommand::ForceStop { job } => job.clone(),
        HubCommand::Logs(logs) => logs.job.clone(),
        _ => return Err(HubError::Usage("unsupported hub command".to_owned())),
    };
    let state = {
        let lock = context.store.lock()?;
        context.store.load(&lock)?
    };
    let job = state
        .job_index(&job_id)
        .map(|index| &state.jobs[index])
        .ok_or_else(|| HubError::JobNotFound(job_id.clone()))?;
    let (Some(target), Some(run_id)) = (&job.target, &job.run_id) else {
        return Err(HubError::InvalidState(format!(
            "hub job {job_id} is {} and has no run yet{}",
            job.display_state(now_ms()),
            if job.state == JobState::Queued {
                "; use `zeroshot hub dequeue` to cancel it"
            } else {
                ""
            }
        )));
    };
    let mut arguments = match command {
        HubCommand::Watch { after, .. } => {
            let mut arguments = vec!["watch".to_owned(), run_id.clone()];
            arguments.extend(
                after
                    .into_iter()
                    .flat_map(|after| ["--after".to_owned(), after]),
            );
            arguments
        }
        HubCommand::Logs(logs) => {
            let mut arguments = vec!["logs".to_owned(), run_id.clone()];
            arguments.extend(
                logs.after
                    .into_iter()
                    .flat_map(|after| ["--after".to_owned(), after]),
            );
            arguments.extend(
                logs.execution
                    .into_iter()
                    .flat_map(|execution| ["--execution".to_owned(), execution]),
            );
            arguments
        }
        HubCommand::Attach { execution, .. } => {
            vec!["attach".to_owned(), run_id.clone(), execution]
        }
        _ => vec!["force-stop".to_owned(), run_id.clone()],
    };
    arguments.extend(route(target));
    passthrough(arguments).await
}
