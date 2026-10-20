//! The daemon's hub: the single writer of hub state.
//!
//! State lives in memory behind one async mutex and is written to `state.json` after every
//! change. The mutex is held only for in-memory work: target calls (discovery, load probes,
//! status polls, submissions) run on snapshots outside it and their results are merged back by
//! job ID or member name, so reads never wait for a slow target. A second mutex serializes
//! placement decisions (dispatcher passes and `submit`), so a job is never placed twice.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use serde_json::{Value, json};
use tokio::sync::Mutex;

use super::clock::{self, now_ms};
use super::cron;
use super::driver::{HubDriver, route};
use super::error::HubError;
use super::metrics::Metrics;
use super::model::{
    HubState, Job, JobState, LOCAL_MEMBER, Member, MemberState, Schedule, ScheduleState,
};
use super::placement::{Fit, Placement, describe, fit, matching_members, place, rejections};
use super::schedule::{self, TickReport};
use super::stats;
use super::store::HubStore;
use super::wire::{
    AddMember, JobFile, JobQuery, MemberQuery, StatsRequest, StreamRequest, UpdateMember,
};
use crate::native_v2_target::{FileTargetRegistry, TargetAccess, TargetRegistry};

/// Run arguments whose value is a file the client uploads with the job.
pub(crate) const FILE_FLAGS: [&str; 5] = [
    "--input",
    "--graph",
    "--runtime-config",
    "--uniform-runtime-config",
    "--environment",
];

pub(crate) struct HubService {
    store: HubStore,
    driver: Arc<dyn HubDriver>,
    registry: FileTargetRegistry,
    state: Mutex<HubState>,
    placement: Mutex<()>,
    metrics: Metrics,
}

pub(crate) fn member_document(member: &Member) -> Value {
    json!({ "schema": "zeroshot.hub.member/v1", "member": member.view() })
}

pub(crate) fn job_document(job: &Job, now: u64) -> Value {
    json!({ "schema": "zeroshot.hub.job/v1", "job": job.view(now) })
}

pub(crate) fn schedule_document(schedule: &Schedule) -> Value {
    json!({ "schema": "zeroshot.hub.schedule/v1", "schedule": schedule.view() })
}

impl HubService {
    /// Load the hub folder's state and put jobs interrupted mid-placement back in the queue.
    pub(crate) fn open(
        store: HubStore,
        driver: Arc<dyn HubDriver>,
        registry: FileTargetRegistry,
    ) -> Result<Self, HubError> {
        let mut state = {
            let lock = store.lock()?;
            store.load(&lock)?
        };
        if schedule::recover_placing(&mut state) > 0 {
            let lock = store.lock()?;
            store.save(&lock, &state)?;
        }
        Ok(Self {
            store,
            driver,
            registry,
            state: Mutex::new(state),
            placement: Mutex::new(()),
            metrics: Metrics::default(),
        })
    }

    /// Export token counters for these job label keys as well.
    #[must_use]
    pub(crate) fn with_metric_labels(mut self, keys: Vec<String>) -> Self {
        self.metrics = Metrics::new(keys);
        self
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// The Prometheus exposition document of this hub.
    pub(crate) async fn render_metrics(&self) -> String {
        let state = self.snapshot().await;
        self.metrics.render(&state, now_ms())
    }

    pub(crate) fn store(&self) -> &HubStore {
        &self.store
    }

    pub(crate) async fn snapshot(&self) -> HubState {
        self.state.lock().await.clone()
    }

    /// Apply `change` to a copy of the state, persist it, then publish it. A failed change or
    /// write leaves the state untouched.
    async fn mutate<R>(
        &self,
        change: impl FnOnce(&mut HubState) -> Result<R, HubError>,
    ) -> Result<R, HubError> {
        let mut state = self.state.lock().await;
        let mut next = state.clone();
        let result = change(&mut next)?;
        if next != *state {
            let lock = self.store.lock()?;
            self.store.save(&lock, &next)?;
            *state = next;
        }
        Ok(result)
    }

    fn driver(&self) -> &dyn HubDriver {
        self.driver.as_ref()
    }

    // ---------------------------------------------------------------- members

    pub(crate) async fn add(&self, request: AddMember) -> Result<Value, HubError> {
        if request.name != LOCAL_MEMBER {
            self.registry.get(&request.name).map_err(|error| {
                HubError::MemberNotFound(format!(
                    "{error}; register it on the hub host first with `zeroshot target add {} \
                     --url ORIGIN`",
                    request.name
                ))
            })?;
        }
        let mut member = Member {
            tags: request.tags,
            slots: request.slots.max(1),
            priority: request.priority,
            ..Member::new(request.name, now_ms())
        };
        schedule::discover(
            std::slice::from_mut(&mut member),
            self.driver(),
            now_ms(),
            0,
        )
        .await;
        self.mutate(|state| {
            if state.member(&member.name).is_some() {
                return Err(HubError::Conflict(format!(
                    "{} is already a hub member; use `zeroshot hub update`",
                    member.name
                )));
            }
            state.members.push(member.clone());
            Ok(member_document(&member))
        })
        .await
    }

    pub(crate) async fn update(
        &self,
        name: &str,
        request: UpdateMember,
    ) -> Result<Value, HubError> {
        self.mutate(|state| {
            let member = state
                .members
                .iter_mut()
                .find(|member| member.name == name)
                .ok_or_else(|| HubError::MemberNotFound(format!("{name} is not a hub member")))?;
            member.tags.retain(|tag| !request.remove_tags.contains(tag));
            for tag in request.add_tags {
                if !member.tags.contains(&tag) {
                    member.tags.push(tag);
                }
            }
            member.tags.sort();
            if let Some(slots) = request.slots {
                member.slots = slots.max(1);
            }
            if let Some(priority) = request.priority {
                member.priority = priority;
            }
            if let Some(member_state) = request.state {
                member.state = member_state;
            }
            Ok(member_document(member))
        })
        .await
    }

    pub(crate) async fn remove(&self, name: &str) -> Result<Value, HubError> {
        self.mutate(|state| {
            let before = state.members.len();
            state.members.retain(|member| member.name != name);
            if state.members.len() == before {
                return Err(HubError::MemberNotFound(format!(
                    "{name} is not a hub member"
                )));
            }
            Ok(json!({ "schema": "zeroshot.hub.member/v1", "removed": name }))
        })
        .await
    }

    fn member_kind(&self, name: &str) -> (Value, Value) {
        if name == LOCAL_MEMBER {
            return (json!("local"), Value::Null);
        }
        match self.registry.get(name) {
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

    pub(crate) async fn list(&self, query: MemberQuery) -> Result<Value, HubError> {
        let mut state = self.snapshot().await;
        let loads = if query.probe {
            let mut members = state.members.clone();
            schedule::discover(&mut members, self.driver(), now_ms(), 0).await;
            state = self
                .mutate(|state| {
                    schedule::merge_capabilities(&mut state.members, &members);
                    Ok(state.clone())
                })
                .await?;
            let probed = state
                .members
                .iter()
                .filter(|member| query.tags.iter().all(|tag| member.tags.contains(tag)))
                .filter(|member| member.state != MemberState::Disabled)
                .collect::<Vec<_>>();
            let loads = schedule::probe(&probed, &state.jobs, self.driver()).await;
            self.metrics.record_loads(&loads);
            Some(loads)
        } else {
            None
        };
        let now = now_ms();
        let targets = state
            .members
            .iter()
            .filter(|member| query.tags.iter().all(|tag| member.tags.contains(tag)))
            .map(|member| {
                let mut view = member.view();
                let (kind, origin) = self.member_kind(&member.name);
                view["kind"] = kind;
                view["origin"] = origin;
                view["hubRunning"] = json!(
                    state
                        .jobs
                        .iter()
                        .filter(|job| job.state == JobState::Running
                            && job.target.as_deref() == Some(member.name.as_str()))
                        .count()
                );
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
        Ok(json!({ "schema": "zeroshot.hub.targets/v1", "targets": targets }))
    }

    // ---------------------------------------------------------------- jobs

    /// Write uploaded files under `spool/ID/` and point the run arguments at them. A working
    /// folder that does not exist on this host is replaced by the spool folder.
    fn spool(&self, job: &mut Job, files: Vec<JobFile>) -> Result<(), HubError> {
        let (run_args, cwd) = (&mut job.run_args, &mut job.cwd);
        let folder = self.store.directory().join("spool").join(&job.job_id);
        if files.is_empty() && cwd.is_dir() {
            return Ok(());
        }
        create_private_directory(&folder)?;
        for (index, file) in files.into_iter().enumerate() {
            let positions = (1..run_args.len())
                .filter(|&position| {
                    FILE_FLAGS.contains(&run_args[position - 1].as_str())
                        && run_args[position] == file.argument
                })
                .collect::<Vec<_>>();
            if positions.is_empty() {
                return Err(HubError::Usage(format!(
                    "uploaded file {:?} is not a file argument of the run",
                    file.argument
                )));
            }
            let name = Path::new(&file.argument).file_name().map_or_else(
                || "file".to_owned(),
                |name| name.to_string_lossy().into_owned(),
            );
            let path = folder.join(format!("{index}-{name}"));
            write_private(&path, file.content.as_bytes())?;
            let text = path.to_string_lossy().into_owned();
            for position in positions {
                run_args[position].clone_from(&text);
            }
        }
        if !cwd.is_dir() {
            cwd.clone_from(&folder);
        }
        Ok(())
    }

    pub(crate) async fn submit(
        &self,
        mut job: Job,
        queue: bool,
        files: Vec<JobFile>,
    ) -> Result<Value, HubError> {
        let _placing = self.placement.lock().await;
        if let Some(existing) = idempotent(&self.snapshot().await, &job) {
            return Ok(job_document(&existing, now_ms()));
        }
        self.spool(&mut job, files)?;
        let mut members = self.snapshot().await.members;
        schedule::discover(
            &mut members,
            self.driver(),
            now_ms(),
            schedule::CAPABILITY_TTL_MS,
        )
        .await;
        let state = self
            .mutate(|state| {
                schedule::merge_capabilities(&mut state.members, &members);
                Ok(state.clone())
            })
            .await?;
        if let Some(rejected) = rejections(&state.members, &job.placement, &job.requirements) {
            self.metrics.record_rejection(Some(&rejected));
            return Err(HubError::no_eligible(describe(&job.placement), rejected));
        }
        let matching = matching_members(&state.members, &job.placement);
        let loads = schedule::probe(&matching, &state.jobs, self.driver()).await;
        self.metrics.record_loads(&loads);
        let now = now_ms();
        match place(&state.members, &job.placement, &job.requirements, &loads) {
            Placement::Place(member) => {
                job.state = JobState::Running;
                job.target = Some(member.clone());
                self.mutate(|state| {
                    state.jobs.push(job.clone());
                    Ok(())
                })
                .await?;
                let placed = self.dispatch(&job.job_id, &member, now).await?;
                let (job, result) = placed;
                result?;
                Ok(job_document(&job, now))
            }
            Placement::NoEligible { rejected } => {
                self.metrics.record_rejection(Some(&rejected));
                Err(HubError::no_eligible(describe(&job.placement), rejected))
            }
            Placement::Busy { .. } if queue => {
                self.mutate(|state| {
                    state.jobs.push(job.clone());
                    Ok(())
                })
                .await?;
                Ok(job_document(&job, now))
            }
            Placement::Busy { .. } => {
                self.metrics.record_rejection(None);
                Err(HubError::NoCapacity(describe(&job.placement)))
            }
        }
    }

    /// Submit a job already marked as placing on `member`, outside the state lock, and record
    /// the outcome.
    async fn dispatch(
        &self,
        job_id: &str,
        member: &str,
        now: u64,
    ) -> Result<(Job, Result<(), HubError>), HubError> {
        let Some(mut job) = self
            .snapshot()
            .await
            .jobs
            .into_iter()
            .find(|job| job.job_id == job_id)
        else {
            return Err(HubError::JobNotFound(job_id.to_owned()));
        };
        let result = schedule::dispatch(&mut job, member, self.driver(), now).await;
        self.metrics.record_placement(member, result.is_ok());
        self.mutate(|state| {
            if let Some(slot) = state.jobs.iter_mut().find(|slot| slot.job_id == job.job_id) {
                *slot = job.clone();
            }
            Ok(())
        })
        .await?;
        Ok((job, result))
    }

    pub(crate) async fn enqueue(
        &self,
        mut job: Job,
        files: Vec<JobFile>,
    ) -> Result<Value, HubError> {
        let now = now_ms();
        if let Some(existing) = idempotent(&self.snapshot().await, &job) {
            return Ok(job_document(&existing, now));
        }
        self.spool(&mut job, files)?;
        self.mutate(|state| {
            if let Some(existing) = idempotent(state, &job) {
                return Ok(job_document(&existing, now));
            }
            if let Some(rejected) = rejections(&state.members, &job.placement, &job.requirements) {
                self.metrics.record_rejection(Some(&rejected));
                return Err(HubError::no_eligible(describe(&job.placement), rejected));
            }
            let document = job_document(&job, now);
            state.jobs.push(job);
            Ok(document)
        })
        .await
    }

    pub(crate) async fn create_schedule(
        &self,
        mut schedule: Schedule,
        files: Vec<JobFile>,
    ) -> Result<Value, HubError> {
        let parsed = cron::parse(&schedule.cron)?;
        let (_, zone) = cron::timezone(Some(&schedule.timezone))?;
        let now = now_ms();
        schedule.next_fire_ms = Some(cron::next_fire(&parsed, &zone, now)?);
        let existing = |state: &HubState| {
            schedule.submission_key.as_ref().and_then(|key| {
                state
                    .schedules
                    .iter()
                    .find(|existing| existing.submission_key.as_ref() == Some(key))
                    .cloned()
            })
        };
        if let Some(existing) = existing(&self.snapshot().await) {
            return Ok(schedule_document(&existing));
        }
        self.spool(&mut schedule.template, files)?;
        self.mutate(|state| {
            if let Some(existing) = existing(state) {
                return Ok(schedule_document(&existing));
            }
            let template = &schedule.template;
            if let Some(rejected) =
                rejections(&state.members, &template.placement, &template.requirements)
            {
                self.metrics.record_rejection(Some(&rejected));
                return Err(HubError::no_eligible(
                    describe(&template.placement),
                    rejected,
                ));
            }
            let document = schedule_document(&schedule);
            state.schedules.push(schedule.clone());
            Ok(document)
        })
        .await
    }

    pub(crate) async fn ps(&self, query: JobQuery) -> Result<Value, HubError> {
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
        if !STATES.contains(&query.state.as_str()) {
            return Err(HubError::Usage(format!(
                "invalid --state {:?}: use one of {}",
                query.state,
                STATES.join(", ")
            )));
        }
        let state = self.snapshot().await;
        let now = now_ms();
        let mut selected = state
            .jobs
            .iter()
            .filter(|job| state_matches(job, &query.state, now))
            .filter(|job| query.target.is_none() || job.target == query.target)
            .filter(|job| {
                query
                    .labels
                    .iter()
                    .all(|(key, value)| job.labels.get(key) == Some(value))
            })
            .collect::<Vec<_>>();
        if let Some(limit) = query.limit {
            let skip = selected.len().saturating_sub(limit);
            selected.drain(..skip);
        }
        if query.state == "recurring" {
            selected.clear();
        }
        let views = selected.iter().map(|job| job.view(now)).collect::<Vec<_>>();
        let schedules = state
            .schedules
            .iter()
            .filter(|schedule| match query.state.as_str() {
                "all" => true,
                "active" | "recurring" => schedule.state == ScheduleState::Active,
                _ => false,
            })
            .filter(|schedule| {
                query.target.is_none() || schedule.template.placement.target == query.target
            })
            .filter(|schedule| {
                query
                    .labels
                    .iter()
                    .all(|(key, value)| schedule.template.labels.get(key) == Some(value))
            })
            .map(Schedule::view)
            .collect::<Vec<_>>();
        Ok(json!({ "schema": "zeroshot.hub.jobs/v1", "jobs": views, "schedules": schedules }))
    }

    /// One job with its live run status, read from the target outside the state lock.
    pub(crate) async fn status(&self, job_id: &str) -> Result<Value, HubError> {
        let job = self.job(job_id).await?;
        let run = match (&job.target, &job.run_id) {
            (Some(target), Some(run_id)) => {
                let result = self.driver().status(target, run_id).await;
                let now = now_ms();
                let run_id = run_id.clone();
                let job = self
                    .mutate(|state| {
                        let job = state
                            .jobs
                            .iter_mut()
                            .find(|job| job.job_id == job_id)
                            .ok_or_else(|| HubError::JobNotFound(job_id.to_owned()))?;
                        let status = if job.state == JobState::Running
                            && job.run_id.as_deref() == Some(run_id.as_str())
                        {
                            schedule::apply_refresh(job, result, now)
                        } else {
                            result.ok()
                        };
                        Ok((job.clone(), status))
                    })
                    .await?;
                let mut document = job_document(&job.0, now);
                document["run"] = job.1.unwrap_or(Value::Null);
                return Ok(document);
            }
            _ => Value::Null,
        };
        let mut document = job_document(&job, now_ms());
        document["run"] = run;
        Ok(document)
    }

    async fn job(&self, job_id: &str) -> Result<Job, HubError> {
        self.state
            .lock()
            .await
            .jobs
            .iter()
            .find(|job| job.job_id == job_id)
            .cloned()
            .ok_or_else(|| HubError::JobNotFound(job_id.to_owned()))
    }

    pub(crate) async fn dequeue(&self, job_id: &str) -> Result<Value, HubError> {
        self.mutate(|state| {
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
                return Ok(schedule_document(schedule));
            }
            let now = now_ms();
            let job = state
                .jobs
                .iter_mut()
                .find(|job| job.job_id == job_id)
                .ok_or_else(|| HubError::JobNotFound(job_id.to_owned()))?;
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
            Ok(job_document(job, now))
        })
        .await
    }

    pub(crate) async fn stats(&self, request: StatsRequest) -> Result<Value, HubError> {
        let by = request
            .by
            .iter()
            .map(|dimension| stats::Dimension::parse(dimension))
            .collect::<Result<Vec<_>, _>>()?;
        let prices = request
            .prices
            .map(|prices| {
                serde_json::from_value::<stats::Prices>(prices).map_err(|error| {
                    HubError::Usage(format!("--prices is not a price table: {error}"))
                })
            })
            .transpose()?;
        let state = self.snapshot().await;
        let query = stats::StatsQuery {
            by: &by,
            since_ms: request.since_ms,
            until_ms: request.until_ms,
            prices: prices.as_ref(),
        };
        Ok(stats::aggregate(&state.jobs, &query))
    }

    /// The `zeroshot` arguments that observe or stop a job's run.
    pub(crate) async fn observation(
        &self,
        job_id: &str,
        request: StreamRequest,
    ) -> Result<Vec<String>, HubError> {
        let job = self.job(job_id).await?;
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
        let mut arguments = match request {
            StreamRequest::Watch { after } => {
                let mut arguments = vec!["watch".to_owned(), run_id.clone()];
                arguments.extend(
                    after
                        .into_iter()
                        .flat_map(|after| ["--after".to_owned(), after]),
                );
                arguments
            }
            StreamRequest::Logs { after, execution } => {
                let mut arguments = vec!["logs".to_owned(), run_id.clone()];
                arguments.extend(
                    after
                        .into_iter()
                        .flat_map(|after| ["--after".to_owned(), after]),
                );
                arguments.extend(
                    execution
                        .into_iter()
                        .flat_map(|execution| ["--execution".to_owned(), execution]),
                );
                arguments
            }
            StreamRequest::Attach { execution } => {
                vec!["attach".to_owned(), run_id.clone(), execution]
            }
            StreamRequest::ForceStop => vec!["force-stop".to_owned(), run_id.clone()],
        };
        arguments.extend(route(target));
        Ok(arguments)
    }

    // ---------------------------------------------------------------- dispatcher

    /// One dispatcher pass. Each phase holds the state lock only to read or merge; polls,
    /// discovery, probes, and submissions run between phases.
    pub(crate) async fn pass(&self) -> Result<(TickReport, Value), HubError> {
        let started = std::time::Instant::now();
        let result = self.pass_phases().await;
        self.metrics
            .record_pass(started.elapsed().as_secs_f64(), result.is_err());
        result
    }

    async fn pass_phases(&self) -> Result<(TickReport, Value), HubError> {
        let _placing = self.placement.lock().await;
        let now = now_ms();
        let (fired, polls) = self
            .mutate(|state| Ok((cron::fire_due(state, now), schedule::polls(state))))
            .await?;
        let results = schedule::poll(polls, self.driver()).await;
        let now = now_ms();
        let (finished, expired, due, members) = self
            .mutate(|state| {
                let finished = schedule::apply_polls(state, results, now);
                let expired = schedule::expire(&mut state.jobs, now);
                let due = !schedule::due_jobs(&state.jobs, now).is_empty();
                Ok((finished, expired, due, state.members.clone()))
            })
            .await?;
        let mut report = TickReport {
            fired,
            finished,
            expired,
            ..TickReport::default()
        };
        if due {
            let mut members = members;
            schedule::discover(
                &mut members,
                self.driver(),
                now,
                schedule::CAPABILITY_TTL_MS,
            )
            .await;
            let state = self
                .mutate(|state| {
                    schedule::merge_capabilities(&mut state.members, &members);
                    Ok(state.clone())
                })
                .await?;
            let active = state
                .members
                .iter()
                .filter(|member| member.state == MemberState::Active)
                .collect::<Vec<_>>();
            let mut loads: BTreeMap<_, _> =
                schedule::probe(&active, &state.jobs, self.driver()).await;
            self.metrics.record_loads(&loads);
            let now = now_ms();
            let chosen = self
                .mutate(|state| Ok(schedule::choose(state, now, &mut loads)))
                .await?;
            for (job_id, member) in chosen {
                let (job, result) = self.dispatch(&job_id, &member, now).await?;
                schedule::record_dispatch(&mut report, &job, result);
            }
            report.loads = Some(loads);
        }
        let view = report.view(&self.snapshot().await, now_ms());
        Ok((report, view))
    }
}

fn state_matches(job: &Job, filter: &str, now: u64) -> bool {
    let state = job.display_state(now);
    match filter {
        "all" => true,
        "active" => matches!(state, "queued" | "scheduled" | "running"),
        filter => state == filter,
    }
}

/// An earlier job accepted with the same hub submission key, if any.
fn idempotent(state: &HubState, job: &Job) -> Option<Job> {
    let key = job.submission_key.as_ref()?;
    state
        .jobs
        .iter()
        .find(|existing| existing.submission_key.as_ref() == Some(key))
        .cloned()
}

pub(crate) fn create_private_directory(path: &Path) -> Result<(), HubError> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path).map_err(|error| {
        HubError::storage(&format!("{} could not be created", path.display()), error)
    })
}

pub(crate) fn write_private(path: &Path, bytes: &[u8]) -> Result<(), HubError> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .and_then(|mut file| file.write_all(bytes))
        .map_err(|error| {
            HubError::storage(&format!("{} could not be written", path.display()), error)
        })
}
