//! The hub service's eval operations: capture, run creation, the measuring and judging
//! worker, and reports.

use zeroshot_engine::native_v2_cli::hub::valid_eval_name;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use openengine_cluster_protocol::NODE_RUNTIME_LANES_KIND;
use serde_json::{Value, json};

use super::super::clock::{now_ms, rfc3339};
use super::super::error::HubError;
use super::super::model::{HubState, Job, JobState};
use super::super::placement::{describe, rejections};
use super::super::service::{FILE_FLAGS, HubService, create_private_directory, replace_private};
use super::super::stats::Prices;

use super::report::{self, Against, Thresholds};
use super::variant::{self, Change, Material, Request};
use super::{
    AddCase, AddSuite, BASELINE, Baseline, Blobs, CaptureSource, Case, CaseQuery, CreateRun,
    EvalRun, Judge, JudgeMode, Override, ReportRequest, RunQuery, Sample, SampleState, Suite,
    Variant, canonical, sample_job_id, short_hash,
};

/// Labels a capture never copies: they belong to the hub job, not the task.
const RESERVED_LABEL_PREFIX: &str = "eval.";
const MAX_REPEAT: u32 = 100;

pub(super) fn not_found(what: &str) -> HubError {
    HubError::EvalNotFound(what.to_owned())
}

fn case_document(case: &Case, created: Option<bool>) -> Value {
    let mut document = json!({ "schema": "zeroshot.hub.eval.case/v1", "case": case_view(case) });
    if let Some(created) = created {
        document["created"] = json!(created);
    }
    document
}

pub(super) fn case_view(case: &Case) -> Value {
    json!({
        "caseId": case.case_id,
        "name": case.name,
        "title": case.title,
        "runArgs": case.run_args,
        "files": case.files,
        "cwd": case.cwd,
        "pinnedRevision": case.pinned_revision,
        "placement": case.placement,
        "requirements": case.requirements,
        "labels": case.labels,
        "project": case.project,
        "models": case.models,
        "sourceJob": case.source_job,
        "createdAt": rfc3339(case.created_at_ms),
    })
}

fn sample_view(sample: &Sample, state: &HubState, now: u64) -> Value {
    let job = state.jobs.iter().find(|job| job.job_id == sample.job_id);
    let mut view = serde_json::to_value(sample).unwrap_or(Value::Null);
    view["jobState"] = json!(job.map(|job| job.display_state(now)));
    view
}

fn run_summary(run: &EvalRun) -> Value {
    let count = |state: SampleState| {
        run.samples
            .iter()
            .filter(|sample| sample.state == state)
            .count()
    };
    json!({
        "runId": run.run_id,
        "key": run.key,
        "suite": run.suite,
        "cases": run.cases,
        "baseline": run.baseline,
        "variants": run.variants,
        "repeat": run.repeat,
        "placement": run.placement,
        "priority": run.priority,
        "judge": run.judge,
        "checks": run.checks,
        "createdAt": rfc3339(run.created_at_ms),
        "complete": run.complete(),
        "samples": {
            "total": run.samples.len(),
            "pending": count(SampleState::Pending),
            "measured": count(SampleState::Measured),
            "judged": count(SampleState::Judged),
            "failed": count(SampleState::Failed),
        },
    })
}

/// Turn the file arguments of `run_args` into blobs: `read` yields each file's name and bytes.
fn capture_files(
    run_args: &mut [String],
    blobs: &Blobs,
    mut read: impl FnMut(&str, &str) -> Result<(String, Vec<u8>), HubError>,
) -> Result<BTreeMap<String, super::BlobRef>, HubError> {
    let mut files = BTreeMap::new();
    for position in 1..run_args.len() {
        let flag = run_args[position - 1].clone();
        if !FILE_FLAGS.contains(&flag.as_str()) {
            continue;
        }
        let (name, bytes) = read(&flag, &run_args[position])?;
        files.insert(flag.clone(), blobs.put(&name, &bytes)?);
        run_args[position] = format!("file:{flag}");
    }
    Ok(files)
}

fn parse_prices(prices: Option<&Value>) -> Result<Option<Prices>, HubError> {
    prices
        .map(|prices| {
            serde_json::from_value::<Prices>(prices.clone())
                .map_err(|error| HubError::Usage(format!("--prices is not a price table: {error}")))
        })
        .transpose()
}

impl HubService {
    pub(super) fn blobs(&self) -> Blobs {
        Blobs::new(self.store().directory())
    }

    pub(super) fn eval_directory(&self) -> PathBuf {
        self.store().directory().join("eval")
    }

    pub(super) fn work_directory(&self, run_id: &str, sample: &Sample) -> PathBuf {
        self.eval_directory()
            .join("work")
            .join(run_id)
            .join(&sample.case_id)
            .join(format!("{}-{}", sample.variant, sample.repeat))
    }

    // ------------------------------------------------------------ suites and cases

    pub(crate) async fn eval_suite_add(&self, request: AddSuite) -> Result<Value, HubError> {
        if !valid_eval_name(&request.name) {
            return Err(HubError::Usage(format!(
                "invalid suite name {:?}: use letters, digits, '.', '_', or '-'",
                request.name
            )));
        }
        let now = now_ms();
        self.mutate(|state| {
            let suites = &mut state.evals.suites;
            let suite =
                if let Some(suite) = suites.iter_mut().find(|suite| suite.name == request.name) {
                    if request.description.is_some() {
                        suite.description.clone_from(&request.description);
                    }
                    suite.clone()
                } else {
                    let suite = Suite {
                        name: request.name.clone(),
                        description: request.description.clone(),
                        cases: Vec::new(),
                        created_at_ms: now,
                    };
                    suites.push(suite.clone());
                    suite
                };
            Ok(json!({
                "schema": "zeroshot.hub.eval.suite/v1",
                "suite": {
                    "name": suite.name,
                    "description": suite.description,
                    "cases": suite.cases,
                    "createdAt": rfc3339(suite.created_at_ms),
                },
            }))
        })
        .await
    }

    pub(crate) async fn eval_suites(&self) -> Result<Value, HubError> {
        let state = self.snapshot().await;
        let suites = state
            .evals
            .suites
            .iter()
            .map(|suite| {
                json!({
                    "name": suite.name,
                    "description": suite.description,
                    "cases": suite.cases,
                    "runs": state.evals.runs.iter().filter(|run| run.suite.as_ref() == Some(&suite.name)).count(),
                    "createdAt": rfc3339(suite.created_at_ms),
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({ "schema": "zeroshot.hub.eval.suites/v1", "suites": suites }))
    }

    /// Snapshot a task as a case. Re-adding the same request returns the same case ID.
    pub(crate) async fn eval_case_add(&self, request: AddCase) -> Result<Value, HubError> {
        let blobs = self.blobs();
        let (mut job, source_job, uploaded) = match request.source {
            CaptureSource::Job { job_id } => {
                let job = self
                    .snapshot()
                    .await
                    .jobs
                    .into_iter()
                    .find(|job| job.job_id == job_id)
                    .ok_or_else(|| HubError::JobNotFound(job_id.clone()))?;
                (job, Some(job_id), None)
            }
            CaptureSource::Flags { job, files } => (*job, None, Some(files)),
        };
        let cwd = job.cwd.clone();
        let files = capture_files(&mut job.run_args, &blobs, |flag, argument| {
            let name = Path::new(argument).file_name().map_or_else(
                || "file".to_owned(),
                |name| name.to_string_lossy().into_owned(),
            );
            if let Some(uploaded) = &uploaded {
                let file = uploaded
                    .iter()
                    .find(|file| file.argument == argument)
                    .ok_or_else(|| {
                        HubError::Usage(format!("{flag} {argument} was not uploaded"))
                    })?;
                return Ok((name, file.content.clone().into_bytes()));
            }
            let path = cwd.join(argument);
            let bytes = std::fs::read(&path).map_err(|error| {
                HubError::Usage(format!(
                    "{flag} {} of the job could not be read on the hub host: {error}",
                    path.display()
                ))
            })?;
            Ok((name, bytes))
        })?;
        let mut labels = job.labels.clone();
        labels.retain(|key, _| !key.starts_with(RESERVED_LABEL_PREFIX));
        for (key, value) in request.labels {
            labels.insert(key, value);
        }
        let mut case = Case {
            case_id: String::new(),
            name: request.name.clone(),
            title: job.title.clone(),
            run_args: job.run_args.clone(),
            files,
            cwd: job.cwd.clone(),
            pinned_revision: job.pinned_revision.clone(),
            placement: job.placement.clone(),
            requirements: job.requirements.clone(),
            labels,
            project: job.project.clone(),
            models: job.models.clone(),
            source_job,
            created_at_ms: now_ms(),
        };
        case.assign_id();
        self.mutate(|state| {
            for suite in &request.suites {
                if !state.evals.suites.iter().any(|existing| &existing.name == suite) {
                    return Err(not_found(&format!(
                        "eval suite {suite} does not exist; create it with `zeroshot hub eval suite add {suite}`"
                    )));
                }
            }
            let created = state.evals.case(&case.case_id).is_none();
            if created {
                state.evals.cases.push(case.clone());
            } else if let Some(existing) = state
                .evals
                .cases
                .iter_mut()
                .find(|existing| existing.case_id == case.case_id)
            {
                if existing.name.is_none() {
                    existing.name.clone_from(&case.name);
                }
            }
            for suite in state
                .evals
                .suites
                .iter_mut()
                .filter(|suite| request.suites.contains(&suite.name))
            {
                if !suite.cases.contains(&case.case_id) {
                    suite.cases.push(case.case_id.clone());
                }
            }
            let stored = state.evals.case(&case.case_id).cloned().unwrap_or(case.clone());
            Ok(case_document(&stored, Some(created)))
        })
        .await
    }

    pub(crate) async fn eval_cases(&self, query: CaseQuery) -> Result<Value, HubError> {
        let state = self.snapshot().await;
        let members = match &query.suite {
            Some(name) => Some(
                state
                    .evals
                    .suites
                    .iter()
                    .find(|suite| &suite.name == name)
                    .ok_or_else(|| not_found(&format!("eval suite {name} does not exist")))?
                    .cases
                    .clone(),
            ),
            None => None,
        };
        let cases = state
            .evals
            .cases
            .iter()
            .filter(|case| members.as_ref().is_none_or(|members| members.contains(&case.case_id)))
            .map(|case| {
                json!({
                    "caseId": case.case_id,
                    "name": case.name,
                    "title": case.title,
                    "labels": case.labels,
                    "models": case.models,
                    "pinnedRevision": case.pinned_revision,
                    "sourceJob": case.source_job,
                    "suites": state.evals.suites.iter().filter(|suite| suite.cases.contains(&case.case_id)).map(|suite| suite.name.clone()).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({ "schema": "zeroshot.hub.eval.cases/v1", "cases": cases }))
    }

    pub(crate) async fn eval_case(&self, case_id: &str) -> Result<Value, HubError> {
        let state = self.snapshot().await;
        let case = state
            .evals
            .cases
            .iter()
            .find(|case| case.case_id == case_id || case.name.as_deref() == Some(case_id))
            .ok_or_else(|| not_found(&format!("eval case {case_id} does not exist")))?;
        Ok(case_document(case, None))
    }

    // ------------------------------------------------------------ runs

    /// Create an eval run and enqueue one hub job per case × variant × repeat.
    pub(crate) async fn eval_run_create(&self, request: CreateRun) -> Result<Value, HubError> {
        let run_id = format!(
            "er_{}",
            short_hash(&format!(
                "{}\n{}",
                canonical(&serde_json::to_value(&request).unwrap_or(Value::Null)),
                request
                    .key
                    .clone()
                    .unwrap_or_else(|| uuid::Uuid::now_v7().to_string())
            ))
        );
        let state = self.snapshot().await;
        if let Some(existing) = state.evals.run(&run_id) {
            return Ok(self.run_document(existing, &state));
        }
        let run = self.plan_run(&state, request, run_id)?;
        let cases = run
            .cases
            .iter()
            .filter_map(|case_id| state.evals.case(case_id).cloned())
            .collect::<Vec<_>>();
        let mut jobs = Vec::new();
        for sample in &run.samples {
            if state.jobs.iter().any(|job| job.job_id == sample.job_id) {
                continue;
            }
            let case = cases
                .iter()
                .find(|case| case.case_id == sample.case_id)
                .ok_or_else(|| {
                    not_found(&format!("eval case {} does not exist", sample.case_id))
                })?;
            jobs.push(self.sample_job(&run, case, sample)?);
        }
        self.mutate(|state| {
            if let Some(existing) = state.evals.run(&run.run_id) {
                return Ok(existing.clone());
            }
            for job in &jobs {
                if let Some(rejected) =
                    rejections(&state.members, &job.placement, &job.requirements)
                {
                    self.metrics().record_rejection(Some(&rejected));
                    return Err(HubError::no_eligible(describe(&job.placement), rejected));
                }
            }
            state.jobs.extend(jobs.iter().cloned());
            state.evals.runs.push(run.clone());
            Ok(run.clone())
        })
        .await
        .map(|run| self.run_document_now(&run, &jobs))
    }

    fn run_document_now(&self, run: &EvalRun, jobs: &[Job]) -> Value {
        let state = HubState {
            jobs: jobs.to_vec(),
            ..HubState::default()
        };
        self.run_document(run, &state)
    }

    fn run_document(&self, run: &EvalRun, state: &HubState) -> Value {
        let now = now_ms();
        json!({
            "schema": "zeroshot.hub.eval.run/v1",
            "run": run_summary(run),
            "samples": run.samples.iter().map(|sample| sample_view(sample, state, now)).collect::<Vec<_>>(),
        })
    }

    /// Validate a run request and decide its samples; uploads go to the blob store.
    fn plan_run(
        &self,
        state: &HubState,
        request: CreateRun,
        run_id: String,
    ) -> Result<EvalRun, HubError> {
        let blobs = self.blobs();
        if request.repeat == 0 || request.repeat > MAX_REPEAT {
            return Err(HubError::Usage(format!(
                "--repeat must be 1..={MAX_REPEAT}"
            )));
        }
        let cases = select_cases(state, &request)?;
        let mut variants = plan_variants(&blobs, &request.variants)?;
        let baseline = plan_baseline(state, request.baseline.as_deref(), &cases, &variants)?;
        add_implicit_baseline(&baseline, &mut variants);
        let judge = plan_judge(&blobs, request.judge.as_ref(), &baseline)?;
        for check in &request.checks {
            if !valid_eval_name(&check.name) || check.command.trim().is_empty() {
                return Err(HubError::Usage(format!("invalid --check {:?}", check.name)));
            }
        }
        parse_prices(request.prices.as_ref())?;
        let placement = (request.target.is_some() || !request.tags.is_empty()).then(|| {
            super::super::model::PlacementSpec {
                target: request.target.clone(),
                tags: request.tags.clone(),
                strategy: super::super::model::Strategy::default(),
            }
        });
        let mut run = EvalRun {
            run_id,
            key: request.key,
            suite: request.suite,
            cases,
            baseline,
            variants,
            repeat: request.repeat,
            placement,
            priority: request.priority,
            judge,
            checks: request.checks,
            prices: request.prices,
            created_at_ms: now_ms(),
            samples: Vec::new(),
        };
        run.samples = plan_samples(&run);
        Ok(run)
    }

    /// The queued hub job of one sample: the case's request with the variant applied, its
    /// files written under `spool/JOB/`.
    fn sample_job(&self, run: &EvalRun, case: &Case, sample: &Sample) -> Result<Job, HubError> {
        let blobs = self.blobs();
        let variant = run
            .variants
            .iter()
            .find(|variant| variant.name == sample.variant)
            .ok_or_else(|| {
                not_found(&format!(
                    "variant {} is not part of the run",
                    sample.variant
                ))
            })?;
        let mut files = BTreeMap::new();
        for (flag, blob) in &case.files {
            files.insert(
                flag.clone(),
                Material {
                    name: blob.name.clone(),
                    content: blobs.read(blob)?,
                },
            );
        }
        let changes = variant
            .overrides
            .iter()
            .map(|change| {
                Ok(Change {
                    key: change.key.clone(),
                    value: change.value.clone(),
                    file: change
                        .file
                        .as_ref()
                        .map(|blob| {
                            blobs.read(blob).map(|content| Material {
                                name: blob.name.clone(),
                                content,
                            })
                        })
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>, HubError>>()?;
        let request = Request {
            run_args: case.run_args.clone(),
            files,
        }
        .apply(&changes)?;
        let folder = self.store().directory().join("spool").join(&sample.job_id);
        create_private_directory(&folder)?;
        let mut run_args = request.run_args.clone();
        for (index, (flag, material)) in request.files.iter().enumerate() {
            let path = folder.join(format!("{index}-{}", material.name));
            replace_private(&path, &material.content)?;
            let placeholder = format!("file:{flag}");
            for argument in &mut run_args {
                if *argument == placeholder {
                    *argument = path.to_string_lossy().into_owned();
                }
            }
        }
        let mut requirements = case.requirements.clone();
        requirements
            .capabilities
            .retain(|capability| capability != NODE_RUNTIME_LANES_KIND);
        if request.lanes() {
            requirements
                .capabilities
                .push(NODE_RUNTIME_LANES_KIND.to_owned());
        }
        if request.files.contains_key("--environment") && requirements.local_unsupported.is_none() {
            requirements.local_unsupported =
                Some("local runs reject --environment hooks".to_owned());
        }
        let models = if request.runtime_flag().is_some() {
            request.models()
        } else {
            case.models.clone()
        };
        let mut labels = case.labels.clone();
        labels.insert("eval.run".to_owned(), run.run_id.clone());
        labels.insert("eval.case".to_owned(), case.case_id.clone());
        labels.insert("eval.variant".to_owned(), sample.variant.clone());
        labels.insert("eval.repeat".to_owned(), sample.repeat.to_string());
        if let Some(suite) = &run.suite {
            labels.insert("eval.suite".to_owned(), suite.clone());
        }
        Ok(Job {
            job_id: sample.job_id.clone(),
            state: JobState::Queued,
            title: format!("{} [{} #{}]", case.title, sample.variant, sample.repeat),
            run_args,
            cwd: if case.cwd.is_dir() {
                case.cwd.clone()
            } else {
                folder
            },
            pinned_revision: case.pinned_revision.clone(),
            placement: run
                .placement
                .clone()
                .unwrap_or_else(|| case.placement.clone()),
            requirements,
            priority: run.priority,
            labels,
            project: case.project.clone(),
            models,
            submission_key: None,
            submitted_at_ms: now_ms(),
            not_before_ms: None,
            attempts: 0,
            unconfirmed_submissions: 0,
            target: None,
            run_id: None,
            dispatched_at_ms: None,
            last_cursor: None,
            last_cursor_at_ms: None,
            target_last_ok_at_ms: None,
            reaper: None,
            finished_at_ms: None,
            failure_reason: None,
            failure_classification: None,
            token_usage: None,
            error: None,
            schedule_id: None,
            output: None,
            archive: None,
            archive_skipped: None,
            graph_hash: None,
            pool_wait: None,
        })
    }

    pub(crate) async fn eval_runs(&self, query: RunQuery) -> Result<Value, HubError> {
        let state = self.snapshot().await;
        let mut runs = state
            .evals
            .runs
            .iter()
            .filter(|run| query.suite.is_none() || run.suite == query.suite)
            .map(run_summary)
            .collect::<Vec<_>>();
        if let Some(limit) = query.limit {
            let skip = runs.len().saturating_sub(limit);
            runs.drain(..skip);
        }
        Ok(json!({ "schema": "zeroshot.hub.eval.runs/v1", "runs": runs }))
    }

    pub(crate) async fn eval_run(&self, run_id: &str) -> Result<Value, HubError> {
        let state = self.snapshot().await;
        let run = state
            .evals
            .run(run_id)
            .ok_or_else(|| not_found(&format!("eval run {run_id} does not exist")))?;
        Ok(self.run_document(run, &state))
    }

    pub(crate) async fn eval_report(
        &self,
        run_id: &str,
        request: ReportRequest,
    ) -> Result<Value, HubError> {
        let state = self.snapshot().await;
        let run = state
            .evals
            .run(run_id)
            .ok_or_else(|| not_found(&format!("eval run {run_id} does not exist")))?;
        let thresholds = Thresholds::parse(&request.thresholds)?;
        let prices = parse_prices(request.prices.as_ref().or(run.prices.as_ref()))?;
        let pairwise = run.judge_mode() == Some(JudgeMode::Pairwise);
        let against = match request.baseline.as_deref() {
            Some(reference) if reference.starts_with("er_") => {
                let (other_id, variant) = reference
                    .split_once(':')
                    .map_or((reference, None), |(id, variant)| (id, Some(variant)));
                let other = state
                    .evals
                    .run(other_id)
                    .ok_or_else(|| not_found(&format!("eval run {other_id} does not exist")))?;
                let variant = variant
                    .map(str::to_owned)
                    .or_else(|| other.baseline_variant().map(str::to_owned))
                    .unwrap_or_else(|| BASELINE.to_owned());
                let samples = other
                    .samples
                    .iter()
                    .filter(|sample| sample.variant == variant)
                    .collect::<Vec<_>>();
                if samples.is_empty() {
                    return Err(not_found(&format!(
                        "eval run {other_id} has no {variant} samples"
                    )));
                }
                Some(Against {
                    label: json!({ "runId": other_id, "variant": variant }),
                    samples,
                    pairwise_reference: false,
                })
            }
            Some(name) => {
                let samples = run
                    .samples
                    .iter()
                    .filter(|sample| sample.variant == name)
                    .collect::<Vec<_>>();
                if samples.is_empty() {
                    return Err(HubError::Usage(format!(
                        "--baseline {name} is not a variant of {run_id}"
                    )));
                }
                Some(Against {
                    label: json!({ "runId": run_id, "variant": name }),
                    samples,
                    pairwise_reference: pairwise && run.baseline_variant() == Some(name),
                })
            }
            None => run.baseline_variant().map(|name| Against {
                label: json!({ "runId": run_id, "variant": name }),
                samples: run
                    .samples
                    .iter()
                    .filter(|sample| sample.variant == name)
                    .collect(),
                pairwise_reference: pairwise,
            }),
        };
        Ok(report::report(
            run,
            against.as_ref(),
            &thresholds,
            prices.as_ref(),
        ))
    }
}

fn new_sample(case_id: &str, variant: &str, repeat: u32, job_id: String) -> Sample {
    Sample {
        case_id: case_id.to_owned(),
        variant: variant.to_owned(),
        repeat,
        job_id,
        state: SampleState::Pending,
        outcome: None,
        checks: Vec::new(),
        judge: None,
        judge_job: None,
        error: None,
    }
}

/// The run's case IDs: the suite's cases, then each `--case` by ID or name, without repeats.
fn select_cases(state: &HubState, request: &CreateRun) -> Result<Vec<String>, HubError> {
    let mut cases = Vec::new();
    if let Some(name) = &request.suite {
        let suite = state
            .evals
            .suites
            .iter()
            .find(|suite| &suite.name == name)
            .ok_or_else(|| not_found(&format!("eval suite {name} does not exist")))?;
        cases.extend(suite.cases.iter().cloned());
    }
    for case in &request.cases {
        let found = state
            .evals
            .cases
            .iter()
            .find(|existing| &existing.case_id == case || existing.name.as_deref() == Some(case))
            .ok_or_else(|| not_found(&format!("eval case {case} does not exist")))?;
        if !cases.contains(&found.case_id) {
            cases.push(found.case_id.clone());
        }
    }
    if cases.is_empty() {
        return Err(HubError::Usage(
            "the eval run selects no cases; add cases to the suite or pass --case".to_owned(),
        ));
    }
    Ok(cases)
}

/// Validate the requested variants and store their file overrides.
fn plan_variants(
    blobs: &Blobs,
    requested: &[super::VariantRequest],
) -> Result<Vec<Variant>, HubError> {
    let mut variants: Vec<Variant> = Vec::new();
    for requested in requested {
        if !valid_eval_name(&requested.name) {
            return Err(HubError::Usage(format!(
                "invalid variant name {:?}",
                requested.name
            )));
        }
        if variants
            .iter()
            .any(|variant| variant.name == requested.name)
        {
            return Err(HubError::Usage(format!(
                "variant {} is given twice",
                requested.name
            )));
        }
        let overrides = requested
            .overrides
            .iter()
            .map(|change| plan_override(blobs, change))
            .collect::<Result<Vec<_>, _>>()?;
        variants.push(Variant {
            name: requested.name.clone(),
            overrides,
        });
    }
    Ok(variants)
}

/// Validate one variant override; a file key's upload goes to the blob store.
fn plan_override(blobs: &Blobs, change: &super::OverrideRequest) -> Result<Override, HubError> {
    if !variant::KEYS.iter().any(|(key, _)| *key == change.key) {
        return Err(HubError::Usage(format!(
            "unknown variant key {:?}; use one of {}",
            change.key,
            variant::KEYS.map(|(key, _)| key).join(", ")
        )));
    }
    if !variant::is_file_key(&change.key) {
        return Ok(Override {
            key: change.key.clone(),
            value: change.value.clone(),
            file: None,
        });
    }
    let upload = change.upload.as_ref().ok_or_else(|| {
        HubError::Usage(format!(
            "variant key {} needs its file uploaded",
            change.key
        ))
    })?;
    Ok(Override {
        key: change.key.clone(),
        value: String::new(),
        file: Some(blobs.put(&upload.name, upload.content.as_bytes())?),
    })
}

/// Resolve `--baseline`: none, a hub job, a variant name, or the implicit baseline.
fn plan_baseline(
    state: &HubState,
    requested: Option<&str>,
    cases: &[String],
    variants: &[Variant],
) -> Result<Baseline, HubError> {
    match requested {
        Some("none") => Ok(Baseline::None),
        Some(job_id) if job_id.starts_with("hj_") => {
            if cases.len() != 1 {
                return Err(HubError::Usage(
                    "--baseline JOB compares one case; select exactly one case".to_owned(),
                ));
            }
            if !state.jobs.iter().any(|job| job.job_id == job_id) {
                return Err(HubError::JobNotFound(job_id.to_owned()));
            }
            if variants.iter().any(|variant| variant.name == BASELINE) {
                return Err(HubError::Usage(format!(
                    "--baseline JOB names its samples {BASELINE}; rename that variant"
                )));
            }
            Ok(Baseline::Job {
                job_id: job_id.to_owned(),
            })
        }
        Some(name) => {
            if name != BASELINE && !variants.iter().any(|variant| variant.name == name) {
                return Err(HubError::Usage(format!(
                    "--baseline {name} is not a variant of this run; use {BASELINE}, a --variant name, a hub job ID, or none"
                )));
            }
            Ok(Baseline::Variant {
                name: name.to_owned(),
            })
        }
        None => Ok(Baseline::Variant {
            name: BASELINE.to_owned(),
        }),
    }
}

/// Add the unchanged baseline variant when the run compares against it or has no variants.
fn add_implicit_baseline(baseline: &Baseline, variants: &mut Vec<Variant>) {
    let has_baseline = variants.iter().any(|variant| variant.name == BASELINE);
    let compares_baseline =
        matches!(baseline, Baseline::Variant { name } if name == BASELINE) && !has_baseline;
    if compares_baseline {
        variants.insert(
            0,
            Variant {
                name: BASELINE.to_owned(),
                overrides: Vec::new(),
            },
        );
    }
    if variants.is_empty() && !matches!(baseline, Baseline::Job { .. }) {
        variants.push(Variant {
            name: BASELINE.to_owned(),
            overrides: Vec::new(),
        });
    }
}

/// Validate the judge against the baseline and store its program and runtime.
fn plan_judge(
    blobs: &Blobs,
    judge: Option<&super::JudgeRequest>,
    baseline: &Baseline,
) -> Result<Option<Judge>, HubError> {
    let Some(judge) = judge else {
        return Ok(None);
    };
    if judge.mode == JudgeMode::Pairwise && *baseline == Baseline::None {
        return Err(HubError::Usage(
            "a pairwise judge needs a baseline".to_owned(),
        ));
    }
    Ok(Some(Judge {
        kind: judge.kind,
        mode: judge.mode,
        program: blobs.put(&judge.program.name, judge.program.content.as_bytes())?,
        runtime: judge
            .runtime
            .as_ref()
            .map(|runtime| blobs.put(&runtime.name, runtime.content.as_bytes()))
            .transpose()?,
    }))
}

/// One sample per case, variant, and repeat; a job baseline adds one sample per case.
fn plan_samples(run: &EvalRun) -> Vec<Sample> {
    let mut samples = Vec::new();
    for case_id in &run.cases {
        if let Baseline::Job { job_id } = &run.baseline {
            samples.push(new_sample(case_id, BASELINE, 1, job_id.clone()));
        }
        for variant in &run.variants {
            for repeat in 1..=run.repeat {
                let job_id = sample_job_id(&run.run_id, case_id, &variant.name, repeat);
                samples.push(new_sample(case_id, &variant.name, repeat, job_id));
            }
        }
    }
    samples
}
