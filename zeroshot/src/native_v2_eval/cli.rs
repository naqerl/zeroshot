use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use openengine_cluster_protocol::TokenUsage as ProtocolTokenUsage;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::native_v2_cli::NativeV2CliBackend;

use super::{
    BackendSampleRequest, Capacity, CapacitySource, EvalArm, EvalCriterion, EvalData, EvalError,
    EvalId, EvalManifest, EvalReport, EvalRunError, EvalRunRequest, EvalSample, EvalSamplePlan,
    EvalStore, FakeRunner, FakeSampleRequest, JUDGE_TEMPLATE_NAME, JudgeGraphExecutor,
    JudgeOptions, LocalJudgeGraphExecutor, LocalRunner, PatchArtifact, PlatformRunner,
    SampleArtifact, SampleOutcome, SampleRunner, SampleStatus, SampleTiming, ScorerInput,
    ScorerRegistry, TemplateJudge, TokenUsage, captured_sample_from_directory, judge_eval,
    load_judge_runtime, model::now_ms, report, resolve_judge_template, store::timestamp,
};

/// CLI command family wired as `zeroshot eval ...`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvalCommand {
    pub directory: PathBuf,
    pub action: EvalAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvalAction {
    Run(EvalRunCommand),
    List,
    Show { id: String },
    Report { id: String },
    Score { id: String, scorer: String },
    Judge(EvalJudgeCommand),
    JudgeCaptures(EvalJudgeCapturesCommand),
    Clean { id: Option<String>, all: bool },
}

/// Judge an existing eval from its captured samples.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvalJudgeCommand {
    pub id: String,
    pub judge_model: Option<String>,
    pub judge_template: Option<String>,
    pub scorer: Option<String>,
}

/// Judge slice-2 capture directories laid out as `<captures>/<arm-id>/<sample>/`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvalJudgeCapturesCommand {
    pub manifest: PathBuf,
    pub captures: PathBuf,
    pub id: Option<String>,
    pub judge_model: Option<String>,
    pub judge_template: Option<String>,
    pub scorer: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvalRunCommand {
    pub manifest: PathBuf,
    pub directory: PathBuf,
    pub id: Option<String>,
    pub scorer: Option<String>,
    pub judge_template: Option<String>,
    pub judge_model: Option<String>,
}

/// A manifest run through an already-constructed runner.
pub struct ManifestRunRequest<'a, R, F> {
    pub command: EvalRunCommand,
    pub runner: &'a R,
    pub capacity_source: Option<&'a dyn CapacitySource>,
    pub concurrency: Option<usize>,
    pub journal_directory: PathBuf,
    pub request: F,
}

/// A manifest run through the backend runner selected by `options.runner`.
pub struct BackendRunRequest<'a, B, F> {
    pub command: EvalRunCommand,
    pub backend: Arc<B>,
    pub target: Option<String>,
    pub capacity_source: Option<&'a dyn CapacitySource>,
    pub concurrency: Option<usize>,
    pub journal_directory: PathBuf,
    pub request: F,
}

/// The executable selected for an eval manifest.
///
/// The manifest's `options.runner` is the explicit switch: fixtures use [`FakeRunner`], while
/// `platform` and `local` select the corresponding native-v2 backend runner. The standalone CLI
/// command runs fixtures directly; backend-aware callers use [`run_manifest_with_backend`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerSelection {
    Fake,
    Platform,
    Local,
}

/// Select the runner for a manifest. Unknown values fail closed instead of silently using a fake.
pub fn select_runner(manifest: &EvalManifest) -> Result<RunnerSelection, EvalError> {
    match manifest.options.runner.as_str() {
        "fake" => Ok(RunnerSelection::Fake),
        "platform" => Ok(RunnerSelection::Platform),
        "local" => Ok(RunnerSelection::Local),
        other => Err(EvalError::Manifest(format!(
            "runner {other:?} is not available; expected \"fake\", \"platform\", or \"local\""
        ))),
    }
}

pub async fn execute(command: &EvalCommand) -> Result<Value, EvalError> {
    execute_impl(command, false).await
}

/// Explicit unit-test entrypoint for the synthetic fixture judge. Production CLI execution never
/// reaches the fixture executor because [`execute`] passes `false`.
#[cfg(test)]
pub(super) async fn execute_fixture(command: &EvalCommand) -> Result<Value, EvalError> {
    execute_impl(command, true).await
}

async fn execute_impl(
    command: &EvalCommand,
    allow_fixture_executor: bool,
) -> Result<Value, EvalError> {
    let store = EvalStore::for_workspace(&command.directory);
    match &command.action {
        EvalAction::Run(run) => {
            let data = run_manifest(run.clone()).await?;
            let report = data
                .report
                .ok_or_else(|| EvalError::ReportUnavailable(data.id.to_string()))?;
            Ok(serde_json::to_value(report)?)
        }
        EvalAction::List => Ok(serde_json::to_value(store.list()?)?),
        EvalAction::Show { id } => {
            let id = EvalId::new(id.clone())?;
            Ok(serde_json::to_value(store.load(&id)?)?)
        }
        EvalAction::Report { id } => {
            let id = EvalId::new(id.clone())?;
            let data = store.load(&id)?;
            let report = ensure_report(&store, &data)?;
            Ok(serde_json::to_value(report)?)
        }
        EvalAction::Score { id, scorer } => {
            let id = EvalId::new(id.clone())?;
            let data = store.load(&id)?;
            let capacity = data.report.as_ref().and_then(|report| report.capacity);
            let report = recompute(&data, scorer, capacity)?;
            store.write_report(&id, &report)?;
            store.write_report_markdown(&id, &report)?;
            Ok(serde_json::to_value(report)?)
        }
        EvalAction::Judge(command) => {
            let id = EvalId::new(command.id.clone())?;
            let mut data = store.load(&id)?;
            let options = JudgeOptions {
                model: command.judge_model.clone(),
                template: command.judge_template.clone(),
                scorer: command.scorer.clone(),
                executor: None,
            };
            judge_with_template(&store, &mut data, options, allow_fixture_executor).await?;
            let report = data
                .report
                .ok_or_else(|| EvalError::ReportUnavailable(data.id.to_string()))?;
            Ok(serde_json::to_value(report)?)
        }
        EvalAction::JudgeCaptures(command) => {
            let report =
                run_captures_through_judge(&store, command.clone(), allow_fixture_executor).await?;
            Ok(serde_json::to_value(report)?)
        }
        EvalAction::Clean { id, all } => clean(&store, id.as_deref(), *all),
    }
}

async fn run_captures_through_judge(
    store: &EvalStore,
    mut command: EvalJudgeCapturesCommand,
    allow_fixture_executor: bool,
) -> Result<EvalReport, EvalError> {
    if !command.captures.is_dir() {
        return Err(EvalError::Command(format!(
            "capture root {} is not a directory",
            command.captures.display()
        )));
    }
    let (mut manifest, source) = EvalManifest::from_file(&command.manifest)?;
    let mut template_override = command.judge_template.clone();
    let changed = resolve_judge_paths(&mut manifest, &command.manifest, &mut template_override)?;
    command.judge_template = template_override;
    let mut imported = Vec::new();
    let mut seen_ids = std::collections::BTreeSet::new();
    let mut seen_runs = std::collections::BTreeSet::new();
    for arm in &manifest.arms {
        let arm_directory = command.captures.join(&arm.id);
        if !arm_directory.is_dir() {
            continue;
        }
        let mut directories = std::fs::read_dir(&arm_directory)
            .map_err(|error| EvalError::io(&arm_directory, error))?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.join("sample.json").is_file())
            .collect::<Vec<_>>();
        directories.sort();
        for directory in directories {
            let captured = captured_sample_from_directory(&directory, &arm.id, 0)?;
            let run_index = capture_run_index(&captured.sample.id)?;
            if !seen_ids.insert(captured.sample.id.clone()) {
                return Err(EvalError::Command(format!(
                    "duplicate captured sample id {:?}",
                    captured.sample.id
                )));
            }
            if !seen_runs.insert((arm.id.clone(), run_index)) {
                return Err(EvalError::Command(format!(
                    "duplicate captured run index {run_index} for arm {:?}",
                    arm.id
                )));
            }
            let mut sample = captured.sample;
            sample.run_index = run_index;
            imported.push((sample, captured.patch));
        }
    }
    if imported.is_empty() {
        return Err(EvalError::Command(format!(
            "capture root {} contains no sample.json files",
            command.captures.display()
        )));
    }
    let id = match command.id {
        Some(id) => EvalId::new(id)?,
        None => EvalId::generate(&manifest.name)?,
    };
    let created_at_ms = timestamp();
    let stored_source = if changed { manifest.to_toml()? } else { source };
    store.create(&stored_source, &id, created_at_ms)?;
    let cleanup_id = id.clone();
    let scorer_name = command
        .scorer
        .clone()
        .unwrap_or_else(|| manifest.options.scorer.clone());
    let options = JudgeOptions {
        model: command.judge_model,
        template: command.judge_template,
        scorer: Some(scorer_name),
        executor: None,
    };
    let result = async {
        let mut samples = Vec::with_capacity(imported.len());
        for (mut sample, patch) in imported {
            if let Some(patch) = patch.as_deref() {
                let patch_path = store.write_patch(&id, &sample.id, patch)?;
                sample.patch = Some(PatchArtifact {
                    path: patch_path,
                    sha256: hex_digest(patch.as_bytes()),
                    bytes: u64::try_from(patch.len()).unwrap_or(u64::MAX),
                });
            } else {
                sample.patch = None;
            }
            samples.push(sample);
        }
        let mut data = EvalData {
            id,
            created_at_ms,
            manifest,
            samples,
            report: None,
        };
        judge_with_template(store, &mut data, options, allow_fixture_executor).await?;
        data.report
            .ok_or_else(|| EvalError::ReportUnavailable(data.id.to_string()))
    }
    .await;
    if result.is_err() {
        let _ = store.clean(&cleanup_id);
    }
    result
}

fn capture_run_index(sample_id: &str) -> Result<u32, EvalError> {
    sample_id
        .rsplit_once('-')
        .and_then(|(_, suffix)| suffix.parse::<u32>().ok())
        .ok_or_else(|| {
            EvalError::Command(format!(
                "captured sample id {sample_id:?} has no numeric run suffix; refusing ambiguous X/Y pairing"
            ))
        })
}

fn resolve_judge_paths(
    manifest: &mut EvalManifest,
    manifest_path: &std::path::Path,
    template_override: &mut Option<String>,
) -> Result<bool, EvalError> {
    let base = manifest_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let base = if base.is_absolute() {
        base.to_owned()
    } else {
        std::path::absolute(base).map_err(|error| EvalError::io(base, error))?
    };
    let mut changed = false;
    if let Some(runtime) = manifest.options.judge_runtime_config.clone() {
        if !std::path::Path::new(&runtime).is_absolute() {
            let resolved = base.join(runtime);
            manifest.options.judge_runtime_config = Some(path_string(&resolved));
            changed = true;
        }
    }
    let effective = template_override
        .clone()
        .or_else(|| manifest.options.judge_template.clone());
    if let Some(template) = effective {
        if template != JUDGE_TEMPLATE_NAME && !std::path::Path::new(&template).is_absolute() {
            let resolved = base.join(template);
            let resolved = path_string(&resolved);
            if template_override.is_some() {
                *template_override = Some(resolved.clone());
            }
            manifest.options.judge_template = Some(resolved);
            changed = true;
        }
    }
    Ok(changed)
}

fn path_string(path: &std::path::Path) -> String {
    path.to_string_lossy().into_owned()
}

fn judge_configured(
    manifest: &EvalManifest,
    run_model: Option<&str>,
    run_template: Option<&str>,
) -> bool {
    run_model.is_some()
        || run_template.is_some()
        || manifest.options.judge_model.is_some()
        || manifest.options.judge_template.is_some()
        || manifest.arms.iter().any(|arm| arm.judge_model.is_some())
}

async fn judge_with_template(
    store: &EvalStore,
    data: &mut EvalData,
    mut options: JudgeOptions,
    allow_fixture_executor: bool,
) -> Result<(), EvalError> {
    let reference = options
        .template
        .as_deref()
        .or(data.manifest.options.judge_template.as_deref())
        .unwrap_or(JUDGE_TEMPLATE_NAME);
    let template = resolve_judge_template(reference)?;
    let (executor, executor_kind): (Box<dyn JudgeGraphExecutor>, &str) = match data
        .manifest
        .options
        .judge_runtime_config
        .as_deref()
    {
        Some(runtime_path) => {
            let manifest_directory = store.root().join(data.id.as_str());
            let workspace = store
                .root()
                .parent()
                .and_then(std::path::Path::parent)
                .unwrap_or_else(|| store.root())
                .to_owned();
            let runtime_path = resolve_manifest_path(&manifest_directory, runtime_path)?;
            let runtime = load_judge_runtime(&runtime_path)?;
            let backend =
                crate::native_v2_cli::local::LocalCliBackend::production().map_err(|error| {
                    EvalError::Command(format!("judge backend is unavailable: {error}"))
                })?;
            (
                Box::new(LocalJudgeGraphExecutor::new(
                    Arc::new(backend),
                    runtime,
                    workspace,
                )),
                "graph",
            )
        }
        None if allow_fixture_executor => (Box::new(super::FixtureJudgeGraphExecutor), "fixture"),
        None => {
            return Err(EvalError::Judge(format!(
                "eval judge requires a real judge runtime; set options.judgeRuntimeConfig in the manifest (the synthetic fixture executor is test-only and cannot judge model {})",
                options.model.as_deref().unwrap_or("<unset>")
            )));
        }
    };
    options.executor = Some(executor_kind.to_owned());
    let judge = TemplateJudge::new(executor, template);
    judge_eval(store, data, &judge, options).await?;
    Ok(())
}

pub(super) fn resolve_manifest_path(
    manifest_directory: &std::path::Path,
    value: &str,
) -> Result<std::path::PathBuf, EvalError> {
    let path = std::path::Path::new(value);
    if path.is_absolute() {
        return Ok(path.to_owned());
    }
    let base = if manifest_directory.is_absolute() {
        manifest_directory.to_owned()
    } else {
        std::path::absolute(manifest_directory)
            .map_err(|error| EvalError::io(manifest_directory, error))?
    };
    Ok(base.join(path))
}

pub(crate) async fn run_manifest_through_fake(
    command: EvalRunCommand,
) -> Result<EvalData, EvalError> {
    run_manifest(command).await
}

async fn run_manifest(command: EvalRunCommand) -> Result<EvalData, EvalError> {
    let (manifest, _) = EvalManifest::from_file(&command.manifest)?;
    match select_runner(&manifest)? {
        RunnerSelection::Fake => {
            let runner =
                FakeRunner::from_fixture(manifest.options.fake.clone(), manifest.options.seed);
            // Fixture runs are deterministic and need no durable resume journal.
            let journal = tempfile::tempdir().map_err(|error| EvalError::io(".", error))?;
            run_manifest_with_runner(ManifestRunRequest {
                command,
                runner: &runner,
                capacity_source: None,
                concurrency: None,
                journal_directory: journal.path().to_path_buf(),
                request: |eval_id: &EvalId,
                          _: &str,
                          arm: &EvalArm,
                          run_index: u32,
                          criteria: &[EvalCriterion]| {
                    FakeSampleRequest::fixture(
                        eval_id.clone(),
                        arm.clone(),
                        run_index,
                        criteria.to_vec(),
                    )
                },
            })
            .await
        }
        selection => Err(EvalError::Command(format!(
            "runner {selection:?} requires a native-v2 backend; use run_manifest_with_backend"
        ))),
    }
}

/// Run a manifest through an explicitly selected backend runner.
///
/// `PlatformRunner` is selected for `runner = "platform"` and `LocalRunner` for `runner = "local"`.
/// The caller supplies the backend and the per-sample request factory because a backend-owned run
/// request is outside the manifest's data model.
pub async fn run_manifest_with_backend<B, F>(
    request: BackendRunRequest<'_, B, F>,
) -> Result<EvalData, EvalError>
where
    B: NativeV2CliBackend,
    F: FnMut(&EvalId, &str, &EvalArm, u32, &[EvalCriterion]) -> BackendSampleRequest,
{
    let BackendRunRequest {
        command,
        backend,
        target,
        capacity_source,
        concurrency,
        journal_directory,
        request,
    } = request;
    let (manifest, _) = EvalManifest::from_file(&command.manifest)?;
    match select_runner(&manifest)? {
        RunnerSelection::Platform => {
            let runner =
                PlatformRunner::new(backend, target.unwrap_or_else(|| "platform".to_owned()));
            run_manifest_with_runner(ManifestRunRequest {
                command,
                runner: &runner,
                capacity_source,
                concurrency,
                journal_directory,
                request,
            })
            .await
        }
        RunnerSelection::Local => {
            let runner = LocalRunner::new(backend);
            run_manifest_with_runner(ManifestRunRequest {
                command,
                runner: &runner,
                capacity_source,
                concurrency,
                journal_directory,
                request,
            })
            .await
        }
        RunnerSelection::Fake => Err(EvalError::Command(
            "runner \"fake\" does not use a native-v2 backend".to_owned(),
        )),
    }
}

/// Run one manifest through the unified [`SampleRunner`] seam and persist its store data.
///
/// Capacity is resolved once by [`super::run_eval`], every sample is journaled through it, and the
/// resulting artifacts are translated into the slice-1 stored sample records.
pub async fn run_manifest_with_runner<R, F>(
    request: ManifestRunRequest<'_, R, F>,
) -> Result<EvalData, EvalError>
where
    R: SampleRunner,
    F: FnMut(&EvalId, &str, &EvalArm, u32, &[EvalCriterion]) -> R::Request,
{
    let ManifestRunRequest {
        mut command,
        runner,
        capacity_source,
        concurrency,
        journal_directory,
        mut request,
    } = request;
    let (mut manifest, source) = EvalManifest::from_file(&command.manifest)?;
    let changed = resolve_judge_paths(
        &mut manifest,
        &command.manifest,
        &mut command.judge_template,
    )?;
    let store = EvalStore::for_workspace(&command.directory);
    let id = match command.id {
        Some(id) => EvalId::new(id)?,
        None => EvalId::generate(&manifest.name)?,
    };
    let created_at_ms = timestamp();
    let stored_source = if changed { manifest.to_toml()? } else { source };
    store.create(&stored_source, &id, created_at_ms)?;
    let scorer_name = command
        .scorer
        .as_deref()
        .unwrap_or(&manifest.options.scorer)
        .to_owned();
    let mut planned = Vec::new();
    let mut samples = Vec::new();
    for arm in &manifest.arms {
        for run_index in 0..manifest.runs_per_arm {
            let sample_id = format!("{}-{run_index:04}", arm.id);
            let sample_request = request(&id, &sample_id, arm, run_index, &manifest.criteria);
            planned.push(SamplePlanMeta {
                sample_id: sample_id.clone(),
                arm_id: arm.id.clone(),
                run_index,
                directory: journal_directory.join(&sample_id),
            });
            samples.push(EvalSamplePlan::new(
                sample_id.clone(),
                journal_directory.join(&sample_id),
                sample_request,
            ));
        }
    }
    let run_report = super::run_eval(EvalRunRequest {
        runner,
        capacity_source,
        concurrency,
        journal_directory,
        samples,
    })
    .await
    .map_err(run_error)?;
    let mut results = BTreeMap::new();
    for artifact in run_report.artifacts {
        results.insert(artifact.sample_id.clone(), Ok(artifact));
    }
    for failure in run_report.failures {
        results.insert(failure.sample_id, Err(failure.error));
    }
    let mut data = EvalData {
        id,
        created_at_ms,
        manifest,
        samples: Vec::new(),
        report: None,
    };
    for meta in planned {
        let sample = match results.remove(&meta.sample_id) {
            Some(Ok(artifact)) => sample_from_artifact(&store, &data.id, artifact, &meta)?,
            Some(Err(error)) => failed_sample(&data.id, &meta, error),
            None => failed_sample(&data.id, &meta, "sample did not settle".to_owned()),
        };
        store.write_sample(&data.id, &sample)?;
        data.samples.push(sample);
    }
    let report = recompute(&data, &scorer_name, Some(run_report.capacity))?;
    store.write_report(&data.id, &report)?;
    store.write_report_markdown(&data.id, &report)?;
    data.report = Some(report);
    if judge_configured(
        &data.manifest,
        command.judge_model.as_deref(),
        command.judge_template.as_deref(),
    ) {
        judge_with_template(
            &store,
            &mut data,
            JudgeOptions {
                model: command.judge_model,
                template: command.judge_template,
                scorer: Some(scorer_name),
                executor: None,
            },
            false,
        )
        .await?;
    }
    Ok(data)
}

fn run_error(error: EvalRunError) -> EvalError {
    EvalError::Command(error.to_string())
}

#[derive(Clone)]
pub(crate) struct SamplePlanMeta {
    pub(crate) sample_id: String,
    pub(crate) arm_id: String,
    pub(crate) run_index: u32,
    pub(crate) directory: PathBuf,
}

pub(crate) fn sample_from_artifact(
    store: &EvalStore,
    eval_id: &EvalId,
    artifact: SampleArtifact,
    meta: &SamplePlanMeta,
) -> Result<EvalSample, EvalError> {
    let id = artifact.sample_id.clone();
    let captured = artifact.captured.as_ref();
    let judge_checkout = match captured.and_then(|captured| captured.judge.as_ref()) {
        Some(judge) => Some(
            super::judge_runner::capture_relative_path(
                &meta.directory,
                &judge.directory,
                "judge checkout",
            )?
            .to_string_lossy()
            .into_owned(),
        ),
        None => None,
    };
    let captured_patch = match captured {
        Some(captured) if !captured.patch.as_os_str().is_empty() => {
            let path = super::judge_runner::capture_relative_path(
                &meta.directory,
                &captured.patch,
                "captured patch",
            )?;
            Some(std::fs::read_to_string(&path).map_err(|error| EvalError::io(&path, error))?)
        }
        _ => None,
    };
    let SampleOutcome {
        run_id,
        status,
        timings,
        token_usage,
        delivered_revision,
        failure_reason,
        patch,
        criteria,
        reasoning,
        ..
    } = artifact.outcome;
    let patch_content = patch.map(|patch| patch.content).or(captured_patch);
    let patch = match patch_content {
        Some(content) => {
            let path = store.write_patch(eval_id, &id, &content)?;
            Some(PatchArtifact {
                path,
                sha256: hex_digest(content.as_bytes()),
                bytes: u64::try_from(content.len()).unwrap_or(u64::MAX),
            })
        }
        None => None,
    };
    Ok(EvalSample {
        id,
        run_id: run_id.as_str().to_owned(),
        arm_id: meta.arm_id.clone(),
        run_index: meta.run_index,
        status,
        timing: SampleTiming {
            started_at_ms: timings.started_at_ms.unwrap_or(timings.submitted_at_ms),
            wall_clock_ms: timings.wall_clock_ms,
            queue_wait_ms: timings.queue_wait_ms,
        },
        tokens: eval_tokens(token_usage),
        delivered_revision,
        failure_reason,
        patch,
        criteria,
        reasoning,
        judge_model: None,
        judge_template: None,
        judge_checkout,
        judge_executor: None,
    })
}

fn failed_sample(eval_id: &EvalId, meta: &SamplePlanMeta, reason: String) -> EvalSample {
    EvalSample {
        id: meta.sample_id.clone(),
        run_id: format!("fake-{eval_id}-{}", meta.sample_id),
        arm_id: meta.arm_id.clone(),
        run_index: meta.run_index,
        status: SampleStatus::Failed,
        timing: SampleTiming {
            started_at_ms: now_ms(),
            wall_clock_ms: 0,
            queue_wait_ms: 0,
        },
        tokens: TokenUsage::default(),
        delivered_revision: None,
        failure_reason: Some(reason),
        patch: None,
        criteria: Default::default(),
        reasoning: Default::default(),
        judge_model: None,
        judge_template: None,
        judge_checkout: None,
        judge_executor: None,
    }
}

fn eval_tokens(usage: Option<ProtocolTokenUsage>) -> TokenUsage {
    usage.map_or_else(TokenUsage::default, |usage| TokenUsage {
        input: usage.input_tokens.get(),
        output: usage.output_tokens.get(),
        cache: usage
            .cache_read_input_tokens
            .map_or(0, |tokens| tokens.get())
            .saturating_add(
                usage
                    .cache_creation_input_tokens
                    .map_or(0, |tokens| tokens.get()),
            ),
    })
}

fn ensure_report(store: &EvalStore, data: &EvalData) -> Result<EvalReport, EvalError> {
    if let Some(report) = &data.report {
        store.write_report_markdown(&data.id, report)?;
        return Ok(report.clone());
    }
    let capacity = data.report.as_ref().and_then(|report| report.capacity);
    let report = recompute(data, &data.manifest.options.scorer, capacity)?;
    store.write_report(&data.id, &report)?;
    store.write_report_markdown(&data.id, &report)?;
    Ok(report)
}

pub(crate) fn recompute(
    data: &EvalData,
    scorer_name: &str,
    capacity: Option<Capacity>,
) -> Result<EvalReport, EvalError> {
    let registry = ScorerRegistry::builtin();
    let scorer = registry.require(scorer_name)?;
    let analysis = report::analyze(data);
    let output = {
        let input = ScorerInput {
            data,
            matrix: &analysis.matrix,
            cost_time: &analysis.cost_time,
            quality: analysis.quality,
        };
        scorer.score(&input)?
    };
    Ok(EvalReport::from_analysis(
        data,
        analysis,
        output,
        report::ReportStamp {
            capacity,
            generated_at_ms: timestamp(),
        },
    ))
}

fn clean(store: &EvalStore, id: Option<&str>, all: bool) -> Result<Value, EvalError> {
    match (id, all) {
        (Some(_), true) => Err(EvalError::Command(
            "clean accepts either an eval id or --all, not both".to_owned(),
        )),
        (Some(id), false) => {
            let id = EvalId::new(id.to_owned())?;
            store.clean(&id)?;
            Ok(json!({ "removed": [id] }))
        }
        (None, true) => {
            let removed = store.clean_all()?;
            Ok(json!({ "removed": removed }))
        }
        (None, false) => Err(EvalError::Command(
            "clean requires an eval id or --all".to_owned(),
        )),
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
#[path = "cli_test.rs"]
mod tests;
