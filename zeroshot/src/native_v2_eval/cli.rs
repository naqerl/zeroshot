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
    EvalStore, FakeRunner, FakeSampleRequest, LocalRunner, PatchArtifact, PlatformRunner,
    SampleArtifact, SampleOutcome, SampleRunner, SampleStatus, SampleTiming, ScorerInput,
    ScorerRegistry, TokenUsage, model::now_ms, report, store::timestamp,
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
    Clean { id: Option<String>, all: bool },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvalRunCommand {
    pub manifest: PathBuf,
    pub directory: PathBuf,
    pub id: Option<String>,
    pub scorer: Option<String>,
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
        EvalAction::Clean { id, all } => clean(&store, id.as_deref(), *all),
    }
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
        command,
        runner,
        capacity_source,
        concurrency,
        journal_directory,
        mut request,
    } = request;
    let (manifest, source) = EvalManifest::from_file(&command.manifest)?;
    let store = EvalStore::for_workspace(&command.directory);
    let id = match command.id {
        Some(id) => EvalId::new(id)?,
        None => EvalId::generate(&manifest.name)?,
    };
    let created_at_ms = timestamp();
    store.create(&source, &id, created_at_ms)?;
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
    Ok(data)
}

fn run_error(error: EvalRunError) -> EvalError {
    EvalError::Command(error.to_string())
}

#[derive(Clone)]
struct SamplePlanMeta {
    sample_id: String,
    arm_id: String,
    run_index: u32,
}

fn sample_from_artifact(
    store: &EvalStore,
    eval_id: &EvalId,
    artifact: SampleArtifact,
    meta: &SamplePlanMeta,
) -> Result<EvalSample, EvalError> {
    let id = artifact.sample_id.clone();
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
    let patch = match patch {
        Some(patch) => {
            let path = store.write_patch(eval_id, &id, &patch.content)?;
            Some(PatchArtifact {
                path,
                sha256: hex_digest(patch.content.as_bytes()),
                bytes: u64::try_from(patch.content.len()).unwrap_or(u64::MAX),
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

fn recompute(
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
