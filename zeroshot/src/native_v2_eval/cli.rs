use std::path::PathBuf;

use openengine_cluster_protocol::TokenUsage as ProtocolTokenUsage;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{
    EvalData, EvalError, EvalId, EvalManifest, EvalReport, EvalSample, EvalStore,
    FakeFixtureRequest, FakeRunner, FakeSampleRequest, PatchArtifact, SampleOutcome, SampleRun,
    SampleRunner, SampleStatus, SampleTiming, ScorerInput, ScorerRegistry, TokenUsage,
    model::now_ms, report, store::timestamp,
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

/// The executable selected for an eval manifest.
///
/// The manifest's `options.runner` is the explicit switch: fixtures use [`FakeRunner`], while
/// `platform` and `local` select the corresponding native-v2 backend runner. A caller that owns a
/// backend constructs [`super::PlatformRunner`] or [`super::LocalRunner`] with this selection; the
/// standalone CLI command only has the fixture runner available.
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
            let report = recompute(&data, scorer)?;
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
    let (manifest, source) = EvalManifest::from_file(&command.manifest)?;
    let selection = select_runner(&manifest)?;
    if selection != RunnerSelection::Fake {
        return Err(EvalError::Command(format!(
            "runner {:?} requires a native-v2 backend; the standalone eval command can only run fixtures",
            manifest.options.runner
        )));
    }
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
    let runner = FakeRunner::from_fixture(manifest.options.fake.clone(), manifest.options.seed);
    let mut data = EvalData {
        id,
        created_at_ms,
        manifest,
        samples: Vec::new(),
        report: None,
    };
    execute_with_runner(&store, &mut data, &runner, &scorer_name).await?;
    Ok(data)
}

async fn execute_with_runner(
    store: &EvalStore,
    data: &mut EvalData,
    runner: &FakeRunner,
    scorer_name: &str,
) -> Result<EvalReport, EvalError> {
    let arms = data.manifest.arms.clone();
    let criteria = data.manifest.criteria.clone();
    let runs_per_arm = data.manifest.runs_per_arm;
    for arm in arms {
        for run_index in 0..runs_per_arm {
            let request = FakeSampleRequest::fixture(
                data.id.clone(),
                arm.clone(),
                run_index,
                criteria.clone(),
            );
            let sample = match runner.submit(request.clone()).await {
                Ok(run) => settle_sample(store, &request, runner, run).await?,
                Err(error) => failed_sample(
                    &request,
                    synthetic_run_id(&data.id, &request),
                    error.to_string(),
                ),
            };
            store.write_sample(&data.id, &sample)?;
            data.samples.push(sample);
        }
    }
    let report = recompute(data, scorer_name)?;
    store.write_report(&data.id, &report)?;
    store.write_report_markdown(&data.id, &report)?;
    data.report = Some(report.clone());
    Ok(report)
}

async fn settle_sample(
    store: &EvalStore,
    request: &FakeSampleRequest,
    runner: &FakeRunner,
    run: SampleRun,
) -> Result<EvalSample, EvalError> {
    match runner.wait(&run).await {
        Ok(outcome) => sample_from_outcome(store, request, outcome),
        Err(error) => Ok(failed_sample(
            request,
            run.run_id.as_str().to_owned(),
            error.to_string(),
        )),
    }
}

fn sample_from_outcome(
    store: &EvalStore,
    request: &FakeSampleRequest,
    outcome: SampleOutcome,
) -> Result<EvalSample, EvalError> {
    let Some(FakeFixtureRequest {
        eval_id,
        arm,
        run_index,
        ..
    }) = &request.fixture
    else {
        return Err(EvalError::Runner(
            "fixture request lost its manifest identity".to_owned(),
        ));
    };
    let id = request.sample_id.clone();
    let patch = match outcome.patch {
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
        run_id: outcome.run_id.as_str().to_owned(),
        arm_id: arm.id.clone(),
        run_index: *run_index,
        status: outcome.status,
        timing: SampleTiming {
            started_at_ms: outcome
                .timings
                .started_at_ms
                .unwrap_or(outcome.timings.submitted_at_ms),
            wall_clock_ms: outcome.timings.wall_clock_ms,
            queue_wait_ms: outcome.timings.queue_wait_ms,
        },
        tokens: eval_tokens(outcome.token_usage),
        delivered_revision: outcome.delivered_revision,
        failure_reason: outcome.failure_reason,
        patch,
        criteria: outcome.criteria,
        reasoning: outcome.reasoning,
    })
}

fn failed_sample(request: &FakeSampleRequest, run_id: String, reason: String) -> EvalSample {
    let Some(fixture) = &request.fixture else {
        return EvalSample {
            id: request.sample_id.clone(),
            run_id,
            arm_id: String::new(),
            run_index: 0,
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
        };
    };
    EvalSample {
        id: request.sample_id.clone(),
        run_id,
        arm_id: fixture.arm.id.clone(),
        run_index: fixture.run_index,
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

fn synthetic_run_id(eval_id: &EvalId, request: &FakeSampleRequest) -> String {
    match &request.fixture {
        Some(fixture) => format!("fake-{eval_id}-{}-{:04}", fixture.arm.id, fixture.run_index),
        None => format!("fake-{eval_id}-{}", request.sample_id),
    }
}

fn ensure_report(store: &EvalStore, data: &EvalData) -> Result<EvalReport, EvalError> {
    if let Some(report) = &data.report {
        store.write_report_markdown(&data.id, report)?;
        return Ok(report.clone());
    }
    let report = recompute(data, &data.manifest.options.scorer)?;
    store.write_report(&data.id, &report)?;
    store.write_report_markdown(&data.id, &report)?;
    Ok(report)
}

fn recompute(data: &EvalData, scorer_name: &str) -> Result<EvalReport, EvalError> {
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
        timestamp(),
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
