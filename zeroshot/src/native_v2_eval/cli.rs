use std::path::PathBuf;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{
    EvalData, EvalError, EvalId, EvalManifest, EvalReport, EvalSample, EvalStore, FakeRunner,
    PatchArtifact, SampleHandle, SampleOutcome, SampleRequest, SampleRunner, SampleStatus,
    SampleTiming, ScorerInput, ScorerRegistry, TokenUsage, model::now_ms, report, store::timestamp,
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

pub async fn execute(command: &EvalCommand) -> Result<Value, EvalError> {
    let store = EvalStore::for_workspace(&command.directory);
    match &command.action {
        EvalAction::Run(run) => {
            let data = run_manifest_through_fake(run.clone()).await?;
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
    let runner = FakeRunner::new(manifest.options.fake.clone(), manifest.options.seed);
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

async fn execute_with_runner<S: SampleRunner>(
    store: &EvalStore,
    data: &mut EvalData,
    runner: &S,
    scorer_name: &str,
) -> Result<EvalReport, EvalError> {
    let arms = data.manifest.arms.clone();
    let criteria = data.manifest.criteria.clone();
    let runs_per_arm = data.manifest.runs_per_arm;
    for arm in arms {
        for run_index in 0..runs_per_arm {
            let request = SampleRequest {
                eval_id: data.id.clone(),
                arm: arm.clone(),
                run_index,
                criteria: criteria.clone(),
            };
            let submitted = runner.submit(request.clone()).await;
            let sample = match submitted {
                Ok(handle) => settle_sample(store, &request, runner, handle).await?,
                Err(error) => failed_sample(
                    &request,
                    format!("fake-{}-{}-{:04}", data.id, request.arm.id, run_index),
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

async fn settle_sample<S: SampleRunner>(
    store: &EvalStore,
    request: &SampleRequest,
    runner: &S,
    handle: SampleHandle,
) -> Result<EvalSample, EvalError> {
    match runner.wait(handle.clone()).await {
        Ok(outcome) => sample_from_outcome(store, request, &handle.run_id, outcome),
        Err(error) => Ok(failed_sample(request, handle.run_id, error.to_string())),
    }
}

fn sample_from_outcome(
    store: &EvalStore,
    request: &SampleRequest,
    run_id: &str,
    outcome: SampleOutcome,
) -> Result<EvalSample, EvalError> {
    let id = sample_id(&request.arm.id, request.run_index);
    let patch = match outcome.patch {
        Some(patch) => {
            let path = store.write_patch(&request.eval_id, &id, &patch.content)?;
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
        run_id: run_id.to_owned(),
        arm_id: request.arm.id.clone(),
        run_index: request.run_index,
        status: outcome.status,
        timing: outcome.timing,
        tokens: outcome.tokens,
        delivered_revision: outcome.delivered_revision,
        failure_reason: outcome.failure_reason,
        patch,
        criteria: outcome.criteria,
        reasoning: outcome.reasoning,
    })
}

fn failed_sample(request: &SampleRequest, run_id: String, reason: String) -> EvalSample {
    EvalSample {
        id: sample_id(&request.arm.id, request.run_index),
        run_id,
        arm_id: request.arm.id.clone(),
        run_index: request.run_index,
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

fn sample_id(arm_id: &str, run_index: u32) -> String {
    format!("{arm_id}-{run_index:04}")
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
