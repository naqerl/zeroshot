//! The eval store commands and the `eval run` execution path.
//!
//! `list`, `show`, `create`, and `clean` only touch the eval directory, so they stay usable without
//! a target. `run` is the one eval command that submits work: every arm's samples go to the eval's
//! named target, what came back is judged by the eval's judge graph, and the artifacts described in
//! [`crate::native_v2_eval::run`] are published under a fresh stamp directory.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use openengine_cluster_protocol::{
    HostedRunStatus, HostedRunStatusResult, RunId, RunStatus, RunStatusParams, RunTitle,
    TerminalResult,
};
use serde_json::Value;

use super::submission::{RunSubmission, submit_run};
use super::{CliExecutionContext, write_json};
use crate::native_v2_cli::{
    CliOutcome, DetachSignal, EvalCommand, EvalRunCommand, NativeV2CliBackend, NativeV2CliError,
    RunCommand, RunGraph, RunInput, RunRuntime, RunSelection,
};
use crate::native_v2_eval::config;
use crate::native_v2_eval::run as artifacts;
use crate::native_v2_eval::run::{
    INPUT_ARTIFACT, JUDGE_DIRECTORY, JUDGE_INPUT_ARTIFACT, JUDGE_RESULT_ARTIFACT, JudgeResult,
    RESULT_ARTIFACT, SOURCE_ARTIFACT, SampleResult, SampleStatus, SampleSummary,
};
use crate::native_v2_eval::{EvalArm, EvalDocument, EvalStore};

/// File names of the exact definitions one submission used.
///
/// They are not part of the published artifact set the report reads, but keeping them beside the
/// evidence makes a stamp directory replayable without the eval document.
const GRAPH_ARTIFACT: &str = artifacts::GRAPH_ARTIFACT;
const RUNTIME_ARTIFACT: &str = artifacts::RUNTIME_ARTIFACT;

/// How often a submitted sample is asked whether it settled.
const SETTLEMENT_POLL: Duration = Duration::from_secs(2);
/// How long one sample may run before the eval gives up on it.
///
/// Node deadlines already bound an authored graph; this only stops an eval from waiting forever on
/// a target that lost a run.
const SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(12 * 60 * 60);

/// Longest run title an eval builds before asking the protocol type to validate it.
const TITLE_CHARACTERS: usize = 200;

pub(super) fn list(root: &Path, output: &mut impl Write) -> Result<CliOutcome, NativeV2CliError> {
    let summaries = EvalStore::new(root).list().map_err(cli_error)?;
    write_json(output, &summaries)?;
    Ok(CliOutcome::Completed)
}

pub(super) fn show(
    root: &Path,
    slug: &str,
    output: &mut impl Write,
) -> Result<CliOutcome, NativeV2CliError> {
    let document = EvalStore::new(root).load(slug).map_err(cli_error)?;
    write_json(output, &document)?;
    Ok(CliOutcome::Completed)
}

pub(super) fn create(
    root: &Path,
    file: &Path,
    overwrite: bool,
    output: &mut impl Write,
) -> Result<CliOutcome, NativeV2CliError> {
    let bytes = std::fs::read(file).map_err(|source| NativeV2CliError::Read {
        kind: "eval",
        path: file.to_path_buf(),
        source,
    })?;
    let document = serde_json::from_slice::<EvalDocument>(&bytes).map_err(|source| {
        NativeV2CliError::Json {
            kind: "eval",
            path: file.to_path_buf(),
            source,
        }
    })?;
    EvalStore::new(root)
        .create(&document, overwrite)
        .map_err(cli_error)?;
    write_json(
        output,
        &serde_json::json!({
            "slug": document.slug,
            "overwritten": overwrite,
        }),
    )?;
    Ok(CliOutcome::Completed)
}

pub(super) fn clean(
    root: &Path,
    slug: &str,
    output: &mut impl Write,
) -> Result<CliOutcome, NativeV2CliError> {
    let removed = EvalStore::new(root).remove(slug).map_err(cli_error)?;
    write_json(
        output,
        &serde_json::json!({
            "slug": slug,
            "removed": removed,
        }),
    )?;
    Ok(CliOutcome::Completed)
}

/// Execute one eval command: the store commands here, `run` through the backend.
pub(super) async fn execute<B, S, W>(
    command: EvalCommand,
    context: &CliExecutionContext<'_, B>,
    signal: &mut S,
    output: &mut W,
) -> Result<CliOutcome, NativeV2CliError>
where
    B: NativeV2CliBackend,
    S: DetachSignal,
    W: Write,
{
    match command {
        EvalCommand::Run(command) => {
            execute_run(EvalRunRequest {
                command,
                context,
                signal,
                output,
            })
            .await
        }
        other => execute_store_command(&other, output),
    }
}

/// Execute one eval store command, which never needs a backend.
pub(super) fn execute_store_command(
    command: &EvalCommand,
    output: &mut impl Write,
) -> Result<CliOutcome, NativeV2CliError> {
    match command {
        EvalCommand::List { root } => list(root, output),
        EvalCommand::Show { root, slug } => show(root, slug, output),
        EvalCommand::Create {
            root,
            file,
            overwrite,
        } => create(root, file, *overwrite, output),
        EvalCommand::Clean { root, slug } => clean(root, slug, output),
        EvalCommand::Run(_) => Err(NativeV2CliError::Usage(
            "eval run submits runs and is never a store command".to_owned(),
        )),
    }
}

struct EvalRunRequest<'a, 'c, B, S, W> {
    command: EvalRunCommand,
    context: &'a CliExecutionContext<'c, B>,
    signal: &'a mut S,
    output: &'a mut W,
}

async fn execute_run<B, S, W>(
    request: EvalRunRequest<'_, '_, B, S, W>,
) -> Result<CliOutcome, NativeV2CliError>
where
    B: NativeV2CliBackend,
    S: DetachSignal,
    W: Write,
{
    let EvalRunRequest {
        command,
        context,
        signal,
        output,
    } = request;
    let root = absolute(&command.root)?;
    let store = EvalStore::new(&root);
    let document = store.load(&command.slug).map_err(cli_error)?;
    // Evals are stored inside the checkout they evaluate, so that checkout is where the sample
    // repository, branch and worktree come from, exactly as for any other named `run`.
    let workspace = config::workspace_root(&store).map_err(cli_error)?;
    let target = config::resolve_target(&document, command.target.as_deref()).map_err(cli_error)?;
    let judge = config::judge_plan(&document).map_err(cli_error)?;
    if let Some(judge) = &judge {
        config::validate_judge_plan(&document, judge)
            .await
            .map_err(cli_error)?;
    }
    // Resolve every branch-named arm exactly once, before any sample starts. Every sample and the
    // judge reuse that revision, so a branch moving mid-run cannot split the comparison.
    let mut sources = BTreeMap::new();
    for arm in &document.arms {
        let source = config::pin_arm_source(&workspace, &document, arm)
            .await
            .map_err(cli_error)?;
        sources.insert(arm.id.clone(), source);
    }
    let stamp = artifacts::create_stamp(&store.runs_root(&document.slug), SystemTime::now())
        .map_err(cli_error)?;
    let stamp_name = stamp
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_owned();

    let mut runner = Runner {
        context,
        signal,
        output,
        document: &document,
        target: &target,
        stamp: &stamp,
        judge,
        workspace: &workspace,
        input: document.input.clone(),
        sources,
        interrupted: false,
    };
    let (samples, interrupted) = runner.run().await?;
    let Runner { output, .. } = runner;

    let mut summary = artifacts::summarize(&document, &stamp_name, &target, samples);
    summary.interrupted = interrupted;
    artifacts::write_text(
        &stamp.join(artifacts::REPORT_ARTIFACT),
        &artifacts::report(&summary),
    )
    .map_err(cli_error)?;
    artifacts::replace_json(
        &store
            .runs_root(&document.slug)
            .join(artifacts::SUMMARY_ARTIFACT),
        &summary,
    )
    .map_err(cli_error)?;
    write_json(output, &summary)?;
    if interrupted {
        return Ok(CliOutcome::Detached);
    }
    Ok(CliOutcome::Completed)
}

/// One `eval run`, from the resolved document to the aggregate.
struct Runner<'a, 'c, B, S, W> {
    context: &'a CliExecutionContext<'c, B>,
    signal: &'a mut S,
    output: &'a mut W,
    document: &'a EvalDocument,
    target: &'a str,
    stamp: &'a Path,
    judge: Option<config::JudgePlan>,
    /// The checkout the eval document is stored in, which supplies the sample source.
    workspace: &'a Path,
    input: Value,
    /// Each arm's source revision, resolved once for this run.
    sources: BTreeMap<String, config::ArmSource>,
    /// Set once the caller asked to detach, so no later sample starts a new run.
    interrupted: bool,
}

impl<'a, 'c, B, S, W> Runner<'a, 'c, B, S, W>
where
    B: NativeV2CliBackend,
    S: DetachSignal,
    W: Write,
{
    /// Run every authored sample, stopping at the first detach request.
    async fn run(&mut self) -> Result<(Vec<SampleSummary>, bool), NativeV2CliError> {
        let document = self.document;
        let mut samples = Vec::new();
        for arm in &document.arms {
            let source = self.sources.get(&arm.id).cloned().ok_or_else(|| {
                NativeV2CliError::Local(format!(
                    "eval {:?} arm {:?} has no resolved source",
                    document.slug, arm.id
                ))
            })?;
            for index in 0..document.runs_per_arm {
                if let Some(summary) = self.sample(arm, index, &source).await? {
                    samples.push(summary);
                }
                if self.interrupted {
                    return Ok((samples, true));
                }
            }
        }
        Ok((samples, false))
    }

    /// Run and, when an eval declares one, judge one sample.
    async fn sample(
        &mut self,
        arm: &EvalArm,
        index: u32,
        source: &config::ArmSource,
    ) -> Result<Option<SampleSummary>, NativeV2CliError> {
        let directory = artifacts::sample_directory(self.stamp, &arm.id, index);
        let input = directory.join(INPUT_ARTIFACT);
        artifacts::write_json(&input, &self.input).map_err(cli_error)?;
        artifacts::write_json(&directory.join(SOURCE_ARTIFACT), source).map_err(cli_error)?;
        artifacts::write_json(&directory.join(GRAPH_ARTIFACT), &arm.graph).map_err(cli_error)?;
        artifacts::write_json(&directory.join(RUNTIME_ARTIFACT), &arm.runtime)
            .map_err(cli_error)?;

        let title = title(self.document, &arm.id, index)?;
        let plan = SampleRun {
            directory: &directory,
            input: &input,
            source,
        };
        let Some(settled) = self.submit(&plan, title).await? else {
            return Ok(None);
        };

        let mut summary = SampleSummary {
            arm: arm.id.clone(),
            sample: index,
            run_id: settled.run_id.clone(),
            source: source.clone(),
            status: settled.status,
            reason: settled.reason.clone(),
            verdicts: Vec::new(),
        };
        artifacts::write_json(
            &directory.join(RESULT_ARTIFACT),
            &SampleResult {
                run_id: settled.run_id.clone(),
                status: settled.status,
                output: settled.output.clone(),
                reason: settled.reason.clone(),
            },
        )
        .map_err(cli_error)?;
        let Some(output) = settled.output else {
            return Ok(Some(summary));
        };
        let Some((judge_graph, judge_runtime)) = self
            .judge
            .as_ref()
            .map(|judge| (judge.graph.clone(), judge.runtime.clone()))
        else {
            return Ok(Some(summary));
        };

        let judge_directory = directory.join(JUDGE_DIRECTORY);
        let judge_input = judge_directory.join(JUDGE_INPUT_ARTIFACT);
        artifacts::write_json(
            &judge_input,
            &artifacts::judge_input(self.document, &artifacts::sample_patch(&output))
                .map_err(cli_error)?,
        )
        .map_err(cli_error)?;
        artifacts::write_json(&judge_directory.join(GRAPH_ARTIFACT), &judge_graph)
            .map_err(cli_error)?;
        artifacts::write_json(&judge_directory.join(RUNTIME_ARTIFACT), &judge_runtime)
            .map_err(cli_error)?;
        let plan = SampleRun {
            directory: &judge_directory,
            input: &judge_input,
            source,
        };
        let Some(settled) = self
            .submit(&plan, judge_title(self.document, index)?)
            .await?
        else {
            // The sample's own evidence is already published, so only the verdicts are lost.
            summary.reason =
                Some("the judge run did not finish before the eval stopped".to_owned());
            return Ok(Some(summary));
        };
        let mut judge_result = JudgeResult {
            run_id: settled.run_id,
            status: settled.status,
            verdicts: Vec::new(),
            reason: settled.reason,
        };
        if let Some(output) = settled.output {
            judge_result.verdicts =
                artifacts::verdicts(&output, &self.document.scoring.criteria).map_err(cli_error)?;
        }
        if judge_result.status == SampleStatus::Succeeded && judge_result.verdicts.is_empty() {
            // A judge that reported no verdict at all measured nothing, so recording it as a
            // successful judgement would report a sample the eval never actually scored.
            judge_result.status = SampleStatus::Failed;
            judge_result.reason = Some("it recorded no verdicts".to_owned());
        }
        artifacts::write_json(&judge_directory.join(JUDGE_RESULT_ARTIFACT), &judge_result)
            .map_err(cli_error)?;
        summary.verdicts = judge_result.verdicts;
        if judge_result.status == SampleStatus::Failed {
            // The judge is part of the measurement: when it fails, the sample is not a success
            // that merely lacks verdicts, and the aggregate has to count it as failed.
            summary.status = SampleStatus::Failed;
            summary.reason = Some(format!(
                "judge run {} failed: {}",
                judge_result.run_id,
                judge_result
                    .reason
                    .as_deref()
                    .unwrap_or("it recorded no verdicts")
            ));
        }
        Ok(Some(summary))
    }

    /// Submit one run and wait for its terminal record.
    ///
    /// `None` means the caller asked to detach before the run settled; the sample's own artifacts
    /// are then the evidence of what was started.
    async fn submit(
        &mut self,
        plan: &SampleRun<'_>,
        title: RunTitle,
    ) -> Result<Option<SettledRun>, NativeV2CliError> {
        let command = RunCommand {
            target: Some(self.target.to_owned()),
            cwd: Some(self.workspace.to_path_buf()),
            worktree: None,
            source_connection: None,
            source_forge: None,
            title,
            selection: RunSelection::Inline {
                graph: RunGraph::File(plan.directory.join(GRAPH_ARTIFACT)),
                runtime: RunRuntime::Exact(plan.directory.join(RUNTIME_ARTIFACT)),
            },
            environment: None,
            input: RunInput::File(plan.input.to_path_buf()),
            repository: None,
            branch: plan.source.branch.clone(),
            revision: plan.source.revision.clone(),
            detach: true,
            validate_only: false,
            submission_key: None,
            forward_github_token: false,
        };
        let Self {
            context,
            signal,
            interrupted: was_interrupted,
            ..
        } = self;
        let submission = {
            let detach = signal.wait();
            tokio::pin!(detach);
            // The submission's own record belongs to the sample's artifacts, so the run's stdout
            // stays the one aggregate a caller parses.
            submit_run(&command, context, detach.as_mut(), &mut Vec::new()).await?
        };
        let run_id = match submission {
            RunSubmission::Submitted {
                receipt,
                interrupted,
            } => {
                if interrupted {
                    // The caller asked to stop while the run was being submitted. The submission's
                    // detach signal already fired, so the eval ends here rather than polling a run
                    // the caller no longer waits for.
                    *was_interrupted = true;
                    return Ok(None);
                }
                receipt.run_id.as_str().to_owned()
            }
            RunSubmission::Interrupted => {
                *was_interrupted = true;
                return Ok(None);
            }
            RunSubmission::Validated => {
                return Err(NativeV2CliError::Usage(
                    "eval samples are never validated without being submitted".to_owned(),
                ));
            }
        };
        self.settle(&run_id).await
    }

    /// Poll one submitted run until it has a terminal record.
    async fn settle(&mut self, run_id: &str) -> Result<Option<SettledRun>, NativeV2CliError> {
        let context = self.context;
        let target = self.target;
        let deadline = Instant::now() + SETTLEMENT_TIMEOUT;
        let params = RunStatusParams {
            run_id: RunId::new(run_id),
        };
        loop {
            // The detach request bounds the status read itself, so Ctrl-C ends the command instead
            // of waiting on a target that never answers.
            let reported = {
                let detach = self.signal.wait();
                tokio::pin!(detach);
                tokio::select! {
                    biased;
                    () = detach.as_mut() => None,
                    status = context.backend.run_status(Some(target), params.clone()) => {
                        Some(status?)
                    }
                }
            };
            let Some(status) = reported else {
                self.interrupted = true;
                return Ok(None);
            };
            if let Some(settled) = settled_from(&status, run_id) {
                return Ok(Some(settled));
            }
            if Instant::now() >= deadline {
                return Err(NativeV2CliError::Local(format!(
                    "eval sample run {run_id} did not settle within {} hours",
                    SETTLEMENT_TIMEOUT.as_secs() / 3600
                )));
            }
            let detached = {
                let detach = self.signal.wait();
                tokio::pin!(detach);
                tokio::select! {
                    biased;
                    () = detach.as_mut() => true,
                    () = tokio::time::sleep(SETTLEMENT_POLL) => false,
                }
            };
            if detached {
                self.interrupted = true;
                return Ok(None);
            }
        }
    }
}

/// One already-published sample directory, ready to submit.
struct SampleRun<'a> {
    directory: &'a Path,
    input: &'a Path,
    source: &'a config::ArmSource,
}

/// How one sample run settled.
struct SettledRun {
    run_id: String,
    status: SampleStatus,
    output: Option<Value>,
    reason: Option<String>,
}

/// Read a reported run status as a settled sample, if it has settled.
fn settled_from(status: &HostedRunStatusResult, run_id: &str) -> Option<SettledRun> {
    let HostedRunStatus::Target(status) = &status.status else {
        return None;
    };
    let (status, output, reason) = match status {
        RunStatus::Finished {
            terminal_result: TerminalResult::Succeeded { output },
            ..
        } => (SampleStatus::Succeeded, Some(output.clone()), None),
        RunStatus::Finished {
            terminal_result: TerminalResult::Failed { reason, .. },
            ..
        } => (SampleStatus::Failed, None, Some(reason.to_string())),
        RunStatus::Suspended { .. } => (
            SampleStatus::Failed,
            None,
            Some("the sample run suspended without an answer".to_owned()),
        ),
        _ => return None,
    };
    Some(SettledRun {
        run_id: run_id.to_owned(),
        status,
        output,
        reason,
    })
}

/// Build the title of one sample run.
fn title(document: &EvalDocument, arm: &str, index: u32) -> Result<RunTitle, NativeV2CliError> {
    bounded_title(format!("eval {} {arm} sample {index}", document.slug))
}

/// Build the title of one judge run.
fn judge_title(document: &EvalDocument, index: u32) -> Result<RunTitle, NativeV2CliError> {
    bounded_title(format!("eval {} judge sample {index}", document.slug))
}

fn bounded_title(mut text: String) -> Result<RunTitle, NativeV2CliError> {
    if text.chars().count() > TITLE_CHARACTERS {
        let end = text
            .char_indices()
            .nth(TITLE_CHARACTERS)
            .map_or(text.len(), |(index, _)| index);
        text.truncate(end);
    }
    RunTitle::new(text).map_err(|error| NativeV2CliError::Usage(error.to_string()))
}

/// Resolve the eval directory against the invoking directory, so every artifact path is absolute.
fn absolute(path: &Path) -> Result<PathBuf, NativeV2CliError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let cwd = std::env::current_dir().map_err(|source| NativeV2CliError::Read {
        kind: "eval",
        path: path.to_path_buf(),
        source,
    })?;
    Ok(cwd.join(path))
}

fn cli_error(error: crate::native_v2_eval::EvalError) -> NativeV2CliError {
    match &error {
        crate::native_v2_eval::EvalError::Io { .. }
        | crate::native_v2_eval::EvalError::Encode(_) => NativeV2CliError::Local(error.to_string()),
        _ => NativeV2CliError::Usage(error.to_string()),
    }
}
