//! The eval store commands and the `eval run` execution path.
//!
//! `list`, `show`, `create`, and `clean` only touch the eval directory, so they stay usable without
//! a target. `run` is the one eval command that submits work: every arm's samples go to the eval's
//! named target, what came back is judged by the eval's judge graph, and the artifacts described in
//! [`crate::native_v2_eval::run`] are published under a fresh stamp directory.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use openengine_cluster_protocol::{
    HostedRunStatus, HostedRunStatusResult, IdempotencyKey, RunId, RunStatus, RunStatusParams,
    RunTitle, TerminalResult,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

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
    RESULT_ARTIFACT, ResumeJournal, RunStage, SOURCE_ARTIFACT, SampleResult, SampleStatus,
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
    // A pass continues the newest incomplete stamp unless the caller asked for a fresh one, so
    // `eval run` after a crash picks up where the evidence stopped instead of starting over.
    let resumed = select_stamp(&store, &command, &document)?;
    if command.retry_failed && resumed.is_none() {
        return Err(NativeV2CliError::Usage(
            "--retry-failed only retries the failed stages of a resumed stamp; \
             pass --stamp NAME or omit --restart"
                .to_owned(),
        ));
    }
    let stamp = match &resumed {
        Some(stamp) => stamp.clone(),
        None => artifacts::create_stamp(&store.runs_root(&document.slug), SystemTime::now())
            .map_err(cli_error)?,
    };
    let stamp_name = stamp
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_owned();
    let pin = artifacts::stamp_pin(&document, &target, &sources).map_err(cli_error)?;
    let journal = match ResumeJournal::load(&stamp).map_err(cli_error)? {
        Some(journal) => {
            if let Some(difference) = journal.pin.difference(&pin) {
                return Err(NativeV2CliError::Usage(format!(
                    "eval {:?} cannot continue stamp {stamp_name:?}: {difference}; \
                     pass --restart to start a new stamp",
                    document.slug
                )));
            }
            journal
        }
        None => {
            if resumed.is_some()
                && artifacts::stamp_has_evidence(&stamp, &document).map_err(cli_error)?
            {
                return Err(NativeV2CliError::Usage(format!(
                    "eval {:?} cannot continue stamp {stamp_name:?}: it holds evidence but no \
                     resume record; pass --restart to start a new stamp",
                    document.slug
                )));
            }
            // The journal is the stamp's first artifact, so a pass that dies at any later step
            // still leaves a record of what it was measuring and which run it started.
            let journal = ResumeJournal::new(pin);
            journal.save(&stamp).map_err(cli_error)?;
            journal
        }
    };

    let mut runner = Runner {
        context,
        signal,
        output,
        document: &document,
        target: &target,
        stamp: &stamp,
        stamp_name: &stamp_name,
        judge,
        workspace: &workspace,
        input: document.input.clone(),
        sources,
        journal,
        retry_failed: command.retry_failed,
        interrupted: false,
    };
    runner.run().await?;
    let Runner { output, .. } = runner;

    // The aggregate is reduced from the stamp's evidence, never from what this pass happened to
    // submit, so a resumed pass reports every sample an earlier pass already settled.
    let (samples, complete) =
        artifacts::summary_from_evidence(&document, &stamp).map_err(cli_error)?;
    let mut summary = artifacts::summarize(&document, &stamp_name, &target, samples);
    summary.interrupted = !complete;
    artifacts::replace_text(
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
    if summary.interrupted {
        return Ok(CliOutcome::Detached);
    }
    Ok(CliOutcome::Completed)
}

/// Choose the stamp one pass writes into.
///
/// `--stamp NAME` continues exactly that stamp. Otherwise the newest stamp that still needs work
/// and carries a resume record is continued, and only an eval with no such stamp starts a fresh
/// one. `--restart` always starts a fresh stamp and never touches earlier evidence.
///
/// Only a stamp with a `resume.json` may be picked automatically: a stamp written before the
/// journal existed holds evidence but no record of what it was measuring, so it is left exactly as
/// it was found and a new stamp is started beside it.
fn select_stamp(
    store: &EvalStore,
    command: &EvalRunCommand,
    document: &EvalDocument,
) -> Result<Option<PathBuf>, NativeV2CliError> {
    let root = store.runs_root(&document.slug);
    if let Some(name) = command.stamp.as_deref() {
        let stamp = stamp_directory(&root, name)?;
        if !stamp.is_dir() {
            return Err(NativeV2CliError::Usage(format!(
                "eval {:?} has no stamp {name:?} under {}; omit --stamp to continue the newest \
                 incomplete stamp",
                document.slug,
                root.display()
            )));
        }
        return Ok(Some(stamp));
    }
    if command.restart {
        return Ok(None);
    }
    let mut names = match std::fs::read_dir(&root) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect::<Vec<_>>(),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(source) => {
            return Err(NativeV2CliError::Read {
                kind: "eval",
                path: root,
                source,
            });
        }
    };
    names.sort();
    for name in names.iter().rev() {
        let stamp = root.join(name);
        if ResumeJournal::load(&stamp).map_err(cli_error)?.is_none() {
            continue;
        }
        if artifacts::stamp_incomplete(&stamp, document).map_err(cli_error)? {
            return Ok(Some(stamp));
        }
    }
    Ok(None)
}

/// Resolve `--stamp NAME` as one stamp directory name, refusing anything that could escape it.
fn stamp_directory(root: &Path, name: &str) -> Result<PathBuf, NativeV2CliError> {
    let trimmed = name.trim();
    let mut components = Path::new(trimmed).components();
    let one_name =
        matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none();
    if trimmed.is_empty() || !one_name {
        return Err(NativeV2CliError::Usage(format!(
            "--stamp must name one stamp directory, not {name:?}"
        )));
    }
    Ok(root.join(trimmed))
}

/// One `eval run`, from the resolved document to the aggregate.
struct Runner<'a, 'c, B, S, W> {
    context: &'a CliExecutionContext<'c, B>,
    signal: &'a mut S,
    output: &'a mut W,
    document: &'a EvalDocument,
    target: &'a str,
    stamp: &'a Path,
    /// Name of `stamp`, which the aggregate and every submission key repeat.
    stamp_name: &'a str,
    judge: Option<config::JudgePlan>,
    /// The checkout the eval document is stored in, which supplies the sample source.
    workspace: &'a Path,
    input: Value,
    /// Each arm's source revision, resolved once for this run.
    sources: BTreeMap<String, config::ArmSource>,
    /// Which stage of each sample this stamp already submitted.
    journal: ResumeJournal,
    /// Whether a recorded failed result is retired and measured again.
    retry_failed: bool,
    /// Set once the caller asked to detach, so no later sample starts a new run.
    interrupted: bool,
}

/// One sample of one arm, with the artifacts directory it owns.
struct SampleTask<'a> {
    directory: PathBuf,
    arm: &'a EvalArm,
    index: u32,
    source: &'a config::ArmSource,
}

/// One stage's submission: what to submit, the title it is filed under, and whether the run the
/// journal already recorded may be adopted instead of submitted again.
struct StageSubmission<'a> {
    plan: SampleRun<'a>,
    title: RunTitle,
    adopt: bool,
}

impl<'a, 'c, B, S, W> Runner<'a, 'c, B, S, W>
where
    B: NativeV2CliBackend,
    S: DetachSignal,
    W: Write,
{
    /// Run every authored sample, stopping at the first detach request.
    async fn run(&mut self) -> Result<(), NativeV2CliError> {
        let document = self.document;
        for arm in &document.arms {
            let source = self.sources.get(&arm.id).cloned().ok_or_else(|| {
                NativeV2CliError::Local(format!(
                    "eval {:?} arm {:?} has no resolved source",
                    document.slug, arm.id
                ))
            })?;
            for index in 0..document.runs_per_arm {
                let task = SampleTask {
                    directory: artifacts::sample_directory(self.stamp, &arm.id, index),
                    arm,
                    index,
                    source: &source,
                };
                self.sample(&task).await?;
                if self.interrupted {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Settle one sample's own run, then judge it when it delivered output.
    async fn sample(&mut self, task: &SampleTask<'_>) -> Result<(), NativeV2CliError> {
        let Some(result) = self.sample_result(task).await? else {
            return Ok(());
        };
        self.judge_sample(task, &result).await
    }

    /// Publish one sample's artifacts and settle its run.
    ///
    /// `None` means the pass stopped before the sample settled, so there is nothing to judge.
    async fn sample_result(
        &mut self,
        task: &SampleTask<'_>,
    ) -> Result<Option<SampleResult>, NativeV2CliError> {
        let arm = task.arm;
        let path = task.directory.join(RESULT_ARTIFACT);
        let mut retry = false;
        let retired = self.journal.retired(&arm.id, task.index) == Some(RunStage::Sample);
        if let Some(recorded) = artifacts::read_result::<SampleResult>(&path).map_err(cli_error)? {
            if recorded.status != SampleStatus::Failed || (!self.retry_failed && !retired) {
                // Already measured: the recorded evidence is the answer, not a reason to resubmit.
                return Ok(Some(recorded));
            }
            if !retired {
                // Keep the failed evidence under its attempt, so a retry cannot destroy what it
                // repeats. Journal the retirement first: if this pass dies before the new run is
                // claimed, the next pass measures the stage again instead of adopting the run
                // whose evidence was intentionally discarded.
                self.retire_stage(&arm.id, task.index, RunStage::Sample)?;
            }
            // A judge verdict describes the output of the failed sample. Retrying the sample
            // invalidates that evidence before it can be mistaken for a verdict on the new run.
            self.retire_judge(task)?;
            artifacts::retire_result(
                &path,
                self.journal.attempt(&arm.id, task.index, RunStage::Sample),
            )
            .map_err(cli_error)?;
            retry = true;
        } else if retired {
            // An earlier retry retired the result but stopped before admitting its replacement.
            retry = true;
        }
        self.publish_sample(task)?;
        let input = task.directory.join(INPUT_ARTIFACT);
        let identity = StageIdentity {
            slug: &self.document.slug,
            stamp: self.stamp_name,
            arm: &arm.id,
            sample: task.index,
            stage: RunStage::Sample,
        };
        let submission = StageSubmission {
            plan: SampleRun {
                directory: &task.directory,
                input: &input,
                source: task.source,
            },
            title: title(self.document, &arm.id, task.index)?,
            adopt: !retry,
        };
        let result = match self.dispatch(&identity, &submission).await? {
            StageSettlement::Stopped => return Ok(None),
            StageSettlement::Vanished { run_id } => SampleResult {
                run_id,
                status: SampleStatus::Failed,
                output: None,
                reason: Some(artifacts::VANISHED_REASON.to_owned()),
            },
            StageSettlement::Settled(settled) => SampleResult {
                run_id: settled.run_id,
                status: settled.status,
                output: settled.output,
                reason: settled.reason,
            },
        };
        artifacts::write_json(&path, &result).map_err(cli_error)?;
        Ok(Some(result))
    }

    /// Retire a judge result that belonged to a sample whose run is being replaced.
    ///
    /// The journal only names the current stage, so a result left by an earlier judge attempt is
    /// removed here, before the sample result itself, and without needing a second retirement
    /// marker: an interruption at any point still leaves the sample marker or its failed result.
    fn retire_judge(&mut self, task: &SampleTask<'_>) -> Result<(), NativeV2CliError> {
        let directory = task.directory.join(JUDGE_DIRECTORY);
        // The input and the verdict both describe the failed sample's output. Keeping either for
        // the replacement run would either judge the wrong patch or make the refreshed input look
        // like conflicting evidence.
        let paths = [
            directory.join(JUDGE_INPUT_ARTIFACT),
            directory.join(JUDGE_RESULT_ARTIFACT),
        ];
        if paths.iter().all(|path| !path.is_file()) {
            return Ok(());
        }
        if self.journal.retired(&task.arm.id, task.index) != Some(RunStage::Judge) {
            self.retire_stage(&task.arm.id, task.index, RunStage::Judge)?;
        }
        let attempt = self
            .journal
            .attempt(&task.arm.id, task.index, RunStage::Judge);
        for path in paths.iter().filter(|path| path.is_file()) {
            artifacts::retire_result(path, attempt).map_err(cli_error)?;
        }
        Ok(())
    }

    /// Publish the artifacts that describe one sample's submission, once.
    fn publish_sample(&self, task: &SampleTask<'_>) -> Result<(), NativeV2CliError> {
        artifacts::write_json_checked(&task.directory.join(INPUT_ARTIFACT), &self.input)
            .map_err(cli_error)?;
        artifacts::write_json_checked(&task.directory.join(SOURCE_ARTIFACT), task.source)
            .map_err(cli_error)?;
        artifacts::write_json_checked(&task.directory.join(GRAPH_ARTIFACT), &task.arm.graph)
            .map_err(cli_error)?;
        artifacts::write_json_checked(&task.directory.join(RUNTIME_ARTIFACT), &task.arm.runtime)
            .map_err(cli_error)
    }

    /// Judge one sample's delivered output, when the eval declares a judge.
    async fn judge_sample(
        &mut self,
        task: &SampleTask<'_>,
        result: &SampleResult,
    ) -> Result<(), NativeV2CliError> {
        let arm = task.arm;
        let index = task.index;
        let Some(output) = result.output.as_ref() else {
            return Ok(());
        };
        if result.status != SampleStatus::Succeeded {
            return Ok(());
        }
        let Some((judge_graph, judge_runtime)) = self
            .judge
            .as_ref()
            .map(|judge| (judge.graph.clone(), judge.runtime.clone()))
        else {
            return Ok(());
        };
        let judge_directory = task.directory.join(JUDGE_DIRECTORY);
        let path = judge_directory.join(JUDGE_RESULT_ARTIFACT);
        let mut retry = false;
        let retired = self.journal.retired(&arm.id, index) == Some(RunStage::Judge);
        if let Some(recorded) = artifacts::read_result::<JudgeResult>(&path).map_err(cli_error)? {
            if recorded.status != SampleStatus::Failed || (!self.retry_failed && !retired) {
                return Ok(());
            }
            if !retired {
                // Like a sample retry, journal the retirement before removing its evidence.
                self.retire_stage(&arm.id, index, RunStage::Judge)?;
            }
            artifacts::retire_result(&path, self.journal.attempt(&arm.id, index, RunStage::Judge))
                .map_err(cli_error)?;
            retry = true;
        } else if retired {
            // An earlier retry retired the judge but stopped before admitting its replacement.
            retry = true;
        }
        let judge_input = judge_directory.join(JUDGE_INPUT_ARTIFACT);
        artifacts::write_json_checked(
            &judge_input,
            &artifacts::judge_input(self.document, &artifacts::sample_patch(output))
                .map_err(cli_error)?,
        )
        .map_err(cli_error)?;
        artifacts::write_json_checked(&judge_directory.join(GRAPH_ARTIFACT), &judge_graph)
            .map_err(cli_error)?;
        artifacts::write_json_checked(&judge_directory.join(RUNTIME_ARTIFACT), &judge_runtime)
            .map_err(cli_error)?;
        let identity = StageIdentity {
            slug: &self.document.slug,
            stamp: self.stamp_name,
            arm: &arm.id,
            sample: index,
            stage: RunStage::Judge,
        };
        let submission = StageSubmission {
            plan: SampleRun {
                directory: &judge_directory,
                input: &judge_input,
                source: task.source,
            },
            title: judge_title(self.document, index)?,
            adopt: !retry,
        };
        let mut judge_result = match self.dispatch(&identity, &submission).await? {
            StageSettlement::Stopped => return Ok(()),
            StageSettlement::Vanished { run_id } => JudgeResult {
                run_id,
                status: SampleStatus::Failed,
                verdicts: Vec::new(),
                reason: Some(artifacts::VANISHED_REASON.to_owned()),
            },
            StageSettlement::Settled(settled) => {
                let mut judge_result = JudgeResult {
                    run_id: settled.run_id,
                    status: settled.status,
                    verdicts: Vec::new(),
                    reason: settled.reason,
                };
                if let Some(output) = settled.output {
                    judge_result.verdicts =
                        artifacts::verdicts(&output, &self.document.scoring.criteria)
                            .map_err(cli_error)?;
                }
                judge_result
            }
        };
        artifacts::normalize_judge(&mut judge_result);
        artifacts::write_json(&path, &judge_result).map_err(cli_error)
    }

    /// Settle one stage: adopt the run the journal recorded, or submit a new one.
    ///
    /// An adopted run is polled with the same settlement the submitting pass used, so a resumed
    /// pass never starts a second run for work an earlier pass already admitted. `adopt` is false
    /// when `--retry-failed` just retired the stage's recorded failure, which must be measured
    /// again rather than polled to the same answer.
    async fn dispatch(
        &mut self,
        identity: &StageIdentity<'_>,
        submission: &StageSubmission<'_>,
    ) -> Result<StageSettlement, NativeV2CliError> {
        let recorded = self
            .journal
            .recorded(identity.arm, identity.sample)
            .filter(|(stage, _)| submission.adopt && *stage == identity.stage)
            .map(|(_, run_id)| run_id.to_owned());
        let run_id = match recorded {
            Some(run_id) => run_id,
            None => match self.submit(identity, submission).await? {
                SubmittedRun::Stopped => return Ok(StageSettlement::Stopped),
                SubmittedRun::Submitted(run_id) => run_id,
            },
        };
        match self.settle(&run_id, identity).await? {
            Some(settled) => Ok(StageSettlement::Settled(settled)),
            None if self.interrupted => Ok(StageSettlement::Stopped),
            None => Ok(StageSettlement::Vanished { run_id }),
        }
    }

    /// Journal that a failed stage is retired, before its result is removed.
    fn retire_stage(
        &mut self,
        arm: &str,
        sample: u32,
        stage: RunStage,
    ) -> Result<(), NativeV2CliError> {
        self.journal.retire(arm, sample, stage);
        self.journal.save(self.stamp).map_err(cli_error)
    }

    /// Journal one submitted run, durably, before it is polled.
    fn claim(
        &mut self,
        identity: &StageIdentity<'_>,
        run_id: &str,
    ) -> Result<(), NativeV2CliError> {
        self.journal.claim(
            artifacts::StageId {
                arm: identity.arm,
                sample: identity.sample,
                stage: identity.stage,
            },
            run_id,
            SystemTime::now(),
        );
        self.journal.save(self.stamp).map_err(cli_error)
    }

    /// Journal one settled stage, so a resumed pass knows it owes no more work here.
    fn settle_stage(&mut self, identity: &StageIdentity<'_>) -> Result<(), NativeV2CliError> {
        self.journal
            .settle(identity.arm, identity.sample, SystemTime::now());
        self.journal.save(self.stamp).map_err(cli_error)
    }

    /// Journal one stage whose run vanished, so its failure is durable evidence.
    fn fail_stage(&mut self, identity: &StageIdentity<'_>) -> Result<(), NativeV2CliError> {
        self.journal
            .fail(identity.arm, identity.sample, SystemTime::now());
        self.journal.save(self.stamp).map_err(cli_error)
    }

    /// Submit one run, recording its receipt in the journal before it is polled.
    ///
    /// A receipt is journaled even when the caller detached while submitting, because the target
    /// may already have admitted the run: a later pass then continues it instead of starting a
    /// second, unreferenced run for the same sample.
    async fn submit(
        &mut self,
        identity: &StageIdentity<'_>,
        submission: &StageSubmission<'_>,
    ) -> Result<SubmittedRun, NativeV2CliError> {
        let plan = &submission.plan;
        let title = submission.title.clone();
        let attempt = self
            .journal
            .attempt(identity.arm, identity.sample, identity.stage)
            .saturating_add(1);
        let command = RunCommand {
            controller: crate::native_v2_cli::local::LocalControllerMode::Detached,
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
            submission_key: Some(submission_key(identity, attempt)),
            forward_github_token: false,
            workspace: None,
        };
        let submission = {
            let detach = self.signal.wait();
            tokio::pin!(detach);
            // The submission's own record belongs to the sample's artifacts, so the run's stdout
            // stays the one aggregate a caller parses.
            submit_run(&command, self.context, detach.as_mut(), &mut Vec::new()).await?
        };
        let (run_id, interrupted) = match submission {
            RunSubmission::Submitted {
                receipt,
                interrupted,
            } => {
                let run_id = receipt.run_id.as_str().to_owned();
                self.claim(identity, &run_id)?;
                (run_id, interrupted)
            }
            RunSubmission::Interrupted => {
                self.interrupted = true;
                return Ok(SubmittedRun::Stopped);
            }
            RunSubmission::Validated => {
                return Err(NativeV2CliError::Usage(
                    "eval samples are never validated without being submitted".to_owned(),
                ));
            }
        };
        if interrupted {
            // The detach signal already fired; the journaled receipt is what lets the next pass
            // continue this run.
            self.interrupted = true;
            return Ok(SubmittedRun::Stopped);
        }
        Ok(SubmittedRun::Submitted(run_id))
    }

    /// Poll one submitted run until it has a terminal record.
    ///
    /// `None` means the pass stopped or the target no longer has the run; both are journaled, so a
    /// resumed pass never polls a run that can no longer settle.
    async fn settle(
        &mut self,
        run_id: &str,
        identity: &StageIdentity<'_>,
    ) -> Result<Option<SettledRun>, NativeV2CliError> {
        let context = self.context;
        let target = self.target;
        let deadline = Instant::now() + SETTLEMENT_TIMEOUT;
        let params = RunStatusParams::new(RunId::new(run_id));
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
                        Some(status)
                    }
                }
            };
            let Some(reported) = reported else {
                self.interrupted = true;
                return Ok(None);
            };
            let status = match reported {
                Ok(status) => status,
                Err(error) if vanished(&error) => {
                    self.fail_stage(identity)?;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            if let Some(settled) = settled_from(&status, run_id) {
                self.settle_stage(identity)?;
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

/// Which sample stage a run belongs to, and where its submission key comes from.
struct StageIdentity<'a> {
    slug: &'a str,
    stamp: &'a str,
    arm: &'a str,
    sample: u32,
    stage: RunStage,
}

/// What one stage dispatch concluded.
enum StageSettlement {
    /// The pass stopped before the stage settled.
    Stopped,
    /// The recorded run no longer exists on the target.
    Vanished { run_id: String },
    /// The run reached a terminal record.
    Settled(SettledRun),
}

/// What one stage submission produced.
enum SubmittedRun {
    /// The pass stopped while submitting.
    Stopped,
    /// The target admitted this run.
    Submitted(String),
}

/// Whether an error says the target no longer has a recorded run.
///
/// Only an explicit not-found answer counts: a transport failure is not evidence that a run is
/// gone, so it keeps its own error handling instead of failing the sample.
fn vanished(error: &NativeV2CliError) -> bool {
    match error {
        NativeV2CliError::RunNotFound { .. } => true,
        NativeV2CliError::Remote { code, .. } => code == "run_not_found" || code == "not_found",
        _ => false,
    }
}

/// The deterministic resubmission key of one stage attempt.
///
/// Derived from the stamp, the sample and the attempt rather than minted once, so re-running the
/// same pass replays the same admission instead of asking the target to admit a second, identical
/// run. A deliberate retry after a durable receipt claims the next attempt, so its key differs and
/// the target cannot replay the run the retry retired.
fn submission_key(identity: &StageIdentity<'_>, attempt: u32) -> IdempotencyKey {
    let stage = match identity.stage {
        RunStage::Sample => "sample",
        RunStage::Judge => "judge",
    };
    let digest = Sha256::digest(
        format!(
            "eval/{}/{}/{}/{}/{stage}/{attempt}",
            identity.slug, identity.stamp, identity.arm, identity.sample
        )
        .as_bytes(),
    );
    IdempotencyKey::new(format!("eval-{digest:x}"))
        .expect("a sha256 hex digest fits an idempotency key")
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
