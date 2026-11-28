//! Artifacts and aggregation for one `eval run`.
//!
//! A run publishes one directory per sample beneath a fresh stamp directory:
//!
//! ```text
//! runs/<slug>/<stamp>/resume.json
//! runs/<slug>/<stamp>/<arm>-<sample>/input.json
//! runs/<slug>/<stamp>/<arm>-<sample>/source.json
//! runs/<slug>/<stamp>/<arm>-<sample>/result.json
//! runs/<slug>/<stamp>/<arm>-<sample>/judge/input.json
//! runs/<slug>/<stamp>/<arm>-<sample>/judge/result.json
//! runs/<slug>/<stamp>/graph.json
//! runs/<slug>/<stamp>/runtime.json
//! runs/<slug>/<stamp>/report.md
//! runs/<slug>/summary.json
//! ```
//!
//! Sample evidence is written once: a new pass gets its own stamp directory, so earlier evidence is
//! never replaced. Three documents are derived and therefore replaced atomically as the pass
//! advances. `report.md` and `summary.json` are regenerated from the samples' evidence, and
//! `resume.json` is the one mutable record of which stage of each sample was already submitted, so a
//! pass that dies between a receipt and its evidence continues at that run instead of submitting it
//! again.
//!
//! The types here are the artifact schema; [`crate::native_v2_cli::execution::eval`] owns the
//! submission, polling, and file publication that fills them.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::config::ArmSource;
use super::{EvalCriterion, EvalDocument, EvalError};

/// File name of the input a sample was submitted with.
pub(crate) const INPUT_ARTIFACT: &str = "input.json";
/// File name of the exact source a sample was pinned to.
pub(crate) const SOURCE_ARTIFACT: &str = "source.json";
/// File name of the bounded terminal status a sample run settled at.
pub(crate) const RESULT_ARTIFACT: &str = "result.json";
/// Directory holding the judge's own input and result.
pub(crate) const JUDGE_DIRECTORY: &str = "judge";
/// File name of the patch and traits handed to the judge.
pub(crate) const JUDGE_INPUT_ARTIFACT: &str = "input.json";
/// File name of the judge's verdicts.
pub(crate) const JUDGE_RESULT_ARTIFACT: &str = "result.json";
/// File name of the expanded sample graph, kept beside the evidence for replay.
pub(crate) const GRAPH_ARTIFACT: &str = "graph.json";
/// File name of the expanded sample runtime, kept beside the evidence for replay.
pub(crate) const RUNTIME_ARTIFACT: &str = "runtime.json";
/// File name of the rendered human-readable report.
pub(crate) const REPORT_ARTIFACT: &str = "report.md";
/// File name of the aggregate the store keeps for the newest run.
pub(crate) const SUMMARY_ARTIFACT: &str = "summary.json";
/// File name of the journal that makes a stamp resumable.
pub(crate) const RESUME_ARTIFACT: &str = "resume.json";

/// Why a stage is recorded as failed when the target no longer has its run.
pub(crate) const VANISHED_REASON: &str = "the recorded run no longer exists on the target";

/// Longest patch a judge is given. A larger delivery is truncated and marked, never silently cut.
pub(crate) const MAX_PATCH_BYTES: usize = 256 * 1024;

const TRUNCATED_MARKER: &str = "\n… [patch truncated]\n";

/// One trait's verdict, as the judge graph returns it.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Verdict {
    pub id: String,
    pub pass: bool,
    pub reason: String,
}

/// The complete input of one judge run.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JudgeInput {
    /// The task the judged sample was asked to do.
    pub task: String,
    /// The eval's criteria as the judge graph's `traits` string.
    pub traits: String,
    /// The delivered patch, or the sample's recorded output when it delivered no patch field.
    pub patch: String,
}

/// How one sample run settled.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleStatus {
    Succeeded,
    Failed,
}

/// The recorded result of one sample run.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SampleResult {
    pub run_id: String,
    pub status: SampleStatus,
    /// Recorded terminal output; absent when the run failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Terminal failure reason; absent when the run succeeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// The recorded result of one judge run.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JudgeResult {
    pub run_id: String,
    pub status: SampleStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verdicts: Vec<Verdict>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// One sample's contribution to the aggregate.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SampleSummary {
    pub arm: String,
    pub sample: u32,
    pub run_id: String,
    pub source: ArmSource,
    pub status: SampleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub verdicts: Vec<Verdict>,
}

/// Per-arm totals computed from the recorded verdicts.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArmSummary {
    pub id: String,
    pub samples: u32,
    pub succeeded: u32,
    pub failed: u32,
    pub verdicts: u32,
    pub passed: u32,
    /// Weighted pass rate over every declared criterion; absent when no verdict was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// The aggregate one eval run publishes as `summary.json`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunSummary {
    pub slug: String,
    pub stamp: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<String>,
    pub arms: Vec<ArmSummary>,
    pub samples: Vec<SampleSummary>,
    /// Whether the run stopped early, so it covers fewer samples than the eval's `runsPerArm`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub interrupted: bool,
}

/// The directory that holds one sample's artifacts.
pub(crate) fn sample_directory(stamp: &Path, arm: &str, sample: u32) -> PathBuf {
    stamp.join(format!("{arm}-{sample}"))
}

/// Create a fresh stamp directory beneath `runs_root`.
///
/// The name is the current time in milliseconds, which sorts by run, and a suffix only when that
/// name is already taken. Creation is exclusive, so two runs can never share a stamp and overwrite
/// each other's write-once artifacts.
pub(crate) fn create_stamp(runs_root: &Path, now: SystemTime) -> Result<PathBuf, EvalError> {
    fs::create_dir_all(runs_root).map_err(|source| EvalError::Io {
        path: runs_root.to_path_buf(),
        source,
    })?;
    let millis = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    let mut problem = None;
    for attempt in 0..64u32 {
        let name = if attempt == 0 {
            format!("{millis:013}")
        } else {
            format!("{millis:013}-{attempt}")
        };
        let path = runs_root.join(name.clone());
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                problem = Some(source);
            }
            Err(source) => {
                return Err(EvalError::Io { path, source });
            }
        }
    }
    Err(EvalError::Io {
        path: runs_root.to_path_buf(),
        source: problem.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "stamp directory is unavailable",
            )
        }),
    })
}

/// Whether `stamp` already holds evidence, so it cannot be adopted as an empty stamp.
pub(crate) fn stamp_has_evidence(stamp: &Path, document: &EvalDocument) -> Result<bool, EvalError> {
    if stamp.join(REPORT_ARTIFACT).is_file() {
        return Ok(true);
    }
    for arm in &document.arms {
        for sample in 0..document.runs_per_arm {
            let directory = sample_directory(stamp, &arm.id, sample);
            if directory.join(RESULT_ARTIFACT).is_file()
                || directory
                    .join(JUDGE_DIRECTORY)
                    .join(JUDGE_RESULT_ARTIFACT)
                    .is_file()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// One stage of a sample, which is what the journal records progress for.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunStage {
    /// The sample's own run.
    Sample,
    /// The judge run of a sample that delivered output.
    Judge,
}

/// The stage of one sample, which is what the journal records progress for.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StageId<'a> {
    pub arm: &'a str,
    pub sample: u32,
    pub stage: RunStage,
}

/// The run one sample stage was last submitted as.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ResumeSample {
    pub arm: String,
    pub sample: u32,
    pub stage: RunStage,
    /// Submission attempt of [`Self::stage`], starting at one.
    pub attempt: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_at: Option<u64>,
    /// Whether the recorded stage can never settle because its run vanished.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub failed: bool,
}

/// The facts a stamp was created with, so a resumed pass cannot measure something else.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct StampPin {
    pub document_sha256: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<String>,
    pub runs_per_arm: u32,
    pub arms: Vec<StampArm>,
}

/// One arm's identity and exact revision, as the stamp was pinned to it.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct StampArm {
    pub id: String,
    pub revision: String,
}

impl StampPin {
    /// Describe the first fact that no longer matches, or `None` when the stamp still applies.
    pub(crate) fn difference(&self, current: &Self) -> Option<String> {
        for arm in &self.arms {
            match current.arms.iter().find(|candidate| candidate.id == arm.id) {
                Some(candidate) if candidate.revision != arm.revision => {
                    return Some(format!(
                        "arm {:?} moved from {} to {}",
                        arm.id, arm.revision, candidate.revision
                    ));
                }
                Some(_) => {}
                None => return Some(format!("arm {:?} was removed", arm.id)),
            }
        }
        for candidate in &current.arms {
            if !self.arms.iter().any(|arm| arm.id == candidate.id) {
                return Some(format!("arm {:?} was added", candidate.id));
            }
        }
        if self.target != current.target {
            return Some(format!(
                "target moved from {:?} to {:?}",
                self.target, current.target
            ));
        }
        if self.judge != current.judge {
            return Some(format!(
                "judge moved from {:?} to {:?}",
                self.judge, current.judge
            ));
        }
        if self.runs_per_arm != current.runs_per_arm {
            return Some(format!(
                "runsPerArm moved from {} to {}",
                self.runs_per_arm, current.runs_per_arm
            ));
        }
        if self.document_sha256 != current.document_sha256 {
            return Some(format!(
                "the eval document changed from {} to {}",
                self.document_sha256, current.document_sha256
            ));
        }
        None
    }
}

/// The mutable record of which sample stages a stamp already submitted.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ResumeJournal {
    pub pin: StampPin,
    #[serde(default)]
    pub samples: Vec<ResumeSample>,
}

impl ResumeJournal {
    pub(crate) fn new(pin: StampPin) -> Self {
        Self {
            pin,
            samples: Vec::new(),
        }
    }

    pub(crate) fn path(stamp: &Path) -> PathBuf {
        stamp.join(RESUME_ARTIFACT)
    }

    /// Read one stamp's journal; `None` when the stamp has none yet.
    pub(crate) fn load(stamp: &Path) -> Result<Option<Self>, EvalError> {
        read_result(&Self::path(stamp))
    }

    /// Replace the journal atomically, so a dying pass leaves either record readable.
    pub(crate) fn save(&self, stamp: &Path) -> Result<(), EvalError> {
        replace_json(&Self::path(stamp), self)
    }

    fn entry(&self, arm: &str, sample: u32) -> Option<&ResumeSample> {
        self.samples
            .iter()
            .find(|entry| entry.arm == arm && entry.sample == sample)
    }

    fn entry_mut(&mut self, arm: &str, sample: u32) -> Option<&mut ResumeSample> {
        self.samples
            .iter_mut()
            .find(|entry| entry.arm == arm && entry.sample == sample)
    }

    /// The stage whose run this stamp already recorded for one sample.
    pub(crate) fn recorded(&self, arm: &str, sample: u32) -> Option<(RunStage, &str)> {
        self.entry(arm, sample)
            .map(|entry| (entry.stage, entry.run_id.as_str()))
    }

    /// How many times one stage of a sample was submitted; zero when it never was.
    pub(crate) fn attempt(&self, arm: &str, sample: u32, stage: RunStage) -> u32 {
        self.entry(arm, sample)
            .filter(|entry| entry.stage == stage)
            .map_or(0, |entry| entry.attempt)
    }

    /// Record that one stage was submitted, returning the attempt it was submitted as.
    pub(crate) fn claim(&mut self, stage: StageId<'_>, run_id: &str, at: SystemTime) -> u32 {
        let arm = stage.arm;
        let sample = stage.sample;
        let attempt = self.attempt(arm, sample, stage.stage).saturating_add(1);
        let entry = ResumeSample {
            arm: arm.to_owned(),
            sample,
            stage: stage.stage,
            attempt,
            run_id: run_id.to_owned(),
            submitted_at: Some(millis(at)),
            settled_at: None,
            failed: false,
        };
        match self.entry_mut(arm, sample) {
            Some(recorded) => *recorded = entry,
            None => self.samples.push(entry),
        }
        attempt
    }

    /// Record that the sample's current stage settled.
    pub(crate) fn settle(&mut self, arm: &str, sample: u32, at: SystemTime) {
        if let Some(entry) = self.entry_mut(arm, sample) {
            entry.settled_at = Some(millis(at));
            entry.failed = false;
        }
    }

    /// Record that the sample's current stage can never settle because its run vanished.
    pub(crate) fn fail(&mut self, arm: &str, sample: u32, at: SystemTime) {
        if let Some(entry) = self.entry_mut(arm, sample) {
            entry.settled_at = Some(millis(at));
            entry.failed = true;
        }
    }
}

fn millis(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |elapsed| {
        u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
    })
}

/// Pin the facts of this pass, so a later pass can prove it continues the same measurement.
pub(crate) fn stamp_pin(
    document: &EvalDocument,
    target: &str,
    sources: &std::collections::BTreeMap<String, ArmSource>,
) -> Result<StampPin, EvalError> {
    let arms = document
        .arms
        .iter()
        .map(|arm| {
            let source = sources.get(&arm.id).ok_or_else(|| {
                EvalError::Invalid(format!(
                    "eval {:?} arm {:?} has no resolved source",
                    document.slug, arm.id
                ))
            })?;
            let revision = source.revision.as_ref().ok_or_else(|| {
                EvalError::Invalid(format!(
                    "eval {:?} arm {:?} is not pinned to a revision",
                    document.slug, arm.id
                ))
            })?;
            Ok(StampArm {
                id: arm.id.clone(),
                revision: revision.as_str().to_owned(),
            })
        })
        .collect::<Result<Vec<_>, EvalError>>()?;
    let document_sha256 = document_digest(document)?;
    Ok(StampPin {
        document_sha256,
        target: target.to_owned(),
        judge: document.judge.as_ref().map(|judge| judge.template.clone()),
        runs_per_arm: document.runs_per_arm,
        arms,
    })
}

/// Digest the exact stored document, independent of its file's formatting.
pub(crate) fn document_digest(document: &EvalDocument) -> Result<String, EvalError> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(document)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

/// One sample's recorded evidence, as read back from its directory.
pub(crate) struct SampleEvidence {
    pub source: ArmSource,
    pub result: SampleResult,
    pub judge: Option<JudgeResult>,
}

/// Whether a sample still owes a judge result before its evidence is terminal.
fn owes_judge(document: &EvalDocument, result: &SampleResult) -> bool {
    document.judge.is_some() && result.status == SampleStatus::Succeeded && result.output.is_some()
}

/// Read one sample directory's evidence; `None` while the sample has no recorded result.
pub(crate) fn read_evidence(
    directory: &Path,
    document: &EvalDocument,
) -> Result<Option<SampleEvidence>, EvalError> {
    let Some(result) = read_result::<SampleResult>(&directory.join(RESULT_ARTIFACT))? else {
        return Ok(None);
    };
    let source = read_result::<ArmSource>(&directory.join(SOURCE_ARTIFACT))?.ok_or_else(|| {
        EvalError::Invalid(format!(
            "{} has a result but no recorded source",
            directory.display()
        ))
    })?;
    let judge = if owes_judge(document, &result) {
        read_result::<JudgeResult>(&directory.join(JUDGE_DIRECTORY).join(JUDGE_RESULT_ARTIFACT))?
    } else {
        None
    };
    Ok(Some(SampleEvidence {
        source,
        result,
        judge,
    }))
}

fn terminal(document: &EvalDocument, evidence: &SampleEvidence) -> bool {
    !owes_judge(document, &evidence.result) || evidence.judge.is_some()
}

/// Whether a stamp still needs work, so a later pass should continue it rather than start anew.
pub(crate) fn stamp_incomplete(stamp: &Path, document: &EvalDocument) -> Result<bool, EvalError> {
    if !stamp.join(REPORT_ARTIFACT).is_file() {
        return Ok(true);
    }
    for arm in &document.arms {
        for sample in 0..document.runs_per_arm {
            let directory = sample_directory(stamp, &arm.id, sample);
            match read_evidence(&directory, document)? {
                Some(evidence) if terminal(document, &evidence) => {}
                _ => return Ok(true),
            }
        }
    }
    Ok(false)
}

/// Read every declared sample's evidence, reducing each exactly once in document order.
///
/// The returned flag is true when every declared sample reached terminal evidence, so a resumed
/// pass that fills the last gap reports a complete aggregate.
pub(crate) fn summary_from_evidence(
    document: &EvalDocument,
    stamp: &Path,
) -> Result<(Vec<SampleSummary>, bool), EvalError> {
    let mut samples = Vec::new();
    let mut complete = true;
    for arm in &document.arms {
        for sample in 0..document.runs_per_arm {
            let directory = sample_directory(stamp, &arm.id, sample);
            let Some(evidence) = read_evidence(&directory, document)? else {
                complete = false;
                continue;
            };
            complete &= terminal(document, &evidence);
            samples.push(sample_summary(&arm.id, sample, evidence));
        }
    }
    Ok((samples, complete))
}

/// Reduce one sample's evidence to its aggregate entry.
///
/// This is the one reduction both a live pass and a regenerated aggregate use, so a resumed stamp
/// reports the same numbers as the pass that recorded it.
pub(crate) fn sample_summary(arm: &str, sample: u32, evidence: SampleEvidence) -> SampleSummary {
    let SampleEvidence {
        source,
        result,
        judge,
    } = evidence;
    let mut status = result.status;
    let mut reason = result.reason.clone();
    let mut verdicts = Vec::new();
    if let Some(mut judge) = judge {
        normalize_judge(&mut judge);
        verdicts = judge.verdicts;
        if judge.status == SampleStatus::Failed {
            // The judge is part of the measurement: a failed judge is a failed sample, not a
            // success that merely lacks verdicts.
            status = SampleStatus::Failed;
            reason = Some(format!(
                "judge run {} failed: {}",
                judge.run_id,
                judge.reason.as_deref().unwrap_or("it recorded no verdicts")
            ));
        }
    }
    SampleSummary {
        arm: arm.to_owned(),
        sample,
        run_id: result.run_id,
        source,
        status,
        reason,
        verdicts,
    }
}

/// Fail a judge that reported no verdict, so recording it cannot report an unscored sample.
pub(crate) fn normalize_judge(judge: &mut JudgeResult) {
    if judge.status == SampleStatus::Succeeded && judge.verdicts.is_empty() {
        judge.status = SampleStatus::Failed;
        judge.reason = Some("it recorded no verdicts".to_owned());
    }
}

/// Read one JSON artifact; `None` when it was never published.
pub(crate) fn read_result<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, EvalError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(EvalError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|error| {
        EvalError::Invalid(format!(
            "{} is not valid eval evidence: {error}",
            path.display()
        ))
    })
}

/// Publish one JSON artifact once, or prove the existing bytes are what this pass would write.
///
/// A resumed pass reuses the evidence an earlier pass published, so a stamp directory always
/// describes exactly one submission: the same input, source, graph and runtime bytes, or a named
/// refusal instead of two contradictory records.
pub(crate) fn write_json_checked(
    destination: &Path,
    value: &impl Serialize,
) -> Result<(), EvalError> {
    let bytes = json_bytes(value)?;
    match fs::read(destination) {
        Ok(existing) if existing == bytes => Ok(()),
        Ok(_) => Err(EvalError::Invalid(format!(
            "{} does not match the evidence this run would write",
            destination.display()
        ))),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            super::write_once(destination, &bytes)
        }
        Err(source) => Err(EvalError::Io {
            path: destination.to_path_buf(),
            source,
        }),
    }
}

/// Replace one derived text artifact atomically.
pub(crate) fn replace_text(destination: &Path, text: &str) -> Result<(), EvalError> {
    super::write_replacing(destination, text.as_bytes())
}

/// Keep a failed stage result under its attempt, then remove it from the canonical path.
///
/// The copy is published before the canonical file is removed, so an interruption leaves both
/// readable and retiring the same evidence again simply replaces the copy.
pub(crate) fn retire_result(result: &Path, attempt: u32) -> Result<(), EvalError> {
    let destination = result.with_file_name(format!("result.failed-{attempt}.json"));
    let bytes = fs::read(result).map_err(|source| EvalError::Io {
        path: result.to_path_buf(),
        source,
    })?;
    super::write_replacing(&destination, &bytes)?;
    fs::remove_file(result).map_err(|source| EvalError::Io {
        path: result.to_path_buf(),
        source,
    })
}

/// Read the eval's task from its initial input.
pub(crate) fn task(document: &EvalDocument) -> Result<String, EvalError> {
    document
        .input
        .get("task")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            EvalError::Invalid(format!(
                "eval {:?} requires a string task in its input",
                document.slug
            ))
        })
}

/// Build the judge input for one sample.
///
/// The criteria are the traits the judge decides: each criterion is one acceptance trait, sent to
/// the judge as the JSON string its graph contract declares.
pub(crate) fn judge_input(document: &EvalDocument, patch: &str) -> Result<JudgeInput, EvalError> {
    let criteria = &document.scoring.criteria;
    if criteria.is_empty() {
        return Err(EvalError::Invalid(format!(
            "eval {:?} asks for a judge but declares no scoring criteria to judge",
            document.slug
        )));
    }
    let traits = criteria
        .iter()
        .map(|criterion| {
            serde_json::json!({
                "id": criterion.id,
                "description": criterion.description.as_deref().unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    Ok(JudgeInput {
        task: task(document)?,
        traits: serde_json::to_string(&traits)?,
        patch: truncate_patch(patch),
    })
}

/// Bound one patch so a single artifact stays readable.
fn truncate_patch(patch: &str) -> String {
    if patch.len() <= MAX_PATCH_BYTES {
        return patch.to_owned();
    }
    let mut end = MAX_PATCH_BYTES;
    while !patch.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = patch[..end].to_owned();
    truncated.push_str(TRUNCATED_MARKER);
    truncated
}

/// The patch one sample delivered: its terminal `patch` field, or its whole recorded output.
pub(crate) fn sample_patch(output: &Value) -> String {
    match output.get("patch").and_then(Value::as_str) {
        Some(patch) => patch.to_owned(),
        None => serde_json::to_string_pretty(output).unwrap_or_else(|_| output.to_string()),
    }
}

/// Parse the verdicts a judge run returned, failing on a malformed or unaddressable judge output.
///
/// A verdict must name one of the criteria the eval declared. An unknown id is a judge fault, not
/// a free pass: silently scoring it at the default weight would let a judge invent its own rubric.
/// Repeated ids are collapsed to their first verdict so one criterion can score at most once.
pub(crate) fn verdicts(
    output: &Value,
    criteria: &[EvalCriterion],
) -> Result<Vec<Verdict>, EvalError> {
    let verdicts = output.get("verdicts").cloned().ok_or_else(|| {
        EvalError::Invalid("the judge returned no verdicts in its terminal output".to_owned())
    })?;
    let parsed: Vec<Verdict> = serde_json::from_value(verdicts).map_err(|error| {
        EvalError::Invalid(format!("the judge returned malformed verdicts: {error}"))
    })?;
    let declared = criteria
        .iter()
        .map(|criterion| criterion.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut unique = Vec::with_capacity(parsed.len());
    for verdict in parsed {
        if !declared.contains(verdict.id.as_str()) {
            return Err(EvalError::Invalid(format!(
                "the judge returned a verdict for unknown criterion {:?}",
                verdict.id
            )));
        }
        if seen.insert(verdict.id.clone()) {
            unique.push(verdict);
        }
    }
    Ok(unique)
}

/// Aggregate every recorded sample into the summary the run publishes.
pub(crate) fn summarize(
    document: &EvalDocument,
    stamp: &str,
    target: &str,
    samples: Vec<SampleSummary>,
) -> RunSummary {
    let arms = document
        .arms
        .iter()
        .map(|arm| {
            let recorded = samples
                .iter()
                .filter(|sample| sample.arm == arm.id)
                .collect::<Vec<_>>();
            let succeeded = recorded
                .iter()
                .filter(|sample| sample.status == SampleStatus::Succeeded)
                .count();
            let verdicts = recorded
                .iter()
                .flat_map(|sample| sample.verdicts.iter())
                .collect::<Vec<_>>();
            let passed = verdicts.iter().filter(|verdict| verdict.pass).count();
            ArmSummary {
                id: arm.id.clone(),
                samples: u32::try_from(recorded.len()).unwrap_or(u32::MAX),
                succeeded: u32::try_from(succeeded).unwrap_or(u32::MAX),
                failed: u32::try_from(recorded.len().saturating_sub(succeeded)).unwrap_or(u32::MAX),
                verdicts: u32::try_from(verdicts.len()).unwrap_or(u32::MAX),
                passed: u32::try_from(passed).unwrap_or(u32::MAX),
                score: weighted_score(&document.scoring.criteria, &verdicts),
            }
        })
        .collect();
    RunSummary {
        slug: document.slug.clone(),
        stamp: stamp.to_owned(),
        target: target.to_owned(),
        judge: document.judge.as_ref().map(|judge| judge.template.clone()),
        arms,
        samples,
        interrupted: false,
    }
}

/// Weighted pass rate over one arm's recorded verdicts, using the eval's declared criteria.
///
/// Every declared criterion is in the denominator: a judge that omits one, or names one twice,
/// cannot raise the score. Only the first verdict for an id contributes, and a missing verdict
/// counts as not passed.
pub(crate) fn weighted_score(criteria: &[EvalCriterion], verdicts: &[&Verdict]) -> Option<f64> {
    if criteria.is_empty() || verdicts.is_empty() {
        return None;
    }
    let mut total = 0.0;
    let mut passed = 0.0;
    for criterion in criteria {
        total += criterion.weight;
        if verdicts
            .iter()
            .find(|verdict| verdict.id == criterion.id)
            .is_some_and(|verdict| verdict.pass)
        {
            passed += criterion.weight;
        }
    }
    (total > 0.0).then(|| passed / total)
}

/// Render the human-readable report for one aggregate.
pub(crate) fn report(summary: &RunSummary) -> String {
    use std::fmt::Write as _;

    let mut report = String::new();
    let _ = writeln!(report, "# Eval report: {}", summary.slug);
    let _ = writeln!(report);
    let _ = writeln!(report, "- Target: `{}`", summary.target);
    let _ = writeln!(report, "- Run: `{}`", summary.stamp);
    if let Some(judge) = &summary.judge {
        let _ = writeln!(report, "- Judge: `{judge}`");
    }
    let _ = writeln!(report);
    let _ = writeln!(
        report,
        "| Arm | Samples | Succeeded | Failed | Passed | Verdicts | Score |"
    );
    let _ = writeln!(report, "| --- | --- | --- | --- | --- | --- | --- |");
    for arm in &summary.arms {
        let _ = writeln!(
            report,
            "| {} | {} | {} | {} | {} | {} | {} |",
            arm.id,
            arm.samples,
            arm.succeeded,
            arm.failed,
            arm.passed,
            arm.verdicts,
            arm.score
                .map_or_else(|| "n/a".to_owned(), |score| format!("{:.2}", score))
        );
    }
    report.push('\n');
    let _ = writeln!(report, "## Verdicts");
    report.push('\n');
    let _ = writeln!(report, "| Arm | Sample | Trait | Verdict | Reason |");
    let _ = writeln!(report, "| --- | --- | --- | --- | --- |");
    for sample in &summary.samples {
        if sample.verdicts.is_empty() {
            // Verdicts are absent for two different reasons, and only one of them is a failure of
            // the sample: it either failed on its own, or it was never judged because the eval
            // declares no judge or the judge run recorded nothing.
            let (verdict, reason) = match (sample.status, summary.judge.is_some()) {
                (SampleStatus::Failed, _) => (
                    "failed",
                    sample
                        .reason
                        .as_deref()
                        .unwrap_or("the sample run did not succeed"),
                ),
                (SampleStatus::Succeeded, true) => (
                    "unjudged",
                    sample
                        .reason
                        .as_deref()
                        .unwrap_or("the judge recorded no verdicts"),
                ),
                (SampleStatus::Succeeded, false) => {
                    ("unjudged", "no judge configured for this eval")
                }
            };
            let _ = writeln!(
                report,
                "| {} | {} | - | {verdict} | {} |",
                sample.arm,
                sample.sample,
                escape_cell(reason)
            );
            continue;
        }
        for verdict in &sample.verdicts {
            let _ = writeln!(
                report,
                "| {} | {} | {} | {} | {} |",
                sample.arm,
                sample.sample,
                escape_cell(&verdict.id),
                if verdict.pass { "pass" } else { "fail" },
                escape_cell(&verdict.reason)
            );
        }
    }
    report
}

/// Keep one table cell on one line.
fn escape_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

/// Publish one JSON artifact without replacing an earlier one.
pub(crate) fn write_json(destination: &Path, value: &impl Serialize) -> Result<(), EvalError> {
    super::write_once(destination, &json_bytes(value)?)
}

/// Replace the aggregate artifact atomically, keeping one readable copy at all times.
pub(crate) fn replace_json(destination: &Path, value: &impl Serialize) -> Result<(), EvalError> {
    super::write_replacing(destination, &json_bytes(value)?)
}

fn json_bytes(value: &impl Serialize) -> Result<Vec<u8>, EvalError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}
