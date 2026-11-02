//! `zeroshot hub eval`: re-run captured tasks with other topologies or models, measure each
//! sample, judge it, and report improvements and regressions against a baseline.
//!
//! Everything lives in hub state (`HubState.evals`) and the content-addressed blob store
//! `eval/blobs/SHA256` of the hub folder, so every client of a hub context sees the same
//! suites, cases, and runs. Identifiers are deterministic: a case ID hashes its captured
//! request, a run ID hashes the run request and its `--key`, and every sample job ID hashes
//! run, case, variant, and repeat, so retried requests never duplicate work.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::error::HubError;
use super::model::{Job, PlacementSpec, Requirements};
use super::usage::JobUsage;
use super::service::{create_private_directory, replace_private};
use super::wire::JobFile;

pub(crate) mod client;
pub(crate) mod judge;
pub(crate) mod report;
mod service;
pub(crate) mod variant;
pub(crate) mod worker;

/// Label naming the eval role of a hub job; judge jobs keep their terminal output.
pub(crate) const ROLE_LABEL: &str = "eval.role";
/// The variant that replays a case exactly as captured.
pub(crate) const BASELINE: &str = "baseline";

// ---------------------------------------------------------------- stored model

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct EvalState {
    #[serde(default)]
    pub suites: Vec<Suite>,
    #[serde(default)]
    pub cases: Vec<Case>,
    #[serde(default)]
    pub runs: Vec<EvalRun>,
}

impl EvalState {
    pub(crate) fn is_empty(&self) -> bool {
        self.suites.is_empty() && self.cases.is_empty() && self.runs.is_empty()
    }

    pub(crate) fn case(&self, case_id: &str) -> Option<&Case> {
        self.cases.iter().find(|case| case.case_id == case_id)
    }

    pub(crate) fn run(&self, run_id: &str) -> Option<&EvalRun> {
        self.runs.iter().find(|run| run.run_id == run_id)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Suite {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub cases: Vec<String>,
    pub created_at_ms: u64,
}

/// A file a case's run arguments name, kept in the blob store.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct BlobRef {
    /// Original file name, used when the blob is materialized.
    pub name: String,
    pub sha256: String,
}

/// A captured task: the run request with every input file, its source pin, and labels.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Case {
    pub case_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub title: String,
    /// `zeroshot run` arguments; the value of every file flag is `blob:SHA256`.
    pub run_args: Vec<String>,
    /// Blobs named by `run_args`, by file flag.
    #[serde(default)]
    pub files: BTreeMap<String, BlobRef>,
    pub cwd: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_revision: Option<String>,
    pub placement: PlacementSpec,
    #[serde(default)]
    pub requirements: Requirements,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_job: Option<String>,
    pub created_at_ms: u64,
}

impl Case {
    /// The canonical captured request a case ID hashes; names and times do not count.
    fn identity(&self) -> Value {
        serde_json::json!({
            "title": self.title,
            "runArgs": self.run_args,
            "files": self.files,
            "cwd": self.cwd,
            "pinnedRevision": self.pinned_revision,
            "placement": self.placement,
            "labels": self.labels,
        })
    }

    pub(crate) fn assign_id(&mut self) {
        self.case_id = format!("ec_{}", short_hash(&canonical(&self.identity())));
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub(crate) enum Baseline {
    /// A variant of this run; `baseline` replays the case as captured.
    Variant { name: String },
    /// An existing hub job's result is the baseline sample of the single case.
    Job { job_id: String },
    /// No baseline: report absolute numbers only.
    None,
}

/// One variant override; file keys carry their uploaded content as a blob.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Override {
    pub key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<BlobRef>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Variant {
    pub name: String,
    #[serde(default)]
    pub overrides: Vec<Override>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JudgeMode {
    #[default]
    Absolute,
    Pairwise,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JudgeKind {
    /// An executable run on the hub host: request JSON on stdin, verdict JSON on stdout.
    Cmd,
    /// A zeroshot graph run as a hub job; its terminal output is the verdict.
    Zeroshot,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Judge {
    pub kind: JudgeKind,
    pub mode: JudgeMode,
    /// The script (`cmd`) or graph (`zeroshot`).
    pub program: BlobRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<BlobRef>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Check {
    pub name: String,
    pub command: String,
}

/// A judge score. Hub state derives `Eq`, so the float compares bitwise.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialOrd)]
#[serde(transparent)]
pub(crate) struct Score(pub f64);

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for Score {}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SampleState {
    /// Waiting for its job to finish.
    Pending,
    /// Outcome and checks recorded; waiting for the judge when there is one.
    Measured,
    /// Judge verdict (or judge error) recorded.
    Judged,
    /// The sample could not be measured (its job vanished or was dequeued).
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Outcome {
    /// Terminal hub job state: succeeded, failed, or dequeued.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<JobUsage>,
    /// Model attribution: one model, `mixed`, or `unknown`.
    pub model: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CheckResult {
    pub name: String,
    pub passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Last lines of combined output.
    #[serde(default)]
    pub output: String,
    /// Why the hub could not run the check. Such a check is not a candidate failure: the
    /// sample's pass is unmeasured instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Verdict {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<Score>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    /// Hub job ID of the baseline sample a pairwise verdict compared against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub against: Option<String>,
    /// Why no score was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Sample {
    pub case_id: String,
    pub variant: String,
    pub repeat: u32,
    pub job_id: String,
    pub state: SampleState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<CheckResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<Verdict>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_job: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Sample {
    /// Stable key of a sample within its run.
    pub(crate) fn key(&self) -> String {
        format!("{}/{}/{}", self.case_id, self.variant, self.repeat)
    }

    /// The pass/fail of a measured sample: its job succeeded, every check passed, and an
    /// absolute judge did not answer `fail`. Unmeasured when the hub could not run a check.
    pub(crate) fn passed(&self, mode: Option<JudgeMode>) -> Option<bool> {
        let outcome = self.outcome.as_ref()?;
        if self.checks.iter().any(|check| check.error.is_some()) {
            return None;
        }
        let judged_fail = mode == Some(JudgeMode::Absolute)
            && self
                .judge
                .as_ref()
                .and_then(|verdict| verdict.verdict.as_deref())
                .is_some_and(|verdict| verdict.eq_ignore_ascii_case("fail"));
        Some(
            outcome.status == "succeeded"
                && self.checks.iter().all(|check| check.passed)
                && !judged_fail,
        )
    }

    pub(crate) fn is_settled(&self, judged: bool) -> bool {
        match self.state {
            SampleState::Pending => false,
            SampleState::Measured => !judged,
            SampleState::Judged | SampleState::Failed => true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct EvalRun {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suite: Option<String>,
    pub cases: Vec<String>,
    pub baseline: Baseline,
    pub variants: Vec<Variant>,
    pub repeat: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<PlacementSpec>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<Judge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<Check>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prices: Option<Value>,
    pub created_at_ms: u64,
    #[serde(default)]
    pub samples: Vec<Sample>,
}

impl EvalRun {
    pub(crate) fn judge_mode(&self) -> Option<JudgeMode> {
        self.judge.as_ref().map(|judge| judge.mode)
    }

    /// The variant name samples of the baseline carry, if any.
    pub(crate) fn baseline_variant(&self) -> Option<&str> {
        match &self.baseline {
            Baseline::Variant { name } => Some(name),
            Baseline::Job { .. } => Some(BASELINE),
            Baseline::None => None,
        }
    }

    pub(crate) fn complete(&self) -> bool {
        let judged = self.judge.is_some();
        self.samples.iter().all(|sample| sample.is_settled(judged))
    }

    /// The baseline sample a variant sample is compared with: same case, same repeat, or the
    /// only baseline sample of that case.
    pub(crate) fn baseline_for(&self, sample: &Sample) -> Option<&Sample> {
        let name = self.baseline_variant()?;
        if sample.variant == name {
            return None;
        }
        let candidates = self
            .samples
            .iter()
            .filter(|other| other.case_id == sample.case_id && other.variant == name)
            .collect::<Vec<_>>();
        candidates
            .iter()
            .find(|other| other.repeat == sample.repeat)
            .or_else(|| (candidates.len() == 1).then(|| &candidates[0]))
            .copied()
    }
}

// ---------------------------------------------------------------- API bodies

/// A file read by the client and uploaded with an eval request.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Upload {
    pub name: String,
    pub content: String,
}

/// `POST /v1/hub/eval/suites`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct AddSuite {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "from")]
pub(crate) enum CaptureSource {
    /// Copy a hub job of this hub.
    Job { job_id: String },
    /// A job built by the client from run flags, with its files.
    Flags {
        job: Box<Job>,
        #[serde(default)]
        files: Vec<JobFile>,
    },
}

/// `POST /v1/hub/eval/cases`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct AddCase {
    #[serde(default)]
    pub suites: Vec<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub labels: Vec<(String, String)>,
    pub source: CaptureSource,
}

/// `POST /v1/hub/eval/cases/query`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CaseQuery {
    #[serde(default)]
    pub suite: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct OverrideRequest {
    pub key: String,
    pub value: String,
    /// The file's content for file keys (graph, runtime-config, ...).
    #[serde(default)]
    pub upload: Option<Upload>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct VariantRequest {
    pub name: String,
    #[serde(default)]
    pub overrides: Vec<OverrideRequest>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct JudgeRequest {
    pub kind: JudgeKind,
    #[serde(default)]
    pub mode: JudgeMode,
    pub program: Upload,
    #[serde(default)]
    pub runtime: Option<Upload>,
}

/// `POST /v1/hub/eval/runs`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CreateRun {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub suite: Option<String>,
    #[serde(default)]
    pub cases: Vec<String>,
    #[serde(default)]
    pub baseline: Option<String>,
    #[serde(default)]
    pub variants: Vec<VariantRequest>,
    #[serde(default = "one")]
    pub repeat: u32,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub judge: Option<JudgeRequest>,
    #[serde(default)]
    pub checks: Vec<Check>,
    #[serde(default)]
    pub prices: Option<Value>,
}

const fn one() -> u32 {
    1
}

/// `POST /v1/hub/eval/runs/query`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct RunQuery {
    #[serde(default)]
    pub suite: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// `POST /v1/hub/eval/runs/{id}/report`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ReportRequest {
    #[serde(default)]
    pub baseline: Option<String>,
    #[serde(default)]
    pub thresholds: Vec<(String, String)>,
    #[serde(default)]
    pub prices: Option<Value>,
}

// ---------------------------------------------------------------- hashing and blobs

/// JSON with object keys sorted, so equal documents hash equally.
pub(crate) fn canonical(value: &Value) -> String {
    fn sort(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted = map
                    .iter()
                    .map(|(key, value)| (key.clone(), sort(value)))
                    .collect::<BTreeMap<_, _>>();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    sort(value).to_string()
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// First 16 hex digits of the SHA-256 of `text`.
pub(crate) fn short_hash(text: &str) -> String {
    sha256_hex(text.as_bytes())[..16].to_owned()
}

/// Deterministic hub job ID of one sample.
pub(crate) fn sample_job_id(run_id: &str, case_id: &str, variant: &str, repeat: u32) -> String {
    format!(
        "hj_{}",
        &sha256_hex(format!("{run_id}\n{case_id}\n{variant}\n{repeat}").as_bytes())[..32]
    )
}

/// Content-addressed files under `eval/blobs/` of the hub folder.
pub(crate) struct Blobs {
    directory: PathBuf,
}

impl Blobs {
    pub(crate) fn new(hub_directory: &Path) -> Self {
        Self {
            directory: hub_directory.join("eval").join("blobs"),
        }
    }

    pub(crate) fn put(&self, name: &str, bytes: &[u8]) -> Result<BlobRef, HubError> {
        let sha256 = sha256_hex(bytes);
        let path = self.directory.join(&sha256);
        if !path.is_file() {
            create_private_directory(&self.directory)?;
            replace_private(&path, bytes)?;
        }
        let name = Path::new(name).file_name().map_or_else(
            || "file".to_owned(),
            |name| name.to_string_lossy().into_owned(),
        );
        Ok(BlobRef { name, sha256 })
    }

    pub(crate) fn path(&self, blob: &BlobRef) -> PathBuf {
        self.directory.join(&blob.sha256)
    }

    pub(crate) fn read(&self, blob: &BlobRef) -> Result<Vec<u8>, HubError> {
        std::fs::read(self.path(blob)).map_err(|error| {
            HubError::storage(
                &format!("eval blob {} could not be read", blob.sha256),
                error,
            )
        })
    }

    /// Copy a blob to `folder/NAME` (private) and return that path.
    ///
    /// Samples run in parallel and share materialized files (a judge program is one file per
    /// content hash), so an identical file is left alone and a different one is replaced by an
    /// atomic rename: a reader or a running program never sees the file truncated.
    pub(crate) fn materialize(&self, blob: &BlobRef, folder: &Path) -> Result<PathBuf, HubError> {
        create_private_directory(folder)?;
        let path = folder.join(&blob.name);
        let bytes = self.read(blob)?;
        if std::fs::read(&path).is_ok_and(|existing| existing == bytes) {
            return Ok(path);
        }
        replace_private(&path, &bytes)?;
        Ok(path)
    }
}
