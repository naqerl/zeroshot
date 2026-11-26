//! Durable storage for self-contained eval definitions.
//!
//! Each eval lives in exactly one JSON document at `.zeroshot/evals/<slug>.json`. A document
//! carries its complete arms, per-arm graph and runtime, input, and scoring configuration. The
//! loader never resolves references between eval files; it only assembles the directory into one
//! in-memory catalog.
//!
//! Writes are published by writing and syncing a temporary file in the destination directory and
//! then renaming it over the target. A write-once create links the complete temporary file into
//! place with a no-clobber hard link, so two concurrent creators cannot both claim a slug.
//!
//! `eval run` keeps its per-sample artifacts under `runs/<slug>/<stamp>/`, beside the documents and
//! never among them: the loader only reads top-level `.json` files, so published runs cannot turn
//! into eval definitions.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use openengine_cluster_protocol::{GraphSpec, RuntimePlan};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[path = "native_v2_eval/config.rs"]
pub(crate) mod config;
#[path = "native_v2_eval/run.rs"]
pub(crate) mod run;

/// Directory below a workspace that owns the per-eval JSON documents.
pub const EVAL_DIRECTORY: &str = ".zeroshot/evals";

/// Directory below the eval store that holds the artifacts of every `eval run`.
pub const EVAL_RUNS_DIRECTORY: &str = "runs";

/// Schema accepted by the eval store.
pub const EVAL_SCHEMA: &str = "zeroshot.eval/v1";

/// Worker reference reserved for the judge step of the built-in eval-judge template.
///
/// Judging writes no file, so admission lets this one step retry a transient provider failure while
/// every other authored step keeps a single attempt.
pub(crate) const EVAL_JUDGE_WORKER_REF: &str = "builtin.eval.judge@1";

/// Attempts the eval judge step declares, and the ceiling admission applies to it.
pub(crate) const EVAL_JUDGE_ATTEMPTS: u64 = 3;

const MAX_ID_BYTES: usize = 128;

/// One self-contained eval document.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalDocument {
    /// Document schema. Omitted values use [`EVAL_SCHEMA`].
    #[serde(default = "default_schema")]
    pub schema: String,
    /// Stable file stem; must match the document's file name.
    pub slug: String,
    /// Human-readable summary shown by `eval list`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Named target that receives this eval's sample runs. `eval run --target` overrides it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Optional judging configuration; `eval run` expands and verifies its template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge: Option<EvalJudge>,
    /// Number of samples to run for every arm during execution.
    #[serde(default = "default_runs_per_arm")]
    pub runs_per_arm: u32,
    /// Complete initial input for every arm.
    #[serde(default)]
    pub input: Value,
    /// How runs are compared.
    #[serde(default)]
    pub scoring: EvalScoring,
    /// Named variants being compared. Each arm is complete; no arm refers to another file.
    pub arms: Vec<EvalArm>,
}

/// One independently runnable eval arm.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalArm {
    /// Stable arm identifier used by scoring.
    pub id: String,
    /// Source revision or branch the arm executes.
    pub head: String,
    /// Complete graph for this arm.
    pub graph: GraphSpec,
    /// Complete runtime for this arm.
    pub runtime: RuntimePlan,
}

/// Judging configuration for one eval.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalJudge {
    /// Name of the built-in graph template that judges one sample.
    pub template: String,
}

/// Scoring configuration for one eval.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalScoring {
    /// Criteria shown to the judge and used to compute weighted scores.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub criteria: Vec<EvalCriterion>,
    /// Optional arm used as the comparison baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<String>,
    /// Optional arm compared against the baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<String>,
}

/// One weighted scoring criterion.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalCriterion {
    /// Stable criterion identifier.
    pub id: String,
    /// Optional explanation shown to the judge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Positive criterion weight.
    #[serde(default = "default_weight")]
    pub weight: f64,
}

/// Stable listing row for one stored eval.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvalSummary {
    pub slug: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub runs_per_arm: u32,
    pub arms: Vec<String>,
}

/// The complete set of evals loaded from one directory.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EvalCatalog {
    evals: BTreeMap<String, EvalDocument>,
}

impl EvalCatalog {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.evals.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.evals.len()
    }

    #[must_use]
    pub fn get(&self, slug: &str) -> Option<&EvalDocument> {
        self.evals.get(slug)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &EvalDocument)> {
        self.evals.iter()
    }

    #[must_use]
    pub fn summaries(&self) -> Vec<EvalSummary> {
        self.evals
            .values()
            .map(|document| EvalSummary {
                slug: document.slug.clone(),
                description: document.description.clone(),
                runs_per_arm: document.runs_per_arm,
                arms: document.arms.iter().map(|arm| arm.id.clone()).collect(),
            })
            .collect()
    }
}

/// Filesystem-backed eval store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvalStore {
    root: PathBuf,
}

impl EvalStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The store for a workspace directory, rooted at `<workspace>/.zeroshot/evals`.
    #[must_use]
    pub fn for_workspace(workspace: impl AsRef<Path>) -> Self {
        Self::new(workspace.as_ref().join(EVAL_DIRECTORY))
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Load and validate every `.json` document in the store.
    ///
    /// A missing directory is an empty catalog. Documents are processed in file-name order, then
    /// duplicates are rejected before file-name/slug mismatches so duplicate slugs always produce
    /// the named duplicate error.
    pub fn load_all(&self) -> Result<EvalCatalog, EvalError> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(EvalCatalog::default());
            }
            Err(source) => {
                return Err(EvalError::Io {
                    path: self.root.clone(),
                    source,
                });
            }
        };

        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| EvalError::Io {
                path: self.root.clone(),
                source,
            })?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let metadata = entry.metadata().map_err(|source| EvalError::Io {
                path: path.clone(),
                source,
            })?;
            if metadata.is_file() {
                paths.push(path);
            }
        }
        paths.sort();

        let mut parsed = Vec::with_capacity(paths.len());
        for path in paths {
            let bytes = fs::read(&path).map_err(|source| EvalError::Io {
                path: path.clone(),
                source,
            })?;
            let document = serde_json::from_slice::<EvalDocument>(&bytes)
                .map_err(|error| EvalError::Invalid(format!("{}: {error}", path.display())))?;
            document.validate().map_err(|error| context(error, &path))?;
            parsed.push((path, document));
        }

        let mut evals: BTreeMap<String, (PathBuf, EvalDocument)> = BTreeMap::new();
        for (path, document) in &parsed {
            if let Some((first, _)) = evals.get(&document.slug) {
                return Err(EvalError::DuplicateSlug {
                    slug: document.slug.clone(),
                    first: first.clone(),
                    second: path.clone(),
                });
            }
            evals.insert(document.slug.clone(), (path.clone(), document.clone()));
        }

        let mut catalog = EvalCatalog::default();
        for (slug, (path, document)) in evals {
            validate_file_name(&path, &slug)?;
            catalog.evals.insert(slug, document);
        }
        Ok(catalog)
    }

    /// Load one eval by slug.
    pub fn load(&self, slug: &str) -> Result<EvalDocument, EvalError> {
        let path = self.path(slug)?;
        let bytes = fs::read(&path).map_err(|source| {
            if source.kind() == io::ErrorKind::NotFound {
                EvalError::NotFound {
                    slug: slug.to_owned(),
                    root: self.root.clone(),
                }
            } else {
                EvalError::Io {
                    path: path.clone(),
                    source,
                }
            }
        })?;
        let document = serde_json::from_slice::<EvalDocument>(&bytes)
            .map_err(|error| EvalError::Invalid(format!("{}: {error}", path.display())))?;
        document.validate().map_err(|error| context(error, &path))?;
        if document.slug != slug {
            return Err(EvalError::SlugMismatch {
                slug: document.slug,
                path,
            });
        }
        Ok(document)
    }

    /// List stored evals in slug order.
    pub fn list(&self) -> Result<Vec<EvalSummary>, EvalError> {
        Ok(self.load_all()?.summaries())
    }

    /// Create one eval, refusing to replace an existing slug unless `overwrite` is set.
    ///
    /// Returns the path of the published document.
    pub fn create(&self, document: &EvalDocument, overwrite: bool) -> Result<PathBuf, EvalError> {
        document.validate()?;
        let destination = self.path(&document.slug)?;
        let mut bytes = serde_json::to_vec_pretty(document)?;
        bytes.push(b'\n');

        if !overwrite && destination.exists() {
            return Err(EvalError::AlreadyExists {
                slug: document.slug.clone(),
                path: destination,
            });
        }

        publish_atomic(&destination, &bytes, overwrite, |_| Ok(())).map_err(
            |error| match error {
                EvalError::Io { source, .. }
                    if !overwrite && source.kind() == io::ErrorKind::AlreadyExists =>
                {
                    EvalError::AlreadyExists {
                        slug: document.slug.clone(),
                        path: destination.clone(),
                    }
                }
                other => other,
            },
        )?;
        Ok(destination)
    }

    /// Directory that holds the run bundles `eval run` publishes for one eval.
    #[must_use]
    pub fn runs_root(&self, slug: &str) -> PathBuf {
        self.root.join(EVAL_RUNS_DIRECTORY).join(slug)
    }

    /// Remove one eval document together with the run bundles published for it.
    pub fn remove(&self, slug: &str) -> Result<PathBuf, EvalError> {
        let path = self.path(slug)?;
        fs::remove_file(&path).map_err(|source| {
            if source.kind() == io::ErrorKind::NotFound {
                EvalError::NotFound {
                    slug: slug.to_owned(),
                    root: self.root.clone(),
                }
            } else {
                EvalError::Io {
                    path: path.clone(),
                    source,
                }
            }
        })?;
        let runs = self.runs_root(slug);
        if runs.exists() {
            fs::remove_dir_all(&runs).map_err(|source| EvalError::Io { path: runs, source })?;
        }
        Ok(path)
    }

    fn path(&self, slug: &str) -> Result<PathBuf, EvalError> {
        validate_slug(slug)?;
        Ok(self.root.join(format!("{slug}.json")))
    }
}

impl EvalDocument {
    fn validate(&self) -> Result<(), EvalError> {
        if self.schema != EVAL_SCHEMA {
            return Err(EvalError::Invalid(format!(
                "schema must be {EVAL_SCHEMA:?}, got {:?}",
                self.schema
            )));
        }
        validate_slug(&self.slug)?;
        if self.runs_per_arm == 0 {
            return Err(EvalError::Invalid(
                "runsPerArm must be at least 1".to_owned(),
            ));
        }
        if self.arms.is_empty() {
            return Err(EvalError::Invalid(
                "an eval must declare at least one arm".to_owned(),
            ));
        }

        if let Some(target) = self.target.as_deref() {
            validate_identifier("target", target)?;
        }
        if let Some(judge) = &self.judge {
            validate_identifier("judge template", &judge.template)?;
        }

        let mut arm_ids = BTreeSet::new();
        for arm in &self.arms {
            validate_identifier("arm id", &arm.id)?;
            if !arm_ids.insert(arm.id.as_str()) {
                return Err(EvalError::Invalid(format!("duplicate arm id {:?}", arm.id)));
            }
            if arm.head.trim().is_empty() {
                return Err(EvalError::Invalid(format!(
                    "arm {:?} must declare a head",
                    arm.id
                )));
            }
        }

        let mut criterion_ids = BTreeSet::new();
        for criterion in &self.scoring.criteria {
            validate_identifier("criterion id", &criterion.id)?;
            if !criterion_ids.insert(criterion.id.as_str()) {
                return Err(EvalError::Invalid(format!(
                    "duplicate criterion id {:?}",
                    criterion.id
                )));
            }
            if !criterion.weight.is_finite() || criterion.weight <= 0.0 {
                return Err(EvalError::Invalid(format!(
                    "criterion {:?} weight must be finite and positive",
                    criterion.id
                )));
            }
        }

        for (field, value) in [
            ("baseline", self.scoring.baseline.as_deref()),
            ("candidate", self.scoring.candidate.as_deref()),
        ] {
            if let Some(value) = value {
                if !arm_ids.contains(value) {
                    return Err(EvalError::Invalid(format!(
                        "scoring {field} names unknown arm {value:?}"
                    )));
                }
            }
        }
        if self.scoring.baseline.is_some() && self.scoring.baseline == self.scoring.candidate {
            return Err(EvalError::Invalid(
                "scoring baseline and candidate must name different arms".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Distinguishable failures for eval storage, loading, and write-once publication.
#[derive(Debug, Error)]
pub enum EvalError {
    #[error("eval store I/O at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid eval document: {0}")]
    Invalid(String),
    #[error("could not encode eval document: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("invalid eval slug {slug:?}: {reason}")]
    InvalidSlug { slug: String, reason: String },
    #[error("duplicate eval slug {slug:?}: both {first} and {second} define it")]
    DuplicateSlug {
        slug: String,
        first: PathBuf,
        second: PathBuf,
    },
    #[error("eval {slug:?} already exists at {path}; pass --overwrite to replace it")]
    AlreadyExists { slug: String, path: PathBuf },
    #[error("eval {slug:?} was not found in {root}")]
    NotFound { slug: String, root: PathBuf },
    #[error("eval slug {slug:?} does not match file name {path}")]
    SlugMismatch { slug: String, path: PathBuf },
}

fn default_schema() -> String {
    EVAL_SCHEMA.to_owned()
}

const fn default_runs_per_arm() -> u32 {
    1
}

const fn default_weight() -> f64 {
    1.0
}

fn validate_slug(slug: &str) -> Result<(), EvalError> {
    match identifier_problem(slug) {
        Some(reason) => Err(EvalError::InvalidSlug {
            slug: slug.to_owned(),
            reason: reason.to_owned(),
        }),
        None => Ok(()),
    }
}

fn validate_identifier(kind: &str, value: &str) -> Result<(), EvalError> {
    match identifier_problem(value) {
        Some(reason) => Err(EvalError::Invalid(format!("{kind} {value:?} {reason}"))),
        None => Ok(()),
    }
}

fn identifier_problem(value: &str) -> Option<&'static str> {
    if value.is_empty() {
        return Some("must not be empty");
    }
    if value.len() > MAX_ID_BYTES {
        return Some("must be at most 128 bytes");
    }
    if value == "." || value == ".." {
        return Some("must not be . or ..");
    }
    let mut characters = value.chars();
    let first = characters.next()?;
    if !first.is_ascii_alphanumeric() {
        return Some("must start with an ASCII letter or digit");
    }
    if !characters
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
    {
        return Some("may contain only ASCII letters, digits, '.', '_' and '-'");
    }
    None
}

fn context(error: EvalError, path: &Path) -> EvalError {
    match error {
        EvalError::Invalid(message) => EvalError::Invalid(format!("{}: {message}", path.display())),
        other => other,
    }
}

fn validate_file_name(path: &Path, slug: &str) -> Result<(), EvalError> {
    let expected = format!("{slug}.json");
    if path.file_name().and_then(|value| value.to_str()) == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(EvalError::SlugMismatch {
            slug: slug.to_owned(),
            path: path.to_path_buf(),
        })
    }
}

/// Publish `bytes` at `destination` once, refusing to replace an existing file.
///
/// `eval run` publishes its artifacts this way, so a rerun never overwrites evidence an earlier
/// run already recorded.
pub(crate) fn write_once(destination: &Path, bytes: &[u8]) -> Result<(), EvalError> {
    publish_atomic(destination, bytes, false, |_| Ok(()))
}

/// Publish `bytes` at `destination`, replacing an existing file atomically.
///
/// `eval run` uses this for the one aggregate it keeps for the newest run, so a reader never sees
/// a partially written summary.
pub(crate) fn write_replacing(destination: &Path, bytes: &[u8]) -> Result<(), EvalError> {
    publish_atomic(destination, bytes, true, |_| Ok(()))
}

/// Publish `bytes` at `destination` through a synced same-directory temporary.
///
/// `before_commit` runs after the temporary file is complete and synced but before it becomes
/// visible. Tests use it to simulate an interruption in the middle of the publication step. Once
/// synchronization succeeds, replacement either renames over the target or, for write-once
/// creation, links the complete file in with no-clobber semantics.
fn publish_atomic(
    destination: &Path,
    bytes: &[u8],
    overwrite: bool,
    before_commit: impl FnOnce(&Path) -> io::Result<()>,
) -> Result<(), EvalError> {
    let parent = destination.parent().ok_or_else(|| EvalError::Io {
        path: destination.to_path_buf(),
        source: io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent"),
    })?;
    fs::create_dir_all(parent).map_err(|source| EvalError::Io {
        path: parent.to_path_buf(),
        source,
    })?;

    let file_name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("eval.json");
    let temporary = parent.join(format!(".{file_name}.{}.tmp", uuid::Uuid::now_v7()));

    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| EvalError::Io {
                path: temporary.clone(),
                source,
            })?;
        file.write_all(bytes).map_err(|source| EvalError::Io {
            path: temporary.clone(),
            source,
        })?;
        file.sync_all().map_err(|source| EvalError::Io {
            path: temporary.clone(),
            source,
        })?;
        drop(file);

        before_commit(&temporary).map_err(|source| EvalError::Io {
            path: destination.to_path_buf(),
            source,
        })?;

        if overwrite {
            crate::execution::platform::commit_file(&temporary, destination, parent).map_err(
                |source| EvalError::Io {
                    path: destination.to_path_buf(),
                    source,
                },
            )?;
        } else {
            // `hard_link` is atomic and fails if the destination exists, so a write-once create
            // cannot be won by two concurrent writers. The complete temp file is already synced.
            std::fs::hard_link(&temporary, destination).map_err(|source| EvalError::Io {
                path: destination.to_path_buf(),
                source,
            })?;
            let _ = fs::remove_file(&temporary);
            sync_directory(parent).map_err(|source| EvalError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
#[path = "native_v2_eval/tests.rs"]
mod tests;
