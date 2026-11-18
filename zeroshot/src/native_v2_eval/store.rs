use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use super::{
    EvalData, EvalError, EvalId, EvalManifest, EvalReport, EvalSample, SampleCounts, model::now_ms,
    report,
};

const STORE_SCHEMA: &str = "zeroshot.eval-store/v1";

/// Filesystem-backed eval store rooted at `.zeroshot/eval`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvalStore {
    root: PathBuf,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvalSummary {
    pub id: EvalId,
    pub name: String,
    pub created_at_ms: u64,
    pub runs_per_arm: u32,
    pub arms: Vec<String>,
    pub samples: SampleCounts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredMetadata {
    schema: String,
    id: EvalId,
    created_at_ms: u64,
}

impl EvalStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn for_workspace(directory: impl AsRef<Path>) -> Self {
        Self::new(directory.as_ref().join(".zeroshot").join("eval"))
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn create(
        &self,
        manifest_source: &str,
        id: &EvalId,
        created_at_ms: u64,
    ) -> Result<(), EvalError> {
        let directory = self.root.join(id.as_str());
        if directory.exists() {
            return Err(EvalError::Command(format!(
                "eval directory {} already exists",
                directory.display()
            )));
        }
        std::fs::create_dir_all(directory.join("samples"))
            .map_err(|error| EvalError::io(&directory, error))?;
        std::fs::create_dir_all(directory.join("patches"))
            .map_err(|error| EvalError::io(&directory, error))?;
        atomic_write(&directory.join("manifest.toml"), manifest_source.as_bytes())?;
        let metadata = StoredMetadata {
            schema: STORE_SCHEMA.to_owned(),
            id: id.clone(),
            created_at_ms,
        };
        write_json(&directory.join("eval.json"), &metadata)?;
        Ok(())
    }

    pub fn load(&self, id: &EvalId) -> Result<EvalData, EvalError> {
        let directory = self.root.join(id.as_str());
        if !directory.is_dir() {
            return Err(EvalError::NotFound(id.to_string()));
        }
        let metadata: StoredMetadata = read_json(&directory.join("eval.json"))?;
        if metadata.schema != STORE_SCHEMA {
            return Err(EvalError::data(
                directory.join("eval.json"),
                format!("unexpected store schema {:?}", metadata.schema),
            ));
        }
        if &metadata.id != id {
            return Err(EvalError::data(
                directory.join("eval.json"),
                format!(
                    "stored id {} does not match directory id {}",
                    metadata.id, id
                ),
            ));
        }
        let manifest_path = directory.join("manifest.toml");
        let manifest_source = std::fs::read_to_string(&manifest_path)
            .map_err(|error| EvalError::io(&manifest_path, error))?;
        let manifest = EvalManifest::parse(&manifest_source)?;
        let samples = self.read_samples(&directory)?;
        let report_path = directory.join("report.json");
        let report = if report_path.is_file() {
            Some(read_json(&report_path)?)
        } else {
            None
        };
        Ok(EvalData {
            id: metadata.id,
            created_at_ms: metadata.created_at_ms,
            manifest,
            samples,
            report,
        })
    }

    pub fn list(&self) -> Result<Vec<EvalSummary>, EvalError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let entries =
            std::fs::read_dir(&self.root).map_err(|error| EvalError::io(&self.root, error))?;
        let mut ids = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| EvalError::io(&self.root, error))?;
            if !entry
                .file_type()
                .map_err(|error| EvalError::io(entry.path(), error))?
                .is_dir()
            {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Ok(id) = EvalId::new(name.to_owned()) else {
                continue;
            };
            ids.push(id);
        }
        ids.sort();
        ids.into_iter()
            .map(|id| {
                let data = self.load(&id)?;
                Ok(EvalSummary {
                    id: data.id.clone(),
                    name: data.manifest.name.clone(),
                    created_at_ms: data.created_at_ms,
                    runs_per_arm: data.manifest.runs_per_arm,
                    arms: data
                        .manifest
                        .arms
                        .iter()
                        .map(|arm| arm.id.clone())
                        .collect(),
                    samples: data.sample_counts(),
                    score: data.report.as_ref().map(|report| report.aggregate_score),
                })
            })
            .collect()
    }

    pub fn write_sample(&self, id: &EvalId, sample: &EvalSample) -> Result<(), EvalError> {
        let directory = self.root.join(id.as_str());
        atomic_write(
            &directory
                .join("samples")
                .join(format!("{}.json", sample.id)),
            &json_bytes(sample)?,
        )
    }

    pub fn write_patch(
        &self,
        id: &EvalId,
        sample_id: &str,
        content: &str,
    ) -> Result<String, EvalError> {
        let relative = format!("patches/{sample_id}.patch");
        let path = self.root.join(id.as_str()).join(&relative);
        atomic_write(&path, content.as_bytes())?;
        Ok(relative)
    }

    /// Read a captured patch by its stored relative path. Absolute and escaping paths are refused
    /// so a rewritten sample record cannot make the judge read outside the eval directory.
    pub fn read_patch(&self, id: &EvalId, relative: &str) -> Result<String, EvalError> {
        let path = std::path::Path::new(relative);
        if path.is_absolute()
            || path.components().any(|component| {
                !matches!(
                    component,
                    std::path::Component::Normal(_) | std::path::Component::CurDir
                )
            })
        {
            return Err(EvalError::Command(format!(
                "stored patch path {relative:?} must stay inside the eval directory"
            )));
        }
        let path = self.root.join(id.as_str()).join(path);
        std::fs::read_to_string(&path).map_err(|error| EvalError::io(&path, error))
    }

    /// Persist `report.json` first. Markdown is rendered only from this stored report.
    pub fn write_report(&self, id: &EvalId, report: &EvalReport) -> Result<(), EvalError> {
        let directory = self.root.join(id.as_str());
        write_json(&directory.join("report.json"), report)
    }

    pub fn write_report_markdown(&self, id: &EvalId, report: &EvalReport) -> Result<(), EvalError> {
        let directory = self.root.join(id.as_str());
        atomic_write(
            &directory.join("report.md"),
            report::render_markdown(report).as_bytes(),
        )
    }

    pub fn clean(&self, id: &EvalId) -> Result<(), EvalError> {
        let directory = self.root.join(id.as_str());
        if !directory.is_dir() {
            return Err(EvalError::NotFound(id.to_string()));
        }
        std::fs::remove_dir_all(&directory).map_err(|error| EvalError::io(&directory, error))
    }

    pub fn clean_all(&self) -> Result<Vec<EvalId>, EvalError> {
        let ids = self
            .list()?
            .into_iter()
            .map(|summary| summary.id)
            .collect::<Vec<_>>();
        for id in &ids {
            self.clean(id)?;
        }
        Ok(ids)
    }

    fn read_samples(&self, directory: &Path) -> Result<Vec<EvalSample>, EvalError> {
        let samples_directory = directory.join("samples");
        if !samples_directory.is_dir() {
            return Ok(Vec::new());
        }
        let entries = std::fs::read_dir(&samples_directory)
            .map_err(|error| EvalError::io(&samples_directory, error))?;
        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| EvalError::io(&samples_directory, error))?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
                paths.push(path);
            }
        }
        paths.sort();
        paths.into_iter().map(|path| read_json(&path)).collect()
    }
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, EvalError> {
    let bytes = std::fs::read(path).map_err(|error| EvalError::io(path, error))?;
    serde_json::from_slice(&bytes).map_err(|error| EvalError::data(path, error.to_string()))
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), EvalError> {
    atomic_write(path, &json_bytes(value)?)
}

fn json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, EvalError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), EvalError> {
    let parent = path
        .parent()
        .ok_or_else(|| EvalError::Command(format!("{} has no parent directory", path.display())))?;
    std::fs::create_dir_all(parent).map_err(|error| EvalError::io(parent, error))?;
    let mut temporary =
        NamedTempFile::new_in(parent).map_err(|error| EvalError::io(parent, error))?;
    temporary
        .write_all(bytes)
        .map_err(|error| EvalError::io(path, error))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| EvalError::io(path, error))?;
    match temporary.persist(path) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let temporary = error.file;
            std::fs::remove_file(path).map_err(|error| EvalError::io(path, error))?;
            temporary
                .persist(path)
                .map_err(|error| EvalError::io(path, error.error))?;
            Ok(())
        }
        Err(error) => Err(EvalError::io(path, error.error)),
    }
}

pub(crate) fn timestamp() -> u64 {
    now_ms()
}
