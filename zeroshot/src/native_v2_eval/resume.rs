//! Resumable eval bookkeeping.
//!
//! This is the eval's small child-resume journal, not the eval store. Slice 1 owns the manifest and
//! report; this journal only remembers which samples were submitted or completed so a partial eval
//! can continue without rerunning finished work.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use openengine_cluster_protocol::RunId;
use serde::{Deserialize, Serialize};

use super::capacity::{Capacity, CapacitySourceKind};
use super::capture::CapturedSample;
use super::runner::{SampleArtifact, SampleOutcome, SampleRun, SampleStatus};
use super::now_ms;

pub const RESUME_FILE: &str = "resume.json";
const RESUME_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleResumeState {
    Pending,
    Running,
    Completed,
    Failed,
    /// The run succeeded but its patch or judge checkout is still missing.
    CapturePending,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResumeSample {
    pub sample_id: String,
    pub state: SampleResumeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    /// Full submitted identity, including timings and target, so resume can reattach accurately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<SampleRun>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<SampleOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured: Option<CapturedSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_error: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CapacityWarning {
    pub previous: usize,
    pub current: usize,
    pub previous_source: CapacitySourceKind,
    pub current_source: CapacitySourceKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResumeAction {
    /// The sample has no completed attempt and should be submitted.
    Start,
    /// The sample completed; its stored outcome is authoritative.
    Skip,
    /// The run succeeded but capture did not. Retry capture without running the sample again.
    Capture,
    /// The previous controller lost this run before completion. Resume its retained workspace.
    Resume(SampleRun),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResumeJournal {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<Capacity>,
    #[serde(default)]
    pub samples: BTreeMap<String, ResumeSample>,
    pub updated_at_ms: u64,
}

impl Default for ResumeJournal {
    fn default() -> Self {
        Self {
            version: RESUME_VERSION,
            capacity: None,
            samples: BTreeMap::new(),
            updated_at_ms: now_ms(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResumeError {
    #[error("resume journal is unsupported version {0}")]
    UnsupportedVersion(u32),
    #[error("resume journal filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("resume journal is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
}

impl ResumeJournal {
    #[must_use]
    pub fn path(directory: &Path) -> PathBuf {
        directory.join(RESUME_FILE)
    }

    pub fn load(directory: &Path) -> Result<Self, ResumeError> {
        let bytes = std::fs::read(Self::path(directory))?;
        let journal: Self = serde_json::from_slice(&bytes)?;
        if journal.version != RESUME_VERSION {
            return Err(ResumeError::UnsupportedVersion(journal.version));
        }
        Ok(journal)
    }

    /// A missing journal is a fresh eval; malformed or unsupported journals fail closed.
    pub fn load_or_default(directory: &Path) -> Result<Self, ResumeError> {
        match Self::load(directory) {
            Ok(journal) => Ok(journal),
            Err(ResumeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::default())
            }
            Err(error) => Err(error),
        }
    }

    pub fn save(&self, directory: &Path) -> Result<(), ResumeError> {
        super::write_json_atomic(&Self::path(directory), self)?;
        Ok(())
    }

    #[must_use]
    pub fn action(&self, sample_id: &str) -> ResumeAction {
        match self.samples.get(sample_id) {
            Some(sample) if sample.state == SampleResumeState::Completed => ResumeAction::Skip,
            Some(sample) if sample.state == SampleResumeState::CapturePending => {
                ResumeAction::Capture
            }
            Some(sample) if sample.state == SampleResumeState::Running => match &sample.run {
                Some(run) => ResumeAction::Resume(run.clone()),
                None => ResumeAction::Start,
            },
            _ => ResumeAction::Start,
        }
    }

    /// A failed run with a retained run identity is a candidate for platform workspace recovery.
    /// The caller decides whether the target actually advertised recovery.
    #[must_use]
    pub fn resumable_run(&self, sample_id: &str) -> Option<&RunId> {
        self.samples.get(sample_id).and_then(|sample| {
            if matches!(
                sample.state,
                SampleResumeState::Running | SampleResumeState::Failed
            ) {
                sample.run_id.as_ref()
            } else {
                None
            }
        })
    }

    #[must_use]
    pub fn incomplete(&self) -> Vec<&str> {
        self.samples
            .values()
            .filter(|sample| sample.state != SampleResumeState::Completed)
            .map(|sample| sample.sample_id.as_str())
            .collect()
    }

    pub fn claim(&mut self, sample_id: &str, run: SampleRun) {
        let now = now_ms();
        self.updated_at_ms = now;
        let sample = self
            .samples
            .entry(sample_id.to_owned())
            .or_insert_with(|| ResumeSample {
                sample_id: sample_id.to_owned(),
                state: SampleResumeState::Pending,
                run_id: None,
                run: None,
                outcome: None,
                captured: None,
                error: None,
                capture_error: None,
                updated_at_ms: now,
            });
        sample.run_id = Some(run.run_id.clone());
        sample.run = Some(run);
        sample.state = SampleResumeState::Running;
        sample.error = None;
        sample.capture_error = None;
        sample.updated_at_ms = now;
    }

    pub fn record_outcome(&mut self, sample_id: &str, outcome: SampleOutcome) {
        let state = match outcome.status {
            SampleStatus::Succeeded => SampleResumeState::Completed,
            SampleStatus::Failed => SampleResumeState::Failed,
        };
        let run_id = outcome.run_id.clone();
        self.update_sample(sample_id, |sample| {
            sample.state = state;
            sample.run_id = Some(run_id);
            sample.error = outcome.failure_reason.clone();
            sample.outcome = Some(outcome);
        });
    }

    pub fn record_capture(&mut self, sample_id: &str, captured: CapturedSample) {
        self.update_sample(sample_id, |sample| {
            sample.captured = Some(captured);
            sample.capture_error = None;
        });
    }

    /// Record the durable artifact, including a capture failure that must not erase its outcome.
    pub fn record_artifact(&mut self, artifact: &SampleArtifact) {
        let state = match artifact.outcome.status {
            SampleStatus::Succeeded
                if artifact.capture_pending || artifact.capture_error.is_some() =>
            {
                SampleResumeState::CapturePending
            }
            SampleStatus::Succeeded => SampleResumeState::Completed,
            SampleStatus::Failed => SampleResumeState::Failed,
        };
        self.update_sample(&artifact.sample_id, |sample| {
            sample.state = state;
            sample.run_id = Some(artifact.outcome.run_id.clone());
            sample.error = artifact.outcome.failure_reason.clone();
            sample.outcome = Some(artifact.outcome.clone());
            sample.captured = artifact.captured.clone();
            sample.capture_error = artifact.capture_error.clone();
        });
    }

    pub fn record_error(&mut self, sample_id: &str, error: impl Into<String>) {
        self.update_sample(sample_id, |sample| {
            sample.state = SampleResumeState::Failed;
            sample.error = Some(error.into());
        });
    }

    /// Keep the submitted run resumable after a transient wait/poll failure. Treating this as a
    /// terminal sample failure would cause the next attempt to submit a duplicate run.
    pub fn record_interruption(&mut self, sample_id: &str, error: impl Into<String>) {
        self.update_sample(sample_id, |sample| {
            sample.state = SampleResumeState::Running;
            sample.error = Some(error.into());
        });
    }

    /// Record the capacity used by this attempt and report a changed concurrency. Separate queue
    /// waits remain valid, but reports should warn when two attempts ran at different widths.
    pub fn observe_capacity(&mut self, capacity: Capacity) -> Option<CapacityWarning> {
        let warning = self.capacity.and_then(|previous| {
            (previous.concurrency != capacity.concurrency).then_some(CapacityWarning {
                previous: previous.concurrency,
                current: capacity.concurrency,
                previous_source: previous.capacity_source,
                current_source: capacity.capacity_source,
            })
        });
        self.capacity = Some(capacity);
        self.updated_at_ms = now_ms();
        warning
    }

    fn update_sample(&mut self, sample_id: &str, update: impl FnOnce(&mut ResumeSample)) {
        let now = now_ms();
        self.updated_at_ms = now;
        let sample = self
            .samples
            .entry(sample_id.to_owned())
            .or_insert_with(|| ResumeSample {
                sample_id: sample_id.to_owned(),
                state: SampleResumeState::Pending,
                run_id: None,
                run: None,
                outcome: None,
                captured: None,
                error: None,
                capture_error: None,
                updated_at_ms: now,
            });
        update(sample);
        sample.updated_at_ms = now;
    }
}

#[cfg(test)]
#[path = "resume_test.rs"]
mod tests;
