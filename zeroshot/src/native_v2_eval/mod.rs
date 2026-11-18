//! Evaluation seams for the native-v2 platform runner.
//!
//! Slice 2 owns the executable part of `zeroshot eval`: submitting samples to a platform or local
//! backend, resolving the concurrency budget, capturing the delivered branch as a patch, and
//! resuming a partially completed eval. Slice 1 owns the manifest, store, report, and scorers;
//! the three traits below are deliberately small so those modules can consume this slice without
//! a rewrite.

use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

pub mod capacity;
pub mod capture;
pub mod resume;
pub mod runner;

pub use capacity::{
    Capacity, CapacityError, CapacitySource, CapacitySourceKind, StaticCapacity, resolve_capacity,
};
pub use capture::{
    CaptureCredentials, CaptureError, CaptureRequest, CapturedSample, DeliveryReceipt,
    JudgeCheckout, capture_patch, checkout_patch,
};
pub use resume::{
    CapacityWarning, RESUME_FILE, ResumeAction, ResumeError, ResumeJournal, ResumeSample,
    SampleResumeState,
};
pub use runner::{
    BackendSampleRequest, FakeRunner, FakeSampleRequest, LocalRunner, PlatformRunner,
    SampleArtifact, SampleExecutionError, SampleOutcome, SampleRun, SampleRunner,
    SampleRunnerError, SampleStatus, SampleTimings, run_sample,
};

/// A registered scorer, implemented by slice 1.
///
/// The stored sample document is the unit of recomputation: adding a scorer after an eval has
/// finished must still work, so a scorer receives persisted JSON rather than live provider state.
pub trait EvalScore: Send + Sync {
    /// Stable scorer identity written into reports.
    fn scorer(&self) -> &str;

    /// Score one stored sample. `None` means the scorer does not apply to that sample.
    fn score(&self, sample: &serde_json::Value) -> Result<Option<f64>, EvalScoreError>;
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("scorer failed: {0}")]
pub struct EvalScoreError(String);

impl EvalScoreError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?;
    write_bytes_atomic(path, &bytes)
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    if !bytes.ends_with(b"\n") {
        temporary.write_all(b"\n")?;
    }
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;
