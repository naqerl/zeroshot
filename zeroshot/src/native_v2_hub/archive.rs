//! Archive of one run before the pool server that ran it is deleted: the lifecycle report, the
//! final status, the full watch replay and the logs, under `hub/archive/JOB_ID/`.

use std::path::{Path, PathBuf};

use serde_json::Value;

use super::driver::HubDriver;
use super::error::HubError;
use super::model::Job;
use super::service::{create_private_directory, replace_private};

/// Files of one archived run, in the order they are written.
pub(crate) const FILES: [&str; 4] = ["status.json", "report.json", "watch.ndjson", "logs.ndjson"];

/// What an archive pass saved for one job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Archived {
    pub folder: PathBuf,
    pub graph_hash: Option<String>,
}

/// Archive one job's run from `member` into `root/JOB_ID/`. Fails unless the run is finished,
/// which includes its postmortem: the target settles the postmortem inside the run before it
/// reports the `finished` phase. Every file is written atomically; a failed capture leaves the
/// job unarchived so the next pass retries, and its server is kept.
pub(crate) async fn archive(
    driver: &dyn HubDriver,
    root: &Path,
    member: &str,
    job: &Job,
) -> Result<Archived, HubError> {
    let run_id = job.run_id.clone().ok_or_else(|| {
        HubError::InvalidState(format!("hub job {} has no run to archive", job.job_id))
    })?;
    let status = driver.status(member, &run_id).await?;
    let phase = status.pointer("/status/phase").and_then(Value::as_str);
    if phase != Some("finished") {
        return Err(HubError::InvalidState(format!(
            "run {run_id} on {member} is {}, not finished; archive waits for it",
            phase.unwrap_or("of unknown phase")
        )));
    }
    let report = driver
        .capture(member, vec!["report".to_owned(), run_id.clone()])
        .await?;
    let report_document = report
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<Value>(line).ok())
        .ok_or_else(|| {
            HubError::Unanswered(format!("zeroshot report {run_id} returned no JSON"))
        })?;
    let watch = driver
        .capture(member, vec!["watch".to_owned(), run_id.clone()])
        .await?;
    let logs = driver
        .capture(member, vec!["logs".to_owned(), run_id.clone()])
        .await?;
    let folder = root.join(&job.job_id);
    create_private_directory(&folder)?;
    let status_bytes = serde_json::to_vec_pretty(&status)
        .map_err(|error| HubError::storage("run status could not be encoded", error))?;
    let report_bytes = serde_json::to_vec_pretty(&report_document)
        .map_err(|error| HubError::storage("run report could not be encoded", error))?;
    for (name, bytes) in FILES.iter().zip([
        status_bytes.as_slice(),
        report_bytes.as_slice(),
        watch.as_bytes(),
        logs.as_bytes(),
    ]) {
        replace_private(&folder.join(name), bytes)?;
    }
    Ok(Archived {
        folder,
        graph_hash: report_document
            .pointer("/run/authoredGraphHash")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}
