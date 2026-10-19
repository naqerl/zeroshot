use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use zeroshot_engine::native_v2_cli::hub::HubJobSpec;

use super::error::HubError;
use super::model::{Job, JobState, PlacementSpec};

/// Build a queued job from parsed arguments, resolving everything that must not change between
/// acceptance and placement: the invoking folder, the source revision, and attribution.
pub(crate) fn new_job(spec: &HubJobSpec, now_ms: u64) -> Result<Job, HubError> {
    let invoking = std::env::current_dir()
        .map_err(|error| HubError::Usage(format!("the invoking folder is unavailable: {error}")))?;
    let workdir = spec
        .cwd
        .as_ref()
        .map_or_else(|| invoking.clone(), |cwd| invoking.join(cwd));
    let labels = spec.labels.iter().cloned().collect::<BTreeMap<_, _>>();
    let pinned_revision = if spec.revision.is_none() && spec.repository.is_none() {
        git(&workdir, &["rev-parse", "HEAD"])
    } else {
        None
    };
    let project = labels
        .get("project")
        .cloned()
        .or_else(|| spec.repository.clone())
        .or_else(|| {
            git(&workdir, &["remote", "get-url", "origin"]).map(|url| repository_name(&url))
        })
        .or_else(|| {
            workdir
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        });
    let models = spec
        .runtime_files
        .iter()
        .flat_map(|path| runtime_models(&invoking.join(path)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(Job {
        job_id: format!("hj_{}", uuid::Uuid::now_v7().simple()),
        state: JobState::Queued,
        title: spec.title.clone(),
        run_args: spec.run_args.clone(),
        cwd: invoking,
        pinned_revision,
        placement: PlacementSpec {
            target: spec.target.clone(),
            tags: spec.tags.clone(),
            strategy: spec.strategy.into(),
        },
        priority: 0,
        labels,
        project,
        models,
        submission_key: spec.submission_key.clone(),
        submitted_at_ms: now_ms,
        not_before_ms: None,
        expires_at_ms: None,
        attempts: 0,
        target: None,
        run_id: None,
        dispatched_at_ms: None,
        finished_at_ms: None,
        failure_reason: None,
        token_usage: None,
        error: None,
    })
}

fn git(workdir: &PathBuf, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workdir)
        .args(arguments)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!text.is_empty()).then_some(text)
}

/// `owner/name` from an HTTPS or SSH remote URL.
pub(crate) fn repository_name(url: &str) -> String {
    let trimmed = url.trim_end_matches('/').trim_end_matches(".git");
    let path = trimmed
        .rsplit_once(':')
        .filter(|(scheme, _)| !scheme.contains('/') && !scheme.starts_with("http"))
        .map_or(trimmed, |(_, path)| path);
    let segments = path.rsplit('/').take(2).collect::<Vec<_>>();
    match segments.as_slice() {
        [name, owner] => format!("{owner}/{name}"),
        _ => trimmed.to_owned(),
    }
}

/// Opaque model identifiers named by a runtime or uniform runtime file.
pub(crate) fn runtime_models(path: &Path) -> Vec<String> {
    let Ok(bytes) = std::fs::read(path) else {
        return Vec::new();
    };
    let Ok(document) = serde_json::from_slice::<Value>(&bytes) else {
        return Vec::new();
    };
    let mut models = Vec::new();
    if let Some(model) = document.get("model").and_then(Value::as_str) {
        models.push(model.to_owned());
    }
    if let Some(nodes) = document.get("nodes").and_then(Value::as_object) {
        models.extend(
            nodes
                .values()
                .filter_map(|node| node.get("model").and_then(Value::as_str))
                .map(str::to_owned),
        );
    }
    models
}
