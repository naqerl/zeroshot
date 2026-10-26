use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use openengine_cluster_protocol::{GIT_TRANSPORT_KIND, NODE_RUNTIME_LANES_KIND};
use serde_json::Value;
use zeroshot_engine::native_v2_cli::hub::HubJobSpec;

use super::error::HubError;
use super::model::{Job, JobState, PlacementSpec, Requirements};

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
    let lanes = spec
        .runtime_files
        .iter()
        .any(|path| runtime_has_lanes(&invoking.join(path)));
    let unpushed = pinned_revision
        .as_deref()
        .filter(|revision| !on_remote_branch(workdir.as_path(), revision));
    let source = SourceFacts {
        pinned_revision: pinned_revision.as_deref(),
        unpushed,
        lanes,
    };
    let requirements = requirements(spec, &source);
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
        requirements,
        priority: 0,
        labels,
        project,
        models,
        submission_key: spec.submission_key.clone(),
        submitted_at_ms: now_ms,
        not_before_ms: None,
        attempts: 0,
        unconfirmed_submissions: 0,
        target: None,
        run_id: None,
        dispatched_at_ms: None,
        last_cursor: None,
        last_cursor_at_ms: None,
        target_last_ok_at_ms: None,
        finished_at_ms: None,
        failure_reason: None,
        token_usage: None,
        error: None,
        schedule_id: None,
        output: None,
    })
}

/// Source and runtime facts resolved from the invoking folder.
pub(crate) struct SourceFacts<'a> {
    /// `HEAD` of the working folder when no explicit source was given.
    pub pinned_revision: Option<&'a str>,
    /// The pinned revision when no remote-tracking branch contains it.
    pub unpushed: Option<&'a str>,
    /// A runtime file binds an agent to a non-default lane.
    pub lanes: bool,
}

/// Placement requirements, mirroring what `zeroshot run` and target admission enforce:
/// explicit sources and hooks need a named target, named targets need a pushed revision, and
/// generic Git transport or per-node lanes need the matching discovery marker.
pub(crate) fn requirements(spec: &HubJobSpec, source: &SourceFacts<'_>) -> Requirements {
    let explicit_source =
        spec.repository.is_some() || spec.branch.is_some() || spec.revision.is_some();
    let local_unsupported = if explicit_source {
        Some("--repository, --branch, and --revision need a named target".to_owned())
    } else if spec.environment {
        Some("local runs reject --environment hooks".to_owned())
    } else {
        None
    };
    let remote_unsupported = if explicit_source {
        None
    } else {
        remote_reason(source)
    };
    let generic_repository = spec
        .repository
        .as_deref()
        .is_some_and(|repository| repository.contains(':') || repository.starts_with('/'));
    let sha256 = spec
        .revision
        .as_deref()
        .or(source.pinned_revision)
        .is_some_and(|revision| revision.len() == 64);
    let mut capabilities = Vec::new();
    if spec.source_connection.is_some() || generic_repository || sha256 {
        capabilities.push(GIT_TRANSPORT_KIND.to_owned());
    }
    if source.lanes {
        capabilities.push(NODE_RUNTIME_LANES_KIND.to_owned());
    }
    Requirements {
        capabilities,
        local_unsupported,
        remote_unsupported,
    }
}

/// Why named targets cannot run the working folder's source, if they cannot.
pub(crate) fn remote_reason(source: &SourceFacts<'_>) -> Option<String> {
    if let Some(revision) = source.unpushed {
        Some(format!(
            "HEAD {} is on no remote-tracking branch; push it for named targets",
            &revision[..revision.len().min(12)]
        ))
    } else if source.pinned_revision.is_none() {
        Some("the working folder has no committed Git revision; named targets need one".to_owned())
    } else {
        None
    }
}

/// The folder's `HEAD`, and that revision again when no remote-tracking branch contains it.
pub(crate) fn source_facts(folder: &Path) -> (Option<String>, Option<String>) {
    let revision = git(folder, &["rev-parse", "HEAD"]);
    let unpushed = revision
        .clone()
        .filter(|revision| !on_remote_branch(folder, revision));
    (revision, unpushed)
}

/// The folder a job's source resolves from: `--cwd` joined to the invoking folder.
pub(crate) fn working_folder(spec: &HubJobSpec) -> Result<PathBuf, HubError> {
    let invoking = std::env::current_dir()
        .map_err(|error| HubError::Usage(format!("the invoking folder is unavailable: {error}")))?;
    Ok(spec
        .cwd
        .as_ref()
        .map_or_else(|| invoking.clone(), |cwd| invoking.join(cwd)))
}

fn on_remote_branch(workdir: &Path, revision: &str) -> bool {
    git(workdir, &["branch", "--remotes", "--contains", revision]).is_some()
}

/// True when a runtime file binds any node to an explicit `lane`.
pub(crate) fn runtime_has_lanes(path: &Path) -> bool {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|document| document.get("nodes").and_then(Value::as_object).cloned())
        .is_some_and(|nodes| {
            nodes
                .values()
                .any(|node| node.get("lane").is_some_and(|lane| !lane.is_null()))
        })
}

fn git(workdir: &Path, arguments: &[&str]) -> Option<String> {
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
