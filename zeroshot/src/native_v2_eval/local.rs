//! `runner = "local"`: run every arm through the same path as `zeroshot run`.
//!
//! Each sample becomes one `zeroshot run` command built from the arm (graph or template, runtime
//! config, delivery, extra arguments) and the arm's initial input. Samples run one at a time so
//! wall time is not skewed by local contention. A succeeded run's pushed branch is captured into
//! `patch.diff` against the workspace `HEAD` the eval started from, which is what the judge reads.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::native_v2_cli::{NativeV2CliCommand, RunCommand, local::LocalCliBackend};

use super::cli::{EvalRunCommand, ManifestRunRequest, run_manifest_with_capture};
use super::{
    BackendSampleRequest, CaptureCredentials, CaptureRequest, DeliveryReceipt, EvalArm, EvalData,
    EvalError, EvalId, EvalManifest, LocalRunner,
};

/// Everything the arm-to-command translation needs besides the arm itself.
pub(crate) struct LocalArmContext<'a> {
    /// Directory relative arm paths resolve against (the manifest's directory).
    pub manifest_directory: &'a Path,
    /// The checkout every sample starts from.
    pub workspace: &'a Path,
    /// JSON file holding this sample's initial input.
    pub input: &'a Path,
    pub title: &'a str,
}

/// Build the `zeroshot run ...` argument vector for one sample of `arm`.
pub(crate) fn run_arguments(arm: &EvalArm, context: &LocalArmContext<'_>) -> Vec<OsString> {
    let resolve = |path: &str| -> OsString {
        let path = Path::new(path);
        if path.is_absolute() {
            path.as_os_str().to_owned()
        } else {
            context.manifest_directory.join(path).into_os_string()
        }
    };
    let mut arguments: Vec<OsString> = vec![
        "run".into(),
        "--title".into(),
        context.title.into(),
        "--input".into(),
        context.input.as_os_str().to_owned(),
        "--cwd".into(),
        context.workspace.as_os_str().to_owned(),
    ];
    if let Some(graph) = &arm.graph {
        arguments.push("--graph".into());
        arguments.push(resolve(graph));
    }
    if let Some(template) = &arm.template {
        arguments.push("--template".into());
        arguments.push(template.into());
        arguments.push("--delivery".into());
        arguments.push(arm.delivery.as_deref().unwrap_or("push").into());
    }
    if let Some(runtime) = &arm.runtime_config {
        arguments.push("--runtime-config".into());
        arguments.push(resolve(runtime));
    }
    if let Some(runtime) = &arm.uniform_runtime_config {
        arguments.push("--uniform-runtime-config".into());
        arguments.push(resolve(runtime));
    }
    arguments.extend(arm.run_args.iter().map(OsString::from));
    arguments
}

/// Parse the arguments with the public CLI schema so evals accept exactly what `zeroshot run` does.
/// `label` names the source in errors, such as `arm "baseline"` or `judge`.
pub(crate) fn run_command(arguments: Vec<OsString>, label: &str) -> Result<RunCommand, EvalError> {
    match crate::native_v2_cli::parse_native_v2_args(arguments) {
        Ok(NativeV2CliCommand::Run(run)) => {
            if run.target.is_some() {
                return Err(EvalError::Manifest(format!(
                    "{label} runArgs must not select --target for the local runner"
                )));
            }
            if run.detach || run.validate_only {
                return Err(EvalError::Manifest(format!(
                    "{label} runArgs must not pass --detach or --validate-only"
                )));
            }
            Ok(*run)
        }
        Ok(_) => Err(EvalError::Manifest(format!(
            "{label} did not produce a `zeroshot run` command"
        ))),
        Err(error) => Err(EvalError::Manifest(format!(
            "{label} is not a valid `zeroshot run`: {error}"
        ))),
    }
}

pub(crate) async fn run_manifest_locally(
    mut command: EvalRunCommand,
    manifest: EvalManifest,
) -> Result<EvalData, EvalError> {
    let manifest_directory = absolute(
        command
            .manifest
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?;
    let workspace = absolute(command.workspace.as_ref().unwrap_or(&command.directory))?;
    let base_revision = git(&workspace, &["rev-parse", "HEAD"])?;
    let remote = match &manifest.options.capture_remote {
        Some(remote) => remote.clone(),
        None => git(&workspace, &["remote", "get-url", "origin"])?,
    };
    let id = match command.id.take() {
        Some(id) => EvalId::new(id)?,
        None => EvalId::generate(&manifest.name)?,
    };
    command.id = Some(id.to_string());
    let backend = Arc::new(LocalCliBackend::production().map_err(|error| {
        EvalError::Command(format!("local run backend is unavailable: {error}"))
    })?);
    let inputs = tempfile::tempdir().map_err(|error| EvalError::io(".", error))?;
    command.validate_arms(&manifest)?;
    let mut prepared = std::collections::BTreeMap::new();
    for arm in manifest.arms.iter().filter(|arm| command.runs_arm(&arm.id)) {
        for run_index in 0..manifest.runs_per_arm {
            let sample_id = format!("{}-{run_index:04}", arm.id);
            let input = inputs.path().join(format!("{sample_id}.json"));
            let bytes = serde_json::to_vec_pretty(&arm.initial_input())?;
            std::fs::write(&input, bytes).map_err(|error| EvalError::io(&input, error))?;
            let title = format!("eval {} {sample_id}", manifest.name);
            let run = run_command(
                run_arguments(
                    arm,
                    &LocalArmContext {
                        manifest_directory: &manifest_directory,
                        workspace: &workspace,
                        input: &input,
                        title: &title,
                    },
                ),
                &format!("arm {:?}", arm.id),
            )?;
            let request =
                crate::native_v2_cli::execution::prepare_run_submission(&run, backend.as_ref())
                    .await
                    .map_err(|error| {
                        EvalError::Command(format!(
                            "arm {:?} could not be prepared: {error}",
                            arm.id
                        ))
                    })?;
            prepared.insert(
                sample_id.clone(),
                BackendSampleRequest {
                    sample_id,
                    concurrency: 1,
                    request,
                },
            );
        }
    }
    // Unreachable in practice: the plan re-derives the same sample ids from the same manifest. An
    // unknown id gets an empty sample id, which the runner rejects and records as a failed sample.
    let fallback = prepared
        .values()
        .next()
        .cloned()
        .map(|request| BackendSampleRequest {
            sample_id: String::new(),
            ..request
        })
        .ok_or_else(|| EvalError::Manifest("the eval plans no samples".to_owned()))?;
    let journal = super::EvalStore::for_workspace(&command.directory)
        .root()
        .join(".runs")
        .join(id.as_str());
    let mut runner = LocalRunner::new(backend);
    if let Some(minutes) = manifest.options.deadline_minutes {
        runner = runner.with_deadline(Duration::from_secs(minutes.saturating_mul(60)));
    }
    let capture = CaptureRequest::new(
        PathBuf::new(),
        remote,
        base_revision,
        DeliveryReceipt::default(),
    )
    .with_credentials(CaptureCredentials {
        token: std::env::var("GH_TOKEN")
            .ok()
            .filter(|token| !token.is_empty()),
        git: None,
    });
    run_manifest_with_capture(
        ManifestRunRequest {
            command,
            runner: &runner,
            capacity_source: None,
            concurrency: None,
            journal_directory: journal,
            request: |_: &EvalId,
                      sample_id: &str,
                      _: &EvalArm,
                      _: u32,
                      _: &[super::EvalCriterion]| {
                prepared
                    .remove(sample_id)
                    .unwrap_or_else(|| fallback.clone())
            },
        },
        Some(capture),
    )
    .await
}

fn absolute(path: &Path) -> Result<PathBuf, EvalError> {
    std::path::absolute(path).map_err(|error| EvalError::io(path, error))
}

fn git(directory: &Path, arguments: &[&str]) -> Result<String, EvalError> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(arguments)
        .output()
        .map_err(|error| EvalError::io(directory, error))?;
    if !output.status.success() {
        return Err(EvalError::Command(format!(
            "`git {}` failed in {}: the local runner needs a committed Git workspace with a pushable remote",
            arguments.join(" "),
            directory.display()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
