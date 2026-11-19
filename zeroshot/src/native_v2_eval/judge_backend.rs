//! Judge graph execution through the same path as `zeroshot run`.
//!
//! The judge is an ordinary graph run: the built-in `eval-judge` template (or a custom graph JSON)
//! with the runtime the manifest names (`judgeRuntimeConfig`, `judgeUniformRuntimeConfig`, or a
//! profile in `judgeRunArgs`). Its provider and model are whatever that runtime configures; the
//! eval code never selects or overrides one.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use async_trait::async_trait;

use crate::native_v2_cli::NativeV2CliBackend;

use super::judge_template::judge_input_for_template;
use super::local::run_command;
use super::{
    BackendSampleRequest, EvalError, JUDGE_TEMPLATE_NAME, JudgeGraphExecutor, JudgeTemplate,
    LocalRunner, run_sample,
};

/// How to submit the judge graph, as `zeroshot run` arguments. Paths are absolute.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JudgeRunConfig {
    /// `eval-judge` (the built-in template) or a graph JSON path.
    pub template: String,
    /// Exact runtime plan (`--runtime-config`).
    pub runtime_config: Option<PathBuf>,
    /// One runtime for every node (`--uniform-runtime-config`).
    pub uniform_runtime_config: Option<PathBuf>,
    /// Extra `zeroshot run` arguments, such as `["--profile", "judge"]`.
    pub run_args: Vec<String>,
}

impl JudgeRunConfig {
    /// The `zeroshot run` argument vector for one judge input file in a scratch directory.
    #[must_use]
    pub fn arguments(&self, input: &Path, cwd: &Path) -> Vec<OsString> {
        let mut arguments: Vec<OsString> = vec![
            "run".into(),
            "--title".into(),
            "Eval judge".into(),
            "--input".into(),
            input.as_os_str().to_owned(),
            "--cwd".into(),
            cwd.as_os_str().to_owned(),
        ];
        if self.template == JUDGE_TEMPLATE_NAME {
            arguments.extend(["--template".into(), JUDGE_TEMPLATE_NAME.into()]);
        } else {
            arguments.extend(["--graph".into(), OsString::from(&self.template)]);
        }
        if let Some(runtime) = &self.runtime_config {
            arguments.extend(["--runtime-config".into(), runtime.as_os_str().to_owned()]);
        }
        if let Some(runtime) = &self.uniform_runtime_config {
            arguments.extend([
                "--uniform-runtime-config".into(),
                runtime.as_os_str().to_owned(),
            ]);
        }
        arguments.extend(self.run_args.iter().map(OsString::from));
        arguments
    }
}

/// Executes the judge graph as a real local `zeroshot run` in private scratch.
pub struct LocalJudgeGraphExecutor<B> {
    backend: Arc<B>,
    run: JudgeRunConfig,
    workspace: PathBuf,
}

impl<B> LocalJudgeGraphExecutor<B> {
    /// `workspace` is the live checkout; judge scratch must stay outside it.
    #[must_use]
    pub fn new(backend: Arc<B>, run: JudgeRunConfig, workspace: impl Into<PathBuf>) -> Self {
        Self {
            backend,
            run,
            workspace: workspace.into(),
        }
    }
}

#[async_trait]
impl<B: NativeV2CliBackend> JudgeGraphExecutor for LocalJudgeGraphExecutor<B> {
    async fn execute(&self, template: &JudgeTemplate, input: Value) -> Result<Value, EvalError> {
        let checkout = input
            .get("checkout")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|checkout| !checkout.is_empty())
            .ok_or_else(|| {
                EvalError::Judge(
                    "judge requires a captured checkout; no delivered checkout was available for this sample"
                        .to_owned(),
                )
            })?;
        let checkout = std::fs::canonicalize(checkout).map_err(|error| {
            EvalError::Judge(format!(
                "judge checkout {checkout:?} is unavailable: {error}"
            ))
        })?;
        if !checkout.is_dir() {
            return Err(EvalError::Judge(format!(
                "judge checkout {} is not a directory",
                checkout.display()
            )));
        }
        let input = judge_input_for_template(template, input)?;
        let identity = uuid::Uuid::now_v7().simple().to_string();
        let scratch = tempfile::Builder::new()
            .prefix("zeroshot-judge-")
            .tempdir()
            .map_err(|error| EvalError::io(std::env::temp_dir(), error))?;
        let inputs = tempfile::Builder::new()
            .prefix("zeroshot-judge-input-")
            .tempdir()
            .map_err(|error| EvalError::io(std::env::temp_dir(), error))?;
        let canonical_scratch = std::fs::canonicalize(scratch.path())
            .map_err(|error| EvalError::io(scratch.path(), error))?;
        let live_workspace = std::fs::canonicalize(&self.workspace)
            .map_err(|error| EvalError::io(&self.workspace, error))?;
        if canonical_scratch.starts_with(&live_workspace) {
            return Err(EvalError::Command(format!(
                "judge scratch {} is inside the live workspace {}",
                canonical_scratch.display(),
                live_workspace.display()
            )));
        }
        let input_path = inputs.path().join("judge-input.json");
        std::fs::write(&input_path, serde_json::to_vec(&input)?)
            .map_err(|error| EvalError::io(&input_path, error))?;
        let run = run_command(self.run.arguments(&input_path, &checkout), "judge")?;
        let request =
            crate::native_v2_cli::execution::prepare_run_submission(&run, self.backend.as_ref())
                .await
                .map_err(|error| {
                    EvalError::Command(format!("judge run was not admitted: {error}"))
                })?;
        let runner = LocalRunner::new(Arc::clone(&self.backend));
        let artifact = run_sample(
            &runner,
            BackendSampleRequest {
                sample_id: format!("eval-judge-{identity}"),
                concurrency: 1,
                request,
            },
            scratch.path(),
            |_, _| None,
        )
        .await
        .map_err(|error| EvalError::Runner(error.to_string()))?;
        if let Some(error) = artifact.capture_error.as_deref() {
            return Err(EvalError::Judge(format!(
                "judge graph capture failed: {error}"
            )));
        }
        if let Some(output) = artifact.outcome.output {
            return Ok(output);
        }
        let status = format!("{:?}", artifact.outcome.status);
        let reason = artifact
            .outcome
            .failure_reason
            .as_deref()
            .unwrap_or("no terminal output was recorded");
        Err(EvalError::Judge(format!(
            "judge graph run finished as {status}: {reason}"
        )))
    }
}
