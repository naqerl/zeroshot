//! Execute a judge over captured samples and persist its per-criterion results.
//!
//! The runner never re-executes an eval arm. It reads the stored `patch.diff` (or the slice-1
//! `patches/<sample>.patch`) plus the captured `sample.json`, asks the judge graph for a pass/fail
//! verdict per acceptance trait, and writes the verdicts and reasons back into the sample records
//! before rebuilding the report.

use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    EvalCriterion, EvalData, EvalError, EvalId, EvalSample, EvalStore, Judge, JudgeRequest,
    JudgeResult, JUDGE_TEMPLATE_NAME, SampleStatus, SampleTiming, TokenUsage, cli::recompute,
    judge_template::resolve_judge_template,
};

/// Per-run judge overrides. Manifest values remain the defaults.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JudgeOptions {
    /// Run-level judge runner override: `synthetic` (no model call) or `local` (`zeroshot run`).
    pub runner: Option<String>,
    /// Run-level graph template override.
    pub template: Option<String>,
    /// Run-level scorer override used when rebuilding the report.
    pub scorer: Option<String>,
    /// Executor label recorded on judged samples (`fixture`, `graph`, ...).
    pub executor: Option<String>,
}

impl JudgeOptions {
    #[must_use]
    pub fn with_runner(mut self, runner: impl Into<String>) -> Self {
        self.runner = Some(runner.into());
        self
    }

    #[must_use]
    pub fn with_template(mut self, template: impl Into<String>) -> Self {
        self.template = Some(template.into());
        self
    }

    #[must_use]
    pub fn with_scorer(mut self, scorer: impl Into<String>) -> Self {
        self.scorer = Some(scorer.into());
        self
    }
}

/// One captured sample read from a directory holding `sample.json` and `patch.diff`.
#[derive(Clone, Debug, PartialEq)]
pub struct CapturedSampleInput {
    pub sample: EvalSample,
    /// Absent for a failed sample: slice-2 writes `sample.json` but only captures patches for
    /// successful outcomes.
    pub patch: Option<String>,
    pub sample_json: Value,
    /// Read-only delivered tree captured by slice 2, when the artifact records one.
    pub checkout: Option<String>,
}

/// One judged sample before it is written back to an eval store.
#[derive(Clone, Debug, PartialEq)]
pub struct JudgedSample {
    pub sample: EvalSample,
    pub template: Option<String>,
    pub result: JudgeResult,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JudgedSampleSummary {
    pub sample_id: String,
    pub arm_id: String,
    pub run_index: u32,
    pub criteria: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JudgeRunReport {
    pub eval_id: EvalId,
    pub template: String,
    pub judged: Vec<JudgedSampleSummary>,
    /// Samples not judged because they did not succeed or lacked captured evidence.
    pub skipped: Vec<String>,
}

/// Judge every succeeded sample in a stored eval, then rebuild and persist its report.
pub async fn judge_eval<J: Judge>(
    store: &EvalStore,
    data: &mut EvalData,
    judge: &J,
    options: JudgeOptions,
) -> Result<JudgeRunReport, EvalError> {
    let template = selected_template(data, options.template.as_deref());
    // Resolve before spending model calls so an unknown name or malformed custom graph fails fast.
    let _ = resolve_judge_template(&template)?;
    let criteria = data.manifest.criteria.clone();
    let mut judged = Vec::new();
    let mut skipped = Vec::new();
    // Stage every judgement before touching the store so a later malformed response cannot leave
    // earlier samples updated against a stale or absent report.
    let mut staged = Vec::new();
    for index in 0..data.samples.len() {
        let sample = data.samples[index].clone();
        if sample.status == SampleStatus::Succeeded && sample.patch.is_none() {
            return Err(EvalError::Judge(format!(
                "sample {:?} succeeded without a captured patch; refusing to judge missing evidence",
                sample.id
            )));
        }
        if sample.status != SampleStatus::Succeeded {
            skipped.push(sample.id.clone());
            staged.push((index, sample, None));
            continue;
        }
        let arm = data.manifest.arm(&sample.arm_id).cloned().ok_or_else(|| {
            EvalError::Command(format!(
                "sample {:?} names unknown arm {:?}",
                sample.id, sample.arm_id
            ))
        })?;
        let patch = store.read_patch(
            &data.id,
            &sample
                .patch
                .as_ref()
                .map(|patch| patch.path.clone())
                .unwrap_or_default(),
        )?;
        let sample_json = serde_json::to_value(&sample)?;
        let mut request = JudgeRequest::new(sample.clone(), criteria.clone(), patch)
            .with_task(arm.prompt.clone())
            .with_sample_json(sample_json)
            .with_template(template.clone());
        if let Some(checkout) = sample.judge_checkout.clone() {
            request = request.with_checkout(checkout);
        }
        let result = judge.judge(request).await?;
        validate_judgements(&result)?;
        validate_coverage(&result, &criteria)?;
        let mut updated = sample;
        updated.criteria = result.criteria();
        updated.reasoning = result.reasoning();
        updated.judge_template = Some(template.clone());
        updated.judge_executor = options.executor.clone();
        let summary = JudgedSampleSummary {
            sample_id: updated.id.clone(),
            arm_id: updated.arm_id.clone(),
            run_index: updated.run_index,
            criteria: criteria
                .iter()
                .map(|criterion| criterion.id.clone())
                .collect(),
        };
        staged.push((index, updated, Some(summary)));
    }
    for (index, updated, summary) in staged {
        store.write_sample(&data.id, &updated)?;
        data.samples[index] = updated;
        if let Some(summary) = summary {
            judged.push(summary);
        }
    }
    let scorer = options
        .scorer
        .as_deref()
        .unwrap_or(&data.manifest.options.scorer);
    let capacity = data.report.as_ref().and_then(|report| report.capacity);
    let report = recompute(data, scorer, capacity)?;
    store.write_report(&data.id, &report)?;
    store.write_report_markdown(&data.id, &report)?;
    data.report = Some(report);
    Ok(JudgeRunReport {
        eval_id: data.id.clone(),
        template,
        judged,
        skipped,
    })
}

/// Judge a set of capture directories without an eval store. Each directory must contain
/// `sample.json` and `patch.diff`; `task` is the prompt the samples implemented.
pub async fn judge_captured_samples<J: Judge>(
    judge: &J,
    criteria: &[EvalCriterion],
    samples: Vec<CapturedSampleInput>,
    options: JudgeOptions,
) -> Result<Vec<JudgedSample>, EvalError> {
    judge_captured_samples_for_task(judge, (criteria, ""), samples, options).await
}

/// [`judge_captured_samples`] with the task prompt the judge graph receives.
pub async fn judge_captured_samples_for_task<J: Judge>(
    judge: &J,
    (criteria, task): (&[EvalCriterion], &str),
    samples: Vec<CapturedSampleInput>,
    options: JudgeOptions,
) -> Result<Vec<JudgedSample>, EvalError> {
    let template = options
        .template
        .clone()
        .unwrap_or_else(|| JUDGE_TEMPLATE_NAME.to_owned());
    let _ = resolve_judge_template(&template)?;
    let mut judged = Vec::with_capacity(samples.len());
    for captured in samples {
        let mut sample = captured.sample;
        if sample.status != SampleStatus::Succeeded {
            continue;
        }
        let Some(patch) = captured.patch else {
            return Err(EvalError::Judge(format!(
                "sample {:?} succeeded without a captured patch; refusing to judge missing evidence",
                sample.id
            )));
        };
        let mut request = JudgeRequest::new(sample.clone(), criteria.to_vec(), patch)
            .with_task(task)
            .with_sample_json(captured.sample_json)
            .with_template(template.clone());
        if let Some(checkout) = captured.checkout {
            request = request.with_checkout(checkout);
        }
        let result = judge.judge(request).await?;
        validate_judgements(&result)?;
        validate_coverage(&result, criteria)?;
        sample.criteria = result.criteria();
        sample.reasoning = result.reasoning();
        sample.judge_template = Some(template.clone());
        sample.judge_executor = options.executor.clone();
        judged.push(JudgedSample {
            sample,
            template: Some(template.clone()),
            result,
        });
    }
    Ok(judged)
}

/// Read `sample.json` and `patch.diff` from one capture directory. `arm_id` and `run_index` are
/// caller facts because slice 2's capture document intentionally stores only the sample identity.
pub fn captured_sample_from_directory(
    directory: &Path,
    arm_id: &str,
    run_index: u32,
) -> Result<CapturedSampleInput, EvalError> {
    let sample_path = directory.join("sample.json");
    let bytes = std::fs::read(&sample_path).map_err(|error| EvalError::io(&sample_path, error))?;
    let sample_json: Value = serde_json::from_slice(&bytes)?;
    // The public artifact is the preferred shape; older nested `outcome` documents fall back to
    // the conventional `patch.diff` path and their optional checkout metadata is unavailable.
    let artifact = super::SampleArtifact::read(directory).ok();
    let patch_relative = artifact
        .as_ref()
        .and_then(|artifact| artifact.captured.as_ref())
        .map_or_else(
            || std::path::PathBuf::from("patch.diff"),
            |captured| captured.patch.clone(),
        );
    let patch_path = capture_relative_path(directory, &patch_relative, "captured patch")?;
    let patch = match std::fs::read_to_string(&patch_path) {
        Ok(patch) => Some(patch),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(EvalError::io(&patch_path, error)),
    };
    let checkout = match artifact
        .as_ref()
        .and_then(|artifact| artifact.captured.as_ref())
        .and_then(|captured| captured.judge.as_ref())
    {
        Some(judge) => Some(
            capture_relative_path(directory, &judge.directory, "judge checkout")?
                .to_string_lossy()
                .into_owned(),
        ),
        None => None,
    };
    let mut sample = sample_from_capture(&sample_json, arm_id, run_index)?;
    if patch.is_none() {
        sample.patch = None;
    }
    sample.judge_checkout = checkout.clone();
    Ok(CapturedSampleInput {
        sample,
        patch,
        sample_json,
        checkout,
    })
}

pub(crate) fn capture_relative_path(
    directory: &Path,
    relative: &Path,
    label: &str,
) -> Result<std::path::PathBuf, EvalError> {
    if relative.is_absolute()
        || relative.components().any(|component| {
            !matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
    {
        return Err(EvalError::Command(format!(
            "{label} path {relative:?} must stay inside the capture directory"
        )));
    }
    let base = std::fs::canonicalize(directory).map_err(|error| EvalError::io(directory, error))?;
    let joined = base.join(relative);
    if joined.exists() {
        let canonical =
            std::fs::canonicalize(&joined).map_err(|error| EvalError::io(&joined, error))?;
        if !canonical.starts_with(&base) {
            return Err(EvalError::Command(format!(
                "{label} path {relative:?} escapes the capture directory"
            )));
        }
        return Ok(canonical);
    }
    Ok(joined)
}

fn selected_template(data: &EvalData, run_override: Option<&str>) -> String {
    run_override
        .filter(|template| !template.trim().is_empty())
        .or(data.manifest.options.judge_template.as_deref())
        .unwrap_or(JUDGE_TEMPLATE_NAME)
        .to_owned()
}

fn validate_judgements(result: &JudgeResult) -> Result<(), EvalError> {
    for (criterion, judgement) in &result.judgements {
        judgement
            .validate(criterion)
            .map_err(|error| EvalError::Judge(error.to_string()))?;
    }
    Ok(())
}

fn validate_coverage(result: &JudgeResult, criteria: &[EvalCriterion]) -> Result<(), EvalError> {
    let expected = criteria
        .iter()
        .map(|criterion| criterion.id.as_str())
        .collect::<BTreeSet<_>>();
    let actual = result
        .judgements
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if expected != actual {
        let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
        let extra = actual.difference(&expected).copied().collect::<Vec<_>>();
        return Err(EvalError::Judge(format!(
            "judgement criteria do not match the manifest; missing [{}], unexpected [{}]",
            missing.join(", "),
            extra.join(", ")
        )));
    }
    Ok(())
}

fn sample_from_capture(
    document: &Value,
    arm_id: &str,
    run_index: u32,
) -> Result<EvalSample, EvalError> {
    let sample_id = document
        .get("sampleId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| EvalError::Command("sample.json has no sampleId".to_owned()))?;
    super::manifest::validate_identifier("sample id", sample_id)?;
    // Slice 2 writes `SampleArtifact`, whose `outcome` fields are flattened into `sample.json`.
    // Older/private capture documents may still nest them under `outcome`; accept both shapes.
    let outcome = document
        .get("outcome")
        .and_then(Value::as_object)
        .or_else(|| document.as_object())
        .ok_or_else(|| EvalError::Command("sample.json has no outcome fields".to_owned()))?;
    let run_id = outcome
        .get("runId")
        .and_then(Value::as_str)
        .ok_or_else(|| EvalError::Command("sample.json has no outcome.runId".to_owned()))?;
    let status = match outcome.get("status").and_then(Value::as_str) {
        Some("succeeded") => SampleStatus::Succeeded,
        Some("failed") => SampleStatus::Failed,
        Some("skipped") => SampleStatus::Skipped,
        Some(other) => {
            return Err(EvalError::Command(format!(
                "sample.json has unsupported outcome.status {other:?}"
            )));
        }
        None => {
            return Err(EvalError::Command(
                "sample.json has no outcome.status".to_owned(),
            ));
        }
    };
    let timings = outcome.get("timings").and_then(Value::as_object);
    let timing = SampleTiming {
        started_at_ms: timings
            .and_then(|timings| timings.get("startedAtMs"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        wall_clock_ms: timings
            .and_then(|timings| timings.get("wallClockMs"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        queue_wait_ms: timings
            .and_then(|timings| timings.get("queueWaitMs"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
    };
    let tokens = outcome.get("tokenUsage").and_then(Value::as_object);
    let count = |key: &str| {
        tokens
            .and_then(|tokens| tokens.get(key))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    };
    let cache_read = count("cacheReadInputTokens");
    let cache_write = count("cacheCreationInputTokens");
    let tokens = TokenUsage {
        input: count("inputTokens"),
        output: count("outputTokens"),
        cache: cache_read.saturating_add(cache_write),
        cache_read,
        cache_write,
    };
    let cost_usd = match outcome.get("costUsd").or_else(|| document.get("costUsd")) {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_f64()
                .filter(|cost| cost.is_finite() && *cost >= 0.0)
                .ok_or_else(|| {
                    EvalError::Command(
                        "sample.json costUsd must be a finite nonnegative number".to_owned(),
                    )
                })?,
        ),
    };
    let criteria = outcome
        .get("criteria")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| EvalError::Command(format!("sample.json has invalid criteria: {error}")))?
        .unwrap_or_default();
    let reasoning = outcome
        .get("reasoning")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| EvalError::Command(format!("sample.json has invalid reasoning: {error}")))?
        .unwrap_or_default();
    Ok(EvalSample {
        id: sample_id.to_owned(),
        run_id: run_id.to_owned(),
        arm_id: arm_id.to_owned(),
        run_index,
        status,
        timing,
        tokens,
        cost_usd,
        delivered_revision: outcome
            .get("deliveredRevision")
            .and_then(Value::as_str)
            .map(str::to_owned),
        failure_reason: outcome
            .get("failureReason")
            .and_then(Value::as_str)
            .map(str::to_owned),
        patch: Some(super::PatchArtifact {
            path: "patch.diff".to_owned(),
            sha256: String::new(),
            bytes: 0,
        }),
        criteria,
        reasoning,
        judge_template: None,
        judge_checkout: None,
        judge_executor: None,
    })
}
