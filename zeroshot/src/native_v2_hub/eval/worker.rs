//! The eval worker: claims ready work (finished samples to measure, samples to judge), runs
//! checks and judges outside the state lock, and records results. The daemon spawns each
//! claimed piece so a slow check or judge never blocks dispatching or other samples.

use std::collections::BTreeMap;
use std::path::Path;

use openengine_cluster_protocol::NODE_RUNTIME_LANES_KIND;
use serde_json::{Value, json};

use super::super::clock::now_ms;
use super::super::error::HubError;
use super::super::model::{HubState, Job, JobState};
use super::super::service::{HubService, create_private_directory, replace_private};
use super::judge::{self, COMMAND_TIMEOUT, Ended, Finished, HostCommand, TAIL_BYTES};
use super::service::{case_view, not_found};
use super::{
    Case, Check, CheckResult, EvalRun, JudgeKind, JudgeMode, Outcome, ROLE_LABEL, Sample,
    SampleState, Verdict, sample_job_id,
};

/// One unit of eval work, claimed by the worker while it runs.
#[derive(Clone, Debug)]
pub(crate) enum Work {
    Measure {
        run_id: String,
        sample: String,
    },
    Judge {
        run_id: String,
        sample: String,
    },
    Settle {
        run_id: String,
        sample: String,
        verdict: Verdict,
    },
}

impl Work {
    fn claim_key(&self) -> String {
        match self {
            Self::Measure { run_id, sample } => format!("measure:{run_id}:{sample}"),
            Self::Judge { run_id, sample } | Self::Settle { run_id, sample, .. } => {
                format!("judge:{run_id}:{sample}")
            }
        }
    }
}

impl HubService {
    /// Claim the eval work that is ready now. Claimed work must be passed to
    /// [`Self::eval_execute`], which releases the claim.
    pub(crate) async fn eval_claim(&self) -> Result<Vec<Work>, HubError> {
        let state = self.snapshot().await;
        let mut ready = Vec::new();
        let mut vanished = Vec::new();
        for run in state.evals.runs.iter().filter(|run| !run.complete()) {
            for sample in &run.samples {
                let job = state.jobs.iter().find(|job| job.job_id == sample.job_id);
                let (run_id, key) = (run.run_id.clone(), sample.key());
                match sample.state {
                    SampleState::Pending => match job {
                        None => vanished.push((run_id, key)),
                        Some(job) if job.state.is_terminal() => {
                            ready.push(Work::Measure {
                                run_id,
                                sample: key,
                            });
                        }
                        Some(_) => {}
                    },
                    SampleState::Measured => {
                        if let Some(work) = judge_work(run, sample, &state) {
                            ready.push(work);
                        }
                    }
                    SampleState::Judged | SampleState::Failed => {}
                }
            }
        }
        if !vanished.is_empty() {
            self.mutate(|state| {
                for (run_id, key) in &vanished {
                    if let Some(sample) = find_sample(state, run_id, key) {
                        sample.state = SampleState::Failed;
                        sample.error = Some(format!("hub job {} no longer exists", sample.job_id));
                    }
                }
                Ok(())
            })
            .await?;
        }
        let mut claimed = self
            .eval_claims()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(ready
            .into_iter()
            .filter(|work| claimed.insert(work.claim_key()))
            .collect())
    }

    /// Do one claimed piece of work, record its result, and release the claim.
    pub(crate) async fn eval_execute(&self, work: Work) -> Result<(), HubError> {
        let key = work.claim_key();
        let result = match work {
            Work::Measure { run_id, sample } => self.measure(&run_id, &sample).await,
            Work::Judge { run_id, sample } => self.judge(&run_id, &sample).await,
            Work::Settle {
                run_id,
                sample,
                verdict,
            } => self.settle(&run_id, &sample, verdict).await,
        };
        self.eval_claims()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
        result
    }

    /// Claim and finish all ready eval work, concurrently. Returns how much work ran.
    #[cfg(test)]
    pub(crate) async fn evaluate(&self) -> Result<usize, HubError> {
        let work = self.eval_claim().await?;
        let count = work.len();
        let results =
            futures_util::future::join_all(work.into_iter().map(|work| self.eval_execute(work)))
                .await;
        results.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(count)
    }

    async fn locate(
        &self,
        run_id: &str,
        key: &str,
    ) -> Result<(EvalRun, Sample, Option<Job>, Case), HubError> {
        let state = self.snapshot().await;
        let run = state
            .evals
            .run(run_id)
            .cloned()
            .ok_or_else(|| not_found(&format!("eval run {run_id} does not exist")))?;
        let sample = run
            .samples
            .iter()
            .find(|sample| sample.key() == key)
            .cloned()
            .ok_or_else(|| not_found(&format!("eval sample {key} does not exist")))?;
        let job = state
            .jobs
            .iter()
            .find(|job| job.job_id == sample.job_id)
            .cloned();
        let case =
            state.evals.case(&sample.case_id).cloned().ok_or_else(|| {
                not_found(&format!("eval case {} does not exist", sample.case_id))
            })?;
        Ok((run, sample, job, case))
    }

    fn environment(
        &self,
        (run, sample): (&EvalRun, &Sample),
        job: Option<&Job>,
        work_dir: &Path,
    ) -> Vec<(String, String)> {
        let mut env = vec![
            ("ZEROSHOT_EVAL_RUN".to_owned(), run.run_id.clone()),
            ("ZEROSHOT_EVAL_CASE".to_owned(), sample.case_id.clone()),
            ("ZEROSHOT_EVAL_VARIANT".to_owned(), sample.variant.clone()),
            ("ZEROSHOT_EVAL_REPEAT".to_owned(), sample.repeat.to_string()),
            ("ZEROSHOT_EVAL_JOB".to_owned(), sample.job_id.clone()),
            (
                "ZEROSHOT_EVAL_WORKDIR".to_owned(),
                work_dir.to_string_lossy().into_owned(),
            ),
            (
                "ZEROSHOT_EVAL_STATUS".to_owned(),
                work_dir.join("status.json").to_string_lossy().into_owned(),
            ),
            (
                "ZEROSHOT_EVAL_INPUTS".to_owned(),
                self.store()
                    .directory()
                    .join("spool")
                    .join(&sample.job_id)
                    .to_string_lossy()
                    .into_owned(),
            ),
        ];
        if let Some(outcome) = &sample.outcome {
            env.push(("ZEROSHOT_EVAL_OUTCOME".to_owned(), outcome.status.clone()));
        }
        if let Some(job) = job {
            env.extend(
                job.target
                    .clone()
                    .map(|target| ("ZEROSHOT_EVAL_TARGET".to_owned(), target)),
            );
            env.extend(
                job.run_id
                    .clone()
                    .map(|run_id| ("ZEROSHOT_EVAL_RUN_ID".to_owned(), run_id)),
            );
        }
        if let Ok(executable) = super::super::driver::hub_executable() {
            env.push((
                "ZEROSHOT_HUB_EXECUTABLE".to_owned(),
                executable.to_string_lossy().into_owned(),
            ));
        }
        env
    }

    /// Record a finished sample's outcome and run its checks in its work folder.
    async fn measure(&self, run_id: &str, key: &str) -> Result<(), HubError> {
        let (run, mut sample, job, case) = self.locate(run_id, key).await?;
        let Some(job) = job else {
            return Ok(());
        };
        let outcome = outcome(&job);
        let status = match (&job.target, &job.run_id) {
            (Some(target), Some(run_id)) => self
                .driver()
                .status(target, run_id)
                .await
                .unwrap_or(Value::Null),
            _ => Value::Null,
        };
        let work_dir = self.work_directory(run_id, &sample);
        create_private_directory(&work_dir)?;
        let now = now_ms();
        replace_private(&work_dir.join("status.json"), &pretty(&status))?;
        replace_private(&work_dir.join("job.json"), &pretty(&job.view(now)))?;
        replace_private(&work_dir.join("case.json"), &pretty(&case_view(&case)))?;
        sample.outcome = Some(outcome.clone());
        replace_private(&work_dir.join("outcome.json"), &pretty(&json!(outcome)))?;
        let env = self.environment((&run, &sample), Some(&job), &work_dir);
        let mut checks = Vec::new();
        for Check { name, command } in &run.checks {
            let finished = HostCommand {
                program: Path::new("sh"),
                arguments: &["-c", command],
                cwd: &work_dir,
                env: &env,
            }
            .run(&[], COMMAND_TIMEOUT)
            .await;
            checks.push(check_result(name, &finished));
        }
        self.mutate(|state| {
            if let Some(stored) = find_sample(state, run_id, key) {
                if stored.state == SampleState::Pending {
                    stored.outcome = Some(outcome.clone());
                    stored.checks.clone_from(&checks);
                    stored.state = SampleState::Measured;
                }
            }
            Ok(())
        })
        .await
    }

    async fn settle(&self, run_id: &str, key: &str, verdict: Verdict) -> Result<(), HubError> {
        self.mutate(|state| {
            if let Some(stored) = find_sample(state, run_id, key) {
                if stored.state == SampleState::Measured {
                    stored.judge = Some(verdict.clone());
                    stored.state = SampleState::Judged;
                }
            }
            Ok(())
        })
        .await
    }

    /// Run a `cmd:` judge, or start (once) the hub job of a `zeroshot:` judge.
    async fn judge(&self, run_id: &str, key: &str) -> Result<(), HubError> {
        let (run, sample, job, case) = self.locate(run_id, key).await?;
        let Some(judge) = run.judge.clone() else {
            return Ok(());
        };
        let blobs = self.blobs();
        let state = self.snapshot().await;
        let baseline = run.baseline_for(&sample).cloned();
        let work_dir = self.work_directory(run_id, &sample);
        create_private_directory(&work_dir)?;
        let case_files = self.eval_directory().join("cases").join(&case.case_id);
        let mut files = serde_json::Map::new();
        for (flag, blob) in &case.files {
            files.insert(flag.clone(), json!(blobs.materialize(blob, &case_files)?));
        }
        let candidate = judge::sample_view(&sample, &work_dir);
        let baseline_view = if judge.mode == JudgeMode::Pairwise {
            baseline.as_ref().map(|baseline| {
                judge::sample_view(baseline, &self.work_directory(run_id, baseline))
            })
        } else {
            None
        };
        let request = judge::request(
            &run,
            &case,
            &Value::Object(files),
            (candidate, baseline_view),
        );
        let against = baseline
            .as_ref()
            .filter(|_| judge.mode == JudgeMode::Pairwise)
            .map(|baseline| baseline.job_id.clone());
        match judge.kind {
            JudgeKind::Cmd => {
                let folder = self
                    .eval_directory()
                    .join("programs")
                    .join(&judge.program.sha256);
                let program = blobs.materialize(&judge.program, &folder)?;
                make_executable(&program)?;
                replace_private(&work_dir.join("judge-request.json"), &pretty(&request))?;
                let env = self.environment((&run, &sample), job.as_ref(), &work_dir);
                let finished = HostCommand {
                    program: &program,
                    arguments: &[],
                    cwd: &work_dir,
                    env: &env,
                }
                .run(
                    &serde_json::to_vec(&request).unwrap_or_default(),
                    COMMAND_TIMEOUT,
                )
                .await;
                let mut verdict = if finished.ended == Ended::Exited(Some(0)) {
                    judge::parse_reply(&finished.stdout, judge.mode)
                } else {
                    Verdict {
                        error: Some(format!(
                            "the judge {}: {}",
                            finished.ended.describe(),
                            judge::tail(finished.stderr.trim(), 1024)
                        )),
                        ..Verdict::default()
                    }
                };
                verdict.against = against;
                replace_private(
                    &work_dir.join("judge-verdict.json"),
                    &pretty(&json!(verdict)),
                )?;
                self.settle(run_id, key, verdict).await
            }
            JudgeKind::Zeroshot => {
                let judge_job_id = sample_job_id(
                    run_id,
                    &sample.case_id,
                    &format!("{}#judge", sample.variant),
                    sample.repeat,
                );
                if state.jobs.iter().any(|job| job.job_id == judge_job_id) {
                    return self
                        .mutate(|state| {
                            if let Some(stored) = find_sample(state, run_id, key) {
                                stored.judge_job = Some(judge_job_id.clone());
                            }
                            Ok(())
                        })
                        .await;
                }
                let Some(candidate_job) = job else {
                    return Ok(());
                };
                let folder = self.store().directory().join("spool").join(&judge_job_id);
                let graph = blobs.materialize(&judge.program, &folder)?;
                let input = folder.join("judge-request.json");
                replace_private(&input, &pretty(&request))?;
                let mut run_args = vec![
                    "--title".to_owned(),
                    format!(
                        "judge {} [{} #{}]",
                        case.title, sample.variant, sample.repeat
                    ),
                    "--graph".to_owned(),
                    graph.to_string_lossy().into_owned(),
                    "--input".to_owned(),
                    input.to_string_lossy().into_owned(),
                ];
                if let Some(runtime) = &judge.runtime {
                    run_args.push("--uniform-runtime-config".to_owned());
                    run_args.push(
                        blobs
                            .materialize(runtime, &folder)?
                            .to_string_lossy()
                            .into_owned(),
                    );
                }
                for pair in candidate_job.run_args.windows(2) {
                    if [
                        "--repository",
                        "--branch",
                        "--revision",
                        "--source-connection",
                        "--cwd",
                    ]
                    .contains(&pair[0].as_str())
                    {
                        run_args.extend(pair.iter().cloned());
                    }
                }
                let mut requirements = candidate_job.requirements.clone();
                requirements
                    .capabilities
                    .retain(|capability| capability != NODE_RUNTIME_LANES_KIND);
                let mut labels = BTreeMap::new();
                for key in [
                    "eval.run",
                    "eval.case",
                    "eval.variant",
                    "eval.repeat",
                    "eval.suite",
                    "project",
                ] {
                    if let Some(value) = candidate_job.labels.get(key) {
                        labels.insert(key.to_owned(), value.clone());
                    }
                }
                labels.insert(ROLE_LABEL.to_owned(), "judge".to_owned());
                let judge_job = Job {
                    job_id: judge_job_id.clone(),
                    state: JobState::Queued,
                    title: run_args[1].clone(),
                    run_args,
                    labels,
                    requirements,
                    models: Vec::new(),
                    submitted_at_ms: now_ms(),
                    attempts: 0,
                    target: None,
                    run_id: None,
                    dispatched_at_ms: None,
                    finished_at_ms: None,
                    failure_reason: None,
                    token_usage: None,
                    error: None,
                    schedule_id: None,
                    output: None,
                    submission_key: None,
                    not_before_ms: None,
                    expires_at_ms: None,
                    ..candidate_job
                };
                self.mutate(|state| {
                    if !state.jobs.iter().any(|job| job.job_id == judge_job.job_id) {
                        state.jobs.push(judge_job.clone());
                    }
                    if let Some(stored) = find_sample(state, run_id, key) {
                        stored.judge_job = Some(judge_job.job_id.clone());
                    }
                    Ok(())
                })
                .await
            }
        }
    }
}

fn find_sample<'a>(state: &'a mut HubState, run_id: &str, key: &str) -> Option<&'a mut Sample> {
    state
        .evals
        .runs
        .iter_mut()
        .find(|run| run.run_id == run_id)?
        .samples
        .iter_mut()
        .find(|sample| sample.key() == key)
}

/// What judging a measured sample needs now, if anything.
fn judge_work(run: &EvalRun, sample: &Sample, state: &HubState) -> Option<Work> {
    let judge = run.judge.as_ref()?;
    let (run_id, key) = (run.run_id.clone(), sample.key());
    let settle = |verdict: Verdict| {
        Some(Work::Settle {
            run_id: run.run_id.clone(),
            sample: sample.key(),
            verdict,
        })
    };
    if judge.mode == JudgeMode::Pairwise {
        if run.baseline_variant() == Some(sample.variant.as_str()) {
            return settle(Verdict {
                verdict: Some("reference".to_owned()),
                ..Verdict::default()
            });
        }
        match run.baseline_for(sample) {
            None => {
                return settle(Verdict {
                    error: Some("no baseline sample to compare with".to_owned()),
                    ..Verdict::default()
                });
            }
            Some(baseline) if baseline.state == SampleState::Failed => {
                return settle(Verdict {
                    error: Some(format!(
                        "baseline sample {} could not be measured",
                        baseline.job_id
                    )),
                    against: Some(baseline.job_id.clone()),
                    ..Verdict::default()
                });
            }
            Some(baseline) if baseline.state == SampleState::Pending => return None,
            Some(_) => {}
        }
    }
    match (&judge.kind, &sample.judge_job) {
        (JudgeKind::Zeroshot, Some(judge_job)) => {
            let job = state.jobs.iter().find(|job| &job.job_id == judge_job)?;
            if !job.state.is_terminal() {
                return None;
            }
            let mut verdict = match (&job.state, &job.output) {
                (JobState::Succeeded, Some(Value::String(text))) => {
                    judge::parse_reply(text, judge.mode)
                }
                (JobState::Succeeded, Some(output)) => judge::parse_verdict(output, judge.mode),
                (JobState::Succeeded, None) => Verdict {
                    error: Some(format!("judge job {judge_job} finished without output")),
                    ..Verdict::default()
                },
                (_, _) => Verdict {
                    error: Some(format!(
                        "judge job {judge_job} finished {}{}",
                        job.display_state(now_ms()),
                        job.failure_reason
                            .as_ref()
                            .map(|reason| format!(": {reason}"))
                            .unwrap_or_default()
                    )),
                    ..Verdict::default()
                },
            };
            if judge.mode == JudgeMode::Pairwise {
                verdict.against = run
                    .baseline_for(sample)
                    .map(|baseline| baseline.job_id.clone());
            }
            settle(verdict)
        }
        _ => Some(Work::Judge {
            run_id,
            sample: key,
        }),
    }
}

fn outcome(job: &Job) -> Outcome {
    let ready = job.not_before_ms.unwrap_or(0).max(job.submitted_at_ms);
    Outcome {
        status: job.display_state(now_ms()).to_owned(),
        failure_reason: job.failure_reason.clone().or_else(|| job.error.clone()),
        target: job.target.clone(),
        run_id: job.run_id.clone(),
        queue_wait_ms: job
            .dispatched_at_ms
            .map(|dispatched| dispatched.saturating_sub(ready)),
        duration_ms: job
            .dispatched_at_ms
            .zip(job.finished_at_ms)
            .map(|(dispatched, finished)| finished.saturating_sub(dispatched)),
        token_usage: job.token_usage.clone(),
        model: job.model_key(),
    }
}

fn pretty(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap_or_default();
    bytes.push(b'\n');
    bytes
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<(), HubError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(|error| {
        HubError::storage(
            &format!("{} could not be made executable", path.display()),
            error,
        )
    })
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<(), HubError> {
    Ok(())
}

/// A check's result. It passes only when it exits 0; a check the hub could not run records why
/// in `error`, so it does not count against the candidate.
pub(crate) fn check_result(name: &str, finished: &Finished) -> CheckResult {
    let mut output = format!("{}{}", finished.stdout, finished.stderr);
    if !matches!(finished.ended, Ended::Exited(_)) {
        output.push_str(&finished.ended.describe());
    }
    CheckResult {
        name: name.to_owned(),
        passed: finished.ended == Ended::Exited(Some(0)),
        exit_code: finished.exit_code(),
        output: judge::tail(&output, TAIL_BYTES),
        error: matches!(finished.ended, Ended::NotStarted(_)).then(|| finished.ended.describe()),
    }
}
