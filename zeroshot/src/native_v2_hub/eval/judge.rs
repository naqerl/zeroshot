//! The judge protocol and the host commands evals run (checks and `cmd:` judges).
//!
//! A judge receives one `zeroshot.hub.eval.judge-request/v1` document: on stdin for a `cmd:`
//! judge, as the `--input` file of a `zeroshot:` judge's run. It answers with one JSON object
//! `{"score": NUMBER, "verdict": STRING?, "rationale": STRING?}`: on stdout (the last JSON
//! object line wins, so logs before it are fine) or as the judge run's terminal output.
//! Absolute scores are in 0..=1; pairwise scores in -1..=1, positive when the candidate beats
//! the baseline. A missing or out-of-range score is recorded as a judge error, never guessed.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use super::{Case, EvalRun, JudgeMode, Sample, Score, Verdict};

pub(crate) const REQUEST_SCHEMA: &str = "zeroshot.hub.eval.judge-request/v1";
/// Longest a check or `cmd:` judge may run.
pub(crate) const COMMAND_TIMEOUT: Duration = Duration::from_secs(600);
/// Output kept from a check, and the longest rationale kept from a judge.
pub(crate) const TAIL_BYTES: usize = 4096;
const RATIONALE_BYTES: usize = 8192;

/// What the judge sees of one sample.
pub(crate) fn sample_view(sample: &Sample, work_dir: &Path) -> Value {
    json!({
        "variant": sample.variant,
        "repeat": sample.repeat,
        "jobId": sample.job_id,
        "outcome": sample.outcome,
        "checks": sample.checks,
        "workDir": work_dir,
        "status": work_dir.join("status.json"),
    })
}

/// The judge request for a candidate sample view, with the baseline sample view of a pairwise
/// judge.
pub(crate) fn request(
    run: &EvalRun,
    case: &Case,
    files: &Value,
    (candidate, baseline): (Value, Option<Value>),
) -> Value {
    let mode = run.judge_mode().unwrap_or_default();
    json!({
        "schema": REQUEST_SCHEMA,
        "mode": match mode {
            JudgeMode::Absolute => "absolute",
            JudgeMode::Pairwise => "pairwise",
        },
        "runId": run.run_id,
        "case": {
            "caseId": case.case_id,
            "name": case.name,
            "title": case.title,
            "labels": case.labels,
            "pinnedRevision": case.pinned_revision,
            "files": files,
        },
        "candidate": candidate,
        "baseline": baseline,
        "reply": match mode {
            JudgeMode::Absolute => "one JSON object {\"score\": 0..1, \"verdict\": \"pass\"|\"fail\"|..., \"rationale\": \"...\"}",
            JudgeMode::Pairwise => "one JSON object {\"score\": -1..1 (positive: candidate is better than baseline), \"verdict\": \"better\"|\"worse\"|\"tie\"|..., \"rationale\": \"...\"}",
        },
    })
}

/// Parse a judge answer: the whole text as JSON, else the last line that is a JSON object.
pub(crate) fn parse_reply(text: &str, mode: JudgeMode) -> Verdict {
    let document = serde_json::from_str::<Value>(text.trim())
        .ok()
        .filter(Value::is_object)
        .or_else(|| {
            text.lines()
                .rev()
                .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
                .find(Value::is_object)
        });
    match document {
        Some(document) => parse_verdict(&document, mode),
        None => Verdict {
            error: Some("the judge answered no JSON object".to_owned()),
            ..Verdict::default()
        },
    }
}

/// Validate one verdict object against the mode's score range.
pub(crate) fn parse_verdict(document: &Value, mode: JudgeMode) -> Verdict {
    let verdict = document
        .get("verdict")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let rationale = document
        .get("rationale")
        .and_then(Value::as_str)
        .map(|text| truncate(text, RATIONALE_BYTES));
    let (low, high) = match mode {
        JudgeMode::Absolute => (0.0, 1.0),
        JudgeMode::Pairwise => (-1.0, 1.0),
    };
    let error = match document.get("score").and_then(Value::as_f64) {
        None => Some("the judge answer has no numeric score".to_owned()),
        Some(score) if !(low..=high).contains(&score) => {
            Some(format!("the judge score {score} is outside {low}..={high}"))
        }
        Some(_) => None,
    };
    Verdict {
        score: if error.is_none() {
            document.get("score").and_then(Value::as_f64).map(Score)
        } else {
            None
        },
        verdict,
        rationale,
        against: None,
        error,
    }
}

pub(crate) fn truncate(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// The tail of `text`, at most `limit` bytes.
pub(crate) fn tail(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    let mut start = text.len() - limit;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    format!("…{}", &text[start..])
}

/// The result of one host command.
pub(crate) struct Finished {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// One host command run for an eval: `program` with `arguments`, in `cwd`, with `env`, and
/// `stdin` written to its standard input. Times out after [`COMMAND_TIMEOUT`].
pub(crate) struct HostCommand<'a> {
    pub program: &'a Path,
    pub arguments: &'a [&'a str],
    pub cwd: &'a Path,
    pub env: &'a [(String, String)],
}

const SPAWN_BUSY_RETRIES: u32 = 10;

impl HostCommand<'_> {
    /// Start the program, retrying briefly while it is busy: a program just written can stay
    /// open for writing in a child another thread is forking, which makes exec fail with
    /// `ETXTBSY` for a moment.
    async fn spawn(&self) -> std::io::Result<tokio::process::Child> {
        let mut attempts = 0;
        loop {
            let spawned = Command::new(self.program)
                .args(self.arguments)
                .current_dir(self.cwd)
                .envs(self.env.iter().map(|(key, value)| (key, value)))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn();
            match spawned {
                Err(error)
                    if error.kind() == std::io::ErrorKind::ExecutableFileBusy
                        && attempts < SPAWN_BUSY_RETRIES =>
                {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                spawned => return spawned,
            }
        }
    }

    pub(crate) async fn run(&self, stdin: &[u8], timeout: Duration) -> Finished {
        let spawned = self.spawn().await;
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) => {
                return Finished {
                    exit_code: None,
                    stdout: String::new(),
                    stderr: format!("{} could not start: {error}", self.program.display()),
                };
            }
        };
        if let Some(mut input) = child.stdin.take() {
            let _ = input.write_all(stdin).await;
            drop(input);
        }
        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(Ok(output)) => Finished {
                exit_code: output.status.code(),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            },
            Ok(Err(error)) => Finished {
                exit_code: None,
                stdout: String::new(),
                stderr: error.to_string(),
            },
            Err(_) => Finished {
                exit_code: None,
                stdout: String::new(),
                stderr: format!("timed out after {}s", timeout.as_secs()),
            },
        }
    }
}
