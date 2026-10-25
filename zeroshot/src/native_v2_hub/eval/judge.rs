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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStdin, Command};

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

/// How a host command ended. Only [`Ended::Exited`] says anything about the candidate: the
/// other kinds are the hub failing to run the command or the command failing to finish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Ended {
    /// The hub could not start the program, or lost track of it: a hub-side failure.
    NotStarted(String),
    /// It did not finish within the timeout and was killed.
    TimedOut(Duration),
    /// The hub could not deliver all of its input (it stopped reading or closed its standard
    /// input early), so whatever it answered was not about the whole request.
    IncompleteInput(String),
    /// It read all of its input and exited with this code (`None` when a signal ended it).
    Exited(Option<i32>),
}

impl Ended {
    /// A description for records and errors.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::NotStarted(reason) => reason.clone(),
            Self::TimedOut(after) => format!("timed out after {}s", after.as_secs()),
            Self::IncompleteInput(reason) => format!("did not read all of its input: {reason}"),
            Self::Exited(Some(code)) => format!("exited {code}"),
            Self::Exited(None) => "exited without a code".to_owned(),
        }
    }
}

/// The result of one host command: how it ended, and the last [`OUTPUT_LIMIT`] bytes of each
/// output stream.
pub(crate) struct Finished {
    pub ended: Ended,
    pub stdout: String,
    pub stderr: String,
}

impl Finished {
    fn without_output(ended: Ended) -> Self {
        Self {
            ended,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    /// The exit code of a command that read its whole input and exited.
    pub(crate) fn exit_code(&self) -> Option<i32> {
        match self.ended {
            Ended::Exited(code) => code,
            _ => None,
        }
    }
}

/// Bytes kept from each output stream of a host command; earlier output is dropped while it is
/// read, so a chatty command cannot exhaust the daemon's memory.
pub(crate) const OUTPUT_LIMIT: usize = 1 << 20;

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

    /// Run the command: its input is written and both output streams are read at the same
    /// time, all within `timeout`, so a command that never reads its input cannot block the
    /// hub. A command killed by the timeout is reaped before this returns.
    pub(crate) async fn run(&self, stdin: &[u8], timeout: Duration) -> Finished {
        let mut child = match self.spawn().await {
            Ok(child) => child,
            Err(error) => {
                return Finished::without_output(Ended::NotStarted(format!(
                    "{} could not start: {error}",
                    self.program.display()
                )));
            }
        };
        let input = child.stdin.take();
        let output = child.stdout.take();
        let errors = child.stderr.take();
        let exchange = async {
            let (written, stdout, stderr) =
                tokio::join!(deliver(input, stdin), read_tail(output), read_tail(errors));
            (written, stdout, stderr, child.wait().await)
        };
        let ended = tokio::time::timeout(timeout, exchange).await;
        let Ok((written, stdout, stderr, status)) = ended else {
            let _ = child.kill().await;
            return Finished::without_output(Ended::TimedOut(timeout));
        };
        let ended = match (status, written) {
            (Err(error), _) => Ended::NotStarted(format!(
                "{} could not be watched: {error}",
                self.program.display()
            )),
            (Ok(_), Err(error)) => Ended::IncompleteInput(error.to_string()),
            (Ok(status), Ok(())) => Ended::Exited(status.code()),
        };
        Finished {
            ended,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        }
    }
}

/// Write all of `bytes` to a command's standard input, then close it.
async fn deliver(input: Option<ChildStdin>, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(mut input) = input {
        input.write_all(bytes).await?;
        input.shutdown().await?;
    }
    Ok(())
}

/// Read a stream to its end, keeping its last [`OUTPUT_LIMIT`] bytes. A read error ends the
/// stream; what was read is kept.
async fn read_tail(stream: Option<impl AsyncRead + Unpin>) -> Vec<u8> {
    let mut kept = Vec::new();
    if let Some(mut stream) = stream {
        let mut chunk = vec![0; 64 * 1024];
        while let Ok(read @ 1..) = stream.read(&mut chunk).await {
            kept.extend_from_slice(&chunk[..read]);
            if kept.len() > OUTPUT_LIMIT {
                kept.drain(..kept.len() - OUTPUT_LIMIT);
            }
        }
    }
    kept
}
