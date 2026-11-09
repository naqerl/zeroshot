//! Provider quota pauses: recognise a model-gateway quota refusal in a failed run's public
//! failure context, derive a bounded retry deadline, and keep one process-wide exhaustion
//! deadline so new placements wait instead of starting work that cannot call the provider.
//!
//! The hub does not call the provider itself; a run on a target does. The provider's error text
//! therefore reaches the hub through the run's bounded failure diagnostics, which is the only
//! public evidence the target exposes.

use std::sync::Mutex;

use serde_json::Value;

use super::clock::{parse_future_ms, parse_rfc3339_ms, rfc3339};
use super::model::{Job, JobState};

/// How long before retrying after the first, second, ... quota pause. The sequence is
/// exponential and capped at one hour; a job never retries faster than once a minute.
pub(crate) const BACKOFF_MS: [u64; 6] = [60_000, 120_000, 300_000, 900_000, 1_800_000, 3_600_000];

/// A quota refusal found in a failed run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QuotaFailure {
    /// Provider limit name, such as `weekly`.
    pub limit: String,
    /// Provider-supplied retry instant, when the payload carries one.
    pub retry_at_ms: Option<u64>,
}

/// One process-wide known exhaustion: the gateway is refusing until `until_ms`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QuotaExhaustion {
    pub until_ms: u64,
    pub limit: String,
}

/// Process-wide provider exhaustion for one hub, owned by the running service. Every request
/// handler and dispatcher pass that places work consults this same deadline, so a refusal
/// recorded by one job is visible to every later placement in the daemon process.
#[derive(Debug, Default)]
pub(crate) struct QuotaDeadline {
    state: Mutex<Option<QuotaExhaustion>>,
}

impl QuotaDeadline {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record that the gateway refused model calls until `until_ms`. A later deadline wins, so a
    /// second refusal cannot shorten an existing wait.
    pub(crate) fn record(&self, until_ms: u64, limit: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.as_mut() {
            Some(current) if current.until_ms >= until_ms => {}
            _ => {
                *state = Some(QuotaExhaustion {
                    until_ms,
                    limit: limit.to_owned(),
                });
            }
        }
    }

    /// Raise the deadline to the latest quota-paused job's retry time. A job that already carries
    /// a quota wait keeps the daemon from placing work at least until that wait ends.
    pub(crate) fn record_jobs(&self, jobs: &[Job]) {
        if let Some((until, limit)) = jobs
            .iter()
            .filter(|job| job.state == JobState::WaitingQuota)
            .filter_map(|job| {
                job.quota
                    .as_ref()
                    .map(|quota| (quota.retry_at_ms, quota.limit.clone()))
            })
            .max_by_key(|(until, _)| *until)
        {
            self.record(until, &limit);
        }
    }

    /// Clear the exhaustion after a provider call succeeded.
    pub(crate) fn clear(&self) {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// Why new placements are refused now, naming the exhausted limit and its retry time.
    pub(crate) fn refusal(&self, now_ms: u64) -> Option<String> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let exhausted = state.as_ref()?;
        (now_ms < exhausted.until_ms).then(|| {
            format!(
                "provider quota {} is exhausted until {}; new placements wait for retryAt",
                exhausted.limit,
                rfc3339(exhausted.until_ms)
            )
        })
    }
}

/// The wait before the given quota-pause attempt, with bounded jitter.
///
/// `attempts` is one-based. Jitter is derived from the job identity so a fleet that hit the same
/// limit does not retry in lockstep, and a job never retries faster than one minute.
pub(crate) fn backoff_ms(attempts: u32, job_id: &str) -> u64 {
    let index = usize::try_from(attempts.saturating_sub(1))
        .unwrap_or(usize::MAX)
        .min(BACKOFF_MS.len() - 1);
    let base = BACKOFF_MS[index];
    let jitter_span = base / 10 + 1;
    base + stable_hash(job_id, attempts) % jitter_span
}

fn stable_hash(job_id: &str, attempts: u32) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in job_id
        .as_bytes()
        .iter()
        .copied()
        .chain(attempts.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Human-readable quota error for a job that is waiting.
pub(crate) fn wait_message(failure: &QuotaFailure, until_ms: u64) -> String {
    format!(
        "provider quota {} is exhausted; the executing node is held and will resume at {}",
        failure.limit,
        rfc3339(until_ms)
    )
}

/// Recognise a provider quota refusal in a run status document.
///
/// Only a failed terminal result is considered; the provider error travels in its bounded
/// `context` (primary and recorded messages), never in a successful result's output.
pub(crate) fn detect(status: &Value, now_ms: u64) -> Option<QuotaFailure> {
    let terminal = status
        .get("terminalResult")
        .or_else(|| status.pointer("/status/terminalResult"))?;
    if terminal.get("status").and_then(Value::as_str) == Some("succeeded") {
        return None;
    }
    let mut messages = Vec::new();
    if let Some(reason) = terminal.get("reason").and_then(Value::as_str) {
        messages.push(reason.to_owned());
    }
    if let Some(context) = terminal.get("context") {
        collect_messages(context, &mut messages);
    }
    messages
        .iter()
        .find_map(|message| parse_message(message, now_ms))
}

fn collect_messages(value: &Value, messages: &mut Vec<String>) {
    match value {
        Value::String(message) => messages.push(message.clone()),
        Value::Array(items) => {
            for item in items {
                collect_messages(item, messages);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                collect_messages(item, messages);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Whether a message is a provider quota refusal, and its limit/retry facts.
fn parse_message(text: &str, now_ms: u64) -> Option<QuotaFailure> {
    let lower = text.to_ascii_lowercase();
    let quota = lower.contains("gousagelimiterror")
        || lower.contains("usage limit exceeded")
        || lower.contains("usage limit exhausted")
        || (lower.contains("quota")
            && (lower.contains("exceed")
                || lower.contains("exhaust")
                || lower.contains("429")
                || lower.contains("too many requests")))
        || ((lower.contains("429") || lower.contains("too many requests"))
            && (lower.contains("limit") || lower.contains("quota")));
    if !quota {
        return None;
    }
    let limit = setting(
        text,
        &lower,
        &["limitname", "limit_name", "limit-id", "limitid"],
    )
    .or_else(|| setting(text, &lower, &["\"limit\"", "'limit'"]))
    .or_else(|| {
        ["weekly", "monthly", "daily", "hourly"]
            .into_iter()
            .find(|candidate| lower.contains(candidate))
            .map(str::to_owned)
    })
    .unwrap_or_else(|| "provider".to_owned());
    let retry_at_ms = [
        "retryat",
        "retry_at",
        "retry-at",
        "retryafter",
        "retry_after",
        "retry-after",
        "resetsat",
        "resets_at",
        "resetat",
        "reset_at",
    ]
    .into_iter()
    .find_map(|key| setting(text, &lower, &[key]))
    .and_then(|value| parse_retry(&value, now_ms));
    Some(QuotaFailure { limit, retry_at_ms })
}

/// Read the value after `key:` or `key=`, quoted or bare. This deliberately accepts the small
/// JSON fragments a provider diagnostic embeds without depending on their surrounding shape.
fn setting(text: &str, lower: &str, keys: &[&str]) -> Option<String> {
    let bytes = text.as_bytes();
    for key in keys {
        let mut from = 0;
        while let Some(offset) = lower[from..].find(key) {
            let mut index = from + offset + key.len();
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                index += 1;
            }
            // A JSON key ends with its closing quote before the colon: `"retryAt":"..."`.
            if matches!(bytes.get(index), Some(b'"' | b'\'')) {
                index += 1;
                while bytes
                    .get(index)
                    .is_some_and(|byte| byte.is_ascii_whitespace())
                {
                    index += 1;
                }
            }
            if !matches!(bytes.get(index), Some(b':' | b'=')) {
                from = index;
                continue;
            }
            index += 1;
            while bytes
                .get(index)
                .is_some_and(|byte| byte.is_ascii_whitespace())
            {
                index += 1;
            }
            let quote = match bytes.get(index) {
                Some(b @ (b'"' | b'\'')) => Some(*b),
                _ => None,
            };
            if quote.is_some() {
                index += 1;
            }
            let start = index;
            while let Some(byte) = bytes.get(index) {
                if let Some(quote) = quote {
                    if *byte == quote {
                        break;
                    }
                } else if !(byte.is_ascii_alphanumeric()
                    || matches!(*byte, b'-' | b'_' | b'.' | b'+' | b':'))
                {
                    break;
                }
                index += 1;
            }
            let value = text.get(start..index).unwrap_or_default();
            if !value.is_empty() {
                return Some(value.to_owned());
            }
            from = index;
        }
    }
    None
}

/// Parse a retry hint: epoch milliseconds, epoch seconds, a delay in seconds (`Retry-After`),
/// an RFC 3339 instant, or a short duration such as `2h`.
fn parse_retry(value: &str, now_ms: u64) -> Option<u64> {
    if let Ok(number) = value.parse::<u64>() {
        return Some(match number {
            // Epoch milliseconds.
            number if number >= 1_000_000_000_000 => number,
            // Epoch seconds.
            number if number >= 10_000_000_000 => number,
            number if number >= 1_000_000_000 => number.saturating_mul(1_000),
            // Relative seconds, as HTTP `Retry-After` uses.
            seconds => now_ms.saturating_add(seconds.saturating_mul(1_000)),
        });
    }
    parse_rfc3339_ms(value).or_else(|| parse_future_ms(value, now_ms).ok())
}
