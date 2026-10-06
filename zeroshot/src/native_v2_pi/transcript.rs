//! Pi JSONL event stream reader.
//!
//! Pi's `message_update` records are delta-only, so live output accumulates deltas while the
//! response value itself is taken from the authoritative final assistant message. Only the last
//! assistant message is the node's response: Pi can emit several per run.

use serde_json::Value;

use crate::native_v2_capsule::provider_json_lines::{ProviderJsonLine, ProviderJsonLines};
use crate::native_v2_capsule::provider_process::safe_provider_text;
use openengine_cluster_protocol::TokenCount;

use crate::native_v2_contract::TokenUsageDelta;
use crate::native_v2_runner::{LiveOutputStream, NodeRunnerError};

/// One readable piece of a turn, already mapped to its output stream.
pub(super) struct PiEmission {
    pub(super) stream: LiveOutputStream,
    pub(super) text: String,
}

pub(super) struct PiResult {
    pub(super) session_id: Option<String>,
    pub(super) message: String,
}

pub(super) struct PiFailure {
    pub(super) session_id: Option<String>,
    pub(super) retryable: bool,
    pub(super) diagnostic: String,
}

pub(super) enum PiAttempt {
    Complete(PiResult),
    Failed(PiFailure),
}

enum Terminal {
    Complete(String),
    Failed(String),
}

pub(crate) struct PiTranscript {
    lines: ProviderJsonLines,
    session_id: Option<String>,
    terminal: Option<Terminal>,
    /// Assistant text blocks of the message currently streaming, keyed by content index.
    blocks: Vec<Option<String>>,
    /// Assistant text of the last completed message, which becomes the node response.
    /// `agent_end` may precede automatic retry or queued work, so it is not terminal.
    settled: bool,
    /// Latest cumulative usage reported by the provider.
    usage: Option<TokenUsageDelta>,
    malformed: usize,
    redactions: Vec<String>,
}

impl PiTranscript {
    pub(super) fn new(mut redactions: Vec<String>) -> Self {
        redactions.retain(|value| !value.is_empty());
        // Longest first, so a redaction that contains another is applied before its substring.
        redactions
            .sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
        redactions.dedup();
        Self {
            lines: ProviderJsonLines::new(),
            session_id: None,
            terminal: None,
            blocks: Vec::new(),
            settled: false,
            usage: None,
            malformed: 0,
            redactions,
        }
    }

    pub(super) fn push(&mut self, chunk: &[u8]) -> Vec<PiEmission> {
        if self.settled {
            // Keep framing bytes bounded without retaining more than the shared guard.
            self.lines.discard();
            return Vec::new();
        }
        let records = self.lines.push(chunk);
        self.accept(records)
    }

    pub(super) fn finish_stream(&mut self) -> Vec<PiEmission> {
        if self.settled {
            self.lines.discard();
            return Vec::new();
        }
        self.lines
            .finish()
            .map_or_else(Vec::new, |record| self.accept([record]))
    }

    pub(super) fn token_usage(&self) -> Option<TokenUsageDelta> {
        self.usage
    }

    /// True only when the provider reported a usable answer. A failed or aborted model response is
    /// reported in the event stream while Pi still exits zero, so exit status alone is insufficient.
    pub(super) fn is_success(&self) -> bool {
        matches!(self.terminal, Some(Terminal::Complete(_)))
    }

    /// Settles the attempt from the last assistant message. A recovered retry is a success, and a
    /// failure reported after the final answer is a failure, because the last message is
    /// authoritative.
    pub(super) fn finish(
        mut self,
        process_failure: Option<&str>,
    ) -> Result<PiAttempt, NodeRunnerError> {
        let process_failure = process_failure.filter(|detail| !detail.trim().is_empty());
        // A final record may arrive without its newline when the process exits, so drain the
        // pending record before settling.
        let _ = self.finish_stream();
        let terminal = self.terminal.take();
        let session_id = self.session_id.clone();
        if let Some(detail) = process_failure {
            return Ok(PiAttempt::Failed(PiFailure {
                session_id,
                retryable: false,
                diagnostic: self.safe(detail),
            }));
        }
        match terminal {
            Some(Terminal::Failed(detail)) => Ok(PiAttempt::Failed(PiFailure {
                session_id,
                retryable: true,
                diagnostic: self.safe(&detail),
            })),
            Some(Terminal::Complete(message)) => Ok(PiAttempt::Complete(PiResult {
                session_id,
                message,
            })),
            None => Ok(PiAttempt::Failed(PiFailure {
                session_id,
                retryable: false,
                diagnostic: self.safe("Pi ended without a settled answer"),
            })),
        }
    }

    fn accept(&mut self, records: impl IntoIterator<Item = ProviderJsonLine>) -> Vec<PiEmission> {
        let mut emissions = Vec::new();
        for record in records {
            let ProviderJsonLine::Record(bytes) = record else {
                // One oversized record is dropped; the turn continues and ends as a failure.
                self.malformed = self.malformed.saturating_add(1);
                continue;
            };
            if bytes.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
                // A malformed record never fails the turn on its own: Pi may interleave records a
                // future version adds. A turn with no terminal event still settles as a failure.
                self.malformed = self.malformed.saturating_add(1);
                continue;
            };
            self.accept_record(&value, &mut emissions);
            if self.settled {
                break;
            }
        }
        emissions
    }

    fn accept_record(&mut self, value: &Value, emissions: &mut Vec<PiEmission>) {
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            return;
        };
        match kind {
            "session" => {
                if let Some(id) = value.get("id").and_then(Value::as_str) {
                    self.session_id = Some(id.to_owned());
                }
            }
            "message_update" => {
                self.record_usage(value);
                if let Some(text) = self.record_delta(value) {
                    push_text(emissions, LiveOutputStream::Output, &text);
                }
            }
            "message_end" => self.record_message_end(value),
            "compaction_start" => {
                push_text(
                    emissions,
                    LiveOutputStream::System,
                    "Pi compacted the session context",
                );
            }
            "auto_retry_start" => {
                let attempt = value.get("attempt").and_then(Value::as_i64).unwrap_or(0);
                push_text(
                    emissions,
                    LiveOutputStream::System,
                    &format!("Pi is retrying the model turn (attempt {attempt})"),
                );
            }
            // Unknown future event types are ignored before any provider-owned field is validated.
            _ => {}
        }
    }

    fn record_message_end(&mut self, value: &Value) {
        let Some(message) = value.get("message") else {
            return;
        };
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            return;
        }
        let text = assistant_text(message);
        self.blocks.clear();
        match message.get("stopReason").and_then(Value::as_str) {
            // An error or aborted stop reason means the turn produced no usable answer, even
            // though Pi exits successfully and reports it only in the event stream.
            Some("error") | Some("aborted") => {
                let detail = message
                    .get("errorMessage")
                    .and_then(Value::as_str)
                    .unwrap_or("Pi reported a failed model response");
                self.terminal = Some(Terminal::Failed(detail.to_owned()));
            }
            // The final assistant message is authoritative. Pi emits one per tool-use step, so an
            // earlier pre-tool message must not become the node's response, and a message that
            // follows a recovered `error` clears that failure.
            _ => {
                if let Some(text) = text {
                    self.terminal = Some(Terminal::Complete(text));
                }
            }
        }
    }

    fn record_delta(&mut self, value: &Value) -> Option<String> {
        let event = value.get("assistantMessageEvent")?;
        let index = event.get("contentIndex").and_then(Value::as_u64)? as usize;
        match event.get("type").and_then(Value::as_str)? {
            "text_start" => {
                self.replace_block(index, "");
                None
            }
            "text_delta" => {
                let delta = event.get("delta").and_then(Value::as_str)?;
                if self.blocks.len() <= index {
                    // A delta before its start event still accumulates: the value, not the event
                    // order, is what the final message is built from.
                    self.blocks.resize(index + 1, None);
                }
                self.blocks[index]
                    .get_or_insert_with(String::new)
                    .push_str(delta);
                Some(delta.to_owned())
            }
            "text_end" => {
                // The completed block is authoritative and replaces the accumulated deltas.
                let content = event.get("content").and_then(Value::as_str)?;
                self.replace_block(index, content);
                None
            }
            _ => None,
        }
    }

    fn replace_block(&mut self, index: usize, content: &str) {
        if self.blocks.len() <= index {
            self.blocks.resize(index + 1, None);
        }
        self.blocks[index] = Some(content.to_owned());
    }

    fn record_usage(&mut self, value: &Value) {
        let Some(usage) = value.get("usage").filter(|usage| usage.is_object()) else {
            return;
        };
        // Pi reports provider-native key names, so they are mapped into the protocol shape rather
        // than parsed through the wire helper.
        let count = |name: &str| {
            usage
                .get(name)
                .and_then(Value::as_u64)
                .and_then(|value| TokenCount::new(value).ok())
        };
        // A record that omits the required counters is ignored rather than treated as zero usage,
        // so a future partial usage shape cannot invent a settled total.
        let (Some(input), Some(output)) = (count("input"), count("output")) else {
            return;
        };
        self.usage = Some(TokenUsageDelta {
            input_tokens: input,
            output_tokens: output,
            cache_read_input_tokens: count("cacheRead"),
            cache_creation_input_tokens: count("cacheWrite"),
        });
    }

    fn safe(&self, text: &str) -> String {
        safe_provider_text(text, &self.redactions)
    }
}

/// Live assistant text of one message, concatenated in content order.
fn assistant_text(message: &Value) -> Option<String> {
    let content = message.get("content")?;
    let blocks = match content {
        Value::Array(blocks) => blocks,
        Value::String(text) => return Some(text.clone()),
        _ => return None,
    };
    let mut text = String::new();
    for block in blocks {
        if block.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        if let Some(part) = block.get("text").and_then(Value::as_str) {
            text.push_str(part);
        }
    }
    (!text.is_empty()).then_some(text)
}

/// Emits one non-empty live fragment. Deltas are emitted verbatim: Pi, like Claude, streams a
/// model response as many small `text_delta` events, and a fragment must never be dropped merely
/// because its characters happen to appear earlier in the same response.
fn push_text(emissions: &mut Vec<PiEmission>, stream: LiveOutputStream, text: &str) {
    if text.is_empty() {
        return;
    }
    emissions.push(PiEmission {
        stream,
        text: text.to_owned(),
    });
}

#[cfg(test)]
mod tests {
    use super::{PiTranscript, assistant_text};
    use crate::native_v2_runner::LiveOutputStream;
    use serde_json::json;

    fn header(id: &str) -> String {
        json!({"type": "session", "version": 3, "id": id, "cwd": "/w"}).to_string()
    }

    fn assistant(text: &str) -> serde_json::Value {
        json!({
            "type": "message_end",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": text}],
                "stopReason": "stop"
            }
        })
    }

    #[test]
    fn the_final_assistant_message_is_the_response() {
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(format!("{}\n{}\n", header("s1"), assistant("done")).as_bytes());
        let attempt = transcript.finish(None).unwrap();
        let super::PiAttempt::Complete(result) = attempt else {
            panic!("expected a complete attempt");
        };
        assert_eq!(result.message, "done");
        assert_eq!(result.session_id.as_deref(), Some("s1"));
    }

    #[test]
    fn deltas_stream_live_but_never_replace_the_authoritative_message() {
        let mut transcript = PiTranscript::new(Vec::new());
        let emissions = transcript.push(
            [
                header("s1").as_str(),
                &json!({"type": "message_update", "assistantMessageEvent": {"type": "text_start", "contentIndex": 0}}).to_string(),
                &json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "Hel"}}).to_string(),
                &json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "lo"}}).to_string(),
                "",
            ]
            .join("\n")
            .as_bytes(),
        );
        let streamed: String = emissions
            .iter()
            .filter(|e| e.stream == LiveOutputStream::Output)
            .map(|e| e.text.as_str())
            .collect();
        assert_eq!(streamed, "Hello");

        transcript.push(assistant("Hello world").to_string().as_bytes());
        let super::PiAttempt::Complete(result) = transcript.finish(None).unwrap() else {
            panic!("expected a complete attempt");
        };
        assert_eq!(result.message, "Hello world");
    }

    #[test]
    fn a_delta_repeating_earlier_text_is_still_emitted() {
        // A fragment must not be dropped just because its characters appeared earlier: a model
        // repeats words constantly, and dropping them renders the live transcript as word salad.
        let mut transcript = PiTranscript::new(Vec::new());
        let emissions = transcript.push(
            [
                header("s1").as_str(),
                &json!({"type": "message_update", "assistantMessageEvent": {"type": "text_start", "contentIndex": 0}}).to_string(),
                &json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "Let"}}).to_string(),
                &json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": " me check"}}).to_string(),
                &json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "Let"}}).to_string(),
                &json!({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": " me check"}}).to_string(),
                "",
            ]
            .join("\n")
            .as_bytes(),
        );
        let streamed: String = emissions
            .iter()
            .filter(|e| e.stream == LiveOutputStream::Output)
            .map(|e| e.text.as_str())
            .collect();
        assert_eq!(streamed, "Let me checkLet me check");
    }

    #[test]
    fn the_last_assistant_message_is_the_response_not_the_pre_tool_text() {
        // Pi emits one assistant message per tool-use step. The node's response must be the final
        // answer, never the narration that preceded a tool call.
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(
            [
                &json!({"type": "message_end", "message": {"role": "assistant", "stopReason": "toolUse",
                        "content": [{"type": "text", "text": "Let me check the file."}]}}).to_string(),
                &json!({"type": "message_end", "message": {"role": "assistant", "stopReason": "stop",
                        "content": [{"type": "text", "text": "The answer is hello."}]}}).to_string(),
                "",
            ]
            .join("\n")
            .as_bytes(),
        );
        let super::PiAttempt::Complete(result) = transcript.finish(None).unwrap() else {
            panic!("expected a complete attempt");
        };
        assert_eq!(result.message, "The answer is hello.");
    }

    #[test]
    fn a_recovered_retry_settles_as_success() {
        // Pi auto-retries a failed model turn. A later successful message must clear the earlier
        // error, since the recovered answer is the authoritative result.
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(
            [
                &json!({"type": "message_end", "message": {"role": "assistant", "content": [],
                        "stopReason": "error", "errorMessage": "529 overloaded"}}).to_string(),
                &json!({"type": "auto_retry_start", "attempt": 1}).to_string(),
                &json!({"type": "message_end", "message": {"role": "assistant", "stopReason": "stop",
                        "content": [{"type": "text", "text": "Recovered answer"}]}}).to_string(),
                "",
            ]
            .join("\n")
            .as_bytes(),
        );
        let super::PiAttempt::Complete(result) = transcript.finish(None).unwrap() else {
            panic!("expected a recovered success");
        };
        assert_eq!(result.message, "Recovered answer");
    }

    #[test]
    fn a_failure_after_the_final_answer_still_fails() {
        // The converse: a failure reported after the answer is authoritative and must fail.
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(
            [
                &json!({"type": "message_end", "message": {"role": "assistant", "stopReason": "stop",
                        "content": [{"type": "text", "text": "Partial answer"}]}}).to_string(),
                &json!({"type": "message_end", "message": {"role": "assistant", "content": [],
                        "stopReason": "aborted", "errorMessage": "aborted by user"}}).to_string(),
                "",
            ]
            .join("\n")
            .as_bytes(),
        );
        assert!(matches!(
            transcript.finish(None).unwrap(),
            super::PiAttempt::Failed(_)
        ));
    }

    #[test]
    fn a_failed_stop_reason_settles_as_a_retryable_provider_failure() {
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(
            json!({
                "type": "message_end",
                "message": {"role": "assistant", "content": [], "stopReason": "error",
                            "errorMessage": "529 overloaded"}
            })
            .to_string()
            .as_bytes(),
        );
        let super::PiAttempt::Failed(failure) = transcript.finish(None).unwrap() else {
            panic!("expected a failure");
        };
        assert!(failure.retryable);
        assert!(failure.diagnostic.contains("529"));
    }

    #[test]
    fn a_process_failure_wins_over_a_complete_message() {
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(assistant("done").to_string().as_bytes());
        let super::PiAttempt::Failed(failure) =
            transcript.finish(Some("pi exited with 1")).unwrap()
        else {
            panic!("expected a failure");
        };
        assert!(!failure.retryable);
        assert!(failure.diagnostic.contains("exited with 1"));
    }

    #[test]
    fn no_terminal_event_settles_as_a_non_retryable_failure() {
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(b"{\"type\":\"agent_start\"}\n");
        let super::PiAttempt::Failed(failure) = transcript.finish(None).unwrap() else {
            panic!("expected a failure");
        };
        assert!(!failure.retryable);
    }

    #[test]
    fn unknown_and_malformed_records_are_ignored_before_the_terminal_event() {
        let mut transcript = PiTranscript::new(Vec::new());
        let stream = [
            header("s1").as_str(),
            "not json at all",
            "{\"type\":\"some_future_event\",\"weird\":{\"nested\":true}}",
            &assistant("ok").to_string(),
            "",
        ]
        .join("\n");
        transcript.push(stream.as_bytes());
        let super::PiAttempt::Complete(result) = transcript.finish(None).unwrap() else {
            panic!("expected a complete attempt");
        };
        assert_eq!(result.message, "ok");
    }

    #[test]
    fn a_complete_final_record_without_a_newline_is_accepted() {
        let mut transcript = PiTranscript::new(Vec::new());
        // Records are LF framed, so only the final record may lack its terminator: the process can
        // exit before writing it.
        transcript.push(format!("{}\n", header("s1")).as_bytes());
        transcript.push(assistant("tail").to_string().as_bytes());
        let super::PiAttempt::Complete(result) = transcript.finish(None).unwrap() else {
            panic!("expected a complete attempt");
        };
        assert_eq!(result.message, "tail");
        assert_eq!(result.session_id.as_deref(), Some("s1"));
    }

    #[test]
    fn an_unterminated_trailing_fragment_is_kept_bounded_and_settles_as_a_failure() {
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(format!("{}\n", header("s1")).as_bytes());
        // A truncated final record is not a complete turn, so it never becomes a response.
        transcript.push(b"{\"type\":\"message_end\",\"mess");
        let super::PiAttempt::Failed(failure) = transcript.finish(None).unwrap() else {
            panic!("expected a failure");
        };
        assert!(!failure.retryable);
    }

    #[test]
    fn usage_is_reported_and_replaced_by_the_latest_cumulative_value() {
        let mut transcript = PiTranscript::new(Vec::new());
        transcript.push(
            format!(
                "{}\n",
                json!({"type": "message_update",
                       "usage": {"input": 10, "output": 2, "cacheRead": 1, "cacheWrite": 0, "totalTokens": 12},
                       "assistantMessageEvent": {"type": "start"}})
            )
            .as_bytes(),
        );
        transcript.push(
            format!(
                "{}\n",
                json!({"type": "message_update",
                       "usage": {"input": 10, "output": 7, "cacheRead": 1, "cacheWrite": 0, "totalTokens": 18},
                       "assistantMessageEvent": {"type": "done", "reason": "stop"}})
            )
            .as_bytes(),
        );
        let usage = transcript.token_usage().unwrap();
        assert_eq!(usage.input_tokens.get(), 10);
        assert_eq!(usage.output_tokens.get(), 7);
        assert_eq!(
            usage.cache_read_input_tokens.map(|count| count.get()),
            Some(1)
        );
    }

    #[test]
    fn credentials_are_redacted_from_diagnostics() {
        let mut transcript = PiTranscript::new(vec!["sk-secret-value".to_owned()]);
        transcript.push(
            json!({"type": "message_end",
                   "message": {"role": "assistant", "content": [], "stopReason": "error",
                               "errorMessage": "rejected sk-secret-value"}})
            .to_string()
            .as_bytes(),
        );
        let super::PiAttempt::Failed(failure) = transcript.finish(None).unwrap() else {
            panic!("expected a failure");
        };
        assert!(!failure.diagnostic.contains("sk-secret-value"));
    }

    #[test]
    fn assistant_text_handles_string_and_block_content() {
        assert_eq!(
            assistant_text(&json!({"content": "plain"})).as_deref(),
            Some("plain")
        );
        assert_eq!(
            assistant_text(&json!({"content": [
                {"type": "thinking", "text": "ignored"},
                {"type": "text", "text": "a"},
                {"type": "text", "text": "b"}
            ]}))
            .as_deref(),
            Some("ab")
        );
        assert_eq!(assistant_text(&json!({"content": []})), None);
    }
}
