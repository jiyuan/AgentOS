//! Decoding of the Responses SSE event stream.
//!
//! Where Chat Completions streams `chat.completion.chunk` deltas terminated by
//! `[DONE]`, Responses streams typed `response.*` events: text arrives as
//! `response.output_text.delta`, function calls are announced by
//! `response.output_item.added` and filled in by
//! `response.function_call_arguments.delta`, and one terminal
//! `response.completed` carries the assembled response with its id and usage.
//! The shared line framing and driver in `providers::stream` are reused; only
//! this accumulator is Responses-specific.

use super::responses::{incomplete_reason, stamp_session};
use crate::providers::reply::record_reply_outcome;
use crate::providers::stream::{
    run_sse_stream, tool_calls_from_builders, Pending, SseAccumulator, ToolCallBuilder,
};
use crate::providers::{attach_token_usage, log_token_usage};
use crate::{CompletionEvent, CompletionStream, LlmError};
use agentos_proto::{Message, MessageRole};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Build a [`CompletionStream`] from a live Responses SSE response.
/// `prefix_len` is how many messages the request was built from; it rides into
/// the session stamp on the final message.
pub(super) fn stream(
    model: &str,
    prefix_len: usize,
    response: reqwest::Response,
) -> CompletionStream {
    run_sse_stream(
        response,
        ResponsesAccumulator {
            model: Arc::from(model),
            prefix_len,
            response_id: None,
            content: String::new(),
            tool_calls: BTreeMap::new(),
            terminal_response: None,
            stop_reason: None,
        },
    )
}

struct ResponsesAccumulator {
    model: Arc<str>,
    prefix_len: usize,
    /// Id of the stored response, captured only once the stream completes: a
    /// failed or incomplete response is not something the next turn should
    /// chain onto.
    response_id: Option<String>,
    content: String,
    /// Function-call items keyed by `output_index`, so interleaved message and
    /// reasoning items at other indices create no empty entries.
    tool_calls: BTreeMap<usize, ToolCallBuilder>,
    /// The `response` object from the terminal event, kept so `Done` logs and
    /// attaches usage through the same path as the buffered reply.
    terminal_response: Option<Value>,
    /// Why the terminal event said the reply ended, in the
    /// [`STOP_REASON_METADATA_KEY`](crate::providers::reply::STOP_REASON_METADATA_KEY)
    /// vocabulary. Taken from the event *type* rather than the nested object's
    /// `status`, because the type is the signal that always arrives. Staying
    /// `None` is itself information: no terminal event reached us.
    stop_reason: Option<String>,
}

impl SseAccumulator for ResponsesAccumulator {
    fn ingest(&mut self, chunk: &Value, pending: &mut Pending) {
        match chunk.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                if let Some(text) = chunk.get("delta").and_then(Value::as_str) {
                    if !text.is_empty() {
                        self.content.push_str(text);
                        pending.push_back(Ok(CompletionEvent::Text(Arc::from(text))));
                    }
                }
            }
            Some("response.output_item.added") | Some("response.output_item.done") => {
                let Some(item) = chunk.get("item") else {
                    return;
                };
                if item.get("type").and_then(Value::as_str) != Some("function_call") {
                    return;
                }
                let builder = self.tool_calls.entry(output_index(chunk)).or_default();
                if let Some(id) = item.get("call_id").and_then(Value::as_str) {
                    builder.id = Some(id.to_owned());
                }
                if let Some(name) = item.get("name").and_then(Value::as_str) {
                    builder.name = Some(name.to_owned());
                }
                // `added` opens with empty arguments and `done` closes with the
                // complete string, which supersedes the accumulated deltas
                // rather than appending to them.
                if let Some(args) = item.get("arguments").and_then(Value::as_str) {
                    if !args.is_empty() {
                        builder.args = args.to_owned();
                    }
                }
            }
            Some("response.function_call_arguments.delta") => {
                if let Some(delta) = chunk.get("delta").and_then(Value::as_str) {
                    self.tool_calls
                        .entry(output_index(chunk))
                        .or_default()
                        .args
                        .push_str(delta);
                }
            }
            Some("response.completed") => {
                let response = chunk.get("response");
                self.response_id = response
                    .and_then(|response| response.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                self.terminal_response = response.cloned();
                self.stop_reason = Some("stop".to_owned());
            }
            // Truncated by a token or time limit: keep what usage it reports and
            // which limit was hit, but leave the session unstamped so the next
            // turn replays history.
            Some("response.incomplete") => {
                let response = chunk.get("response");
                self.stop_reason = Some(incomplete_reason(response).to_owned());
                self.terminal_response = response.cloned();
            }
            Some("response.failed") | Some("error") => {
                // `response.failed` nests the cause; a bare `error` event is
                // the cause. Either way the turn stops here — the driver's Done
                // still follows, but the consumer propagates this first.
                let error = chunk
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .unwrap_or(chunk);
                pending.push_back(Err(LlmError::Provider(Arc::from(format!(
                    "openai responses stream failed: {error}"
                )))));
            }
            // Lifecycle events (response.created, *.output_text.done, reasoning
            // summaries) carry nothing the final message needs.
            _ => {}
        }
    }

    fn finalize(&mut self, _saw_sentinel: bool) -> Result<Message, LlmError> {
        // This shape has no `[DONE]`, so the driver's `saw_sentinel` is
        // meaningless here. What plays the same role is the terminal
        // `response.*` event — reaching the end of the byte stream without one
        // means the response was cut, by a proxy or a dropped connection, and
        // everything accumulated so far is a partial answer. Reported as a
        // transport failure so the caller retries buffered rather than handing
        // the run a truncated turn it cannot detect. A `response.failed` stream
        // already queued its own error ahead of this one, so that cause wins.
        let Some(reason) = self.stop_reason.take() else {
            return Err(LlmError::StreamTransport(Arc::from(format!(
                "openai responses stream ended without a terminal response event after {} \
                 content byte(s); the response was cut short",
                self.content.len()
            ))));
        };
        let builders = std::mem::take(&mut self.tool_calls);
        let announced = builders
            .values()
            .filter(|builder| builder.id.is_some() && builder.name.is_some())
            .count();
        let mut message = Message {
            role: MessageRole::Assistant,
            content: Arc::from(std::mem::take(&mut self.content)),
            attachments: Vec::new(),
            tool_calls: tool_calls_from_builders(builders.into_values()),
            tool_call_id: None,
            metadata: Default::default(),
        };
        // `produced_reasoning` is always false: Responses keeps reasoning
        // server-side and streams no reasoning text, so there is never a
        // reasoning channel for an otherwise empty reply to count as output.
        record_reply_outcome("openai", &mut message, Some(&reason), announced, false)
            .map_err(|defect| LlmError::Provider(Arc::from(defect.to_string())))?;
        stamp_session(
            &mut message,
            self.response_id.as_deref(),
            self.model.as_ref(),
            self.prefix_len,
        );
        if let Some(body) = self.terminal_response.take() {
            if let Some(usage) = log_token_usage("openai", self.model.as_ref(), &body) {
                attach_token_usage(&mut message, usage);
            }
        }
        Ok(message)
    }
}

/// Which item in the response's `output` list an event addresses.
fn output_index(chunk: &Value) -> usize {
    chunk
        .get("output_index")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::reply::STOP_REASON_METADATA_KEY;
    use agentos_proto::TOKEN_USAGE_METADATA_KEY;
    use serde_json::json;
    use std::collections::VecDeque;

    fn accumulator() -> ResponsesAccumulator {
        ResponsesAccumulator {
            model: Arc::from("test-model"),
            prefix_len: 3,
            response_id: None,
            content: String::new(),
            tool_calls: BTreeMap::new(),
            terminal_response: None,
            stop_reason: None,
        }
    }

    /// Feed events straight into the accumulator. SSE line framing is already
    /// covered by the shared driver's own tests.
    fn decode(events: &[Value]) -> (String, Message) {
        let mut acc = accumulator();
        let mut pending: Pending = VecDeque::new();
        for event in events {
            acc.ingest(event, &mut pending);
        }
        let text = pending
            .into_iter()
            .filter_map(|item| match item.expect("no error in fixture") {
                CompletionEvent::Text(chunk) => Some(chunk.to_string()),
                CompletionEvent::Done(_) => None,
            })
            .collect();
        // The Responses shape has no `[DONE]`, so the sentinel flag is
        // irrelevant here; `false` is what the live driver passes.
        (
            text,
            acc.finalize(false).expect("fixture assembles a reply"),
        )
    }

    #[test]
    fn text_deltas_concatenate_and_carry_usage_and_session() {
        let (text, message) = decode(&[
            json!({ "type": "response.created", "response": { "id": "resp_1" } }),
            json!({ "type": "response.output_text.delta", "output_index": 0, "delta": "Hel" }),
            json!({ "type": "response.output_text.delta", "output_index": 0, "delta": "lo" }),
            json!({ "type": "response.output_text.done", "output_index": 0, "text": "Hello" }),
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_1",
                    "usage": {
                        "input_tokens": 120,
                        "output_tokens": 8,
                        "total_tokens": 128,
                        "input_tokens_details": { "cached_tokens": 100 }
                    }
                }
            }),
        ]);

        assert_eq!(text, "Hello");
        assert_eq!(message.content.as_ref(), "Hello");
        let usage: agentos_proto::Usage = serde_json::from_value(
            message
                .metadata
                .get(TOKEN_USAGE_METADATA_KEY)
                .expect("usage attached")
                .clone(),
        )
        .expect("usage shape");
        assert_eq!(usage.input_tokens, 120);
        assert_eq!(usage.output_tokens, 8);
        assert_eq!(usage.cache_read_tokens, 100);
        assert_eq!(
            message.metadata.get("openai.session"),
            Some(&json!({ "response_id": "resp_1", "model": "test-model", "prefix_len": 3 }))
        );
    }

    #[test]
    fn function_call_arguments_assemble_into_one_call() {
        let (text, message) = decode(&[
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "function_call", "id": "fc_1", "call_id": "call_1",
                    "name": "get_weather", "arguments": ""
                }
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0, "item_id": "fc_1", "delta": "{\"city\":"
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0, "item_id": "fc_1", "delta": "\"paris\"}"
            }),
            json!({ "type": "response.completed", "response": { "id": "resp_1" } }),
        ]);

        assert!(text.is_empty(), "a tool-call turn emits no visible text");
        assert_eq!(message.tool_calls.len(), 1);
        assert_eq!(message.tool_calls[0].id.as_str(), "call_1");
        assert_eq!(message.tool_calls[0].name.as_ref(), "get_weather");
        assert_eq!(message.tool_calls[0].args.get(), r#"{"city":"paris"}"#);
    }

    #[test]
    fn output_item_done_supersedes_argument_deltas() {
        // A stream that also replays the complete arguments on `done` must not
        // end up with them concatenated twice.
        let (_, message) = decode(&[
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": { "type": "function_call", "call_id": "call_1", "name": "file", "arguments": "" }
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0, "delta": "{\"path\":\"a.txt\"}"
            }),
            json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call", "call_id": "call_1", "name": "file",
                    "arguments": "{\"path\":\"a.txt\"}"
                }
            }),
            json!({ "type": "response.completed", "response": { "id": "resp_1" } }),
        ]);

        assert_eq!(message.tool_calls.len(), 1);
        assert_eq!(message.tool_calls[0].args.get(), r#"{"path":"a.txt"}"#);
    }

    #[test]
    fn incomplete_response_is_not_stamped_as_a_resumable_session() {
        let (_, message) = decode(&[
            json!({ "type": "response.output_text.delta", "delta": "half an ans" }),
            json!({
                "type": "response.incomplete",
                "response": {
                    "id": "resp_1",
                    "status": "incomplete",
                    "incomplete_details": { "reason": "max_output_tokens" },
                    "usage": { "input_tokens": 5, "output_tokens": 2 }
                }
            }),
        ]);

        assert_eq!(message.content.as_ref(), "half an ans");
        assert!(message.metadata.contains_key(TOKEN_USAGE_METADATA_KEY));
        assert!(!message.metadata.contains_key("openai.session"));
        // `max_output_tokens` is this shape's `length`: the text is usable but
        // the transcript has to record that it was cut.
        assert_eq!(
            message
                .metadata
                .get(STOP_REASON_METADATA_KEY)
                .and_then(Value::as_str),
            Some("length")
        );
    }

    #[test]
    fn an_ordinary_completed_reply_gains_no_stop_reason() {
        let (_, message) = decode(&[
            json!({ "type": "response.output_text.delta", "delta": "the answer is four" }),
            json!({
                "type": "response.completed",
                "response": { "id": "resp_1", "status": "completed" }
            }),
        ]);

        assert!(!message.metadata.contains_key(STOP_REASON_METADATA_KEY));
    }

    #[test]
    fn a_stream_that_ends_without_a_terminal_event_is_a_transport_failure() {
        // The Responses analogue of a chat-completions stream cut before
        // `[DONE]`: text arrived, no `response.completed` or `response.incomplete`
        // ever did, so what we hold is half an answer. `StreamTransport` is the
        // one error the orchestrator recovers from by retrying buffered.
        let mut acc = accumulator();
        let mut pending: Pending = VecDeque::new();
        acc.ingest(
            &json!({ "type": "response.output_text.delta", "delta": "half an ans" }),
            &mut pending,
        );

        let error = acc
            .finalize(false)
            .expect_err("a cut stream is not a finished reply");

        assert!(
            matches!(error, LlmError::StreamTransport(_)),
            "expected StreamTransport, got {error:?}"
        );
        assert!(
            error.to_string().contains("terminal response event"),
            "{error}"
        );
    }

    #[test]
    fn a_stream_cut_off_mid_tool_call_is_rejected_rather_than_losing_the_call() {
        // The terminal event arrives, so the stream itself is intact — but the
        // arguments stop mid-object, so the call cannot be assembled and would
        // otherwise be dropped silently.
        let mut acc = accumulator();
        let mut pending: Pending = VecDeque::new();
        for event in [
            json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "function_call", "call_id": "call_1", "name": "file", "arguments": ""
                }
            }),
            json!({
                "type": "response.function_call_arguments.delta",
                "output_index": 0, "delta": "{\"path\":\"/etc/ho"
            }),
            json!({
                "type": "response.incomplete",
                "response": {
                    "id": "resp_1",
                    "status": "incomplete",
                    "incomplete_details": { "reason": "max_output_tokens" }
                }
            }),
        ] {
            acc.ingest(&event, &mut pending);
        }

        let error = acc
            .finalize(false)
            .expect_err("a lost tool call is not a valid reply");

        let rendered = error.to_string();
        assert!(rendered.contains("announced 1 tool call(s)"), "{rendered}");
        assert!(rendered.contains("finish_reason=length"), "{rendered}");
    }

    #[test]
    fn a_reply_with_nothing_in_it_is_rejected() {
        // Every output token went to server-side reasoning, so the run loop
        // would otherwise be handed a finished turn containing nothing.
        let mut acc = accumulator();
        let mut pending: Pending = VecDeque::new();
        acc.ingest(
            &json!({
                "type": "response.completed",
                "response": { "id": "resp_1", "status": "completed" }
            }),
            &mut pending,
        );

        let error = acc
            .finalize(false)
            .expect_err("an empty reply is not a finished turn");

        assert!(error.to_string().contains("no content"), "{error}");
    }

    #[test]
    fn failed_response_event_surfaces_an_error() {
        let mut acc = ResponsesAccumulator {
            model: Arc::from("test-model"),
            prefix_len: 0,
            response_id: None,
            content: String::new(),
            tool_calls: BTreeMap::new(),
            terminal_response: None,
            stop_reason: None,
        };
        let mut pending: Pending = VecDeque::new();
        acc.ingest(
            &json!({
                "type": "response.failed",
                "response": { "error": { "code": "server_error", "message": "boom" } }
            }),
            &mut pending,
        );

        let err = pending.pop_front().expect("event queued").unwrap_err();
        assert!(err.to_string().contains("boom"));
    }
}
