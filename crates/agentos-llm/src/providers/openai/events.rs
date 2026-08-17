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

use super::responses::stamp_session;
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
            usage_body: None,
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
    usage_body: Option<Value>,
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
                self.usage_body = response.cloned();
            }
            // Truncated by a token or time limit: keep what usage it reports,
            // but leave the session unstamped so the next turn replays history.
            Some("response.incomplete") => self.usage_body = chunk.get("response").cloned(),
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
        // The Responses stream is terminated by its own `response.completed`
        // event and the connection closing, not by a `[DONE]` sentinel.
        let mut message = Message {
            role: MessageRole::Assistant,
            content: Arc::from(std::mem::take(&mut self.content)),
            attachments: Vec::new(),
            tool_calls: tool_calls_from_builders(
                std::mem::take(&mut self.tool_calls).into_values(),
            ),
            tool_call_id: None,
            metadata: Default::default(),
        };
        stamp_session(
            &mut message,
            self.response_id.as_deref(),
            self.model.as_ref(),
            self.prefix_len,
        );
        if let Some(body) = self.usage_body.take() {
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
    use agentos_proto::TOKEN_USAGE_METADATA_KEY;
    use serde_json::json;
    use std::collections::VecDeque;

    /// Feed events straight into the accumulator. SSE line framing is already
    /// covered by the shared driver's own tests.
    fn decode(events: &[Value]) -> (String, Message) {
        let mut acc = ResponsesAccumulator {
            model: Arc::from("test-model"),
            prefix_len: 3,
            response_id: None,
            content: String::new(),
            tool_calls: BTreeMap::new(),
            usage_body: None,
        };
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
                "response": { "id": "resp_1", "usage": { "input_tokens": 5, "output_tokens": 2 } }
            }),
        ]);

        assert_eq!(message.content.as_ref(), "half an ans");
        assert!(message.metadata.contains_key(TOKEN_USAGE_METADATA_KEY));
        assert!(!message.metadata.contains_key("openai.session"));
    }

    #[test]
    fn failed_response_event_surfaces_an_error() {
        let mut acc = ResponsesAccumulator {
            model: Arc::from("test-model"),
            prefix_len: 0,
            response_id: None,
            content: String::new(),
            tool_calls: BTreeMap::new(),
            usage_body: None,
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
