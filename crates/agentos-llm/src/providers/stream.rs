//! Server-Sent-Events decoding for streaming chat completions.
//!
//! OpenAI and DeepSeek both stream `chat.completion.chunk` objects over SSE:
//! `data: {json}` lines terminated by a `data: [DONE]` sentinel, with content
//! and tool-call fragments arriving incrementally and (when
//! `stream_options.include_usage` is requested) a final usage-only chunk. This
//! module turns that byte stream into the provider-neutral [`CompletionStream`]
//! contract — incremental [`CompletionEvent::Text`] chunks followed by one
//! terminal [`CompletionEvent::Done`] carrying the assembled message, its tool
//! calls, and `agentos.token_usage` metalogged identically to the buffered
//! path.

use crate::providers::{attach_token_usage, log_token_usage};
use crate::{CompletionEvent, CompletionStream, LlmError};
use agentos_proto::{Message, MessageRole, ToolCall, ToolCallId};
use bytes::Bytes;
use futures_util::stream::{self, BoxStream, StreamExt};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;

/// Build a [`CompletionStream`] from a live OpenAI-compatible SSE response.
///
/// `reasoning_key`, when set, captures a provider-specific
/// `delta.reasoning_content` field into the final message metadata (DeepSeek
/// reasoning models); it is never surfaced as assistant-visible `Text`, mirroring
/// the buffered path.
pub(crate) fn openai_compatible_stream(
    provider: &'static str,
    model: Arc<str>,
    reasoning_key: Option<&'static str>,
    response: reqwest::Response,
) -> CompletionStream {
    let state = SseState {
        bytes: response.bytes_stream().boxed(),
        buffer: Vec::new(),
        pending: VecDeque::new(),
        acc: Accumulator {
            provider,
            model,
            reasoning_key,
            content: String::new(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            usage_body: None,
        },
        sse_done: false,
        terminated: false,
    };
    stream::unfold(state, |mut st| async move {
        loop {
            if let Some(item) = st.pending.pop_front() {
                return Some((item, st));
            }
            if st.terminated {
                return None;
            }
            if st.sse_done {
                st.terminated = true;
                return Some((Ok(CompletionEvent::Done(st.acc.finalize())), st));
            }
            match st.bytes.next().await {
                Some(Ok(chunk)) => {
                    st.buffer.extend_from_slice(&chunk);
                    drain_sse_lines(&mut st);
                }
                Some(Err(err)) => {
                    // A transport failure mid-stream is terminal: surface the
                    // error and stop without a spurious Done.
                    st.terminated = true;
                    return Some((
                        Err(LlmError::Provider(Arc::from(format!(
                            "streaming transport error: {err}"
                        )))),
                        st,
                    ));
                }
                None => {
                    // Connection closed without an explicit [DONE]; assemble
                    // whatever we accumulated.
                    st.sse_done = true;
                }
            }
        }
    })
    .boxed()
}

type ByteStream = BoxStream<'static, reqwest::Result<Bytes>>;

struct SseState {
    bytes: ByteStream,
    /// Raw undecoded tail: SSE lines split on `\n` and only decoded once
    /// complete, so a multi-byte UTF-8 sequence straddling two network chunks
    /// is never decoded mid-codepoint.
    buffer: Vec<u8>,
    pending: VecDeque<Result<CompletionEvent, LlmError>>,
    acc: Accumulator,
    /// Saw `[DONE]` or end of stream; the Done event is emitted next.
    sse_done: bool,
    /// Done (or a terminal error) has been emitted; the stream is exhausted.
    terminated: bool,
}

fn drain_sse_lines(st: &mut SseState) {
    while let Some(pos) = st.buffer.iter().position(|&b| b == b'\n') {
        let line: Vec<u8> = st.buffer.drain(..=pos).collect();
        let line = String::from_utf8_lossy(&line);
        process_sse_line(st, line.trim());
    }
}

fn process_sse_line(st: &mut SseState, line: &str) {
    if line.is_empty() {
        return;
    }
    // Ignore SSE framing other than `data:` (comments, `event:`, `id:`).
    let Some(data) = line.strip_prefix("data:") else {
        return;
    };
    let data = data.trim();
    if data == "[DONE]" {
        st.sse_done = true;
        return;
    }
    match serde_json::from_str::<Value>(data) {
        Ok(value) => st.acc.ingest(&value, &mut st.pending),
        Err(err) => st
            .pending
            .push_back(Err(LlmError::Provider(Arc::from(format!(
                "malformed SSE chunk: {err}"
            ))))),
    }
}

struct Accumulator {
    provider: &'static str,
    model: Arc<str>,
    reasoning_key: Option<&'static str>,
    content: String,
    reasoning: String,
    tool_calls: Vec<ToolCallBuilder>,
    /// The raw chunk that carried `usage`, kept so the Done step logs and
    /// attaches it through the same path as the buffered response.
    usage_body: Option<Value>,
}

#[derive(Default)]
struct ToolCallBuilder {
    id: Option<String>,
    name: Option<String>,
    args: String,
}

impl Accumulator {
    fn ingest(&mut self, chunk: &Value, pending: &mut VecDeque<Result<CompletionEvent, LlmError>>) {
        if chunk.get("usage").is_some_and(|usage| !usage.is_null()) {
            self.usage_body = Some(chunk.clone());
        }
        let Some(delta) = chunk
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("delta"))
        else {
            return;
        };
        if let Some(text) = delta.get("content").and_then(Value::as_str) {
            if !text.is_empty() {
                self.content.push_str(text);
                pending.push_back(Ok(CompletionEvent::Text(Arc::from(text))));
            }
        }
        if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str) {
            self.reasoning.push_str(reasoning);
        }
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                self.merge_tool_call(call);
            }
        }
    }

    fn merge_tool_call(&mut self, call: &Value) {
        let index = call.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
        while self.tool_calls.len() <= index {
            self.tool_calls.push(ToolCallBuilder::default());
        }
        let builder = &mut self.tool_calls[index];
        if let Some(id) = call.get("id").and_then(Value::as_str) {
            if !id.is_empty() {
                builder.id = Some(id.to_owned());
            }
        }
        if let Some(function) = call.get("function") {
            if let Some(name) = function.get("name").and_then(Value::as_str) {
                if !name.is_empty() {
                    builder.name = Some(name.to_owned());
                }
            }
            if let Some(args) = function.get("arguments").and_then(Value::as_str) {
                builder.args.push_str(args);
            }
        }
    }

    /// Assemble the final message. Takes `&mut self` (draining its buffers)
    /// rather than `self` so the caller can keep the surrounding stream state.
    fn finalize(&mut self) -> Message {
        let tool_calls = std::mem::take(&mut self.tool_calls)
            .into_iter()
            .filter_map(|builder| {
                let id = builder.id?;
                let name = builder.name?;
                let args = if builder.args.trim().is_empty() {
                    "{}".to_owned()
                } else {
                    builder.args
                };
                let args = serde_json::value::RawValue::from_string(args).ok()?;
                Some(ToolCall {
                    id: ToolCallId::new(id.as_str()),
                    name: Arc::from(name),
                    args,
                })
            })
            .collect();
        let mut message = Message {
            role: MessageRole::Assistant,
            content: Arc::from(std::mem::take(&mut self.content)),
            attachments: Vec::new(),
            tool_calls,
            tool_call_id: None,
            metadata: Default::default(),
        };
        let reasoning = std::mem::take(&mut self.reasoning);
        if !reasoning.is_empty() {
            if let Some(key) = self.reasoning_key {
                message
                    .metadata
                    .insert(Arc::from(key), Value::String(reasoning));
            }
        }
        if let Some(body) = self.usage_body.take() {
            if let Some(usage) = log_token_usage(self.provider, self.model.as_ref(), &body) {
                attach_token_usage(&mut message, usage);
            }
        }
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::TOKEN_USAGE_METADATA_KEY;

    /// Feed canned SSE bytes (optionally in awkward chunk splits) through the
    /// accumulator the way the live decoder would, and collect the events.
    fn decode(chunks: &[&[u8]], reasoning_key: Option<&'static str>) -> Vec<CompletionEvent> {
        let mut acc = Accumulator {
            provider: "test",
            model: Arc::from("test-model"),
            reasoning_key,
            content: String::new(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            usage_body: None,
        };
        let mut buffer: Vec<u8> = Vec::new();
        let mut pending: VecDeque<Result<CompletionEvent, LlmError>> = VecDeque::new();
        let mut sse_done = false;
        for chunk in chunks {
            buffer.extend_from_slice(chunk);
            while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = buffer.drain(..=pos).collect();
                let line = String::from_utf8_lossy(&line);
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(data) = line.strip_prefix("data:") {
                    let data = data.trim();
                    if data == "[DONE]" {
                        sse_done = true;
                        continue;
                    }
                    let value: Value = serde_json::from_str(data).expect("valid chunk json");
                    acc.ingest(&value, &mut pending);
                }
            }
        }
        assert!(sse_done, "fixture must terminate with [DONE]");
        let mut events: Vec<CompletionEvent> = pending
            .into_iter()
            .map(|item| item.expect("no error in fixture"))
            .collect();
        events.push(CompletionEvent::Done(acc.finalize()));
        events
    }

    fn text_chunk(content: &str) -> String {
        format!(
            "data: {}\n\n",
            serde_json::json!({ "choices": [{ "index": 0, "delta": { "content": content } }] })
        )
    }

    #[test]
    fn text_deltas_concatenate_to_final_content() {
        let chunks = [
            text_chunk("Hello").into_bytes(),
            text_chunk(", ").into_bytes(),
            text_chunk("world").into_bytes(),
            b"data: [DONE]\n\n".to_vec(),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let events = decode(&refs, None);

        let mut text = String::new();
        let mut done = None;
        for event in events {
            match event {
                CompletionEvent::Text(chunk) => text.push_str(&chunk),
                CompletionEvent::Done(message) => done = Some(message),
            }
        }
        let done = done.expect("done event present");
        assert_eq!(text, "Hello, world");
        assert_eq!(done.content.as_ref(), "Hello, world");
    }

    #[test]
    fn partial_lines_across_chunk_boundaries_are_buffered() {
        // The same payload, but split mid-line and mid-multibyte-character.
        let full = format!("{}data: [DONE]\n\n", text_chunk("héllo"));
        let bytes = full.into_bytes();
        let mid = bytes.len() / 2;
        let chunks = [&bytes[..mid], &bytes[mid..]];
        let events = decode(&chunks, None);
        let CompletionEvent::Done(message) = events.last().expect("events present") else {
            panic!("last event must be Done");
        };
        assert_eq!(message.content.as_ref(), "héllo");
    }

    #[test]
    fn tool_call_fragments_assemble_into_one_call() {
        let chunks = [
            format!(
                "data: {}\n\n",
                serde_json::json!({ "choices": [{ "index": 0, "delta": { "tool_calls": [
                    { "index": 0, "id": "call_1", "type": "function",
                      "function": { "name": "get_weather", "arguments": "" } }
                ] } }] })
            )
            .into_bytes(),
            format!(
                "data: {}\n\n",
                serde_json::json!({ "choices": [{ "index": 0, "delta": { "tool_calls": [
                    { "index": 0, "function": { "arguments": "{\"city\":" } }
                ] } }] })
            )
            .into_bytes(),
            format!(
                "data: {}\n\n",
                serde_json::json!({ "choices": [{ "index": 0, "delta": { "tool_calls": [
                    { "index": 0, "function": { "arguments": "\"paris\"}" } }
                ] } }] })
            )
            .into_bytes(),
            b"data: [DONE]\n\n".to_vec(),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let events = decode(&refs, None);
        let CompletionEvent::Done(message) = events.last().expect("events present") else {
            panic!("last event must be Done");
        };
        assert_eq!(message.tool_calls.len(), 1);
        let call = &message.tool_calls[0];
        assert_eq!(call.id.as_str(), "call_1");
        assert_eq!(call.name.as_ref(), "get_weather");
        assert_eq!(call.args.get(), r#"{"city":"paris"}"#);
        // Tool-call-only reply carries no assistant-visible text.
        assert!(message.content.is_empty());
    }

    #[test]
    fn final_usage_chunk_attaches_token_metadata() {
        let chunks = [
            text_chunk("hi").into_bytes(),
            format!(
                "data: {}\n\n",
                serde_json::json!({
                    "choices": [],
                    "usage": { "prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13 }
                })
            )
            .into_bytes(),
            b"data: [DONE]\n\n".to_vec(),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let events = decode(&refs, None);
        let CompletionEvent::Done(message) = events.last().expect("events present") else {
            panic!("last event must be Done");
        };
        assert!(message.metadata.contains_key(TOKEN_USAGE_METADATA_KEY));
    }

    #[test]
    fn reasoning_content_captured_into_metadata_not_text() {
        let chunks = [
            format!(
                "data: {}\n\n",
                serde_json::json!({ "choices": [{ "index": 0, "delta": { "reasoning_content": "thinking" } }] })
            )
            .into_bytes(),
            text_chunk("answer").into_bytes(),
            b"data: [DONE]\n\n".to_vec(),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let events = decode(&refs, Some("deepseek.reasoning_content"));

        let text: String = events
            .iter()
            .filter_map(|event| match event {
                CompletionEvent::Text(chunk) => Some(chunk.to_string()),
                CompletionEvent::Done(_) => None,
            })
            .collect();
        assert_eq!(text, "answer");
        let CompletionEvent::Done(message) = events.last().expect("events present") else {
            panic!("last event must be Done");
        };
        assert_eq!(
            message
                .metadata
                .get("deepseek.reasoning_content")
                .and_then(Value::as_str),
            Some("thinking")
        );
    }
}
