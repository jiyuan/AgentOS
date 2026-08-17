//! Server-Sent-Events decoding for streaming chat completions.
//!
//! A shared driver ([`run_sse_stream`]) handles transport, `\n`-framed line
//! buffering, and the terminal event; a provider-specific [`SseAccumulator`]
//! decodes each `data:` chunk. Two shapes live here:
//!
//! - **OpenAI / DeepSeek** — `chat.completion.chunk` objects terminated by a
//!   `data: [DONE]` sentinel, content and tool-call fragments arriving
//!   incrementally, plus a final usage-only chunk when
//!   `stream_options.include_usage` is set.
//! - **Anthropic Messages** — typed events (`message_start`,
//!   `content_block_*`, `message_delta`, `message_stop`) discriminated by a
//!   JSON `type` field, with usage split across `message_start` (input + cache)
//!   and `message_delta` (output); the stream ends by connection close.
//!
//! A third shape — the OpenAI Responses event stream — implements
//! [`SseAccumulator`] next to its request builder in `providers::openai`.
//!
//! All are normalised to the provider-neutral [`CompletionStream`] contract —
//! incremental [`CompletionEvent::Text`] chunks followed by one terminal
//! [`CompletionEvent::Done`] carrying the assembled message, its tool calls, and
//! `agentos.token_usage` logged identically to the buffered path.

use crate::providers::chat_completions::record_reply_outcome;
use crate::providers::{attach_token_usage, log_token_usage};
use crate::{CompletionEvent, CompletionStream, LlmError};
use agentos_proto::{Message, MessageRole, ToolCall, ToolCallId};
use bytes::Bytes;
use futures_util::stream::{self, BoxStream, StreamExt};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
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
    run_sse_stream(
        response,
        OpenAiAccumulator {
            provider,
            model,
            reasoning_key,
            content: String::new(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            usage_body: None,
            finish_reason: None,
        },
    )
}

/// Build a [`CompletionStream`] from a live Anthropic Messages SSE response.
///
/// Anthropic's event stream differs from the OpenAI shape: typed events
/// (`message_start`, `content_block_start/delta/stop`, `message_delta`,
/// `message_stop`) discriminated by a JSON `type` field, with usage split across
/// `message_start` (input + cache) and `message_delta` (final output). The
/// shared line framing and driver are reused; only the accumulator differs.
pub(crate) fn anthropic_stream(model: Arc<str>, response: reqwest::Response) -> CompletionStream {
    run_sse_stream(
        response,
        AnthropicAccumulator {
            model,
            content: String::new(),
            tool_calls: BTreeMap::new(),
            usage_input: None,
            output_tokens: None,
        },
    )
}

/// Pending events queued by an accumulator between network reads.
pub(crate) type Pending = VecDeque<Result<CompletionEvent, LlmError>>;

/// Provider-specific assembly of an SSE event stream into the
/// [`CompletionEvent`] contract. The driver handles transport, line framing,
/// and the terminal `Done`; implementors decode each `data:` JSON chunk.
pub(crate) trait SseAccumulator {
    /// Fold one decoded `data:` JSON chunk in, queueing any `Text` events.
    fn ingest(&mut self, chunk: &Value, pending: &mut Pending);
    /// Assemble the final assistant message once the stream ends. Takes
    /// `&mut self` (draining buffers) so the driver can keep its state value.
    ///
    /// `saw_sentinel` reports whether the wire's own end-of-stream marker
    /// arrived, distinguishing "the provider finished" from "the connection
    /// stopped producing bytes". Only shapes that *have* such a marker can tell
    /// the difference, which is why the decision belongs to the accumulator and
    /// not the driver: `[DONE]` is required by the chat-completions shape, and
    /// Anthropic's stream legitimately ends by connection close.
    ///
    /// Returning `Err` rejects a reply the accumulator built but does not
    /// trust; the driver emits it in place of the terminal `Done`.
    fn finalize(&mut self, saw_sentinel: bool) -> Result<Message, LlmError>;
}

/// Shared driver: decode `response`'s byte stream into SSE lines and feed
/// `acc`, emitting queued `Text` chunks then one terminal `Done`.
pub(crate) fn run_sse_stream<A>(response: reqwest::Response, acc: A) -> CompletionStream
where
    A: SseAccumulator + Send + 'static,
{
    let state = SseState {
        bytes: response.bytes_stream().boxed(),
        buffer: Vec::new(),
        pending: VecDeque::new(),
        acc,
        sse_done: false,
        saw_sentinel: false,
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
                let saw_sentinel = st.saw_sentinel;
                return Some((st.acc.finalize(saw_sentinel).map(CompletionEvent::Done), st));
            }
            match st.bytes.next().await {
                Some(Ok(chunk)) => {
                    st.buffer.extend_from_slice(&chunk);
                    drain_sse_lines(&mut st);
                }
                Some(Err(err)) => {
                    // A transport failure mid-stream is terminal: surface the
                    // error and stop without a spurious Done. Flatten reqwest's
                    // source() chain — its top-level Display ("error decoding
                    // response body") hides the real cause (connection reset,
                    // h2 stream error, incomplete message, proxy cutting SSE).
                    st.terminated = true;
                    return Some((
                        Err(LlmError::StreamTransport(Arc::from(
                            stream_transport_detail(&err),
                        ))),
                        st,
                    ));
                }
                None => {
                    // Connection closed. For Anthropic that *is* the end of
                    // the stream; for the chat-completions shape it means the
                    // bytes stopped before `[DONE]`, which the accumulator
                    // rejects. Either way, stop reading and let it decide.
                    st.sse_done = true;
                }
            }
        }
    })
    .boxed()
}

type ByteStream = BoxStream<'static, reqwest::Result<Bytes>>;

struct SseState<A> {
    bytes: ByteStream,
    /// Raw undecoded tail: SSE lines split on `\n` and only decoded once
    /// complete, so a multi-byte UTF-8 sequence straddling two network chunks
    /// is never decoded mid-codepoint.
    buffer: Vec<u8>,
    pending: Pending,
    acc: A,
    /// Saw `[DONE]` or end of stream; the Done event is emitted next.
    sse_done: bool,
    /// Saw the wire's own terminal marker (`[DONE]`) rather than reaching it by
    /// the byte stream ending. Passed to [`SseAccumulator::finalize`] so shapes
    /// that promise a marker can tell a finished reply from a cut one.
    saw_sentinel: bool,
    /// Done (or a terminal error) has been emitted; the stream is exhausted.
    terminated: bool,
}

/// Flatten a mid-stream reqwest error and its `source()` chain into one detail
/// string. The top-level Display ("error decoding response body") is opaque;
/// the actionable cause — connection reset, HTTP/2 stream error, incomplete
/// message, a proxy terminating the SSE response — lives in the source chain.
/// The `LlmError::StreamTransport` Display adds the "streaming transport error:"
/// prefix, so this returns just the flattened cause.
fn stream_transport_detail(err: &reqwest::Error) -> String {
    let mut detail = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        detail.push_str(": ");
        detail.push_str(&cause.to_string());
        source = cause.source();
    }
    detail
}

fn drain_sse_lines<A: SseAccumulator>(st: &mut SseState<A>) {
    while let Some(pos) = st.buffer.iter().position(|&b| b == b'\n') {
        let line: Vec<u8> = st.buffer.drain(..=pos).collect();
        let line = String::from_utf8_lossy(&line);
        process_sse_line(st, line.trim());
    }
}

fn process_sse_line<A: SseAccumulator>(st: &mut SseState<A>, line: &str) {
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
        st.saw_sentinel = true;
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

struct OpenAiAccumulator {
    provider: &'static str,
    model: Arc<str>,
    reasoning_key: Option<&'static str>,
    content: String,
    reasoning: String,
    tool_calls: Vec<ToolCallBuilder>,
    /// The raw chunk that carried `usage`, kept so the Done step logs and
    /// attaches it through the same path as the buffered response.
    usage_body: Option<Value>,
    /// Latest non-null `choices[0].finish_reason`. Arrives on the terminal
    /// content chunk, which is not the last chunk of the stream — a usage-only
    /// chunk and `[DONE]` follow it.
    finish_reason: Option<String>,
}

/// One tool call under construction across stream fragments: identity arrives
/// once, arguments accumulate as a JSON string. Shared by every accumulator,
/// including the Responses one in `providers::openai`.
#[derive(Default)]
pub(crate) struct ToolCallBuilder {
    pub(crate) id: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) args: String,
}

impl SseAccumulator for OpenAiAccumulator {
    fn ingest(&mut self, chunk: &Value, pending: &mut Pending) {
        if chunk.get("usage").is_some_and(|usage| !usage.is_null()) {
            self.usage_body = Some(chunk.clone());
        }
        let Some(choice) = chunk
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return;
        };
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.finish_reason = Some(reason.to_owned());
        }
        let Some(delta) = choice.get("delta") else {
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
                merge_openai_tool_call(&mut self.tool_calls, call);
            }
        }
    }

    fn finalize(&mut self, saw_sentinel: bool) -> Result<Message, LlmError> {
        if !saw_sentinel {
            // The chat-completions shape always ends with `data: [DONE]`.
            // Reaching the end of the byte stream without it means the response
            // was cut — by a proxy, a dropped connection, a killed upstream —
            // and everything accumulated so far is a partial answer. Reported
            // as a transport failure so the caller retries buffered rather than
            // handing the run a truncated turn it cannot detect.
            return Err(LlmError::StreamTransport(Arc::from(format!(
                "{} stream ended without the [DONE] sentinel after {} content byte(s); the \
                 response was cut short",
                self.provider,
                self.content.len()
            ))));
        }
        let builders = std::mem::take(&mut self.tool_calls);
        let announced = builders
            .iter()
            .filter(|builder| builder.id.is_some() && builder.name.is_some())
            .count();
        let mut message = Message {
            role: MessageRole::Assistant,
            content: Arc::from(std::mem::take(&mut self.content)),
            attachments: Vec::new(),
            tool_calls: tool_calls_from_builders(builders),
            tool_call_id: None,
            metadata: Default::default(),
        };
        let reasoning = std::mem::take(&mut self.reasoning);
        let produced_reasoning = !reasoning.is_empty();
        if produced_reasoning {
            if let Some(key) = self.reasoning_key {
                message
                    .metadata
                    .insert(Arc::from(key), Value::String(reasoning));
            }
        }
        record_reply_outcome(
            self.provider,
            &mut message,
            self.finish_reason.as_deref(),
            announced,
            produced_reasoning,
        )
        .map_err(|defect| LlmError::Provider(Arc::from(defect.to_string())))?;
        if let Some(body) = self.usage_body.take() {
            if let Some(usage) = log_token_usage(self.provider, self.model.as_ref(), &body) {
                attach_token_usage(&mut message, usage);
            }
        }
        Ok(message)
    }
}

/// Merge one OpenAI streamed `tool_calls[]` fragment (indexed, with id/name on
/// the first fragment and `function.arguments` accumulated across the rest).
fn merge_openai_tool_call(builders: &mut Vec<ToolCallBuilder>, call: &Value) {
    let index = call.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
    while builders.len() <= index {
        builders.push(ToolCallBuilder::default());
    }
    let builder = &mut builders[index];
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

/// Finalize accumulated tool-call builders into [`ToolCall`]s, dropping any that
/// never received both an id and a name and defaulting empty args to `{}`.
/// Shared by every provider accumulator.
pub(crate) fn tool_calls_from_builders(
    builders: impl IntoIterator<Item = ToolCallBuilder>,
) -> Vec<ToolCall> {
    builders
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
        .collect()
}

/// Accumulates an Anthropic Messages SSE stream. Text and tool-use content
/// blocks are interleaved and addressed by a shared block `index`; usage arrives
/// split across `message_start` (input + cache) and `message_delta` (output).
struct AnthropicAccumulator {
    model: Arc<str>,
    content: String,
    /// Tool-use blocks keyed by their content-block index, so text blocks at
    /// other indices don't create empty tool entries. Ordered for stable output.
    tool_calls: BTreeMap<usize, ToolCallBuilder>,
    /// `message.usage` from `message_start` (input + cache token counts, plus an
    /// initial output estimate the final `message_delta` supersedes).
    usage_input: Option<Value>,
    /// Final cumulative output tokens from `message_delta`.
    output_tokens: Option<u64>,
}

impl SseAccumulator for AnthropicAccumulator {
    fn ingest(&mut self, chunk: &Value, pending: &mut Pending) {
        match chunk.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                self.usage_input = chunk
                    .get("message")
                    .and_then(|message| message.get("usage"))
                    .filter(|usage| usage.is_object())
                    .cloned();
            }
            Some("content_block_start") => {
                let block = chunk.get("content_block");
                if block.and_then(|b| b.get("type")).and_then(Value::as_str) == Some("tool_use") {
                    let index = block_index(chunk);
                    let builder = self.tool_calls.entry(index).or_default();
                    if let Some(id) = block.and_then(|b| b.get("id")).and_then(Value::as_str) {
                        builder.id = Some(id.to_owned());
                    }
                    if let Some(name) = block.and_then(|b| b.get("name")).and_then(Value::as_str) {
                        builder.name = Some(name.to_owned());
                    }
                }
            }
            Some("content_block_delta") => {
                let delta = chunk.get("delta");
                match delta.and_then(|d| d.get("type")).and_then(Value::as_str) {
                    Some("text_delta") => {
                        if let Some(text) =
                            delta.and_then(|d| d.get("text")).and_then(Value::as_str)
                        {
                            if !text.is_empty() {
                                self.content.push_str(text);
                                pending.push_back(Ok(CompletionEvent::Text(Arc::from(text))));
                            }
                        }
                    }
                    Some("input_json_delta") => {
                        if let Some(partial) = delta
                            .and_then(|d| d.get("partial_json"))
                            .and_then(Value::as_str)
                        {
                            self.tool_calls
                                .entry(block_index(chunk))
                                .or_default()
                                .args
                                .push_str(partial);
                        }
                    }
                    _ => {}
                }
            }
            Some("message_delta") => {
                self.output_tokens = chunk
                    .get("usage")
                    .and_then(|usage| usage.get("output_tokens"))
                    .and_then(Value::as_u64);
            }
            // ping, content_block_stop, message_stop: no state to fold.
            _ => {}
        }
    }

    fn finalize(&mut self, _saw_sentinel: bool) -> Result<Message, LlmError> {
        // Anthropic's Messages stream has no `[DONE]`: it ends by connection
        // close, so there is no sentinel to have missed.
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
        // Reassemble the buffered-path usage shape: input + cache from
        // message_start, output overridden by the final message_delta count.
        if let Some(Value::Object(mut usage)) = self.usage_input.take() {
            if let Some(output) = self.output_tokens {
                usage.insert("output_tokens".to_owned(), Value::from(output));
            }
            let body = serde_json::json!({ "usage": Value::Object(usage) });
            if let Some(usage) = log_token_usage("anthropic", self.model.as_ref(), &body) {
                attach_token_usage(&mut message, usage);
            }
        }
        Ok(message)
    }
}

/// Read the content-block `index` from an Anthropic stream event.
fn block_index(chunk: &Value) -> usize {
    chunk.get("index").and_then(Value::as_u64).unwrap_or(0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::TOKEN_USAGE_METADATA_KEY;

    /// Drive any accumulator over canned SSE bytes through the *production*
    /// line framing and sentinel detection ([`drain_sse_lines`]), and collect
    /// the events. Only the network is replaced: the byte stream is empty
    /// because the fixture feeds `buffer` directly, which is exactly what the
    /// driver does with each chunk it reads. Provider-agnostic, so both the
    /// chat-completions and Anthropic fixtures reuse it.
    fn try_decode_with<A: SseAccumulator>(
        acc: A,
        chunks: &[&[u8]],
    ) -> Result<Vec<CompletionEvent>, LlmError> {
        let mut st = SseState {
            bytes: stream::empty().boxed(),
            buffer: Vec::new(),
            pending: VecDeque::new(),
            acc,
            sse_done: false,
            saw_sentinel: false,
            terminated: false,
        };
        for chunk in chunks {
            st.buffer.extend_from_slice(chunk);
            drain_sse_lines(&mut st);
        }
        let mut events: Vec<CompletionEvent> = std::mem::take(&mut st.pending)
            .into_iter()
            .map(|item| item.expect("no error in fixture"))
            .collect();
        let saw_sentinel = st.saw_sentinel;
        events.push(CompletionEvent::Done(st.acc.finalize(saw_sentinel)?));
        Ok(events)
    }

    /// [`try_decode_with`] for the fixtures that are expected to assemble.
    fn decode_with<A: SseAccumulator>(acc: A, chunks: &[&[u8]]) -> Vec<CompletionEvent> {
        try_decode_with(acc, chunks).expect("fixture assembles a reply")
    }

    fn openai_acc(reasoning_key: Option<&'static str>) -> OpenAiAccumulator {
        OpenAiAccumulator {
            provider: "test",
            model: Arc::from("test-model"),
            reasoning_key,
            content: String::new(),
            reasoning: String::new(),
            tool_calls: Vec::new(),
            usage_body: None,
            finish_reason: None,
        }
    }

    /// OpenAI-shaped convenience wrapper over [`decode_with`].
    fn decode(chunks: &[&[u8]], reasoning_key: Option<&'static str>) -> Vec<CompletionEvent> {
        decode_with(openai_acc(reasoning_key), chunks)
    }

    /// OpenAI-shaped wrapper for the fixtures that are expected to be rejected.
    fn try_decode(chunks: &[&[u8]]) -> Result<Vec<CompletionEvent>, LlmError> {
        try_decode_with(openai_acc(None), chunks)
    }

    fn anthropic_acc() -> AnthropicAccumulator {
        AnthropicAccumulator {
            model: Arc::from("test-model"),
            content: String::new(),
            tool_calls: BTreeMap::new(),
            usage_input: None,
            output_tokens: None,
        }
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
    fn a_stream_that_stops_before_done_is_a_transport_failure() {
        // A proxy or dropped connection cuts the SSE body. Everything decoded
        // so far is a partial answer, and accepting it hands the run a
        // truncated turn nothing downstream can detect. Reported as
        // `StreamTransport` specifically, because that is the one error the
        // orchestrator recovers from by retrying buffered.
        let chunks = [text_chunk("Hello, wor").into_bytes()];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();

        let error = try_decode(&refs).expect_err("a cut stream is not a finished reply");

        assert!(
            matches!(error, LlmError::StreamTransport(_)),
            "expected StreamTransport, got {error:?}"
        );
        assert!(error.to_string().contains("[DONE]"), "{error}");
    }

    #[test]
    fn a_stream_cut_off_mid_tool_call_is_rejected_rather_than_losing_the_call() {
        // The sentinel arrives, so the stream itself is intact — but the
        // arguments stop mid-object, so the assembled call cannot be built and
        // would otherwise be dropped silently.
        let chunks = [
            format!(
                "data: {}\n\n",
                serde_json::json!({ "choices": [{ "index": 0, "delta": { "tool_calls": [{
                    "index": 0, "id": "call_1", "type": "function",
                    "function": { "name": "file", "arguments": "{\"path\":\"/etc/ho" }
                }] } }] })
            )
            .into_bytes(),
            format!(
                "data: {}\n\n",
                serde_json::json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "length" }] })
            )
            .into_bytes(),
            b"data: [DONE]\n\n".to_vec(),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();

        let error = try_decode(&refs).expect_err("a lost tool call is not a valid reply");

        let rendered = error.to_string();
        assert!(rendered.contains("announced 1 tool call(s)"), "{rendered}");
        assert!(rendered.contains("finish_reason=length"), "{rendered}");
    }

    #[test]
    fn a_stream_that_produced_nothing_at_all_is_rejected() {
        let chunks = [
            format!(
                "data: {}\n\n",
                serde_json::json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }] })
            )
            .into_bytes(),
            b"data: [DONE]\n\n".to_vec(),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();

        let error = try_decode(&refs).expect_err("an empty reply is not a finished turn");

        assert!(error.to_string().contains("no content"), "{error}");
    }

    #[test]
    fn a_truncated_stream_that_kept_its_sentinel_records_the_stop_reason() {
        let chunks = [
            text_chunk("the answer is fo").into_bytes(),
            format!(
                "data: {}\n\n",
                serde_json::json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": "length" }] })
            )
            .into_bytes(),
            b"data: [DONE]\n\n".to_vec(),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let events = decode(&refs, None);

        let CompletionEvent::Done(message) = events.last().expect("events present") else {
            panic!("last event must be Done");
        };
        assert_eq!(message.content.as_ref(), "the answer is fo");
        assert_eq!(
            message
                .metadata
                .get(crate::providers::chat_completions::STOP_REASON_METADATA_KEY)
                .and_then(Value::as_str),
            Some("length")
        );
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

    // --- Anthropic Messages SSE ---

    fn anthropic_event(event: &str, data: serde_json::Value) -> Vec<u8> {
        // Anthropic prefixes each event with an `event:` line the decoder
        // ignores; the `data:` JSON carries the discriminating `type` field.
        format!("event: {event}\ndata: {data}\n\n").into_bytes()
    }

    #[test]
    fn anthropic_text_deltas_concatenate_and_carry_usage() {
        let chunks = [
            anthropic_event(
                "message_start",
                serde_json::json!({
                    "type": "message_start",
                    "message": { "usage": { "input_tokens": 12, "output_tokens": 1 } }
                }),
            ),
            anthropic_event(
                "content_block_start",
                serde_json::json!({
                    "type": "content_block_start", "index": 0,
                    "content_block": { "type": "text", "text": "" }
                }),
            ),
            anthropic_event(
                "content_block_delta",
                serde_json::json!({
                    "type": "content_block_delta", "index": 0,
                    "delta": { "type": "text_delta", "text": "Hel" }
                }),
            ),
            anthropic_event(
                "content_block_delta",
                serde_json::json!({
                    "type": "content_block_delta", "index": 0,
                    "delta": { "type": "text_delta", "text": "lo" }
                }),
            ),
            anthropic_event(
                "message_delta",
                serde_json::json!({
                    "type": "message_delta",
                    "delta": { "stop_reason": "end_turn" },
                    "usage": { "output_tokens": 7 }
                }),
            ),
            anthropic_event(
                "message_stop",
                serde_json::json!({ "type": "message_stop" }),
            ),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let events = decode_with(anthropic_acc(), &refs);

        let mut text = String::new();
        let mut done = None;
        for event in events {
            match event {
                CompletionEvent::Text(chunk) => text.push_str(&chunk),
                CompletionEvent::Done(message) => done = Some(message),
            }
        }
        let done = done.expect("done event present");
        assert_eq!(text, "Hello");
        assert_eq!(done.content.as_ref(), "Hello");
        // Usage merges message_start input with message_delta's final output.
        let usage: agentos_proto::Usage = serde_json::from_value(
            done.metadata
                .get(TOKEN_USAGE_METADATA_KEY)
                .expect("usage attached")
                .clone(),
        )
        .expect("usage shape");
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 7);
    }

    #[test]
    fn anthropic_tool_use_block_assembles_input_json() {
        let chunks = [
            anthropic_event(
                "message_start",
                serde_json::json!({ "type": "message_start", "message": { "usage": {} } }),
            ),
            anthropic_event(
                "content_block_start",
                serde_json::json!({
                    "type": "content_block_start", "index": 0,
                    "content_block": { "type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {} }
                }),
            ),
            anthropic_event(
                "content_block_delta",
                serde_json::json!({
                    "type": "content_block_delta", "index": 0,
                    "delta": { "type": "input_json_delta", "partial_json": "{\"city\":" }
                }),
            ),
            anthropic_event(
                "content_block_delta",
                serde_json::json!({
                    "type": "content_block_delta", "index": 0,
                    "delta": { "type": "input_json_delta", "partial_json": "\"paris\"}" }
                }),
            ),
            anthropic_event(
                "message_stop",
                serde_json::json!({ "type": "message_stop" }),
            ),
        ];
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let events = decode_with(anthropic_acc(), &refs);
        let CompletionEvent::Done(message) = events.last().expect("events present") else {
            panic!("last event must be Done");
        };
        assert_eq!(message.tool_calls.len(), 1);
        let call = &message.tool_calls[0];
        assert_eq!(call.id.as_str(), "toolu_1");
        assert_eq!(call.name.as_ref(), "get_weather");
        assert_eq!(call.args.get(), r#"{"city":"paris"}"#);
        // A tool-use-only reply surfaces no assistant-visible text.
        assert!(message.content.is_empty());
    }
}
