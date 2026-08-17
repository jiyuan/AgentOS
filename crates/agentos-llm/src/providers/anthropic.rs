use crate::providers::content::{
    append_descriptors, document_mime, format_text_document, image_mime, read_base64,
    read_text_document,
};
use crate::providers::reply::record_reply_outcome;
use crate::providers::stream::anthropic_stream;
use crate::providers::{
    attach_token_usage, log_token_usage, post_json, post_sse, raw_args_from_json_value,
    ProviderError,
};
use crate::CompletionStream;
use agentos_interfaces::tool::ToolSpec;
use agentos_proto::{Attachment, AttachmentKind, Message, MessageRole, ToolCall, ToolCallId};
use serde_json::{json, Value};
use std::env;
use std::sync::Arc;

pub async fn complete(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<Message, ProviderError> {
    let (url, headers) = endpoint_and_headers()?;
    let payload = base_payload(model, messages, tools, false);
    let response = post_json("llm", &url, &headers, &payload).await?;
    // Logged before the reply is validated: the tokens were spent either way.
    let token_usage = log_token_usage("anthropic", model, &response.body);
    let mut message = reply_from_body(&response.body)?;
    if let Some(usage) = token_usage {
        attach_token_usage(&mut message, usage);
    }
    Ok(message)
}

/// Assemble and validate the assistant turn from a buffered Messages body — the
/// buffered counterpart of the streaming accumulator's `finalize`, applying the
/// same [`record_reply_outcome`] rules to the same fields.
fn reply_from_body(body: &Value) -> Result<Message, ProviderError> {
    let content_blocks = body
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| ProviderError::MalformedResponse {
            detail: format!("Anthropic response missing assistant content: {body}"),
        })?;
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    let mut announced = 0usize;
    for block in content_blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(chunk) = block.get("text").and_then(Value::as_str) {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(chunk);
                }
            }
            Some("tool_use") => {
                let Some(id) = block.get("id").and_then(Value::as_str) else {
                    continue;
                };
                let Some(name) = block.get("name").and_then(Value::as_str) else {
                    continue;
                };
                // Counted before the arguments are built, so a call the reply
                // fully identified but whose arguments do not survive is visible
                // as a loss rather than silently absent. Unlike the two
                // OpenAI-shaped providers this is a guard rather than a fix:
                // Anthropic sends `input` as a JSON object, so a truncated one
                // fails the response parse upstream instead of arriving here.
                announced += 1;
                if let Some(args) = raw_args_from_json_value(block.get("input")) {
                    tool_calls.push(ToolCall {
                        id: ToolCallId::new(id),
                        name: Arc::from(name),
                        args,
                    });
                }
            }
            _ => {}
        }
    }
    let mut message = Message {
        role: MessageRole::Assistant,
        content: Arc::from(text),
        attachments: Vec::new(),
        tool_calls,
        tool_call_id: None,
        metadata: Default::default(),
    };
    // `produced_reasoning` is always false: extended thinking arrives as
    // `thinking` content blocks, which this provider neither requests nor
    // captures, so there is no reasoning channel for an otherwise empty reply to
    // count as output.
    record_reply_outcome(
        "anthropic",
        &mut message,
        canonical_stop_reason(body.get("stop_reason").and_then(Value::as_str)),
        announced,
        false,
    )
    .map_err(|defect| ProviderError::MalformedResponse {
        detail: defect.to_string(),
    })?;
    Ok(message)
}

/// Translate an Anthropic `stop_reason` into the vocabulary
/// [`STOP_REASON_METADATA_KEY`](crate::providers::reply::STOP_REASON_METADATA_KEY)
/// uses, so a session log reads the same whichever provider wrote it. Shared with
/// the streaming path, which reads the same field from `message_delta`.
///
/// `end_turn` and `tool_use` are this shape's two ordinary endings and map onto
/// `stop` and `tool_calls`. `stop_sequence` is ordinary too — Chat Completions
/// reports a stop sequence as plain `stop` — and `max_tokens` is `length`.
/// Anything else passes through under its own name rather than being folded into
/// a near neighbour: `refusal` is the model declining, which is not the same
/// event as a provider content filter, and `pause_turn` means the turn should be
/// continued rather than that anything went wrong. Both are worth recording as
/// themselves.
pub(crate) fn canonical_stop_reason(raw: Option<&str>) -> Option<&str> {
    match raw? {
        "end_turn" | "stop_sequence" => Some("stop"),
        "tool_use" => Some("tool_calls"),
        "max_tokens" => Some("length"),
        other => Some(other),
    }
}

/// Stream a Messages completion over SSE. Builds the same request as
/// [`complete`] plus `stream: true`, then hands the live response to the shared
/// Anthropic SSE decoder.
pub async fn complete_stream(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<CompletionStream, ProviderError> {
    let (url, headers) = endpoint_and_headers()?;
    let mut payload = base_payload(model, messages, tools, true);
    payload["stream"] = json!(true);
    let response = post_sse("anthropic", &url, &headers, &payload).await?;
    Ok(anthropic_stream(Arc::from(model), response))
}

/// `(messages URL, request headers)` shared by the buffered and streaming
/// request builders.
type Endpoint = (String, Vec<(&'static str, String)>);

fn endpoint_and_headers() -> Result<Endpoint, ProviderError> {
    let api_key = env::var("ANTHROPIC_API_KEY").map_err(|_| ProviderError::MissingCredentials {
        variable: "ANTHROPIC_API_KEY",
    })?;
    let base_url = env::var("AGENTOS_ANTHROPIC_BASE_URL")
        .or_else(|_| env::var("ANTHROPIC_BASE_URL"))
        .unwrap_or_else(|_| "https://api.anthropic.com/v1".to_owned());
    Ok((
        format!("{}/messages", base_url.trim_end_matches('/')),
        vec![
            ("x-api-key", api_key),
            ("anthropic-version", "2023-06-01".to_owned()),
            ("Content-Type", "application/json".to_owned()),
        ],
    ))
}

/// Overrides the resolved output cap for every Anthropic request. For a model
/// this build's table does not know, or knows wrongly, without a rebuild. The
/// buffered path still clamps it — see [`BUFFERED_MAX_OUTPUT_TOKENS`], which is
/// an API constraint rather than a preference.
pub const MAX_OUTPUT_TOKENS_ENV: &str = "AGENTOS_ANTHROPIC_MAX_OUTPUT_TOKENS";

/// Published maximum output tokens by model-id prefix, longest prefix first.
///
/// Unlike the other providers here, the Messages API *requires* `max_tokens`:
/// there is no "let the server decide" option, so some number is always sent and
/// whatever it is becomes a hard ceiling on every reply. These are external facts
/// about each model rather than deployment policy, so they are a table for the
/// same reason `CONTEXT_BUDGETS` is one, with [`MAX_OUTPUT_TOKENS_ENV`] covering
/// a model the table has wrong or has never heard of.
const MAX_OUTPUT_TOKENS: &[(&str, usize)] = &[
    ("claude-haiku-4", 64_000),
    ("claude-opus-4", 32_000),
    ("claude-sonnet-4", 64_000),
    ("claude-3-7", 64_000),
    ("claude-3-5", 8_192),
    ("claude-3", 4_096),
];

/// Cap for a model the table does not list. The smallest limit any Claude model
/// has published, so it is always accepted. A floor rather than `None` — unlike
/// `context_budget_for_model`, which may decline to guess, this field is
/// required and *something* has to be sent.
const DEFAULT_MAX_OUTPUT_TOKENS: usize = 4_096;

/// Ceiling on buffered (non-streaming) requests. Anthropic refuses a
/// non-streaming request whose `max_tokens` implies a reply that could exceed
/// its ten-minute limit, so the buffered path cannot ask for a model's full
/// output cap the way the streaming path can. If this is more conservative than
/// the real threshold the only cost is a lower buffered cap than necessary,
/// which is the safe direction to be wrong in.
const BUFFERED_MAX_OUTPUT_TOKENS: usize = 8_192;

/// The `max_tokens` to send for one request. `streaming` selects between a
/// model's full published cap and the buffered ceiling.
fn max_output_tokens(model: &str, streaming: bool) -> usize {
    let requested = env::var(MAX_OUTPUT_TOKENS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|tokens| *tokens > 0)
        .unwrap_or_else(|| published_max_output_tokens(model));
    if streaming {
        requested
    } else {
        requested.min(BUFFERED_MAX_OUTPUT_TOKENS)
    }
}

/// Resolve a model id to its published output cap by longest matching prefix.
fn published_max_output_tokens(model: &str) -> usize {
    let normalized = model.trim().to_ascii_lowercase();
    MAX_OUTPUT_TOKENS
        .iter()
        .filter(|(prefix, _)| normalized.starts_with(prefix))
        .max_by_key(|(prefix, _)| prefix.len())
        .map_or(DEFAULT_MAX_OUTPUT_TOKENS, |(_, cap)| *cap)
}

fn base_payload(model: &str, messages: &[Message], tools: &[ToolSpec], streaming: bool) -> Value {
    let serialized = messages
        .iter()
        .filter(|message| message.role != MessageRole::System)
        .map(build_message)
        .collect::<Vec<_>>();
    let mut payload = json!({
        "model": model,
        "max_tokens": max_output_tokens(model, streaming),
        "messages": serialized,
    });
    if !tools.is_empty() {
        payload["tools"] = json!(tools.iter().map(anthropic_tool_spec).collect::<Vec<_>>());
    }
    payload
}

fn anthropic_tool_spec(spec: &ToolSpec) -> Value {
    json!({
        "name": spec.name.as_ref(),
        "description": spec.description.as_ref(),
        "input_schema": spec.input_schema,
    })
}

fn build_message(message: &Message) -> Value {
    // Anthropic encodes tool results as a `tool_result` content block inside
    // a user turn (mirroring how `tool_use` lives in the assistant turn).
    if message.role == MessageRole::Tool {
        let tool_use_id = message
            .tool_call_id
            .as_ref()
            .map(|id| id.as_str().to_owned())
            .unwrap_or_default();
        return json!({
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": message.content.as_ref(),
            }],
        });
    }

    // Assistant turn that requested tools: emit any text the model produced,
    // followed by one `tool_use` block per pending call. Anthropic requires
    // the turn that carries tool_use to precede the user turn carrying its
    // matching tool_result.
    if message.role == MessageRole::Assistant && !message.tool_calls.is_empty() {
        let mut blocks: Vec<Value> = Vec::with_capacity(message.tool_calls.len() + 1);
        if !message.content.is_empty() {
            blocks.push(json!({ "type": "text", "text": message.content.as_ref() }));
        }
        for call in &message.tool_calls {
            let input: Value = serde_json::from_str(call.args.get()).unwrap_or_else(|_| json!({}));
            blocks.push(json!({
                "type": "tool_use",
                "id": call.id.as_str(),
                "name": call.name.as_ref(),
                "input": input,
            }));
        }
        return json!({
            "role": "assistant",
            "content": blocks,
        });
    }

    let role = match message.role {
        MessageRole::Assistant => "assistant",
        MessageRole::User => "user",
        MessageRole::System | MessageRole::Tool => "user",
    };

    let base_text = message.content.to_string();

    if message.attachments.is_empty() {
        return json!({
            "role": role,
            "content": base_text,
        });
    }

    // Split each attachment into either an inline content block or a textual
    // descriptor that gets appended to the leading text block.
    let mut inline_blocks: Vec<Value> = Vec::new();
    let mut fallback_attachments: Vec<&Attachment> = Vec::new();
    for attachment in &message.attachments {
        match content_block_for(attachment) {
            Some(block) => inline_blocks.push(block),
            None => fallback_attachments.push(attachment),
        }
    }

    let leading_text = if fallback_attachments.is_empty() {
        base_text
    } else {
        let owned: Vec<Attachment> = fallback_attachments.into_iter().cloned().collect();
        append_descriptors(&base_text, &owned)
    };

    // Always lead with a text block. Some models reply with a generic
    // "I don't have access to that file" when given an image content block
    // alone; a neutral placeholder keeps the turn well-formed.
    let text_part = if leading_text.is_empty() {
        "(user attached files without a caption)".to_owned()
    } else {
        leading_text
    };
    let mut blocks: Vec<Value> = Vec::with_capacity(inline_blocks.len() + 1);
    blocks.push(json!({ "type": "text", "text": text_part }));
    blocks.extend(inline_blocks);

    json!({
        "role": role,
        "content": blocks,
    })
}

fn content_block_for(attachment: &Attachment) -> Option<Value> {
    match attachment.kind {
        AttachmentKind::Image => image_block(attachment),
        AttachmentKind::Document => document_block(attachment),
    }
}

fn image_block(attachment: &Attachment) -> Option<Value> {
    let media_type = image_mime(attachment)?;
    let data = read_base64(&attachment.path).ok()?;
    Some(json!({
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": data,
        }
    }))
}

fn document_block(attachment: &Attachment) -> Option<Value> {
    if let Some(media_type) = document_mime(attachment) {
        if let Ok(data) = read_base64(&attachment.path) {
            return Some(json!({
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": data,
                }
            }));
        }
    }
    if let Some(Ok(body)) = read_text_document(attachment) {
        let formatted = format_text_document(&attachment.name, &body);
        return Some(json!({
            "type": "text",
            "text": formatted,
        }));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_interfaces::tool::SandboxMode;
    use agentos_proto::{Attachment, AttachmentKind};
    use serde_json::value::RawValue;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn write_tmp(name: &str, bytes: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("anthropic-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }

    fn image_message(path: PathBuf, mime: Option<&str>, text: &str) -> Message {
        Message {
            role: MessageRole::User,
            content: Arc::from(text),
            attachments: vec![Attachment {
                kind: AttachmentKind::Image,
                name: Arc::from("photo.jpg"),
                path,
                mime: mime.map(Arc::from),
                size: Some(3),
                source: None,
            }],
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: Default::default(),
        }
    }

    #[test]
    fn text_only_message_keeps_string_content() {
        let msg = Message::text(MessageRole::User, "hi");
        let value = build_message(&msg);
        assert_eq!(value.get("content").and_then(Value::as_str), Some("hi"));
    }

    #[test]
    fn image_attachment_emits_image_block() {
        let path = write_tmp("photo.jpg", &[0xff, 0xd8, 0xff]);
        let msg = image_message(path, Some("image/jpeg"), "look");
        let value = build_message(&msg);
        let blocks = value.get("content").and_then(Value::as_array).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "look");
        assert_eq!(blocks[1]["type"], "image");
        assert_eq!(blocks[1]["source"]["media_type"], "image/jpeg");
        assert!(!blocks[1]["source"]["data"].as_str().unwrap().is_empty());
    }

    #[test]
    fn unsupported_image_falls_back_to_descriptor() {
        let path = write_tmp("photo.bmp", &[0x42, 0x4d]);
        let msg = image_message(path, Some("image/bmp"), "hi");
        let value = build_message(&msg);
        let blocks = value.get("content").and_then(Value::as_array).unwrap();
        // No inline image block; only a single text block carrying the descriptor.
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "text");
        let text = blocks[0]["text"].as_str().unwrap();
        assert!(text.starts_with("hi"));
        assert!(text.contains("[attached image:"));
    }

    #[test]
    fn pdf_attachment_emits_document_block() {
        let path = write_tmp("spec.pdf", b"%PDF-1.4 fake");
        let msg = Message {
            role: MessageRole::User,
            content: Arc::from(""),
            attachments: vec![Attachment {
                kind: AttachmentKind::Document,
                name: Arc::from("spec.pdf"),
                path,
                mime: Some(Arc::from("application/pdf")),
                size: Some(13),
                source: None,
            }],
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        let blocks = value.get("content").and_then(Value::as_array).unwrap();
        // Leading placeholder text block + document block.
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[1]["type"], "document");
        assert_eq!(blocks[1]["source"]["media_type"], "application/pdf");
    }

    #[test]
    fn missing_file_falls_back_to_descriptor() {
        let msg = image_message(
            PathBuf::from("/nonexistent/missing.jpg"),
            Some("image/jpeg"),
            "hi",
        );
        let value = build_message(&msg);
        let blocks = value.get("content").and_then(Value::as_array).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "text");
    }

    fn raw_args(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_owned()).unwrap()
    }

    #[test]
    fn output_caps_resolve_by_longest_matching_prefix() {
        assert_eq!(published_max_output_tokens("claude-sonnet-4-5"), 64_000);
        assert_eq!(published_max_output_tokens("claude-opus-4-1"), 32_000);
        assert_eq!(published_max_output_tokens("claude-haiku-4-5"), 64_000);
        // `claude-3-5` must win over the shorter `claude-3`.
        assert_eq!(
            published_max_output_tokens("claude-3-5-sonnet-latest"),
            8_192
        );
        assert_eq!(published_max_output_tokens("claude-3-opus-20240229"), 4_096);
        assert_eq!(published_max_output_tokens("  Claude-Sonnet-4-5  "), 64_000);
    }

    #[test]
    fn the_table_never_shadows_a_longer_prefix_with_a_shorter_one() {
        // `published_max_output_tokens` takes the longest match, so this ordering
        // is belt-and-braces rather than load-bearing — swapping it for a plain
        // first-match today changes no answer. It is asserted anyway because that
        // equivalence is exactly what a later edit can quietly destroy: append
        // `claude-3-5-haiku` after `claude-3-5` and the two disagree.
        for (index, (prefix, _)) in MAX_OUTPUT_TOKENS.iter().enumerate() {
            for (later, _) in &MAX_OUTPUT_TOKENS[index + 1..] {
                assert!(
                    !later.starts_with(prefix),
                    "{later:?} extends {prefix:?} but is listed after it, so a \
                     first-match reading of the table would never reach it"
                );
            }
        }
    }

    #[test]
    fn an_unlisted_model_gets_the_floor_rather_than_no_answer() {
        // The field is required, so declining to guess is not an option; 4,096 is
        // the smallest cap any Claude model publishes and is always accepted.
        assert_eq!(published_max_output_tokens("claude-9-ultra"), 4_096);
        assert_eq!(published_max_output_tokens(""), 4_096);
    }

    #[test]
    fn the_buffered_path_clamps_where_the_streaming_path_does_not() {
        // Anthropic refuses a non-streaming request that could run past its
        // ten-minute limit, so the buffered path cannot ask for the full cap.
        assert_eq!(max_output_tokens("claude-sonnet-4-5", true), 64_000);
        assert_eq!(max_output_tokens("claude-sonnet-4-5", false), 8_192);
        // A model whose published cap is already under the ceiling is unaffected.
        assert_eq!(max_output_tokens("claude-3-opus-20240229", false), 4_096);
    }

    #[test]
    fn the_request_carries_the_resolved_cap_not_a_fixed_one() {
        let messages = [Message::text(MessageRole::User, "hello")];

        let streamed = base_payload("claude-sonnet-4-5", &messages, &[], true);
        let buffered = base_payload("claude-sonnet-4-5", &messages, &[], false);

        assert_eq!(streamed["max_tokens"], 64_000);
        assert_eq!(buffered["max_tokens"], 8_192);
    }

    #[test]
    fn a_buffered_reply_stopped_at_max_tokens_records_length() {
        // `base_payload` pins max_tokens at 1024, so this is the routine case:
        // the text is usable, but the transcript has to record that it was cut.
        let body = json!({
            "stop_reason": "max_tokens",
            "content": [{ "type": "text", "text": "the answer is fo" }]
        });

        let message = reply_from_body(&body).expect("truncated text is still usable");

        assert_eq!(message.content.as_ref(), "the answer is fo");
        assert_eq!(
            message
                .metadata
                .get(crate::providers::reply::STOP_REASON_METADATA_KEY)
                .and_then(Value::as_str),
            Some("length")
        );
    }

    #[test]
    fn an_ordinary_buffered_reply_gains_no_stop_reason() {
        let body = json!({
            "stop_reason": "end_turn",
            "content": [{ "type": "text", "text": "the answer is four" }]
        });

        let message = reply_from_body(&body).expect("an ordinary reply is not a defect");

        assert!(!message
            .metadata
            .contains_key(crate::providers::reply::STOP_REASON_METADATA_KEY));
    }

    #[test]
    fn a_buffered_reply_with_nothing_in_it_is_rejected() {
        // Every output token went to a thinking block this provider does not
        // capture, so the run loop would be handed a finished turn containing
        // nothing.
        let body = json!({
            "stop_reason": "max_tokens",
            "content": [{ "type": "thinking", "thinking": "...", "signature": "sig" }]
        });

        let error = reply_from_body(&body).expect_err("an empty reply is not a finished turn");

        assert!(error.to_string().contains("no content"), "{error}");
    }

    #[test]
    fn a_buffered_tool_use_turn_is_accepted_and_left_unmarked() {
        let body = json!({
            "stop_reason": "tool_use",
            "content": [{
                "type": "tool_use", "id": "toolu_1", "name": "file",
                "input": { "path": "a.txt" }
            }]
        });

        let message = reply_from_body(&body).expect("a tool-use turn is not a defect");

        assert_eq!(message.tool_calls.len(), 1);
        assert_eq!(message.tool_calls[0].id.as_str(), "toolu_1");
        assert!(!message
            .metadata
            .contains_key(crate::providers::reply::STOP_REASON_METADATA_KEY));
    }

    #[test]
    fn canonical_stop_reason_maps_the_ordinary_endings_and_passes_the_rest_through() {
        assert_eq!(canonical_stop_reason(Some("end_turn")), Some("stop"));
        assert_eq!(canonical_stop_reason(Some("stop_sequence")), Some("stop"));
        assert_eq!(canonical_stop_reason(Some("tool_use")), Some("tool_calls"));
        assert_eq!(canonical_stop_reason(Some("max_tokens")), Some("length"));
        // Not folded into `content_filter`: a model declining is a different
        // event from a provider filtering the output.
        assert_eq!(canonical_stop_reason(Some("refusal")), Some("refusal"));
        assert_eq!(
            canonical_stop_reason(Some("pause_turn")),
            Some("pause_turn")
        );
        // A reason this build has never heard of is still worth recording.
        assert_eq!(
            canonical_stop_reason(Some("future_reason")),
            Some("future_reason")
        );
        assert_eq!(canonical_stop_reason(None), None);
    }

    #[test]
    fn anthropic_tool_spec_uses_input_schema_field() {
        let spec = ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read or write"),
            input_schema: json!({"type":"object","properties":{}}),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        };
        let value = anthropic_tool_spec(&spec);
        assert_eq!(value["name"], "file");
        assert!(value["input_schema"].is_object());
    }

    #[test]
    fn assistant_with_tool_calls_emits_tool_use_block() {
        let msg = Message {
            role: MessageRole::Assistant,
            content: Arc::from("looking that up"),
            attachments: Vec::new(),
            tool_calls: vec![ToolCall {
                id: ToolCallId::new("toolu_abc"),
                name: Arc::from("file"),
                args: raw_args(r#"{"operation":"read","path":"a.md"}"#),
            }],
            tool_call_id: None,
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        assert_eq!(value["role"], "assistant");
        let blocks = value["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[1]["type"], "tool_use");
        assert_eq!(blocks[1]["id"], "toolu_abc");
        assert_eq!(blocks[1]["name"], "file");
        assert_eq!(blocks[1]["input"]["operation"], "read");
    }

    #[test]
    fn tool_role_emits_user_with_tool_result_block() {
        let msg = Message {
            role: MessageRole::Tool,
            content: Arc::from("read 19 bytes"),
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: Some(ToolCallId::new("toolu_abc")),
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        assert_eq!(value["role"], "user");
        let blocks = value["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "tool_result");
        assert_eq!(blocks[0]["tool_use_id"], "toolu_abc");
        assert_eq!(blocks[0]["content"], "read 19 bytes");
    }
}
