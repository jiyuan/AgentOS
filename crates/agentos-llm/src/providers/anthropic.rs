use crate::providers::content::{
    append_descriptors, document_mime, format_text_document, image_mime, read_base64,
    read_text_document,
};
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
    let payload = base_payload(model, messages, tools);
    let response = post_json("llm", &url, &headers, &payload).await?;
    let token_usage = log_token_usage("anthropic", model, &response.body);
    let content_blocks = response
        .body
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| ProviderError::MalformedResponse {
            detail: format!(
                "Anthropic response missing assistant content: {}",
                response.body
            ),
        })?;
    let mut text = String::new();
    let mut tool_calls = Vec::new();
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
    if let Some(usage) = token_usage {
        attach_token_usage(&mut message, usage);
    }
    Ok(message)
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
    let mut payload = base_payload(model, messages, tools);
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

fn base_payload(model: &str, messages: &[Message], tools: &[ToolSpec]) -> Value {
    let serialized = messages
        .iter()
        .filter(|message| message.role != MessageRole::System)
        .map(build_message)
        .collect::<Vec<_>>();
    let mut payload = json!({
        "model": model,
        "max_tokens": 1024,
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
    fn anthropic_tool_spec_uses_input_schema_field() {
        let spec = ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read or write"),
            input_schema: json!({"type":"object","properties":{}}),
            requires_isolation: false,
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
