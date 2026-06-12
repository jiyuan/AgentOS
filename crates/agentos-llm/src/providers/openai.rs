use crate::providers::content::{
    append_descriptors, document_mime, format_text_document, image_mime, read_base64,
    read_text_document,
};
use crate::providers::{
    attach_token_usage, first_env, format_openai_error, log_token_usage, post_json,
    raw_args_from_json_str,
};
use agentos_interfaces::tool::ToolSpec;
use agentos_proto::{Attachment, AttachmentKind, Message, MessageRole, ToolCall, ToolCallId};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::env;
use std::sync::Arc;

pub async fn complete(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<Message, String> {
    let api_key = env::var("OPENAI_API_KEY").map_err(|_| "missing OPENAI_API_KEY".to_owned())?;
    let base_url = env::var("AGENTOS_OPENAI_BASE_URL")
        .or_else(|_| env::var("OPENAI_BASE_URL"))
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_owned());
    let mut headers = vec![
        ("Authorization", format!("Bearer {api_key}")),
        ("Content-Type", "application/json".to_owned()),
    ];
    if let Some(organization) = first_env(["OPENAI_ORGANIZATION", "OPENAI_ORG_ID"]) {
        headers.push(("OpenAI-Organization", organization));
    }
    if let Some(project) = first_env(["OPENAI_PROJECT", "OPENAI_PROJECT_ID"]) {
        headers.push(("OpenAI-Project", project));
    }
    let serialized = serialize_messages(messages);
    let mut payload = json!({
        "model": model,
        "messages": serialized,
        "temperature": 0.7,
    });
    if !tools.is_empty() {
        payload["tools"] = json!(tools.iter().map(tool_to_function).collect::<Vec<_>>());
        // One call per turn — the loop iterates so we don't need parallelism.
        payload["parallel_tool_calls"] = json!(false);
    }
    let response = post_json(
        "llm",
        &format!("{}/chat/completions", base_url.trim_end_matches('/')),
        &headers,
        &payload,
    )
    .await?;
    if let Some(error) = response.body.get("error") {
        return Err(format_openai_error(&response, error));
    }
    let token_usage = log_token_usage("openai", model, &response.body);
    let message = response
        .body
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .ok_or_else(|| format!("OpenAI response missing message: {}", response.body))?;
    let content = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let tool_calls = parse_tool_calls(message);
    let mut message = Message {
        role: MessageRole::Assistant,
        content: Arc::from(content),
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

fn tool_to_function(spec: &ToolSpec) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": spec.name.as_ref(),
            "description": spec.description.as_ref(),
            "parameters": spec.input_schema,
        }
    })
}

fn parse_tool_calls(message: &Value) -> Vec<ToolCall> {
    let Some(calls) = message.get("tool_calls").and_then(Value::as_array) else {
        return Vec::new();
    };
    calls
        .iter()
        .filter_map(|call| {
            let id = call.get("id").and_then(Value::as_str)?;
            let function = call.get("function")?;
            let name = function.get("name").and_then(Value::as_str)?;
            // Arguments come as a JSON string. Validate into RawValue so we
            // can hand a canonical RawValue downstream without re-encoding.
            let args = raw_args_from_json_str(function.get("arguments"))?;
            Some(ToolCall {
                id: ToolCallId::new(id),
                name: Arc::from(name),
                args,
            })
        })
        .collect()
}

fn build_message(message: &Message) -> Value {
    // Tool result rides on a dedicated tool-role message that links back to
    // the assistant's tool_calls entry by id. OpenAI 400s if you try to send
    // tool results without a preceding assistant turn carrying that id.
    if message.role == MessageRole::Tool {
        let tool_call_id = message
            .tool_call_id
            .as_ref()
            .map(|id| id.as_str().to_owned())
            .unwrap_or_default();
        return json!({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": message.content.as_ref(),
        });
    }

    let role = match message.role {
        MessageRole::Assistant => "assistant",
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Tool => unreachable!("tool handled above"),
    };

    // Assistant turns that requested tools: emit the canonical
    // `tool_calls` field alongside whatever text the model also produced.
    if message.role == MessageRole::Assistant && !message.tool_calls.is_empty() {
        let calls = message
            .tool_calls
            .iter()
            .map(serialize_tool_call)
            .collect::<Vec<_>>();
        let content: Value = if message.content.is_empty() {
            Value::Null
        } else {
            Value::String(message.content.to_string())
        };
        return json!({
            "role": role,
            "content": content,
            "tool_calls": calls,
        });
    }

    let base_text = message.content.to_string();

    if message.attachments.is_empty() {
        return json!({
            "role": role,
            "content": base_text,
        });
    }

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

    // OpenAI vision models often reply with a generic "I don't have access
    // to attached files" when an image content block arrives without any
    // accompanying text block. Always lead with a text part — empty caption
    // becomes a neutral placeholder so the model treats the image as part of
    // a user turn rather than as a standalone, contextless payload.
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

fn serialize_messages(messages: &[Message]) -> Vec<Value> {
    let mut pending_tool_call_ids = BTreeSet::new();
    let mut serialized = Vec::with_capacity(messages.len());
    for message in messages {
        match message.role {
            MessageRole::Assistant if !message.tool_calls.is_empty() => {
                pending_tool_call_ids.clear();
                pending_tool_call_ids.extend(
                    message
                        .tool_calls
                        .iter()
                        .map(|call| call.id.as_str().to_owned()),
                );
                serialized.push(build_message(message));
            }
            MessageRole::Tool => {
                let paired = message
                    .tool_call_id
                    .as_ref()
                    .is_some_and(|id| pending_tool_call_ids.remove(id.as_str()));
                if paired {
                    serialized.push(build_message(message));
                } else {
                    pending_tool_call_ids.clear();
                    serialized.push(orphan_tool_message_as_user(message));
                }
            }
            _ => {
                pending_tool_call_ids.clear();
                serialized.push(build_message(message));
            }
        }
    }
    serialized
}

fn orphan_tool_message_as_user(message: &Message) -> Value {
    let kind = message
        .metadata
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("tool_result");
    let content = format!(
        "Internal AgentOS observation ({kind}):\n{}",
        message.content.as_ref()
    );
    json!({ "role": "user", "content": content })
}

fn serialize_tool_call(call: &ToolCall) -> Value {
    json!({
        "id": call.id.as_str(),
        "type": "function",
        "function": {
            "name": call.name.as_ref(),
            "arguments": call.args.get(),
        }
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
    let url = format!("data:{media_type};base64,{data}");
    Some(json!({
        "type": "image_url",
        "image_url": { "url": url }
    }))
}

/// Translate a document attachment into a Chat Completions content block.
/// PDFs become a native `file` block; text-like documents (.txt, .md, .csv,
/// source code, etc.) are read off disk and inlined as a fenced text block.
/// Pure binary formats (.docx, .xlsx, .zip) return `None` and fall back to
/// the text descriptor — Chat Completions has no generic binary shape.
fn document_block(attachment: &Attachment) -> Option<Value> {
    if let Some(media_type) = document_mime(attachment) {
        if let Ok(data) = read_base64(&attachment.path) {
            let file_data = format!("data:{media_type};base64,{data}");
            return Some(json!({
                "type": "file",
                "file": {
                    "filename": attachment.name.as_ref(),
                    "file_data": file_data,
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
    use agentos_proto::AttachmentKind;
    use serde_json::value::RawValue;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn write_tmp(name: &str, bytes: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("openai-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn text_only_keeps_string_content() {
        let msg = Message::text(MessageRole::User, "hi");
        let value = build_message(&msg);
        assert_eq!(value.get("content").and_then(Value::as_str), Some("hi"));
    }

    #[test]
    fn image_emits_image_url_data_uri() {
        let path = write_tmp("frame.png", &[0x89, 0x50, 0x4e, 0x47]);
        let msg = Message {
            role: MessageRole::User,
            content: Arc::from(""),
            attachments: vec![Attachment {
                kind: AttachmentKind::Image,
                name: Arc::from("frame.png"),
                path,
                mime: Some(Arc::from("image/png")),
                size: Some(4),
                source: None,
            }],
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        let blocks = value.get("content").and_then(Value::as_array).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "text");
        assert!(blocks[0]["text"]
            .as_str()
            .unwrap()
            .contains("without a caption"));
        assert_eq!(blocks[1]["type"], "image_url");
        let url = blocks[1]["image_url"]["url"].as_str().unwrap();
        assert!(url.starts_with("data:image/png;base64,"));
    }

    #[test]
    fn image_with_caption_uses_caption_as_text_block() {
        let path = write_tmp("photo.jpg", &[0xff, 0xd8, 0xff]);
        let msg = Message {
            role: MessageRole::User,
            content: Arc::from("what's in here?"),
            attachments: vec![Attachment {
                kind: AttachmentKind::Image,
                name: Arc::from("photo.jpg"),
                path,
                mime: Some(Arc::from("image/jpeg")),
                size: Some(3),
                source: None,
            }],
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        let blocks = value.get("content").and_then(Value::as_array).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["text"], "what's in here?");
        assert_eq!(blocks[1]["type"], "image_url");
    }

    #[test]
    fn pdf_attachment_emits_file_block() {
        let path = write_tmp("spec.pdf", b"%PDF-1.4 fake");
        let msg = Message {
            role: MessageRole::User,
            content: Arc::from("see attached"),
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
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["text"], "see attached");
        assert_eq!(blocks[1]["type"], "file");
        assert_eq!(blocks[1]["file"]["filename"], "spec.pdf");
        let file_data = blocks[1]["file"]["file_data"].as_str().unwrap();
        assert!(file_data.starts_with("data:application/pdf;base64,"));
    }

    #[test]
    fn text_document_inlined_as_fenced_block() {
        let path = write_tmp("notes.md", b"# heading\nbody text");
        let msg = Message {
            role: MessageRole::User,
            content: Arc::from("summarize"),
            attachments: vec![Attachment {
                kind: AttachmentKind::Document,
                name: Arc::from("notes.md"),
                path,
                mime: None,
                size: Some(19),
                source: None,
            }],
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        let blocks = value.get("content").and_then(Value::as_array).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["text"], "summarize");
        assert_eq!(blocks[1]["type"], "text");
        let inlined = blocks[1]["text"].as_str().unwrap();
        assert!(inlined.starts_with("File: notes.md"));
        assert!(inlined.contains("# heading"));
    }

    #[test]
    fn non_pdf_binary_document_falls_back_to_descriptor() {
        let msg = Message {
            role: MessageRole::User,
            content: Arc::from("see attached"),
            attachments: vec![Attachment {
                kind: AttachmentKind::Document,
                name: Arc::from("notes.docx"),
                path: PathBuf::from("/tmp/notes.docx"),
                mime: Some(Arc::from(
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )),
                size: Some(2048),
                source: None,
            }],
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        let blocks = value.get("content").and_then(Value::as_array).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "text");
        let text = blocks[0]["text"].as_str().unwrap();
        assert!(text.contains("[attached document:"));
        assert!(text.contains("notes.docx"));
    }

    fn raw_args(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_owned()).unwrap()
    }

    #[test]
    fn tool_spec_serializes_as_function_definition() {
        let spec = ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read or write"),
            input_schema: json!({"type":"object","properties":{}}),
            requires_isolation: false,
        };
        let value = tool_to_function(&spec);
        assert_eq!(value["type"], "function");
        assert_eq!(value["function"]["name"], "file");
        assert_eq!(value["function"]["description"], "read or write");
    }

    #[test]
    fn assistant_with_tool_calls_serializes_tool_calls_field() {
        let msg = Message {
            role: MessageRole::Assistant,
            content: Arc::from(""),
            attachments: Vec::new(),
            tool_calls: vec![ToolCall {
                id: ToolCallId::new("call_1"),
                name: Arc::from("file"),
                args: raw_args(r#"{"operation":"write","path":"hi.txt"}"#),
            }],
            tool_call_id: None,
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        assert_eq!(value["role"], "assistant");
        let calls = value["tool_calls"].as_array().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["id"], "call_1");
        assert_eq!(calls[0]["type"], "function");
        assert_eq!(calls[0]["function"]["name"], "file");
    }

    #[test]
    fn tool_role_serializes_as_tool_message() {
        let msg = Message {
            role: MessageRole::Tool,
            content: Arc::from("wrote 42 bytes"),
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: Some(ToolCallId::new("call_1")),
            metadata: Default::default(),
        };
        let value = build_message(&msg);
        assert_eq!(value["role"], "tool");
        assert_eq!(value["tool_call_id"], "call_1");
        assert_eq!(value["content"], "wrote 42 bytes");
    }

    #[test]
    fn paired_tool_result_stays_tool_role() {
        let call = ToolCall {
            id: ToolCallId::new("call_1"),
            name: Arc::from("file"),
            args: raw_args(r#"{"operation":"write","path":"hi.txt"}"#),
        };
        let messages = vec![
            Message {
                role: MessageRole::Assistant,
                content: Arc::from(""),
                attachments: Vec::new(),
                tool_calls: vec![call],
                tool_call_id: None,
                metadata: Default::default(),
            },
            Message {
                role: MessageRole::Tool,
                content: Arc::from("wrote 42 bytes"),
                attachments: Vec::new(),
                tool_calls: Vec::new(),
                tool_call_id: Some(ToolCallId::new("call_1")),
                metadata: Default::default(),
            },
        ];

        let serialized = serialize_messages(&messages);

        assert_eq!(serialized[0]["role"], "assistant");
        assert_eq!(serialized[1]["role"], "tool");
        assert_eq!(serialized[1]["tool_call_id"], "call_1");
    }

    #[test]
    fn orphan_tool_result_becomes_user_context() {
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            Arc::from("kind"),
            Value::String("subagent_result".to_owned()),
        );
        let messages = vec![
            Message::text(MessageRole::User, "call audit-skill"),
            Message {
                role: MessageRole::Tool,
                content: Arc::from("audit-skill completed"),
                attachments: Vec::new(),
                tool_calls: Vec::new(),
                tool_call_id: None,
                metadata,
            },
        ];

        let serialized = serialize_messages(&messages);

        assert_eq!(serialized[0]["role"], "user");
        assert_eq!(serialized[1]["role"], "user");
        let content = serialized[1]["content"].as_str().unwrap();
        assert!(content.contains("Internal AgentOS observation (subagent_result)"));
        assert!(content.contains("audit-skill completed"));
        assert!(serialized[1].get("tool_call_id").is_none());
    }

    #[test]
    fn parse_tool_calls_extracts_function_calls() {
        let response = json!({
            "tool_calls": [{
                "id": "call_abc",
                "type": "function",
                "function": {
                    "name": "file",
                    "arguments": "{\"operation\":\"write\",\"path\":\"a.txt\"}"
                }
            }]
        });
        let calls = parse_tool_calls(&response);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id.as_str(), "call_abc");
        assert_eq!(calls[0].name.as_ref(), "file");
        assert!(calls[0].args.get().contains("\"operation\":\"write\""));
    }
}
