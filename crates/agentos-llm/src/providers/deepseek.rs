use crate::providers::content::append_descriptors;
use crate::providers::stream::openai_compatible_stream;
use crate::providers::{
    attach_token_usage, format_provider_error, log_token_usage, post_json, post_sse,
    raw_args_from_json_str, ProviderError,
};
use crate::CompletionStream;
use agentos_interfaces::tool::ToolSpec;
use agentos_proto::{Message, MessageRole, ToolCall, ToolCallId};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::sync::Arc;

const REASONING_CONTENT_METADATA_KEY: &str = "deepseek.reasoning_content";

pub async fn complete(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<Message, ProviderError> {
    let api_key = env::var("DEEPSEEK_API_KEY").map_err(|_| ProviderError::MissingCredentials {
        variable: "DEEPSEEK_API_KEY",
    })?;
    let base_url = env::var("AGENTOS_DEEPSEEK_BASE_URL")
        .or_else(|_| env::var("DEEPSEEK_BASE_URL"))
        .or_else(|_| env::var("DEEPSEEK_HOST"))
        .unwrap_or_else(|_| "https://api.deepseek.com".to_owned());
    let serialized = serialize_messages(messages);
    let mut payload = json!({
        "model": model,
        "messages": serialized,
        "stream": false
    });
    if !tools.is_empty() {
        // DeepSeek follows OpenAI's Chat Completions shape for function tools.
        payload["tools"] = json!(tools.iter().map(tool_to_function).collect::<Vec<_>>());
    }
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let headers = [
        ("Authorization", format!("Bearer {api_key}")),
        ("Content-Type", "application/json".to_owned()),
    ];
    let mut response = post_json("llm", &url, &headers, &payload).await?;
    if response
        .body
        .get("error")
        .is_some_and(is_reasoning_content_passback_error)
    {
        payload["thinking"] = json!({ "type": "disabled" });
        response = post_json("llm", &url, &headers, &payload).await?;
    }
    if let Some(error) = response.body.get("error") {
        return Err(format_provider_error("DeepSeek", &response, error));
    }
    let token_usage = log_token_usage("deepseek", model, &response.body);
    let message = response
        .body
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .ok_or_else(|| ProviderError::MalformedResponse {
            detail: format!(
                "DeepSeek response missing assistant message: {}",
                response.body
            ),
        })?;
    let mut message = assistant_message_from_value(message);
    if let Some(usage) = token_usage {
        attach_token_usage(&mut message, usage);
    }
    Ok(message)
}

/// Stream a chat completion over SSE. Mirrors [`complete`]'s request with
/// `stream: true` and usage in the final chunk; reasoning models' incremental
/// `reasoning_content` is captured into message metadata, not surfaced as text.
/// The non-stream reasoning-passback retry is intentionally not replicated — a
/// streamed request cannot be safely resent once bytes have flowed.
pub async fn complete_stream(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<CompletionStream, ProviderError> {
    let api_key = env::var("DEEPSEEK_API_KEY").map_err(|_| ProviderError::MissingCredentials {
        variable: "DEEPSEEK_API_KEY",
    })?;
    let base_url = env::var("AGENTOS_DEEPSEEK_BASE_URL")
        .or_else(|_| env::var("DEEPSEEK_BASE_URL"))
        .or_else(|_| env::var("DEEPSEEK_HOST"))
        .unwrap_or_else(|_| "https://api.deepseek.com".to_owned());
    let serialized = serialize_messages(messages);
    let mut payload = json!({
        "model": model,
        "messages": serialized,
        "stream": true,
        "stream_options": { "include_usage": true },
    });
    if !tools.is_empty() {
        payload["tools"] = json!(tools.iter().map(tool_to_function).collect::<Vec<_>>());
    }
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let headers = [
        ("Authorization", format!("Bearer {api_key}")),
        ("Content-Type", "application/json".to_owned()),
    ];
    let response = post_sse("deepseek", &url, &headers, &payload).await?;
    Ok(openai_compatible_stream(
        "deepseek",
        Arc::from(model),
        Some(REASONING_CONTENT_METADATA_KEY),
        response,
    ))
}

fn assistant_message_from_value(message: &Value) -> Message {
    let content = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let tool_calls = parse_tool_calls(message);
    let mut metadata = BTreeMap::new();
    if let Some(reasoning_content) = message.get("reasoning_content").and_then(Value::as_str) {
        metadata.insert(
            Arc::from(REASONING_CONTENT_METADATA_KEY),
            Value::String(reasoning_content.to_owned()),
        );
    }
    Message {
        role: MessageRole::Assistant,
        content: Arc::from(content),
        attachments: Vec::new(),
        tool_calls,
        tool_call_id: None,
        metadata,
    }
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
            let args = raw_args_from_json_str(function.get("arguments"))?;
            Some(ToolCall {
                id: ToolCallId::new(id),
                name: Arc::from(name),
                args,
            })
        })
        .collect()
}

fn flat_message(message: &Message) -> Value {
    // Tool-role: emit as OpenAI-compatible tool message linked to the call id.
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
    // Assistant turn that requested tools: include tool_calls.
    if message.role == MessageRole::Assistant && !message.tool_calls.is_empty() {
        let calls = message
            .tool_calls
            .iter()
            .map(|call| {
                json!({
                    "id": call.id.as_str(),
                    "type": "function",
                    "function": {
                        "name": call.name.as_ref(),
                        "arguments": call.args.get(),
                    }
                })
            })
            .collect::<Vec<_>>();
        let content = if message.content.is_empty() {
            Value::Null
        } else {
            Value::String(message.content.to_string())
        };
        let mut serialized = json!({
            "role": "assistant",
            "content": content,
            "tool_calls": calls,
        });
        append_reasoning_content(message, &mut serialized);
        return serialized;
    }
    let role = match message.role {
        MessageRole::Assistant => "assistant",
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Tool => unreachable!("tool handled above"),
    };
    let base = message.content.to_string();
    let content = append_descriptors(&base, &message.attachments);
    let mut serialized = json!({ "role": role, "content": content });
    append_reasoning_content(message, &mut serialized);
    serialized
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
                serialized.push(flat_message(message));
            }
            MessageRole::Tool => {
                let paired = message
                    .tool_call_id
                    .as_ref()
                    .is_some_and(|id| pending_tool_call_ids.remove(id.as_str()));
                if paired {
                    serialized.push(flat_message(message));
                } else {
                    pending_tool_call_ids.clear();
                    serialized.push(orphan_tool_message_as_user(message));
                }
            }
            _ => {
                pending_tool_call_ids.clear();
                serialized.push(flat_message(message));
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

fn append_reasoning_content(message: &Message, serialized: &mut Value) {
    if message.role != MessageRole::Assistant {
        return;
    }
    let Some(reasoning_content) = message
        .metadata
        .get(REASONING_CONTENT_METADATA_KEY)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    serialized["reasoning_content"] = Value::String(reasoning_content.to_owned());
}

fn is_reasoning_content_passback_error(error: &Value) -> bool {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    message.contains("reasoning_content") && message.contains("must be passed back")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::value::RawValue;

    fn raw_args(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_owned()).unwrap()
    }

    #[test]
    fn assistant_response_preserves_reasoning_content_metadata() {
        let response = json!({
            "content": "I will create the skill.",
            "reasoning_content": "plan the tool call",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {
                    "name": "skill_create",
                    "arguments": "{\"name\":\"audit-skill\"}"
                }
            }]
        });

        let message = assistant_message_from_value(&response);

        assert_eq!(message.content.as_ref(), "I will create the skill.");
        assert_eq!(message.tool_calls.len(), 1);
        assert_eq!(message.tool_calls[0].name.as_ref(), "skill_create");
        assert_eq!(
            message
                .metadata
                .get(REASONING_CONTENT_METADATA_KEY)
                .and_then(Value::as_str),
            Some("plan the tool call")
        );
    }

    #[test]
    fn assistant_tool_call_request_echoes_reasoning_content() {
        let mut metadata = BTreeMap::new();
        metadata.insert(
            Arc::from(REASONING_CONTENT_METADATA_KEY),
            Value::String("reason through tool selection".to_owned()),
        );
        let message = Message {
            role: MessageRole::Assistant,
            content: Arc::from(""),
            attachments: Vec::new(),
            tool_calls: vec![ToolCall {
                id: ToolCallId::new("call_1"),
                name: Arc::from("skill_create"),
                args: raw_args(r#"{"name":"audit-skill"}"#),
            }],
            tool_call_id: None,
            metadata,
        };

        let serialized = flat_message(&message);

        assert_eq!(serialized["role"], "assistant");
        assert_eq!(serialized["content"], Value::Null);
        assert_eq!(
            serialized["reasoning_content"],
            "reason through tool selection"
        );
        assert_eq!(
            serialized["tool_calls"][0]["function"]["name"],
            "skill_create"
        );
    }

    #[test]
    fn non_assistant_request_does_not_emit_reasoning_content() {
        let mut message = Message::text(MessageRole::User, "create a skill");
        message.metadata.insert(
            Arc::from(REASONING_CONTENT_METADATA_KEY),
            Value::String("should not leak".to_owned()),
        );

        let serialized = flat_message(&message);

        assert!(serialized.get("reasoning_content").is_none());
    }

    #[test]
    fn detects_reasoning_content_passback_errors() {
        let error = json!({
            "code": "invalid_request_error",
            "message": "The `reasoning_content` in the thinking mode must be passed back to the API.",
            "type": "invalid_request_error"
        });

        assert!(is_reasoning_content_passback_error(&error));
        assert!(!is_reasoning_content_passback_error(&json!({
            "message": "missing API key"
        })));
    }

    #[test]
    fn paired_tool_result_stays_tool_role() {
        let call = ToolCall {
            id: ToolCallId::new("call_1"),
            name: Arc::from("skill_create"),
            args: raw_args(r#"{"name":"audit-skill"}"#),
        };
        let messages = vec![
            Message {
                role: MessageRole::Assistant,
                content: Arc::from(""),
                attachments: Vec::new(),
                tool_calls: vec![call],
                tool_call_id: None,
                metadata: BTreeMap::new(),
            },
            Message {
                role: MessageRole::Tool,
                content: Arc::from("created audit-skill"),
                attachments: Vec::new(),
                tool_calls: Vec::new(),
                tool_call_id: Some(ToolCallId::new("call_1")),
                metadata: BTreeMap::new(),
            },
        ];

        let serialized = serialize_messages(&messages);

        assert_eq!(serialized[0]["role"], "assistant");
        assert_eq!(serialized[1]["role"], "tool");
        assert_eq!(serialized[1]["tool_call_id"], "call_1");
    }

    #[test]
    fn orphan_tool_result_becomes_user_context() {
        let mut metadata = BTreeMap::new();
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
}
