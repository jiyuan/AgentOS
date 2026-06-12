use crate::providers::content::append_descriptors;
use crate::providers::{
    attach_token_usage, log_token_usage, post_json, raw_args_from_json_value, ProviderError,
};
use agentos_interfaces::tool::ToolSpec;
use agentos_proto::{Message, MessageRole, ToolCall, ToolCallId};
use serde_json::{json, Value};
use std::env;
use std::sync::Arc;

pub async fn complete(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<Message, ProviderError> {
    let host = env::var("OLLAMA_HOST").unwrap_or_else(|_| "http://localhost:11434".to_owned());
    let serialized = messages.iter().map(flat_message).collect::<Vec<_>>();
    let mut payload = json!({
        "model": model,
        "messages": serialized,
        "stream": false
    });
    if !tools.is_empty() {
        // Ollama's tools support is model-dependent. Pass them through;
        // models that don't support tools simply ignore the field.
        payload["tools"] = json!(tools.iter().map(tool_to_function).collect::<Vec<_>>());
    }
    let response = post_json(
        "llm",
        &format!("{}/api/chat", host.trim_end_matches('/')),
        &[("Content-Type", "application/json".to_owned())],
        &payload,
    )
    .await?;
    let message_value =
        response
            .body
            .get("message")
            .ok_or_else(|| ProviderError::MalformedResponse {
                detail: format!("Ollama response missing message: {}", response.body),
            })?;
    let content = message_value
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let tool_calls = parse_tool_calls(message_value);
    let token_usage = log_token_usage("ollama", model, &response.body);
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
            let function = call.get("function")?;
            let name = function.get("name").and_then(Value::as_str)?;
            // Ollama returns `function.arguments` as a JSON object directly,
            // not a string. Serialize it into a RawValue (no clone, no
            // re-parse) so it round-trips identically to OpenAI/DeepSeek
            // through the rest of the pipeline.
            let args = raw_args_from_json_value(function.get("arguments"))?;
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("ollama-{name}"));
            Some(ToolCall {
                id: ToolCallId::new(id),
                name: Arc::from(name),
                args,
            })
        })
        .collect()
}

fn flat_message(message: &Message) -> Value {
    if message.role == MessageRole::Tool {
        // Ollama accepts a "tool" role for results; the tool_call_id pairs it
        // with the assistant's preceding tool_calls.
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
    if message.role == MessageRole::Assistant && !message.tool_calls.is_empty() {
        let calls = message
            .tool_calls
            .iter()
            .map(|call| {
                let args_value: Value =
                    serde_json::from_str(call.args.get()).unwrap_or_else(|_| json!({}));
                json!({
                    "function": {
                        "name": call.name.as_ref(),
                        "arguments": args_value,
                    }
                })
            })
            .collect::<Vec<_>>();
        return json!({
            "role": "assistant",
            "content": message.content.as_ref(),
            "tool_calls": calls,
        });
    }
    let role = match message.role {
        MessageRole::Assistant => "assistant",
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Tool => unreachable!("tool handled above"),
    };
    let base = message.content.to_string();
    let content = append_descriptors(&base, &message.attachments);
    json!({ "role": role, "content": content })
}
