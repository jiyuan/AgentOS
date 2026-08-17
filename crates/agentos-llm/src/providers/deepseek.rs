use crate::providers::chat_completions::record_reply_outcome;
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

/// Stand-in body for a tool result that produced no output. A `role: "tool"`
/// message with empty content is not a shape the API promises to accept, and
/// tool results are frequently empty — a successful write, a command with no
/// stdout.
const NO_TOOL_OUTPUT: &str = "(no output)";

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
    let choice = response
        .body
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or_else(|| ProviderError::MalformedResponse {
            detail: format!("DeepSeek response carried no choices: {}", response.body),
        })?;
    let mut message = reply_from_choice(choice)?;
    if let Some(usage) = token_usage {
        attach_token_usage(&mut message, usage);
    }
    Ok(message)
}

/// Assemble and validate the assistant message from one buffered `choices[]`
/// entry — the buffered counterpart of the streaming accumulator's `finalize`,
/// applying the same [`record_reply_outcome`] rules to the same wire fields.
fn reply_from_choice(choice: &Value) -> Result<Message, ProviderError> {
    let raw_message = choice
        .get("message")
        .ok_or_else(|| ProviderError::MalformedResponse {
            detail: format!("DeepSeek choice carried no assistant message: {choice}"),
        })?;
    // Counted before parsing, because parsing is where a truncated call is
    // lost: `raw_args_from_json_str` validates the argument JSON and yields
    // `None` for a half-written object, and `parse_tool_calls` then drops it.
    let announced = raw_message
        .get("tool_calls")
        .and_then(Value::as_array)
        .map(|calls| {
            calls
                .iter()
                .filter(|call| {
                    call.get("id").and_then(Value::as_str).is_some()
                        && call
                            .get("function")
                            .and_then(|function| function.get("name"))
                            .and_then(Value::as_str)
                            .is_some()
                })
                .count()
        })
        .unwrap_or(0);
    let mut message = assistant_message_from_value(raw_message);
    let produced_reasoning = message
        .metadata
        .contains_key(REASONING_CONTENT_METADATA_KEY);
    record_reply_outcome(
        "deepseek",
        &mut message,
        choice.get("finish_reason").and_then(Value::as_str),
        announced,
        produced_reasoning,
    )
    .map_err(|defect| ProviderError::MalformedResponse {
        detail: defect.to_string(),
    })?;
    Ok(message)
}

/// Stream a chat completion over SSE. Mirrors [`complete`]'s request with
/// `stream: true` and usage in the final chunk; reasoning models' incremental
/// `reasoning_content` is captured into message metadata, not surfaced as text.
///
/// The reasoning-passback rejection arrives as an HTTP 400 *before* any stream
/// bytes flow (surfaced by `post_sse`, not mid-stream), so — like the buffered
/// path — we can safely disable thinking and resend once.
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
    let response = match post_sse("deepseek", &url, &headers, &payload).await {
        Ok(response) => response,
        Err(err) if is_reasoning_passback_text(&err.to_string()) => {
            // Pre-stream HTTP 400 — no bytes flowed, so resending is safe.
            payload["thinking"] = json!({ "type": "disabled" });
            post_sse("deepseek", &url, &headers, &payload).await?
        }
        Err(err) => return Err(err),
    };
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
    // Non-empty only: thinking mode's first reasoning field is an empty string,
    // and an empty trace is not output — it must not make an otherwise empty
    // reply look like the model produced something.
    if let Some(reasoning_content) = message
        .get("reasoning_content")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
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
        // A tool that produced nothing still has to say so: an empty string
        // here is a message with no content, which the API is entitled to
        // reject, and a rejected history message fails every later turn of the
        // conversation rather than just this one.
        let content = if message.content.is_empty() {
            NO_TOOL_OUTPUT
        } else {
            message.content.as_ref()
        };
        return json!({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": content,
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
        // Empty content on a tool-call turn serializes as `""`, never `null`.
        // The OpenAI schema permits null, but DeepSeek-compatible gateways are
        // not uniform about it and a rejected assistant message is durable —
        // it sits in the session log and fails every subsequent turn. `""` is
        // what DeepSeek's own samples replay.
        let mut serialized = json!({
            "role": "assistant",
            "content": message.content.as_ref(),
            "tool_calls": calls,
        });
        // Thinking mode's passback rule is specific: `reasoning_content` must
        // be returned on assistant turns that carried tool calls, and is
        // ignored on turns that did not. Replaying it on plain turns therefore
        // buys nothing and costs the whole reasoning trace in prompt tokens on
        // every subsequent request, so this is the only branch that sends it.
        if let Some(reasoning) = message
            .metadata
            .get(REASONING_CONTENT_METADATA_KEY)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            serialized["reasoning_content"] = Value::String(reasoning.to_owned());
        }
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
    json!({ "role": role, "content": content })
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

fn is_reasoning_content_passback_error(error: &Value) -> bool {
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    is_reasoning_passback_text(message)
}

/// True when an error message is DeepSeek's thinking-mode reasoning-passback
/// rejection. Used for both the structured buffered error object and the
/// streaming [`ProviderError`]'s rendered detail (which embeds the same text).
fn is_reasoning_passback_text(message: &str) -> bool {
    message.contains("reasoning_content") && message.contains("must be passed back")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::chat_completions::STOP_REASON_METADATA_KEY;
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
        assert_eq!(serialized["content"], "");
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
    fn a_buffered_reply_cut_off_mid_tool_call_is_rejected() {
        // `length` truncates the argument JSON. Parsing drops the call, and a
        // dropped call is a turn that asked for nothing — the run loop would
        // finish as if the model had chosen to stop.
        let choice = json!({
            "finish_reason": "length",
            "message": {
                "content": "",
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": { "name": "file", "arguments": "{\"path\":\"/etc/ho" }
                }]
            }
        });

        let error = reply_from_choice(&choice).expect_err("a lost tool call is not a valid reply");

        let rendered = error.to_string();
        assert!(rendered.contains("announced 1 tool call(s)"), "{rendered}");
        assert!(rendered.contains("finish_reason=length"), "{rendered}");
    }

    #[test]
    fn a_buffered_reply_with_nothing_in_it_is_rejected() {
        let choice = json!({ "finish_reason": "stop", "message": { "content": "" } });

        let error = reply_from_choice(&choice).expect_err("an empty reply is not a finished turn");

        assert!(error.to_string().contains("no content"), "{error}");
    }

    #[test]
    fn an_empty_reasoning_trace_does_not_make_an_empty_reply_look_productive() {
        // Thinking mode opens with `reasoning_content: ""`. Treating a present
        // but empty field as output would let a reply with nothing in it pass
        // the emptiness check.
        let choice = json!({
            "finish_reason": "stop",
            "message": { "content": "", "reasoning_content": "" }
        });

        let error = reply_from_choice(&choice).expect_err("an empty trace is not output");

        assert!(error.to_string().contains("no reasoning"), "{error}");
    }

    #[test]
    fn a_reasoning_only_buffered_reply_is_accepted() {
        let choice = json!({
            "finish_reason": "stop",
            "message": { "content": "", "reasoning_content": "it is four" }
        });

        let message = reply_from_choice(&choice).expect("reasoning is output too");

        assert_eq!(
            message
                .metadata
                .get(REASONING_CONTENT_METADATA_KEY)
                .and_then(Value::as_str),
            Some("it is four")
        );
    }

    #[test]
    fn a_buffered_reply_truncated_after_its_text_keeps_the_text_and_the_reason() {
        let choice = json!({
            "finish_reason": "length",
            "message": { "content": "the answer is fo" }
        });

        let message = reply_from_choice(&choice).expect("truncated text is still usable");

        assert_eq!(message.content.as_ref(), "the answer is fo");
        assert_eq!(
            message
                .metadata
                .get(STOP_REASON_METADATA_KEY)
                .and_then(Value::as_str),
            Some("length")
        );
    }

    #[test]
    fn an_ordinary_buffered_reply_gains_no_metadata() {
        let choice = json!({
            "finish_reason": "stop",
            "message": { "content": "the answer is four" }
        });

        let message = reply_from_choice(&choice).expect("an ordinary reply is not a defect");

        assert!(message.metadata.is_empty());
    }

    #[test]
    fn a_plain_assistant_turn_does_not_replay_its_reasoning() {
        // Thinking mode requires the passback only on tool-call turns and
        // ignores it elsewhere, so replaying a whole reasoning trace on a plain
        // turn would be paid for in prompt tokens on every later request.
        let mut message = Message::text(MessageRole::Assistant, "the answer is four");
        message.metadata.insert(
            Arc::from(REASONING_CONTENT_METADATA_KEY),
            Value::String("a long chain of thought".to_owned()),
        );

        let serialized = flat_message(&message);

        assert_eq!(serialized["content"], "the answer is four");
        assert!(serialized.get("reasoning_content").is_none());
    }

    #[test]
    fn an_empty_tool_result_still_says_something() {
        let message = Message {
            role: MessageRole::Tool,
            content: Arc::from(""),
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: Some(ToolCallId::new("call_1")),
            metadata: BTreeMap::new(),
        };

        let serialized = flat_message(&message);

        assert_eq!(serialized["role"], "tool");
        assert_eq!(serialized["content"], NO_TOOL_OUTPUT);
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
    fn detects_reasoning_passback_in_streaming_error_detail() {
        // The streaming path matches on the rendered ProviderError detail, which
        // embeds the error object's text plus an http_status suffix.
        let detail = "deepseek streaming error: {\"code\":\"invalid_request_error\",\
            \"message\":\"The `reasoning_content` in the thinking mode must be passed \
            back to the API.\"}; http_status=400";
        assert!(is_reasoning_passback_text(detail));
        assert!(!is_reasoning_passback_text(
            "deepseek streaming error: rate limited; http_status=429"
        ));
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
