//! Responses API request and response mapping, including server-side sessions.
//!
//! The wire shape: a flat `input` item list (messages, `function_call`,
//! `function_call_output`) instead of chat `messages` with nested tool fields,
//! tool definitions without a `function` wrapper, and an `output` item list
//! instead of `choices[].message`.
//!
//! Sessions: OpenAI stores each response, so a turn can chain onto the previous
//! one with `previous_response_id` and send only what is new. The pointer rides
//! on the assistant message's own metadata rather than in provider-side state,
//! so it survives `RunState` serialization, pause/resume, and sub-agent
//! transcripts exactly as the messages it annotates do.

use super::{
    api_error, endpoint_and_headers, events, post_sse_with_param_fixes, post_with_param_fixes,
};
use crate::providers::content::{
    append_descriptors, document_mime, format_text_document, image_mime, read_base64,
    read_text_document,
};
use crate::providers::{
    attach_token_usage, format_openai_error, log_token_usage, raw_args_from_json_str, ProviderError,
};
use crate::CompletionStream;
use agentos_interfaces::tool::ToolSpec;
use agentos_proto::{Attachment, AttachmentKind, Message, MessageRole, ToolCall, ToolCallId};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::env;
use std::sync::{Arc, OnceLock};

pub(super) async fn complete(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<Message, ProviderError> {
    let (url, headers) = endpoint_and_headers()?;
    let resume = resume_point(model, messages);
    let mut payload = base_payload(model, messages, tools, resume);
    let mut response = post_with_param_fixes(model, &url, &headers, &mut payload).await?;
    if resume.is_some() && stale_session_error(&response.body) {
        payload = base_payload(model, messages, tools, None);
        response = post_with_param_fixes(model, &url, &headers, &mut payload).await?;
    }
    if let Some(error) = api_error(&response.body) {
        return Err(format_openai_error(&response, error));
    }
    let output = response
        .body
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| ProviderError::MalformedResponse {
            detail: format!("OpenAI Responses reply missing output: {}", response.body),
        })?;
    let token_usage = log_token_usage("openai", model, &response.body);
    let mut message = assistant_message_from_output(output);
    stamp_session(
        &mut message,
        response.body.get("id").and_then(Value::as_str),
        model,
        messages.len(),
    );
    if let Some(usage) = token_usage {
        attach_token_usage(&mut message, usage);
    }
    Ok(message)
}

/// Stream a turn over SSE. Unlike Chat Completions there is no `stream_options`
/// opt-in: usage rides on the terminal `response.completed` event, which carries
/// the whole assembled response object.
pub(super) async fn complete_stream(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
) -> Result<CompletionStream, ProviderError> {
    let (url, headers) = endpoint_and_headers()?;
    let resume = resume_point(model, messages);
    let mut payload = base_payload(model, messages, tools, resume);
    payload["stream"] = json!(true);
    let response = match post_sse_with_param_fixes(model, &url, &headers, &mut payload).await {
        Ok(response) => response,
        Err(err) if resume.is_some() && is_stale_session(&err.to_string()) => {
            // Pre-stream HTTP 400 — no bytes flowed, so replaying the full
            // transcript in a fresh request is safe.
            let mut payload = base_payload(model, messages, tools, None);
            payload["stream"] = json!(true);
            post_sse_with_param_fixes(model, &url, &headers, &mut payload).await?
        }
        Err(err) => return Err(err),
    };
    Ok(events::stream(model, messages.len(), response))
}

fn base_payload(
    model: &str,
    messages: &[Message],
    tools: &[ToolSpec],
    resume: Option<Resume<'_>>,
) -> Value {
    let (input, previous_response_id) = match resume {
        // Everything through the anchor is already server-side, so only what
        // followed it is new. The anchor's own tool calls seed the pairing set:
        // their `function_call` items were stored with that response, so the
        // results that follow are paired even though the calls aren't resent.
        Some(resume) => (
            serialize_input(
                &messages[resume.anchor + 1..],
                messages[resume.anchor]
                    .tool_calls
                    .iter()
                    .map(|call| call.id.as_str().to_owned())
                    .collect(),
            ),
            Some(resume.response_id),
        ),
        None => (serialize_input(messages, BTreeSet::new()), None),
    };
    let mut payload = json!({
        "model": model,
        "input": input,
        "tools": tools.iter().map(tool_to_function).collect::<Vec<_>>(),
        // One call per turn — the loop iterates so we don't need parallelism.
        "parallel_tool_calls": false,
        "store": stateful_sessions(),
        "temperature": 0.7,
    });
    if let Some(previous_response_id) = previous_response_id {
        payload["previous_response_id"] = json!(previous_response_id);
    }
    payload
}

/// Responses tool definitions are flat: name, description, and parameters sit
/// on the tool itself rather than under a `function` object. `strict` is pinned
/// off because strict mode demands `additionalProperties: false` plus every
/// property marked required, which extension-supplied schemas don't guarantee.
fn tool_to_function(spec: &ToolSpec) -> Value {
    json!({
        "type": "function",
        "name": spec.name.as_ref(),
        "description": spec.description.as_ref(),
        "parameters": spec.input_schema,
        "strict": false,
    })
}

/// Flatten transcript messages into `input` items. Tool calls and their results
/// become sibling `function_call` / `function_call_output` items rather than
/// nested message fields, but the pairing rule matches Chat Completions': an
/// output whose `call_id` has no matching call is rejected, so orphans degrade
/// to a plain user observation. `carried` holds call ids that are already
/// server-side from the resumed response.
fn serialize_input(messages: &[Message], carried: BTreeSet<String>) -> Vec<Value> {
    let mut pending_call_ids = carried;
    let mut items = Vec::with_capacity(messages.len());
    for message in messages {
        match message.role {
            MessageRole::Assistant if !message.tool_calls.is_empty() => {
                pending_call_ids.clear();
                if !message.content.is_empty() {
                    items.push(json!({
                        "role": "assistant",
                        "content": message.content.as_ref(),
                    }));
                }
                for call in &message.tool_calls {
                    pending_call_ids.insert(call.id.as_str().to_owned());
                    items.push(json!({
                        "type": "function_call",
                        "call_id": call.id.as_str(),
                        "name": call.name.as_ref(),
                        // Arguments ride as a JSON string, as the model emitted them.
                        "arguments": call.args.get(),
                    }));
                }
            }
            MessageRole::Tool => {
                let call_id = message
                    .tool_call_id
                    .as_ref()
                    .filter(|id| pending_call_ids.remove(id.as_str()));
                match call_id {
                    Some(id) => items.push(json!({
                        "type": "function_call_output",
                        "call_id": id.as_str(),
                        "output": message.content.as_ref(),
                    })),
                    None => {
                        pending_call_ids.clear();
                        items.push(orphan_tool_message_as_user(message));
                    }
                }
            }
            _ => {
                pending_call_ids.clear();
                items.push(input_message(message));
            }
        }
    }
    items
}

/// A tool result with no call to pair with — a sub-agent result, or a resumed
/// transcript whose assistant turn was dropped. Sending it as a tool output
/// would be rejected, so it becomes labelled user context instead.
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

/// One non-tool turn as an input message. Plain text rides as a bare string;
/// only user turns get structured content parts, since `input_image` and
/// `input_file` are rejected on assistant and system items.
fn input_message(message: &Message) -> Value {
    let role = match message.role {
        MessageRole::Assistant => "assistant",
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Tool => unreachable!("tool handled by serialize_input"),
    };
    let text = message.content.to_string();
    if message.attachments.is_empty() {
        return json!({ "role": role, "content": text });
    }
    if message.role != MessageRole::User {
        return json!({
            "role": role,
            "content": append_descriptors(&text, &message.attachments),
        });
    }

    let mut parts: Vec<Value> = Vec::new();
    let mut fallback: Vec<Attachment> = Vec::new();
    for attachment in &message.attachments {
        match content_part_for(attachment) {
            Some(part) => parts.push(part),
            None => fallback.push(attachment.clone()),
        }
    }
    let leading = if fallback.is_empty() {
        text
    } else {
        append_descriptors(&text, &fallback)
    };
    // Lead with a text part: an image arriving with no accompanying text draws
    // a generic "I don't have access to attached files" reply often enough to
    // be worth a neutral placeholder.
    let leading = if leading.is_empty() {
        "(user attached files without a caption)".to_owned()
    } else {
        leading
    };
    let mut content = Vec::with_capacity(parts.len() + 1);
    content.push(json!({ "type": "input_text", "text": leading }));
    content.extend(parts);
    json!({ "role": role, "content": content })
}

/// Translate one attachment into a content part. Images and PDFs go native;
/// text-like documents inline as text; pure binaries (.docx, .zip) have no
/// Responses shape and return `None` for the descriptor fallback.
fn content_part_for(attachment: &Attachment) -> Option<Value> {
    match attachment.kind {
        AttachmentKind::Image => {
            let media_type = image_mime(attachment)?;
            let data = read_base64(&attachment.path).ok()?;
            Some(json!({
                "type": "input_image",
                "image_url": format!("data:{media_type};base64,{data}"),
            }))
        }
        AttachmentKind::Document => {
            if let Some(media_type) = document_mime(attachment) {
                if let Ok(data) = read_base64(&attachment.path) {
                    return Some(json!({
                        "type": "input_file",
                        "filename": attachment.name.as_ref(),
                        "file_data": format!("data:{media_type};base64,{data}"),
                    }));
                }
            }
            let body = read_text_document(attachment)?.ok()?;
            Some(json!({
                "type": "input_text",
                "text": format_text_document(&attachment.name, &body),
            }))
        }
    }
}

/// Assemble the assistant turn from the response's `output` items. Text lives
/// in `message` items' `output_text` parts; tool calls are top-level
/// `function_call` items. `reasoning` items carry no assistant-visible text and
/// stay server-side, which is what makes a resumed session worth having.
fn assistant_message_from_output(output: &[Value]) -> Message {
    let mut content = String::new();
    let mut tool_calls = Vec::new();
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                let parts = item.get("content").and_then(Value::as_array);
                for part in parts.into_iter().flatten() {
                    if part.get("type").and_then(Value::as_str) == Some("output_text") {
                        if let Some(text) = part.get("text").and_then(Value::as_str) {
                            content.push_str(text);
                        }
                    }
                }
            }
            Some("function_call") => {
                // `call_id` is the pairing key a later `function_call_output`
                // must echo; the item's own `id` is not accepted for that.
                let id = item.get("call_id").and_then(Value::as_str);
                let name = item.get("name").and_then(Value::as_str);
                let args = raw_args_from_json_str(item.get("arguments"));
                if let (Some(id), Some(name), Some(args)) = (id, name, args) {
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
    Message {
        role: MessageRole::Assistant,
        content: Arc::from(content),
        attachments: Vec::new(),
        tool_calls,
        tool_call_id: None,
        metadata: Default::default(),
    }
}

/// Metadata key holding the server-side session pointer stamped onto every
/// assistant reply OpenAI stored: `{response_id, model, prefix_len}`.
const SESSION_METADATA_KEY: &str = "openai.session";

/// Env var disabling server-side session state (`0`, `false`, `off`, `no`).
const STATEFUL_ENV: &str = "AGENTOS_OPENAI_STATEFUL";

/// Whether to keep conversation state server-side. On by default: chaining
/// `previous_response_id` keeps a reasoning model's earlier reasoning items
/// alive across tool calls — the loop's whole tool cycle stops re-deriving its
/// plan from scratch — and the transcript prefix stops being re-uploaded every
/// turn. It requires `store: true`, i.e. OpenAI retains the conversation, so
/// `AGENTOS_OPENAI_STATEFUL=0` switches both off together and every request
/// carries its own full history.
fn stateful_sessions() -> bool {
    // Read once: the setting is process configuration, and every request
    // consults it three times (payload, resume lookup, stamp).
    static STATEFUL: OnceLock<bool> = OnceLock::new();
    *STATEFUL.get_or_init(|| {
        !env::var(STATEFUL_ENV).is_ok_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
    })
}

/// A stored response to continue from: everything up to and including the
/// anchor assistant turn already lives server-side.
#[derive(Clone, Copy)]
struct Resume<'a> {
    response_id: &'a str,
    /// Index of the anchor assistant message in the caller's message list.
    anchor: usize,
}

/// Find the newest assistant turn whose stored response this request can chain
/// onto, or `None` to send the whole transcript.
fn resume_point<'a>(model: &str, messages: &'a [Message]) -> Option<Resume<'a>> {
    if !stateful_sessions() {
        return None;
    }
    let (anchor, session) = messages
        .iter()
        .enumerate()
        .rev()
        .find_map(|(index, message)| Some((index, message.metadata.get(SESSION_METADATA_KEY)?)))?;
    // A pointer only applies to the model that produced it: `/model` can switch
    // models mid-conversation, and stored reasoning items are not portable.
    if session.get("model").and_then(Value::as_str) != Some(model) {
        return None;
    }
    // Position is the integrity check. This provider always sends the entire
    // prefix, so an anchor that no longer sits where its own request ended means
    // the history ahead of it was rewritten — compaction, an ephemeral run, a
    // replayed transcript — and the stored context is not what the caller means.
    let prefix_len = session.get("prefix_len").and_then(Value::as_u64)? as usize;
    if prefix_len != anchor {
        return None;
    }
    Some(Resume {
        response_id: session.get("response_id").and_then(Value::as_str)?,
        anchor,
    })
}

/// Record the stored response on the assistant message so the next turn can
/// chain onto it. `prefix_len` is the number of messages this request was built
/// from, which is where this reply will sit in the next request's list.
pub(super) fn stamp_session(
    message: &mut Message,
    response_id: Option<&str>,
    model: &str,
    prefix_len: usize,
) {
    if !stateful_sessions() {
        return;
    }
    let Some(response_id) = response_id else {
        return;
    };
    message.metadata.insert(
        Arc::from(SESSION_METADATA_KEY),
        json!({
            "response_id": response_id,
            "model": model,
            "prefix_len": prefix_len,
        }),
    );
}

fn stale_session_error(body: &Value) -> bool {
    api_error(body)
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .is_some_and(is_stale_session)
}

/// True when the error says the stored response we chained onto is gone —
/// expired (OpenAI keeps stored responses for 30 days), deleted, or created
/// under other credentials. Recovery is always available: the transcript is the
/// source of truth and the server copy only ever a cache of it.
fn is_stale_session(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("previous response")
        && (message.contains("not found") || message.contains("expired"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::value::RawValue;
    use std::fs;
    use std::path::PathBuf;

    fn raw_args(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_owned()).unwrap()
    }

    fn tool_call(id: &str, name: &str, args: &str) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id),
            name: Arc::from(name),
            args: raw_args(args),
        }
    }

    fn assistant_calling(call: ToolCall) -> Message {
        Message {
            role: MessageRole::Assistant,
            content: Arc::from(""),
            attachments: Vec::new(),
            tool_calls: vec![call],
            tool_call_id: None,
            metadata: Default::default(),
        }
    }

    fn tool_result(call_id: &str, content: &str) -> Message {
        Message {
            role: MessageRole::Tool,
            content: Arc::from(content),
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: Some(ToolCallId::new(call_id)),
            metadata: Default::default(),
        }
    }

    #[test]
    fn tool_spec_serializes_without_function_wrapper() {
        let spec = ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("read or write"),
            input_schema: json!({"type":"object","properties":{}}),
            requires_isolation: false,
        };
        assert_eq!(
            tool_to_function(&spec),
            json!({
                "type": "function",
                "name": "file",
                "description": "read or write",
                "parameters": {"type":"object","properties":{}},
                "strict": false,
            })
        );
    }

    #[test]
    fn payload_carries_input_items_not_messages() {
        let payload = base_payload(
            "gpt-5.6-luna",
            &[Message::text(MessageRole::User, "hi")],
            &[],
            None,
        );
        assert_eq!(
            payload["input"][0],
            json!({ "role": "user", "content": "hi" })
        );
        assert!(payload.get("messages").is_none());
        assert!(payload.get("previous_response_id").is_none());
        assert_eq!(payload["parallel_tool_calls"], false);
    }

    #[test]
    fn tool_call_and_result_become_sibling_items() {
        let messages = vec![
            assistant_calling(tool_call("call_1", "file", r#"{"path":"a.txt"}"#)),
            tool_result("call_1", "wrote 42 bytes"),
        ];

        assert_eq!(
            serialize_input(&messages, BTreeSet::new()),
            vec![
                json!({
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "file",
                    "arguments": r#"{"path":"a.txt"}"#,
                }),
                json!({
                    "type": "function_call_output",
                    "call_id": "call_1",
                    "output": "wrote 42 bytes",
                }),
            ]
        );
    }

    #[test]
    fn assistant_text_alongside_tool_call_leads_the_call_item() {
        let mut message = assistant_calling(tool_call("call_1", "file", "{}"));
        message.content = Arc::from("writing the file now");

        let items = serialize_input(std::slice::from_ref(&message), BTreeSet::new());

        assert_eq!(
            items[0],
            json!({ "role": "assistant", "content": "writing the file now" })
        );
        assert_eq!(items[1]["type"], "function_call");
    }

    #[test]
    fn orphan_tool_result_becomes_user_context() {
        // No matching function_call, so the output would be rejected on the wire.
        let messages = vec![
            Message::text(MessageRole::User, "call audit-skill"),
            tool_result("call_missing", "audit-skill completed"),
        ];

        let items = serialize_input(&messages, BTreeSet::new());

        assert_eq!(items[1]["role"], "user");
        assert!(items[1]["content"]
            .as_str()
            .unwrap()
            .contains("audit-skill completed"));
        assert!(items[1].get("call_id").is_none());
    }

    #[test]
    fn carried_call_ids_pair_results_from_a_resumed_response() {
        // The call itself was stored server-side with the resumed response, so
        // only its result is resent — and must still pair.
        let carried = BTreeSet::from(["call_1".to_owned()]);

        let items = serialize_input(&[tool_result("call_1", "wrote 42 bytes")], carried);

        assert_eq!(items[0]["type"], "function_call_output");
        assert_eq!(items[0]["call_id"], "call_1");
    }

    #[test]
    fn image_attachment_becomes_input_image_part() {
        let dir = std::env::temp_dir().join(format!("responses-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("frame.png");
        fs::write(&path, [0x89, 0x50, 0x4e, 0x47]).unwrap();
        let message = Message {
            role: MessageRole::User,
            content: Arc::from("what's this?"),
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

        let content = input_message(&message);
        let content = content["content"].as_array().unwrap();

        assert_eq!(
            content[0],
            json!({ "type": "input_text", "text": "what's this?" })
        );
        assert_eq!(content[1]["type"], "input_image");
        assert!(content[1]["image_url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));
    }

    #[test]
    fn binary_document_falls_back_to_descriptor_text() {
        let message = Message {
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

        let item = input_message(&message);

        let content = item["content"].as_array().unwrap();
        assert_eq!(content.len(), 1);
        assert!(content[0]["text"]
            .as_str()
            .unwrap()
            .contains("[attached document:"));
    }

    #[test]
    fn buffered_output_yields_text_and_tool_calls() {
        let output = json!([
            { "type": "reasoning", "id": "rs_1", "summary": [] },
            {
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "content": [{ "type": "output_text", "text": "checking the file" }]
            },
            {
                "type": "function_call",
                "id": "fc_1",
                "call_id": "call_1",
                "name": "file",
                "arguments": "{\"path\":\"a.txt\"}"
            }
        ]);

        let message = assistant_message_from_output(output.as_array().unwrap());

        assert_eq!(message.content.as_ref(), "checking the file");
        assert_eq!(message.tool_calls.len(), 1);
        // The pairing key is call_id, never the item's own fc_ id.
        assert_eq!(message.tool_calls[0].id.as_str(), "call_1");
        assert_eq!(message.tool_calls[0].name.as_ref(), "file");
        assert_eq!(message.tool_calls[0].args.get(), r#"{"path":"a.txt"}"#);
    }

    // --- server-side sessions ---

    /// A transcript whose last assistant turn was stamped by a request built
    /// from `prefix_len` messages.
    fn stamped(model: &str, prefix_len: usize) -> Message {
        let mut message = Message::text(MessageRole::Assistant, "on it");
        stamp_session(&mut message, Some("resp_1"), model, prefix_len);
        message
    }

    #[test]
    fn resumed_turn_sends_only_what_followed_the_stored_response() {
        // Turn 1 was built from one message and its reply — a tool call — was
        // stamped, so the reply sits at index 1 and only the result is new.
        // The call itself stays server-side but must still pair with it.
        let mut anchor = assistant_calling(tool_call("call_1", "file", "{}"));
        stamp_session(&mut anchor, Some("resp_1"), "gpt-5.6-luna", 1);
        let messages = vec![
            Message::text(MessageRole::User, "read a.txt"),
            anchor,
            tool_result("call_1", "42 bytes"),
        ];

        let payload = base_payload(
            "gpt-5.6-luna",
            &messages,
            &[],
            resume_point("gpt-5.6-luna", &messages),
        );

        assert_eq!(payload["previous_response_id"], "resp_1");
        assert_eq!(payload["store"], true);
        let input = payload["input"].as_array().unwrap();
        assert_eq!(input.len(), 1, "only the new item is resent: {input:?}");
        assert_eq!(input[0]["output"], "42 bytes");
    }

    #[test]
    fn resume_is_refused_when_the_model_changed() {
        // `/model` switched providers mid-conversation; stored reasoning items
        // from the old model are not replayable under the new one.
        let messages = vec![
            Message::text(MessageRole::User, "read a.txt"),
            stamped("gpt-5.6-luna", 1),
        ];

        assert!(resume_point("gpt-5.4-mini", &messages).is_none());
    }

    #[test]
    fn resume_is_refused_when_history_ahead_of_the_anchor_changed() {
        // The stamp says the reply followed one message, but two now precede
        // it — something rewrote the history the stored response was built on.
        let messages = vec![
            Message::text(MessageRole::System, "an added prelude"),
            Message::text(MessageRole::User, "read a.txt"),
            stamped("gpt-5.6-luna", 1),
        ];

        assert!(resume_point("gpt-5.6-luna", &messages).is_none());
    }

    #[test]
    fn unstamped_transcript_sends_its_whole_history() {
        let messages = vec![
            Message::text(MessageRole::User, "one"),
            Message::text(MessageRole::Assistant, "two"),
            Message::text(MessageRole::User, "three"),
        ];

        let resume = resume_point("gpt-5.6-luna", &messages);
        let payload = base_payload("gpt-5.6-luna", &messages, &[], resume);

        assert!(resume.is_none());
        assert!(payload.get("previous_response_id").is_none());
        assert_eq!(payload["input"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn stale_session_errors_are_recognised() {
        assert!(stale_session_error(&json!({
            "error": {
                "message": "Previous response with id 'resp_1' not found.",
                "type": "invalid_request_error"
            }
        })));
        // The streaming path matches the same text inside a rendered error.
        assert!(is_stale_session(
            "openai streaming error: {\"message\":\"Previous response with id \
             'resp_1' not found.\"}; http_status=400"
        ));
        assert!(!stale_session_error(&json!({ "error": null })));
        assert!(!is_stale_session("rate limited"));
    }
}
