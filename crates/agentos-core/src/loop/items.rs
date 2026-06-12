use super::telemetry::field_key;
use crate::subagents::SubAgentRunOutput;
use agentos_interfaces::orchestrator::SubOrchSpec;
use agentos_interfaces::session::Item;
use agentos_proto::{Message, MessageRole, ToolCall, ToolResult, ToolStatus};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

const MAX_TOOL_RESULT_CONTENT_BYTES: usize = 64 * 1024;

pub(super) fn tool_result_item(result: ToolResult) -> Item {
    let mut metadata = result.metadata;
    let content = bounded_tool_content(result.content, &mut metadata);
    metadata.insert(
        field_key("tool_call_id"),
        metadata_value(result.call_id.as_str()),
    );
    metadata.insert(
        field_key("tool_status"),
        metadata_value(tool_status_name(&result.status)),
    );
    Item {
        message: Message {
            role: MessageRole::Tool,
            content,
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: Some(result.call_id),
            metadata,
        },
        metadata: BTreeMap::new(),
    }
}

fn bounded_tool_content(content: Arc<str>, metadata: &mut BTreeMap<Arc<str>, Value>) -> Arc<str> {
    if content.len() <= MAX_TOOL_RESULT_CONTENT_BYTES {
        return content;
    }

    let mut end = MAX_TOOL_RESULT_CONTENT_BYTES;
    while !content.is_char_boundary(end) {
        end -= 1;
    }

    metadata.insert(field_key("content_truncated"), Value::Bool(true));
    metadata.insert(
        field_key("content_original_bytes"),
        Value::from(content.len() as u64),
    );
    metadata.insert(field_key("content_returned_bytes"), Value::from(end as u64));

    Arc::from(format!(
        "{}\n\n[tool result truncated: returned {} of {} bytes; request a smaller range, tail, or summary if more detail is needed]",
        &content[..end],
        end,
        content.len()
    ))
}

pub(super) fn assistant_tool_call_item(call: &ToolCall) -> Item {
    let mut metadata = BTreeMap::new();
    metadata.insert(field_key("kind"), metadata_value("tool_call"));
    metadata.insert(field_key("tool_call_id"), metadata_value(call.id.as_str()));
    metadata.insert(field_key("tool_name"), metadata_value(call.name.as_ref()));
    Item {
        message: Message {
            role: MessageRole::Assistant,
            content: Arc::from(""),
            attachments: Vec::new(),
            tool_calls: vec![call.clone()],
            tool_call_id: None,
            metadata: BTreeMap::new(),
        },
        metadata,
    }
}

pub(super) fn subagent_result_item(result: SubAgentRunOutput) -> Item {
    let mut metadata = BTreeMap::new();
    metadata.insert(field_key("kind"), metadata_value("subagent_result"));
    metadata.insert(
        field_key("subagent_id"),
        metadata_value(result.agent_id.as_str()),
    );
    metadata.insert(
        field_key("policy_id"),
        metadata_value(result.policy_id.as_ref()),
    );
    metadata.insert(
        field_key("child_run_id"),
        metadata_value(result.state.run_id.as_str()),
    );
    Item {
        message: Message {
            role: MessageRole::Tool,
            content: result.message.content,
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata,
        },
        metadata: BTreeMap::new(),
    }
}

pub(super) fn suborchestrator_result_item(
    spec: &SubOrchSpec,
    results: Vec<(Arc<str>, SubAgentRunOutput)>,
) -> Item {
    let mut metadata = BTreeMap::new();
    metadata.insert(field_key("kind"), metadata_value("suborchestrator_result"));
    metadata.insert(
        field_key("template"),
        metadata_value(spec.template.name.as_ref()),
    );
    metadata.insert(field_key("task_id"), metadata_value(spec.task_id.as_str()));
    metadata.insert(field_key("stages"), Value::from(results.len()));
    let content = if results.is_empty() {
        format!(
            "sub-orchestrator '{}' completed with no stages",
            spec.template.name
        )
    } else {
        results
            .iter()
            .map(|(stage, result)| format!("{}: {}", stage, result.message.content))
            .collect::<Vec<_>>()
            .join("\n")
    };
    Item {
        message: Message {
            role: MessageRole::Tool,
            content: Arc::from(content),
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata,
        },
        metadata: BTreeMap::new(),
    }
}

pub(super) fn tool_status_name(status: &ToolStatus) -> &'static str {
    match status {
        ToolStatus::Succeeded => "succeeded",
        ToolStatus::Failed => "failed",
        ToolStatus::Denied => "denied",
    }
}

/// Build a `Value::String` from any string-like input.
///
/// Centralised so the (unavoidable, given `serde_json::Value`'s contract)
/// `&str → String` allocation lives in one place.
pub(super) fn metadata_value(s: impl Into<String>) -> Value {
    Value::String(s.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::ToolCallId;

    #[test]
    fn tool_result_item_caps_large_content() {
        let result = ToolResult {
            call_id: ToolCallId::new("tool-1"),
            status: ToolStatus::Succeeded,
            content: Arc::from("x".repeat(MAX_TOOL_RESULT_CONTENT_BYTES + 100)),
            metadata: BTreeMap::new(),
        };

        let item = tool_result_item(result);

        assert!(item.message.content.len() < MAX_TOOL_RESULT_CONTENT_BYTES + 300);
        assert!(item
            .message
            .content
            .contains("tool result truncated: returned"));
        assert_eq!(
            item.message.metadata.get("content_truncated"),
            Some(&Value::Bool(true))
        );
    }
}
