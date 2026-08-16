use super::telemetry::field_key;
use crate::spill::{SpillRef, SPILL_LOCATOR_KEY};
use crate::subagents::SubAgentRunOutput;
use agentos_interfaces::orchestrator::SubOrchSpec;
use agentos_interfaces::session::Item;
use agentos_proto::{Message, MessageRole, ToolCall, ToolResult, ToolStatus};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) fn tool_result_item(
    result: ToolResult,
    inline_bytes: usize,
    spilled: Option<SpillRef>,
) -> Item {
    let mut metadata = result.metadata;
    let content = bounded_tool_content(result.content, inline_bytes, spilled, &mut metadata);
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

/// Bound an oversized tool result for the transcript.
///
/// With a spill reference the remainder is on disk, so the notice points at it
/// and the content is recoverable. Without one — no store configured, or the
/// save failed — this degrades to the pre-C2 behavior: a truncation notice and
/// a lost tail. A storage failure must never turn a successful tool call into
/// an error, so the caller passes `None` rather than propagating.
fn bounded_tool_content(
    content: Arc<str>,
    inline_bytes: usize,
    spilled: Option<SpillRef>,
    metadata: &mut BTreeMap<Arc<str>, Value>,
) -> Arc<str> {
    if content.len() <= inline_bytes {
        return content;
    }

    let mut end = inline_bytes;
    while !content.is_char_boundary(end) {
        end -= 1;
    }

    metadata.insert(field_key("content_truncated"), Value::Bool(true));
    metadata.insert(
        field_key("content_original_bytes"),
        Value::from(content.len() as u64),
    );
    metadata.insert(field_key("content_returned_bytes"), Value::from(end as u64));

    match spilled {
        Some(spill) => {
            metadata.insert(
                field_key(SPILL_LOCATOR_KEY),
                Value::String(spill.locator.as_str().to_owned()),
            );
            Arc::from(format!(
                "{}\n\n[tool result truncated inline: showing {} of {} bytes. {}]",
                &content[..end],
                end,
                content.len(),
                spill.retrieval_hint
            ))
        }
        None => Arc::from(format!(
            "{}\n\n[tool result truncated: returned {} of {} bytes; request a smaller range, tail, or summary if more detail is needed]",
            &content[..end],
            end,
            content.len()
        )),
    }
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
    use crate::spill::SpillLocator;
    use agentos_proto::ToolCallId;

    const CAP: usize = 1_024;

    fn oversized() -> ToolResult {
        ToolResult {
            call_id: ToolCallId::new("tool-1"),
            status: ToolStatus::Succeeded,
            content: Arc::from("x".repeat(CAP + 100)),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn without_a_spill_store_the_tail_is_still_lost() {
        // Pre-C2 behavior, kept as the degraded path: no store configured, or
        // the save failed.
        let item = tool_result_item(oversized(), CAP, None);

        assert!(item.message.content.len() < CAP + 300);
        assert!(item
            .message
            .content
            .contains("tool result truncated: returned"));
        assert_eq!(
            item.message.metadata.get("content_truncated"),
            Some(&Value::Bool(true))
        );
        assert!(!item.message.metadata.contains_key("content_spill_locator"));
    }

    #[test]
    fn a_spilled_result_points_at_its_artifact() {
        let spill = SpillRef {
            locator: SpillLocator::for_tests("/spill/run-1/shell-tool_1.txt"),
            bytes: (CAP + 100) as u64,
            retrieval_hint: Arc::from(
                "The full output was saved to /spill/run-1/shell-tool_1.txt.",
            ),
        };

        let item = tool_result_item(oversized(), CAP, Some(spill));

        // The model is told where the rest is, not to run the command again.
        assert!(item
            .message
            .content
            .contains("/spill/run-1/shell-tool_1.txt"));
        assert!(!item.message.content.contains("request a smaller range"));
        assert_eq!(
            item.message.metadata.get("content_spill_locator"),
            Some(&Value::String("/spill/run-1/shell-tool_1.txt".to_owned()))
        );
    }

    #[test]
    fn content_within_the_cap_is_untouched() {
        let result = ToolResult {
            call_id: ToolCallId::new("tool-1"),
            status: ToolStatus::Succeeded,
            content: Arc::from("small"),
            metadata: BTreeMap::new(),
        };
        let item = tool_result_item(result, CAP, None);
        assert_eq!(item.message.content.as_ref(), "small");
        assert!(!item.message.metadata.contains_key("content_truncated"));
    }
}
