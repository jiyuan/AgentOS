mod commands;
mod max;
mod min;
mod routing;

pub use max::{MaxOrchestrator, MemoryHydrationSettings};
pub use min::{EchoOrchestrator, MinOrchestrator};

use agentos_interfaces::orchestrator::{OrchestratorError, Plan};
use agentos_llm::LlmError;
use std::sync::Arc;

/// Project a provider failure into the orchestrator's error, keeping the one
/// class the run loop acts on.
///
/// Every orchestrator that reaches a model routes its errors through here.
/// Collapsing a context-length rejection into `Backend` would cost the run its
/// only chance to recover from one (roadmap item C4), and the compiler cannot
/// catch that omission — hence one shared conversion rather than a `map_err`
/// closure per call site.
pub(crate) fn planning_error(error: LlmError) -> OrchestratorError {
    match error {
        LlmError::ContextLengthExceeded(detail) => OrchestratorError::ContextLengthExceeded(detail),
        other => OrchestratorError::Backend(Arc::from(other.to_string())),
    }
}

/// Turn a response's tool calls into a plan (roadmap item X1).
///
/// Every call the model asked for is kept. Taking only the first used to
/// discard the rest silently: the Anthropic and DeepSeek adapters parse the
/// whole vector, so those calls were paid for and thrown away, and the model
/// usually re-asked for them a turn later.
///
/// A single call still plans as [`Plan::CallTool`], so the common path is
/// unchanged; the loop splits a batch back into single calls anyway, one per
/// turn, so each crosses `Approve` on its own.
pub(crate) fn plan_from_response(response: agentos_proto::Message) -> Plan {
    let mut calls = response.tool_calls;
    match calls.len() {
        0 => Plan::Reply(agentos_proto::Message {
            tool_calls: calls,
            ..response
        }),
        1 => Plan::CallTool(calls.remove(0)),
        _ => Plan::CallTools(calls),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{Message, MessageRole, ToolCall, ToolCallId};
    use serde_json::value::RawValue;

    fn response(call_ids: &[&str]) -> agentos_proto::Message {
        Message {
            role: MessageRole::Assistant,
            content: Arc::from("thinking"),
            attachments: Vec::new(),
            tool_calls: call_ids
                .iter()
                .map(|id| ToolCall {
                    id: ToolCallId::new(*id),
                    name: Arc::from("echo"),
                    args: RawValue::from_string("{}".to_owned()).expect("valid JSON"),
                })
                .collect(),
            tool_call_id: None,
            metadata: Default::default(),
        }
    }

    #[test]
    fn no_calls_plans_a_reply() {
        assert!(matches!(
            plan_from_response(response(&[])),
            Plan::Reply(message) if message.content.as_ref() == "thinking"
        ));
    }

    #[test]
    fn one_call_plans_an_ordinary_tool_call() {
        assert!(matches!(
            plan_from_response(response(&["a"])),
            Plan::CallTool(call) if call.id.as_str() == "a"
        ));
    }

    /// The regression this function exists for: taking `first()` here paid for
    /// the other calls and threw them away.
    #[test]
    fn several_calls_are_all_kept_in_order() {
        let Plan::CallTools(calls) = plan_from_response(response(&["a", "b", "c"])) else {
            panic!("several calls must plan as a batch");
        };
        assert_eq!(
            calls
                .iter()
                .map(|call| call.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
    }
}
