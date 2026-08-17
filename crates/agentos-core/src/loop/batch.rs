//! Tool-call batches (roadmap item X1).
//!
//! A model that asks for three tools in one response used to get one of them
//! run: `orchestrator/max.rs` took `tool_calls.first()` and dropped the rest.
//! The Anthropic and DeepSeek adapters had already parsed the whole vector, so
//! those calls were paid for and discarded — and the model, seeing one result
//! come back, would usually ask for the others again on the next turn.
//!
//! The fix is not to run them together. It is to *keep* them: the batch is
//! queued on the run state and drained one call per turn, each going back
//! through `Plan → Approve → Act → Observe` on its own. That preserves the two
//! things that matter — every call crosses the policy engine individually, and
//! results append in the order the model asked for them — without a new state
//! or a new transition.
//!
//! **Draining costs turns, deliberately.** A five-call batch spends five of
//! `max_turns`, so a model that emits a hundred calls hits the same budget as a
//! model that loops a hundred times. It does not cost five LLM round trips: the
//! model already said what it wanted.
//!
//! **Execution stays sequential.** Running a batch concurrently is the larger
//! prize and a separate decision: tool calls in one batch routinely touch the
//! same files, and interleaving two `file` writes because a model emitted them
//! together would be a bug introduced by an optimisation nobody asked for.
//! Sequential is also what makes "results append in order" free rather than a
//! reordering step.

use super::telemetry::field_key;
use super::LoopDeps;
use crate::trace;
use agentos_interfaces::run_state::RunState;
use agentos_proto::{SpanId, ToolCall};
use serde_json::Value;
use std::collections::BTreeMap;

/// Take the first call of a freshly planned batch and queue the rest.
///
/// Returns `None` for an empty batch, which is a plan that asked for nothing.
pub(super) fn enqueue(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    plan_span_id: &SpanId,
    calls: Vec<ToolCall>,
) -> Option<ToolCall> {
    let mut calls = calls.into_iter();
    let first = calls.next()?;
    let rest: Vec<ToolCall> = calls.collect();
    if rest.is_empty() {
        // A one-call "batch" is an ordinary tool call; nothing to record.
        return Some(first);
    }
    let mut fields = BTreeMap::new();
    fields.insert(field_key("tool_calls"), Value::from(rest.len() + 1));
    fields.insert(field_key("queued"), Value::from(rest.len()));
    trace::record_event(
        state,
        deps.hooks,
        plan_span_id.clone(),
        "tool_batch_planned",
        fields,
    );
    // Queued after the event, so a trace reads "batch planned" before anything
    // of it has been claimed.
    state.queued_tool_calls = rest;
    Some(first)
}

/// Take the next call of a batch already in flight, if there is one.
pub(super) fn next_queued(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    plan_span_id: &SpanId,
) -> Option<ToolCall> {
    if state.queued_tool_calls.is_empty() {
        return None;
    }
    let next = state.queued_tool_calls.remove(0);
    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("tool_name"),
        Value::String(next.name.as_ref().to_owned()),
    );
    fields.insert(
        field_key("tool_call_id"),
        Value::String(next.id.as_str().to_owned()),
    );
    fields.insert(
        field_key("remaining"),
        Value::from(state.queued_tool_calls.len()),
    );
    trace::record_event(
        state,
        deps.hooks,
        plan_span_id.clone(),
        "tool_batch_claimed",
        fields,
    );
    Some(next)
}
