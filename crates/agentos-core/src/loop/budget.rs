//! Turn-budget exhaustion and per-call LLM usage accounting.
//!
//! Split out of `loop/mod.rs` (roadmap Phase 4.1) — these are the two
//! self-contained pieces of the Plan state: synthesizing a terminal message
//! when a run hits `max_turns`, and folding each LLM call's token usage into
//! `RunState.usage` plus the `llm_token_usage` trace event.

use super::telemetry::field_key;
use super::{FinalOutput, LoopDeps, StepFailure};
use crate::trace;
use agentos_interfaces::run_state::RunState;
use agentos_proto::{Message, MessageRole, SpanId, SpanKind, Usage};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

/// Build the terminal message for a run that hit its turn budget: the most
/// useful content the run already produced, plus an explicit truncation
/// notice. Prefers the latest substantive assistant reply, then the latest
/// tool result, then a generic notice.
fn budget_exhausted_message(state: &RunState, max_turns: usize) -> Message {
    let mut assistant_text: Option<Arc<str>> = None;
    let mut tool_text: Option<Arc<str>> = None;
    for item in state.transcript.items.iter().rev() {
        match item.message.role {
            MessageRole::Assistant
                if item.message.tool_calls.is_empty()
                    && !item.message.content.trim().is_empty() =>
            {
                assistant_text = Some(item.message.content.clone());
                break;
            }
            MessageRole::Tool if tool_text.is_none() && !item.message.content.trim().is_empty() => {
                tool_text = Some(item.message.content.clone());
            }
            _ => {}
        }
    }
    let note = format!(
        "\n\n[AgentOS: stopped after the {max_turns}-step budget for this run. The text above \
         is the best result produced so far — continue with a narrower follow-up, or raise \
         max_turns, to finish.]"
    );
    let body = match (assistant_text, tool_text) {
        (Some(text), _) => format!("{text}{note}"),
        (None, Some(text)) => {
            format!("I hit the step budget before writing a final summary. Latest progress:\n\n{text}{note}")
        }
        (None, None) => {
            format!("I reached the step budget for this run before producing a result.{note}")
        }
    };
    let mut message = Message::text(MessageRole::Assistant, body);
    message
        .metadata
        .insert(field_key("run_truncated"), Value::Bool(true));
    message.metadata.insert(
        field_key("run_truncated_max_turns"),
        Value::from(max_turns as u64),
    );
    message
}

/// Finish a run that exhausted its turn budget. Records a `run_truncated`
/// trace span/event and runs output guardrails. A tripped guardrail (or
/// guardrail backend error) downgrades to a neutral completion notice. The
/// trip's required safety event is not best-effort: if it cannot be persisted,
/// `AUD-002` stops the run with a typed evidence failure.
pub(super) async fn budget_exhausted_finish(
    mut state: RunState,
    turns: usize,
    deps: &LoopDeps<'_>,
) -> Result<FinalOutput, StepFailure> {
    let parent_id = trace::run_span_id(&state);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("turns"), Value::from(turns as u64));
    fields.insert(field_key("max_turns"), Value::from(deps.max_turns as u64));
    let span_id = trace::record_span(
        &mut state,
        parent_id,
        SpanKind::State,
        "run_truncated",
        fields,
    );
    trace::record_event(
        &mut state,
        deps.hooks,
        span_id,
        "run_truncated",
        BTreeMap::new(),
    );
    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        turns,
        max_turns = deps.max_turns,
        "run_truncated_budget_exhausted"
    );

    let message = budget_exhausted_message(&state, deps.max_turns);
    let fallback = format!(
        "I reached the {}-step budget for this run and can't return the partial \
         output here. Please retry with a narrower request.",
        deps.max_turns
    );
    let message = match super::output::gated(&state, deps, message, &fallback).await {
        Ok(message) => message,
        Err(error) => return Err(StepFailure::new(state, error)),
    };
    Ok(FinalOutput { state, message })
}

pub(super) fn record_llm_usage(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    span_id: SpanId,
    call: Usage,
) {
    state.usage.record_call(&call);

    let total = state.usage;
    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("call_input_tokens"),
        Value::from(call.input_tokens),
    );
    fields.insert(
        field_key("call_output_tokens"),
        Value::from(call.output_tokens),
    );
    fields.insert(
        field_key("call_total_tokens"),
        Value::from(call.total_tokens),
    );
    fields.insert(
        field_key("call_cache_read_tokens"),
        Value::from(call.cache_read_tokens),
    );
    fields.insert(
        field_key("call_cache_write_tokens"),
        Value::from(call.cache_write_tokens),
    );
    fields.insert(
        field_key("call_cache_miss_tokens"),
        Value::from(call.cache_miss_tokens),
    );
    fields.insert(
        field_key("run_input_tokens"),
        Value::from(total.input_tokens),
    );
    fields.insert(
        field_key("run_output_tokens"),
        Value::from(total.output_tokens),
    );
    fields.insert(
        field_key("run_total_tokens"),
        Value::from(total.total_tokens),
    );
    fields.insert(
        field_key("run_cache_read_tokens"),
        Value::from(total.cache_read_tokens),
    );
    fields.insert(
        field_key("run_cache_write_tokens"),
        Value::from(total.cache_write_tokens),
    );
    fields.insert(
        field_key("run_cache_miss_tokens"),
        Value::from(total.cache_miss_tokens),
    );
    fields.insert(field_key("run_llm_calls"), Value::from(total.tool_calls));
    trace::record_event(state, deps.hooks, span_id, "llm_token_usage", fields);

    info!(
        run_id = state.run_id.as_str(),
        active_agent = state.active_agent.as_str(),
        call_input_tokens = call.input_tokens,
        call_output_tokens = call.output_tokens,
        call_cache_read_tokens = call.cache_read_tokens,
        call_cache_miss_tokens = call.cache_miss_tokens,
        run_input_tokens = total.input_tokens,
        run_output_tokens = total.output_tokens,
        "llm_token_usage"
    );
}
