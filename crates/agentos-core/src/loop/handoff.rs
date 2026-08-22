//! Switching the active agent mid-run.
//!
//! Split out of `loop/mod.rs` to keep it under the module ceiling. A handoff
//! is the one `Act` arm that changes the run's identity rather than appending
//! to its transcript, which is why it gets a span of its own: the trace has to
//! show where the agent on the record stopped being the one that started.

use super::items::metadata_value;
use super::telemetry::field_key;
use super::LoopDeps;
use crate::trace;
use agentos_interfaces::run_state::RunState;
use agentos_proto::{AgentId, SpanKind};
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) fn execute_handoff(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    agent_id: AgentId,
    payload: Option<Value>,
) {
    let from_agent = state.active_agent.clone();
    let parent_id = trace::run_span_id(state);
    let mut fields = BTreeMap::new();
    fields.insert(field_key("from_agent"), metadata_value(from_agent.as_str()));
    fields.insert(field_key("to_agent"), metadata_value(agent_id.as_str()));
    if let Some(payload) = payload {
        fields.insert(field_key("payload"), payload);
    }
    let span_id = trace::record_span(
        state,
        parent_id,
        SpanKind::Handoff,
        format!("handoff.{}", agent_id.as_str()),
        fields,
    );
    trace::record_event(
        state,
        deps.hooks,
        span_id.clone(),
        "handoff_started",
        BTreeMap::new(),
    );

    state.active_agent = agent_id;

    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("active_agent"),
        metadata_value(state.active_agent.as_str()),
    );
    trace::record_event(state, deps.hooks, span_id, "handoff_finished", fields);
}
