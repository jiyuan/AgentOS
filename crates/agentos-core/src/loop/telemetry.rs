use super::LoopDeps;
use crate::hooks::Hooks;
use crate::subagents::SubAgentError;
use crate::trace;
use agentos_interfaces::orchestrator::{Plan, Stage, SubAgentSpec, SubOrchSpec};
use agentos_interfaces::run_state::RunState;
use agentos_proto::SpanId;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

/// Field keys used in trace/telemetry maps on the loop hot path. Interned
/// once so building an event clones a shared `Arc<str>` per key instead of
/// allocating it anew for every span and event (roadmap Phase 3.3).
const INTERNED_FIELD_KEYS: &[&str] = &[
    "active_agent",
    "approval_denied",
    "call_cache_miss_tokens",
    "call_cache_read_tokens",
    "call_cache_write_tokens",
    "call_input_tokens",
    "call_output_tokens",
    "call_total_tokens",
    "child_run_id",
    "content_bytes",
    "content_original_bytes",
    "content_returned_bytes",
    "content_spill_locator",
    "content_truncated",
    "context_budget_tokens",
    "conversation_id",
    "depends_on_count",
    "elided_chars",
    "elided_messages",
    "error",
    "from_agent",
    "guardrail",
    "guardrail_tripped",
    "has_payload",
    "kind",
    "max_turns",
    "memory_fragments",
    "memory_hydration_candidate_count",
    "memory_hydration_namespace_count",
    "memory_hydration_selected_count",
    "message_role",
    "metadata_keys",
    "plan_kind",
    "policy_id",
    "pressure_percent",
    "prompt",
    "prompt_estimated_tokens",
    "reason",
    "resources",
    "run_cache_miss_tokens",
    "run_cache_read_tokens",
    "run_cache_write_tokens",
    "run_input_tokens",
    "run_llm_calls",
    "run_output_tokens",
    "run_total_tokens",
    "run_truncated",
    "run_truncated_max_turns",
    "sections",
    "stage",
    "stage_count",
    "stage_policy_id",
    "stages",
    "status",
    "subagent_id",
    "target_agent_id",
    "target_type",
    "task_id",
    "template",
    "to_agent",
    "tool_call_id",
    "tool_name",
    "tool_status",
    "tool_tokens",
    "total_chars",
    "total_messages",
    "trace_events",
    "trace_spans",
    "turn",
    "turns",
];

thread_local! {
    // Interned per *thread*, not process-wide: a global table measured ~3.5×
    // worse p50 tail latency in the 1000-conversation concurrency bench,
    // because every `Arc` clone/drop of a shared key became a contended
    // refcount RMW across all shard threads. Thread-local Arcs keep clones
    // uncontended at the cost of one ~60-entry table per thread.
    static FIELD_KEYS: Vec<(&'static str, Arc<str>)> = {
        let mut keys: Vec<(&'static str, Arc<str>)> = INTERNED_FIELD_KEYS
            .iter()
            .map(|key| (*key, Arc::<str>::from(*key)))
            .collect();
        keys.sort_unstable_by_key(|(key, _)| *key);
        keys
    };
}

/// Resolve a field key to its interned `Arc<str>`. Unknown keys fall back to
/// a fresh allocation, so this is safe for any literal.
pub(super) fn field_key(name: &'static str) -> Arc<str> {
    FIELD_KEYS.with(
        |keys| match keys.binary_search_by_key(&name, |(key, _)| *key) {
            Ok(index) => Arc::clone(&keys[index].1),
            Err(_) => Arc::from(name),
        },
    )
}

pub(super) fn plan_assignment_fields(state: &RunState, plan: &Plan) -> BTreeMap<Arc<str>, Value> {
    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("active_agent"),
        Value::String(state.active_agent.as_str().to_owned()),
    );
    match plan {
        Plan::Reply(message) => {
            fields.insert(field_key("plan_kind"), Value::String("reply".to_owned()));
            fields.insert(
                field_key("target_type"),
                Value::String("assistant".to_owned()),
            );
            fields.insert(
                field_key("message_role"),
                Value::String(format!("{:?}", message.role)),
            );
            fields.insert(
                field_key("content_bytes"),
                Value::from(message.content.len()),
            );
        }
        Plan::CallTool(call) => {
            fields.insert(field_key("plan_kind"), Value::String("tool".to_owned()));
            fields.insert(field_key("target_type"), Value::String("tool".to_owned()));
            fields.insert(
                field_key("tool_call_id"),
                Value::String(call.id.as_str().to_owned()),
            );
            fields.insert(
                field_key("tool_name"),
                Value::String(call.name.as_ref().to_owned()),
            );
        }
        Plan::Handoff(agent_id, payload) => {
            fields.insert(field_key("plan_kind"), Value::String("handoff".to_owned()));
            fields.insert(field_key("target_type"), Value::String("agent".to_owned()));
            fields.insert(
                field_key("target_agent_id"),
                Value::String(agent_id.as_str().to_owned()),
            );
            fields.insert(field_key("has_payload"), Value::Bool(payload.is_some()));
        }
        Plan::Delegate(spec) => {
            fields.insert(field_key("plan_kind"), Value::String("delegate".to_owned()));
            fields.insert(
                field_key("target_type"),
                Value::String("subagent".to_owned()),
            );
            fields.insert(
                field_key("subagent_id"),
                Value::String(spec.agent_id.as_str().to_owned()),
            );
            fields.insert(
                field_key("policy_id"),
                Value::String(spec.policy_id.as_ref().to_owned()),
            );
        }
        Plan::Escalate(spec) => {
            fields.insert(field_key("plan_kind"), Value::String("escalate".to_owned()));
            fields.insert(
                field_key("target_type"),
                Value::String("suborch".to_owned()),
            );
            fields.insert(
                field_key("template"),
                Value::String(spec.template.name.as_ref().to_owned()),
            );
            fields.insert(
                field_key("task_id"),
                Value::String(spec.task_id.as_str().to_owned()),
            );
            fields.insert(
                field_key("policy_id"),
                Value::String(spec.policy_id.as_ref().to_owned()),
            );
            fields.insert(
                field_key("stage_count"),
                Value::from(spec.template.stages.len()),
            );
        }
        Plan::ResumeSubAgent { spec, .. } => {
            fields.insert(
                field_key("plan_kind"),
                Value::String("resume_subagent".to_owned()),
            );
            fields.insert(
                field_key("target_type"),
                Value::String("subagent".to_owned()),
            );
            fields.insert(
                field_key("subagent_id"),
                Value::String(spec.agent_id.as_str().to_owned()),
            );
            fields.insert(
                field_key("policy_id"),
                Value::String(spec.policy_id.as_ref().to_owned()),
            );
        }
    }
    fields
}

pub(super) fn record_telemetry_event(
    state: &mut RunState,
    hooks: Option<&Hooks>,
    span_id: SpanId,
    name: &'static str,
    fields: BTreeMap<Arc<str>, Value>,
) {
    // Serializing the fields is only needed for the log line — skip it
    // entirely when INFO logging is disabled instead of paying the JSON
    // string on every telemetry event.
    let fields_json = tracing::enabled!(tracing::Level::INFO)
        .then(|| serde_json::to_string(&fields).unwrap_or_else(|_| "{}".to_owned()));
    trace::record_event(state, hooks, span_id, name, fields);
    if let Some(fields_json) = fields_json {
        info!(
            run_id = state.run_id.as_str(),
            active_agent = state.active_agent.as_str(),
            telemetry_event = name,
            telemetry_fields = %fields_json,
            "orchestration_telemetry"
        );
    }
}

pub(super) fn subagent_telemetry_fields(spec: &SubAgentSpec) -> BTreeMap<Arc<str>, Value> {
    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("subagent_id"),
        Value::String(spec.agent_id.as_str().to_owned()),
    );
    fields.insert(
        field_key("policy_id"),
        Value::String(spec.policy_id.as_ref().to_owned()),
    );
    fields
}

pub(super) fn suborch_telemetry_fields(spec: &SubOrchSpec) -> BTreeMap<Arc<str>, Value> {
    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("template"),
        Value::String(spec.template.name.as_ref().to_owned()),
    );
    fields.insert(
        field_key("task_id"),
        Value::String(spec.task_id.as_str().to_owned()),
    );
    fields.insert(
        field_key("policy_id"),
        Value::String(spec.policy_id.as_ref().to_owned()),
    );
    fields.insert(
        field_key("stage_count"),
        Value::from(spec.template.stages.len()),
    );
    fields
}

pub(super) fn suborch_stage_telemetry_fields(
    spec: &SubOrchSpec,
    stage: &Stage,
) -> BTreeMap<Arc<str>, Value> {
    let mut fields = suborch_telemetry_fields(spec);
    fields.insert(
        field_key("stage"),
        Value::String(stage.name.as_ref().to_owned()),
    );
    fields.insert(
        field_key("subagent_id"),
        Value::String(stage.agent.agent_id.as_str().to_owned()),
    );
    fields.insert(
        field_key("stage_policy_id"),
        Value::String(stage.agent.policy_id.as_ref().to_owned()),
    );
    fields.insert(
        field_key("depends_on_count"),
        Value::from(stage.depends_on.len()),
    );
    fields
}

pub(super) fn suborch_stage_agent_telemetry_fields(
    spec: &SubOrchSpec,
    stage_name: &Arc<str>,
    stage_agent: &SubAgentSpec,
) -> BTreeMap<Arc<str>, Value> {
    let mut fields = suborch_telemetry_fields(spec);
    fields.insert(
        field_key("stage"),
        Value::String(stage_name.as_ref().to_owned()),
    );
    fields.insert(
        field_key("subagent_id"),
        Value::String(stage_agent.agent_id.as_str().to_owned()),
    );
    fields.insert(
        field_key("stage_policy_id"),
        Value::String(stage_agent.policy_id.as_ref().to_owned()),
    );
    fields
}

pub(super) fn record_subagent_failure(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    span_id: SpanId,
    spec: &SubAgentSpec,
    event_name: &'static str,
    error: &SubAgentError,
) {
    let mut fields = subagent_telemetry_fields(spec);
    fields.insert(field_key("status"), Value::String("failed".to_owned()));
    fields.insert(field_key("error"), Value::String(error.to_string()));
    record_telemetry_event(
        state,
        deps.hooks,
        span_id.clone(),
        event_name,
        fields.clone(),
    );
    record_telemetry_event(state, deps.hooks, span_id, "subagent_teardown", fields);
}

pub(super) fn record_suborch_failure(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    span_id: SpanId,
    spec: &SubOrchSpec,
    event_name: &'static str,
    error: &str,
) {
    let mut fields = suborch_telemetry_fields(spec);
    fields.insert(field_key("status"), Value::String("failed".to_owned()));
    fields.insert(field_key("error"), Value::String(error.to_owned()));
    record_telemetry_event(
        state,
        deps.hooks,
        span_id.clone(),
        event_name,
        fields.clone(),
    );
    record_telemetry_event(state, deps.hooks, span_id, "suborch_teardown", fields);
}
