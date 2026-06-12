use super::telemetry::{
    field_key, record_subagent_failure, record_telemetry_event, subagent_telemetry_fields,
};
use super::{LoopDeps, RunError};
use crate::runner::ResumeDecision;
use crate::subagents::{
    child_input_envelope, child_run_id, SubAgentError, SubAgentPausedRun, SubAgentRun,
    SubAgentRunOutput,
};
use crate::trace;
use agentos_interfaces::orchestrator::SubAgentSpec;
use agentos_interfaces::run_state::RunState;
use agentos_proto::{Envelope, Message, MessageRole, SpanKind};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) enum DelegateOutcome {
    Finished(SubAgentRunOutput),
    Paused(SubAgentPausedRun),
}

pub(super) async fn execute_delegate(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    spec: &SubAgentSpec,
) -> Result<DelegateOutcome, RunError> {
    let parent_id = trace::run_span_id(state);
    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("subagent_id"),
        Value::String(spec.agent_id.as_str().to_owned()),
    );
    fields.insert(
        field_key("policy_id"),
        Value::String(spec.policy_id.as_ref().to_owned()),
    );
    let span_id = trace::record_span(
        state,
        parent_id,
        SpanKind::Handoff,
        format!("delegate.{}", spec.agent_id.as_str()),
        fields,
    );
    trace::record_event(
        state,
        deps.hooks,
        span_id.clone(),
        "subagent_started",
        BTreeMap::new(),
    );

    let mut create_fields = subagent_telemetry_fields(spec);
    let input = child_input_envelope(spec, state);
    let run_id = child_run_id(spec, state);
    create_fields.insert(
        field_key("child_run_id"),
        Value::String(run_id.as_str().to_owned()),
    );
    create_fields.insert(
        field_key("conversation_id"),
        Value::String(input.conversation_id.as_str().to_owned()),
    );
    create_fields.insert(
        field_key("metadata_keys"),
        Value::from(input.metadata.len()),
    );
    record_telemetry_event(
        state,
        deps.hooks,
        span_id.clone(),
        "subagent_create_started",
        create_fields.clone(),
    );

    let subagents = match deps.subagents {
        Some(subagents) => subagents,
        None => {
            let error = SubAgentError::Unknown {
                agent_id: spec.agent_id.clone(),
                policy_id: Arc::clone(&spec.policy_id),
            };
            record_subagent_failure(state, deps, span_id, spec, "subagent_create_failed", &error);
            return Err(error.into());
        }
    };
    let invocation = match subagents.prepare(spec, deps.policy, input, run_id) {
        Ok(invocation) => invocation,
        Err(error) => {
            record_subagent_failure(state, deps, span_id, spec, "subagent_create_failed", &error);
            return Err(error.into());
        }
    };
    record_telemetry_event(
        state,
        deps.hooks,
        span_id.clone(),
        "subagent_created",
        create_fields,
    );
    record_telemetry_event(
        state,
        deps.hooks,
        span_id.clone(),
        "subagent_call_started",
        subagent_telemetry_fields(spec),
    );
    let result = match invocation.run().await {
        Ok(SubAgentRun::Finished(result)) => result,
        Ok(SubAgentRun::Paused(paused)) => {
            record_telemetry_event(
                state,
                deps.hooks,
                span_id.clone(),
                "subagent_call_paused",
                subagent_telemetry_fields(spec),
            );
            return Ok(DelegateOutcome::Paused(paused));
        }
        Err(error) => {
            record_subagent_failure(state, deps, span_id, spec, "subagent_call_failed", &error);
            return Err(error.into());
        }
    };

    let mut fields = BTreeMap::new();
    fields.insert(
        field_key("child_run_id"),
        Value::String(result.state.run_id.as_str().to_owned()),
    );
    fields.insert(
        field_key("trace_spans"),
        Value::from(result.state.trace_spans.len()),
    );
    fields.insert(
        field_key("trace_events"),
        Value::from(result.state.trace_events.len()),
    );
    record_telemetry_event(
        state,
        deps.hooks,
        span_id.clone(),
        "subagent_call_finished",
        fields.clone(),
    );
    trace::record_event(
        state,
        deps.hooks,
        span_id.clone(),
        "subagent_finished",
        fields,
    );
    let mut teardown_fields = subagent_telemetry_fields(spec);
    teardown_fields.insert(field_key("status"), Value::String("succeeded".to_owned()));
    teardown_fields.insert(
        field_key("child_run_id"),
        Value::String(result.state.run_id.as_str().to_owned()),
    );
    record_telemetry_event(
        state,
        deps.hooks,
        span_id,
        "subagent_teardown",
        teardown_fields,
    );
    Ok(DelegateOutcome::Finished(result))
}

pub(super) async fn execute_resume_delegate(
    state: &mut RunState,
    deps: &LoopDeps<'_>,
    spec: &SubAgentSpec,
    paused: SubAgentPausedRun,
) -> Result<DelegateOutcome, RunError> {
    let subagents = deps.subagents.ok_or_else(|| SubAgentError::Unknown {
        agent_id: spec.agent_id.clone(),
        policy_id: Arc::clone(&spec.policy_id),
    })?;
    let input = Envelope {
        channel_id: paused.channel_id.clone(),
        conversation_id: paused.conversation_id.clone(),
        sender: field_key("subagent-resume"),
        message: Message::text(MessageRole::User, ""),
        metadata: BTreeMap::new(),
    };
    let invocation = subagents.prepare(spec, deps.policy, input, paused.state.run_id.clone())?;
    match Box::pin(invocation.resume(paused, ResumeDecision::Approve)).await? {
        SubAgentRun::Finished(result) => {
            let parent_id = trace::run_span_id(state);
            let span_id = trace::record_span(
                state,
                parent_id,
                SpanKind::Handoff,
                format!("resume_delegate.{}", spec.agent_id.as_str()),
                subagent_telemetry_fields(spec),
            );
            let mut fields = subagent_telemetry_fields(spec);
            fields.insert(
                field_key("child_run_id"),
                Value::String(result.state.run_id.as_str().to_owned()),
            );
            record_telemetry_event(
                state,
                deps.hooks,
                span_id,
                "subagent_resume_finished",
                fields,
            );
            Ok(DelegateOutcome::Finished(result))
        }
        SubAgentRun::Paused(paused) => Ok(DelegateOutcome::Paused(paused)),
    }
}
