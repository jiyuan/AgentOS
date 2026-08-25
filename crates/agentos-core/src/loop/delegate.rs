use super::telemetry::{
    field_key, record_subagent_failure, record_telemetry_event, subagent_telemetry_fields,
};
use super::{LoopDeps, ResumeWitness, RunError};
use crate::audit::{SafetyEvent, SafetyEventKind, SafetyOutcome};
use crate::subagents::{
    child_input_envelope, child_run_id, parent_principal, ParentSeed, SubAgentError,
    SubAgentPausedRun, SubAgentRun, SubAgentRunOutput,
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
    // X6: offer the branch point. The parent's in-memory transcript length is
    // the delegation point as the parent sees it; the store holds a prefix of
    // it, since the turn in flight is not persisted until it finishes, and the
    // ask itself reaches the child as its input message. Whether any of it is
    // used is the sub-agent definition's call.
    let seed = ParentSeed {
        principal: parent_principal(state),
        boundary: state.transcript.items.len(),
    };
    let invocation = match subagents
        .prepare(
            spec,
            deps.policy,
            deps.audit
                .actor_principal()
                .ok_or(SubAgentError::MissingInitiatingActor)?,
            input,
            run_id,
        )
        .map(|invocation| invocation.with_cancel(&deps.cancel).with_parent_seed(seed))
    {
        Ok(invocation) => invocation,
        Err(error) => {
            record_subagent_failure(state, deps, span_id, spec, "subagent_create_failed", &error);
            return Err(error.into());
        }
    };
    // Narrowing happens per delegation, so this is where a bounded template in
    // `agent.toml` becomes authority actually conferred on a running child.
    // Recorded by the parent, because the parent is the principal the
    // elevation was made against (M6 / `AUD-001`).
    for grant in invocation.grants_relied_on() {
        deps.audit
            .record(
                SafetyEvent::new(
                    SafetyEventKind::DelegationGrantIssued,
                    SafetyOutcome::Issued,
                    spec.agent_id.as_str(),
                )
                .with_detail(grant.reason())
                .with_delegation_grant(Arc::from(grant.id())),
            )
            .map_err(|source| {
                RunError::safety_evidence(SafetyEventKind::DelegationGrantIssued, source)
            })?;
    }
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
    let invocation = subagents
        .prepare_resume(
            spec,
            deps.policy,
            deps.audit
                .actor_principal()
                .ok_or(SubAgentError::MissingInitiatingActor)?,
            input,
            paused.state.run_id.clone(),
            &paused.state,
        )?
        .with_cancel(&deps.cancel);
    let witness = paused
        .state
        .pending_approval()
        .and_then(ResumeWitness::approved_internal)
        .ok_or_else(|| SubAgentError::Run(Arc::from("child approval witness is incomplete")))?;
    match Box::pin(invocation.resume(paused, witness)).await? {
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
