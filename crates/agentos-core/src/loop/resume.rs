//! Exact transition from a ticketed approval into `Act`.

use super::{
    resume_turns, ActCtx, ApprovalOutcome, AuthorizedPlan, ResumeWitness, RunError, RunLoopState,
    StepFailure,
};
use agentos_interfaces::orchestrator::Plan;
use agentos_interfaces::run_state::{ApprovalStatus, InterruptionAction, RunState};
use agentos_proto::ApprovalInstanceId;

/// Resume a loop only with the opaque witness produced by actor-qualified
/// approval routing.
pub fn resume_approved(
    mut state: RunState,
    witness: ResumeWitness,
) -> Result<RunLoopState, StepFailure> {
    if witness.outcome != ApprovalOutcome::Approved
        || witness
            .expires_at
            .is_some_and(|expires_at| crate::gateway::unix_now() >= expires_at)
    {
        return Err(StepFailure::new(state, RunError::NotResumable));
    }
    let Some(interruption) = state.approvals.iter_mut().find(|interruption| {
        interruption.approval_instance_id == witness.approval_instance_id
            && interruption.approval_ticket.as_ref() == witness.ticket.as_str()
            && interruption.id == witness.interruption_id
            && interruption.prompting_principal == witness.prompting_principal
            && interruption.status == ApprovalStatus::Pending
            && !interruption.consumed
    }) else {
        return Err(StepFailure::new(state, RunError::NotResumable));
    };
    interruption.status = ApprovalStatus::Approved;
    interruption.resolver_principal = Some(witness.resolver_principal);
    enter_approved(state, &witness.approval_instance_id)
}

/// Enter `Act` after the runner has persisted and audited an exact resolution.
pub(crate) fn enter_approved(
    mut state: RunState,
    approval_instance_id: &ApprovalInstanceId,
) -> Result<RunLoopState, StepFailure> {
    let turns = resume_turns(&state);
    let Some(interruption) = state.approvals.iter_mut().find(|interruption| {
        &interruption.approval_instance_id == approval_instance_id
            && !interruption.consumed
            && interruption.status == ApprovalStatus::Approved
    }) else {
        return Err(StepFailure::new(state, RunError::NotResumable));
    };
    interruption.consumed = true;
    let plan = match interruption.action.clone() {
        InterruptionAction::ToolCall(call) => Plan::CallTool(call),
        InterruptionAction::Delegate(spec) => Plan::Delegate(spec),
        InterruptionAction::Escalate(spec) => Plan::Escalate(spec),
        InterruptionAction::Handoff { agent_id, payload } => Plan::Handoff(agent_id, payload),
        InterruptionAction::ResumeSubAgent {
            spec,
            child_channel_id,
            child_conversation_id,
            child_state,
        } => Plan::ResumeSubAgent {
            spec,
            child_channel_id,
            child_conversation_id,
            child_state,
        },
    };
    Ok(RunLoopState::Act(ActCtx {
        authorized_plan: AuthorizedPlan::approved_by_human(plan),
        state,
        turns,
    }))
}
