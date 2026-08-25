use crate::approve::{tool_call_approval_id, PolicyDecision};
use crate::audit::{ArgumentDigest, SafetyEvent, SafetyEventKind, SafetyLogError, SafetyOutcome};
use agentos_interfaces::orchestrator::{Plan, SubOrchSpec};
use agentos_interfaces::run_state::{Interruption, InterruptionAction, RunState};
use agentos_proto::{ApprovalInstanceId, InterruptionId, ToolCall};
use std::sync::Arc;

use super::authorization::AuthorizedPlan;
use super::{
    delegation_grant, plan_subject, ActCtx, ApprovalTicket, ApproveCtx, LoopDeps, RunError,
    RunLoopState, StepFailure,
};

pub(super) enum ApproveTransition {
    Allow {
        state: RunState,
        plan: Plan,
        turns: usize,
    },
    /// A policy `deny` on a tool call. Recoverable: the loop records a denied
    /// `ToolResult` and lets the model observe it and replan, rather than
    /// aborting the whole run (which would also kill the sub-agent and the
    /// gateway conversation above it).
    DenyTool {
        state: RunState,
        call: ToolCall,
        reason: Arc<str>,
        turns: usize,
    },
    /// A policy `deny` on a structural plan (handoff/delegate/escalate/resume).
    /// Fatal: there is no tool-call id to attach a result to, and a denied
    /// routing decision is not something the model can retry around.
    Deny {
        state: RunState,
        subject: Arc<str>,
        reason: Arc<str>,
    },
    Pause {
        state: RunState,
    },
    Unsupported {
        state: RunState,
        reason: Arc<str>,
    },
    EvidenceFailure {
        state: RunState,
        kind: SafetyEventKind,
        source: SafetyLogError,
    },
}

pub(super) fn approve_transition(ctx: ApproveCtx, deps: &LoopDeps<'_>) -> ApproveTransition {
    match deps.policy.decide(&ctx.plan) {
        PolicyDecision::Allow => ApproveTransition::Allow {
            state: ctx.state,
            plan: ctx.plan,
            turns: ctx.turns,
        },
        PolicyDecision::Deny { reason } => match ctx.plan {
            Plan::CallTool(call) => ApproveTransition::DenyTool {
                state: ctx.state,
                call,
                reason,
                turns: ctx.turns,
            },
            plan => ApproveTransition::Deny {
                subject: plan_subject(&plan),
                state: ctx.state,
                reason,
            },
        },
        PolicyDecision::AskUser { reason } => pause_for_approval(ctx, deps, reason),
    }
}

/// Decide one planned action and construct only the next state that decision
/// permits. Required evidence is persisted before a pause, denial recovery,
/// or delegated elevation can cross this boundary.
pub(super) async fn approve(
    ctx: ApproveCtx,
    deps: &LoopDeps<'_>,
) -> Result<RunLoopState, StepFailure> {
    match approve_transition(ctx, deps) {
        ApproveTransition::Allow { state, plan, turns } => {
            if let Err(failure) = delegation_grant::validate(&plan, deps) {
                match failure {
                    delegation_grant::ValidationError::Denied(reason) => {
                        let subject = plan_subject(&plan);
                        if let Plan::CallTool(call) = plan {
                            return Ok(RunLoopState::Act(ActCtx {
                                authorized_plan: AuthorizedPlan::denied_tool(
                                    Plan::CallTool(call),
                                    reason,
                                ),
                                state,
                                turns,
                            }));
                        }
                        return Err(StepFailure::new(
                            state,
                            RunError::StructuralDenial { subject, reason },
                        ));
                    }
                    delegation_grant::ValidationError::Evidence(error) => {
                        return Err(StepFailure::new(state, error));
                    }
                }
            }
            Ok(RunLoopState::Act(ActCtx {
                authorized_plan: AuthorizedPlan::allowed(plan),
                state,
                turns,
            }))
        }
        ApproveTransition::DenyTool {
            state,
            call,
            reason,
            turns,
        } => {
            if let Err(source) = deps.audit.record(
                SafetyEvent::new(
                    SafetyEventKind::PolicyDenial,
                    SafetyOutcome::Denied,
                    Arc::clone(&call.name),
                )
                .with_detail(reason.as_ref())
                .with_digest(ArgumentDigest::of(call.args.get().as_bytes())),
            ) {
                return Err(StepFailure::new(
                    state,
                    RunError::safety_evidence(SafetyEventKind::PolicyDenial, source),
                ));
            }
            let plan = Plan::CallTool(call);
            Ok(RunLoopState::Act(ActCtx {
                authorized_plan: AuthorizedPlan::denied_tool(plan, reason),
                state,
                turns,
            }))
        }
        ApproveTransition::Deny {
            state,
            subject,
            reason,
        } => {
            if let Err(source) = deps.audit.record_reason(
                SafetyEventKind::PolicyDenial,
                SafetyOutcome::Denied,
                Arc::clone(&subject),
                reason.as_ref(),
            ) {
                return Err(StepFailure::new(
                    state,
                    RunError::safety_evidence(SafetyEventKind::PolicyDenial, source),
                ));
            }
            Err(StepFailure::new(
                state,
                RunError::StructuralDenial { subject, reason },
            ))
        }
        ApproveTransition::Pause { state } => Ok(RunLoopState::Paused(state)),
        ApproveTransition::Unsupported { state, reason } => Err(StepFailure::new(
            state,
            RunError::ApprovalUnsupported { reason },
        )),
        ApproveTransition::EvidenceFailure {
            state,
            kind,
            source,
        } => Err(StepFailure::new(
            state,
            RunError::safety_evidence(kind, source),
        )),
    }
}

fn pause_for_approval(ctx: ApproveCtx, deps: &LoopDeps<'_>, reason: Arc<str>) -> ApproveTransition {
    let subject = plan_subject(&ctx.plan);
    // Only a tool call has arguments to digest; a handoff payload is the
    // engine's own routing data, not a call the model composed.
    let digest = match &ctx.plan {
        Plan::CallTool(call) => Some(ArgumentDigest::of(call.args.get().as_bytes())),
        _ => None,
    };
    let (approval_id, action) = match ctx.plan {
        Plan::CallTool(call) => (
            tool_call_approval_id(&call),
            InterruptionAction::ToolCall(call),
        ),
        Plan::Delegate(spec) => (
            delegate_approval_id(&spec),
            InterruptionAction::Delegate(spec),
        ),
        Plan::Escalate(spec) => (
            escalate_approval_id(&spec),
            InterruptionAction::Escalate(spec),
        ),
        Plan::Handoff(agent_id, payload) => (
            handoff_approval_id(&agent_id),
            InterruptionAction::Handoff { agent_id, payload },
        ),
        // A batch never reaches `Approve` — the loop splits it at `Plan` — so
        // there is nothing here to ask about. Failing closed rather than
        // pausing on a batch as if it were one action: approving "these five
        // calls" is not a decision a user was shown enough to make.
        Plan::CallTools(_) | Plan::Reply(_) | Plan::ResumeSubAgent { .. } => {
            return ApproveTransition::Unsupported {
                state: ctx.state,
                reason,
            }
        }
    };

    let interruption_id = InterruptionId::new(approval_id);
    let ticket = match ApprovalTicket::mint() {
        Ok(ticket) => ticket,
        Err(error) => {
            return ApproveTransition::Unsupported {
                state: ctx.state,
                reason: Arc::from(format!("could not create approval ticket: {error}")),
            }
        }
    };
    let Some(prompting_principal) = deps.audit.actor_principal() else {
        return ApproveTransition::Unsupported {
            state: ctx.state,
            reason: Arc::from("approval requires a sender-qualified prompting principal"),
        };
    };
    let approval_instance_id = ApprovalInstanceId::new(ticket.as_str());
    // Recorded here, at the pause, and not when someone answers: the question
    // having been asked is the fact a later reader needs, and it is the one
    // the old code left no trace of at all. An answer that never comes is then
    // a `requested` with no `resolved` beside it, rather than silence.
    let mut event = SafetyEvent::new(
        SafetyEventKind::ApprovalRequested,
        SafetyOutcome::Requested,
        subject,
    )
    .with_detail(reason.as_ref())
    .with_interruption(interruption_id.clone())
    .with_approval_instance(approval_instance_id.clone());
    if let Some(digest) = digest {
        event = event.with_digest(digest);
    }
    if let Err(source) = deps.audit.record(event) {
        return ApproveTransition::EvidenceFailure {
            state: ctx.state,
            kind: SafetyEventKind::ApprovalRequested,
            source,
        };
    }

    let mut state = ctx.state;
    state.approvals.push(Interruption::pending(
        approval_instance_id,
        ticket.as_str(),
        prompting_principal,
        interruption_id,
        action,
    ));
    ApproveTransition::Pause { state }
}

fn delegate_approval_id(spec: &agentos_interfaces::orchestrator::SubAgentSpec) -> Arc<str> {
    Arc::from(format!(
        "approval-delegate-{}-{}",
        spec.agent_id.as_str(),
        spec.policy_id
    ))
}

fn handoff_approval_id(agent_id: &agentos_proto::AgentId) -> Arc<str> {
    Arc::from(format!("approval-handoff-{}", agent_id.as_str()))
}

fn escalate_approval_id(spec: &SubOrchSpec) -> Arc<str> {
    Arc::from(format!(
        "approval-escalate-{}-{}",
        spec.template.name,
        spec.task_id.as_str()
    ))
}
