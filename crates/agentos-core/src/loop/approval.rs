use crate::approve::{tool_call_approval_id, PolicyDecision};
use crate::audit::{ArgumentDigest, SafetyEvent, SafetyEventKind, SafetyOutcome};
use agentos_interfaces::orchestrator::{Plan, SubOrchSpec};
use agentos_interfaces::run_state::{Interruption, InterruptionAction, RunState};
use agentos_proto::{InterruptionId, ToolCall};
use std::sync::Arc;

use super::{plan_subject, ApproveCtx, LoopDeps};

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
        subject: Arc<str>,
        reason: Arc<str>,
    },
    Pause {
        state: RunState,
    },
    Unsupported {
        reason: Arc<str>,
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
                reason,
            },
        },
        PolicyDecision::AskUser { reason } => pause_for_approval(ctx, deps, reason),
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
            return ApproveTransition::Unsupported { reason }
        }
    };

    let interruption_id = InterruptionId::new(approval_id);
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
    .with_interruption(interruption_id.clone());
    if let Some(digest) = digest {
        event = event.with_digest(digest);
    }
    deps.audit.record(event);

    let mut state = ctx.state;
    state
        .approvals
        .push(Interruption::pending(interruption_id, action));
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
