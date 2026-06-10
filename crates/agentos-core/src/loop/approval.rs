use crate::approve::{tool_call_approval_id, Policy, PolicyDecision};
use agentos_interfaces::orchestrator::{Plan, SubOrchSpec};
use agentos_interfaces::run_state::{ApprovalStatus, Interruption, InterruptionAction, RunState};
use agentos_proto::{InterruptionId, ToolCall};
use std::sync::Arc;

use super::ApproveCtx;

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
        reason: Arc<str>,
    },
    Pause {
        state: RunState,
    },
    Unsupported {
        reason: Arc<str>,
    },
}

pub(super) fn approve_transition(ctx: ApproveCtx, policy: &Policy) -> ApproveTransition {
    match policy.decide(&ctx.plan) {
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
            _ => ApproveTransition::Deny { reason },
        },
        PolicyDecision::AskUser { reason } => pause_for_approval(ctx, reason),
    }
}

fn pause_for_approval(ctx: ApproveCtx, reason: Arc<str>) -> ApproveTransition {
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
        Plan::Reply(_) | Plan::ResumeSubAgent { .. } => {
            return ApproveTransition::Unsupported { reason }
        }
    };

    let mut state = ctx.state;
    state.pending_approvals.push(Interruption {
        id: InterruptionId::new(approval_id),
        action,
        status: ApprovalStatus::Pending,
    });
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
