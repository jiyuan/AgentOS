use crate::orchestrator::{SubAgentSpec, SubOrchSpec};
use crate::session::Transcript;
use agentos_proto::{
    AgentId, ChannelId, ConversationId, InterruptionId, RunId, SchemaVersion, SessionKey, TaskId,
    ToolCall, TraceEvent, TraceSpan, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Interruption {
    pub id: InterruptionId,
    pub action: InterruptionAction,
    pub status: ApprovalStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum InterruptionAction {
    ToolCall(ToolCall),
    Delegate(SubAgentSpec),
    Escalate(SubOrchSpec),
    Handoff {
        agent_id: AgentId,
        payload: Option<Value>,
    },
    ResumeSubAgent {
        spec: SubAgentSpec,
        child_channel_id: ChannelId,
        child_conversation_id: ConversationId,
        child_state: Box<RunState>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    /// Someone was asked and said no.
    Rejected {
        reason: Arc<str>,
    },
    /// Ended without anyone deciding it — the prompt expired, or the run had
    /// no one who could answer (a cron tick).
    ///
    /// Separate from [`ApprovalStatus::Rejected`] because the audit trail has
    /// to be able to tell a refusal from a question nobody answered. Both fail
    /// closed; only one of them is a decision.
    Unanswered {
        reason: Arc<str>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunState {
    pub run_id: RunId,
    pub active_agent: AgentId,
    /// The complete principal and epoch owning every persisted artifact for
    /// this run. Legacy states omit it and are handled by the ID-002 migration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_key: Option<SessionKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_session_id: Option<Arc<str>>,
    pub transcript: Transcript,
    pub pending_approvals: Vec<Interruption>,
    /// Tool calls the model asked for that this run has not reached yet
    /// (roadmap item X1).
    ///
    /// A response carrying several calls is planned once and drained one call
    /// per turn, so each re-enters `Approve` on its own and the results append
    /// in the order the model asked for them. Part of `RunState` rather than
    /// the loop's own frame because a run can pause for approval mid-batch, and
    /// what is left of the batch has to survive being written to disk.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_tool_calls: Vec<ToolCall>,
    pub usage: Usage,
    pub version: SchemaVersion,
    #[serde(default)]
    pub trace_spans: Vec<TraceSpan>,
    #[serde(default)]
    pub trace_events: Vec<TraceEvent>,
}

impl RunState {
    pub fn new(run_id: RunId, active_agent: AgentId) -> Self {
        Self {
            run_id,
            active_agent,
            session_key: None,
            task_id: None,
            task_session_id: None,
            transcript: Transcript::default(),
            pending_approvals: Vec::new(),
            queued_tool_calls: Vec::new(),
            usage: Usage::default(),
            version: SchemaVersion::default(),
            trace_spans: Vec::new(),
            trace_events: Vec::new(),
        }
    }

    pub fn approve(&mut self, id: &InterruptionId) -> bool {
        for interruption in &mut self.pending_approvals {
            if &interruption.id == id {
                interruption.status = ApprovalStatus::Approved;
                return true;
            }
        }
        false
    }

    pub fn reject(&mut self, id: &InterruptionId, reason: impl Into<Arc<str>>) -> bool {
        let reason = reason.into();
        for interruption in &mut self.pending_approvals {
            if &interruption.id == id {
                interruption.status = ApprovalStatus::Rejected {
                    reason: Arc::clone(&reason),
                };
                return true;
            }
        }
        false
    }

    pub fn take_approved_action(&mut self) -> Option<InterruptionAction> {
        let index = self
            .pending_approvals
            .iter()
            .position(|interruption| interruption.status == ApprovalStatus::Approved)?;
        Some(self.pending_approvals.remove(index).action)
    }

    pub fn take_approved_tool_call(&mut self) -> Option<ToolCall> {
        match self.take_approved_action()? {
            InterruptionAction::ToolCall(call) => Some(call),
            InterruptionAction::Delegate(_)
            | InterruptionAction::Escalate(_)
            | InterruptionAction::Handoff { .. }
            | InterruptionAction::ResumeSubAgent { .. } => None,
        }
    }

    /// Mark an approval as ended without a decision. See
    /// [`ApprovalStatus::Unanswered`].
    pub fn mark_unanswered(&mut self, id: &InterruptionId, reason: impl Into<Arc<str>>) -> bool {
        let reason = reason.into();
        for interruption in &mut self.pending_approvals {
            if &interruption.id == id {
                interruption.status = ApprovalStatus::Unanswered {
                    reason: Arc::clone(&reason),
                };
                return true;
            }
        }
        false
    }

    pub fn take_rejected_reason(&mut self) -> Option<Arc<str>> {
        let index = self.pending_approvals.iter().position(|interruption| {
            matches!(interruption.status, ApprovalStatus::Rejected { .. })
        })?;
        let interruption = self.pending_approvals.remove(index);
        match interruption.status {
            ApprovalStatus::Rejected { reason } => Some(reason),
            ApprovalStatus::Pending
            | ApprovalStatus::Approved
            | ApprovalStatus::Unanswered { .. } => None,
        }
    }

    /// Take the reason an approval ended without a decision, removing it.
    pub fn take_unanswered_reason(&mut self) -> Option<Arc<str>> {
        let index = self.pending_approvals.iter().position(|interruption| {
            matches!(interruption.status, ApprovalStatus::Unanswered { .. })
        })?;
        let interruption = self.pending_approvals.remove(index);
        match interruption.status {
            ApprovalStatus::Unanswered { reason } => Some(reason),
            ApprovalStatus::Pending
            | ApprovalStatus::Approved
            | ApprovalStatus::Rejected { .. } => None,
        }
    }
}
