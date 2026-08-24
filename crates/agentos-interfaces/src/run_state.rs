use crate::orchestrator::{SubAgentSpec, SubOrchSpec};
use crate::session::Transcript;
use agentos_proto::{
    ActorPrincipal, AgentId, ApprovalInstanceId, ChannelId, ConversationId, InterruptionId, RunId,
    SchemaVersion, TaskId, ToolCall, TraceEvent, TraceSpan, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Interruption {
    /// Unique identity of this asking, even when the gated action is retried.
    pub approval_instance_id: ApprovalInstanceId,
    /// The exact channel capability bound one-to-one to this instance.
    pub approval_ticket: Arc<str>,
    /// The actor this asking belongs to.
    pub prompting_principal: ActorPrincipal,
    /// Filled exactly once when the approval is resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolver_principal: Option<ActorPrincipal>,
    pub id: InterruptionId,
    pub action: InterruptionAction,
    pub status: ApprovalStatus,
    /// Whether the loop has already carried `status` out.
    ///
    /// Separate from the status because the status is *what was decided* and
    /// this is *whether it has been acted on*, and an interruption is no
    /// longer removed once it has been (M6 / `AUD-001`,
    /// [ADR-0005](../../../docs/adr/0005-SAFETY_EVENTS.md)). Deleting the
    /// record used to be the only evidence that an approval succeeded, and an
    /// absence cannot be told apart from a record that was never written, one
    /// that was cleaned up, and one that was removed to hide something.
    #[serde(default, skip_serializing_if = "is_false")]
    pub consumed: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl Interruption {
    /// A new interruption, awaiting a decision.
    pub fn pending(
        approval_instance_id: ApprovalInstanceId,
        approval_ticket: impl Into<Arc<str>>,
        prompting_principal: ActorPrincipal,
        id: InterruptionId,
        action: InterruptionAction,
    ) -> Self {
        Self {
            approval_instance_id,
            approval_ticket: approval_ticket.into(),
            prompting_principal,
            resolver_principal: None,
            id,
            action,
            status: ApprovalStatus::Pending,
            consumed: false,
        }
    }

    /// Whether this one is still waiting on somebody.
    pub fn is_pending(&self) -> bool {
        self.status == ApprovalStatus::Pending
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_session_id: Option<Arc<str>>,
    pub transcript: Transcript,
    /// Every approval this run has asked for, decided or not.
    ///
    /// Retained rather than drained: see [`Interruption::consumed`]. The wire
    /// name stays `pending_approvals` so state written before M6 still loads,
    /// but the Rust name says what the field now holds — use
    /// [`RunState::pending_approval`] for the one that is actually waiting.
    #[serde(rename = "pending_approvals")]
    pub approvals: Vec<Interruption>,
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
            task_id: None,
            task_session_id: None,
            transcript: Transcript::default(),
            approvals: Vec::new(),
            queued_tool_calls: Vec::new(),
            usage: Usage::default(),
            version: SchemaVersion::default(),
            trace_spans: Vec::new(),
            trace_events: Vec::new(),
        }
    }

    /// The interruption still waiting on somebody, if any.
    ///
    /// A run pauses on one approval at a time, so this is the pause a caller
    /// is answering. Resolved interruptions stay in `approvals` behind it.
    pub fn pending_approval(&self) -> Option<&Interruption> {
        self.approvals
            .iter()
            .find(|interruption| interruption.is_pending())
    }

    /// The sole pending interruption, for kernel-internal lifecycle updates.
    pub fn pending_approval_mut(&mut self) -> Option<&mut Interruption> {
        self.approvals
            .iter_mut()
            .find(|interruption| interruption.is_pending())
    }
}
