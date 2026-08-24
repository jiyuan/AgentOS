//! Rendering the envelope that asks a user to decide a pending approval.
//!
//! Split out of `runner.rs` to keep it under the module ceiling. Everything
//! here is presentation over a `PausedRun`: what the prompt says, and the
//! metadata a channel needs to render buttons and a gateway needs to route the
//! answer back to the pause it answers (roadmap G2).

use super::PausedRun;
use crate::r#loop::{
    prompt_actions, ApprovalTicket, ACTIONS_KEY, EXPIRES_AT_KEY, INTERRUPTION_KEY, PROMPT_KIND,
    TICKET_KEY,
};
use agentos_interfaces::run_state::InterruptionAction;
use agentos_proto::{Envelope, Message, MessageRole};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Build the envelope that asks a user to decide a pending approval.
///
/// `ticket` names *this asking* (roadmap G2): an answer has to carry it back or
/// it decides nothing. `expires_at` is unix seconds after which the prompt
/// stops counting, carried in metadata so a channel can render it and the
/// gateway can sweep it.
///
/// The `InterruptionId` travels alongside as `approval_id`. It says what is
/// being authorised where the ticket says which asking, and the pair is what
/// makes a decision traceable back to the action it permitted.
pub fn approval_prompt_envelope(
    paused: &PausedRun,
    sender: Arc<str>,
    expires_at: Option<u64>,
) -> Option<Envelope> {
    let approval = paused.state.pending_approval()?;
    let ticket = ApprovalTicket::parse(&approval.approval_ticket)?;
    let mut metadata = BTreeMap::new();
    metadata.insert(Arc::from("kind"), Value::String(PROMPT_KIND.to_owned()));
    metadata.insert(
        Arc::from(INTERRUPTION_KEY),
        Value::String(approval.id.as_str().to_owned()),
    );
    metadata.insert(
        Arc::from(TICKET_KEY),
        Value::String(ticket.as_str().to_owned()),
    );
    metadata.insert(Arc::from(ACTIONS_KEY), prompt_actions(&ticket));
    if let Some(expires_at) = expires_at {
        metadata.insert(Arc::from(EXPIRES_AT_KEY), Value::from(expires_at));
    }
    metadata.insert(
        Arc::from("run_id"),
        Value::String(paused.state.run_id.as_str().to_owned()),
    );
    let (action_kind, action_label) = approval_action_label(&approval.action);
    metadata.insert(
        Arc::from("action_kind"),
        Value::String(action_kind.to_owned()),
    );
    metadata.insert(
        Arc::from("action_label"),
        Value::String(action_label.clone()),
    );
    if let Some(call) = approval_tool_call(&approval.action) {
        metadata.insert(
            Arc::from("tool_name"),
            Value::String(call.name.as_ref().to_owned()),
        );
    }

    Some(Envelope {
        channel_id: paused.channel_id.clone(),
        conversation_id: paused.conversation_id.clone(),
        sender,
        message: Message {
            role: MessageRole::Assistant,
            // The ticket is spelled out because on a channel with no buttons
            // it is the only way to answer, and a user who cannot see it
            // cannot approve anything.
            content: Arc::from(format!(
                "Approve {action_kind} '{action_label}'?\n\
                 Reply /approve {ticket} to allow it, or /deny {ticket} <reason> to refuse.",
            )),
            attachments: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: BTreeMap::new(),
        },
        metadata,
    })
}

pub(super) fn approval_action_label(action: &InterruptionAction) -> (&'static str, String) {
    match action {
        InterruptionAction::ToolCall(call) => ("tool", call.name.as_ref().to_owned()),
        InterruptionAction::Delegate(spec) => (
            "delegate",
            format!("{} ({})", spec.agent_id.as_str(), spec.policy_id),
        ),
        InterruptionAction::Escalate(spec) => (
            "escalate",
            format!("{} ({})", spec.template.name, spec.task_id.as_str()),
        ),
        InterruptionAction::Handoff { agent_id, .. } => ("handoff", agent_id.as_str().to_owned()),
        InterruptionAction::ResumeSubAgent {
            spec, child_state, ..
        } => {
            let child_label = child_state
                .pending_approval()
                .map(|approval| {
                    let (kind, label) = approval_action_label(&approval.action);
                    format!("{kind} '{label}'")
                })
                .unwrap_or_else(|| "unknown child approval".to_owned());
            (
                "subagent",
                format!(
                    "{} ({}) waiting on {}",
                    spec.agent_id.as_str(),
                    spec.policy_id,
                    child_label
                ),
            )
        }
    }
}

fn approval_tool_call(action: &InterruptionAction) -> Option<&agentos_proto::ToolCall> {
    match action {
        InterruptionAction::ToolCall(call) => Some(call),
        InterruptionAction::ResumeSubAgent { child_state, .. } => child_state
            .pending_approval()
            .and_then(|approval| approval_tool_call(&approval.action)),
        InterruptionAction::Delegate(_)
        | InterruptionAction::Escalate(_)
        | InterruptionAction::Handoff { .. } => None,
    }
}
