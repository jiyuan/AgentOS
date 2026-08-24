//! AUTH-003 regression gates for opaque, actor-bound approval witnesses.

use agentos_core::r#loop::{resume_approved, route, ApprovalBinding, ApprovalTicket, Routed};
use agentos_interfaces::run_state::{ApprovalStatus, Interruption, InterruptionAction};
use agentos_interfaces::RunState;
use agentos_proto::{
    ActorPrincipal, AgentId, ApprovalInstanceId, ChannelId, ConversationId, Envelope,
    InterruptionId, Message, MessageRole, RunId, ToolCall, ToolCallId,
};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::Arc;

fn actor(sender: &str, channel: &str) -> ActorPrincipal {
    ActorPrincipal::new(
        AgentId::new("agent"),
        ChannelId::new(channel),
        ConversationId::new("conversation"),
        sender,
    )
}

fn action(call_id: &str) -> InterruptionAction {
    InterruptionAction::ToolCall(ToolCall {
        id: ToolCallId::new(call_id),
        name: Arc::from("gated"),
        args: RawValue::from_string("{}".to_owned()).expect("static JSON"),
    })
}

fn answer(sender: &str, channel: &str, ticket: &ApprovalTicket) -> Envelope {
    Envelope {
        channel_id: ChannelId::new(channel),
        conversation_id: ConversationId::new("conversation"),
        sender: Arc::from(sender),
        message: Message::text(MessageRole::User, format!("/approve {ticket}")),
        metadata: BTreeMap::new(),
    }
}

#[test]
/// AF-012: an old action-derived id cannot select a consumed interruption.
fn resolution_selects_only_the_exact_pending_instance() {
    let requester = actor("requester", "telegram");
    let old_ticket = ApprovalTicket::parse("ticketold").expect("fixture ticket");
    let live_ticket = ApprovalTicket::parse("ticketlive").expect("fixture ticket");
    let mut old = Interruption::pending(
        ApprovalInstanceId::new(old_ticket.as_str()),
        old_ticket.as_str(),
        requester.clone(),
        InterruptionId::new("same-action"),
        action("old-call"),
    );
    old.status = ApprovalStatus::Approved;
    old.consumed = true;
    let live = Interruption::pending(
        ApprovalInstanceId::new(live_ticket.as_str()),
        live_ticket.as_str(),
        requester.clone(),
        InterruptionId::new("same-action"),
        action("live-call"),
    );
    let mut state = RunState::new(RunId::new("run"), AgentId::new("agent"));
    state.approvals = vec![old, live];
    let binding = ApprovalBinding::new(
        ApprovalInstanceId::new(live_ticket.as_str()),
        live_ticket.clone(),
        InterruptionId::new("same-action"),
        requester,
        None,
    )
    .expect("instance matches ticket");
    let Routed::Decides { witness } = route(
        Some(&binding),
        &answer("requester", "telegram", &live_ticket),
    ) else {
        panic!("exact answer creates witness");
    };
    let replay = witness.clone();
    let resumed = resume_approved(state, witness).expect("exact pending instance resumes");
    let agentos_core::r#loop::RunLoopState::Act(ctx) = resumed else {
        panic!("approval enters Act");
    };
    assert!(ctx.state.approvals[0].consumed);
    assert!(ctx.state.approvals[1].consumed);
    assert_eq!(ctx.state.approvals[1].status, ApprovalStatus::Approved);
    assert!(
        resume_approved(ctx.state, replay).is_err(),
        "consumed witness cannot replay"
    );
}

#[test]
/// AF-010: only routing can mint the opaque witness the resume API accepts.
fn resume_rejects_bare_or_wrong_actor_witness() {
    let ticket = ApprovalTicket::parse("actorbound").expect("fixture ticket");
    let binding = ApprovalBinding::new(
        ApprovalInstanceId::new(ticket.as_str()),
        ticket.clone(),
        InterruptionId::new("approval"),
        actor("requester", "telegram"),
        None,
    )
    .expect("instance matches ticket");
    assert!(matches!(
        route(Some(&binding), &answer("intruder", "telegram", &ticket)),
        Routed::NotYours { .. }
    ));
    let wrong = ApprovalTicket::parse("wrongticket").expect("fixture ticket");
    assert!(matches!(
        route(Some(&binding), &answer("requester", "telegram", &wrong)),
        Routed::Stale { .. }
    ));
    // A bare id/decision cannot be tested at runtime: `resume_run` and
    // `resume_approved` accept only the opaque `ResumeWitness`, whose
    // compile-fail documentation proves it cannot be constructed literally.
}
