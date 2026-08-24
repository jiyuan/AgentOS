//! AUTH-003 administrator namespace regression.

use agentos_core::r#loop::{route, ApprovalBinding, ApprovalTicket, Routed};
use agentos_proto::{
    ActorPrincipal, AgentId, ApprovalInstanceId, ChannelId, ConversationId, Envelope,
    InterruptionId, Message, MessageRole,
};
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

#[test]
/// AF-013: a bare sender id never grants authority in another channel.
fn same_sender_id_on_two_channels_is_not_one_administrator() {
    let ticket = ApprovalTicket::parse("adminbound").expect("fixture ticket");
    let binding = ApprovalBinding::new(
        ApprovalInstanceId::new(ticket.as_str()),
        ticket.clone(),
        InterruptionId::new("approval"),
        actor("requester", "telegram"),
        None,
    )
    .expect("instance matches ticket")
    .with_administrators(vec![actor("ops", "telegram")]);
    let answer = Envelope {
        channel_id: ChannelId::new("feishu"),
        conversation_id: ConversationId::new("conversation"),
        sender: Arc::from("ops"),
        message: Message::text(MessageRole::User, format!("/approve {ticket}")),
        metadata: BTreeMap::new(),
    };
    assert!(matches!(
        route(Some(&binding), &answer),
        Routed::NotYours { .. }
    ));
}
