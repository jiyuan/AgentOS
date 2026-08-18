use agentos_core::config::RemoteChannelConfig;
use agentos_core::r#loop::{
    approval_resolver_authorized, route, ApprovalOutcome, ApprovalTicket, Routed,
};
use agentos_core::runner::ResumeDecision;
use agentos_interfaces::Channel;
use agentos_proto::{AgentId, SessionKey};
use std::sync::Arc;

pub(crate) async fn await_approval<C>(
    channel: &mut C,
    ticket: &ApprovalTicket,
    owner: &SessionKey,
    active_agent: &AgentId,
    remote_config: &RemoteChannelConfig,
) -> Option<ResumeDecision>
where
    C: Channel,
{
    loop {
        let answer = channel.receive().await?;
        if !approval_resolver_authorized(
            owner,
            &answer,
            active_agent,
            remote_config.is_administrator(&answer.sender),
        ) {
            println!(
                "Approval {ticket} can only be answered by its initiator or an authorized \
                 administrator in the same conversation."
            );
            continue;
        }
        match route(Some(ticket), &answer) {
            Routed::Decides {
                outcome: ApprovalOutcome::Approved,
                ..
            } => return Some(ResumeDecision::Approve),
            Routed::Decides { reason, .. } => {
                return Some(ResumeDecision::Reject {
                    reason: reason.unwrap_or_else(|| Arc::from("rejected by user")),
                });
            }
            Routed::Stale { ticket: named } => {
                println!("That approval ({named}) is not the one waiting. Answer {ticket}.");
            }
            Routed::Unrelated => {
                println!(
                    "Waiting on approval {ticket}: reply /approve {ticket} or /deny {ticket}."
                );
            }
        }
    }
}
