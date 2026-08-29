//! Shard-local projection of restart-safe approval records.

use agentos_core::gateway::{DurableApproval, IngressEventKey};
use agentos_core::r#loop::ApprovalBinding;
use agentos_core::runner::PausedRun;
use agentos_proto::ConversationId;
use std::collections::HashMap;

/// A run parked on one exact actor-bound approval.
pub(super) struct PendingApproval {
    pub(super) paused: PausedRun,
    pub(super) ingress_events: Vec<IngressEventKey>,
    pub(super) binding: ApprovalBinding,
    pub(super) expires_at: Option<u64>,
}

impl PendingApproval {
    pub(super) fn is_expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }
}

pub(super) fn restore(approvals: Vec<DurableApproval>) -> HashMap<ConversationId, PendingApproval> {
    approvals
        .into_iter()
        .map(|approval| {
            (
                approval.paused.conversation_id.clone(),
                PendingApproval {
                    paused: approval.paused,
                    ingress_events: approval.ingress_events,
                    binding: approval.binding,
                    expires_at: approval.expires_at,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_core::r#loop::ApprovalTicket;
    use agentos_interfaces::RunState;
    use agentos_proto::{
        ActorPrincipal, AgentId, ApprovalInstanceId, ChannelId, InterruptionId, RunId,
    };

    fn binding(ticket: &str) -> ApprovalBinding {
        let ticket = ApprovalTicket::parse(ticket).expect("fixture ticket parses");
        ApprovalBinding::new(
            ApprovalInstanceId::new(ticket.as_str()),
            ticket,
            InterruptionId::new("interruption"),
            ActorPrincipal::new(
                AgentId::new("agent"),
                ChannelId::new("telegram"),
                ConversationId::new("conversation"),
                "asker",
            ),
            None,
        )
        .expect("instance matches ticket")
    }

    fn approval(conversation: &str, ticket: &str, expires_at: Option<u64>) -> DurableApproval {
        DurableApproval {
            paused: PausedRun {
                channel_id: ChannelId::new("telegram"),
                conversation_id: ConversationId::new(conversation),
                state: RunState::new(RunId::new("run"), AgentId::new("agent")),
            },
            ingress_events: vec![IngressEventKey {
                channel_id: ChannelId::new("telegram"),
                event_id: std::sync::Arc::from(ticket),
            }],
            binding: binding(ticket),
            expires_at,
        }
    }

    /// Expiry is inclusive: a prompt whose deadline is exactly now is over.
    /// `decide` reads this to refuse letting a late answer resolve anything.
    #[test]
    fn expiry_is_inclusive_and_absent_deadlines_never_expire() {
        let pending = restore(vec![approval("c", "aaaaaaaa", Some(100))]);
        let entry = pending
            .get(&ConversationId::new("c"))
            .expect("the approval is restored under its conversation");
        assert!(!entry.is_expired(99));
        assert!(entry.is_expired(100), "the deadline instant is expired");
        assert!(entry.is_expired(101));

        let standing = restore(vec![approval("c", "bbbbbbbb", None)]);
        let entry = standing
            .get(&ConversationId::new("c"))
            .expect("the approval is restored");
        assert!(!entry.is_expired(u64::MAX));
    }

    /// Restart rebuilds one live prompt per conversation, keeping the exact
    /// binding and ingress rows the resumed run has to settle.
    #[test]
    fn restore_keys_each_approval_by_conversation_and_keeps_its_bindings() {
        let restored = restore(vec![
            approval("first", "aaaaaaaa", Some(10)),
            approval("second", "bbbbbbbb", None),
        ]);
        assert_eq!(restored.len(), 2);

        let first = restored
            .get(&ConversationId::new("first"))
            .expect("the first conversation is restored");
        assert_eq!(first.binding.ticket.as_str(), "aaaaaaaa");
        assert_eq!(first.expires_at, Some(10));
        assert_eq!(first.ingress_events.len(), 1);
        assert_eq!(first.ingress_events[0].event_id.as_ref(), "aaaaaaaa");
        assert_eq!(
            first.paused.conversation_id,
            ConversationId::new("first"),
            "a restored run must keep the conversation it was paused in"
        );

        let second = restored
            .get(&ConversationId::new("second"))
            .expect("the second conversation is restored");
        assert_eq!(second.binding.ticket.as_str(), "bbbbbbbb");
        assert_eq!(second.expires_at, None);
    }
}
