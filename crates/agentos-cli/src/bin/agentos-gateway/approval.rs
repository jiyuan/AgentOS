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
