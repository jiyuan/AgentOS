//! Per-action validation and evidence for delegated authority.

use super::{plan_subject, LoopDeps};
use crate::audit::{ArgumentDigest, SafetyEvent, SafetyEventKind, SafetyOutcome};
use agentos_interfaces::orchestrator::Plan;
use std::sync::Arc;

/// Require a currently live runtime grant whenever the child is more
/// permissive than its immediate parent for this exact action.
pub(super) fn validate(plan: &Plan, deps: &LoopDeps<'_>) -> Result<(), Arc<str>> {
    let Some(authority) = deps.delegated_authority else {
        return Ok(());
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        // Before the Unix epoch is a clock failure. Treat it like an
        // arbitrarily large forward jump so every bounded grant fails closed.
        .map_or(u64::MAX, |since| since.as_secs());
    match authority.grant_for_action(plan, now) {
        Ok(Some(grant)) => {
            deps.audit.record(
                SafetyEvent::new(
                    SafetyEventKind::DelegationGrantUsed,
                    SafetyOutcome::Used,
                    plan_subject(plan),
                )
                .with_detail(grant.reason())
                .with_delegation_grant(Arc::from(grant.id())),
            );
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(reason) => {
            let mut event = SafetyEvent::new(
                SafetyEventKind::PolicyDenial,
                SafetyOutcome::Denied,
                plan_subject(plan),
            )
            .with_detail(reason.as_ref());
            if let Plan::CallTool(call) = plan {
                event = event.with_digest(ArgumentDigest::of(call.args.get().as_bytes()));
            }
            deps.audit.record(event);
            Err(reason)
        }
    }
}
