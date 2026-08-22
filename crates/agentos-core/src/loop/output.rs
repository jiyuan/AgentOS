//! The gate every terminal reply passes through (M6 / `STATE-001`,
//! deliverable 6).
//!
//! A run ends in one of four ways: the model replied, the turn budget ran out,
//! the context overflowed past recovery, or somebody stopped it. Three of
//! those synthesize their own message inside the loop, and cancellation used
//! to emit its notice without the output policy seeing it at all — a second
//! path around the check the first path takes
//! ([ADR-0007](../../../../docs/adr/0007-BUFFERED_OUTPUT.md)).
//!
//! # Why a synthesized reply substitutes rather than fails
//!
//! A model reply that trips an output guardrail fails the run: the answer was
//! not fit to send, and there is nothing else to say. The loop's own notices
//! cannot do that. The run *must* terminate here — re-raising would resurrect
//! the hard-failure path the budget and cancellation safeguards exist to
//! remove, and it would throw away the tool results a user who pressed stop
//! most wants kept. So a tripped notice is replaced by a fixed one that says
//! only that the run ended, carrying no run content for a content guardrail to
//! object to.
//!
//! A guardrail *backend* error counts as a trip here for the same reason: a
//! check that could not run has not passed.

use super::LoopDeps;
use crate::audit::{SafetyEventKind, SafetyOutcome};
use agentos_interfaces::guardrail::GuardrailOutcome;
use agentos_interfaces::orchestrator::RunContext;
use agentos_interfaces::run_state::RunState;
use agentos_proto::{Message, MessageRole};
use std::sync::Arc;

/// Put `message` to the declared output policy, substituting `fallback` if any
/// guardrail refuses it.
pub(super) async fn gated(
    state: &RunState,
    deps: &LoopDeps<'_>,
    message: Message,
    fallback: &str,
) -> Message {
    let Some(refused_by) = refused_by(state, deps, &message).await else {
        return message;
    };
    deps.audit.record_reason(
        SafetyEventKind::OutputGuardrailTrip,
        SafetyOutcome::Tripped,
        refused_by,
        "a terminal notice the loop synthesized was withheld",
    );
    Message::text(MessageRole::Assistant, fallback)
}

/// The name of the first guardrail that refuses `message`, if any.
async fn refused_by(state: &RunState, deps: &LoopDeps<'_>, message: &Message) -> Option<Arc<str>> {
    let mut run_ctx = RunContext::from_state(state);
    run_ctx.cancel = deps.cancel.clone();
    for entry in deps.output_guardrails {
        match entry.guardrail.check(message, &run_ctx).await {
            Ok(GuardrailOutcome::Passed) => {}
            Ok(GuardrailOutcome::Tripped(_)) | Err(_) => return Some(Arc::clone(&entry.name)),
        }
    }
    None
}
