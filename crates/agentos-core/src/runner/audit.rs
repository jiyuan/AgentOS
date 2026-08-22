//! Emitting a run's safety events (M6 / `AUD-001`).
//!
//! Split out of `runner.rs` to keep it under the module ceiling. Two of the
//! nine event kinds are the runner's to write, because both happen at the
//! boundary the loop cannot see: an approval is *resolved* by whoever answers
//! the prompt, and a run *ends* only once the error has escaped every state.

use super::RunError;
use crate::audit::{SafetyEvent, SafetyEventKind, SafetyJournal, SafetyOutcome};
use agentos_proto::InterruptionId;
use std::sync::Arc;

/// Record how a pause ended.
///
/// One function for all three outcomes so a new resume decision cannot be
/// added without deciding what it writes here — the failure mode this module
/// exists to prevent is an outcome that quietly records nothing.
pub(super) fn record_resolution(
    audit: &SafetyJournal<'_>,
    outcome: SafetyOutcome,
    subject: &Arc<str>,
    approval_id: &InterruptionId,
    reason: Option<&str>,
) {
    let mut event = SafetyEvent::new(
        SafetyEventKind::ApprovalResolved,
        outcome,
        Arc::clone(subject),
    )
    .with_interruption(approval_id.clone());
    if let Some(reason) = reason {
        event = event.with_detail(reason);
    }
    audit.record(event);
}

/// Record that the run ended badly.
///
/// The reasons that are *themselves* safety decisions — a denial, a guardrail
/// trip, a sandbox refusal — already wrote their own event at the point of
/// decision, with the subject and digest this one cannot reconstruct. This is
/// the record that the run stopped there, which is a different fact and the
/// one a reader looking for "why is there no answer" needs.
pub(super) fn record_terminal_error(audit: &SafetyJournal<'_>, error: &RunError) {
    audit.record(
        SafetyEvent::new(
            SafetyEventKind::TerminalError,
            SafetyOutcome::Failed,
            run_error_kind(error),
        )
        .with_detail(error.to_string()),
    );
}

/// The variant name, as the `subject` of a terminal-error event. Stable enough
/// to group on, which the rendered message is not.
fn run_error_kind(error: &RunError) -> &'static str {
    match error {
        RunError::AlreadyDone => "already_done",
        RunError::NotResumable => "not_resumable",
        RunError::MaxTurnsExceeded => "max_turns_exceeded",
        RunError::Orchestrator(_) => "orchestrator",
        RunError::Guardrail(_) => "guardrail_backend",
        RunError::GuardrailTripped { .. } => "guardrail_tripped",
        RunError::Tool(_) => "tool",
        RunError::SubAgent(_) => "subagent",
        RunError::TaskWorkspace(_) => "task_workspace",
        RunError::ApprovalDenied { .. } => "approval_denied",
        RunError::ApprovalUnanswered { .. } => "approval_unanswered",
        RunError::ApprovalUnsupported { .. } => "approval_unsupported",
        RunError::Cancelled => "cancelled",
    }
}
