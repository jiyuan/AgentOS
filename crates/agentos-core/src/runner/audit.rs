//! Emitting a run's safety events (M6 / `AUD-001`).
//!
//! Split out of `runner.rs` to keep it under the module ceiling. Two of the
//! nine event kinds are the runner's to write, because both happen at the
//! boundary the loop cannot see: an approval is *resolved* by whoever answers
//! the prompt, and a run *ends* only once the error has escaped every state.

use super::episodes::{record_error_episode, EpisodeSeed};
use super::{persist_trace_records_with_sink, RunError, RunnerDeps};
use crate::audit::{SafetyEvent, SafetyEventKind, SafetyJournal, SafetyLogError, SafetyOutcome};
use crate::r#loop::{ResumeWitness, StepFailure};
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
    witness: &ResumeWitness,
    reason: Option<&str>,
) -> Result<(), SafetyLogError> {
    let mut event = SafetyEvent::new(
        SafetyEventKind::ApprovalResolved,
        outcome,
        Arc::clone(subject),
    )
    .with_interruption(witness.interruption_id.clone())
    .with_approval_instance(witness.approval_instance_id.clone())
    .with_approval_actors(
        witness.prompting_principal.clone().into_principal(),
        Some(witness.resolver_principal.clone().into_principal()),
    );
    if let Some(reason) = reason {
        event = event.with_detail(reason);
    }
    audit.record(event)
}

/// Record that the run ended badly.
///
/// The reasons that are *themselves* safety decisions — a denial, a guardrail
/// trip, a sandbox refusal — already wrote their own event at the point of
/// decision, with the subject and digest this one cannot reconstruct. This is
/// the record that the run stopped there, which is a different fact and the
/// one a reader looking for "why is there no answer" needs.
pub(super) fn record_terminal_error(
    audit: &SafetyJournal<'_>,
    error: &RunError,
) -> Result<(), SafetyLogError> {
    audit.record(
        SafetyEvent::new(
            SafetyEventKind::TerminalError,
            SafetyOutcome::Failed,
            run_error_kind(error),
        )
        .with_detail(error.to_string()),
    )
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
        RunError::DurableState { .. } => "durable_state",
        RunError::SafetyEvidence { .. } => "safety_evidence",
        RunError::ApprovalDenied { .. } => "approval_denied",
        RunError::StructuralDenial { .. } => "structural_denial",
        RunError::ApprovalUnanswered { .. } => "approval_unanswered",
        RunError::ApprovalUnsupported { .. } => "approval_unsupported",
        RunError::Cancelled => "cancelled",
    }
}

/// Persist everything a failed run owes a later reader.
pub(super) async fn record_failed_run(
    failure: StepFailure,
    audit: &SafetyJournal<'_>,
    seed: &EpisodeSeed,
    deps: &RunnerDeps<'_>,
    span_start: usize,
    event_start: usize,
) -> RunError {
    let (state, error) = failure.into_parts();
    let evidence_error = if matches!(error, RunError::SafetyEvidence { .. }) {
        None
    } else {
        record_terminal_error(audit, &error)
            .err()
            .map(|source| RunError::safety_evidence(SafetyEventKind::TerminalError, source))
    };
    record_error_episode(seed, &error, deps).await;
    if let Err(persist) =
        persist_trace_records_with_sink(&state, deps.trace_sink, span_start, event_start, "failed")
    {
        tracing::warn!(
            run_id = state.run_id.as_str(),
            error = %persist,
            "failed run's trace records could not be persisted"
        );
    }
    evidence_error.unwrap_or(error)
}
