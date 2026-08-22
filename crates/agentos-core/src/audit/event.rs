//! The vocabulary of safety events.
//!
//! One type per thing a later reader has to be able to reconstruct, and
//! nothing that would copy a payload into a store that is never cleaned up.
//! [ADR-0005](../../../../docs/adr/0005-SAFETY_EVENTS.md) is explicit about
//! the second half: an audit log that duplicates the secrets it audits is a
//! second copy of the secret with a longer retention policy. So a
//! [`SafetyEvent`] has no field that accepts tool arguments — the only way
//! they enter is [`ArgumentDigest::of`], which keeps the hash and drops the
//! bytes.

use agentos_proto::{InterruptionId, Principal, RunId};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Ceiling on a stored `detail` string.
///
/// A reason is written by the engine, an operator's policy rule, or a
/// guardrail, and several guardrails interpolate a model-chosen fragment into
/// it — a program name, a path. That fragment is already in the transcript and
/// the trace, so recording it here is not a new exposure, but this store
/// outlives both, so it gets a bound rather than the guardrail's word for it.
const MAX_DETAIL_BYTES: usize = 512;

/// What kind of safety-boundary decision an event records.
///
/// Closed on purpose. A new boundary needs a variant here, which is the point
/// where someone has to decide what it may and may not carry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SafetyEventKind {
    /// The policy engine asked a human, and the run paused.
    ApprovalRequested,
    /// The pause ended: approved, refused, or never answered.
    ApprovalResolved,
    /// The policy engine decided without asking anyone, and said no.
    PolicyDenial,
    /// An input guardrail refused what arrived.
    InputGuardrailTrip,
    /// A tool guardrail refused a call before it ran ([`SafetyOutcome::Refused`])
    /// or its result afterwards ([`SafetyOutcome::Tripped`]). One kind, because
    /// the guardrail is the same object; two outcomes, because "the call never
    /// happened" and "the call happened and its output was withheld" are not
    /// the same fact.
    ToolGuardrailTrip,
    /// An output guardrail refused a terminal reply.
    OutputGuardrailTrip,
    /// A sandboxed tool had no executor that could enforce its declared mode.
    SandboxRefusal,
    /// A run was stopped in flight.
    Cancellation,
    /// A `[[subagents.delegation_grants]]` entry admitted an elevation that
    /// narrowing would otherwise have refused.
    DelegationGrantIssued,
    /// A call was decided by authority the parent does not hold.
    DelegationGrantUsed,
    /// The run ended with an error rather than an answer.
    TerminalError,
}

impl SafetyEventKind {
    /// Stable name, as stored and as traced.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ApprovalRequested => "approval_requested",
            Self::ApprovalResolved => "approval_resolved",
            Self::PolicyDenial => "policy_denial",
            Self::InputGuardrailTrip => "input_guardrail_trip",
            Self::ToolGuardrailTrip => "tool_guardrail_trip",
            Self::OutputGuardrailTrip => "output_guardrail_trip",
            Self::SandboxRefusal => "sandbox_refusal",
            Self::Cancellation => "cancellation",
            Self::DelegationGrantIssued => "delegation_grant_issued",
            Self::DelegationGrantUsed => "delegation_grant_used",
            Self::TerminalError => "terminal_error",
        }
    }
}

/// How the decision came out.
///
/// Separate from the kind because the same kind has several endings and a
/// reader should not have to parse prose to tell them apart. In particular
/// [`Self::Rejected`] and [`Self::Unanswered`] are both failures and only one
/// of them is a decision somebody made.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SafetyOutcome {
    /// A question was put to someone; nothing is decided yet.
    Requested,
    /// Someone with the ticket said yes.
    Approved,
    /// Someone with the ticket said no.
    Rejected,
    /// Nobody answered: the prompt expired, or the run had no one to ask.
    Unanswered,
    /// The engine refused without asking.
    Denied,
    /// Content was refused by a guardrail.
    Tripped,
    /// The runtime could not enforce what the call declared, so it did not run
    /// it.
    Refused,
    /// A run was stopped.
    Stopped,
    /// Authority was granted.
    Issued,
    /// Authority was exercised.
    Used,
    /// The run ended badly.
    Failed,
}

impl SafetyOutcome {
    /// Stable name, as stored and as traced.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Unanswered => "unanswered",
            Self::Denied => "denied",
            Self::Tripped => "tripped",
            Self::Refused => "refused",
            Self::Stopped => "stopped",
            Self::Issued => "issued",
            Self::Used => "used",
            Self::Failed => "failed",
        }
    }
}

/// A hash of the arguments a decision applied to, with the arguments dropped.
///
/// Enough to tell whether two events concern the same call, to match an event
/// against a transcript entry a reader already holds, and to notice that the
/// same denied call is being retried. Not enough to recover what was in it,
/// which is the entire point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArgumentDigest(Arc<str>);

impl ArgumentDigest {
    /// Digest a call's raw arguments.
    ///
    /// SHA-256 rather than something cheaper: a digest an attacker can find a
    /// collision for is a digest that can be made to corroborate a call that
    /// never happened.
    pub fn of(arguments: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(arguments);
        let mut rendered = String::with_capacity(64);
        for byte in hasher.finalize() {
            rendered.push_str(&format!("{byte:02x}"));
        }
        Self(Arc::from(rendered))
    }

    /// Re-wrap a digest read back out of the store. Not a constructor for new
    /// digests: the only way to make one of those is [`Self::of`], which is
    /// what keeps arguments from reaching a row.
    pub(crate) fn from_stored(rendered: &str) -> Self {
        Self(Arc::from(rendered))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One immutable record of one safety-boundary decision.
#[derive(Clone, Debug)]
pub struct SafetyEvent {
    pub kind: SafetyEventKind,
    pub outcome: SafetyOutcome,
    /// Who the decision was made for.
    ///
    /// `None` only for events that precede any conversation: a delegation
    /// grant is issued while the runtime is being built, before a principal
    /// exists. An absent principal is that case, not an unknown one.
    pub principal: Option<Principal>,
    /// The run the decision happened in, where there is one.
    pub run_id: Option<RunId>,
    /// What the decision was about: a tool name, a guardrail name, a
    /// sub-agent id.
    pub subject: Arc<str>,
    /// Why, in the engine's own words. Bounded — see [`MAX_DETAIL_BYTES`].
    pub detail: Option<Arc<str>>,
    /// The call the decision applied to, as a hash. There is deliberately no
    /// field that takes the arguments themselves.
    pub argument_digest: Option<ArgumentDigest>,
    /// The pause this event belongs to, for the two approval kinds.
    pub interruption_id: Option<InterruptionId>,
}

impl SafetyEvent {
    /// A new event with only the fields every kind has.
    pub fn new(
        kind: SafetyEventKind,
        outcome: SafetyOutcome,
        subject: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            kind,
            outcome,
            principal: None,
            run_id: None,
            subject: subject.into(),
            detail: None,
            argument_digest: None,
            interruption_id: None,
        }
    }

    /// Attach the engine's explanation, truncated to [`MAX_DETAIL_BYTES`] on a
    /// character boundary.
    pub fn with_detail(mut self, detail: impl AsRef<str>) -> Self {
        let detail = detail.as_ref();
        let end = detail
            .char_indices()
            .map(|(index, character)| index + character.len_utf8())
            .take_while(|end| *end <= MAX_DETAIL_BYTES)
            .last()
            .unwrap_or(0);
        self.detail = Some(Arc::from(&detail[..end]));
        self
    }

    pub fn with_digest(mut self, digest: ArgumentDigest) -> Self {
        self.argument_digest = Some(digest);
        self
    }

    pub fn with_interruption(mut self, id: InterruptionId) -> Self {
        self.interruption_id = Some(id);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_digest_keeps_the_hash_and_nothing_else() {
        let digest = ArgumentDigest::of(br#"{"token":"hunter2"}"#);
        assert_eq!(digest.as_str().len(), 64);
        assert!(!digest.as_str().contains("hunter2"));
        assert!(digest.as_str().chars().all(|c| c.is_ascii_hexdigit()));
        // Same call, same digest — that is what makes a retry visible.
        assert_eq!(digest, ArgumentDigest::of(br#"{"token":"hunter2"}"#));
        assert_ne!(digest, ArgumentDigest::of(br#"{"token":"hunter3"}"#));
    }

    #[test]
    fn a_long_detail_is_cut_on_a_character_boundary() {
        // A guardrail interpolates model-chosen text into its reason, so the
        // ceiling has to hold for multi-byte input too — cutting mid-character
        // would panic on the slice.
        let event = SafetyEvent::new(
            SafetyEventKind::ToolGuardrailTrip,
            SafetyOutcome::Tripped,
            "ShellCommandAllowlist",
        )
        .with_detail("é".repeat(MAX_DETAIL_BYTES));
        let detail = event.detail.expect("detail is set");
        assert!(detail.len() <= MAX_DETAIL_BYTES);
        assert_eq!(detail.chars().count(), MAX_DETAIL_BYTES / 2);
    }

    #[test]
    fn a_short_detail_survives_intact() {
        let event = SafetyEvent::new(
            SafetyEventKind::PolicyDenial,
            SafetyOutcome::Denied,
            "shell",
        )
        .with_detail("denied by rule 3");
        assert_eq!(event.detail.as_deref(), Some("denied by rule 3"));
    }
}
