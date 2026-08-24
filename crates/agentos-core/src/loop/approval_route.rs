//! Matching an approval answer to the prompt it answers (roadmap item G2).
//!
//! Before this, a paused run was resumed by whatever message arrived next: `y`
//! approved, anything else rejected. That is fine for a one-shot CLI where the
//! prompt is the last thing on screen, and wrong everywhere else. A user who
//! answers a question the agent asked ten minutes ago, or who says "yes, go
//! ahead" meaning something entirely different, silently authorises a tool
//! call. Nothing in the envelope had to refer to the approval at all.
//!
//! So an answer now has to name what it is answering, and the name has to be
//! unique to *this prompt*:
//!
//! - [`ApprovalTicket`] is minted per prompt, not derived from the action. An
//!   `InterruptionId` is derived (`approval-<tool call id>`), so two prompts
//!   for the same tool call — a model retrying, a user ignoring the first ask —
//!   share one. A stale button carrying that id would decide a later prompt.
//!   The ticket is also short, which matters: Telegram caps `callback_data` at
//!   64 bytes.
//! - The [`InterruptionId`] still travels with the prompt. It says *what* was
//!   approved where the ticket says *which asking*, and the pair is what makes
//!   the audit trail readable.
//!
//! Everything that is not an answer stays ordinary input and queues on the
//! conversation's inbox (roadmap G1), so talking past a pending approval is
//! just talking.

use agentos_proto::{base64url, ActorPrincipal, ApprovalInstanceId, Envelope, InterruptionId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use thiserror::Error;

/// Envelope metadata: the prompt's ticket, on the prompt and on any answer.
pub const TICKET_KEY: &str = "approval_ticket";
/// Envelope metadata on an answer: `approve` or `deny`.
pub const DECISION_KEY: &str = "approval_decision";
/// Envelope metadata on an answer: why, for a denial.
pub const REASON_KEY: &str = "approval_reason";
/// Envelope metadata on a prompt: the `InterruptionId` being gated.
pub const INTERRUPTION_KEY: &str = "approval_id";
/// Envelope metadata on a prompt: unix seconds after which it stops counting.
pub const EXPIRES_AT_KEY: &str = "approval_expires_at";
/// Envelope metadata on a prompt: the buttons a channel should render.
pub const ACTIONS_KEY: &str = "approval_actions";
/// Envelope metadata `kind` marking a prompt.
pub const PROMPT_KIND: &str = "approval_prompt";

/// Decision verbs, as they appear in metadata and in callback payloads.
pub const APPROVE: &str = "approve";
pub const DENY: &str = "deny";

/// How a pending approval ended.
///
/// Closed on purpose: every way out of a pause is one of these four, and none
/// of them is "unknown". The split that matters is [`Self::Rejected`] against
/// the other two failures — a refusal is a decision somebody made, and the
/// other two are the absence of one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalOutcome {
    /// Someone with the ticket said yes.
    Approved,
    /// Someone with the ticket said no.
    Rejected,
    /// The prompt expired before anyone answered.
    Cancelled,
    /// There was nobody who could answer — a run with no interactive user
    /// behind it, such as a cron tick that hit an `ask_user` policy.
    Unavailable,
}

impl ApprovalOutcome {
    /// Whether the gated action may proceed. Only one outcome says yes; the
    /// rest fail closed.
    pub fn permits_action(self) -> bool {
        matches!(self, Self::Approved)
    }

    /// Stable name for traces and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
            Self::Unavailable => "unavailable",
        }
    }
}

/// A short, unique name for one asking.
///
/// Unique per prompt rather than per action — see the module docs. Short
/// enough to fit a Telegram `callback_data` payload alongside a verb, and
/// short enough that a user on a text-only channel can type it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApprovalTicket(Arc<str>);

/// Why a fresh approval capability could not be issued.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TicketError {
    /// The operating system could not provide the per-process CSPRNG nonce.
    #[error("operating-system entropy unavailable")]
    EntropyUnavailable,
    /// Every counter value under this process nonce has been used.
    #[error("approval ticket counter exhausted")]
    CounterExhausted,
}

trait NonceSource: Send + Sync {
    fn fill(&self, nonce: &mut [u8; 16]) -> Result<(), TicketError>;
}

struct OsNonceSource;

impl NonceSource for OsNonceSource {
    fn fill(&self, nonce: &mut [u8; 16]) -> Result<(), TicketError> {
        getrandom::fill(nonce).map_err(|_| TicketError::EntropyUnavailable)
    }
}

struct TicketIssuer {
    prefix: Arc<str>,
    next: AtomicU64,
}

impl TicketIssuer {
    fn initialize(source: &dyn NonceSource) -> Result<Self, TicketError> {
        let mut nonce = [0_u8; 16];
        source.fill(&mut nonce)?;
        Ok(Self {
            // `a1` is a format/version marker. The nonce is unpadded
            // base64url (22 bytes), leaving ample room for the counter and a
            // channel verb inside Telegram's 64-byte callback ceiling.
            prefix: Arc::from(format!("a1{}", base64url(&nonce))),
            next: AtomicU64::new(0),
        })
    }

    fn mint(&self) -> Result<ApprovalTicket, TicketError> {
        let counter = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| TicketError::CounterExhausted)?;
        Ok(ApprovalTicket(Arc::from(format!(
            "{}{}",
            self.prefix,
            base36(counter)
        ))))
    }
}

impl ApprovalTicket {
    /// Mint a ticket for a new prompt.
    ///
    /// A 128-bit OS-CSPRNG nonce is generated exactly once for the process;
    /// each asking then claims one checked counter value beneath it.
    pub fn mint() -> Result<Self, TicketError> {
        static ISSUER: OnceLock<Result<TicketIssuer, TicketError>> = OnceLock::new();
        ISSUER
            .get_or_init(|| TicketIssuer::initialize(&OsNonceSource))
            .as_ref()
            .map_err(Clone::clone)?
            .mint()
    }

    /// Read a ticket someone sent back. Rejects anything that is not the
    /// alphabet [`ApprovalTicket::mint`] produces, so a user's prose can never
    /// be mistaken for a ticket.
    pub fn parse(input: &str) -> Option<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty()
            || trimmed.len() > 40
            || !trimmed
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return None;
        }
        Some(Self(Arc::from(trimmed)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ApprovalTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn base36(mut value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".to_owned();
    }
    let mut out = Vec::new();
    while value > 0 {
        out.push(DIGITS[(value % 36) as usize]);
        value /= 36;
    }
    out.reverse();
    String::from_utf8(out).expect("base36 digits are ASCII")
}

/// What an inbound envelope does to the approval a conversation is waiting on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Routed {
    /// Names the pending prompt and decides it.
    Decides { witness: ResumeWitness },
    /// Names a prompt that is not the pending one: a button from an asking
    /// that has already been answered or has expired. It decides nothing, and
    /// it is not conversation input either — the sender pressed a control, not
    /// typed a message, so they get told rather than answered.
    Stale { ticket: ApprovalTicket },
    /// Names the pending prompt, from someone it was not put to.
    ///
    /// Distinct from [`Routed::Stale`]: the prompt is live and the ticket is
    /// right, but this sender is not the one being asked. Telling them so is
    /// better than silence — a group member who pressed the button should
    /// learn why nothing happened — and it must not decide the prompt
    /// (`AUTH-001`).
    NotYours { ticket: ApprovalTicket },
    /// Names no prompt at all: ordinary input.
    Unrelated,
}

/// A live prompt, and who may answer it.
///
/// Approval used to turn on the ticket alone, so in a group conversation any
/// member who saw the prompt — or guessed a short base36 ticket — could decide
/// another member's approval. A ticket says *which* asking; it was never meant
/// to say *whose*.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalBinding {
    pub approval_instance_id: ApprovalInstanceId,
    pub ticket: ApprovalTicket,
    pub interruption_id: InterruptionId,
    /// The actor the prompt was put to: whoever sent the message that caused
    /// the run to pause, fully qualified by agent/channel/conversation.
    pub prompting_principal: ActorPrincipal,
    pub expires_at: Option<u64>,
    /// Principals who may answer matching prompts, from
    /// `[policy] approval_administrators`. Empty in the usual case.
    pub administrators: Vec<ActorPrincipal>,
}

impl ApprovalBinding {
    pub fn new(
        approval_instance_id: ApprovalInstanceId,
        ticket: ApprovalTicket,
        interruption_id: InterruptionId,
        prompting_principal: ActorPrincipal,
        expires_at: Option<u64>,
    ) -> Option<Self> {
        // One-to-one, deliberately simple: the instance identifier is the
        // ticket text under a distinct type. A persisted record where they
        // differ is corrupt rather than something routing may guess at.
        if approval_instance_id.as_str() != ticket.as_str() {
            return None;
        }
        Some(Self {
            approval_instance_id,
            ticket,
            interruption_id,
            prompting_principal,
            expires_at,
            administrators: Vec::new(),
        })
    }

    pub fn with_administrators(mut self, administrators: Vec<ActorPrincipal>) -> Self {
        self.administrators = administrators;
        self
    }

    /// Whether this exact actor may answer this prompt.
    fn answerable_by(&self, actor: &ActorPrincipal) -> bool {
        &self.prompting_principal == actor || self.administrators.iter().any(|admin| admin == actor)
    }

    /// Create the opaque witness for expiry or a non-interactive run. The
    /// caller states who resolved it even when that actor is the runtime
    /// itself, so audit never falls back to an unqualified conversation.
    pub fn unanswered_witness(
        &self,
        resolver_principal: ActorPrincipal,
        outcome: ApprovalOutcome,
        reason: Arc<str>,
    ) -> Option<ResumeWitness> {
        if !matches!(
            outcome,
            ApprovalOutcome::Cancelled | ApprovalOutcome::Unavailable
        ) {
            return None;
        }
        Some(self.witness(resolver_principal, outcome, Some(reason)))
    }

    fn witness(
        &self,
        resolver_principal: ActorPrincipal,
        outcome: ApprovalOutcome,
        reason: Option<Arc<str>>,
    ) -> ResumeWitness {
        ResumeWitness {
            approval_instance_id: self.approval_instance_id.clone(),
            ticket: self.ticket.clone(),
            interruption_id: self.interruption_id.clone(),
            prompting_principal: self.prompting_principal.clone(),
            resolver_principal,
            expires_at: self.expires_at,
            outcome,
            reason,
        }
    }
}

/// Opaque authority to resolve exactly one pending approval instance.
///
/// Its fields are private and it can only be minted by actor-qualified routing
/// or by [`ApprovalBinding::unanswered_witness`]. Possessing an interruption
/// id and a decision is therefore insufficient to reach the public resume API.
///
/// ```compile_fail
/// use agentos_core::r#loop::{ApprovalOutcome, ApprovalTicket, ResumeWitness};
/// use agentos_proto::{ActorPrincipal, ApprovalInstanceId, InterruptionId};
/// # fn cannot_fabricate(
/// #     actor: ActorPrincipal,
/// #     ticket: ApprovalTicket,
/// # ) {
/// let _ = ResumeWitness {
///     approval_instance_id: ApprovalInstanceId::new("instance"),
///     ticket,
///     interruption_id: InterruptionId::new("interruption"),
///     prompting_principal: actor.clone(),
///     resolver_principal: actor,
///     expires_at: None,
///     outcome: ApprovalOutcome::Approved,
///     reason: None,
/// };
/// # }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumeWitness {
    pub(crate) approval_instance_id: ApprovalInstanceId,
    pub(crate) ticket: ApprovalTicket,
    pub(crate) interruption_id: InterruptionId,
    pub(crate) prompting_principal: ActorPrincipal,
    pub(crate) resolver_principal: ActorPrincipal,
    pub(crate) expires_at: Option<u64>,
    pub(crate) outcome: ApprovalOutcome,
    pub(crate) reason: Option<Arc<str>>,
}

impl ResumeWitness {
    pub fn outcome(&self) -> ApprovalOutcome {
        self.outcome
    }

    pub(crate) fn approved_internal(
        interruption: &agentos_interfaces::run_state::Interruption,
    ) -> Option<Self> {
        let ticket = ApprovalTicket::parse(&interruption.approval_ticket)?;
        if ticket.as_str() != interruption.approval_instance_id.as_str() {
            return None;
        }
        Some(Self {
            approval_instance_id: interruption.approval_instance_id.clone(),
            ticket,
            interruption_id: interruption.id.clone(),
            prompting_principal: interruption.prompting_principal.clone(),
            resolver_principal: interruption.resolver_principal.clone()?,
            expires_at: None,
            outcome: ApprovalOutcome::Approved,
            reason: None,
        })
    }
}

/// Route `envelope` against the prompt a conversation is waiting on.
///
/// `pending` is `None` when nothing is waiting, which is the common case; an
/// answer arriving then is [`Routed::Stale`] rather than input, for the same
/// reason a mismatched one is.
///
/// Two things have to line up for a decision: the envelope carries the live
/// ticket, *and* its sender is the one that was asked (or a configured
/// administrator). The second check is what stops one participant in a group
/// conversation from answering another's prompt.
pub fn route(pending: Option<&ApprovalBinding>, envelope: &Envelope) -> Routed {
    let Some((ticket, decision, reason)) = answer(envelope) else {
        return Routed::Unrelated;
    };
    let Some(pending) = pending.filter(|pending| pending.ticket == ticket) else {
        return Routed::Stale { ticket };
    };
    let prompting = pending.prompting_principal.as_principal();
    let resolver = ActorPrincipal::new(
        prompting.agent.clone(),
        envelope.channel_id.clone(),
        envelope.conversation_id.clone(),
        Arc::clone(&envelope.sender),
    );
    if !pending.answerable_by(&resolver) {
        return Routed::NotYours { ticket };
    }
    let outcome = if decision == APPROVE {
        ApprovalOutcome::Approved
    } else {
        ApprovalOutcome::Rejected
    };
    Routed::Decides {
        witness: pending.witness(resolver, outcome, reason),
    }
}

/// Extract `(ticket, verb, reason)` from an envelope, from its metadata if a
/// channel put it there and otherwise from an explicit slash command.
fn answer(envelope: &Envelope) -> Option<(ApprovalTicket, &'static str, Option<Arc<str>>)> {
    // A channel that renders buttons (Telegram's inline keyboard) sends the
    // decision structurally; nothing about it is guessable from prose.
    if let Some(ticket) = envelope
        .metadata
        .get(TICKET_KEY)
        .and_then(Value::as_str)
        .and_then(ApprovalTicket::parse)
    {
        let decision = match envelope.metadata.get(DECISION_KEY).and_then(Value::as_str) {
            Some(APPROVE) => APPROVE,
            // Anything else in a decision field is not an approval. Fail
            // closed rather than guess at a typo'd verb.
            _ => DENY,
        };
        let reason = envelope
            .metadata
            .get(REASON_KEY)
            .and_then(Value::as_str)
            .map(Arc::from);
        return Some((ticket, decision, reason));
    }
    text_answer(&envelope.message.content)
}

/// `/approve <ticket>` and `/deny <ticket> [reason]`, for channels with no
/// buttons. The ticket is required: an answer that does not name its prompt is
/// the thing this module exists to refuse.
fn text_answer(content: &str) -> Option<(ApprovalTicket, &'static str, Option<Arc<str>>)> {
    let trimmed = content.trim();
    let (head, rest) = trimmed.split_once(char::is_whitespace)?;
    let decision = match head.trim_start_matches('/').to_ascii_lowercase().as_str() {
        APPROVE => APPROVE,
        DENY => DENY,
        _ => return None,
    };
    let rest = rest.trim();
    let (ticket, reason) = match rest.split_once(char::is_whitespace) {
        Some((ticket, reason)) => (ticket, Some(reason.trim())),
        None => (rest, None),
    };
    let ticket = ApprovalTicket::parse(ticket)?;
    let reason = reason.filter(|text| !text.is_empty()).map(Arc::from);
    Some((ticket, decision, reason))
}

/// The buttons a channel should render for a prompt, as envelope metadata.
///
/// Each carries the payload a channel echoes back: `approve:<ticket>`. Kept to
/// well under Telegram's 64-byte `callback_data` limit by construction.
pub fn prompt_actions(ticket: &ApprovalTicket) -> Value {
    Value::Array(
        [(APPROVE, "Approve"), (DENY, "Deny")]
            .into_iter()
            .map(|(decision, label)| {
                serde_json::json!({
                    "label": label,
                    "decision": decision,
                    "data": format!("{decision}:{ticket}"),
                })
            })
            .collect(),
    )
}

/// Read back what [`prompt_actions`] encoded, for a channel receiving a button
/// press. Returns `(verb, ticket)`.
pub fn parse_action_data(data: &str) -> Option<(&'static str, ApprovalTicket)> {
    let (decision, ticket) = data.split_once(':')?;
    let decision = match decision {
        APPROVE => APPROVE,
        DENY => DENY,
        _ => return None,
    };
    Some((decision, ApprovalTicket::parse(ticket)?))
}

/// The interruption a prompt named, so an answer can be traced back to what it
/// authorised rather than only to which asking it answered.
pub fn prompt_interruption(envelope: &Envelope) -> Option<InterruptionId> {
    envelope
        .metadata
        .get(INTERRUPTION_KEY)
        .and_then(Value::as_str)
        .map(InterruptionId::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{AgentId, ChannelId, ConversationId, Message, MessageRole};
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Barrier;

    /// The sender every envelope in these tests carries.
    const SENDER: &str = "user";

    /// A prompt put to that sender, so these tests exercise ticket routing
    /// rather than the sender check — which has its own tests below.
    fn pending_for(ticket_value: &str) -> ApprovalBinding {
        ApprovalBinding::new(
            ApprovalInstanceId::new(ticket_value),
            ticket(ticket_value),
            InterruptionId::new("approval-action"),
            actor(SENDER, "test"),
            None,
        )
        .expect("instance and ticket match")
    }

    fn actor(sender: &str, channel: &str) -> ActorPrincipal {
        ActorPrincipal::new(
            AgentId::new("agent"),
            ChannelId::new(channel),
            ConversationId::new("conv"),
            sender,
        )
    }

    fn decided(routed: Routed) -> (ApprovalOutcome, Option<Arc<str>>) {
        match routed {
            Routed::Decides { witness } => (witness.outcome, witness.reason),
            other => panic!("expected decision, got {other:?}"),
        }
    }

    fn text(content: &str) -> Envelope {
        Envelope {
            channel_id: ChannelId::new("test"),
            conversation_id: ConversationId::new("conv"),
            sender: Arc::from(SENDER),
            message: Message::text(MessageRole::User, content),
            metadata: BTreeMap::new(),
        }
    }

    fn pressed(ticket: &str, decision: &str) -> Envelope {
        let mut envelope = text("");
        envelope
            .metadata
            .insert(Arc::from(TICKET_KEY), Value::String(ticket.to_owned()));
        envelope
            .metadata
            .insert(Arc::from(DECISION_KEY), Value::String(decision.to_owned()));
        envelope
    }

    fn ticket(value: &str) -> ApprovalTicket {
        ApprovalTicket::parse(value).expect("test ticket is well formed")
    }

    /// The whole point of the item: ordinary conversation cannot decide an
    /// approval, however affirmative it sounds.
    #[test]
    fn prose_never_decides_an_approval() {
        let pending = pending_for("k3f");
        for content in ["y", "yes", "yes, go ahead", "approve", "sure do it", "no"] {
            assert_eq!(
                route(Some(&pending), &text(content)),
                Routed::Unrelated,
                "{content:?} must be ordinary input"
            );
        }
    }

    #[test]
    fn a_slash_command_naming_the_pending_ticket_decides_it() {
        let pending = pending_for("k3f");
        assert_eq!(
            decided(route(Some(&pending), &text("/approve k3f"))),
            (ApprovalOutcome::Approved, None)
        );
        assert_eq!(
            decided(route(Some(&pending), &text("/deny k3f too risky"))),
            (ApprovalOutcome::Rejected, Some(Arc::from("too risky")))
        );
    }

    /// A stale button decides nothing — this is what per-prompt tickets buy
    /// over the action-derived `InterruptionId`.
    #[test]
    fn an_answer_naming_another_prompt_is_stale() {
        let pending = pending_for("k3f");
        assert_eq!(
            route(Some(&pending), &pressed("zz9", APPROVE)),
            Routed::Stale {
                ticket: ticket("zz9")
            }
        );
        // ...including when nothing is pending at all.
        assert_eq!(
            route(None, &text("/approve k3f")),
            Routed::Stale {
                ticket: ticket("k3f")
            }
        );
    }

    /// An answer with a garbled verb is a denial, not an approval.
    #[test]
    fn an_unreadable_verb_fails_closed() {
        let pending = pending_for("k3f");
        assert_eq!(
            decided(route(Some(&pending), &pressed("k3f", "aprove"))),
            (ApprovalOutcome::Rejected, None)
        );
    }

    /// A bare verb with no ticket is not an answer. It cannot be: resolving it
    /// against "the one pending prompt" is exactly the ambient authority the
    /// item removes.
    #[test]
    fn a_verb_without_a_ticket_is_not_an_answer() {
        let pending = pending_for("k3f");
        assert_eq!(route(Some(&pending), &text("/approve")), Routed::Unrelated);
        assert_eq!(route(Some(&pending), &text("/deny")), Routed::Unrelated);
    }

    fn from_sender(envelope: Envelope, sender: &str) -> Envelope {
        Envelope {
            sender: Arc::from(sender),
            ..envelope
        }
    }

    /// `AUTH-001`, and the plan's acceptance criterion: "A second group
    /// participant cannot approve or clear the initiator's state."
    ///
    /// The ticket is right and the prompt is live — this is not staleness. It
    /// is the wrong person, and the answer must decide nothing.
    #[test]
    fn another_participant_cannot_answer_the_prompt() {
        let pending = pending_for("k3f");
        let answer = from_sender(text("/approve k3f"), "someone-else");

        assert_eq!(
            route(Some(&pending), &answer),
            Routed::NotYours {
                ticket: ticket("k3f")
            }
        );
    }

    /// Including by button, which is the easier path to press by accident and
    /// the harder one to notice.
    #[test]
    fn another_participant_cannot_press_the_button() {
        let pending = pending_for("k3f");
        let answer = from_sender(pressed("k3f", APPROVE), "someone-else");

        assert_eq!(
            route(Some(&pending), &answer),
            Routed::NotYours {
                ticket: ticket("k3f")
            }
        );
    }

    #[test]
    fn the_sender_who_was_asked_still_decides() {
        let pending = pending_for("k3f");
        assert_eq!(
            decided(route(Some(&pending), &text("/approve k3f"))).0,
            ApprovalOutcome::Approved
        );
    }

    /// The named exception. An administrator can unblock someone else's
    /// prompt — deliberately configured, never the default.
    #[test]
    fn a_configured_administrator_may_answer_for_someone_else() {
        let pending = pending_for("k3f")
            .with_administrators(vec![actor("ops", "test"), actor("oncall", "test")]);
        let answer = from_sender(text("/approve k3f"), "oncall");

        assert_eq!(
            decided(route(Some(&pending), &answer)).0,
            ApprovalOutcome::Approved
        );
        // And someone who is not on that list still cannot.
        let intruder = from_sender(text("/approve k3f"), "someone-else");
        assert_eq!(
            route(Some(&pending), &intruder),
            Routed::NotYours {
                ticket: ticket("k3f")
            }
        );
    }

    #[test]
    fn an_administrator_identity_does_not_cross_channels() {
        let pending = pending_for("k3f").with_administrators(vec![actor("oncall", "telegram")]);
        let mut answer = from_sender(text("/approve k3f"), "oncall");
        answer.channel_id = ChannelId::new("feishu");

        assert_eq!(
            route(Some(&pending), &answer),
            Routed::NotYours {
                ticket: ticket("k3f")
            }
        );
    }

    /// A wrong ticket from the wrong sender is stale, not `NotYours`: there is
    /// no live prompt by that name to be excluded from.
    #[test]
    fn a_wrong_ticket_is_stale_whoever_sent_it() {
        let pending = pending_for("k3f");
        let answer = from_sender(text("/approve zz9"), "someone-else");
        assert_eq!(
            route(Some(&pending), &answer),
            Routed::Stale {
                ticket: ticket("zz9")
            }
        );
    }

    #[test]
    fn only_approved_permits_the_action() {
        assert!(ApprovalOutcome::Approved.permits_action());
        for outcome in [
            ApprovalOutcome::Rejected,
            ApprovalOutcome::Cancelled,
            ApprovalOutcome::Unavailable,
        ] {
            assert!(!outcome.permits_action(), "{outcome:?} must fail closed");
        }
    }

    /// Two prompts for the same action must not share a name.
    #[test]
    fn minted_tickets_are_unique() {
        let minted: Vec<_> = (0..64)
            .map(|_| ApprovalTicket::mint().expect("OS entropy is available"))
            .collect();
        let mut seen = std::collections::BTreeSet::new();
        for ticket in &minted {
            assert!(seen.insert(ticket.as_str().to_owned()), "{ticket} repeated");
            assert!(ApprovalTicket::parse(ticket.as_str()).is_some());
        }
    }

    /// Round-trip: what a channel renders is what it can read back.
    #[test]
    fn action_payloads_round_trip_within_telegrams_limit() {
        let ticket = ApprovalTicket::mint().expect("OS entropy is available");
        let actions = prompt_actions(&ticket);
        let entries = actions.as_array().expect("an array of buttons");
        assert_eq!(entries.len(), 2);
        for (entry, expected) in entries.iter().zip([APPROVE, DENY]) {
            let data = entry.get("data").and_then(Value::as_str).expect("data");
            assert!(data.len() <= 64, "callback_data must fit Telegram's limit");
            assert_eq!(parse_action_data(data), Some((expected, ticket.clone())));
        }
    }

    struct FixedNonce {
        calls: AtomicUsize,
    }

    impl NonceSource for FixedNonce {
        fn fill(&self, nonce: &mut [u8; 16]) -> Result<(), TicketError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            *nonce = [7; 16];
            Ok(())
        }
    }

    #[test]
    /// AF-011: concurrent first use shares one process nonce and cannot issue
    /// duplicate prompt identities.
    fn synchronized_prompts_receive_distinct_ids() {
        let source = Arc::new(FixedNonce {
            calls: AtomicUsize::new(0),
        });
        let issuer = Arc::new(OnceLock::<Result<TicketIssuer, TicketError>>::new());
        let barrier = Arc::new(Barrier::new(16));
        let mut threads = Vec::new();
        for _ in 0..16 {
            let source = Arc::clone(&source);
            let issuer = Arc::clone(&issuer);
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                issuer
                    .get_or_init(|| TicketIssuer::initialize(source.as_ref()))
                    .as_ref()
                    .expect("fixed nonce succeeds")
                    .mint()
                    .expect("counter has capacity")
            }));
        }
        let tickets: std::collections::BTreeSet<_> = threads
            .into_iter()
            .map(|thread| thread.join().expect("issuer thread").to_string())
            .collect();
        assert_eq!(tickets.len(), 16);
        assert_eq!(source.calls.load(Ordering::Relaxed), 1);
    }

    struct FailedNonce;

    impl NonceSource for FailedNonce {
        fn fill(&self, _nonce: &mut [u8; 16]) -> Result<(), TicketError> {
            Err(TicketError::EntropyUnavailable)
        }
    }

    #[test]
    fn entropy_failure_and_counter_exhaustion_fail_closed() {
        assert_eq!(
            TicketIssuer::initialize(&FailedNonce).err(),
            Some(TicketError::EntropyUnavailable)
        );
        let issuer = TicketIssuer {
            prefix: Arc::from("a1fixed"),
            next: AtomicU64::new(u64::MAX),
        };
        assert_eq!(issuer.mint(), Err(TicketError::CounterExhausted));
    }

    #[test]
    fn ticket_serialization_round_trips_without_case_folding() {
        let ticket = ApprovalTicket::mint().expect("OS entropy is available");
        let json = serde_json::to_string(&ticket).expect("ticket serializes");
        let restored: ApprovalTicket = serde_json::from_str(&json).expect("ticket deserializes");
        assert_eq!(restored, ticket);
    }

    #[test]
    fn a_payload_that_is_not_ours_is_not_parsed() {
        assert_eq!(parse_action_data("open:settings"), None);
        assert_eq!(parse_action_data("approve"), None);
        assert_eq!(parse_action_data("approve:not a ticket"), None);
    }
}
