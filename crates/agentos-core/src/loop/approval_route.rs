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

use agentos_proto::{Envelope, InterruptionId};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalTicket(Arc<str>);

impl ApprovalTicket {
    /// Mint a ticket for a new prompt.
    ///
    /// Seeded from the wall clock so tickets do not restart at the same value
    /// after a process restart: a button pressed from before a restart should
    /// look stale, not land on whatever prompt now holds that number.
    pub fn mint() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let seeded = NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(if current == 0 { seed() } else { current + 1 })
        });
        let value = match seeded {
            Ok(0) => seed(),
            Ok(previous) => previous + 1,
            // `fetch_update` with an always-`Some` closure cannot fail.
            Err(previous) => previous,
        };
        Self(Arc::from(base36(value)))
    }

    /// Read a ticket someone sent back. Rejects anything that is not the
    /// alphabet [`ApprovalTicket::mint`] produces, so a user's prose can never
    /// be mistaken for a ticket.
    pub fn parse(input: &str) -> Option<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty()
            || trimmed.len() > 32
            || !trimmed.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return None;
        }
        Some(Self(Arc::from(trimmed.to_ascii_lowercase())))
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

fn seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_micros() as u64)
        // A clock before the epoch is not worth failing a run over; any
        // non-zero start is fine because uniqueness only has to hold among
        // prompts that are alive at the same time.
        .unwrap_or(1)
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
    Decides {
        outcome: ApprovalOutcome,
        reason: Option<Arc<str>>,
    },
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
    pub ticket: ApprovalTicket,
    /// The sender the prompt was put to: whoever sent the message that caused
    /// the run to pause.
    pub asked_of: Arc<str>,
    /// Senders who may answer any prompt in this deployment, from
    /// `[policy] approval_administrators`. Empty in the usual case.
    pub administrators: Vec<Arc<str>>,
}

impl ApprovalBinding {
    pub fn new(ticket: ApprovalTicket, asked_of: impl Into<Arc<str>>) -> Self {
        Self {
            ticket,
            asked_of: asked_of.into(),
            administrators: Vec::new(),
        }
    }

    pub fn with_administrators(mut self, administrators: Vec<Arc<str>>) -> Self {
        self.administrators = administrators;
        self
    }

    /// Whether `sender` may answer this prompt.
    fn answerable_by(&self, sender: &str) -> bool {
        self.asked_of.as_ref() == sender
            || self
                .administrators
                .iter()
                .any(|admin| admin.as_ref() == sender)
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
    if !pending.answerable_by(&envelope.sender) {
        return Routed::NotYours { ticket };
    }
    let outcome = if decision == APPROVE {
        ApprovalOutcome::Approved
    } else {
        ApprovalOutcome::Rejected
    };
    Routed::Decides { outcome, reason }
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
    use agentos_proto::{ChannelId, ConversationId, Message, MessageRole};
    use std::collections::BTreeMap;

    /// The sender every envelope in these tests carries.
    const SENDER: &str = "user";

    /// A prompt put to that sender, so these tests exercise ticket routing
    /// rather than the sender check — which has its own tests below.
    fn pending_for(ticket_value: &str) -> ApprovalBinding {
        ApprovalBinding::new(ticket(ticket_value), SENDER)
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
            route(Some(&pending), &text("/approve k3f")),
            Routed::Decides {
                outcome: ApprovalOutcome::Approved,
                reason: None
            }
        );
        assert_eq!(
            route(Some(&pending), &text("/deny k3f too risky")),
            Routed::Decides {
                outcome: ApprovalOutcome::Rejected,
                reason: Some(Arc::from("too risky")),
            }
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
            route(Some(&pending), &pressed("k3f", "aprove")),
            Routed::Decides {
                outcome: ApprovalOutcome::Rejected,
                reason: None
            }
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
        assert!(matches!(
            route(Some(&pending), &text("/approve k3f")),
            Routed::Decides {
                outcome: ApprovalOutcome::Approved,
                ..
            }
        ));
    }

    /// The named exception. An administrator can unblock someone else's
    /// prompt — deliberately configured, never the default.
    #[test]
    fn a_configured_administrator_may_answer_for_someone_else() {
        let pending =
            pending_for("k3f").with_administrators(vec![Arc::from("ops"), Arc::from("oncall")]);
        let answer = from_sender(text("/approve k3f"), "oncall");

        assert!(matches!(
            route(Some(&pending), &answer),
            Routed::Decides {
                outcome: ApprovalOutcome::Approved,
                ..
            }
        ));
        // And someone who is not on that list still cannot.
        let intruder = from_sender(text("/approve k3f"), "someone-else");
        assert_eq!(
            route(Some(&pending), &intruder),
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
        let minted: Vec<_> = (0..64).map(|_| ApprovalTicket::mint()).collect();
        let mut seen = std::collections::BTreeSet::new();
        for ticket in &minted {
            assert!(seen.insert(ticket.as_str().to_owned()), "{ticket} repeated");
            assert!(ApprovalTicket::parse(ticket.as_str()).is_some());
        }
    }

    /// Round-trip: what a channel renders is what it can read back.
    #[test]
    fn action_payloads_round_trip_within_telegrams_limit() {
        let ticket = ApprovalTicket::mint();
        let actions = prompt_actions(&ticket);
        let entries = actions.as_array().expect("an array of buttons");
        assert_eq!(entries.len(), 2);
        for (entry, expected) in entries.iter().zip([APPROVE, DENY]) {
            let data = entry.get("data").and_then(Value::as_str).expect("data");
            assert!(data.len() <= 64, "callback_data must fit Telegram's limit");
            assert_eq!(parse_action_data(data), Some((expected, ticket.clone())));
        }
    }

    #[test]
    fn a_payload_that_is_not_ours_is_not_parsed() {
        assert_eq!(parse_action_data("open:settings"), None);
        assert_eq!(parse_action_data("approve"), None);
        assert_eq!(parse_action_data("approve:not a ticket"), None);
    }
}
