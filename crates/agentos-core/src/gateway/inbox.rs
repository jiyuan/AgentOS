//! One conversation's mailbox and the run currently draining it (roadmap G1).
//!
//! A conversation is an actor: strictly one run at a time, because two runs on
//! the same conversation both load the transcript, both append to it, and the
//! second write-back silently drops the first one's turns. Everything else in
//! G1 — the shard threads, the stable-hash routing — exists to get concurrency
//! *between* conversations without losing that serialization *within* one.
//!
//! Inbound envelopes therefore land here rather than starting a run, and split
//! by what the conversation is doing when they arrive:
//!
//! - **next-turn** — nothing is running, or the running turn is past the point
//!   where it can absorb more input. The envelope waits for a run of its own.
//! - **next-step** — a run is in flight. The envelope is queued on that run's
//!   [`Steering`] handle and claimed at its next `Plan`, so "actually, do X
//!   instead" reaches the model in seconds rather than after the tool call it
//!   is trying to interrupt.
//!
//! Both lists are bounded. The steer queue drops its oldest entry (the newest
//! instruction is the one the user means); the turn queue refuses the newest
//! (a run already queued is work the user asked for, and dropping it to make
//! room for a later message would lose the request silently).

use crate::r#loop::{Steered, Steering};
use agentos_proto::Envelope;
use std::collections::VecDeque;
use tokio_util::sync::CancellationToken;

/// Envelopes one conversation may have waiting for a run of their own.
///
/// Small on purpose: a conversation with a deep backlog is one whose user is
/// talking much faster than the agent answers, and queueing hundreds of turns
/// would mean working through a stale backlog long after the user gave up.
pub const DEFAULT_INBOX_CAPACITY: usize = 32;

/// Where an admitted envelope went.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admitted {
    /// Queued for a run of its own; nothing is in flight.
    NextTurn,
    /// Handed to the in-flight run, which will claim it at its next `Plan`.
    NextStep,
    /// Handed to the in-flight run, displacing the oldest un-claimed steer.
    NextStepDisplacing,
    /// Refused: the turn queue is full.
    ///
    /// The caller *should* tell the user — a silently dropped message is
    /// indistinguishable from an ignored one — and today it does not. The
    /// shard loop logs `inbox full; message refused` and drops it, because the
    /// loop that admits has no egress to answer on. See
    /// `docs/OBSERVABILITY.md`, which names this as the open gap rather than
    /// leaving this comment describing behaviour the tree does not have
    /// (M9 / `CI-002`).
    Full,
}

impl Admitted {
    /// Whether the envelope was taken at all.
    pub fn accepted(self) -> bool {
        !matches!(self, Self::Full)
    }
}

/// One conversation's mailbox plus a handle on its active run.
///
/// Not itself shareable: the shard owns it and is the only thing that touches
/// it. What crosses threads is the [`Steering`] handle inside, which the run
/// loop reads, and the [`CancellationToken`], which `/stop` fires.
pub struct Inbox {
    capacity: usize,
    /// Envelopes waiting for a run of their own, oldest first.
    next_turn: VecDeque<Envelope>,
    /// Set while a run is in flight: where to queue input for it, and how to
    /// stop it.
    active: Option<ActiveRun>,
}

/// The run currently draining a conversation.
struct ActiveRun {
    steering: Steering,
    cancel: CancellationToken,
}

impl Inbox {
    pub fn bounded(capacity: usize) -> Self {
        Self {
            // A zero-capacity mailbox would refuse every message, which reads
            // as a dead conversation rather than a configured bound.
            capacity: capacity.max(1),
            next_turn: VecDeque::new(),
            active: None,
        }
    }

    /// Take `envelope`, routing it to the in-flight run if there is one.
    pub fn admit(&mut self, envelope: Envelope) -> Admitted {
        if let Some(active) = self.active.as_ref() {
            return match active.steering.push(envelope.message) {
                Steered::Queued => Admitted::NextStep,
                Steered::Displaced => Admitted::NextStepDisplacing,
            };
        }
        if self.next_turn.len() >= self.capacity {
            return Admitted::Full;
        }
        self.next_turn.push_back(envelope);
        Admitted::NextTurn
    }

    /// Take the next envelope to run, if one is waiting and nothing is already
    /// running. Returns `None` while a run is in flight — that is the
    /// serialization guarantee, enforced here rather than trusted to callers.
    pub fn claim(&mut self) -> Option<Envelope> {
        if self.active.is_some() {
            return None;
        }
        self.next_turn.pop_front()
    }

    /// Record that a run has started, and with what handles.
    ///
    /// Called by the shard immediately after [`Inbox::claim`], so the window in
    /// which a message could arrive and find nothing to steer is the shard's
    /// own straight-line code.
    pub fn begin(&mut self, steering: Steering, cancel: CancellationToken) {
        self.active = Some(ActiveRun { steering, cancel });
    }

    /// Record that the active run has ended. Anything it never got round to
    /// claiming is *not* lost: the shard hands leftovers back through
    /// [`Inbox::requeue`] so the next turn sees them.
    pub fn end(&mut self) -> Option<Steering> {
        self.active.take().map(|active| active.steering)
    }

    /// Put an envelope back at the front of the turn queue.
    ///
    /// Used for input a finished run never claimed. It jumps the queue because
    /// it is older than everything already waiting.
    pub fn requeue(&mut self, envelope: Envelope) {
        self.next_turn.push_front(envelope);
    }

    /// Cancel the active run, if any. Returns whether there was one — the
    /// difference between "stopping" and "nothing to stop", which is what a
    /// user typing `/stop` needs to be told.
    pub fn stop(&self) -> bool {
        match self.active.as_ref() {
            Some(active) => {
                active.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Whether a run is in flight for this conversation.
    pub fn is_running(&self) -> bool {
        self.active.is_some()
    }

    /// Nothing running and nothing waiting: the shard may forget it.
    pub fn is_idle(&self) -> bool {
        self.active.is_none() && self.next_turn.is_empty()
    }

    /// How many envelopes are waiting for a run of their own.
    pub fn waiting(&self) -> usize {
        self.next_turn.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{ChannelId, ConversationId, Message, MessageRole};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn envelope(text: &str) -> Envelope {
        Envelope {
            channel_id: ChannelId::new("test"),
            conversation_id: ConversationId::new("conv"),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, text),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn an_idle_conversation_queues_for_a_run_of_its_own() {
        let mut inbox = Inbox::bounded(4);
        assert_eq!(inbox.admit(envelope("first")), Admitted::NextTurn);
        assert_eq!(inbox.waiting(), 1);
        assert_eq!(
            inbox.claim().expect("one waiting").message.content.as_ref(),
            "first"
        );
    }

    /// The serialization guarantee: while a run is in flight nothing else can
    /// be claimed, however much is waiting.
    #[test]
    fn a_running_conversation_yields_nothing_to_claim() {
        let mut inbox = Inbox::bounded(4);
        inbox.admit(envelope("first"));
        let first = inbox.claim().expect("one waiting");
        inbox.begin(Steering::default(), CancellationToken::new());
        assert_eq!(first.message.content.as_ref(), "first");
        assert!(inbox.claim().is_none(), "a second run must not start");
    }

    /// The item's own verification criterion: a second message during a run is
    /// steered rather than queued as a new run.
    #[test]
    fn a_message_during_a_run_steers_it_instead_of_queueing() {
        let mut inbox = Inbox::bounded(4);
        let steering = Steering::default();
        inbox.begin(steering.clone(), CancellationToken::new());

        assert_eq!(inbox.admit(envelope("actually, stop")), Admitted::NextStep);
        assert_eq!(inbox.waiting(), 0, "no second run was queued");
        assert_eq!(
            steering
                .take()
                .iter()
                .map(|m| m.content.as_ref())
                .collect::<Vec<_>>(),
            vec!["actually, stop"]
        );
    }

    /// A full steer queue drops its *oldest* entry and says so; a full turn
    /// queue refuses the *newest* and says so. Opposite ends, because a queued
    /// run is work already promised and a queued steer is a superseded thought.
    #[test]
    fn the_two_lists_shed_load_from_opposite_ends() {
        let mut steering_inbox = Inbox::bounded(4);
        steering_inbox.begin(Steering::bounded(1), CancellationToken::new());
        assert_eq!(steering_inbox.admit(envelope("a")), Admitted::NextStep);
        assert_eq!(
            steering_inbox.admit(envelope("b")),
            Admitted::NextStepDisplacing
        );

        let mut turn_inbox = Inbox::bounded(1);
        assert_eq!(turn_inbox.admit(envelope("a")), Admitted::NextTurn);
        assert_eq!(turn_inbox.admit(envelope("b")), Admitted::Full);
        assert!(!Admitted::Full.accepted());
        assert_eq!(turn_inbox.waiting(), 1);
    }

    /// Input a run never claimed goes back to the *front*: it is older than
    /// anything queued behind it.
    #[test]
    fn unclaimed_input_is_requeued_ahead_of_later_messages() {
        let mut inbox = Inbox::bounded(4);
        inbox.admit(envelope("later"));
        inbox.requeue(envelope("earlier"));
        assert_eq!(
            inbox.claim().expect("queued").message.content.as_ref(),
            "earlier"
        );
    }

    #[test]
    fn stop_cancels_the_active_run_and_reports_whether_there_was_one() {
        let mut inbox = Inbox::bounded(4);
        assert!(!inbox.stop(), "nothing running is not a stop");

        let cancel = CancellationToken::new();
        inbox.begin(Steering::default(), cancel.clone());
        assert!(inbox.stop());
        assert!(cancel.is_cancelled());

        inbox.end();
        assert!(!inbox.is_running());
        assert!(inbox.is_idle());
    }

    /// A conversation still holding queued work is not idle, so the shard must
    /// not forget it.
    #[test]
    fn a_conversation_with_queued_work_is_not_idle() {
        let mut inbox = Inbox::bounded(4);
        inbox.admit(envelope("waiting"));
        assert!(!inbox.is_idle());
    }
}
