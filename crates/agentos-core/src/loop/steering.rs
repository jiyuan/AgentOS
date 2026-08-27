//! Input that arrives while a run is already in flight (roadmap item G1).
//!
//! A conversation is strictly serial: one run at a time, because two runs on
//! one conversation would both load the transcript, both append to it, and the
//! second write-back would clobber the first. So a message that lands mid-run
//! cannot start a second run. It also must not be dropped, and making the user
//! wait for the whole run to end before the agent even reads it is what makes
//! a long tool call feel like a hang.
//!
//! It is instead queued here and claimed by the run itself at its next `Plan`,
//! which is the one point in the loop where new user input is safe to fold in:
//! the previous turn has been observed and recorded, and the next planning call
//! has not been assembled yet. From the model's side a steered message is an
//! ordinary user turn that happens to arrive between tool calls, so "actually,
//! stop and do X instead" works without any new orchestrator vocabulary.
//!
//! The queue is bounded. A conversation that outruns its own agent displaces
//! its oldest un-claimed envelope rather than growing an unbounded buffer, and
//! returns that exact envelope (see [`Steering::push`]) so durable ingress can
//! give its sender a terminal disposition.

use super::telemetry::field_key;
use super::LoopDeps;
use crate::trace;
use agentos_interfaces::run_state::RunState;
use agentos_interfaces::session::Item;
use agentos_proto::{Envelope, SpanId};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Messages one conversation may have waiting for its in-flight run.
///
/// Deliberately small: this is the "you typed again while it was working"
/// buffer, not a work queue. Anything past it is the conversation talking to
/// itself faster than the agent can answer.
pub const DEFAULT_STEERING_CAPACITY: usize = 32;

/// What [`Steering::push`] did with an envelope.
#[derive(Clone, Debug, PartialEq)]
pub enum Steered {
    /// Queued; the run will claim it at its next `Plan`.
    Queued,
    /// Queued, but the queue was full so the oldest waiting envelope was
    /// displaced to make room. The newest input is the one the user most
    /// likely means, so it wins; the value is the exact displaced input.
    Displaced(Envelope),
}

#[derive(Default)]
struct SteeringState {
    queued: VecDeque<Envelope>,
    claimed: Vec<Envelope>,
}

/// The mid-run input queue for one conversation, shared between whoever
/// receives (the gateway's router, on another thread) and the run loop.
#[derive(Clone)]
pub struct Steering {
    state: Arc<Mutex<SteeringState>>,
    capacity: usize,
}

impl Default for Steering {
    fn default() -> Self {
        Self::bounded(DEFAULT_STEERING_CAPACITY)
    }
}

impl Steering {
    /// A queue holding at most `capacity` un-claimed messages. A capacity of
    /// zero is raised to one: a queue that cannot hold anything would silently
    /// discard every steer, which is worse than the bound it was asked for.
    pub fn bounded(capacity: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(SteeringState::default())),
            capacity: capacity.max(1),
        }
    }

    /// Queue `envelope` for the in-flight run.
    ///
    /// A displacement returns the exact envelope that lost its place. The
    /// caller can therefore requeue it or produce a terminal sender-facing
    /// outcome and settle that event's own durable ingress row.
    pub fn push(&self, envelope: Envelope) -> Steered {
        let mut state = self.lock();
        let displaced = if state.queued.len() >= self.capacity {
            state.queued.pop_front()
        } else {
            None
        };
        state.queued.push_back(envelope);
        match displaced {
            Some(displaced) => Steered::Displaced(displaced),
            None => Steered::Queued,
        }
    }

    /// Claim everything waiting for the active run, oldest first.
    ///
    /// Claimed envelopes remain available through [`Self::take_claimed`] so
    /// the gateway can settle the exact transport events the run consumed.
    pub fn claim_all(&self) -> Vec<Envelope> {
        let mut state = self.lock();
        let claimed: Vec<_> = state.queued.drain(..).collect();
        state.claimed.extend(claimed.iter().cloned());
        claimed
    }

    /// Take envelopes consumed by the active run, oldest first.
    pub fn take_claimed(&self) -> Vec<Envelope> {
        self.lock().claimed.drain(..).collect()
    }

    /// Snapshot envelopes already folded into this run without consuming the
    /// settlement identities retained for terminal delivery.
    pub(crate) fn claimed_snapshot(&self) -> Vec<Envelope> {
        self.lock().claimed.clone()
    }

    /// Take everything the active run did not claim, oldest first.
    pub fn take_unclaimed(&self) -> Vec<Envelope> {
        self.lock().queued.drain(..).collect()
    }

    /// How many messages are waiting to be claimed.
    pub fn len(&self) -> usize {
        self.lock().queued.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().queued.is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SteeringState> {
        // A panic while holding this lock leaves at most a partially updated
        // queue of user messages — nothing an invariant depends on — so
        // recovering beats poisoning the whole conversation.
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }
}

/// Fold any waiting mid-run input into the transcript, at the `Plan` boundary.
///
/// The claimed messages become ordinary user items, so they are persisted with
/// the rest of the run (the runner appends everything the loop added past the
/// mark it took at load) and the orchestrator sees them as the newest turns.
pub(super) fn claim(state: &mut RunState, deps: &LoopDeps<'_>, plan_span_id: &SpanId) {
    let Some(steering) = deps.steering.as_ref() else {
        return;
    };
    let claimed = steering.claim_all();
    if claimed.is_empty() {
        return;
    }
    let count = claimed.len();
    for envelope in claimed {
        state.transcript.items.push(Item {
            message: envelope.message,
            metadata: envelope.metadata,
        });
    }
    let mut fields = BTreeMap::new();
    fields.insert(field_key("messages"), Value::from(count));
    trace::record_event(
        state,
        deps.hooks,
        plan_span_id.clone(),
        "steering_claimed",
        fields,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{ChannelId, ConversationId, Message, MessageRole};
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
    fn takes_in_arrival_order_and_empties() {
        let steering = Steering::default();
        assert_eq!(steering.push(envelope("one")), Steered::Queued);
        assert_eq!(steering.push(envelope("two")), Steered::Queued);
        let taken = steering.claim_all();
        assert_eq!(
            taken
                .iter()
                .map(|e| e.message.content.as_ref())
                .collect::<Vec<_>>(),
            vec!["one", "two"]
        );
        assert!(steering.is_empty());
        assert!(steering.claim_all().is_empty());
        assert_eq!(steering.take_claimed(), taken);
    }

    /// The bound drops the *oldest* waiting message, and says so.
    #[test]
    fn a_full_queue_displaces_its_oldest_message() {
        let steering = Steering::bounded(2);
        steering.push(envelope("one"));
        steering.push(envelope("two"));
        assert_eq!(
            steering.push(envelope("three")),
            Steered::Displaced(envelope("one"))
        );
        assert_eq!(steering.len(), 2);
        assert_eq!(
            steering
                .take_unclaimed()
                .iter()
                .map(|e| e.message.content.as_ref())
                .collect::<Vec<_>>(),
            vec!["two", "three"]
        );
    }

    /// A zero capacity would otherwise discard every steer silently.
    #[test]
    fn zero_capacity_still_holds_one() {
        let steering = Steering::bounded(0);
        steering.push(envelope("only"));
        assert_eq!(steering.len(), 1);
    }

    /// Both halves see one queue: the gateway pushes, the run loop takes.
    #[test]
    fn clones_share_one_queue() {
        let writer = Steering::default();
        let reader = writer.clone();
        writer.push(envelope("hello"));
        assert_eq!(reader.claim_all().len(), 1);
        assert!(writer.is_empty());
    }
}
