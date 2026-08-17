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
//! The queue is bounded. A conversation that outruns its own agent gets its
//! oldest un-claimed message dropped rather than an unbounded buffer, and the
//! caller is told (see [`Steering::push`]) so it can say so.

use super::telemetry::field_key;
use super::LoopDeps;
use crate::trace;
use agentos_interfaces::run_state::RunState;
use agentos_interfaces::session::Item;
use agentos_proto::{Message, SpanId};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Messages one conversation may have waiting for its in-flight run.
///
/// Deliberately small: this is the "you typed again while it was working"
/// buffer, not a work queue. Anything past it is the conversation talking to
/// itself faster than the agent can answer.
pub const DEFAULT_STEERING_CAPACITY: usize = 32;

/// What [`Steering::push`] did with a message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Steered {
    /// Queued; the run will claim it at its next `Plan`.
    Queued,
    /// Queued, but the queue was full so the oldest waiting message was
    /// dropped to make room. The newest input is the one the user most likely
    /// means, so it wins.
    Displaced,
}

/// The mid-run input queue for one conversation, shared between whoever
/// receives (the gateway's router, on another thread) and the run loop.
#[derive(Clone)]
pub struct Steering {
    queue: Arc<Mutex<VecDeque<Message>>>,
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
            queue: Arc::new(Mutex::new(VecDeque::new())),
            capacity: capacity.max(1),
        }
    }

    /// Queue `message` for the in-flight run.
    pub fn push(&self, message: Message) -> Steered {
        let mut queue = self.lock();
        let displaced = if queue.len() >= self.capacity {
            queue.pop_front();
            true
        } else {
            false
        };
        queue.push_back(message);
        if displaced {
            Steered::Displaced
        } else {
            Steered::Queued
        }
    }

    /// Take everything waiting, oldest first, leaving the queue empty.
    pub fn take(&self) -> Vec<Message> {
        self.lock().drain(..).collect()
    }

    /// How many messages are waiting to be claimed.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Message>> {
        // A panic while holding this lock leaves at most a partially updated
        // queue of user messages — nothing an invariant depends on — so
        // recovering beats poisoning the whole conversation.
        self.queue.lock().unwrap_or_else(|err| err.into_inner())
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
    let claimed = steering.take();
    if claimed.is_empty() {
        return;
    }
    let count = claimed.len();
    for message in claimed {
        state.transcript.items.push(Item {
            message,
            metadata: BTreeMap::new(),
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
    use agentos_proto::MessageRole;

    fn user(text: &str) -> Message {
        Message::text(MessageRole::User, text)
    }

    #[test]
    fn takes_in_arrival_order_and_empties() {
        let steering = Steering::default();
        assert_eq!(steering.push(user("one")), Steered::Queued);
        assert_eq!(steering.push(user("two")), Steered::Queued);
        let taken = steering.take();
        assert_eq!(
            taken.iter().map(|m| m.content.as_ref()).collect::<Vec<_>>(),
            vec!["one", "two"]
        );
        assert!(steering.is_empty());
        assert!(steering.take().is_empty());
    }

    /// The bound drops the *oldest* waiting message, and says so.
    #[test]
    fn a_full_queue_displaces_its_oldest_message() {
        let steering = Steering::bounded(2);
        steering.push(user("one"));
        steering.push(user("two"));
        assert_eq!(steering.push(user("three")), Steered::Displaced);
        assert_eq!(steering.len(), 2);
        assert_eq!(
            steering
                .take()
                .iter()
                .map(|m| m.content.as_ref())
                .collect::<Vec<_>>(),
            vec!["two", "three"]
        );
    }

    /// A zero capacity would otherwise discard every steer silently.
    #[test]
    fn zero_capacity_still_holds_one() {
        let steering = Steering::bounded(0);
        steering.push(user("only"));
        assert_eq!(steering.len(), 1);
    }

    /// Both halves see one queue: the gateway pushes, the run loop takes.
    #[test]
    fn clones_share_one_queue() {
        let writer = Steering::default();
        let reader = writer.clone();
        writer.push(user("hello"));
        assert_eq!(reader.take().len(), 1);
        assert!(writer.is_empty());
    }
}
