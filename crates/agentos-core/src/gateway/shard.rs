//! Conversations as actors, sharded across threads (roadmap item G1).
//!
//! The gateway used to be one loop: receive an envelope, run it to completion,
//! send the reply, receive the next. Correct, and serial in the worst possible
//! place — one conversation waiting on a slow tool stalled every other
//! conversation on the deployment.
//!
//! The fix is the shape [`BENCHMARKS.md`]'s concurrency bench already models,
//! promoted from bench harness to product. The run loop's future is `!Send`
//! (sub-agent execution drives a `LocalSet`), so it can never be handed to a
//! work-stealing scheduler; the only scaling shape available is thread-per-core.
//! Each shard is one OS thread with its own `current_thread` runtime, and
//! conversations are assigned to shards by a stable hash of the conversation id
//! so one conversation always lands on one thread.
//!
//! Within a shard, turns run interleaved on a [`FuturesUnordered`] rather than
//! `spawn_local`. That is deliberate: a spawned task must be `'static`, which
//! would force every shard to build its own [`AgentRuntime`] — separate session
//! stores, separate memory, separate job registries. Polling borrowed futures
//! in place lets every shard share one runtime while keeping each `!Send` run
//! pinned to the thread that started it.
//!
//! Serialization within a conversation is [`Inbox`]'s job, not this module's:
//! a shard claims work through the inbox, which refuses to hand out a second
//! envelope while a run is in flight.
//!
//! [`BENCHMARKS.md`]: https://github.com/jiyuan/agentos/blob/main/BENCHMARKS.md
//! [`AgentRuntime`]: crate::runtime::AgentRuntime

use super::inbox::{Admitted, Inbox, DEFAULT_INBOX_CAPACITY};
use crate::r#loop::Steering;
use agentos_proto::{ConversationId, Envelope, Message};
use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// How long a shard sits quiet before it runs maintenance work.
///
/// Matches the old serial loop's one-second idle tick, so cron resolution is
/// unchanged by sharding.
pub const DEFAULT_IDLE_INTERVAL: Duration = Duration::from_secs(1);

/// Dispatches one shard may have queued before the router feels backpressure.
const SHARD_QUEUE_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardConfig {
    /// Shard threads. Conversations are spread across them by a stable hash, so
    /// changing this changes which thread a conversation lands on but never
    /// whether two of its runs can overlap.
    pub shards: usize,
    /// Envelopes one conversation may have waiting for a run of their own.
    pub inbox_capacity: usize,
    /// How long a shard must be quiet before [`TurnHandler::idle`] runs.
    pub idle_interval: Duration,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            shards: 1,
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            idle_interval: DEFAULT_IDLE_INTERVAL,
        }
    }
}

#[derive(Debug, Error)]
pub enum RouteError {
    #[error("shard {shard} has stopped")]
    ShardGone { shard: usize },
}

/// What the router hands a shard.
#[derive(Debug)]
enum Dispatch {
    /// Ordinary inbound input.
    Input(Envelope),
    /// Cancel whatever this conversation is running, and report whether there
    /// was anything to cancel.
    Stop(ConversationId, oneshot::Sender<bool>),
}

/// One turn's worth of work, handed to a [`TurnHandler`] on the shard thread.
pub struct Turn {
    /// The envelope that starts this turn.
    pub input: Envelope,
    /// Where input arriving mid-run queues up. Install it on the run's
    /// `RunnerDeps` so the loop claims it at each `Plan`.
    pub steering: Steering,
    /// This run's cancellation token, which `/stop` fires. Install it on the
    /// run's `RunnerDeps`.
    pub cancel: CancellationToken,
}

/// Runs one conversation turn, on the shard's own thread.
///
/// `?Send` because the run loop's future is `!Send`, and by the same token an
/// implementation may borrow freely from the shard thread's stack — which is
/// what lets every shard share one [`AgentRuntime`](crate::runtime::AgentRuntime)
/// rather than building its own.
#[async_trait(?Send)]
pub trait TurnHandler {
    /// Run `turn` to completion, including delivering its reply. Errors are the
    /// handler's to report: a failed turn must not stop the shard, since the
    /// other conversations on it are unaffected.
    async fn run(&self, turn: Turn);

    /// Called when this shard has been quiet for
    /// [`ShardConfig::idle_interval`] — nothing running, nothing queued.
    ///
    /// This is where the cron scan and memory reflection belong, so maintenance
    /// competes with nothing. It is called on *every* shard, so work that must
    /// happen once per process (firing a due cron) has to be guarded on
    /// `shard`; work that is per-shard state needs no guard.
    async fn idle(&self, shard: usize) {
        let _ = shard;
    }
}

/// The dispatch side of a shard set: hashes conversations onto shards.
///
/// Cheap to clone and `Send + Sync`, so the thread that owns the channel can
/// route from wherever it receives.
#[derive(Clone)]
pub struct Router {
    shards: Vec<mpsc::Sender<Dispatch>>,
}

impl Router {
    /// Which shard owns `conversation`.
    ///
    /// A stable hash, not round-robin: a conversation must land on the same
    /// shard every time or its runs could overlap on different threads, which
    /// is the one thing sharding must not allow.
    pub fn shard_of(&self, conversation: &ConversationId) -> usize {
        let mut hasher = DefaultHasher::new();
        conversation.as_str().hash(&mut hasher);
        (hasher.finish() % self.shards.len() as u64) as usize
    }

    /// Hand `envelope` to its conversation's shard.
    ///
    /// Awaits if that shard's queue is full. A shard drains its queue in
    /// straight-line code — claiming work never blocks its loop, because turns
    /// are polled alongside it rather than awaited by it — so a full queue
    /// means genuine saturation and the backpressure is the correct answer.
    pub async fn deliver(&self, envelope: Envelope) -> Result<(), RouteError> {
        let shard = self.shard_of(&envelope.conversation_id);
        self.send(shard, Dispatch::Input(envelope)).await
    }

    /// Cancel whatever `conversation` is running.
    ///
    /// Reports whether there was a run to cancel: "stopping" and "nothing was
    /// running" are different answers, and a user who typed `/stop` because the
    /// agent looked stuck needs to be told which one they got.
    pub async fn stop(&self, conversation: &ConversationId) -> Result<bool, RouteError> {
        let shard = self.shard_of(conversation);
        let (answer, stopped) = oneshot::channel();
        self.send(shard, Dispatch::Stop(conversation.clone(), answer))
            .await?;
        stopped.await.map_err(|_| RouteError::ShardGone { shard })
    }

    /// Close every shard's queue, which ends its loop once it has drained.
    pub fn shutdown(&mut self) {
        self.shards.clear();
    }

    async fn send(&self, shard: usize, dispatch: Dispatch) -> Result<(), RouteError> {
        self.shards
            .get(shard)
            .ok_or(RouteError::ShardGone { shard })?
            .send(dispatch)
            .await
            .map_err(|_| RouteError::ShardGone { shard })
    }
}

/// One shard's inbound queue. Opaque: the only thing to do with it is hand it
/// to [`run_shard`] on the thread that shard owns.
pub struct ShardInbound(mpsc::Receiver<Dispatch>);

/// Build a router plus one inbound queue per shard.
///
/// The caller starts a thread per queue and calls [`run_shard`] on it.
pub fn shard_set(config: &ShardConfig) -> (Router, Vec<ShardInbound>) {
    let shards = config.shards.max(1);
    let mut senders = Vec::with_capacity(shards);
    let mut receivers = Vec::with_capacity(shards);
    for _ in 0..shards {
        let (tx, rx) = mpsc::channel(SHARD_QUEUE_CAPACITY);
        senders.push(tx);
        receivers.push(ShardInbound(rx));
    }
    (Router { shards: senders }, receivers)
}

/// Drive one shard until its queue closes and its in-flight turns finish.
///
/// Runs on the calling thread's runtime, which must be `current_thread` — the
/// turns it polls are `!Send` and must never migrate.
pub async fn run_shard<H: TurnHandler>(
    shard: usize,
    inbound: ShardInbound,
    config: &ShardConfig,
    handler: &H,
) {
    let ShardInbound(mut inbound) = inbound;
    let mut conversations: HashMap<ConversationId, Inbox> = HashMap::new();
    let mut running = FuturesUnordered::new();
    // Conversations that may have claimable work. Cheaper than rescanning the
    // whole map on every event, and correct because the only two ways work can
    // become claimable are an arrival and a completion.
    let mut ready: Vec<ConversationId> = Vec::new();
    let mut open = true;

    loop {
        // Start every turn that can start before parking.
        while let Some(conversation) = ready.pop() {
            let Some(inbox) = conversations.get_mut(&conversation) else {
                continue;
            };
            let Some(input) = inbox.claim() else {
                continue;
            };
            let steering = Steering::bounded(config.inbox_capacity);
            let cancel = CancellationToken::new();
            inbox.begin(steering.clone(), cancel.clone());
            let turn = Turn {
                input: input.clone(),
                steering,
                cancel,
            };
            running.push(async move {
                handler.run(turn).await;
                (conversation, input)
            });
        }

        if !open && running.is_empty() {
            return;
        }

        tokio::select! {
            biased;
            dispatch = inbound.recv(), if open => match dispatch {
                Some(Dispatch::Input(envelope)) => {
                    let conversation = envelope.conversation_id.clone();
                    let inbox = conversations
                        .entry(conversation.clone())
                        .or_insert_with(|| Inbox::bounded(config.inbox_capacity));
                    match inbox.admit(envelope) {
                        Admitted::NextTurn => ready.push(conversation),
                        Admitted::NextStep | Admitted::NextStepDisplacing => {}
                        // The refusal was decided in `Inbox::admit` and then
                        // discarded here, so a conversation that outran its
                        // `[gateway] inbox_capacity` lost the message with no
                        // trace anywhere — the one thing `Admitted::Full`'s own
                        // documentation says must not happen. Saturation is now
                        // at least *observable*; telling the sender needs an
                        // egress this loop does not have, and is recorded as
                        // the remaining gap in `docs/OBSERVABILITY.md`
                        // (M9 / `CI-002`).
                        Admitted::Full => tracing::warn!(
                            conversation_id = conversation.as_str(),
                            inbox_capacity = config.inbox_capacity,
                            "inbox full; message refused"
                        ),
                    }
                }
                Some(Dispatch::Stop(conversation, answer)) => {
                    let stopped = conversations
                        .get(&conversation)
                        .is_some_and(Inbox::stop);
                    // The asker may have given up; nobody else cares.
                    let _ = answer.send(stopped);
                }
                // The router is gone: finish what is in flight, then stop.
                None => open = false,
            },
            Some((conversation, input)) = running.next() => {
                finish_turn(&mut conversations, &conversation, input, &mut ready);
            }
            () = tokio::time::sleep(config.idle_interval),
                if open && running.is_empty() && ready.is_empty() =>
            {
                handler.idle(shard).await;
            }
        }
    }
}

/// Close out a finished turn: retire the run, recover anything it never
/// claimed, and re-queue the conversation if it still has work.
fn finish_turn(
    conversations: &mut HashMap<ConversationId, Inbox>,
    conversation: &ConversationId,
    input: Envelope,
    ready: &mut Vec<ConversationId>,
) {
    let Some(inbox) = conversations.get_mut(conversation) else {
        return;
    };
    // Anything still queued to steer was never claimed — the run finished
    // before its next `Plan`. It is input the user sent and nobody answered, so
    // it becomes the next turn rather than being dropped. Oldest first, and
    // each pushed to the front, so the original order survives.
    if let Some(steering) = inbox.end() {
        for message in steering.take().into_iter().rev() {
            inbox.requeue(unanswered(&input, message));
        }
    }
    if inbox.is_idle() {
        // Nothing running and nothing waiting: drop the entry rather than grow
        // a map entry per conversation the deployment has ever seen.
        conversations.remove(conversation);
    } else {
        ready.push(conversation.clone());
    }
}

/// Re-envelope a steer the finished run never got to, addressed like the turn
/// it arrived during.
fn unanswered(input: &Envelope, message: Message) -> Envelope {
    Envelope {
        channel_id: input.channel_id.clone(),
        conversation_id: input.conversation_id.clone(),
        sender: input.sender.clone(),
        message,
        metadata: input.metadata.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{ChannelId, MessageRole};
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use std::sync::Arc;

    fn envelope(conversation: &str, text: &str) -> Envelope {
        Envelope {
            channel_id: ChannelId::new("test"),
            conversation_id: ConversationId::new(conversation),
            sender: Arc::from("user"),
            message: Message::text(MessageRole::User, text),
            metadata: BTreeMap::new(),
        }
    }

    fn config(shards: usize) -> ShardConfig {
        ShardConfig {
            shards,
            inbox_capacity: 8,
            idle_interval: Duration::from_millis(50),
        }
    }

    /// Records what ran, in completion order, and can be told to stall a
    /// specific conversation.
    struct Recorder {
        finished: Rc<RefCell<Vec<String>>>,
        stall: Option<(String, Duration)>,
        idle_ticks: Rc<RefCell<usize>>,
        /// Set once a turn has begun, so a test can send its second message at
        /// the moment the first turn is genuinely in flight rather than
        /// guessing at a number of yields.
        started: Rc<RefCell<bool>>,
    }

    #[async_trait(?Send)]
    impl TurnHandler for Recorder {
        async fn run(&self, turn: Turn) {
            *self.started.borrow_mut() = true;
            let conversation = turn.input.conversation_id.as_str().to_owned();
            if let Some((slow, delay)) = &self.stall {
                if slow == &conversation {
                    tokio::time::sleep(*delay).await;
                }
            }
            // Claim whatever steered in, so the test can see it was delivered
            // to the running turn rather than queued as a second one.
            for message in turn.steering.take() {
                self.finished
                    .borrow_mut()
                    .push(format!("{conversation}:steer:{}", message.content));
            }
            self.finished
                .borrow_mut()
                .push(format!("{conversation}:{}", turn.input.message.content));
        }

        async fn idle(&self, _shard: usize) {
            *self.idle_ticks.borrow_mut() += 1;
        }
    }

    /// A stable hash, so a conversation cannot migrate between shards and have
    /// two of its runs overlap on different threads.
    #[test]
    fn routing_is_stable_per_conversation() {
        let (router, _rx) = shard_set(&config(4));
        let first = router.shard_of(&ConversationId::new("alice"));
        for _ in 0..16 {
            assert_eq!(router.shard_of(&ConversationId::new("alice")), first);
        }
        assert!(first < 4);
    }

    /// The item's exit condition, at shard level: one conversation's slow turn
    /// does not delay another's.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_slow_conversation_does_not_stall_another() {
        let finished = Rc::new(RefCell::new(Vec::new()));
        let handler = Recorder {
            finished: Rc::clone(&finished),
            stall: Some(("slow".to_owned(), Duration::from_secs(30))),
            idle_ticks: Rc::new(RefCell::new(0)),
            started: Rc::new(RefCell::new(false)),
        };
        let (tx, rx) = mpsc::channel(8);
        let rx = ShardInbound(rx);
        tx.send(Dispatch::Input(envelope("slow", "grind")))
            .await
            .expect("queued");
        tx.send(Dispatch::Input(envelope("quick", "hello")))
            .await
            .expect("queued");
        drop(tx);

        run_shard(0, rx, &config(1), &handler).await;

        assert_eq!(
            finished.borrow().as_slice(),
            ["quick:hello", "slow:grind"],
            "the quick conversation must not wait on the slow one"
        );
    }

    /// The item's other verification criterion: a second message during a run
    /// is claimed by that run rather than starting a second one.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_second_message_steers_the_running_turn() {
        let finished = Rc::new(RefCell::new(Vec::new()));
        let started = Rc::new(RefCell::new(false));
        let handler = Recorder {
            finished: Rc::clone(&finished),
            stall: Some(("conv".to_owned(), Duration::from_secs(5))),
            idle_ticks: Rc::new(RefCell::new(0)),
            started: Rc::clone(&started),
        };
        let (tx, rx) = mpsc::channel(8);
        let rx = ShardInbound(rx);
        tx.send(Dispatch::Input(envelope("conv", "first")))
            .await
            .expect("queued");

        let driver = async {
            while !*started.borrow() {
                tokio::task::yield_now().await;
            }
            tx.send(Dispatch::Input(envelope("conv", "second")))
                .await
                .expect("queued");
            drop(tx);
        };
        let config = config(1);
        tokio::join!(run_shard(0, rx, &config, &handler), driver);

        assert_eq!(
            finished.borrow().as_slice(),
            ["conv:steer:second", "conv:first"],
            "the second message must reach the running turn, not a second run"
        );
    }

    /// Maintenance runs when the shard has nothing else to do.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_idle_shard_runs_maintenance() {
        let idle_ticks = Rc::new(RefCell::new(0));
        let handler = Recorder {
            finished: Rc::new(RefCell::new(Vec::new())),
            stall: None,
            idle_ticks: Rc::clone(&idle_ticks),
            started: Rc::new(RefCell::new(false)),
        };
        let (tx, rx) = mpsc::channel(8);
        let rx = ShardInbound(rx);
        let driver = async {
            tokio::time::sleep(Duration::from_millis(260)).await;
            drop(tx);
        };
        let config = config(1);
        tokio::join!(run_shard(0, rx, &config, &handler), driver);
        assert!(
            *idle_ticks.borrow() >= 2,
            "an idle shard should have ticked, got {}",
            idle_ticks.borrow()
        );
    }
}
