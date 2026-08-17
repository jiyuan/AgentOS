use agentos_proto::{ChannelId, ConversationId, Envelope};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("channel backend failed: {0}")]
    Backend(Arc<str>),
}

/// The send half of a channel, detached from the receive half.
///
/// Cheap to clone (it is always held as an `Arc`) and `'static`, so a gateway
/// can hand one to every shard thread while the router keeps the channel itself
/// parked in [`Channel::receive`]. See [`Channel::egress`].
#[async_trait]
pub trait Egress: Send + Sync {
    /// Deliver `env` to the transport.
    async fn send(&self, env: Envelope) -> Result<(), ChannelError>;
}

#[async_trait]
pub trait Channel: Send + Sync {
    /// Return the stable channel identifier used in envelopes and traces.
    fn id(&self) -> ChannelId;

    /// Receive the next inbound envelope.
    ///
    /// Returning `None` means the channel is closed.
    async fn receive(&mut self) -> Option<Envelope>;

    /// Return this channel's shareable send half.
    ///
    /// [`Channel::receive`] takes `&mut self`, so whoever is receiving holds the
    /// channel exclusively and nothing else can call [`Channel::send`] on it.
    /// That is fine for a serial receive-run-send loop and fatal for a sharded
    /// one, where the runs happen on other threads while the receiver is parked
    /// in a long poll. An implementation therefore keeps the state `send` needs
    /// — credentials, per-conversation edit state — behind an `Arc` and hands
    /// out this handle, exactly as [`Channel::stream_egress`] already does for
    /// streaming deltas.
    ///
    /// The handle must share that state rather than copy it: a reply finalized
    /// through the egress has to see the placeholder a stream delta created.
    fn egress(&self) -> Arc<dyn Egress>;

    /// Send an outbound envelope.
    ///
    /// Delegates to [`Channel::egress`]; implement that instead of this.
    async fn send(&self, env: Envelope) -> Result<(), ChannelError> {
        self.egress().send(env).await
    }

    /// Return an edit-in-place streaming handle if this channel supports it.
    ///
    /// Default `None`: the run finishes with a single buffered [`Channel::send`].
    /// A channel that supports streaming returns a [`StreamEgress`] the gateway
    /// wires to the run's stream sink; the channel's own `send` then finalizes
    /// the streamed message on the terminal reply.
    fn stream_egress(&self) -> Option<Arc<dyn StreamEgress>> {
        None
    }
}

/// Optional edit-in-place streaming egress for a channel.
///
/// [`Channel::receive`] takes `&mut self`, so the receive-owning channel cannot
/// be captured by the run loop's `'static` stream sink. A streaming channel
/// instead hands out this cheap, shareable handle (typically an `Arc` over the
/// channel's HTTP credentials plus per-conversation edit state). The gateway
/// installs a `RunContext` stream sink that forwards each assistant text delta
/// to [`StreamEgress::push_delta`], which edits a single placeholder message in
/// place; the channel's [`Channel::send`] finalizes that message on the terminal
/// reply using the same shared state. Streaming is best-effort — `send` always
/// delivers the complete reply, so `push_delta` swallows transient failures.
#[async_trait]
pub trait StreamEgress: Send + Sync {
    /// Append `delta` to the in-flight reply for `conversation` and, subject to
    /// the implementation's own throttle, edit the placeholder message in place.
    async fn push_delta(&self, conversation: &ConversationId, delta: &str);
}
