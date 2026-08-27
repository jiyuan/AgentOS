use agentos_proto::{ChannelId, ConversationId, Envelope};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

/// One transport delivery and the receipt that acknowledges that exact event.
#[derive(Clone, Debug)]
pub struct InboundEvent {
    pub envelope: Envelope,
    pub receipt: IngressReceipt,
}

impl InboundEvent {
    pub fn new(envelope: Envelope, receipt: IngressReceipt) -> Self {
        Self { envelope, receipt }
    }

    pub fn without_receipt(envelope: Envelope) -> Self {
        Self::new(envelope, IngressReceipt::default())
    }
}

/// Opaque, event-bound state needed to acknowledge one inbound delivery.
///
/// `checkpoint` is the resumable position the gateway persists atomically with
/// the envelope. `token` is interpreted only by the channel that created it;
/// for example, Feishu stores the encoded success frame and Telegram stores a
/// callback-query id. Neither value is model-visible.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IngressReceipt {
    checkpoint: Option<Arc<str>>,
    token: Option<Arc<[u8]>>,
}

impl IngressReceipt {
    pub fn new(
        checkpoint: Option<Arc<str>>,
        token: Option<Arc<[u8]>>,
    ) -> Result<Self, ChannelError> {
        let bytes =
            checkpoint.as_deref().map_or(0, str::len) + token.as_deref().map_or(0, <[u8]>::len);
        if bytes > MAX_INGRESS_RECEIPT_BYTES {
            return Err(ChannelError::ReceiptTooLarge {
                bytes,
                max: MAX_INGRESS_RECEIPT_BYTES,
            });
        }
        Ok(Self { checkpoint, token })
    }

    pub fn checkpoint(&self) -> Option<&str> {
        self.checkpoint.as_deref()
    }

    pub fn token(&self) -> Option<&[u8]> {
        self.token.as_deref()
    }
}

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("channel backend failed: {0}")]
    Backend(Arc<str>),
    #[error("ingress receipt is {bytes} bytes; maximum is {max}")]
    ReceiptTooLarge { bytes: usize, max: usize },
}

/// Maximum combined checkpoint and opaque-token size for one inbound receipt.
pub const MAX_INGRESS_RECEIPT_BYTES: usize = 64 * 1024;

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

    /// Receive the next inbound envelope and its event-bound receipt.
    ///
    /// Returning `None` means the channel is closed.
    /// The implementation must not acknowledge the transport or advance a
    /// global cursor while parsing. The caller first persists the envelope and
    /// [`IngressReceipt::checkpoint`], then calls [`Channel::acknowledge`].
    async fn receive(&mut self) -> Option<InboundEvent>;

    /// Acknowledge the exact event represented by `receipt`.
    ///
    /// The default is a no-op for channels such as a local terminal. Durable
    /// gateways call this only after the envelope and checkpoint commit.
    async fn acknowledge(&mut self, _receipt: IngressReceipt) -> Result<(), ChannelError> {
        Ok(())
    }

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

    /// Resume from a cursor this channel previously reported.
    ///
    /// Called once before the receive loop starts, with whatever
    /// [`IngressReceipt::checkpoint`] last returned. The value is opaque to the gateway:
    /// it is stored and handed back verbatim, so a channel may encode whatever
    /// it needs. A channel that cannot make sense of it — a format from an
    /// older release — should ignore it rather than fail, and let its
    /// transport's own recovery take over.
    fn resume_from(&mut self, _cursor: &str) {}
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingress_receipts_have_a_named_byte_ceiling() {
        let at_limit = Arc::<[u8]>::from(vec![0; MAX_INGRESS_RECEIPT_BYTES]);
        assert!(IngressReceipt::new(None, Some(at_limit)).is_ok());
        let over_limit = Arc::<[u8]>::from(vec![0; MAX_INGRESS_RECEIPT_BYTES + 1]);
        assert!(matches!(
            IngressReceipt::new(None, Some(over_limit)),
            Err(ChannelError::ReceiptTooLarge { .. })
        ));
    }
}
