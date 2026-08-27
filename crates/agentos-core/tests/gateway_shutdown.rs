//! CTRL-002 process shutdown regressions.

use agentos_core::gateway::{self, ProcessShutdown, ReceiveOutcome};
use agentos_interfaces::{Channel, ChannelError, Egress};
use agentos_proto::{ChannelId, Envelope};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

struct NullEgress;

#[async_trait]
impl Egress for NullEgress {
    async fn send(&self, _envelope: Envelope) -> Result<(), ChannelError> {
        Ok(())
    }
}

struct ForeverBlockedChannel;

#[async_trait]
impl Channel for ForeverBlockedChannel {
    fn id(&self) -> ChannelId {
        ChannelId::new("blocked")
    }

    async fn receive(&mut self) -> Option<agentos_interfaces::InboundEvent> {
        std::future::pending().await
    }

    fn egress(&self) -> Arc<dyn Egress> {
        Arc::new(NullEgress)
    }
}

/// AF-026: even a transport future that never produces or closes is dropped
/// when the process cancellation root begins its one grace interval.
#[tokio::test]
async fn forever_blocked_channel_is_abandoned_at_shutdown_deadline() {
    let shutdown = ProcessShutdown::new(CancellationToken::new(), Duration::from_millis(75));
    let receiver_shutdown = shutdown.clone();
    let receiver = tokio::spawn(async move {
        let mut channel = ForeverBlockedChannel;
        gateway::receive_or_cancel(&mut channel, &receiver_shutdown).await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    let started = Instant::now();
    let deadline = shutdown.begin();
    assert_eq!(
        shutdown.begin(),
        deadline,
        "the grace deadline must latch once"
    );
    let outcome = tokio::time::timeout(Duration::from_millis(100), receiver)
        .await
        .expect("the blocked receive must leave within the grace bound")
        .expect("the receive task must not panic");
    assert!(matches!(outcome, ReceiveOutcome::Cancelled));
    assert!(started.elapsed() < Duration::from_millis(100));
}
