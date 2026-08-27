//! Durable terminal egress for one gateway turn (`GW-004`).

use agentos_core::gateway::{
    ApprovalStore, DeliveryBatch, IngressEventKey, IngressLedger, Settlement,
};
use agentos_core::r#loop::Steering;
use agentos_interfaces::{ChannelError, Egress};
use agentos_proto::Envelope;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// Persists a complete reply and its exact input set before touching the
/// transport, then settles only after transport success.
pub(super) struct DurableEgress<'a> {
    ledger: &'a IngressLedger,
    inner: &'a dyn Egress,
    primary: IngressEventKey,
    steering: Steering,
    predecessors: Mutex<Vec<IngressEventKey>>,
    approval_completion: Mutex<Option<Arc<str>>>,
}

impl<'a> DurableEgress<'a> {
    pub(super) fn new(
        ledger: &'a IngressLedger,
        inner: &'a dyn Egress,
        primary: IngressEventKey,
        steering: Steering,
    ) -> Self {
        Self {
            ledger,
            inner,
            primary,
            steering,
            predecessors: Mutex::new(Vec::new()),
            approval_completion: Mutex::new(None),
        }
    }

    pub(super) fn include(&self, predecessors: &[IngressEventKey]) {
        let mut events = self
            .predecessors
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for event in predecessors {
            if event != &self.primary && !events.contains(event) {
                events.push(event.clone());
            }
        }
    }

    pub(super) fn primary(&self) -> &IngressEventKey {
        &self.primary
    }

    /// Bind terminal settlement to consumption of this exact resolution.
    pub(super) fn consume_approval(&self, instance: Arc<str>) {
        *self
            .approval_completion
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(instance);
    }

    pub(super) fn journal_events(&self) -> Vec<IngressEventKey> {
        let mut events = vec![self.primary.clone()];
        events.extend(
            self.predecessors
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .cloned(),
        );
        events
    }

    /// Finalize the run's exact ingress group, including steering claimed
    /// during the run. May be called once, immediately before persistence.
    pub(super) fn take_events(&self) -> Result<Vec<IngressEventKey>, ChannelError> {
        let mut events = self.journal_events();
        for claimed in self.steering.take_claimed() {
            let event = IngressEventKey::from_envelope(&claimed).map_err(channel_backend)?;
            if !events.contains(&event) {
                events.push(event);
            }
        }
        Ok(events)
    }
}

#[async_trait]
impl Egress for DurableEgress<'_> {
    async fn send(&self, envelope: Envelope) -> Result<(), ChannelError> {
        let events = self.take_events()?;
        let paused = envelope
            .metadata
            .get("kind")
            .and_then(serde_json::Value::as_str)
            == Some(agentos_core::r#loop::PROMPT_KIND);
        let batch = if paused {
            self.ledger
                .prepare_pause(&events, envelope)
                .map_err(channel_backend)?
        } else {
            self.ledger
                .prepare_reply(&events, envelope, Settlement::Handled)
                .map_err(channel_backend)?
        };
        self.ledger
            .begin_delivery(&batch)
            .map_err(channel_backend)?;
        if let Err(error) = self.inner.send(batch.envelope.clone()).await {
            self.ledger
                .delivery_ambiguous(&batch, &error.to_string())
                .map_err(channel_backend)?;
            return Err(error);
        }
        if paused {
            if let Err(error) = self.ledger.pause_delivered(&batch) {
                let _ = self
                    .ledger
                    .delivery_ambiguous(&batch, "approval prompt accepted but pause commit failed");
                return Err(channel_backend(error));
            }
        } else if let Err(error) = self.ledger.delivery_succeeded_consuming(
            &batch,
            self.approval_completion
                .lock()
                .unwrap_or_else(|lock_error| lock_error.into_inner())
                .as_deref(),
        ) {
            let _ = self
                .ledger
                .delivery_ambiguous(&batch, "transport accepted reply but settlement failed");
            return Err(channel_backend(error));
        }
        Ok(())
    }
}

/// Deliver a prompt whose paused run was committed in the same transaction as
/// its ingress group. A crash after `begin` is intentionally ambiguous.
pub(super) async fn send_approval_prompt(
    store: &ApprovalStore,
    inner: &dyn Egress,
    instance: &str,
    batch: &DeliveryBatch,
) -> Result<(), ChannelError> {
    store
        .begin_prompt_delivery(instance, batch)
        .map_err(channel_backend)?;
    if let Err(error) = inner.send(batch.envelope.clone()).await {
        store
            .prompt_ambiguous(instance, batch, &error.to_string())
            .map_err(channel_backend)?;
        return Err(error);
    }
    if let Err(error) = store.prompt_delivered(instance, batch) {
        let _ = store.prompt_ambiguous(
            instance,
            batch,
            "approval prompt accepted but durable completion failed",
        );
        return Err(channel_backend(error));
    }
    Ok(())
}

fn channel_backend(error: impl std::fmt::Display) -> ChannelError {
    ChannelError::Backend(Arc::from(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_core::gateway::DeliveryState;
    use agentos_core::memory::SqliteStore;
    use agentos_proto::{ChannelId, ConversationId, Message, MessageRole, INGRESS_ID_KEY};
    use serde_json::Value;
    use std::collections::BTreeMap;

    struct OutcomeEgress {
        fails: bool,
        sent: Mutex<Vec<Envelope>>,
    }

    #[async_trait]
    impl Egress for OutcomeEgress {
        async fn send(&self, envelope: Envelope) -> Result<(), ChannelError> {
            self.sent
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(envelope);
            if self.fails {
                Err(ChannelError::Backend(Arc::from("injected egress failure")))
            } else {
                Ok(())
            }
        }
    }

    fn envelope(event_id: &str, role: MessageRole, text: &str) -> Envelope {
        let mut metadata = BTreeMap::new();
        metadata.insert(Arc::from(INGRESS_ID_KEY), Value::from(event_id));
        Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("conversation"),
            sender: Arc::from("sender"),
            message: Message::text(role, text),
            metadata,
        }
    }

    fn claimed(ledger: &IngressLedger, input: &Envelope) -> IngressEventKey {
        ledger.admit(input).expect("input persists");
        let event = IngressEventKey::from_envelope(input).expect("input has identity");
        ledger
            .mark_acknowledged(&event.channel_id, &event.event_id)
            .expect("input becomes dispatchable");
        ledger
            .claim_next(&event.channel_id)
            .expect("queue reads")
            .expect("input is claimed");
        event
    }

    #[tokio::test]
    async fn failed_egress_is_ambiguous_and_never_handled() {
        let ledger = IngressLedger::new(Arc::new(
            SqliteStore::open_in_memory().expect("store opens"),
        ));
        let input = envelope("failed-egress", MessageRole::User, "answer me");
        let event = claimed(&ledger, &input);
        let transport = OutcomeEgress {
            fails: true,
            sent: Mutex::new(Vec::new()),
        };
        let durable = DurableEgress::new(&ledger, &transport, event.clone(), Steering::default());

        durable
            .send(envelope("reply", MessageRole::Assistant, "answer"))
            .await
            .expect_err("uncertain transport result is surfaced");
        assert_eq!(
            ledger.state(&event).expect("state reads"),
            DeliveryState::DeliveryAmbiguous
        );
        assert_eq!(
            transport
                .sent
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn successful_egress_settles_only_after_the_transport_returns() {
        let ledger = IngressLedger::new(Arc::new(
            SqliteStore::open_in_memory().expect("store opens"),
        ));
        let input = envelope("successful-egress", MessageRole::User, "answer me");
        let event = claimed(&ledger, &input);
        let transport = OutcomeEgress {
            fails: false,
            sent: Mutex::new(Vec::new()),
        };
        let durable = DurableEgress::new(&ledger, &transport, event.clone(), Steering::default());

        durable
            .send(envelope("reply", MessageRole::Assistant, "answer"))
            .await
            .expect("transport and settlement succeed");
        assert_eq!(
            ledger.state(&event).expect("state reads"),
            DeliveryState::Handled
        );
        let sent = transport
            .sent
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert_eq!(sent.len(), 1);
        assert!(sent[0]
            .metadata
            .contains_key(agentos_core::gateway::DELIVERY_ID_KEY));
    }
}
