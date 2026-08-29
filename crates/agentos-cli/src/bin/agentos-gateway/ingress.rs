//! Durable transport admission and live dispatch (`GW-002`).

use super::{command_reply_envelope, log_line, ServiceConfig};
use agentos_cli::slash::{self, Parsed, SlashCommand};
use agentos_core::gateway::{
    self, Admission, DeliveryBatch, DeliveryOutcome, IngressEventKey, IngressLedger, Router,
    Settlement,
};
use agentos_interfaces::{Channel, Egress};
use agentos_proto::{ChannelId, Envelope};
use std::sync::Arc;
use std::time::Duration;

/// Receive until shutdown, admitting and acknowledging each exact event.
pub(super) async fn receive<C>(
    config: &ServiceConfig,
    channel: &mut C,
    channel_name: &str,
    ledger: &IngressLedger,
    dispatch_gate: &tokio::sync::Mutex<()>,
    shutdown: &gateway::ProcessShutdown,
) -> Result<(), String>
where
    C: Channel,
{
    loop {
        let inbound = match gateway::receive_or_cancel(channel, shutdown).await {
            gateway::ReceiveOutcome::Cancelled => {
                log_line(
                    config,
                    &format!("{channel_name} gateway loop draining on shutdown"),
                )?;
                return Ok(());
            }
            gateway::ReceiveOutcome::Received(inbound) => *inbound,
            gateway::ReceiveOutcome::Closed => {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                continue;
            }
        };
        let input = inbound.envelope;
        let admission = ledger
            .admit_with_checkpoint(&input, inbound.receipt.checkpoint())
            .map_err(|err| format!("{channel_name} ingress admission failed closed: {err}"))?;
        if matches!(admission, Admission::Unidentified | Admission::Quarantined) {
            return Err(format!(
                "{channel_name} refused ingress without a replayable durable identity"
            ));
        }
        let event_id = input
            .ingress_id()
            .ok_or_else(|| format!("{channel_name} admitted input lost its ingress identity"))?;
        // The gate keeps a claim from overtaking the exact receipt. There is
        // no fallible durable write after the transport ACK.
        let dispatch_guard = dispatch_gate.lock().await;
        ledger
            .mark_acknowledged(&input.channel_id, &event_id)
            .map_err(|err| format!("{channel_name} ACK state persist failed closed: {err}"))?;
        channel
            .acknowledge(inbound.receipt)
            .await
            .map_err(|err| format!("{channel_name} ingress acknowledgement failed: {err}"))?;
        drop(dispatch_guard);

        match admission {
            Admission::AlreadySettled => {
                log_line(
                    config,
                    &format!(
                        "{channel_name} acknowledged and suppressed a settled replay ({})",
                        input.conversation_id.as_str()
                    ),
                )?;
            }
            Admission::Retry { attempts } => log_line(
                config,
                &format!(
                    "{channel_name} re-running an event abandoned by an earlier attempt \
                     ({}, attempt {attempts})",
                    input.conversation_id.as_str()
                ),
            )?,
            Admission::Fresh => {}
            Admission::Unidentified | Admission::Quarantined => unreachable!(),
        }
    }
}

/// Continuously drain durable accepted rows while the receiver blocks.
pub(super) async fn dispatch(
    config: &ServiceConfig,
    channel_name: &str,
    router: &Router,
    egress: &dyn Egress,
    ledger: &IngressLedger,
    channel_id: &ChannelId,
    dispatch_gate: &tokio::sync::Mutex<()>,
) -> Result<(), String> {
    loop {
        drain(
            config,
            channel_name,
            router,
            egress,
            ledger,
            channel_id,
            dispatch_gate,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn drain(
    _config: &ServiceConfig,
    channel_name: &str,
    router: &Router,
    egress: &dyn Egress,
    ledger: &IngressLedger,
    channel_id: &ChannelId,
    dispatch_gate: &tokio::sync::Mutex<()>,
) -> Result<(), String> {
    loop {
        if let Some(batch) = ledger
            .claim_pending_delivery(channel_id)
            .map_err(|err| format!("{channel_name} pending delivery claim failed: {err}"))?
        {
            deliver_started(channel_name, egress, ledger, batch).await?;
            continue;
        }
        let event = {
            let dispatch_guard = dispatch_gate.lock().await;
            let event = ledger
                .claim_next(channel_id)
                .map_err(|err| format!("{channel_name} durable dispatch failed: {err}"))?;
            drop(dispatch_guard);
            event
        };
        let Some(event) = event else {
            return Ok(());
        };
        let event_id = Arc::clone(&event.event_id);
        let input = event.envelope;
        if matches!(
            slash::parse(&input.message.content),
            Parsed::Cmd(SlashCommand::Stop)
        ) {
            let stopped = router
                .stop(&input.conversation_id)
                .await
                .map_err(|err| format!("{channel_name} stop failed: {err}"))?;
            let reply = command_reply_envelope(&input, channel_name, &slash::format_stop(stopped));
            deliver_terminal(
                channel_name,
                egress,
                ledger,
                vec![IngressEventKey {
                    channel_id: channel_id.clone(),
                    event_id,
                }],
                reply,
                Settlement::Handled,
            )
            .await?;
            continue;
        }
        match router.deliver(input).await {
            Ok(DeliveryOutcome::Queued | DeliveryOutcome::Steered) => {}
            Ok(DeliveryOutcome::Displaced(displaced)) => {
                refuse_exact(
                    channel_name,
                    egress,
                    ledger,
                    displaced,
                    "Your earlier message was displaced by newer input before the agent could read it. Please send it again.",
                )
                .await?;
            }
            Ok(DeliveryOutcome::Refused(refused)) => {
                refuse_exact(
                    channel_name,
                    egress,
                    ledger,
                    refused,
                    "This conversation's inbox is full, so your message was not accepted. Please retry after the current work finishes.",
                )
                .await?;
            }
            Err(err) => {
                ledger.release(channel_id, &event_id).map_err(|release| {
                    format!(
                        "{channel_name} routing failed ({err}) and dispatch release failed: {release}"
                    )
                })?;
                return Err(format!("{channel_name} routing failed: {err}"));
            }
        }
    }
}

/// Tell the exact sender that its accepted event will not run, then settle
/// only that event. A failed transport call is ambiguous, never auto-retried.
async fn refuse_exact(
    channel_name: &str,
    egress: &dyn Egress,
    ledger: &IngressLedger,
    refused: Envelope,
    reason: &str,
) -> Result<(), String> {
    let event = IngressEventKey::from_envelope(&refused)
        .map_err(|err| format!("{channel_name} refused envelope lost its identity: {err}"))?;
    let reply = command_reply_envelope(&refused, channel_name, reason);
    deliver_terminal(
        channel_name,
        egress,
        ledger,
        vec![event],
        reply,
        Settlement::Refused,
    )
    .await
}

async fn deliver_terminal(
    channel_name: &str,
    egress: &dyn Egress,
    ledger: &IngressLedger,
    events: Vec<IngressEventKey>,
    reply: Envelope,
    settlement: Settlement,
) -> Result<(), String> {
    let batch = ledger
        .prepare_reply(&events, reply, settlement)
        .map_err(|err| format!("{channel_name} could not persist terminal reply: {err}"))?;
    ledger
        .begin_delivery(&batch)
        .map_err(|err| format!("{channel_name} could not start terminal delivery: {err}"))?;
    deliver_started(channel_name, egress, ledger, batch).await
}

async fn deliver_started(
    channel_name: &str,
    egress: &dyn Egress,
    ledger: &IngressLedger,
    batch: DeliveryBatch,
) -> Result<(), String> {
    if let Err(err) = egress.send(batch.envelope.clone()).await {
        ledger
            .delivery_ambiguous(&batch, &err.to_string())
            .map_err(|state| {
                format!(
                    "{channel_name} egress failed ({err}) and ambiguity persist failed: {state}"
                )
            })?;
        return Err(format!(
            "{channel_name} terminal delivery is ambiguous after egress failure: {err}"
        ));
    }
    if let Err(err) = ledger.delivery_succeeded(&batch) {
        let _ = ledger.delivery_ambiguous(
            &batch,
            "transport accepted reply but settlement commit failed",
        );
        return Err(format!(
            "{channel_name} transport accepted delivery {} but settlement failed: {err}",
            batch.delivery_id
        ));
    }
    Ok(())
}

/// Report accepted events that have not reached settlement.
pub(super) fn report_unsettled(
    config: &ServiceConfig,
    channel_name: &str,
    ledger: &IngressLedger,
    channel_id: &ChannelId,
) -> Result<(), String> {
    let abandoned = ledger
        .unsettled(channel_id)
        .map_err(|err| format!("failed to read the {channel_name} ingress ledger: {err}"))?;
    if abandoned.is_empty() {
        return Ok(());
    }
    log_line(
        config,
        &format!(
            "{channel_name} has {} accepted event(s) that never finished",
            abandoned.len()
        ),
    )?;
    for event in abandoned.iter().take(20) {
        log_line(
            config,
            &format!(
                "{channel_name} abandoned event {} in conversation {} from {} \
                 (attempt {}, accepted at {})",
                event.event_id,
                event.conversation_id.as_str(),
                event.sender,
                event.attempts,
                event.accepted_at
            ),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_core::memory::SqliteStore;
    use agentos_interfaces::{ChannelError, InboundEvent, IngressReceipt};
    use agentos_proto::{ConversationId, Message, MessageRole, INGRESS_ID_KEY};
    use async_trait::async_trait;
    use rusqlite::Connection;
    use serde_json::Value;
    use std::collections::{BTreeMap, VecDeque};
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_util::sync::CancellationToken;

    struct RecordingEgress {
        fail: bool,
        sent: Mutex<Vec<Envelope>>,
    }

    #[async_trait]
    impl Egress for RecordingEgress {
        async fn send(&self, envelope: Envelope) -> Result<(), ChannelError> {
            if self.fail {
                return Err(ChannelError::Backend(Arc::from("injected failure")));
            }
            self.sent
                .lock()
                .expect("recording egress lock is not poisoned")
                .push(envelope);
            Ok(())
        }
    }

    fn input(event_id: &str) -> Envelope {
        let mut metadata = BTreeMap::new();
        metadata.insert(Arc::from(INGRESS_ID_KEY), Value::from(event_id));
        Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("conversation"),
            sender: Arc::from("sender"),
            message: Message::text(MessageRole::User, "work"),
            metadata,
        }
    }

    fn claimed(input: &Envelope) -> IngressLedger {
        let ledger = IngressLedger::new(Arc::new(
            SqliteStore::open_in_memory().expect("store opens"),
        ));
        ledger.admit(input).expect("input is durable");
        let event_id = input.ingress_id().expect("fixture has ingress id");
        ledger
            .mark_acknowledged(&input.channel_id, &event_id)
            .expect("input is dispatchable");
        ledger
            .claim_next(&input.channel_id)
            .expect("queue reads")
            .expect("input is claimed");
        ledger
    }

    #[tokio::test]
    async fn displaced_event_settles_only_after_sender_facing_terminal_outcome() {
        let refused = input("event-1");
        let ledger = claimed(&refused);
        let egress = RecordingEgress {
            fail: false,
            sent: Mutex::new(Vec::new()),
        };

        refuse_exact("Telegram", &egress, &ledger, refused.clone(), "retry")
            .await
            .expect("terminal refusal is delivered and settled");

        assert_eq!(
            egress
                .sent
                .lock()
                .expect("recording egress lock is not poisoned")
                .len(),
            1
        );
        assert!(ledger
            .unsettled(&refused.channel_id)
            .expect("unsettled rows read")
            .is_empty());
    }

    #[tokio::test]
    async fn failed_terminal_outcome_is_ambiguous_and_not_retried() {
        let refused = input("event-2");
        let ledger = claimed(&refused);
        let egress = RecordingEgress {
            fail: true,
            sent: Mutex::new(Vec::new()),
        };

        assert!(
            refuse_exact("Telegram", &egress, &ledger, refused.clone(), "retry")
                .await
                .is_err()
        );
        let key = IngressEventKey::from_envelope(&refused).expect("fixture has ingress id");
        assert_eq!(
            ledger.state(&key).expect("delivery state reads"),
            agentos_core::gateway::DeliveryState::DeliveryAmbiguous
        );
        assert!(ledger
            .claim_next(&refused.channel_id)
            .expect("queue reads")
            .is_none());
    }

    /// What the durable ledger already held when the transport was acknowledged.
    #[derive(Debug)]
    struct AckObservation {
        durable: Vec<String>,
        cursor: Option<String>,
    }

    struct SilentEgress;

    #[async_trait]
    impl Egress for SilentEgress {
        async fn send(&self, _envelope: Envelope) -> Result<(), ChannelError> {
            Ok(())
        }
    }

    /// A transport that hands out fixed events and records, at each ACK, what
    /// the ledger had already committed.
    ///
    /// These two tests drive [`receive`] itself rather than sequencing an
    /// `IngressLedger` by hand. The `persist -> ACK -> route` order is a
    /// property of this loop, so a test that calls the ledger in the right
    /// order proves only that the test knows the order; reversing the two
    /// statements in `receive` has to be what fails.
    struct OrderProbe {
        pending: VecDeque<InboundEvent>,
        store: Arc<SqliteStore>,
        shutdown: gateway::ProcessShutdown,
        observed: Arc<Mutex<Vec<AckObservation>>>,
    }

    #[async_trait]
    impl Channel for OrderProbe {
        fn id(&self) -> ChannelId {
            ChannelId::new("telegram")
        }

        async fn receive(&mut self) -> Option<InboundEvent> {
            match self.pending.pop_front() {
                Some(event) => Some(event),
                // Nothing further to admit: end the loop the way a drain does.
                None => {
                    self.shutdown.begin();
                    None
                }
            }
        }

        async fn acknowledge(&mut self, _receipt: IngressReceipt) -> Result<(), ChannelError> {
            let ledger = IngressLedger::new(Arc::clone(&self.store));
            let channel = self.id();
            let durable = ledger
                .unsettled(&channel)
                .expect("the ledger is readable at acknowledgement")
                .into_iter()
                .map(|event| event.event_id.to_string())
                .collect();
            let cursor = ledger
                .cursor(&channel)
                .expect("the cursor is readable at acknowledgement")
                .map(|value| value.to_string());
            self.observed
                .lock()
                .expect("observation lock is not poisoned")
                .push(AckObservation { durable, cursor });
            Ok(())
        }

        fn egress(&self) -> Arc<dyn Egress> {
            Arc::new(SilentEgress)
        }
    }

    fn service_home(name: &str) -> (ServiceConfig, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after the Unix epoch")
            .as_nanos();
        let home = std::env::temp_dir().join(format!("agentos-{name}-{nonce}"));
        std::fs::create_dir_all(home.join("logs")).expect("the gateway home is creatable");
        (ServiceConfig::from_home(home.clone()), home)
    }

    fn probe(
        store: &Arc<SqliteStore>,
        observed: &Arc<Mutex<Vec<AckObservation>>>,
        inbound: &Envelope,
        checkpoint: &str,
    ) -> OrderProbe {
        let receipt =
            IngressReceipt::new(Some(Arc::from(checkpoint)), None).expect("the receipt fits");
        OrderProbe {
            pending: VecDeque::from(vec![InboundEvent::new(inbound.clone(), receipt)]),
            store: Arc::clone(store),
            shutdown: gateway::ProcessShutdown::new(
                CancellationToken::new(),
                Duration::from_secs(5),
            ),
            observed: Arc::clone(observed),
        }
    }

    /// AF-002: the shipped receive loop commits a replayable envelope and its
    /// event-bound checkpoint before it acknowledges the transport.
    #[tokio::test]
    async fn transport_ack_follows_durable_admission_on_the_receive_path() {
        let (config, home) = service_home("af002-receive-order");
        let store = Arc::new(SqliteStore::open_in_memory().expect("the store opens"));
        let ledger = IngressLedger::new(Arc::clone(&store));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let inbound = input("event-42");

        let mut channel = probe(&store, &observed, &inbound, "43");
        let shutdown = channel.shutdown.clone();
        let gate = tokio::sync::Mutex::new(());
        receive(&config, &mut channel, "Telegram", &ledger, &gate, &shutdown)
            .await
            .expect("the receive loop drains cleanly");

        {
            let observations = observed.lock().expect("observation lock is not poisoned");
            assert_eq!(observations.len(), 1, "the event is acknowledged once");
            assert_eq!(
                observations[0].durable,
                vec!["event-42".to_owned()],
                "the transport was acknowledged before its replay payload committed"
            );
            assert_eq!(
                observations[0].cursor.as_deref(),
                Some("43"),
                "the checkpoint must commit with the envelope, not after the ACK"
            );
        }

        // Routing is downstream of the acknowledgement: the dispatcher replays
        // the exact envelope, which it could not claim before the ACK state.
        let claimed = ledger
            .claim_next(&inbound.channel_id)
            .expect("queue reads")
            .expect("the acknowledged event is dispatchable");
        assert_eq!(claimed.envelope, inbound);

        std::fs::remove_dir_all(&home).ok();
    }

    /// AF-004: a ledger write failure on that same path acknowledges nothing,
    /// moves no cursor, and leaves nothing executable.
    #[tokio::test]
    async fn ledger_failure_on_the_receive_path_acknowledges_and_runs_nothing() {
        let (config, home) = service_home("af004-receive-failclosed");
        let database = home.join("ingress.sqlite");
        let store = Arc::new(SqliteStore::open(&database).expect("the store opens"));
        Connection::open(&database)
            .expect("a raw connection opens")
            .execute_batch(
                "CREATE TRIGGER inject_ingress_failure BEFORE INSERT ON ingress_events \
                 BEGIN SELECT RAISE(FAIL, 'injected ingress failure'); END;",
            )
            .expect("the failure trigger installs");
        let ledger = IngressLedger::new(Arc::clone(&store));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let inbound = input("event-7");

        let mut channel = probe(&store, &observed, &inbound, "8");
        let shutdown = channel.shutdown.clone();
        let gate = tokio::sync::Mutex::new(());
        let failure = receive(&config, &mut channel, "Telegram", &ledger, &gate, &shutdown)
            .await
            .expect_err("a failed admission must stop the receive loop");
        assert!(failure.contains("admission failed closed"), "{failure}");

        assert!(
            observed
                .lock()
                .expect("observation lock is not poisoned")
                .is_empty(),
            "the transport was acknowledged despite a failed durable admission"
        );
        assert!(
            ledger
                .claim_next(&inbound.channel_id)
                .expect("queue reads")
                .is_none(),
            "unadmitted input must never become executable"
        );
        assert_eq!(
            ledger.cursor(&inbound.channel_id).expect("cursor reads"),
            None
        );

        std::fs::remove_dir_all(&home).ok();
    }
}
