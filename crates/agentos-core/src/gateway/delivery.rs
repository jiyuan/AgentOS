//! Crash-conservative action and terminal-delivery state (`GW-004`).

use super::ingress::{unix_now, IngressError, IngressLedger, Settlement};
use agentos_proto::{ChannelId, Envelope};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Metadata carried to transports that can use a stable edit/idempotency key.
pub const DELIVERY_ID_KEY: &str = "agentos.delivery_id";

/// One exact durable ingress row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IngressEventKey {
    pub channel_id: ChannelId,
    pub event_id: Arc<str>,
}

impl IngressEventKey {
    pub fn from_envelope(envelope: &Envelope) -> Result<Self, IngressError> {
        let event_id = envelope.ingress_id().ok_or_else(|| {
            IngressError::InvalidTransition(Arc::from(
                "delivery state requires a transport ingress identity",
            ))
        })?;
        Ok(Self {
            channel_id: envelope.channel_id.clone(),
            event_id,
        })
    }
}

/// Durable semantic state of one accepted event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryState {
    Accepted,
    Running,
    Paused,
    ActionStarted,
    ActionCompleted,
    ReplyPending,
    DeliveryStarted,
    Handled,
    Rejected,
    ActionAmbiguous,
    DeliveryAmbiguous,
}

impl DeliveryState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::ActionStarted => "action_started",
            Self::ActionCompleted => "action_completed",
            Self::ReplyPending => "reply_pending",
            Self::DeliveryStarted => "delivery_started",
            Self::Handled => "handled",
            Self::Rejected => "rejected",
            Self::ActionAmbiguous => "action_ambiguous",
            Self::DeliveryAmbiguous => "delivery_ambiguous",
        }
    }

    fn parse(value: &str) -> Result<Self, IngressError> {
        match value {
            "accepted" => Ok(Self::Accepted),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "action_started" => Ok(Self::ActionStarted),
            "action_completed" => Ok(Self::ActionCompleted),
            "reply_pending" => Ok(Self::ReplyPending),
            "delivery_started" => Ok(Self::DeliveryStarted),
            "handled" => Ok(Self::Handled),
            "rejected" => Ok(Self::Rejected),
            "action_ambiguous" => Ok(Self::ActionAmbiguous),
            "delivery_ambiguous" => Ok(Self::DeliveryAmbiguous),
            other => Err(IngressError::InvalidTransition(Arc::from(format!(
                "unknown delivery state '{other}'"
            )))),
        }
    }
}

/// A terminal envelope and every accepted input it answers.
#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryBatch {
    pub delivery_id: Arc<str>,
    pub events: Vec<IngressEventKey>,
    pub envelope: Envelope,
    pub settlement: Settlement,
}

/// An event requiring operator reconciliation rather than automatic replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AmbiguousEvent {
    pub key: IngressEventKey,
    pub state: DeliveryState,
    pub action_id: Option<Arc<str>>,
    pub delivery_id: Option<Arc<str>>,
    pub reason: Arc<str>,
}

/// The action boundary installed on one top-level run.
#[derive(Clone)]
pub struct RunJournal {
    ledger: IngressLedger,
    events: Vec<IngressEventKey>,
}

pub(crate) struct ActionBatch {
    events: Vec<IngressEventKey>,
    action_id: Arc<str>,
}

impl RunJournal {
    pub fn new(ledger: IngressLedger, event: IngressEventKey) -> Self {
        Self {
            ledger,
            events: vec![event],
        }
    }

    /// Commit before invoking a tool. Failure aborts before the action starts.
    pub fn action_started(&self, action_id: &str) -> Result<(), IngressError> {
        self.ledger.transition_actions(
            &self.events,
            &[
                DeliveryState::Running,
                DeliveryState::Paused,
                DeliveryState::ActionCompleted,
            ],
            DeliveryState::ActionStarted,
            action_id,
            None,
        )
    }

    /// Commit after the tool returns. Until a terminal reply is durable, a
    /// restart still treats this boundary as ambiguous because run state is
    /// not yet restartable at the instruction after the tool.
    pub fn action_completed(&self, action_id: &str) -> Result<(), IngressError> {
        self.ledger.transition_actions(
            &self.events,
            &[DeliveryState::ActionStarted],
            DeliveryState::ActionCompleted,
            action_id,
            None,
        )
    }

    pub fn action_ambiguous(&self, action_id: &str, reason: &str) -> Result<(), IngressError> {
        self.ledger.transition_actions(
            &self.events,
            &[DeliveryState::ActionStarted],
            DeliveryState::ActionAmbiguous,
            action_id,
            Some(reason),
        )
    }

    pub(crate) fn start_action(
        &self,
        action_id: &str,
        claimed: &[Envelope],
    ) -> Result<ActionBatch, IngressError> {
        let mut events = self.events.clone();
        for envelope in claimed {
            let key = IngressEventKey::from_envelope(envelope)?;
            if !events.contains(&key) {
                events.push(key);
            }
        }
        self.ledger.transition_actions(
            &events,
            &[
                DeliveryState::Running,
                DeliveryState::Paused,
                DeliveryState::ActionCompleted,
            ],
            DeliveryState::ActionStarted,
            action_id,
            None,
        )?;
        Ok(ActionBatch {
            events,
            action_id: Arc::from(action_id),
        })
    }

    pub(crate) fn complete_action(&self, batch: &ActionBatch) -> Result<(), IngressError> {
        self.ledger.transition_actions(
            &batch.events,
            &[DeliveryState::ActionStarted],
            DeliveryState::ActionCompleted,
            &batch.action_id,
            None,
        )
    }

    pub(crate) fn ambiguous_action(
        &self,
        batch: &ActionBatch,
        reason: &str,
    ) -> Result<(), IngressError> {
        self.ledger.transition_actions(
            &batch.events,
            &[DeliveryState::ActionStarted],
            DeliveryState::ActionAmbiguous,
            &batch.action_id,
            Some(reason),
        )
    }
}

impl IngressLedger {
    pub fn run_journal(&self, envelope: &Envelope) -> Result<RunJournal, IngressError> {
        Ok(RunJournal::new(
            self.clone(),
            IngressEventKey::from_envelope(envelope)?,
        ))
    }

    /// Bind every exact input participating in one resumed or grouped run.
    pub fn run_journal_for(
        &self,
        events: Vec<IngressEventKey>,
    ) -> Result<RunJournal, IngressError> {
        if events.is_empty() {
            return Err(IngressError::InvalidTransition(Arc::from(
                "a run journal must bind at least one ingress event",
            )));
        }
        Ok(RunJournal {
            ledger: self.clone(),
            events,
        })
    }

    pub fn state(&self, key: &IngressEventKey) -> Result<DeliveryState, IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let state = conn
            .query_row(
                "SELECT delivery_state FROM ingress_events \
                 WHERE channel_id = ?1 AND event_id = ?2",
                params![key.channel_id.as_str(), key.event_id.as_ref()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(backend)?
            .ok_or_else(|| missing(key))?;
        DeliveryState::parse(&state)
    }

    pub fn prepare_reply(
        &self,
        events: &[IngressEventKey],
        mut envelope: Envelope,
        settlement: Settlement,
    ) -> Result<DeliveryBatch, IngressError> {
        let first = events.first().ok_or_else(|| {
            IngressError::InvalidTransition(Arc::from("a delivery must answer at least one event"))
        })?;
        let delivery_id: Arc<str> = Arc::from(format!(
            "{}:{}:reply",
            first.channel_id.as_str(),
            first.event_id
        ));
        envelope.metadata.insert(
            Arc::from(DELIVERY_ID_KEY),
            Value::from(delivery_id.as_ref()),
        );
        self.persist_envelope(
            events,
            &envelope,
            &delivery_id,
            DeliveryState::ReplyPending,
            Some(settlement),
        )?;
        Ok(DeliveryBatch {
            delivery_id,
            events: events.to_vec(),
            envelope,
            settlement,
        })
    }

    pub fn prepare_pause(
        &self,
        events: &[IngressEventKey],
        mut prompt: Envelope,
    ) -> Result<DeliveryBatch, IngressError> {
        let first = events.first().ok_or_else(|| {
            IngressError::InvalidTransition(Arc::from("a pause must belong to at least one event"))
        })?;
        let delivery_id: Arc<str> = Arc::from(format!(
            "{}:{}:approval",
            first.channel_id.as_str(),
            first.event_id
        ));
        prompt.metadata.insert(
            Arc::from(DELIVERY_ID_KEY),
            Value::from(delivery_id.as_ref()),
        );
        self.persist_envelope(events, &prompt, &delivery_id, DeliveryState::Paused, None)?;
        Ok(DeliveryBatch {
            delivery_id,
            events: events.to_vec(),
            envelope: prompt,
            settlement: Settlement::Handled,
        })
    }

    pub fn begin_delivery(&self, batch: &DeliveryBatch) -> Result<(), IngressError> {
        self.transition_batch(
            batch,
            &[DeliveryState::ReplyPending, DeliveryState::Paused],
            DeliveryState::DeliveryStarted,
            None,
        )
    }

    pub fn delivery_ambiguous(
        &self,
        batch: &DeliveryBatch,
        reason: &str,
    ) -> Result<(), IngressError> {
        self.transition_batch(
            batch,
            &[DeliveryState::DeliveryStarted],
            DeliveryState::DeliveryAmbiguous,
            Some(reason),
        )
    }

    pub fn pause_delivered(&self, batch: &DeliveryBatch) -> Result<(), IngressError> {
        self.transition_batch(
            batch,
            &[DeliveryState::DeliveryStarted],
            DeliveryState::Paused,
            None,
        )
    }

    pub fn delivery_succeeded(&self, batch: &DeliveryBatch) -> Result<(), IngressError> {
        self.delivery_succeeded_consuming(batch, None)
    }

    /// Settle terminal delivery and consume its approval lifecycle in one
    /// transaction, so neither half can be recovered without the other.
    pub fn delivery_succeeded_consuming(
        &self,
        batch: &DeliveryBatch,
        approval_instance: Option<&str>,
    ) -> Result<(), IngressError> {
        let settlement = batch.settlement;
        let mut conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let transaction = conn.transaction().map_err(backend)?;
        for event in &batch.events {
            let changed = transaction
                .execute(
                    "UPDATE ingress_events \
                     SET settlement = ?3, settled_at = ?4, dispatch_state = 'settled', \
                         delivery_state = ?5, state_updated_at = ?4 \
                     WHERE channel_id = ?1 AND event_id = ?2 \
                       AND settlement IS NULL AND delivery_id = ?6 \
                       AND delivery_state = 'delivery_started'",
                    params![
                        event.channel_id.as_str(),
                        event.event_id.as_ref(),
                        settlement.as_str(),
                        unix_now() as i64,
                        match settlement {
                            Settlement::Handled => DeliveryState::Handled.as_str(),
                            Settlement::Refused => DeliveryState::Rejected.as_str(),
                        },
                        batch.delivery_id.as_ref(),
                    ],
                )
                .map_err(backend)?;
            ensure_one(changed, event, "delivery_started -> settled")?;
        }
        if let Some(instance) = approval_instance {
            let changed = transaction
                .execute(
                    "DELETE FROM pending_approvals WHERE approval_instance_id = ?1 \
                     AND status = 'resolving'",
                    params![instance],
                )
                .map_err(backend)?;
            if changed != 1 {
                return Err(IngressError::InvalidTransition(Arc::from(
                    "terminal delivery did not find its active approval resolution",
                )));
            }
        }
        transaction.commit().map_err(backend)
    }

    /// Atomically claim a reply that was durable before a crash but whose
    /// transport delivery never started. Ambiguous deliveries are excluded.
    pub fn claim_pending_delivery(
        &self,
        channel_id: &ChannelId,
    ) -> Result<Option<DeliveryBatch>, IngressError> {
        let mut conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let transaction = conn.transaction().map_err(backend)?;
        let pending = transaction
            .query_row(
                "SELECT delivery_id, reply_json, pending_settlement FROM ingress_events \
                 WHERE channel_id = ?1 AND settlement IS NULL \
                   AND delivery_state = 'reply_pending' \
                 ORDER BY row_id ASC LIMIT 1",
                params![channel_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(backend)?;
        let Some((delivery_id, reply_json, pending_settlement)) = pending else {
            transaction.commit().map_err(backend)?;
            return Ok(None);
        };
        let envelope = serde_json::from_str(&reply_json)
            .map_err(|err| IngressError::Codec(Arc::from(err.to_string())))?;
        let mut statement = transaction
            .prepare(
                "SELECT channel_id, event_id FROM ingress_events \
                 WHERE delivery_id = ?1 AND settlement IS NULL \
                   AND delivery_state = 'reply_pending' ORDER BY row_id ASC",
            )
            .map_err(backend)?;
        let events = statement
            .query_map(params![delivery_id.as_str()], |row| {
                Ok(IngressEventKey {
                    channel_id: ChannelId::new(row.get::<_, String>(0)?),
                    event_id: Arc::from(row.get::<_, String>(1)?),
                })
            })
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?;
        drop(statement);
        let changed = transaction
            .execute(
                "UPDATE ingress_events \
                 SET delivery_state = 'delivery_started', dispatch_state = 'in_flight', \
                     state_updated_at = ?2 \
                 WHERE delivery_id = ?1 AND settlement IS NULL \
                   AND delivery_state = 'reply_pending'",
                params![delivery_id.as_str(), unix_now() as i64],
            )
            .map_err(backend)?;
        if changed != events.len() {
            return Err(IngressError::InvalidTransition(Arc::from(
                "pending delivery group changed while it was claimed",
            )));
        }
        transaction.commit().map_err(backend)?;
        Ok(Some(DeliveryBatch {
            delivery_id: Arc::from(delivery_id),
            events,
            envelope,
            settlement: Settlement::parse(&pending_settlement)?,
        }))
    }

    pub fn ambiguities(&self, channel_id: &ChannelId) -> Result<Vec<AmbiguousEvent>, IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let mut statement = conn
            .prepare(
                "SELECT event_id, delivery_state, action_id, delivery_id, state_reason \
                 FROM ingress_events WHERE channel_id = ?1 AND settlement IS NULL \
                   AND delivery_state IN ('action_ambiguous', 'delivery_ambiguous') \
                 ORDER BY row_id ASC",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map(params![channel_id.as_str()], |row| {
                let state = row.get::<_, String>(1)?;
                Ok((
                    row.get::<_, String>(0)?,
                    state,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .map_err(backend)?;
        rows.map(|row| {
            let (event_id, state, action_id, delivery_id, reason) = row.map_err(backend)?;
            Ok(AmbiguousEvent {
                key: IngressEventKey {
                    channel_id: channel_id.clone(),
                    event_id: Arc::from(event_id),
                },
                state: DeliveryState::parse(&state)?,
                action_id: action_id.map(Arc::from),
                delivery_id: delivery_id.map(Arc::from),
                reason: Arc::from(reason.unwrap_or_else(|| "reconciliation required".to_owned())),
            })
        })
        .collect()
    }

    fn transition_actions(
        &self,
        events: &[IngressEventKey],
        from: &[DeliveryState],
        to: DeliveryState,
        action_id: &str,
        reason: Option<&str>,
    ) -> Result<(), IngressError> {
        let mut conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let transaction = conn.transaction().map_err(backend)?;
        for event in events {
            let current = transaction
                .query_row(
                    "SELECT delivery_state FROM ingress_events \
                     WHERE channel_id = ?1 AND event_id = ?2 AND settlement IS NULL",
                    params![event.channel_id.as_str(), event.event_id.as_ref()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(backend)?
                .ok_or_else(|| missing(event))?;
            let current = DeliveryState::parse(&current)?;
            if !from.contains(&current) {
                return Err(IngressError::InvalidTransition(Arc::from(format!(
                    "{}:{} cannot move from {} to {}",
                    event.channel_id.as_str(),
                    event.event_id,
                    current.as_str(),
                    to.as_str()
                ))));
            }
            let changed = transaction
                .execute(
                    "UPDATE ingress_events SET delivery_state = ?3, action_id = ?4, \
                         state_reason = ?5, state_updated_at = ?6, \
                         dispatch_state = CASE WHEN ?3 LIKE '%_ambiguous' THEN 'quarantined' \
                                               ELSE dispatch_state END \
                     WHERE channel_id = ?1 AND event_id = ?2 AND settlement IS NULL \
                       AND delivery_state = ?7",
                    params![
                        event.channel_id.as_str(),
                        event.event_id.as_ref(),
                        to.as_str(),
                        action_id,
                        reason,
                        unix_now() as i64,
                        current.as_str(),
                    ],
                )
                .map_err(backend)?;
            ensure_one(changed, event, "action state transition")?;
        }
        transaction.commit().map_err(backend)
    }

    fn persist_envelope(
        &self,
        events: &[IngressEventKey],
        envelope: &Envelope,
        delivery_id: &str,
        state: DeliveryState,
        settlement: Option<Settlement>,
    ) -> Result<(), IngressError> {
        let reply_json = serde_json::to_string(envelope)
            .map_err(|err| IngressError::Codec(Arc::from(err.to_string())))?;
        let mut conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let transaction = conn.transaction().map_err(backend)?;
        for event in events {
            let changed = transaction
                .execute(
                    "UPDATE ingress_events SET reply_json = ?3, delivery_id = ?4, \
                         delivery_state = ?5, dispatch_state = ?6, state_updated_at = ?7, \
                         pending_settlement = ?8 \
                     WHERE channel_id = ?1 AND event_id = ?2 AND settlement IS NULL \
                       AND delivery_state IN ('running', 'paused', 'action_completed')",
                    params![
                        event.channel_id.as_str(),
                        event.event_id.as_ref(),
                        reply_json,
                        delivery_id,
                        state.as_str(),
                        if state == DeliveryState::Paused {
                            "paused"
                        } else {
                            "reply_pending"
                        },
                        unix_now() as i64,
                        settlement.map(Settlement::as_str),
                    ],
                )
                .map_err(backend)?;
            ensure_one(changed, event, "persist terminal envelope")?;
        }
        transaction.commit().map_err(backend)
    }

    fn transition_batch(
        &self,
        batch: &DeliveryBatch,
        from: &[DeliveryState],
        to: DeliveryState,
        reason: Option<&str>,
    ) -> Result<(), IngressError> {
        let mut conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let transaction = conn.transaction().map_err(backend)?;
        for event in &batch.events {
            let current = transaction
                .query_row(
                    "SELECT delivery_state FROM ingress_events \
                     WHERE channel_id = ?1 AND event_id = ?2 AND settlement IS NULL \
                       AND delivery_id = ?3",
                    params![
                        event.channel_id.as_str(),
                        event.event_id.as_ref(),
                        batch.delivery_id.as_ref()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(backend)?
                .ok_or_else(|| missing(event))?;
            let current = DeliveryState::parse(&current)?;
            if !from.contains(&current) {
                return Err(IngressError::InvalidTransition(Arc::from(format!(
                    "{}:{} cannot move from {} to {}",
                    event.channel_id.as_str(),
                    event.event_id,
                    current.as_str(),
                    to.as_str()
                ))));
            }
            transaction
                .execute(
                    "UPDATE ingress_events SET delivery_state = ?4, state_reason = ?5, \
                         state_updated_at = ?6, dispatch_state = ?7 \
                     WHERE channel_id = ?1 AND event_id = ?2 AND delivery_id = ?3",
                    params![
                        event.channel_id.as_str(),
                        event.event_id.as_ref(),
                        batch.delivery_id.as_ref(),
                        to.as_str(),
                        reason,
                        unix_now() as i64,
                        match to {
                            DeliveryState::Paused => "paused",
                            DeliveryState::DeliveryAmbiguous => "quarantined",
                            DeliveryState::DeliveryStarted => "in_flight",
                            _ => "in_flight",
                        },
                    ],
                )
                .map_err(backend)?;
        }
        transaction.commit().map_err(backend)
    }
}

fn backend(err: rusqlite::Error) -> IngressError {
    IngressError::Backend(Arc::from(err.to_string()))
}

fn missing(event: &IngressEventKey) -> IngressError {
    IngressError::InvalidTransition(Arc::from(format!(
        "missing ingress event {}:{}",
        event.channel_id.as_str(),
        event.event_id
    )))
}

fn ensure_one(
    changed: usize,
    event: &IngressEventKey,
    transition: &str,
) -> Result<(), IngressError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(IngressError::InvalidTransition(Arc::from(format!(
            "{transition} changed {changed} rows for {}:{}",
            event.channel_id.as_str(),
            event.event_id
        ))))
    }
}
