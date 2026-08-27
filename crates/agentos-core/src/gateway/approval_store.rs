//! Durable approval pauses and restart recovery (`STATE-002`).

use super::{
    unix_now, DeliveryBatch, DeliveryState, IngressError, IngressEventKey, IngressLedger,
    Settlement, DELIVERY_ID_KEY,
};
use crate::r#loop::{ApprovalBinding, ApprovalTicket};
use crate::runner::PausedRun;
use agentos_proto::{ActorPrincipal, ChannelId, Envelope};
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::Value;
use std::sync::Arc;

const RECORD_VERSION: i64 = 1;

#[derive(Clone, Copy)]
enum PromptTransition {
    DeliveryStarted,
    Delivered,
    Ambiguous,
}

/// One validated paused run restored from durable storage.
#[derive(Debug)]
pub struct DurableApproval {
    pub paused: PausedRun,
    pub ingress_events: Vec<IngressEventKey>,
    pub binding: ApprovalBinding,
    pub expires_at: Option<u64>,
}

/// Recovery work split by whether a prompt was definitely never attempted.
#[derive(Debug, Default)]
pub struct RecoveredApprovals {
    pub prompts: Vec<(Arc<str>, DeliveryBatch)>,
    pub pending: Vec<DurableApproval>,
}

/// Active approval state in the same transaction domain as ingress.
#[derive(Clone)]
pub struct ApprovalStore {
    ledger: IngressLedger,
}

impl ApprovalStore {
    pub fn new(ledger: IngressLedger) -> Self {
        Self { ledger }
    }

    /// Persist every fact needed to resume before prompt delivery starts.
    pub fn prepare_pause(
        &self,
        paused: &PausedRun,
        binding: &ApprovalBinding,
        events: &[IngressEventKey],
        mut prompt: Envelope,
        replaces_resolution: Option<&str>,
    ) -> Result<DeliveryBatch, IngressError> {
        let first = events
            .first()
            .ok_or_else(|| invalid("a pause has no ingress identity"))?;
        let pending = paused
            .state
            .pending_approval()
            .ok_or_else(|| invalid("paused run has no pending interruption"))?;
        if pending.approval_instance_id != binding.approval_instance_id
            || pending.id != binding.interruption_id
            || pending.approval_ticket.as_ref() != binding.ticket.as_str()
            || pending.prompting_principal != binding.prompting_principal
            || paused.channel_id != first.channel_id
            || events
                .iter()
                .any(|event| event.channel_id != paused.channel_id)
        {
            return Err(invalid("paused approval record is internally inconsistent"));
        }
        let delivery_id: Arc<str> = Arc::from(format!(
            "{}:{}:approval",
            first.channel_id.as_str(),
            first.event_id
        ));
        prompt.metadata.insert(
            Arc::from(DELIVERY_ID_KEY),
            Value::from(delivery_id.as_ref()),
        );
        let paused_json = codec(paused)?;
        let scope_json = codec(&pending.action)?;
        let principal_json = codec(&binding.prompting_principal)?;
        let administrators_json = codec(&binding.administrators)?;
        let events_json = codec(events)?;
        let prompt_json = codec(&prompt)?;
        let mut conn = self.ledger.store.ingress_conn().map_err(memory)?;
        let transaction = conn.transaction().map_err(backend)?;
        let now = unix_now() as i64;
        if let Some(previous) = replaces_resolution {
            let changed = transaction
                .execute(
                    "DELETE FROM pending_approvals WHERE approval_instance_id = ?1 \
                     AND status = 'resolving'",
                    params![previous],
                )
                .map_err(backend)?;
            if changed != 1 {
                return Err(invalid("previous approval resolution was not active"));
            }
        }
        transaction
            .execute(
                "INSERT INTO pending_approvals(approval_instance_id, record_version, channel_id, \
                 conversation_id, ticket, interruption_id, prompting_principal_json, \
                 paused_run_json, approval_scope_json, administrators_json, ingress_events_json, \
                 prompt_json, delivery_id, expires_at, status, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
                         'prompt_pending', ?15, ?15)",
                params![
                    binding.approval_instance_id.as_str(),
                    RECORD_VERSION,
                    paused.channel_id.as_str(),
                    paused.conversation_id.as_str(),
                    binding.ticket.as_str(),
                    binding.interruption_id.as_str(),
                    principal_json,
                    paused_json,
                    scope_json,
                    administrators_json,
                    events_json,
                    prompt_json,
                    delivery_id.as_ref(),
                    binding.expires_at.map(|value| value as i64),
                    now,
                ],
            )
            .map_err(backend)?;
        persist_prompt(&transaction, events, &prompt_json, &delivery_id, now)?;
        transaction.commit().map_err(backend)?;
        Ok(DeliveryBatch {
            delivery_id,
            events: events.to_vec(),
            envelope: prompt,
            settlement: Settlement::Handled,
        })
    }

    pub fn begin_prompt_delivery(
        &self,
        instance: &str,
        batch: &DeliveryBatch,
    ) -> Result<(), IngressError> {
        self.prompt_transition(instance, batch, PromptTransition::DeliveryStarted, None)
    }

    pub fn prompt_delivered(
        &self,
        instance: &str,
        batch: &DeliveryBatch,
    ) -> Result<(), IngressError> {
        self.prompt_transition(instance, batch, PromptTransition::Delivered, None)
    }

    pub fn prompt_ambiguous(
        &self,
        instance: &str,
        batch: &DeliveryBatch,
        reason: &str,
    ) -> Result<(), IngressError> {
        self.prompt_transition(instance, batch, PromptTransition::Ambiguous, Some(reason))
    }

    /// Reset resolution claims only after GW-004 has ruled out ambiguity, and
    /// reject malformed or identity-less records rather than guessing.
    pub fn recover(
        &self,
        channel_id: &ChannelId,
        current_administrators: &[ActorPrincipal],
    ) -> Result<RecoveredApprovals, IngressError> {
        let mut conn = self.ledger.store.ingress_conn().map_err(memory)?;
        let transaction = conn.transaction().map_err(backend)?;
        transaction
            .execute(
                "UPDATE pending_approvals SET status = 'pending', \
                 resolution_channel_id = NULL, resolution_event_id = NULL, updated_at = ?2 \
                 WHERE channel_id = ?1 AND status = 'resolving' \
                 AND NOT EXISTS (SELECT 1 FROM ingress_events AS input \
                     WHERE input.channel_id = resolution_channel_id \
                       AND input.event_id = resolution_event_id \
                       AND input.delivery_state IN ('action_ambiguous', 'delivery_ambiguous'))",
                params![channel_id.as_str(), unix_now() as i64],
            )
            .map_err(backend)?;
        let mut statement = transaction
            .prepare(
                "SELECT approval_instance_id, record_version, conversation_id, ticket, \
                 interruption_id, prompting_principal_json, paused_run_json, approval_scope_json, \
                 administrators_json, ingress_events_json, prompt_json, delivery_id, expires_at, \
                 status FROM pending_approvals WHERE channel_id = ?1 ORDER BY created_at ASC",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map(params![channel_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, Option<i64>>(12)?,
                    row.get::<_, String>(13)?,
                ))
            })
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?;
        drop(statement);
        transaction.commit().map_err(backend)?;
        // In-memory stores intentionally have one pooled connection. Release
        // it before per-record validation reacquires through the ledger.
        drop(conn);

        let mut recovered = RecoveredApprovals::default();
        for row in rows {
            let (
                instance,
                version,
                conversation,
                ticket_text,
                interruption,
                principal_json,
                paused_json,
                scope_json,
                administrators_json,
                events_json,
                prompt_json,
                delivery_id,
                expires_at,
                status,
            ) = row;
            if version != RECORD_VERSION {
                return Err(invalid("unsupported durable approval record version"));
            }
            let paused: PausedRun = decode(&paused_json, "paused run")?;
            let prompting: ActorPrincipal = decode(&principal_json, "prompting principal")?;
            let original_admins: Vec<ActorPrincipal> =
                decode(&administrators_json, "administrators")?;
            let events: Vec<IngressEventKey> = decode(&events_json, "ingress identities")?;
            let scope: Value = decode(&scope_json, "approval scope")?;
            let prompt: Envelope = decode(&prompt_json, "approval prompt")?;
            let ticket = ApprovalTicket::parse(&ticket_text)
                .ok_or_else(|| invalid("durable approval ticket is invalid"))?;
            let pending = paused
                .state
                .pending_approval()
                .ok_or_else(|| invalid("durable paused run has no pending interruption"))?;
            let pending_scope = serde_json::to_value(&pending.action)
                .map_err(|error| IngressError::Codec(Arc::from(error.to_string())))?;
            if instance != pending.approval_instance_id.as_str()
                || instance != ticket.as_str()
                || interruption != pending.id.as_str()
                || pending.approval_ticket.as_ref() != ticket.as_str()
                || pending.prompting_principal != prompting
                || paused.channel_id != *channel_id
                || paused.conversation_id.as_str() != conversation
                || pending_scope != scope
                || events.is_empty()
                || events.iter().any(|event| event.channel_id != *channel_id)
                || prompt.channel_id != *channel_id
                || prompt.conversation_id != paused.conversation_id
                || prompt.metadata.get(DELIVERY_ID_KEY).and_then(Value::as_str)
                    != Some(delivery_id.as_str())
            {
                return Err(invalid("durable approval identity or scope is corrupt"));
            }
            self.validate_ingress_events(&events, &conversation, &delivery_id)?;
            let administrators = original_admins
                .into_iter()
                .filter(|administrator| current_administrators.contains(administrator))
                .collect();
            let binding = ApprovalBinding::new(
                pending.approval_instance_id.clone(),
                ticket,
                pending.id.clone(),
                prompting,
                expires_at.map(|value| value.max(0) as u64),
            )
            .ok_or_else(|| invalid("durable approval instance does not match its ticket"))?
            .with_administrators(administrators);
            let durable = DurableApproval {
                paused,
                ingress_events: events.clone(),
                expires_at: expires_at.map(|value| value.max(0) as u64),
                binding,
            };
            match status.as_str() {
                "prompt_pending" => recovered.prompts.push((
                    Arc::from(instance),
                    DeliveryBatch {
                        delivery_id: Arc::from(delivery_id),
                        events,
                        envelope: prompt,
                        settlement: Settlement::Handled,
                    },
                )),
                "pending" => recovered.pending.push(durable),
                "delivery_started" | "delivery_ambiguous" => {
                    return Err(invalid("approval prompt delivery outcome is ambiguous"));
                }
                _ => return Err(invalid("durable approval has an unknown lifecycle state")),
            }
        }
        Ok(recovered)
    }

    /// Claim the one resolution event. A duplicate caller cannot resume.
    pub fn begin_resolution(
        &self,
        instance: &str,
        event: &IngressEventKey,
    ) -> Result<bool, IngressError> {
        let conn = self.ledger.store.ingress_conn().map_err(memory)?;
        let changed = conn
            .execute(
                "UPDATE pending_approvals SET status = 'resolving', \
                 resolution_channel_id = ?2, resolution_event_id = ?3, updated_at = ?4 \
                 WHERE approval_instance_id = ?1 AND status = 'pending' \
                 AND EXISTS (SELECT 1 FROM ingress_events AS input \
                     WHERE input.channel_id = ?2 AND input.event_id = ?3 \
                       AND input.conversation_id = pending_approvals.conversation_id \
                       AND input.settlement IS NULL AND input.delivery_state = 'running')",
                params![
                    instance,
                    event.channel_id.as_str(),
                    event.event_id.as_ref(),
                    unix_now() as i64
                ],
            )
            .map_err(backend)?;
        Ok(changed == 1)
    }

    /// Claim a system resolution such as expiry, which has no inbound event.
    pub fn begin_unanswered(&self, instance: &str) -> Result<bool, IngressError> {
        let conn = self.ledger.store.ingress_conn().map_err(memory)?;
        let changed = conn
            .execute(
                "UPDATE pending_approvals SET status = 'resolving', \
                 resolution_channel_id = NULL, resolution_event_id = NULL, updated_at = ?2 \
                 WHERE approval_instance_id = ?1 AND status = 'pending'",
                params![instance, unix_now() as i64],
            )
            .map_err(backend)?;
        Ok(changed == 1)
    }

    /// Return a failed system resolution to its durable pending state. Only a
    /// claim without an inbound resolver can use this path.
    pub fn abort_unanswered(&self, instance: &str) -> Result<(), IngressError> {
        let conn = self.ledger.store.ingress_conn().map_err(memory)?;
        let changed = conn
            .execute(
                "UPDATE pending_approvals SET status = 'pending', updated_at = ?2 \
                 WHERE approval_instance_id = ?1 AND status = 'resolving' \
                   AND resolution_channel_id IS NULL AND resolution_event_id IS NULL",
                params![instance, unix_now() as i64],
            )
            .map_err(backend)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(invalid("unanswered approval claim could not be restored"))
        }
    }

    /// Finish an unanswered prompt without a transport reply. The resolution
    /// audit/trace is already durable; settle its original ingress identities
    /// in the same transaction that removes active lifecycle state.
    pub fn consume_unanswered(
        &self,
        instance: &str,
        events: &[IngressEventKey],
    ) -> Result<(), IngressError> {
        if events.is_empty() {
            return Err(invalid("unanswered approval has no ingress identities"));
        }
        let mut conn = self.ledger.store.ingress_conn().map_err(memory)?;
        let transaction = conn.transaction().map_err(backend)?;
        let now = unix_now() as i64;
        for event in events {
            let changed = transaction
                .execute(
                    "UPDATE ingress_events SET settlement = 'handled', settled_at = ?3, \
                     dispatch_state = 'settled', delivery_state = 'handled', state_updated_at = ?3 \
                     WHERE channel_id = ?1 AND event_id = ?2 AND settlement IS NULL \
                     AND delivery_state = 'paused'",
                    params![event.channel_id.as_str(), event.event_id.as_ref(), now],
                )
                .map_err(backend)?;
            if changed != 1 {
                return Err(invalid(
                    "unanswered approval ingress state changed concurrently",
                ));
            }
        }
        let changed = transaction
            .execute(
                "DELETE FROM pending_approvals WHERE approval_instance_id = ?1 \
                 AND status = 'resolving'",
                params![instance],
            )
            .map_err(backend)?;
        if changed != 1 {
            return Err(invalid("unanswered approval resolution was not active"));
        }
        transaction.commit().map_err(backend)
    }

    fn prompt_transition(
        &self,
        instance: &str,
        batch: &DeliveryBatch,
        transition: PromptTransition,
        reason: Option<&str>,
    ) -> Result<(), IngressError> {
        let (from_status, to_status, from_delivery, to_delivery) = match transition {
            PromptTransition::DeliveryStarted => (
                "prompt_pending",
                "delivery_started",
                DeliveryState::Paused,
                DeliveryState::DeliveryStarted,
            ),
            PromptTransition::Delivered => (
                "delivery_started",
                "pending",
                DeliveryState::DeliveryStarted,
                DeliveryState::Paused,
            ),
            PromptTransition::Ambiguous => (
                "delivery_started",
                "delivery_ambiguous",
                DeliveryState::DeliveryStarted,
                DeliveryState::DeliveryAmbiguous,
            ),
        };
        let mut conn = self.ledger.store.ingress_conn().map_err(memory)?;
        let transaction = conn.transaction().map_err(backend)?;
        let now = unix_now() as i64;
        let changed = transaction
            .execute(
                "UPDATE pending_approvals SET status = ?3, updated_at = ?4 \
                 WHERE approval_instance_id = ?1 AND status = ?2 AND delivery_id = ?5",
                params![
                    instance,
                    from_status,
                    to_status,
                    now,
                    batch.delivery_id.as_ref()
                ],
            )
            .map_err(backend)?;
        if changed != 1 {
            return Err(invalid("approval prompt lifecycle changed concurrently"));
        }
        for event in &batch.events {
            let changed = transaction
                .execute(
                    "UPDATE ingress_events SET delivery_state = ?4, dispatch_state = ?5, \
                     state_reason = ?6, state_updated_at = ?7 WHERE channel_id = ?1 \
                     AND event_id = ?2 AND delivery_id = ?3 AND settlement IS NULL \
                     AND delivery_state = ?8",
                    params![
                        event.channel_id.as_str(),
                        event.event_id.as_ref(),
                        batch.delivery_id.as_ref(),
                        to_delivery.as_str(),
                        if to_delivery == DeliveryState::Paused {
                            "paused"
                        } else if to_delivery == DeliveryState::DeliveryAmbiguous {
                            "quarantined"
                        } else {
                            "in_flight"
                        },
                        reason,
                        now,
                        from_delivery.as_str()
                    ],
                )
                .map_err(backend)?;
            if changed != 1 {
                return Err(invalid(
                    "approval prompt ingress state changed concurrently",
                ));
            }
        }
        transaction.commit().map_err(backend)
    }

    fn validate_ingress_events(
        &self,
        events: &[IngressEventKey],
        conversation: &str,
        delivery_id: &str,
    ) -> Result<(), IngressError> {
        let conn = self.ledger.store.ingress_conn().map_err(memory)?;
        for event in events {
            let row = conn
                .query_row(
                    "SELECT conversation_id, delivery_state, delivery_id, settlement \
                     FROM ingress_events WHERE channel_id = ?1 AND event_id = ?2",
                    params![event.channel_id.as_str(), event.event_id.as_ref()],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .optional()
                .map_err(backend)?
                .ok_or_else(|| invalid("durable approval names a missing ingress event"))?;
            if row.0 != conversation
                || row.1 != DeliveryState::Paused.as_str()
                || row.2.as_deref() != Some(delivery_id)
                || row.3.is_some()
            {
                return Err(invalid(
                    "durable approval ingress identity is not an unsettled paused event",
                ));
            }
        }
        Ok(())
    }
}

fn persist_prompt(
    transaction: &Transaction<'_>,
    events: &[IngressEventKey],
    prompt_json: &str,
    delivery_id: &str,
    now: i64,
) -> Result<(), IngressError> {
    for event in events {
        let changed = transaction
            .execute(
                "UPDATE ingress_events SET reply_json = ?3, delivery_id = ?4, \
                 delivery_state = 'paused', dispatch_state = 'paused', state_updated_at = ?5, \
                 pending_settlement = NULL WHERE channel_id = ?1 AND event_id = ?2 \
                 AND settlement IS NULL \
                 AND delivery_state IN ('running', 'paused', 'action_completed')",
                params![
                    event.channel_id.as_str(),
                    event.event_id.as_ref(),
                    prompt_json,
                    delivery_id,
                    now
                ],
            )
            .map_err(backend)?;
        if changed != 1 {
            return Err(invalid("could not bind approval prompt to ingress event"));
        }
    }
    Ok(())
}

fn codec(value: &(impl serde::Serialize + ?Sized)) -> Result<String, IngressError> {
    serde_json::to_string(value).map_err(|error| IngressError::Codec(Arc::from(error.to_string())))
}

fn decode<T: serde::de::DeserializeOwned>(json: &str, label: &str) -> Result<T, IngressError> {
    serde_json::from_str(json).map_err(|error| {
        IngressError::Codec(Arc::from(format!("invalid durable {label}: {error}")))
    })
}

fn invalid(reason: &str) -> IngressError {
    IngressError::InvalidTransition(Arc::from(reason))
}

fn backend(error: rusqlite::Error) -> IngressError {
    IngressError::Backend(Arc::from(error.to_string()))
}

fn memory(error: impl std::fmt::Display) -> IngressError {
    IngressError::Backend(Arc::from(error.to_string()))
}
