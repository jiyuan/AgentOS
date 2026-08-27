//! The durable record of what the gateway has accepted from a transport
//! (M8 / `GW-001`, deliverable 3).
//!
//! # What was wrong
//!
//! Telegram's update offset lived in process memory and advanced *before* the
//! run. Both halves of that are failures, in opposite directions:
//!
//! - The offset is only communicated to Telegram by the *next* `getUpdates`
//!   call carrying it, so a process that dies has acknowledged nothing and is
//!   re-sent everything Telegram still holds. Every one of those messages
//!   starts a second run: the agent answers the same question twice, and any
//!   tool it called it calls again.
//! - Within a live process the offset advances the moment the envelope is
//!   parsed. A crash after that and before the run finishes loses the message
//!   entirely — Telegram considers it delivered and the agent never answered.
//!
//! Feishu reads an `event_id` off every event and dedupes on none of them.
//!
//! # What a ledger is for
//!
//! At-least-once is what these transports offer, and it is not negotiable:
//! there is no acknowledgement a client can send that makes a redelivery
//! impossible. So the redelivery has to be *recognised*, which needs a durable
//! record of what has been seen and how far it got. Admission has three
//! answers:
//!
//! - **[`Admission::Fresh`]** — never seen. Run it.
//! - **[`Admission::Retry`]** — seen, accepted, and never settled. Recover its
//!   durable stage; this does not imply rerunning it.
//! - **[`Admission::AlreadySettled`]** — seen, and the run reached an end. Drop
//!   it. This is the one that suppresses the replayed side effect.
//!
//! The delivery state then says which recovery is safe. `accepted`/`running`
//! may return to routing; `reply_pending` may resume delivery; `paused` waits
//! for approval recovery; `action_started` and `delivery_started` become
//! explicit ambiguity after restart. Only a successful terminal transport
//! call moves a row to `handled` or `rejected`.
//!
//! # The cursor
//!
//! Separate from the events, because it answers a different question: not
//! "have I seen this" but "where should I resume asking". A channel that has a
//! resumable position — Telegram's offset — persists it here after the event
//! it belongs to is durably in the ledger, so a restart resumes from the last
//! *recorded* event rather than from the last one that happened to be parsed.

use crate::memory::SqliteStore;
use agentos_proto::{ChannelId, ConversationId, Envelope};
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngressError {
    #[error("ingress ledger failed: {0}")]
    Backend(Arc<str>),
    #[error("ingress envelope codec failed: {0}")]
    Codec(Arc<str>),
    #[error("invalid ingress state transition: {0}")]
    InvalidTransition(Arc<str>),
}

fn backend(err: rusqlite::Error) -> IngressError {
    IngressError::Backend(Arc::from(err.to_string()))
}

/// What the ledger says about an inbound envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// Never seen. Deliver it.
    Fresh,
    /// Accepted before and never settled. Recover the row's durable stage;
    /// `attempts` counts transport admissions, not action executions.
    Retry { attempts: u32 },
    /// Accepted before and settled. Suppress it.
    AlreadySettled,
    /// The channel supplied no transport id, so a redelivery is
    /// indistinguishable from a new message. Deliver it, and record nothing —
    /// see [`INGRESS_ID_KEY`](agentos_proto::INGRESS_ID_KEY).
    Unidentified,
    /// A pre-replay-schema row cannot be reconstructed safely.
    Quarantined,
}

impl Admission {
    /// Whether the envelope should reach a shard.
    pub fn deliver(self) -> bool {
        !matches!(self, Self::AlreadySettled | Self::Quarantined)
    }
}

/// How an accepted event ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Settlement {
    /// The run reached an end — answered, denied, cancelled, or failed. All
    /// four are settled: a failure the user was told about is not a message to
    /// replay on the next restart.
    Handled,
    /// The gateway refused it without running: a full inbox, a shutdown in
    /// progress. Settled too, because the sender was told.
    Refused,
}

impl Settlement {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Handled => "handled",
            Self::Refused => "refused",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, IngressError> {
        match value {
            "handled" => Ok(Self::Handled),
            "refused" => Ok(Self::Refused),
            other => Err(IngressError::InvalidTransition(Arc::from(format!(
                "unknown pending settlement '{other}'"
            )))),
        }
    }

    fn delivery_state(self) -> &'static str {
        match self {
            Self::Handled => "handled",
            Self::Refused => "rejected",
        }
    }
}

pub(super) const CREATE_TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS ingress_schema (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ingress_events (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    sender TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    settlement TEXT,
    envelope_json TEXT,
    checkpoint TEXT,
    dispatch_state TEXT NOT NULL DEFAULT 'awaiting_ack',
    delivery_state TEXT NOT NULL DEFAULT 'accepted',
    reply_json TEXT,
    delivery_id TEXT,
    pending_settlement TEXT,
    action_id TEXT,
    state_reason TEXT,
    state_updated_at INTEGER,
    quarantine_reason TEXT,
    accepted_at INTEGER NOT NULL,
    settled_at INTEGER,
    UNIQUE(channel_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_ingress_events_unsettled
    ON ingress_events(channel_id, settlement);

CREATE TABLE IF NOT EXISTS ingress_cursors (
    channel_id TEXT PRIMARY KEY,
    cursor TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Active approval pauses only. A row is removed after its one terminal
-- resolution has been delivered; the append-only interruption and safety
-- event remain the durable record of that outcome.
CREATE TABLE IF NOT EXISTS pending_approvals (
    approval_instance_id TEXT PRIMARY KEY,
    record_version INTEGER NOT NULL,
    channel_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    ticket TEXT NOT NULL,
    interruption_id TEXT NOT NULL,
    prompting_principal_json TEXT NOT NULL,
    paused_run_json TEXT NOT NULL,
    approval_scope_json TEXT NOT NULL,
    administrators_json TEXT NOT NULL,
    ingress_events_json TEXT NOT NULL,
    prompt_json TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    expires_at INTEGER,
    status TEXT NOT NULL,
    resolution_channel_id TEXT,
    resolution_event_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(channel_id, conversation_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_approvals_channel_status
    ON pending_approvals(channel_id, status);
"#;

pub(crate) fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(CREATE_TABLES)?;
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let migration = (|| {
        let mut statement = conn.prepare("PRAGMA table_info(ingress_events)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let had_delivery_state = columns.iter().any(|column| column == "delivery_state");
        for (name, declaration) in [
            ("envelope_json", "TEXT"),
            ("checkpoint", "TEXT"),
            ("dispatch_state", "TEXT NOT NULL DEFAULT 'awaiting_ack'"),
            ("delivery_state", "TEXT NOT NULL DEFAULT 'accepted'"),
            ("reply_json", "TEXT"),
            ("delivery_id", "TEXT"),
            ("pending_settlement", "TEXT"),
            ("action_id", "TEXT"),
            ("state_reason", "TEXT"),
            ("state_updated_at", "INTEGER"),
            ("quarantine_reason", "TEXT"),
        ] {
            if !columns.iter().any(|column| column == name) {
                conn.execute_batch(&format!(
                    "ALTER TABLE ingress_events ADD COLUMN {name} {declaration};"
                ))?;
            }
        }
        conn.execute_batch(
            r#"
            UPDATE ingress_events
               SET dispatch_state = 'settled'
             WHERE settlement IS NOT NULL;
            UPDATE ingress_events
               SET delivery_state = CASE settlement
                   WHEN 'handled' THEN 'handled'
                   WHEN 'refused' THEN 'rejected'
                   ELSE delivery_state
               END
             WHERE settlement IS NOT NULL;
            UPDATE ingress_events
               SET dispatch_state = 'quarantined',
                   quarantine_reason = 'legacy row has no replayable envelope'
             WHERE settlement IS NULL AND envelope_json IS NULL;
            CREATE INDEX IF NOT EXISTS idx_ingress_events_dispatch
                ON ingress_events(channel_id, dispatch_state, row_id);
            CREATE INDEX IF NOT EXISTS idx_ingress_events_delivery
                ON ingress_events(channel_id, delivery_state, row_id);
            INSERT INTO ingress_schema(singleton, version) VALUES (1, 4)
            ON CONFLICT(singleton) DO UPDATE SET version = excluded.version;
            "#,
        )?;
        if !had_delivery_state {
            conn.execute_batch(
                r#"
                UPDATE ingress_events
                   SET delivery_state = 'action_ambiguous',
                       dispatch_state = 'quarantined',
                       state_reason = 'pre-GW-004 in-flight row has unknown action boundary',
                       state_updated_at = strftime('%s','now')
                 WHERE settlement IS NULL AND dispatch_state = 'in_flight';
                "#,
            )?;
        }
        Ok(())
    })();
    match migration {
        Ok(()) => conn.execute_batch("COMMIT"),
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(err)
        }
    }
}

/// The gateway's ingress ledger, backed by the same database as everything
/// else — deliberately, so "the message is recorded" and "the transcript is
/// written" cannot disagree across a crash.
#[derive(Clone)]
pub struct IngressLedger {
    pub(super) store: Arc<SqliteStore>,
}

pub(super) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

impl IngressLedger {
    pub fn new(store: Arc<SqliteStore>) -> Self {
        Self { store }
    }

    /// Record `envelope` as accepted, and say whether it should run.
    ///
    /// Call this *before* the envelope reaches a shard. The insert is what
    /// makes the acceptance durable, so a channel may acknowledge to its
    /// transport as soon as this returns and not before.
    pub fn admit(&self, envelope: &Envelope) -> Result<Admission, IngressError> {
        self.admit_with_checkpoint(envelope, None)
    }

    /// Atomically persist a replayable envelope and its transport checkpoint.
    pub fn admit_with_checkpoint(
        &self,
        envelope: &Envelope,
        checkpoint: Option<&str>,
    ) -> Result<Admission, IngressError> {
        let Some(event_id) = envelope.ingress_id() else {
            return Ok(Admission::Unidentified);
        };
        let envelope_json = serde_json::to_string(envelope)
            .map_err(|err| IngressError::Codec(Arc::from(err.to_string())))?;
        let now = unix_now();
        let mut conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let transaction = conn.transaction().map_err(backend)?;
        let (attempts, settlement, dispatch_state): (i64, Option<String>, String) = transaction
            .query_row(
                r#"
                INSERT INTO ingress_events
                    (channel_id, event_id, conversation_id, sender, attempts,
                     envelope_json, checkpoint, dispatch_state, accepted_at)
                VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, 'awaiting_ack', ?7)
                ON CONFLICT(channel_id, event_id) DO UPDATE SET
                    attempts = ingress_events.attempts + 1
                RETURNING attempts, settlement, dispatch_state
                "#,
                params![
                    envelope.channel_id.as_str(),
                    event_id,
                    envelope.conversation_id.as_str(),
                    envelope.sender.as_ref(),
                    envelope_json,
                    checkpoint,
                    now as i64
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(backend)?;
        if let Some(checkpoint) = checkpoint {
            transaction
                .execute(
                    r#"
                    INSERT INTO ingress_cursors (channel_id, cursor, updated_at)
                    VALUES (?1, ?2, ?3)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        cursor = excluded.cursor,
                        updated_at = excluded.updated_at
                    "#,
                    params![envelope.channel_id.as_str(), checkpoint, now as i64],
                )
                .map_err(backend)?;
        }
        transaction.commit().map_err(backend)?;

        if dispatch_state == "quarantined" {
            return Ok(Admission::Quarantined);
        }
        if settlement.is_some() {
            return Ok(Admission::AlreadySettled);
        }
        if attempts <= 1 {
            return Ok(Admission::Fresh);
        }
        Ok(Admission::Retry {
            attempts: attempts.clamp(0, i64::from(u32::MAX)) as u32,
        })
    }

    /// Mark an accepted event finished. Call this *after* the run ends.
    ///
    /// Idempotent, and deliberately does not check the current state: a
    /// settlement written twice is the same settlement, and refusing the
    /// second one would mean a crash between the run ending and this call
    /// leaves the event permanently un-settleable.
    pub fn settle(
        &self,
        channel_id: &ChannelId,
        event_id: &str,
        settlement: Settlement,
    ) -> Result<(), IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        conn.execute(
            r#"
            UPDATE ingress_events
               SET settlement = ?3, settled_at = ?4
                 , dispatch_state = 'settled', delivery_state = ?5
                 , state_updated_at = ?4
             WHERE channel_id = ?1 AND event_id = ?2
            "#,
            params![
                channel_id.as_str(),
                event_id,
                settlement.as_str(),
                unix_now() as i64,
                settlement.delivery_state()
            ],
        )
        .map_err(backend)?;
        Ok(())
    }

    /// Events this channel accepted and never settled, oldest first.
    ///
    /// A restart reads this to know what it abandoned. Not replayed
    /// automatically: the transport will redeliver anything it never got an
    /// acknowledgement for, and replaying from here as well would be the
    /// duplicate the ledger exists to prevent. This is for saying so — the
    /// "reports abandoned work" half of the shutdown criterion.
    pub fn unsettled(&self, channel_id: &ChannelId) -> Result<Vec<AbandonedEvent>, IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let mut statement = conn
            .prepare(
                r#"
                SELECT event_id, conversation_id, sender, attempts, accepted_at
                  FROM ingress_events
                 WHERE channel_id = ?1 AND settlement IS NULL
                 ORDER BY row_id ASC
                "#,
            )
            .map_err(backend)?;
        let rows = statement
            .query_map(params![channel_id.as_str()], |row| {
                Ok(AbandonedEvent {
                    event_id: Arc::from(row.get::<_, String>(0)?.as_str()),
                    conversation_id: ConversationId::new(row.get::<_, String>(1)?.as_str()),
                    sender: Arc::from(row.get::<_, String>(2)?.as_str()),
                    attempts: row.get::<_, i64>(3)?.clamp(0, i64::from(u32::MAX)) as u32,
                    accepted_at: row.get::<_, i64>(4)? as u64,
                })
            })
            .map_err(backend)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(backend)
    }

    /// Reset work a dead process had claimed so the new dispatcher can replay it.
    pub fn recover_dispatches(&self, channel_id: &ChannelId) -> Result<usize, IngressError> {
        let mut conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let transaction = conn.transaction().map_err(backend)?;
        let now = unix_now() as i64;
        transaction
            .execute(
                "UPDATE ingress_events \
                 SET delivery_state = 'action_ambiguous', \
                     dispatch_state = 'quarantined', \
                     state_reason = 'process stopped after action start and before terminal reply', \
                     state_updated_at = ?2 \
                 WHERE channel_id = ?1 AND settlement IS NULL \
                   AND delivery_state IN ('action_started', 'action_completed')",
                params![channel_id.as_str(), now],
            )
            .map_err(backend)?;
        transaction
            .execute(
                "UPDATE ingress_events \
                 SET delivery_state = 'delivery_ambiguous', \
                     dispatch_state = 'quarantined', \
                     state_reason = 'process stopped after delivery started and before settlement', \
                     state_updated_at = ?2 \
                 WHERE channel_id = ?1 AND settlement IS NULL \
                   AND delivery_state = 'delivery_started'",
                params![channel_id.as_str(), now],
            )
            .map_err(backend)?;
        transaction
            .execute(
                "UPDATE ingress_events SET dispatch_state = 'reply_pending' \
                 WHERE channel_id = ?1 AND settlement IS NULL \
                   AND delivery_state = 'reply_pending'",
                params![channel_id.as_str()],
            )
            .map_err(backend)?;
        transaction
            .execute(
                "UPDATE ingress_events SET dispatch_state = 'paused' \
                 WHERE channel_id = ?1 AND settlement IS NULL \
                   AND delivery_state = 'paused'",
                params![channel_id.as_str()],
            )
            .map_err(backend)?;
        let recovered = transaction
            .execute(
                "UPDATE ingress_events \
                 SET dispatch_state = 'accepted', delivery_state = 'accepted', \
                     state_updated_at = ?2 \
                 WHERE channel_id = ?1 AND settlement IS NULL \
                   AND dispatch_state IN ('in_flight', 'awaiting_ack') \
                   AND delivery_state IN ('accepted', 'running')",
                params![channel_id.as_str(), now],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)?;
        Ok(recovered)
    }

    /// Make one durably persisted event visible to the dispatcher after ACK.
    pub fn mark_acknowledged(
        &self,
        channel_id: &ChannelId,
        event_id: &str,
    ) -> Result<(), IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        conn.execute(
            "UPDATE ingress_events SET dispatch_state = 'accepted' \
             WHERE channel_id = ?1 AND event_id = ?2 AND settlement IS NULL \
               AND dispatch_state = 'awaiting_ack'",
            params![channel_id.as_str(), event_id],
        )
        .map_err(backend)?;
        Ok(())
    }

    /// Claim the oldest durable event for dispatch.
    pub fn claim_next(
        &self,
        channel_id: &ChannelId,
    ) -> Result<Option<ReplayableEvent>, IngressError> {
        let mut conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let transaction = conn.transaction().map_err(backend)?;
        let row = transaction
            .query_row(
                "SELECT row_id, event_id, envelope_json FROM ingress_events \
                 WHERE channel_id = ?1 AND settlement IS NULL \
                   AND dispatch_state = 'accepted' ORDER BY row_id ASC LIMIT 1",
                params![channel_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(backend)?;
        let Some((row_id, event_id, envelope_json)) = row else {
            transaction.commit().map_err(backend)?;
            return Ok(None);
        };
        let envelope = serde_json::from_str(&envelope_json)
            .map_err(|err| IngressError::Codec(Arc::from(err.to_string())))?;
        transaction
            .execute(
                "UPDATE ingress_events \
                 SET dispatch_state = 'in_flight', delivery_state = 'running', \
                     state_updated_at = ?2 \
                 WHERE row_id = ?1 AND dispatch_state = 'accepted'",
                params![row_id, unix_now() as i64],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)?;
        Ok(Some(ReplayableEvent {
            event_id: Arc::from(event_id),
            envelope,
        }))
    }

    /// Return a failed dispatch claim to the durable queue.
    pub fn release(&self, channel_id: &ChannelId, event_id: &str) -> Result<(), IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        conn.execute(
            "UPDATE ingress_events \
             SET dispatch_state = 'accepted', delivery_state = 'accepted', \
                 state_updated_at = ?3 \
             WHERE channel_id = ?1 AND event_id = ?2 \
               AND settlement IS NULL AND dispatch_state = 'in_flight' \
               AND delivery_state IN ('accepted', 'running')",
            params![channel_id.as_str(), event_id, unix_now() as i64],
        )
        .map_err(backend)?;
        Ok(())
    }

    /// Legacy unsettled rows which cannot be reconstructed safely.
    pub fn quarantined(&self, channel_id: &ChannelId) -> Result<Vec<AbandonedEvent>, IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let mut statement = conn
            .prepare(
                "SELECT event_id, conversation_id, sender, attempts, accepted_at \
                 FROM ingress_events WHERE channel_id = ?1 \
                   AND dispatch_state = 'quarantined' \
                   AND quarantine_reason IS NOT NULL ORDER BY row_id ASC",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map(params![channel_id.as_str()], |row| {
                Ok(AbandonedEvent {
                    event_id: Arc::from(row.get::<_, String>(0)?),
                    conversation_id: ConversationId::new(row.get::<_, String>(1)?),
                    sender: Arc::from(row.get::<_, String>(2)?),
                    attempts: row.get::<_, i64>(3)?.clamp(0, i64::from(u32::MAX)) as u32,
                    accepted_at: row.get::<_, i64>(4)? as u64,
                })
            })
            .map_err(backend)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(backend)
    }

    /// Preserve and settle one quarantined or ambiguous row after operator
    /// reconciliation. Refusal is the conservative decision: never rerun an
    /// action or delivery whose external outcome is unknown.
    pub fn adjudicate_quarantined(
        &self,
        channel_id: &ChannelId,
        event_id: &str,
    ) -> Result<bool, IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let changed = conn
            .execute(
                "UPDATE ingress_events \
                 SET settlement = 'refused', settled_at = ?3, \
                     dispatch_state = 'settled', \
                     delivery_state = 'rejected', state_updated_at = ?3, \
                     quarantine_reason = 'operator adjudicated as refused' \
                 WHERE channel_id = ?1 AND event_id = ?2 \
                   AND (dispatch_state = 'quarantined' OR delivery_state IN \
                       ('action_ambiguous', 'delivery_ambiguous'))",
                params![channel_id.as_str(), event_id, unix_now() as i64],
            )
            .map_err(backend)?;
        Ok(changed == 1)
    }

    /// Where this channel should resume asking its transport, if it recorded
    /// a position.
    pub fn cursor(&self, channel_id: &ChannelId) -> Result<Option<Arc<str>>, IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        conn.query_row(
            "SELECT cursor FROM ingress_cursors WHERE channel_id = ?1",
            params![channel_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(backend)
        .map(|cursor| cursor.map(|cursor| Arc::from(cursor.as_str())))
    }

    /// Drop settled rows older than `max_age`, returning how many went.
    ///
    /// Across every channel, not one: the ledger is one table and a sweep that
    /// had to be told which channels exist would quietly stop pruning a
    /// channel the deployment turned off.
    ///
    /// **Settled rows only, at any age.** An unsettled row is the record that
    /// a message was accepted and never finished — the whole reason the ledger
    /// is durable — and deleting one would turn the crash it documents into a
    /// message that was never received. What bounds those is that there are
    /// only ever as many as there were in-flight turns when the process died.
    ///
    /// Safe against redelivery because settlement is what the transport is
    /// waiting on: a row is settled only once the sender heard back, and no
    /// transport this runtime speaks redelivers an event it was acknowledged
    /// for weeks earlier. `[retention].ingress_days` is the margin.
    pub fn prune_settled(&self, max_age: Duration) -> Result<usize, IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        let cutoff = unix_now().saturating_sub(max_age.as_secs());
        conn.execute(
            "DELETE FROM ingress_events \
             WHERE settlement IS NOT NULL AND settled_at IS NOT NULL AND settled_at < ?1",
            params![cutoff as i64],
        )
        .map_err(backend)
    }

    /// Record where the channel has read up to. Call this only after the
    /// event at that position is in the ledger.
    pub fn set_cursor(&self, channel_id: &ChannelId, cursor: &str) -> Result<(), IngressError> {
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        conn.execute(
            r#"
            INSERT INTO ingress_cursors (channel_id, cursor, updated_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(channel_id) DO UPDATE SET
                cursor = excluded.cursor,
                updated_at = excluded.updated_at
            "#,
            params![channel_id.as_str(), cursor, unix_now() as i64],
        )
        .map_err(backend)?;
        Ok(())
    }
}

/// An event the gateway accepted and never finished.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbandonedEvent {
    pub event_id: Arc<str>,
    pub conversation_id: ConversationId,
    pub sender: Arc<str>,
    pub attempts: u32,
    pub accepted_at: u64,
}

/// A durable envelope claimed by the live dispatcher.
#[derive(Clone, Debug)]
pub struct ReplayableEvent {
    pub event_id: Arc<str>,
    pub envelope: Envelope,
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{Message, MessageRole};
    use serde_json::Value;

    fn ledger() -> IngressLedger {
        IngressLedger::new(Arc::new(
            SqliteStore::open_in_memory().expect("the store opens"),
        ))
    }

    fn envelope(ingress_id: Option<Value>) -> Envelope {
        let mut metadata = std::collections::BTreeMap::new();
        if let Some(id) = ingress_id {
            metadata.insert(Arc::from(agentos_proto::INGRESS_ID_KEY), id);
        }
        Envelope {
            channel_id: ChannelId::new("telegram"),
            conversation_id: ConversationId::new("chat-1"),
            sender: Arc::from("user-9"),
            message: Message::text(MessageRole::User, "hello"),
            metadata,
        }
    }

    #[test]
    fn a_new_event_is_fresh_and_a_replay_of_it_is_a_retry() {
        let ledger = ledger();
        let envelope = envelope(Some(Value::from(42)));
        assert_eq!(
            ledger.admit(&envelope).expect("the query runs"),
            Admission::Fresh
        );
        assert_eq!(
            ledger.admit(&envelope).expect("the query runs"),
            Admission::Retry { attempts: 2 }
        );
    }

    /// The whole point: a replay of an event whose run already finished must
    /// not start a second one.
    #[test]
    fn a_replay_of_a_settled_event_is_suppressed() {
        let ledger = ledger();
        let envelope = envelope(Some(Value::from(42)));
        ledger.admit(&envelope).expect("the query runs");
        ledger
            .settle(&envelope.channel_id, "42", Settlement::Handled)
            .expect("the settlement writes");
        let replay = ledger.admit(&envelope).expect("the query runs");
        assert_eq!(replay, Admission::AlreadySettled);
        assert!(!replay.deliver());
    }

    /// A crash between acceptance and settlement is the case the ledger
    /// cannot afford to get wrong in the other direction: the user asked, and
    /// nobody answered.
    #[test]
    fn an_accepted_but_unsettled_event_is_delivered_again_and_reported() {
        let ledger = ledger();
        let envelope = envelope(Some(Value::from(7)));
        ledger.admit(&envelope).expect("the query runs");

        let abandoned = ledger
            .unsettled(&envelope.channel_id)
            .expect("the query runs");
        assert_eq!(abandoned.len(), 1);
        assert_eq!(abandoned[0].event_id.as_ref(), "7");
        assert_eq!(abandoned[0].sender.as_ref(), "user-9");

        assert!(ledger.admit(&envelope).expect("the query runs").deliver());
        ledger
            .settle(&envelope.channel_id, "7", Settlement::Handled)
            .expect("the settlement writes");
        assert!(ledger
            .unsettled(&envelope.channel_id)
            .expect("the query runs")
            .is_empty());
    }

    /// Two channels can use the same transport id space without colliding.
    #[test]
    fn the_ledger_is_keyed_by_channel_as_well_as_event() {
        let ledger = ledger();
        let mut feishu = envelope(Some(Value::from(42)));
        feishu.channel_id = ChannelId::new("feishu");
        assert_eq!(
            ledger
                .admit(&envelope(Some(Value::from(42))))
                .expect("runs"),
            Admission::Fresh
        );
        assert_eq!(ledger.admit(&feishu).expect("runs"), Admission::Fresh);
    }

    #[test]
    fn a_channel_with_no_transport_id_is_delivered_and_not_recorded() {
        let ledger = ledger();
        let admission = ledger.admit(&envelope(None)).expect("the query runs");
        assert_eq!(admission, Admission::Unidentified);
        assert!(admission.deliver());
        assert!(ledger
            .unsettled(&ChannelId::new("telegram"))
            .expect("the query runs")
            .is_empty());
    }

    #[test]
    fn a_cursor_round_trips_and_is_per_channel() {
        let ledger = ledger();
        let telegram = ChannelId::new("telegram");
        assert_eq!(ledger.cursor(&telegram).expect("the query runs"), None);
        ledger
            .set_cursor(&telegram, "1001")
            .expect("the cursor writes");
        ledger
            .set_cursor(&telegram, "1002")
            .expect("the cursor writes");
        assert_eq!(
            ledger.cursor(&telegram).expect("the query runs").as_deref(),
            Some("1002")
        );
        assert_eq!(
            ledger
                .cursor(&ChannelId::new("feishu"))
                .expect("the query runs"),
            None
        );
    }

    /// Settling twice is the same settlement, because a crash between the run
    /// ending and the settle call must not leave the event un-settleable.
    #[test]
    fn settling_twice_is_not_an_error() {
        let ledger = ledger();
        let envelope = envelope(Some(Value::from(3)));
        ledger.admit(&envelope).expect("the query runs");
        for _ in 0..2 {
            ledger
                .settle(&envelope.channel_id, "3", Settlement::Handled)
                .expect("the settlement writes");
        }
        assert_eq!(
            ledger.admit(&envelope).expect("the query runs"),
            Admission::AlreadySettled
        );
    }
}
