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
//! record of what has been seen and how far it got. Three answers:
//!
//! - **[`Admission::Fresh`]** — never seen. Run it.
//! - **[`Admission::Retry`]** — seen, accepted, and never settled. The process
//!   died mid-run, so run it again. Losing this message is the worse outcome:
//!   the user asked and got no answer, and a duplicate answer at least tells
//!   them the agent is alive.
//! - **[`Admission::AlreadySettled`]** — seen, and the run reached an end. Drop
//!   it. This is the one that suppresses the replayed side effect.
//!
//! The ledger is written *before* the envelope reaches a shard and settled
//! *after* the run ends, which is what makes the crash window fall inside
//! `Retry` rather than between the two.
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
}

fn backend(err: rusqlite::Error) -> IngressError {
    IngressError::Backend(Arc::from(err.to_string()))
}

/// What the ledger says about an inbound envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// Never seen. Deliver it.
    Fresh,
    /// Accepted before and never settled, so the previous attempt did not
    /// finish. Deliver it again; `attempts` counts the deliveries including
    /// this one.
    Retry { attempts: u32 },
    /// Accepted before and settled. Suppress it.
    AlreadySettled,
    /// The channel supplied no transport id, so a redelivery is
    /// indistinguishable from a new message. Deliver it, and record nothing —
    /// see [`INGRESS_ID_KEY`](agentos_proto::INGRESS_ID_KEY).
    Unidentified,
}

impl Admission {
    /// Whether the envelope should reach a shard.
    pub fn deliver(self) -> bool {
        !matches!(self, Self::AlreadySettled)
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
    fn as_str(self) -> &'static str {
        match self {
            Self::Handled => "handled",
            Self::Refused => "refused",
        }
    }
}

pub(super) const CREATE_TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS ingress_events (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    sender TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    settlement TEXT,
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
"#;

pub(crate) fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(CREATE_TABLES)
}

/// The gateway's ingress ledger, backed by the same database as everything
/// else — deliberately, so "the message is recorded" and "the transcript is
/// written" cannot disagree across a crash.
pub struct IngressLedger {
    store: Arc<SqliteStore>,
}

fn unix_now() -> u64 {
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
        let Some(event_id) = envelope.ingress_id() else {
            return Ok(Admission::Unidentified);
        };
        self.admit_event(
            &envelope.channel_id,
            &event_id,
            &envelope.conversation_id,
            &envelope.sender,
        )
    }

    /// [`admit`](Self::admit) for a caller that has the parts but not an
    /// `Envelope` — a channel deduplicating before it builds one.
    pub fn admit_event(
        &self,
        channel_id: &ChannelId,
        event_id: &str,
        conversation_id: &ConversationId,
        sender: &str,
    ) -> Result<Admission, IngressError> {
        let now = unix_now();
        let conn = self
            .store
            .ingress_conn()
            .map_err(|err| IngressError::Backend(Arc::from(err.to_string())))?;
        // One statement again, so two shard sets racing on the same redelivery
        // cannot both read "absent" and both insert. `RETURNING` gives back the
        // row as it now stands, which is what distinguishes the three answers.
        let (attempts, settlement): (i64, Option<String>) = conn
            .query_row(
                r#"
                INSERT INTO ingress_events
                    (channel_id, event_id, conversation_id, sender, attempts, accepted_at)
                VALUES (?1, ?2, ?3, ?4, 1, ?5)
                ON CONFLICT(channel_id, event_id) DO UPDATE SET
                    attempts = ingress_events.attempts + 1
                RETURNING attempts, settlement
                "#,
                params![
                    channel_id.as_str(),
                    event_id,
                    conversation_id.as_str(),
                    sender,
                    now as i64
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(backend)?;

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
             WHERE channel_id = ?1 AND event_id = ?2
            "#,
            params![
                channel_id.as_str(),
                event_id,
                settlement.as_str(),
                unix_now() as i64
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
