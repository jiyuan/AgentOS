//! The session log, and the epoch that makes `/clear` a projection over it.
//!
//! Split out of `memory/sqlite.rs` to keep it under the module ceiling, and it
//! is not memory anyway: `session_items` is the conversation transcript, and
//! `memory_records` is what the agent chose to remember about it.
//!
//! Everything that reads `session_items` lives here, which is deliberate. The
//! epoch has to constrain every one of those readers — `fork` included, which
//! would otherwise copy hidden history into a child — and a filter applied at
//! the call sites is a filter somebody eventually forgets
//! ([ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md)).

use super::sqlite::{session_json_error, session_sqlite_error, SqliteStore};
use crate::audit::{SafetyEvent, SafetyEventKind, SafetyOutcome};
use agentos_interfaces::session::{Item, Session, SessionError, Transcript};
use agentos_proto::Principal;
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::Arc;

impl SqliteStore {
    /// Start a new epoch for `conv_id`, returning how many items it hides.
    ///
    /// `/clear` used to run `DELETE FROM session_items`, which made it the one
    /// operation that destroyed the record irreversibly — and the one a user
    /// reaches for most casually. It now writes a marker and removes nothing:
    /// [`Session::load`] returns only items at or after the newest epoch, so
    /// the model sees a fresh conversation while the log still holds what
    /// happened. Same mechanism as a compaction checkpoint, applied to the
    /// whole history rather than to a span (M6 / `STATE-001`,
    /// [ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md)).
    ///
    /// The epoch table is itself append-only: a second `/clear` adds a row
    /// rather than moving one, so the sequence of clears is as readable as the
    /// items between them. Irreversible removal is [`Self::purge_session`].
    pub fn clear_session(&self, principal: &Principal) -> Result<usize, SessionError> {
        let conversation = principal.conversation_name();
        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let epoch = current_epoch(&tx, principal)?;
        let next: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(ordinal) + 1, 0) FROM session_items \
                 WHERE conversation_key = ?1",
                params![conversation],
                |row| row.get(0),
            )
            .map_err(session_sqlite_error)?;
        // What the user is clearing is what they can see, which is what a
        // second `/clear` on an already-cleared conversation reports as zero.
        let hidden = usize::try_from(next.saturating_sub(epoch)).unwrap_or(0);
        tx.execute(
            "INSERT INTO session_epochs (conversation_key, principal, epoch_ordinal) \
             VALUES (?1, ?2, ?3)",
            params![conversation, principal.storage_name(), next],
        )
        .map_err(session_sqlite_error)?;
        tx.commit().map_err(session_sqlite_error)?;
        Ok(hidden)
    }

    /// Every item ever written for `conv_id`, epochs ignored.
    ///
    /// [`Session::load`] returns the *projection* — what the model sees. This
    /// returns the log underneath it, which is what makes "cleared" and
    /// "deleted" distinguishable to anything other than the storage layer:
    /// an operator exporting a conversation, and the tests that check `/clear`
    /// hides rather than removes.
    pub fn session_log(&self, principal: &Principal) -> Result<Vec<Item>, SessionError> {
        let conn = self.session_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT item_json FROM session_items \
                 WHERE conversation_key = ?1 ORDER BY ordinal ASC",
            )
            .map_err(session_sqlite_error)?;
        let rows = stmt
            .query_map(params![principal.conversation_name()], |row| {
                row.get::<_, String>(0)
            })
            .map_err(session_sqlite_error)?;
        let mut items = Vec::new();
        for row in rows {
            let item_json = row.map_err(session_sqlite_error)?;
            items.push(serde_json::from_str(&item_json).map_err(session_json_error)?);
        }
        Ok(items)
    }

    /// Conversations whose newest session item is older than `before_unix`,
    /// with how many items each holds, oldest first.
    ///
    /// A *survey*, not a sweep. Nothing here deletes: the session log is
    /// append-only without qualification and the one path that removes from it
    /// is [`Self::purge_session`], reached by an operator naming what they are
    /// destroying ([ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md)). This
    /// exists so `agentos-gateway purge --idle-days` can show them the list
    /// first, which is the only way an answer to "is this safe to delete" can
    /// be a considered one (M7 / `QUOTA-001`).
    ///
    /// Newest item rather than oldest: a conversation that started a year ago
    /// and was used this morning is not idle.
    /// `before` is Unix seconds. The cutoff is rendered by SQLite's own
    /// `datetime(?, 'unixepoch')` rather than formatted here, because
    /// `created_at` is written by `CURRENT_TIMESTAMP` and the two have to be
    /// the same shape for `<` to mean what it looks like — an RFC 3339 literal
    /// compares wrong against `YYYY-MM-DD HH:MM:SS` on the `T` alone.
    pub fn idle_conversations(
        &self,
        before_unix: u64,
    ) -> Result<Vec<(Principal, usize)>, SessionError> {
        let conn = self.session_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT conversation_key, COUNT(*), MAX(created_at) AS newest \
                   FROM session_items \
                  GROUP BY conversation_key \
                 HAVING newest < datetime(?1, 'unixepoch') \
                  ORDER BY newest ASC",
            )
            .map_err(session_sqlite_error)?;
        let rows = stmt
            .query_map(params![before_unix as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?.max(0) as usize,
                ))
            })
            .map_err(session_sqlite_error)?;
        let mut idle = Vec::new();
        for row in rows {
            let (key, count) = row.map_err(session_sqlite_error)?;
            // A key that does not decode is a row this build did not write —
            // a legacy conversation, or one from a newer encoding. Reported
            // rather than skipped, because a purge survey that quietly omits
            // rows understates what is on disk.
            let principal = Principal::from_storage_name(&key).ok_or_else(|| {
                SessionError::Backend(Arc::from(format!(
                    "session key '{key}' is not a principal name; run `agentos-gateway migrate`"
                )))
            })?;
            idle.push((principal, count));
        }
        Ok(idle)
    }

    /// Irreversibly remove a conversation's items and its epoch markers.
    ///
    /// The legitimate requirement `/clear` does not serve: somebody asking to
    /// be forgotten is not asking for a projection. Deliberately a separate
    /// method with a separate name, so nothing reaches it by the casual path.
    /// The operator-confirmed count is checked and the safety marker is written
    /// in the deletion transaction
    /// ([ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md)).
    pub fn purge_session(
        &self,
        principal: &Principal,
        expected: usize,
        by: &str,
    ) -> Result<usize, SessionError> {
        let conversation = principal.conversation_name();
        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let actual = tx
            .query_row(
                "SELECT COUNT(*) FROM session_items WHERE conversation_key = ?1",
                params![conversation],
                |row| row.get::<_, i64>(0),
            )
            .map_err(session_sqlite_error)?
            .max(0) as usize;
        if actual != expected {
            return Err(SessionError::Backend(Arc::from(format!(
                "session purge confirmation is stale: expected {expected} item(s), found {actual}"
            ))));
        }
        let removed = tx
            .execute(
                "DELETE FROM session_items WHERE conversation_key = ?1",
                params![conversation],
            )
            .map_err(session_sqlite_error)?;
        // Every participant's epoch, not just this principal's: the
        // conversation is being destroyed, and a marker left behind would
        // hide the first items of whatever is written under the same key
        // next.
        tx.execute(
            "DELETE FROM session_epochs WHERE conversation_key = ?1",
            params![conversation],
        )
        .map_err(session_sqlite_error)?;
        crate::audit::insert_event(
            &tx,
            &SafetyEvent::new(
                SafetyEventKind::SessionPurged,
                SafetyOutcome::Purged,
                principal.conversation_name(),
            )
            .with_principal(principal.clone().without_sender())
            .with_detail(format!("{removed} session items deleted by {by}")),
        )
        .map_err(|err| SessionError::Backend(Arc::from(err.to_string())))?;
        tx.commit().map_err(session_sqlite_error)?;
        Ok(removed)
    }
}

/// The ordinal the visible history starts at: one past the last item that
/// existed when the conversation was most recently cleared, or 0.
///
/// Every reader of `session_items` has to respect this. It lives here, beside
/// the two queries that read the table, rather than at the call sites — a
/// query that forgets it is a correctness bug that surfaces as resurrected
/// history.
/// Where this participant's visible history starts.
///
/// **Two levels, and the maximum of them.** A `/clear` from a participant is
/// written against their full principal and hides history for them alone —
/// which is what stops one member of a group conversation clearing another's
/// view ([ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md)). A `/clear`
/// with no sender is written against the conversation itself and hides
/// history for everyone in it; the TUI's single participant writes one, and so
/// does the migration that carries a pre-principal epoch forward, because that
/// is exactly what a per-conversation epoch used to mean.
///
/// Taking the maximum rather than the participant's alone is what makes those
/// two compose: a conversation-wide clear cannot be escaped by having spoken
/// before it, and a participant who clears again afterwards still moves only
/// their own line.
fn current_epoch(conn: &Connection, principal: &Principal) -> Result<i64, SessionError> {
    conn.query_row(
        "SELECT COALESCE(MAX(epoch_ordinal), 0) FROM session_epochs \
         WHERE conversation_key = ?1 AND principal IN (?2, ?3)",
        params![
            principal.conversation_name(),
            principal.storage_name(),
            // The conversation-wide marker. Equal to the one above when the
            // caller has no sender, which `IN` handles without a special case.
            principal.conversation_name(),
        ],
        |row| row.get(0),
    )
    .map_err(session_sqlite_error)
}

#[async_trait]
impl Session for SqliteStore {
    async fn load(&self, principal: &Principal) -> Result<Transcript, SessionError> {
        let conn = self.session_conn()?;
        let epoch = current_epoch(&conn, principal)?;
        let mut stmt = conn
            .prepare(
                "SELECT item_json \
                 FROM session_items \
                 WHERE conversation_key = ?1 AND ordinal >= ?2 \
                 ORDER BY ordinal ASC",
            )
            .map_err(session_sqlite_error)?;
        let rows = stmt
            .query_map(params![principal.conversation_name(), epoch], |row| {
                row.get::<_, String>(0)
            })
            .map_err(session_sqlite_error)?;

        let mut transcript = Transcript::default();
        for row in rows {
            let item_json = row.map_err(session_sqlite_error)?;
            transcript
                .items
                .push(serde_json::from_str(&item_json).map_err(session_json_error)?);
        }
        Ok(transcript)
    }

    async fn append(&self, principal: &Principal, items: Vec<Item>) -> Result<(), SessionError> {
        if items.is_empty() {
            return Ok(());
        }
        let conversation = principal.conversation_name();

        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let next_ordinal = tx
            .query_row(
                "SELECT COALESCE(MAX(ordinal) + 1, 0) FROM session_items \
                 WHERE conversation_key = ?1",
                params![conversation],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(session_sqlite_error)?
            .unwrap_or(0);

        for (offset, item) in items.into_iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| {
                SessionError::Backend(Arc::from("session append batch is too large"))
            })?;
            let item_json = serde_json::to_string(&item).map_err(session_json_error)?;
            tx.execute(
                "INSERT INTO session_items (conversation_key, ordinal, item_json) \
                 VALUES (?1, ?2, ?3)",
                params![conversation, next_ordinal + offset, item_json],
            )
            .map_err(session_sqlite_error)?;
        }

        tx.commit().map_err(session_sqlite_error)
    }

    /// Copy a prefix with one statement (roadmap X6).
    ///
    /// The default implementation would deserialize every item, move it through
    /// memory, and re-serialize it. Here the rows never leave the database, and
    /// the ordinals carry over unchanged — which is what keeps a compaction
    /// checkpoint's absolute positions meaningful in the child.
    ///
    /// The emptiness and self-fork checks happen inside the same transaction as
    /// the copy, so a concurrent append to the target cannot slip between the
    /// look and the write.
    async fn fork(
        &self,
        source: &Principal,
        boundary: usize,
        child: &Principal,
    ) -> Result<usize, SessionError> {
        let (source_key, child_key) = (source.conversation_name(), child.conversation_name());
        if source_key == child_key {
            return Err(SessionError::Backend(Arc::from(format!(
                "cannot fork conversation '{source_key}' onto itself"
            ))));
        }
        let boundary = i64::try_from(boundary).unwrap_or(i64::MAX);

        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let existing: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM session_items WHERE conversation_key = ?1",
                params![child_key],
                |row| row.get(0),
            )
            .map_err(session_sqlite_error)?;
        if existing > 0 {
            return Err(SessionError::Backend(Arc::from(format!(
                "fork target '{child_key}' already holds {existing} items; seeding it would \
                 interleave two histories"
            ))));
        }

        // `boundary` counts *visible* items, because that is what the caller
        // is holding — a parent's in-memory transcript is the projection, not
        // the log. Ordinals still carry over unchanged, which is what keeps a
        // compaction checkpoint's absolute positions meaningful in the child.
        let epoch = current_epoch(&tx, source)?;
        let seeded = tx
            .execute(
                "INSERT INTO session_items (conversation_key, ordinal, item_json) \
                 SELECT ?1, ordinal, item_json FROM session_items \
                 WHERE conversation_key = ?2 AND ordinal >= ?3 AND ordinal < ?4 \
                 ORDER BY ordinal ASC",
                params![child_key, source_key, epoch, epoch.saturating_add(boundary)],
            )
            .map_err(session_sqlite_error)?;
        tx.commit().map_err(session_sqlite_error)?;
        Ok(seeded)
    }
}
