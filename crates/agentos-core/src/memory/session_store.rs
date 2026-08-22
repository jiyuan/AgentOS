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
use agentos_interfaces::session::{Item, Session, SessionError, Transcript};
use agentos_proto::ConversationId;
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
    pub fn clear_session(&self, conv_id: &ConversationId) -> Result<usize, SessionError> {
        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let epoch = current_epoch(&tx, conv_id)?;
        let next: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(ordinal) + 1, 0) FROM session_items \
                 WHERE conversation_id = ?1",
                params![conv_id.as_str()],
                |row| row.get(0),
            )
            .map_err(session_sqlite_error)?;
        // What the user is clearing is what they can see, which is what a
        // second `/clear` on an already-cleared conversation reports as zero.
        let hidden = usize::try_from(next.saturating_sub(epoch)).unwrap_or(0);
        tx.execute(
            "INSERT INTO session_epochs (conversation_id, epoch_ordinal) VALUES (?1, ?2)",
            params![conv_id.as_str(), next],
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
    pub fn session_log(&self, conv_id: &ConversationId) -> Result<Vec<Item>, SessionError> {
        let conn = self.session_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT item_json FROM session_items \
                 WHERE conversation_id = ?1 ORDER BY ordinal ASC",
            )
            .map_err(session_sqlite_error)?;
        let rows = stmt
            .query_map(params![conv_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(session_sqlite_error)?;
        let mut items = Vec::new();
        for row in rows {
            let item_json = row.map_err(session_sqlite_error)?;
            items.push(serde_json::from_str(&item_json).map_err(session_json_error)?);
        }
        Ok(items)
    }

    /// Irreversibly remove a conversation's items and its epoch markers.
    ///
    /// The legitimate requirement `/clear` does not serve: somebody asking to
    /// be forgotten is not asking for a projection. Deliberately a separate
    /// method with a separate name, so nothing reaches it by the casual path,
    /// and callers are expected to confirm explicitly and record a safety
    /// event ([ADR-0006](../../../../docs/adr/0006-CLEAR_EPOCH.md)).
    pub fn purge_session(&self, conv_id: &ConversationId) -> Result<usize, SessionError> {
        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let removed = tx
            .execute(
                "DELETE FROM session_items WHERE conversation_id = ?1",
                params![conv_id.as_str()],
            )
            .map_err(session_sqlite_error)?;
        tx.execute(
            "DELETE FROM session_epochs WHERE conversation_id = ?1",
            params![conv_id.as_str()],
        )
        .map_err(session_sqlite_error)?;
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
fn current_epoch(conn: &Connection, conv_id: &ConversationId) -> Result<i64, SessionError> {
    conn.query_row(
        "SELECT COALESCE(MAX(epoch_ordinal), 0) FROM session_epochs WHERE conversation_id = ?1",
        params![conv_id.as_str()],
        |row| row.get(0),
    )
    .map_err(session_sqlite_error)
}

#[async_trait]
impl Session for SqliteStore {
    async fn load(&self, conv_id: &ConversationId) -> Result<Transcript, SessionError> {
        let conn = self.session_conn()?;
        let epoch = current_epoch(&conn, conv_id)?;
        let mut stmt = conn
            .prepare(
                "SELECT item_json \
                 FROM session_items \
                 WHERE conversation_id = ?1 AND ordinal >= ?2 \
                 ORDER BY ordinal ASC",
            )
            .map_err(session_sqlite_error)?;
        let rows = stmt
            .query_map(params![conv_id.as_str(), epoch], |row| {
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

    async fn append(&self, conv_id: &ConversationId, items: Vec<Item>) -> Result<(), SessionError> {
        if items.is_empty() {
            return Ok(());
        }

        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let next_ordinal = tx
            .query_row(
                "SELECT COALESCE(MAX(ordinal) + 1, 0) FROM session_items WHERE conversation_id = ?1",
                params![conv_id.as_str()],
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
                "INSERT INTO session_items (conversation_id, ordinal, item_json) VALUES (?1, ?2, ?3)",
                params![conv_id.as_str(), next_ordinal + offset, item_json],
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
        source: &ConversationId,
        boundary: usize,
        child_id: &ConversationId,
    ) -> Result<usize, SessionError> {
        if source == child_id {
            return Err(SessionError::Backend(Arc::from(format!(
                "cannot fork conversation '{}' onto itself",
                source.as_str()
            ))));
        }
        let boundary = i64::try_from(boundary).unwrap_or(i64::MAX);

        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let existing: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM session_items WHERE conversation_id = ?1",
                params![child_id.as_str()],
                |row| row.get(0),
            )
            .map_err(session_sqlite_error)?;
        if existing > 0 {
            return Err(SessionError::Backend(Arc::from(format!(
                "fork target '{}' already holds {existing} items; seeding it would interleave \
                 two histories",
                child_id.as_str()
            ))));
        }

        // `boundary` counts *visible* items, because that is what the caller
        // is holding — a parent's in-memory transcript is the projection, not
        // the log. Ordinals still carry over unchanged, which is what keeps a
        // compaction checkpoint's absolute positions meaningful in the child.
        let epoch = current_epoch(&tx, source)?;
        let seeded = tx
            .execute(
                "INSERT INTO session_items (conversation_id, ordinal, item_json) \
                 SELECT ?1, ordinal, item_json FROM session_items \
                 WHERE conversation_id = ?2 AND ordinal >= ?3 AND ordinal < ?4 \
                 ORDER BY ordinal ASC",
                params![
                    child_id.as_str(),
                    source.as_str(),
                    epoch,
                    epoch.saturating_add(boundary)
                ],
            )
            .map_err(session_sqlite_error)?;
        tx.commit().map_err(session_sqlite_error)?;
        Ok(seeded)
    }
}
