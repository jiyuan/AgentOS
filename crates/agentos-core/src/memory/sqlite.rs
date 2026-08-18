use super::{
    record_matches_query, MemoryAccessLogEntry, MemoryAccounting, MemoryCaller, MemoryError,
};
use agentos_interfaces::memory::{Memory, Query, Record, Selector};
use agentos_interfaces::session::{Item, Session, SessionError, Transcript};
use agentos_proto::{Namespace, RecordId, SessionKey};
use async_trait::async_trait;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MemoryError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                MemoryError::Backend(Arc::from(format!(
                    "failed to create sqlite parent directory '{}': {err}",
                    parent.display()
                )))
            })?;
        }

        let conn = Connection::open(path).map_err(memory_sqlite_error)?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.init_schema()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, MemoryError> {
        let store = Self {
            conn: Mutex::new(Connection::open_in_memory().map_err(memory_sqlite_error)?),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), MemoryError> {
        let conn = self.memory_conn()?;
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS memory_records (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT UNIQUE,
                namespace TEXT NOT NULL,
                body_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                updated_at TEXT,
                last_accessed_at TEXT,
                access_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                store TEXT,
                owner_kind TEXT,
                owner_id TEXT,
                visibility TEXT,
                domain TEXT,
                source_run_id TEXT,
                source_task_id TEXT,
                source_agent_id TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS session_items (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                item_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(conversation_id, ordinal)
            );

            CREATE INDEX IF NOT EXISTS idx_session_items_conversation_ordinal
                ON session_items(conversation_id, ordinal);
            "#,
        )
        .map_err(memory_sqlite_error)?;

        ensure_memory_record_columns(&conn)?;
        conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_memory_records_namespace_row
                ON memory_records(namespace, row_id);

            CREATE INDEX IF NOT EXISTS idx_memory_records_scope
                ON memory_records(visibility, owner_kind, owner_id, store, domain, status);

            CREATE VIRTUAL TABLE IF NOT EXISTS memory_records_fts
                USING fts5(id UNINDEXED, namespace UNINDEXED, body_text, metadata_text);

            CREATE TABLE IF NOT EXISTS memory_links (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_id TEXT NOT NULL,
                to_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_memory_links_from
                ON memory_links(from_id, relation);

            CREATE TABLE IF NOT EXISTS memory_access_log (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                record_id TEXT,
                namespace TEXT NOT NULL,
                caller_agent_id TEXT NOT NULL,
                caller_task_id TEXT,
                caller_conversation_id TEXT,
                reason TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_memory_access_log_namespace_row
                ON memory_access_log(namespace, row_id);
            "#,
        )
        .map_err(memory_sqlite_error)?;
        Self::backfill_fts_records(&conn)?;
        Ok(())
    }

    pub(crate) fn memory_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, MemoryError> {
        self.conn
            .lock()
            .map_err(|_| MemoryError::Backend(Arc::from("sqlite store lock poisoned")))
    }

    fn session_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, SessionError> {
        self.conn
            .lock()
            .map_err(|_| SessionError::Backend(Arc::from("sqlite store lock poisoned")))
    }

    pub fn clear_session(&self, session_key: &SessionKey) -> Result<usize, SessionError> {
        let conn = self.session_conn()?;
        conn.execute(
            "DELETE FROM session_items WHERE conversation_id = ?1",
            params![session_key.storage_key()],
        )
        .map_err(session_sqlite_error)
    }
}

impl MemoryAccounting for SqliteStore {
    fn record_read_access(&self, record_ids: &[RecordId]) -> Result<(), MemoryError> {
        if record_ids.is_empty() {
            return Ok(());
        }

        let conn = self.memory_conn()?;
        let placeholders = std::iter::repeat_n("?", record_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "UPDATE memory_records \
             SET last_accessed_at = CURRENT_TIMESTAMP, access_count = access_count + 1 \
             WHERE id IN ({placeholders})"
        );
        conn.execute(
            &sql,
            params_from_iter(record_ids.iter().map(RecordId::as_str)),
        )
        .map_err(memory_sqlite_error)?;
        Ok(())
    }

    fn append_access_log(&self, entry: MemoryAccessLogEntry<'_>) -> Result<(), MemoryError> {
        let conn = self.memory_conn()?;
        conn.execute(
            "INSERT INTO memory_access_log \
             (operation, record_id, namespace, caller_agent_id, caller_task_id, caller_conversation_id, reason) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                entry.operation,
                entry.record_id.map(RecordId::as_str),
                entry.namespace.as_str(),
                entry.caller.agent_id.as_str(),
                entry.caller.task_id.as_str(),
                entry.caller.conversation_id.as_str(),
                entry.reason,
            ],
        )
        .map_err(memory_sqlite_error)?;
        Ok(())
    }

    fn append_access_log_for_records(
        &self,
        operation: &'static str,
        record_ids: &[RecordId],
        namespace: &Namespace,
        caller: &MemoryCaller,
        reason: Option<&str>,
    ) -> Result<(), MemoryError> {
        if record_ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.memory_conn()?;
        let tx = conn.transaction().map_err(memory_sqlite_error)?;
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO memory_access_log \
                     (operation, record_id, namespace, caller_agent_id, caller_task_id, caller_conversation_id, reason) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                )
                .map_err(memory_sqlite_error)?;
            for record_id in record_ids {
                stmt.execute(params![
                    operation,
                    record_id.as_str(),
                    namespace.as_str(),
                    caller.agent_id.as_str(),
                    caller.task_id.as_str(),
                    caller.conversation_id.as_str(),
                    reason,
                ])
                .map_err(memory_sqlite_error)?;
            }
        }
        tx.commit().map_err(memory_sqlite_error)?;
        Ok(())
    }
}

#[async_trait]
impl Memory for SqliteStore {
    async fn write(&self, ns: &Namespace, mut record: Record) -> Result<RecordId, MemoryError> {
        let conn = self.memory_conn()?;
        record.namespace = ns.clone();
        let body_json = serde_json::to_string(&record.body).map_err(memory_json_error)?;
        let metadata_json = serde_json::to_string(&record.metadata).map_err(memory_json_error)?;
        let existing_id = record.id.as_ref().map(RecordId::as_str);
        let status = metadata_string(&record.metadata, "status").unwrap_or("active");
        let store = metadata_string(&record.metadata, "store");
        let owner_kind = metadata_string(&record.metadata, "owner_kind");
        let owner_id = metadata_string(&record.metadata, "owner_id");
        let visibility = metadata_string(&record.metadata, "visibility");
        let domain = metadata_string(&record.metadata, "domain");
        let source_run_id = metadata_string(&record.metadata, "source_run_id");
        let source_task_id = metadata_string(&record.metadata, "source_task_id");
        let source_agent_id = metadata_string(&record.metadata, "source_agent_id");

        conn.execute(
            "INSERT INTO memory_records \
             (id, namespace, body_json, metadata_json, updated_at, status, store, owner_kind, owner_id, visibility, domain, source_run_id, source_task_id, source_agent_id) \
             VALUES (?1, ?2, ?3, ?4, CURRENT_TIMESTAMP, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                existing_id,
                ns.as_str(),
                &body_json,
                &metadata_json,
                status,
                store,
                owner_kind,
                owner_id,
                visibility,
                domain,
                source_run_id,
                source_task_id,
                source_agent_id,
            ],
        )
        .map_err(memory_sqlite_error)?;

        let row_id = conn.last_insert_rowid();
        let id = record
            .id
            .unwrap_or_else(|| RecordId::new(format!("record-{row_id}")));
        conn.execute(
            "UPDATE memory_records SET id = ?1 WHERE row_id = ?2",
            params![id.as_str(), row_id],
        )
        .map_err(memory_sqlite_error)?;
        Self::upsert_fts_record_if_present(&conn, &id, ns, &body_json, &metadata_json, status)?;

        Ok(id)
    }

    async fn read(&self, ns: &Namespace, q: &Query) -> Result<Vec<Record>, MemoryError> {
        if q.limit == 0 {
            return Ok(Vec::new());
        }

        if let Some(search_text) = q
            .lexical_text()
            .map(str::trim)
            .filter(|search_text| !search_text.is_empty())
        {
            if let Some(records) = self.perform_fts_search(ns, search_text, q.limit)? {
                return Ok(records);
            }
        }

        self.perform_standard_read(ns, q)
    }

    async fn forget(&self, ns: &Namespace, sel: &Selector) -> Result<usize, MemoryError> {
        let conn = self.memory_conn()?;
        let removed = if let Some(id) = &sel.id {
            conn.execute(
                "DELETE FROM memory_records WHERE namespace = ?1 AND id = ?2",
                params![ns.as_str(), id.as_str()],
            )
        } else if let Some(namespace) = &sel.namespace {
            conn.execute(
                "DELETE FROM memory_records WHERE namespace = ?1 AND namespace = ?2",
                params![ns.as_str(), namespace.as_str()],
            )
        } else {
            conn.execute(
                "DELETE FROM memory_records WHERE namespace = ?1",
                params![ns.as_str()],
            )
        }
        .map_err(memory_sqlite_error)?;
        Self::delete_fts_records_if_present(&conn, ns, sel)?;
        Ok(removed)
    }
}

impl SqliteStore {
    fn perform_standard_read(&self, ns: &Namespace, q: &Query) -> Result<Vec<Record>, MemoryError> {
        let limit = q.limit;
        let conn = self.memory_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, namespace, body_json, metadata_json \
                 FROM memory_records \
                 WHERE namespace = ?1 \
                 ORDER BY row_id ASC",
            )
            .map_err(memory_sqlite_error)?;
        let rows = stmt
            .query_map(params![ns.as_str()], |row| {
                let id: String = row.get(0)?;
                let namespace: String = row.get(1)?;
                let body_json: String = row.get(2)?;
                let metadata_json: String = row.get(3)?;
                Ok((id, namespace, body_json, metadata_json))
            })
            .map_err(memory_sqlite_error)?;

        let mut records = Vec::new();
        for row in rows {
            let (id, namespace, body_json, metadata_json) = row.map_err(memory_sqlite_error)?;
            let record = record_from_json_parts(id, namespace, body_json, metadata_json)?;
            if record_matches_query(&record, q) {
                records.push(record);
            }
            if records.len() >= limit {
                break;
            }
        }
        Ok(records)
    }

    fn perform_fts_search(
        &self,
        ns: &Namespace,
        search_text: &str,
        limit: usize,
    ) -> Result<Option<Vec<Record>>, MemoryError> {
        let Some(match_query) = fts_match_query(search_text) else {
            return Ok(None);
        };
        let conn = self.memory_conn()?;
        if !Self::fts_table_exists(&conn)? {
            return Ok(None);
        }

        let mut stmt = conn
            .prepare(
                "SELECT r.id, r.namespace, r.body_json, r.metadata_json \
                 FROM memory_records_fts \
                 JOIN memory_records r ON r.id = memory_records_fts.id \
                 WHERE memory_records_fts MATCH ?1 \
                   AND r.namespace = ?2 \
                   AND r.status = 'active' \
                 ORDER BY bm25(memory_records_fts), r.row_id ASC \
                 LIMIT ?3",
            )
            .map_err(memory_sqlite_error)?;
        let rows = stmt
            .query_map(params![match_query, ns.as_str(), limit as i64], |row| {
                let id: String = row.get(0)?;
                let namespace: String = row.get(1)?;
                let body_json: String = row.get(2)?;
                let metadata_json: String = row.get(3)?;
                Ok((id, namespace, body_json, metadata_json))
            })
            .map_err(memory_sqlite_error)?;

        let mut records = Vec::new();
        for row in rows {
            let (id, namespace, body_json, metadata_json) = row.map_err(memory_sqlite_error)?;
            records.push(record_from_json_parts(
                id,
                namespace,
                body_json,
                metadata_json,
            )?);
        }
        Ok(Some(records))
    }

    fn fts_table_exists(conn: &Connection) -> Result<bool, MemoryError> {
        let count = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'memory_records_fts'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(memory_sqlite_error)?;
        Ok(count > 0)
    }

    fn backfill_fts_records(conn: &Connection) -> Result<(), MemoryError> {
        conn.execute(
            "INSERT INTO memory_records_fts (id, namespace, body_text, metadata_text) \
             SELECT r.id, r.namespace, r.body_json, r.metadata_json \
             FROM memory_records r \
             WHERE r.status = 'active' \
               AND r.id IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM memory_records_fts f WHERE f.id = r.id)",
            [],
        )
        .map_err(memory_sqlite_error)?;
        Ok(())
    }

    fn upsert_fts_record_if_present(
        conn: &Connection,
        id: &RecordId,
        ns: &Namespace,
        body_json: &str,
        metadata_json: &str,
        status: &str,
    ) -> Result<(), MemoryError> {
        if !Self::fts_table_exists(conn)? {
            return Ok(());
        }
        conn.execute(
            "DELETE FROM memory_records_fts WHERE id = ?1",
            params![id.as_str()],
        )
        .map_err(memory_sqlite_error)?;
        if status != "active" {
            return Ok(());
        }
        conn.execute(
            "INSERT INTO memory_records_fts (id, namespace, body_text, metadata_text) \
             VALUES (?1, ?2, ?3, ?4)",
            params![id.as_str(), ns.as_str(), body_json, metadata_json],
        )
        .map_err(memory_sqlite_error)?;
        Ok(())
    }

    fn delete_fts_records_if_present(
        conn: &Connection,
        ns: &Namespace,
        sel: &Selector,
    ) -> Result<(), MemoryError> {
        if !Self::fts_table_exists(conn)? {
            return Ok(());
        }
        if let Some(id) = &sel.id {
            conn.execute(
                "DELETE FROM memory_records_fts WHERE id = ?1",
                params![id.as_str()],
            )
        } else {
            conn.execute(
                "DELETE FROM memory_records_fts WHERE namespace = ?1",
                params![ns.as_str()],
            )
        }
        .map_err(memory_sqlite_error)?;
        Ok(())
    }
}

#[async_trait]
impl Session for SqliteStore {
    async fn load(&self, session_key: &SessionKey) -> Result<Transcript, SessionError> {
        let conn = self.session_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT item_json \
                 FROM session_items \
                 WHERE conversation_id = ?1 \
                 ORDER BY ordinal ASC",
            )
            .map_err(session_sqlite_error)?;
        let rows = stmt
            .query_map(params![session_key.storage_key()], |row| {
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

    async fn append(&self, session_key: &SessionKey, items: Vec<Item>) -> Result<(), SessionError> {
        if items.is_empty() {
            return Ok(());
        }

        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let next_ordinal = tx
            .query_row(
                "SELECT COALESCE(MAX(ordinal) + 1, 0) FROM session_items WHERE conversation_id = ?1",
                params![session_key.storage_key()],
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
                params![session_key.storage_key(), next_ordinal + offset, item_json],
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
        source: &SessionKey,
        boundary: usize,
        child_id: &SessionKey,
    ) -> Result<usize, SessionError> {
        if source == child_id {
            return Err(SessionError::Backend(Arc::from(format!(
                "cannot fork session '{}' onto itself",
                source.storage_key()
            ))));
        }
        let boundary = i64::try_from(boundary).unwrap_or(i64::MAX);

        let mut conn = self.session_conn()?;
        let tx = conn.transaction().map_err(session_sqlite_error)?;
        let existing: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM session_items WHERE conversation_id = ?1",
                params![child_id.storage_key()],
                |row| row.get(0),
            )
            .map_err(session_sqlite_error)?;
        if existing > 0 {
            return Err(SessionError::Backend(Arc::from(format!(
                "fork target '{}' already holds {existing} items; seeding it would interleave \
                 two histories",
                child_id.storage_key()
            ))));
        }

        let seeded = tx
            .execute(
                "INSERT INTO session_items (conversation_id, ordinal, item_json) \
                 SELECT ?1, ordinal, item_json FROM session_items \
                 WHERE conversation_id = ?2 AND ordinal < ?3 \
                 ORDER BY ordinal ASC",
                params![child_id.storage_key(), source.storage_key(), boundary],
            )
            .map_err(session_sqlite_error)?;
        tx.commit().map_err(session_sqlite_error)?;
        Ok(seeded)
    }
}

fn ensure_memory_record_columns(conn: &Connection) -> Result<(), MemoryError> {
    for (name, definition) in [
        ("updated_at", "TEXT"),
        ("last_accessed_at", "TEXT"),
        ("access_count", "INTEGER NOT NULL DEFAULT 0"),
        ("status", "TEXT NOT NULL DEFAULT 'active'"),
        ("store", "TEXT"),
        ("owner_kind", "TEXT"),
        ("owner_id", "TEXT"),
        ("visibility", "TEXT"),
        ("domain", "TEXT"),
        ("source_run_id", "TEXT"),
        ("source_task_id", "TEXT"),
        ("source_agent_id", "TEXT"),
    ] {
        if !memory_record_column_exists(conn, name)? {
            conn.execute(
                &format!("ALTER TABLE memory_records ADD COLUMN {name} {definition}"),
                [],
            )
            .map_err(memory_sqlite_error)?;
        }
    }
    Ok(())
}

pub(crate) fn memory_record_column_exists(
    conn: &Connection,
    column: &str,
) -> Result<bool, MemoryError> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(memory_records)")
        .map_err(memory_sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(memory_sqlite_error)?;
    for row in rows {
        if row.map_err(memory_sqlite_error)? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn metadata_string<'a>(metadata: &'a BTreeMap<Arc<str>, Value>, key: &str) -> Option<&'a str> {
    metadata.get(key).and_then(Value::as_str)
}

pub(crate) fn memory_sqlite_error(err: rusqlite::Error) -> MemoryError {
    MemoryError::Backend(Arc::from(err.to_string()))
}

fn session_sqlite_error(err: rusqlite::Error) -> SessionError {
    SessionError::Backend(Arc::from(err.to_string()))
}

pub(crate) fn memory_json_error(err: serde_json::Error) -> MemoryError {
    MemoryError::Backend(Arc::from(err.to_string()))
}

fn session_json_error(err: serde_json::Error) -> SessionError {
    SessionError::Backend(Arc::from(err.to_string()))
}

fn record_from_json_parts(
    id: String,
    namespace: String,
    body_json: String,
    metadata_json: String,
) -> Result<Record, MemoryError> {
    Ok(Record {
        id: Some(RecordId::new(id)),
        namespace: Namespace::new(namespace),
        body: serde_json::from_str(&body_json).map_err(memory_json_error)?,
        metadata: serde_json::from_str(&metadata_json).map_err(memory_json_error)?,
    })
}

fn fts_match_query(input: &str) -> Option<String> {
    let terms = input
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>();
    if terms.is_empty() {
        None
    } else {
        Some(terms.join(" AND "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::{AgentId, ConversationId, TaskId};

    fn caller() -> MemoryCaller {
        MemoryCaller {
            agent_id: AgentId::new("alice"),
            task_id: TaskId::new("t1"),
            conversation_id: ConversationId::new("c1"),
            principal_key: None,
            user_id: None,
            allowed_shared_domains: Vec::new(),
            audit_read_access: false,
        }
    }

    fn seed_record(store: &SqliteStore, ns: &Namespace, id: &str) {
        let conn = store.memory_conn().unwrap();
        conn.execute(
            "INSERT INTO memory_records (id, namespace, body_json, metadata_json, status) \
             VALUES (?1, ?2, '{}', '{}', 'active')",
            params![id, ns.as_str()],
        )
        .unwrap();
    }

    fn access_count(store: &SqliteStore, id: &str) -> i64 {
        let conn = store.memory_conn().unwrap();
        conn.query_row(
            "SELECT access_count FROM memory_records WHERE id = ?1",
            params![id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
    }

    fn access_log_count(store: &SqliteStore, operation: &str) -> i64 {
        let conn = store.memory_conn().unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM memory_access_log WHERE operation = ?1",
            params![operation],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
    }

    #[test]
    fn record_read_access_increments_each_record_in_one_statement() {
        let store = SqliteStore::open_in_memory().unwrap();
        let ns = Namespace::new("memory:test");
        for id in ["a", "b", "c"] {
            seed_record(&store, &ns, id);
        }

        let ids = ["a", "b", "c"]
            .iter()
            .map(|s| RecordId::new(*s))
            .collect::<Vec<_>>();
        store.record_read_access(&ids).unwrap();

        for id in ["a", "b", "c"] {
            assert_eq!(access_count(&store, id), 1, "first access counted for {id}");
        }

        store.record_read_access(&ids).unwrap();
        for id in ["a", "b", "c"] {
            assert_eq!(
                access_count(&store, id),
                2,
                "second access counted for {id}"
            );
        }
    }

    #[test]
    fn record_read_access_is_a_noop_for_empty_input() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.record_read_access(&[]).unwrap();
    }

    #[test]
    fn append_access_log_for_records_writes_one_row_per_id() {
        let store = SqliteStore::open_in_memory().unwrap();
        let ns = Namespace::new("memory:test");
        let caller = caller();
        let ids = ["a", "b", "c", "d"]
            .iter()
            .map(|s| RecordId::new(*s))
            .collect::<Vec<_>>();

        store
            .append_access_log_for_records("read", &ids, &ns, &caller, Some("hydrate"))
            .unwrap();

        assert_eq!(access_log_count(&store, "read"), 4);
    }

    #[test]
    fn append_access_log_for_records_is_a_noop_for_empty_input() {
        let store = SqliteStore::open_in_memory().unwrap();
        let ns = Namespace::new("memory:test");
        let caller = caller();
        store
            .append_access_log_for_records("read", &[], &ns, &caller, None)
            .unwrap();
        assert_eq!(access_log_count(&store, "read"), 0);
    }
}
