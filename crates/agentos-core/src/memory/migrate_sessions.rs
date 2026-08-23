//! Moving the session log onto principal keys (M3 deliverable 2).
//!
//! The second half of the `ID-002` migration, and the last thing keyed on a
//! bare `ConversationId`. `session_items` and `session_epochs` were keyed by
//! the number the transport chose, so Telegram's chat `42`, Feishu's chat `42`
//! and a second agent sharing the database were one transcript.
//!
//! Separate from `migrate.rs` for two reasons. It is a *schema* change as well
//! as a data change — the columns are renamed, and `session_epochs` gains one
//! — and `migrate.rs` was already at the module ceiling.
//!
//! # Why the whole table is rebuilt
//!
//! `ALTER TABLE … RENAME COLUMN` would leave the old rows in place with new
//! column names and old *values*, which is the worst of the available states:
//! the code would read them, find no principal, and report every conversation
//! as empty. Building the new tables beside the old ones and swapping means
//! there are only ever two states — before and after — and a crash lands in
//! the first.
//!
//! # What happens to an epoch
//!
//! A legacy epoch was per conversation: one `/clear` hid history from
//! everybody in it. That is exactly what a *conversation-wide* epoch means
//! now, so each one carries forward under
//! [`Principal::conversation_name`], and nobody's cleared history comes back.
//! See `session_store::current_epoch` for how the two levels compose.

use super::migrate::{ensure_schema_version_table, MigrationSettings};
use super::sqlite::SqliteStore;
use agentos_interfaces::memory::MemoryError;
use agentos_proto::{ChannelId, ConversationId, Principal};
use rusqlite::{params, Connection};
use std::sync::Arc;

/// Schema version written once the session log is principal-keyed.
pub const SESSION_PRINCIPAL_SCHEMA_VERSION: i64 = 3;

/// What shape the session tables are in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionSchema {
    /// No `session_items` yet. A database this runtime has not written to.
    Absent,
    /// Keyed by a bare conversation id. Needs `agentos-gateway migrate`.
    Legacy,
    /// Keyed by `Principal::conversation_name`.
    Current,
}

/// Which shape this database's session tables are in.
///
/// Read from `PRAGMA table_info` rather than from `schema_version`, because
/// the column layout is the thing the queries actually depend on and a version
/// row can be absent, stale, or written by a build that got half way.
pub fn session_schema(store: &SqliteStore) -> Result<SessionSchema, MemoryError> {
    let conn = store.memory_conn()?;
    schema_of(&conn)
}

/// [`session_schema`] against a connection the caller already holds.
///
/// `SqliteStore::init_schema` needs this before the store is usable, which is
/// exactly when it cannot borrow one from the store.
pub(super) fn schema_of_connection(conn: &Connection) -> Result<SessionSchema, MemoryError> {
    schema_of(conn)
}

fn schema_of(conn: &Connection) -> Result<SessionSchema, MemoryError> {
    let mut statement = conn
        .prepare("SELECT name FROM pragma_table_info('session_items')")
        .map_err(super::memory_sqlite_error)?;
    let columns: Vec<String> = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(super::memory_sqlite_error)?
        .collect::<Result<_, _>>()
        .map_err(super::memory_sqlite_error)?;
    if columns.is_empty() {
        return Ok(SessionSchema::Absent);
    }
    if columns.iter().any(|name| name == "conversation_key") {
        return Ok(SessionSchema::Current);
    }
    Ok(SessionSchema::Legacy)
}

/// One legacy conversation and the principal it becomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRewrite {
    pub from: String,
    pub to: String,
    pub items: usize,
}

/// What a session migration would do.
#[derive(Clone, Debug, Default)]
pub struct SessionPlan {
    pub rewrites: Vec<SessionRewrite>,
    pub epochs: usize,
    /// Conversations whose target key another conversation already claims.
    ///
    /// Only reachable when a legacy database somehow holds two ids that encode
    /// alike, which the injective encoder rules out — kept because "cannot
    /// happen" is not a reason to merge two people's transcripts if it does.
    pub collisions: Vec<SessionRewrite>,
}

impl SessionPlan {
    pub fn is_empty(&self) -> bool {
        self.rewrites.is_empty() && self.collisions.is_empty()
    }

    pub fn items_to_move(&self) -> usize {
        self.rewrites.iter().map(|rewrite| rewrite.items).sum()
    }

    pub fn report(&self) -> String {
        let mut out = String::new();
        if !self.collisions.is_empty() {
            out.push_str(&format!(
                "{} conversation(s) would collide and will NOT be migrated:\n",
                self.collisions.len()
            ));
            for collision in &self.collisions {
                out.push_str(&format!(
                    "  {} -> {} ({} item(s))\n",
                    collision.from, collision.to, collision.items
                ));
            }
            out.push('\n');
        }
        out.push_str(&format!(
            "{} conversation(s) holding {} session item(s) will be rekeyed:\n",
            self.rewrites.len(),
            self.items_to_move()
        ));
        for rewrite in &self.rewrites {
            out.push_str(&format!(
                "  {} -> {} ({} item(s))\n",
                rewrite.from, rewrite.to, rewrite.items
            ));
        }
        if self.epochs > 0 {
            out.push_str(&format!(
                "\n{} `/clear` marker(s) carry forward as conversation-wide epochs, so no \
                 cleared history reappears.\n",
                self.epochs
            ));
        }
        out
    }
}

/// Work out what the session migration would do. Reads only.
pub fn plan(store: &SqliteStore, settings: &MigrationSettings) -> Result<SessionPlan, MemoryError> {
    let conn = store.memory_conn()?;
    if schema_of(&conn)? != SessionSchema::Legacy {
        return Ok(SessionPlan::default());
    }

    let mut statement = conn
        .prepare("SELECT conversation_id, COUNT(*) FROM session_items GROUP BY conversation_id")
        .map_err(super::memory_sqlite_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as usize,
            ))
        })
        .map_err(super::memory_sqlite_error)?;

    let mut plan = SessionPlan::default();
    let mut claimed: Vec<String> = Vec::new();
    for row in rows {
        let (conversation, items) = row.map_err(super::memory_sqlite_error)?;
        let to = target_key(settings, &conversation);
        let rewrite = SessionRewrite {
            from: conversation,
            to,
            items,
        };
        if claimed.contains(&rewrite.to) {
            plan.collisions.push(rewrite);
            continue;
        }
        claimed.push(rewrite.to.clone());
        plan.rewrites.push(rewrite);
    }
    plan.rewrites.sort_by(|a, b| a.from.cmp(&b.from));

    plan.epochs = conn
        .query_row("SELECT COUNT(*) FROM session_epochs", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(super::memory_sqlite_error)?
        .max(0) as usize;
    Ok(plan)
}

/// Rebuild both session tables under principal keys.
///
/// One transaction. A crash leaves the legacy tables untouched, which is the
/// same "not yet" the plan describes rather than a third state to reason
/// about.
pub fn apply(
    store: &SqliteStore,
    settings: &MigrationSettings,
    plan: &SessionPlan,
) -> Result<usize, MemoryError> {
    if plan.is_empty() {
        return Ok(0);
    }
    if !plan.collisions.is_empty() {
        return Err(MemoryError::Backend(Arc::from(format!(
            "refusing to migrate sessions: {} conversation(s) would collide; see the report",
            plan.collisions.len()
        ))));
    }

    let mut conn = store.memory_conn()?;
    ensure_schema_version_table(&conn)?;
    let transaction = conn.transaction().map_err(super::memory_sqlite_error)?;
    if schema_of(&transaction)? != SessionSchema::Legacy {
        return Err(MemoryError::Backend(Arc::from(
            "session tables changed shape between the plan and the apply",
        )));
    }

    transaction
        .execute_batch(
            r#"
            CREATE TABLE session_items_migrated (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_key TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                item_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(conversation_key, ordinal)
            );
            CREATE TABLE session_epochs_migrated (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_key TEXT NOT NULL,
                principal TEXT NOT NULL,
                epoch_ordinal INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            "#,
        )
        .map_err(super::memory_sqlite_error)?;

    let mut moved = 0usize;
    for rewrite in &plan.rewrites {
        // `created_at` is carried over, not re-stamped: it is what
        // `purge --sessions --before` reads, and a migration must not make
        // every conversation look like it was written today.
        moved += transaction
            .execute(
                "INSERT INTO session_items_migrated \
                   (conversation_key, ordinal, item_json, created_at) \
                 SELECT ?1, ordinal, item_json, created_at FROM session_items \
                 WHERE conversation_id = ?2 ORDER BY ordinal ASC",
                params![rewrite.to, rewrite.from],
            )
            .map_err(super::memory_sqlite_error)?;
        transaction
            .execute(
                "INSERT INTO session_epochs_migrated \
                   (conversation_key, principal, epoch_ordinal, created_at) \
                 SELECT ?1, ?1, epoch_ordinal, created_at FROM session_epochs \
                 WHERE conversation_id = ?2",
                params![rewrite.to, rewrite.from],
            )
            .map_err(super::memory_sqlite_error)?;
    }

    // Epoch rows for a conversation with no items are dropped with the old
    // table. A marker over an empty log hides nothing, and keeping it would
    // mean carrying a key nothing can be resolved against.
    transaction
        .execute_batch(
            r#"
            DROP TABLE session_items;
            DROP TABLE session_epochs;
            ALTER TABLE session_items_migrated RENAME TO session_items;
            ALTER TABLE session_epochs_migrated RENAME TO session_epochs;
            CREATE INDEX IF NOT EXISTS idx_session_items_conversation_ordinal
                ON session_items(conversation_key, ordinal);
            CREATE INDEX IF NOT EXISTS idx_session_epochs_principal
                ON session_epochs(conversation_key, principal, epoch_ordinal);
            "#,
        )
        .map_err(super::memory_sqlite_error)?;

    transaction
        .execute(
            "INSERT INTO schema_version (id, version) VALUES (1, ?1)
             ON CONFLICT(id) DO UPDATE SET version = ?1, updated_at = CURRENT_TIMESTAMP",
            params![SESSION_PRINCIPAL_SCHEMA_VERSION],
        )
        .map_err(super::memory_sqlite_error)?;
    transaction.commit().map_err(super::memory_sqlite_error)?;
    let _ = settings;
    Ok(moved)
}

/// The principal name a legacy conversation id becomes.
fn target_key(settings: &MigrationSettings, conversation: &str) -> String {
    Principal::conversation(
        settings.agent.clone(),
        ChannelId::new(settings.channel.as_str()),
        ConversationId::new(conversation),
    )
    .conversation_name()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_proto::AgentId;

    fn settings() -> MigrationSettings {
        MigrationSettings {
            agent: AgentId::new("main"),
            channel: ChannelId::new("telegram"),
            assume_literal_underscores: false,
        }
    }

    /// Build a database in the pre-principal shape, with items and a clear.
    fn legacy_store(label: &str) -> (SqliteStore, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "agentos-session-migrate-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("agentos.sqlite");

        let conn = Connection::open(&path).expect("a legacy database");
        conn.execute_batch(
            r#"
            CREATE TABLE session_items (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                item_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(conversation_id, ordinal)
            );
            CREATE TABLE session_epochs (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                epoch_ordinal INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO session_items (conversation_id, ordinal, item_json)
                VALUES ('42', 0, '{"message":{"role":"user","content":"one"}}'),
                       ('42', 1, '{"message":{"role":"user","content":"two"}}'),
                       ('99', 0, '{"message":{"role":"user","content":"other"}}');
            INSERT INTO session_epochs (conversation_id, epoch_ordinal) VALUES ('42', 1);
            "#,
        )
        .expect("legacy rows");
        drop(conn);

        let store = SqliteStore::open(&path).expect("the store opens a legacy database");
        (store, dir)
    }

    #[test]
    fn a_legacy_database_is_recognised_and_planned() {
        let (store, dir) = legacy_store("plan");
        assert_eq!(
            session_schema(&store).expect("schema"),
            SessionSchema::Legacy
        );

        let plan = plan(&store, &settings()).expect("plan");
        assert_eq!(plan.rewrites.len(), 2);
        assert_eq!(plan.items_to_move(), 3);
        assert_eq!(plan.epochs, 1);
        assert_eq!(plan.rewrites[0].to, "v1.main.telegram.42.n");
        assert!(plan.report().contains("v1.main.telegram.42.n"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Planning reads. A database nobody applied to is still legacy.
    #[test]
    fn planning_changes_nothing() {
        let (store, dir) = legacy_store("plan-only");
        plan(&store, &settings()).expect("plan");
        assert_eq!(
            session_schema(&store).expect("schema"),
            SessionSchema::Legacy
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn applying_rekeys_items_and_carries_the_epoch_forward() {
        let (store, dir) = legacy_store("apply");
        let plan = plan(&store, &settings()).expect("plan");
        assert_eq!(apply(&store, &settings(), &plan).expect("apply"), 3);
        assert_eq!(
            session_schema(&store).expect("schema"),
            SessionSchema::Current
        );

        let conn = store.memory_conn().expect("a connection");
        let items: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_items WHERE conversation_key = 'v1.main.telegram.42.n'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(items, 2);

        // The clear becomes a conversation-wide epoch, which is what it meant.
        let (key, principal, ordinal): (String, String, i64) = conn
            .query_row(
                "SELECT conversation_key, principal, epoch_ordinal FROM session_epochs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("the epoch");
        assert_eq!(key, "v1.main.telegram.42.n");
        assert_eq!(principal, "v1.main.telegram.42.n");
        assert_eq!(ordinal, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A second run has nothing to do rather than something wrong to do.
    #[test]
    fn applying_twice_is_a_no_op() {
        let (store, dir) = legacy_store("twice");
        let first = plan(&store, &settings()).expect("plan");
        apply(&store, &settings(), &first).expect("apply");

        let second = plan(&store, &settings()).expect("re-plan");
        assert!(second.is_empty());
        assert_eq!(apply(&store, &settings(), &second).expect("apply"), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
