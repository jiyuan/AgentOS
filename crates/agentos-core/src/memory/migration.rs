//! Versioned migration of the pre-principal SQLite layout.

use super::{memory_sqlite_error, MemoryError, MemoryOwner, MemoryScope};
use agentos_interfaces::session::Item;
use agentos_proto::{AgentId, ChannelId, ConversationId, PrincipalKey, SenderIdentity, SessionKey};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

mod report;

use report::{
    column_exists, item_is_user, memory_collisions, memory_issues, metadata_value, namespace_is_v1,
    parse_store, parse_visibility, principal_sender_matches, schema_version, session_collisions,
    session_issues, table_count, table_exists,
};

pub const CURRENT_PERSISTENCE_VERSION: u32 = 1;
const MIGRATION_ID: &str = "id-002-principal-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationCollision {
    pub kind: Arc<str>,
    pub legacy_key: Arc<str>,
    pub target_keys: Vec<Arc<str>>,
    pub row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationIssue {
    pub kind: Arc<str>,
    pub legacy_key: Arc<str>,
    pub row_count: usize,
    pub reason: Arc<str>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MigrationReport {
    pub database_path: PathBuf,
    pub from_version: u32,
    pub to_version: u32,
    pub already_current: bool,
    pub backup_required: bool,
    pub session_rows: usize,
    pub session_rows_to_migrate: usize,
    pub session_rows_to_quarantine: usize,
    pub memory_rows: usize,
    pub memory_rows_to_migrate: usize,
    pub memory_rows_to_quarantine: usize,
    pub collisions: Vec<MigrationCollision>,
    pub issues: Vec<MigrationIssue>,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("persistence migration I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("persistence migration SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("persistence migration JSON decode failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("database schema version {found} is newer than supported version {supported}")]
    FutureVersion { found: u32, supported: u32 },
    #[error("backup path already exists and is not the prepared migration backup: {0}")]
    BackupExists(PathBuf),
    #[error("prepared migration backup is missing: {0}")]
    PreparedBackupMissing(PathBuf),
    #[error("backup path is not valid UTF-8: {0}")]
    NonUtf8Backup(PathBuf),
    #[cfg(test)]
    #[error("injected migration failure: {0}")]
    Injected(&'static str),
}

#[derive(Clone, Debug)]
struct SessionRow {
    row_id: i64,
    legacy_key: String,
    item_json: String,
    created_at: String,
    target: Option<SessionKey>,
    reason: Option<Arc<str>>,
}

#[derive(Clone, Debug)]
struct MemoryRow {
    row_id: i64,
    id: Option<String>,
    legacy_namespace: String,
    body_json: String,
    metadata_json: String,
    preserve: bool,
    target_scope: Option<MemoryScope>,
    reason: Option<Arc<str>>,
}

struct Analysis {
    report: MigrationReport,
    sessions: Vec<SessionRow>,
    memories: Vec<MemoryRow>,
}

/// Inspect a database without changing it.
pub fn inspect_persistence(
    path: impl AsRef<Path>,
    default_agent: &AgentId,
) -> Result<MigrationReport, MigrationError> {
    let path = path.as_ref();
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    Ok(analyze(&conn, path, default_agent)?.report)
}

/// Create a consistent backup and atomically migrate legacy rows.
pub fn migrate_persistence(
    path: impl AsRef<Path>,
    backup_path: impl AsRef<Path>,
    default_agent: &AgentId,
) -> Result<MigrationReport, MigrationError> {
    migrate_inner(path.as_ref(), backup_path.as_ref(), default_agent, None)
}

pub(crate) fn require_current_schema(conn: &Connection) -> Result<(), MemoryError> {
    let version = schema_version(conn).map_err(memory_sqlite_error)?;
    if version > CURRENT_PERSISTENCE_VERSION {
        return Err(MemoryError::Backend(Arc::from(format!(
            "database schema version {version} is newer than this runtime supports ({CURRENT_PERSISTENCE_VERSION})"
        ))));
    }
    if version < CURRENT_PERSISTENCE_VERSION {
        return Err(MemoryError::Backend(Arc::from(format!(
            "database uses legacy schema version {version}; run `agentos-gateway migrate --dry-run` then `agentos-gateway migrate --backup PATH`"
        ))));
    }
    Ok(())
}

fn migrate_inner(
    path: &Path,
    backup_path: &Path,
    default_agent: &AgentId,
    failure: Option<&'static str>,
) -> Result<MigrationReport, MigrationError> {
    let mut conn = Connection::open(path)?;
    let analysis = analyze(&conn, path, default_agent)?;
    if analysis.report.already_current {
        return Ok(analysis.report);
    }

    let prepared_backup = prepared_backup_path(&conn)?;
    if let Some(prepared) = prepared_backup {
        if prepared != backup_path {
            return Err(MigrationError::BackupExists(backup_path.to_path_buf()));
        }
        if !prepared.exists() {
            return Err(MigrationError::PreparedBackupMissing(prepared));
        }
    } else {
        if backup_path.exists() {
            return Err(MigrationError::BackupExists(backup_path.to_path_buf()));
        }
        if let Some(parent) = backup_path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| MigrationError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let backup = backup_path
            .to_str()
            .ok_or_else(|| MigrationError::NonUtf8Backup(backup_path.to_path_buf()))?;
        conn.execute("VACUUM INTO ?1", params![backup])?;
        create_migration_tables(&conn)?;
        conn.execute(
            "INSERT OR REPLACE INTO agentos_schema_migrations \
             (migration_id, target_version, state, backup_path, report_json, started_at, completed_at) \
             VALUES (?1, ?2, 'prepared', ?3, ?4, CURRENT_TIMESTAMP, NULL)",
            params![
                MIGRATION_ID,
                CURRENT_PERSISTENCE_VERSION,
                backup,
                serde_json::to_string(&analysis.report)?,
            ],
        )?;
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    rebuild_sessions(&tx, &analysis.sessions)?;
    fail_if_requested(failure, "after_session_rebuild")?;
    ensure_memory_columns(&tx)?;
    migrate_memory_rows(&tx, &analysis.memories)?;
    fail_if_requested(failure, "disk_full")?;
    tx.pragma_update(None, "user_version", CURRENT_PERSISTENCE_VERSION)?;
    tx.execute(
        "UPDATE agentos_schema_migrations \
         SET state = 'complete', completed_at = CURRENT_TIMESTAMP, report_json = ?2 \
         WHERE migration_id = ?1",
        params![MIGRATION_ID, serde_json::to_string(&analysis.report)?],
    )?;
    tx.commit()?;
    Ok(analysis.report)
}

fn analyze(
    conn: &Connection,
    path: &Path,
    default_agent: &AgentId,
) -> Result<Analysis, MigrationError> {
    let version = schema_version(conn)?;
    if version > CURRENT_PERSISTENCE_VERSION {
        return Err(MigrationError::FutureVersion {
            found: version,
            supported: CURRENT_PERSISTENCE_VERSION,
        });
    }
    if version == CURRENT_PERSISTENCE_VERSION {
        return Ok(Analysis {
            report: MigrationReport {
                database_path: path.to_path_buf(),
                from_version: version,
                to_version: version,
                already_current: true,
                backup_required: false,
                session_rows: table_count(conn, "session_items")?,
                session_rows_to_migrate: 0,
                session_rows_to_quarantine: 0,
                memory_rows: table_count(conn, "memory_records")?,
                memory_rows_to_migrate: 0,
                memory_rows_to_quarantine: 0,
                collisions: Vec::new(),
                issues: Vec::new(),
            },
            sessions: Vec::new(),
            memories: Vec::new(),
        });
    }

    let sessions = analyze_sessions(conn, default_agent)?;
    let principals = principals_by_conversation(&sessions);
    let memories = analyze_memories(conn, &principals)?;
    let mut collisions = session_collisions(&sessions);
    collisions.extend(memory_collisions(&memories));
    let mut issues = session_issues(&sessions);
    issues.extend(memory_issues(&memories));
    let report = MigrationReport {
        database_path: path.to_path_buf(),
        from_version: version,
        to_version: CURRENT_PERSISTENCE_VERSION,
        already_current: false,
        backup_required: true,
        session_rows: sessions.len(),
        session_rows_to_migrate: sessions.iter().filter(|row| row.target.is_some()).count(),
        session_rows_to_quarantine: sessions.iter().filter(|row| row.target.is_none()).count(),
        memory_rows: memories.len(),
        memory_rows_to_migrate: memories
            .iter()
            .filter(|row| !row.preserve && row.target_scope.is_some())
            .count(),
        memory_rows_to_quarantine: memories
            .iter()
            .filter(|row| !row.preserve && row.target_scope.is_none())
            .count(),
        collisions,
        issues,
    };
    Ok(Analysis {
        report,
        sessions,
        memories,
    })
}

fn analyze_sessions(
    conn: &Connection,
    default_agent: &AgentId,
) -> Result<Vec<SessionRow>, MigrationError> {
    if !table_exists(conn, "session_items")? {
        return Ok(Vec::new());
    }
    let key_column = if column_exists(conn, "session_items", "session_key")? {
        "session_key"
    } else {
        "conversation_id"
    };
    let sql = format!(
        "SELECT row_id, {key_column}, item_json, created_at FROM session_items ORDER BY row_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let raw = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = Vec::with_capacity(raw.len());
    let mut cursor = 0;
    while cursor < raw.len() {
        let legacy_key = raw[cursor].1.clone();
        let group_end = raw[cursor..]
            .iter()
            .position(|row| row.1 != legacy_key)
            .map_or(raw.len(), |offset| cursor + offset);
        let group = &raw[cursor..group_end];
        if let Some(typed) = SessionKey::parse_storage_key(&legacy_key) {
            rows.extend(group.iter().map(|row| SessionRow {
                row_id: row.0,
                legacy_key: legacy_key.clone(),
                item_json: row.2.clone(),
                created_at: row.3.clone(),
                target: Some(typed.clone()),
                reason: None,
            }));
        } else {
            rows.extend(analyze_legacy_session_group(group, default_agent)?);
        }
        cursor = group_end;
    }
    rows.sort_by_key(|row| row.row_id);
    Ok(rows)
}

fn analyze_legacy_session_group(
    group: &[(i64, String, String, String)],
    default_agent: &AgentId,
) -> Result<Vec<SessionRow>, MigrationError> {
    let mut output = Vec::with_capacity(group.len());
    let mut start = 0;
    while start < group.len() {
        let end = group[start + 1..]
            .iter()
            .position(|row| item_is_user(&row.2))
            .map_or(group.len(), |offset| start + 1 + offset);
        let principal = derive_turn_principal(&group[start..end], default_agent);
        let (target, reason) = match principal {
            Ok(principal) => (Some(SessionKey::initial(principal)), None),
            Err(reason) => (None, Some(reason)),
        };
        output.extend(group[start..end].iter().map(|row| SessionRow {
            row_id: row.0,
            legacy_key: row.1.clone(),
            item_json: row.2.clone(),
            created_at: row.3.clone(),
            target: target.clone(),
            reason: reason.clone(),
        }));
        start = end;
    }
    Ok(output)
}

fn derive_turn_principal(
    rows: &[(i64, String, String, String)],
    default_agent: &AgentId,
) -> Result<PrincipalKey, Arc<str>> {
    let items = rows
        .iter()
        .map(|row| serde_json::from_str::<Item>(&row.2))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| Arc::from(format!("invalid session item JSON: {err}")))?;
    let user = items
        .iter()
        .find(|item| matches!(item.message.role, agentos_proto::MessageRole::User))
        .ok_or_else(|| Arc::from("turn has no user item carrying channel and sender identity"))?;
    let channel = metadata_value(&user.metadata, "channel_id")
        .ok_or_else(|| Arc::from("user item has no channel_id"))?;
    let conversation =
        metadata_value(&user.metadata, "conversation_id").unwrap_or_else(|| rows[0].1.as_str());
    if conversation != rows[0].1 {
        return Err(Arc::from(
            "stored conversation key disagrees with item metadata",
        ));
    }
    let sender = metadata_value(&user.metadata, "sender").unwrap_or_default();
    let agents = items
        .iter()
        .filter_map(|item| metadata_value(&item.metadata, "active_agent"))
        .collect::<BTreeSet<_>>();
    if agents.len() > 1 {
        return Err(Arc::from("turn contains conflicting active_agent values"));
    }
    let agent = agents
        .first()
        .map_or_else(|| default_agent.clone(), |agent| AgentId::new(*agent));
    Ok(PrincipalKey::v1(
        agent,
        ChannelId::new(channel),
        ConversationId::new(conversation),
        if sender.is_empty() {
            SenderIdentity::Unattributed
        } else {
            SenderIdentity::identified(sender)
        },
    ))
}

fn analyze_memories(
    conn: &Connection,
    principals: &BTreeMap<String, BTreeSet<PrincipalKey>>,
) -> Result<Vec<MemoryRow>, MigrationError> {
    if !table_exists(conn, "memory_records")? {
        return Ok(Vec::new());
    }
    let optional = |column: &str| -> Result<String, MigrationError> {
        Ok(if column_exists(conn, "memory_records", column)? {
            column.to_owned()
        } else {
            format!("NULL AS {column}")
        })
    };
    let sql = format!(
        "SELECT row_id, {}, namespace, body_json, metadata_json, {}, {}, {}, {}, {}, {}, {} \
         FROM memory_records ORDER BY row_id",
        optional("id")?,
        optional("store")?,
        optional("owner_kind")?,
        optional("owner_id")?,
        optional("visibility")?,
        optional("domain")?,
        optional("source_agent_id")?,
        optional("source_task_id")?,
    );
    let mut stmt = conn.prepare(&sql)?;
    let raw = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<String>>(11)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    raw.into_iter()
        .map(|row| {
            let metadata: BTreeMap<Arc<str>, Value> = serde_json::from_str(&row.4)?;
            let preserve = namespace_is_v1(&row.2, row.6.as_deref(), row.7.as_deref());
            let resolved = resolve_memory_scope(
                row.5.as_deref(),
                row.6.as_deref(),
                row.7.as_deref(),
                row.8.as_deref(),
                row.9.as_deref(),
                row.10.as_deref(),
                row.11.as_deref(),
                &metadata,
                principals,
            );
            let (target_scope, reason) = if preserve {
                (None, None)
            } else {
                match resolved {
                    Ok(scope) => (Some(scope), None),
                    Err(reason) => (None, Some(reason)),
                }
            };
            Ok(MemoryRow {
                row_id: row.0,
                id: row.1,
                legacy_namespace: row.2,
                body_json: row.3,
                metadata_json: row.4,
                preserve,
                target_scope,
                reason,
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn resolve_memory_scope(
    store: Option<&str>,
    owner_kind: Option<&str>,
    owner_id: Option<&str>,
    visibility: Option<&str>,
    domain: Option<&str>,
    source_agent: Option<&str>,
    source_task: Option<&str>,
    metadata: &BTreeMap<Arc<str>, Value>,
    principals: &BTreeMap<String, BTreeSet<PrincipalKey>>,
) -> Result<MemoryScope, Arc<str>> {
    let store = parse_store(store.ok_or_else(|| Arc::from("memory row has no store"))?)?;
    let visibility =
        parse_visibility(visibility.ok_or_else(|| Arc::from("memory row has no visibility"))?)?;
    let owner_kind = owner_kind.ok_or_else(|| Arc::from("memory row has no owner_kind"))?;
    let owner_id = owner_id.unwrap_or_default();
    let conversation = metadata
        .get("conversation_id")
        .and_then(Value::as_str)
        .unwrap_or(owner_id);
    let candidates = principals.get(conversation);
    let owner = match owner_kind {
        "principal" => metadata
            .get("owner")
            .cloned()
            .and_then(|value| serde_json::from_value::<MemoryOwner>(value).ok())
            .or_else(|| PrincipalKey::parse_storage_key(owner_id).map(MemoryOwner::Principal))
            .ok_or_else(|| Arc::from("principal memory has no decodable owner"))?,
        "conversation" => MemoryOwner::Principal(unique_principal(candidates)?),
        "user" => {
            let matching = candidates
                .into_iter()
                .flatten()
                .filter(|principal| principal_sender_matches(principal, owner_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            MemoryOwner::Principal(unique_principal(Some(&matching))?)
        }
        "agent" => MemoryOwner::Agent(AgentId::new(
            source_agent.ok_or_else(|| Arc::from("agent memory has no source_agent_id"))?,
        )),
        "task" => MemoryOwner::Task(agentos_proto::TaskId::new(
            source_task.ok_or_else(|| Arc::from("task memory has no source_task_id"))?,
        )),
        "shared" => MemoryOwner::Shared,
        other => return Err(Arc::from(format!("unknown memory owner_kind '{other}'"))),
    };
    let domain = match domain {
        None | Some("") | Some("general") => None,
        Some(value) if value.contains('_') => {
            return Err(Arc::from(
                "legacy domain contains '_' and may have collided with a '/' component",
            ));
        }
        Some(value) => Some(Arc::from(value)),
    };
    Ok(MemoryScope::new(store, owner, visibility, domain))
}

fn unique_principal(candidates: Option<&BTreeSet<PrincipalKey>>) -> Result<PrincipalKey, Arc<str>> {
    let candidates = candidates.ok_or_else(|| Arc::from("no session principal matches memory"))?;
    if candidates.len() != 1 {
        return Err(Arc::from(format!(
            "memory maps to {} session principals",
            candidates.len()
        )));
    }
    candidates
        .first()
        .cloned()
        .ok_or_else(|| Arc::from("no session principal matches memory"))
}

fn rebuild_sessions(
    tx: &rusqlite::Transaction<'_>,
    rows: &[SessionRow],
) -> Result<(), MigrationError> {
    create_quarantine_tables(tx)?;
    tx.execute_batch(
        "DROP TABLE IF EXISTS session_items_v1; \
         CREATE TABLE session_items_v1 ( \
           row_id INTEGER PRIMARY KEY AUTOINCREMENT, session_key TEXT NOT NULL, \
           ordinal INTEGER NOT NULL, item_json TEXT NOT NULL, \
           created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(session_key, ordinal));",
    )?;
    let mut ordinals = BTreeMap::<String, i64>::new();
    for row in rows {
        if let Some(target) = &row.target {
            let key = target.storage_key();
            let ordinal = ordinals.entry(key.clone()).or_default();
            tx.execute(
                "INSERT INTO session_items_v1 (session_key, ordinal, item_json, created_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![key, *ordinal, row.item_json, row.created_at],
            )?;
            *ordinal += 1;
        } else {
            tx.execute(
                "INSERT OR REPLACE INTO legacy_session_quarantine \
                 (legacy_row_id, legacy_key, item_json, reason) VALUES (?1, ?2, ?3, ?4)",
                params![
                    row.row_id,
                    row.legacy_key,
                    row.item_json,
                    row.reason
                        .as_deref()
                        .unwrap_or("unresolved legacy session identity")
                ],
            )?;
        }
    }
    tx.execute_batch(
        "DROP TABLE session_items; \
         ALTER TABLE session_items_v1 RENAME TO session_items; \
         CREATE INDEX idx_session_items_session_ordinal \
           ON session_items(session_key, ordinal);",
    )?;
    Ok(())
}

fn migrate_memory_rows(
    tx: &rusqlite::Transaction<'_>,
    rows: &[MemoryRow],
) -> Result<(), MigrationError> {
    create_quarantine_tables(tx)?;
    let fts_exists = table_exists(tx, "memory_records_fts")?;
    for row in rows {
        if row.preserve {
            continue;
        }
        let Some(scope) = &row.target_scope else {
            tx.execute(
                "INSERT OR REPLACE INTO legacy_memory_quarantine \
                 (legacy_row_id, record_id, namespace, body_json, metadata_json, reason) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    row.row_id,
                    row.id,
                    row.legacy_namespace,
                    row.body_json,
                    row.metadata_json,
                    row.reason
                        .as_deref()
                        .unwrap_or("unresolved legacy memory identity")
                ],
            )?;
            if fts_exists {
                if let Some(id) = &row.id {
                    tx.execute("DELETE FROM memory_records_fts WHERE id = ?1", params![id])?;
                }
            }
            tx.execute(
                "DELETE FROM memory_records WHERE row_id = ?1",
                params![row.row_id],
            )?;
            continue;
        };
        let mut metadata: BTreeMap<Arc<str>, Value> = serde_json::from_str(&row.metadata_json)?;
        metadata.insert(
            Arc::from("owner_kind"),
            Value::String(scope.owner.kind().to_owned()),
        );
        metadata.insert(
            Arc::from("owner_id"),
            Value::String(scope.owner.metadata_id()),
        );
        metadata.insert(Arc::from("owner"), serde_json::to_value(&scope.owner)?);
        metadata.insert(Arc::from("domain"), Value::String(scope.domain_name()));
        let namespace = scope.namespace();
        tx.execute(
            "UPDATE memory_records SET namespace = ?1, metadata_json = ?2, owner_kind = ?3, \
             owner_id = ?4, domain = ?5 WHERE row_id = ?6",
            params![
                namespace.as_str(),
                serde_json::to_string(&metadata)?,
                scope.owner.kind(),
                scope.owner.metadata_id(),
                scope.domain_name(),
                row.row_id,
            ],
        )?;
        if fts_exists {
            if let Some(id) = &row.id {
                tx.execute(
                    "UPDATE memory_records_fts SET namespace = ?1 WHERE id = ?2",
                    params![namespace.as_str(), id],
                )?;
            }
        }
    }
    Ok(())
}

fn create_migration_tables(conn: &Connection) -> Result<(), MigrationError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS agentos_schema_migrations ( \
           migration_id TEXT PRIMARY KEY, target_version INTEGER NOT NULL, state TEXT NOT NULL, \
           backup_path TEXT NOT NULL, report_json TEXT NOT NULL, started_at TEXT NOT NULL, completed_at TEXT);",
    )?;
    Ok(())
}

fn ensure_memory_columns(conn: &Connection) -> Result<(), MigrationError> {
    if !table_exists(conn, "memory_records")? {
        return Ok(());
    }
    for (name, definition) in [
        ("store", "TEXT"),
        ("owner_kind", "TEXT"),
        ("owner_id", "TEXT"),
        ("visibility", "TEXT"),
        ("domain", "TEXT"),
        ("source_agent_id", "TEXT"),
        ("source_task_id", "TEXT"),
    ] {
        if !column_exists(conn, "memory_records", name)? {
            conn.execute(
                &format!("ALTER TABLE memory_records ADD COLUMN {name} {definition}"),
                [],
            )?;
        }
    }
    Ok(())
}

fn create_quarantine_tables(conn: &Connection) -> Result<(), MigrationError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS legacy_session_quarantine ( \
           legacy_row_id INTEGER PRIMARY KEY, legacy_key TEXT NOT NULL, item_json TEXT NOT NULL, \
           reason TEXT NOT NULL, quarantined_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP); \
         CREATE TABLE IF NOT EXISTS legacy_memory_quarantine ( \
           legacy_row_id INTEGER PRIMARY KEY, record_id TEXT, namespace TEXT NOT NULL, \
           body_json TEXT NOT NULL, metadata_json TEXT NOT NULL, reason TEXT NOT NULL, \
           quarantined_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);",
    )?;
    Ok(())
}

fn prepared_backup_path(conn: &Connection) -> Result<Option<PathBuf>, MigrationError> {
    if !table_exists(conn, "agentos_schema_migrations")? {
        return Ok(None);
    }
    Ok(conn
        .query_row(
            "SELECT backup_path FROM agentos_schema_migrations \
             WHERE migration_id = ?1 AND state = 'prepared'",
            params![MIGRATION_ID],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(PathBuf::from))
}

fn principals_by_conversation(rows: &[SessionRow]) -> BTreeMap<String, BTreeSet<PrincipalKey>> {
    let mut output = BTreeMap::<String, BTreeSet<PrincipalKey>>::new();
    for row in rows {
        if let Some(SessionKey { principal, .. }) = &row.target {
            let PrincipalKey::V1(value) = principal;
            output
                .entry(value.conversation_id.as_str().to_owned())
                .or_default()
                .insert(principal.clone());
        }
    }
    output
}

fn fail_if_requested(
    failure: Option<&'static str>,
    point: &'static str,
) -> Result<(), MigrationError> {
    #[cfg(test)]
    if failure == Some(point) {
        return Err(MigrationError::Injected(point));
    }
    let _ = (failure, point);
    Ok(())
}

#[cfg(test)]
pub(crate) fn migrate_with_failure(
    path: &Path,
    backup_path: &Path,
    default_agent: &AgentId,
    failure: &'static str,
) -> Result<MigrationReport, MigrationError> {
    migrate_inner(path, backup_path, default_agent, Some(failure))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_interfaces::session::Item;
    use agentos_proto::{Message, MessageRole};
    use rusqlite::params;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn tempdir() -> TestDir {
        let nonce = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("agentos-migration-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&path).expect("tempdir creates");
        TestDir(path)
    }

    fn legacy_database(path: &Path) -> Connection {
        let conn = Connection::open(path).expect("legacy database opens");
        conn.execute_batch(
            "CREATE TABLE session_items ( \
               row_id INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id TEXT NOT NULL, \
               ordinal INTEGER NOT NULL, item_json TEXT NOT NULL, \
               created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, \
               UNIQUE(conversation_id, ordinal)); \
             CREATE TABLE memory_records ( \
               row_id INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE, namespace TEXT NOT NULL, \
               body_json TEXT NOT NULL, metadata_json TEXT NOT NULL, updated_at TEXT, \
               last_accessed_at TEXT, access_count INTEGER NOT NULL DEFAULT 0, \
               status TEXT NOT NULL DEFAULT 'active', store TEXT, owner_kind TEXT, owner_id TEXT, \
               visibility TEXT, domain TEXT, source_run_id TEXT, source_task_id TEXT, \
               source_agent_id TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP); \
             CREATE VIRTUAL TABLE memory_records_fts \
               USING fts5(id UNINDEXED, namespace UNINDEXED, body_text, metadata_text); \
             PRAGMA user_version = 0;",
        )
        .expect("legacy schema creates");
        conn
    }

    fn item(role: MessageRole, text: &str, metadata: Value) -> String {
        let metadata = serde_json::from_value(metadata).expect("metadata map is valid");
        serde_json::to_string(&Item {
            message: Message::text(role, text),
            metadata,
        })
        .expect("item serializes")
    }

    fn insert_turn(
        conn: &Connection,
        conversation: &str,
        ordinal: i64,
        channel: &str,
        sender: &str,
        agent: &str,
    ) {
        let user = item(
            MessageRole::User,
            "hello",
            json!({
                "channel_id": channel,
                "conversation_id": conversation,
                "sender": sender,
            }),
        );
        let assistant = item(
            MessageRole::Assistant,
            "hi",
            json!({ "active_agent": agent }),
        );
        conn.execute(
            "INSERT INTO session_items (conversation_id, ordinal, item_json) VALUES (?1, ?2, ?3)",
            params![conversation, ordinal, user],
        )
        .expect("user row inserts");
        conn.execute(
            "INSERT INTO session_items (conversation_id, ordinal, item_json) VALUES (?1, ?2, ?3)",
            params![conversation, ordinal + 1, assistant],
        )
        .expect("assistant row inserts");
    }

    fn insert_conversation_memory(conn: &Connection, conversation: &str) {
        let namespace = format!("private/conversation/{conversation}/episodic/general");
        let metadata = json!({
            "store": "episodic",
            "owner_kind": "conversation",
            "owner_id": conversation,
            "visibility": "private",
            "domain": "general",
            "conversation_id": conversation,
            "source_agent_id": "agent-a",
            "source_task_id": "task-a",
        })
        .to_string();
        conn.execute(
            "INSERT INTO memory_records \
             (id, namespace, body_json, metadata_json, store, owner_kind, owner_id, visibility, \
              domain, source_agent_id, source_task_id) \
             VALUES ('memory-1', ?1, '{\"text\":\"remember\"}', ?2, 'episodic', \
                     'conversation', ?3, 'private', 'general', 'agent-a', 'task-a')",
            params![namespace, metadata, conversation],
        )
        .expect("memory row inserts");
        conn.execute(
            "INSERT INTO memory_records_fts (id, namespace, body_text, metadata_text) \
             VALUES ('memory-1', ?1, 'remember', ?2)",
            params![namespace, metadata],
        )
        .expect("memory FTS row inserts");
    }

    #[test]
    fn dry_run_reports_cross_principal_collision_without_mutating() {
        let dir = tempdir();
        let database = dir.path().join("legacy.sqlite");
        let conn = legacy_database(&database);
        insert_turn(&conn, "42", 0, "telegram", "alice", "agent-a");
        insert_turn(&conn, "42", 2, "feishu", "alice", "agent-a");
        insert_turn(&conn, "42", 4, "telegram", "alice", "agent-b");
        drop(conn);

        let report = inspect_persistence(&database, &AgentId::new("fallback")).expect("inspect");
        assert_eq!(report.session_rows_to_migrate, 6);
        assert_eq!(report.session_rows_to_quarantine, 0);
        assert_eq!(report.collisions.len(), 1);
        assert_eq!(report.collisions[0].target_keys.len(), 3);

        let conn = Connection::open(&database).expect("database reopens");
        assert_eq!(schema_version(&conn).expect("version reads"), 0);
        assert!(!table_exists(&conn, "agentos_schema_migrations").expect("table check"));
    }

    #[test]
    fn migration_splits_sessions_migrates_memory_and_preserves_backup() {
        let dir = tempdir();
        let database = dir.path().join("legacy.sqlite");
        let backup = dir.path().join("legacy.backup.sqlite");
        let conn = legacy_database(&database);
        insert_turn(&conn, "42", 0, "telegram", "alice", "agent-a");
        insert_turn(&conn, "42", 2, "feishu", "alice", "agent-a");
        insert_turn(&conn, "solo", 0, "telegram", "bob", "agent-a");
        insert_conversation_memory(&conn, "solo");
        drop(conn);

        let report = migrate_persistence(&database, &backup, &AgentId::new("fallback"))
            .expect("migration succeeds");
        assert_eq!(report.session_rows_to_migrate, 6);
        assert_eq!(report.memory_rows_to_migrate, 1);
        assert!(backup.exists());

        let migrated = Connection::open(&database).expect("migrated database opens");
        assert_eq!(schema_version(&migrated).expect("version reads"), 1);
        assert!(column_exists(&migrated, "session_items", "session_key").expect("column check"));
        assert_eq!(
            migrated
                .query_row(
                    "SELECT COUNT(DISTINCT session_key) FROM session_items",
                    [],
                    |row| row.get::<_, usize>(0),
                )
                .expect("session count"),
            3
        );
        let (owner_kind, namespace): (String, String) = migrated
            .query_row(
                "SELECT owner_kind, namespace FROM memory_records WHERE id = 'memory-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("memory migrated");
        assert_eq!(owner_kind, "principal");
        assert!(namespace.starts_with("private/principal/pk1_"));

        let original = Connection::open(&backup).expect("backup opens");
        assert_eq!(schema_version(&original).expect("version reads"), 0);
        assert!(column_exists(&original, "session_items", "conversation_id")
            .expect("legacy column check"));
        assert_eq!(table_count(&original, "session_items").expect("count"), 6);
    }

    #[test]
    fn crash_and_disk_full_failures_roll_back_and_restart_from_prepared_marker() {
        for failure in ["after_session_rebuild", "disk_full"] {
            let dir = tempdir();
            let database = dir.path().join(format!("{failure}.sqlite"));
            let backup = dir.path().join(format!("{failure}.backup.sqlite"));
            let conn = legacy_database(&database);
            insert_turn(&conn, "solo", 0, "telegram", "bob", "agent-a");
            drop(conn);

            let error =
                migrate_with_failure(&database, &backup, &AgentId::new("fallback"), failure)
                    .expect_err("failure is injected");
            assert!(matches!(error, MigrationError::Injected(_)));

            let conn = Connection::open(&database).expect("database reopens");
            assert_eq!(schema_version(&conn).expect("version reads"), 0);
            assert!(column_exists(&conn, "session_items", "conversation_id")
                .expect("legacy table remains"));
            assert_eq!(table_count(&conn, "session_items").expect("count"), 2);
            assert_eq!(
                prepared_backup_path(&conn).expect("marker reads"),
                Some(backup.clone())
            );
            drop(conn);

            migrate_persistence(&database, &backup, &AgentId::new("fallback"))
                .expect("restart completes");
            let conn = Connection::open(&database).expect("database reopens");
            assert_eq!(schema_version(&conn).expect("version reads"), 1);
            assert_eq!(table_count(&conn, "session_items").expect("count"), 2);
        }
    }

    #[test]
    fn ambiguous_memory_is_quarantined_instead_of_merged() {
        let dir = tempdir();
        let database = dir.path().join("ambiguous.sqlite");
        let backup = dir.path().join("ambiguous.backup.sqlite");
        let conn = legacy_database(&database);
        insert_turn(&conn, "42", 0, "telegram", "alice", "agent-a");
        insert_turn(&conn, "42", 2, "feishu", "alice", "agent-a");
        insert_conversation_memory(&conn, "42");
        drop(conn);

        let report = migrate_persistence(&database, &backup, &AgentId::new("fallback"))
            .expect("migration succeeds");
        assert_eq!(report.memory_rows_to_quarantine, 1);
        assert!(!report.issues.is_empty());
        let conn = Connection::open(&database).expect("database opens");
        assert_eq!(table_count(&conn, "memory_records").expect("count"), 0);
        assert_eq!(
            table_count(&conn, "legacy_memory_quarantine").expect("count"),
            1
        );
    }

    #[test]
    fn minimal_legacy_memory_shape_is_reported_and_quarantined_without_fts() {
        let dir = tempdir();
        let database = dir.path().join("minimal.sqlite");
        let backup = dir.path().join("minimal.backup.sqlite");
        let conn = Connection::open(&database).expect("database opens");
        conn.execute_batch(
            "CREATE TABLE session_items ( \
               row_id INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id TEXT NOT NULL, \
               ordinal INTEGER NOT NULL, item_json TEXT NOT NULL, \
               created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, \
               UNIQUE(conversation_id, ordinal)); \
             CREATE TABLE memory_records ( \
               row_id INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE, namespace TEXT NOT NULL, \
               body_json TEXT NOT NULL, metadata_json TEXT NOT NULL, \
               created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP); \
             INSERT INTO memory_records (id, namespace, body_json, metadata_json) \
               VALUES ('old', 'memory:old', '{}', '{}');",
        )
        .expect("minimal schema creates");
        drop(conn);

        let report = migrate_persistence(&database, &backup, &AgentId::new("agent-a"))
            .expect("minimal schema migrates");
        assert_eq!(report.memory_rows_to_quarantine, 1);
        let conn = Connection::open(&database).expect("database opens");
        assert_eq!(schema_version(&conn).expect("version reads"), 1);
        assert_eq!(
            table_count(&conn, "legacy_memory_quarantine").expect("count"),
            1
        );
    }
}
