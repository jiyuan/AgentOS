use super::{MemoryRow, MigrationCollision, MigrationIssue, SessionRow};
use crate::memory::{MemoryStore, MemoryVisibility};
use agentos_interfaces::session::Item;
use agentos_proto::{PrincipalKey, SenderIdentity, SessionKey};
use rusqlite::{params, Connection};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(super) fn session_collisions(rows: &[SessionRow]) -> Vec<MigrationCollision> {
    let mut groups = BTreeMap::<String, (BTreeSet<String>, usize)>::new();
    for row in rows {
        if SessionKey::parse_storage_key(&row.legacy_key).is_none() {
            let entry = groups.entry(row.legacy_key.clone()).or_default();
            entry.1 += 1;
            if let Some(target) = &row.target {
                entry.0.insert(target.storage_key());
            }
        }
    }
    groups
        .into_iter()
        .filter(|(_, (targets, _))| targets.len() > 1)
        .map(|(legacy_key, (targets, row_count))| MigrationCollision {
            kind: Arc::from("session_principal_split"),
            legacy_key: Arc::from(legacy_key),
            target_keys: targets.into_iter().map(Arc::from).collect(),
            row_count,
        })
        .collect()
}

pub(super) fn memory_collisions(rows: &[MemoryRow]) -> Vec<MigrationCollision> {
    let mut groups = BTreeMap::<String, (BTreeSet<String>, usize)>::new();
    for row in rows {
        let entry = groups.entry(row.legacy_namespace.clone()).or_default();
        entry.1 += 1;
        if row.preserve {
            entry.0.insert(row.legacy_namespace.clone());
        } else if let Some(scope) = &row.target_scope {
            entry.0.insert(scope.namespace().as_str().to_owned());
        }
    }
    groups
        .into_iter()
        .filter(|(_, (targets, _))| targets.len() > 1)
        .map(|(legacy_key, (targets, row_count))| MigrationCollision {
            kind: Arc::from("memory_namespace_split"),
            legacy_key: Arc::from(legacy_key),
            target_keys: targets.into_iter().map(Arc::from).collect(),
            row_count,
        })
        .collect()
}

pub(super) fn session_issues(rows: &[SessionRow]) -> Vec<MigrationIssue> {
    grouped_issues(
        rows.iter().filter_map(|row| {
            row.reason
                .as_ref()
                .map(|reason| (row.legacy_key.as_str(), reason.as_ref()))
        }),
        "session",
    )
}

pub(super) fn memory_issues(rows: &[MemoryRow]) -> Vec<MigrationIssue> {
    grouped_issues(
        rows.iter().filter_map(|row| {
            row.reason
                .as_ref()
                .map(|reason| (row.legacy_namespace.as_str(), reason.as_ref()))
        }),
        "memory",
    )
}

fn grouped_issues<'a>(
    issues: impl Iterator<Item = (&'a str, &'a str)>,
    kind: &'static str,
) -> Vec<MigrationIssue> {
    let mut groups = BTreeMap::<(String, String), usize>::new();
    for (key, reason) in issues {
        *groups
            .entry((key.to_owned(), reason.to_owned()))
            .or_default() += 1;
    }
    groups
        .into_iter()
        .map(|((legacy_key, reason), row_count)| MigrationIssue {
            kind: Arc::from(kind),
            legacy_key: Arc::from(legacy_key),
            row_count,
            reason: Arc::from(reason),
        })
        .collect()
}

pub(super) fn parse_store(value: &str) -> Result<MemoryStore, Arc<str>> {
    match value {
        "working" => Ok(MemoryStore::Working),
        "episodic" => Ok(MemoryStore::Episodic),
        "semantic" => Ok(MemoryStore::Semantic),
        "procedural" => Ok(MemoryStore::Procedural),
        "audit" => Ok(MemoryStore::Audit),
        other => Err(Arc::from(format!("unknown memory store '{other}'"))),
    }
}

pub(super) fn parse_visibility(value: &str) -> Result<MemoryVisibility, Arc<str>> {
    match value {
        "private" => Ok(MemoryVisibility::Private),
        "shared" => Ok(MemoryVisibility::Shared),
        "public" => Ok(MemoryVisibility::Public),
        other => Err(Arc::from(format!("unknown memory visibility '{other}'"))),
    }
}

pub(super) fn principal_sender_matches(principal: &PrincipalKey, legacy_owner_id: &str) -> bool {
    let PrincipalKey::V1(principal) = principal;
    match &principal.sender {
        SenderIdentity::Identified(sender) => {
            sender.as_str().trim().replace('/', "_") == legacy_owner_id
        }
        SenderIdentity::Unattributed => legacy_owner_id.is_empty(),
    }
}

pub(super) fn namespace_is_v1(
    namespace: &str,
    owner_kind: Option<&str>,
    owner_id: Option<&str>,
) -> bool {
    let parts = namespace.split('/').collect::<Vec<_>>();
    if parts.len() != 5 {
        return false;
    }
    let encoded_component = |component: &str| {
        component
            .strip_prefix("id_")
            .and_then(agentos_proto::decode_base64url)
            .is_some()
    };
    let owner_is_v1 = match owner_kind {
        Some("principal") => owner_id
            .and_then(PrincipalKey::parse_storage_key)
            .is_some_and(|principal| parts[2] == principal.storage_key()),
        Some("agent" | "task" | "user") => encoded_component(parts[2]),
        Some("shared") => parts[2] == "global",
        _ => false,
    };
    owner_is_v1 && (parts[4] == "general" || encoded_component(parts[4]))
}

pub(super) fn item_is_user(item_json: &str) -> bool {
    serde_json::from_str::<Item>(item_json)
        .is_ok_and(|item| matches!(item.message.role, agentos_proto::MessageRole::User))
}

pub(super) fn metadata_value<'a>(
    metadata: &'a BTreeMap<Arc<str>, Value>,
    key: &str,
) -> Option<&'a str> {
    metadata.get(key).and_then(Value::as_str)
}

pub(super) fn schema_version(conn: &Connection) -> Result<u32, rusqlite::Error> {
    conn.query_row("PRAGMA user_version", [], |row| row.get(0))
}

pub(super) fn table_exists(conn: &Connection, table: &str) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        params![table],
        |row| row.get(0),
    )
}

pub(super) fn column_exists(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<bool, rusqlite::Error> {
    let sql = format!("PRAGMA table_info({table})");
    let mut stmt = conn.prepare(&sql)?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|candidate| candidate == column))
}

pub(super) fn table_count(conn: &Connection, table: &str) -> Result<usize, rusqlite::Error> {
    if !table_exists(conn, table)? {
        return Ok(0);
    }
    let sql = format!("SELECT COUNT(*) FROM {table}");
    conn.query_row(&sql, [], |row| row.get(0))
}
