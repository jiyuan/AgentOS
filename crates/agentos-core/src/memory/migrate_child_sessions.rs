//! Rekeying delegated session rows written before `child.v1` (`ID-003`).
//!
//! Old delegated conversation keys omitted the parent agent, parent channel,
//! and child policy. Those values cannot be reconstructed from the key. A row
//! is therefore migrated only when its persisted child identity source names
//! the complete tuple; anything missing, malformed, conflicting, or already
//! claimed is reported as quarantined and never merged by inference.

use super::migrate_sessions::{session_schema, SessionSchema};
use super::sqlite::SqliteStore;
use crate::subagents::{ChildIdentitySource, CHILD_IDENTITY_SOURCE_KEY};
use agentos_interfaces::memory::MemoryError;
use agentos_proto::{ConversationPrincipal, Principal};
use rusqlite::params;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One pre-versioned delegated conversation that can be rekeyed exactly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildSessionRewrite {
    pub from: String,
    pub to: String,
    pub items: usize,
}

/// One pre-versioned delegated conversation that cannot be resolved safely.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantinedChildSession {
    pub key: String,
    pub items: usize,
    pub reason: Arc<str>,
}

/// What the delegated-session migration would do.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChildSessionPlan {
    pub rewrites: Vec<ChildSessionRewrite>,
    pub quarantined: Vec<QuarantinedChildSession>,
}

impl ChildSessionPlan {
    pub fn is_empty(&self) -> bool {
        self.rewrites.is_empty() && self.quarantined.is_empty()
    }

    pub fn items_to_move(&self) -> usize {
        self.rewrites.iter().map(|rewrite| rewrite.items).sum()
    }

    pub fn report(&self) -> String {
        let mut out = String::new();
        if !self.quarantined.is_empty() {
            out.push_str(&format!(
                "{} legacy child conversation(s) are quarantined and will NOT be migrated:\n",
                self.quarantined.len()
            ));
            for entry in &self.quarantined {
                out.push_str(&format!(
                    "  {} ({} item(s)): {}\n",
                    entry.key, entry.items, entry.reason
                ));
            }
            out.push('\n');
        }
        out.push_str(&format!(
            "{} legacy child conversation(s) holding {} session item(s) can be rekeyed:\n",
            self.rewrites.len(),
            self.items_to_move()
        ));
        for rewrite in &self.rewrites {
            out.push_str(&format!(
                "  {} -> {} ({} item(s))\n",
                rewrite.from, rewrite.to, rewrite.items
            ));
        }
        out
    }
}

/// Inspect current-schema session rows for pre-versioned delegated keys.
pub fn plan(store: &SqliteStore) -> Result<ChildSessionPlan, MemoryError> {
    if session_schema(store)? != SessionSchema::Current {
        return Ok(ChildSessionPlan::default());
    }
    let conn = store.memory_conn()?;
    let mut statement = conn
        .prepare("SELECT conversation_key, COUNT(*) FROM session_items GROUP BY conversation_key")
        .map_err(super::memory_sqlite_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as usize,
            ))
        })
        .map_err(super::memory_sqlite_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(super::memory_sqlite_error)?;

    let claimed = rows
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut plan = ChildSessionPlan::default();
    let mut candidates = Vec::new();
    for (key, items) in rows {
        let Some(principal) = Principal::from_storage_name(&key) else {
            continue;
        };
        if principal.sender.is_some()
            || !principal.channel.as_str().starts_with("subagent:")
            || principal.conversation.as_str().starts_with("child.v1.")
        {
            continue;
        }

        match source_for_key(&conn, &key) {
            Ok(source) => {
                let expected_channel = format!("subagent:{}", source.child_agent().as_str());
                if source.child_agent() != &principal.agent
                    || principal.channel.as_str() != expected_channel
                {
                    plan.quarantined.push(QuarantinedChildSession {
                        key,
                        items,
                        reason: Arc::from(
                            "persisted child identity does not match the stored agent/channel",
                        ),
                    });
                    continue;
                }
                let Some(conversation) = source.conversation_id() else {
                    plan.quarantined.push(QuarantinedChildSession {
                        key,
                        items,
                        reason: Arc::from("unsupported or sender-qualified child identity source"),
                    });
                    continue;
                };
                let target = ConversationPrincipal::new(
                    principal.agent.clone(),
                    principal.channel.clone(),
                    conversation,
                )
                .storage_name();
                if claimed.contains(&target) {
                    plan.quarantined.push(QuarantinedChildSession {
                        key,
                        items,
                        reason: Arc::from(format!(
                            "target key '{target}' already contains session rows"
                        )),
                    });
                    continue;
                }
                candidates.push(ChildSessionRewrite {
                    from: key,
                    to: target,
                    items,
                });
            }
            Err(reason) => plan
                .quarantined
                .push(QuarantinedChildSession { key, items, reason }),
        }
    }

    let mut targets: BTreeMap<String, Vec<ChildSessionRewrite>> = BTreeMap::new();
    for candidate in candidates {
        targets
            .entry(candidate.to.clone())
            .or_default()
            .push(candidate);
    }
    for (target, mut entries) in targets {
        if entries.len() == 1 {
            plan.rewrites.push(entries.remove(0));
        } else {
            for entry in entries {
                plan.quarantined.push(QuarantinedChildSession {
                    key: entry.from,
                    items: entry.items,
                    reason: Arc::from(format!(
                        "multiple legacy keys claim the same target '{target}'"
                    )),
                });
            }
        }
    }
    plan.rewrites
        .sort_by(|left, right| left.from.cmp(&right.from));
    plan.quarantined
        .sort_by(|left, right| left.key.cmp(&right.key));
    Ok(plan)
}

fn source_for_key(conn: &rusqlite::Connection, key: &str) -> Result<ChildIdentitySource, Arc<str>> {
    let mut statement = conn
        .prepare("SELECT item_json FROM session_items WHERE conversation_key = ?1 ORDER BY ordinal")
        .map_err(|error| Arc::from(format!("could not inspect rows: {error}")))?;
    let rows = statement
        .query_map(params![key], |row| row.get::<_, String>(0))
        .map_err(|error| Arc::from(format!("could not inspect rows: {error}")))?;
    let mut found: Option<ChildIdentitySource> = None;
    for row in rows {
        let item: serde_json::Value = serde_json::from_str(
            &row.map_err(|error| Arc::from(format!("could not read a row: {error}")))?,
        )
        .map_err(|error| Arc::from(format!("malformed session item JSON: {error}")))?;
        let Some(value) = item
            .get("metadata")
            .and_then(|metadata| metadata.get(CHILD_IDENTITY_SOURCE_KEY))
        else {
            continue;
        };
        let source: ChildIdentitySource = serde_json::from_value(value.clone())
            .map_err(|error| Arc::from(format!("malformed child identity source: {error}")))?;
        if found.as_ref().is_some_and(|prior| prior != &source) {
            return Err(Arc::from(
                "session rows contain conflicting child identity sources",
            ));
        }
        found = Some(source);
    }
    found.ok_or_else(|| Arc::from("complete child identity source is not persisted"))
}

/// Rekey every unambiguous row in one transaction. Quarantined rows remain
/// under their legacy keys and are never folded into another conversation.
pub fn apply(store: &SqliteStore, plan: &ChildSessionPlan) -> Result<usize, MemoryError> {
    if plan.rewrites.is_empty() {
        return Ok(0);
    }
    let mut conn = store.memory_conn()?;
    let transaction = conn.transaction().map_err(super::memory_sqlite_error)?;
    let mut moved = 0usize;
    for rewrite in &plan.rewrites {
        let occupied: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM session_items WHERE conversation_key = ?1",
                params![rewrite.to],
                |row| row.get(0),
            )
            .map_err(super::memory_sqlite_error)?;
        if occupied != 0 {
            return Err(MemoryError::Backend(Arc::from(format!(
                "refusing child session migration: target '{}' became occupied after planning",
                rewrite.to
            ))));
        }
        moved += transaction
            .execute(
                "UPDATE session_items SET conversation_key = ?1 WHERE conversation_key = ?2",
                params![rewrite.to, rewrite.from],
            )
            .map_err(super::memory_sqlite_error)?;

        let target = Principal::from_storage_name(&rewrite.to).ok_or_else(|| {
            MemoryError::Backend(Arc::from("planned child target is not a principal key"))
        })?;
        let mut epochs = transaction
            .prepare("SELECT row_id, principal FROM session_epochs WHERE conversation_key = ?1")
            .map_err(super::memory_sqlite_error)?;
        let rows = epochs
            .query_map(params![rewrite.from], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(super::memory_sqlite_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(super::memory_sqlite_error)?;
        drop(epochs);
        for (row_id, encoded) in rows {
            let mut principal = Principal::from_storage_name(&encoded).ok_or_else(|| {
                MemoryError::Backend(Arc::from(format!(
                    "legacy child epoch contains invalid principal '{encoded}'"
                )))
            })?;
            if principal.conversation_name() != rewrite.from {
                return Err(MemoryError::Backend(Arc::from(format!(
                    "legacy child epoch principal '{encoded}' does not belong to '{}'",
                    rewrite.from
                ))));
            }
            principal.conversation = target.conversation.clone();
            transaction
                .execute(
                    "UPDATE session_epochs SET conversation_key = ?1, principal = ?2 WHERE row_id = ?3",
                    params![rewrite.to, principal.storage_name(), row_id],
                )
                .map_err(super::memory_sqlite_error)?;
        }
    }
    transaction.commit().map_err(super::memory_sqlite_error)?;
    Ok(moved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subagents::child_input_envelope;
    use agentos_interfaces::orchestrator::SubAgentSpec;
    use agentos_interfaces::session::{Item, Session};
    use agentos_interfaces::RunState;
    use agentos_proto::{AgentId, ChannelId, ConversationId, Message, MessageRole, RunId};
    use serde_json::Value;
    use std::collections::BTreeMap;

    fn source_item() -> (Item, String) {
        let mut state = RunState::new(RunId::new("run"), AgentId::new("parent"));
        state.transcript.items.push(Item {
            message: Message::text(MessageRole::User, "task"),
            metadata: BTreeMap::from([
                (
                    Arc::from("channel_id"),
                    Value::String("telegram".to_owned()),
                ),
                (Arc::from("conversation_id"), Value::String("42".to_owned())),
            ]),
        });
        let spec = SubAgentSpec {
            agent_id: AgentId::new("worker"),
            policy_id: Arc::from("restricted"),
            metadata: BTreeMap::from([(
                Arc::from("prompt"),
                Value::String("same task".to_owned()),
            )]),
        };
        let envelope = child_input_envelope(&spec, &state);
        (
            Item {
                message: envelope.message,
                metadata: envelope.metadata,
            },
            ConversationPrincipal::new(
                AgentId::new("worker"),
                envelope.channel_id,
                envelope.conversation_id,
            )
            .storage_name(),
        )
    }

    #[tokio::test]
    async fn complete_legacy_source_is_rekeyed_without_merging() {
        let store = SqliteStore::open_in_memory().expect("store");
        let (item, target) = source_item();
        let legacy = ConversationPrincipal::new(
            AgentId::new("worker"),
            ChannelId::new("subagent:worker"),
            ConversationId::new("42:worker:oldhash"),
        );
        store
            .append(legacy.as_principal(), vec![item])
            .await
            .expect("legacy row");

        let plan = plan(&store).expect("plan");
        assert_eq!(plan.quarantined, Vec::new());
        assert_eq!(plan.rewrites.len(), 1);
        assert_eq!(plan.rewrites[0].to, target);
        assert_eq!(apply(&store, &plan).expect("apply"), 1);
        assert_eq!(
            store
                .load(legacy.as_principal())
                .await
                .expect("legacy load")
                .items
                .len(),
            0
        );
        let target = Principal::from_storage_name(&target).expect("target principal");
        assert_eq!(
            store.load(&target).await.expect("target load").items.len(),
            1
        );
    }

    #[tokio::test]
    async fn incomplete_legacy_source_is_reported_and_left_in_place() {
        let store = SqliteStore::open_in_memory().expect("store");
        let legacy = ConversationPrincipal::new(
            AgentId::new("worker"),
            ChannelId::new("subagent:worker"),
            ConversationId::new("42:worker:oldhash"),
        );
        store
            .append(
                legacy.as_principal(),
                vec![Item {
                    message: Message::text(MessageRole::User, "no source tuple"),
                    metadata: BTreeMap::new(),
                }],
            )
            .await
            .expect("legacy row");

        let plan = plan(&store).expect("plan");
        assert_eq!(plan.rewrites, Vec::new());
        assert_eq!(plan.quarantined.len(), 1);
        assert!(plan.report().contains("will NOT be migrated"));
        assert_eq!(apply(&store, &plan).expect("apply"), 0);
        assert_eq!(
            store
                .load(legacy.as_principal())
                .await
                .expect("legacy load")
                .items
                .len(),
            1
        );
    }
}
