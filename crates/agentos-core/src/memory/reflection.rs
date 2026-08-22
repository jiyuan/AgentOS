use super::retention::{RetentionReport, RetentionRequest};
use super::{
    memory_json_error, memory_sqlite_error, record_is_active, MemoryCaller, MemoryManager,
    MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility, SqliteStore,
};
use agentos_interfaces::memory::{MemoryError, Query, Record};
use agentos_proto::RecordId;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Parameters for a whole-memory maintenance pass over every conversation that
/// holds episodes. The per-conversation scopes are derived at sweep time, so
/// this carries only the knobs that apply uniformly across them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReflectionParams {
    #[serde(default = "default_min_episode_repetitions")]
    pub min_episode_repetitions: usize,
    #[serde(default)]
    pub rebuild_lexical_index: bool,
    /// The deployment's `[memory.retention]` budgets, applied once per sweep
    /// after every conversation has been promoted.
    ///
    /// Empty by default. Before M7 / `MEM-001` the sweep overwrote whatever it
    /// was given with `RetentionRequest::default()`, so the three configured
    /// budgets parsed, validated, appeared in the catalog, and pruned nothing.
    #[serde(default)]
    pub retention: RetentionRequest,
}

impl Default for ReflectionParams {
    fn default() -> Self {
        Self {
            min_episode_repetitions: default_min_episode_repetitions(),
            rebuild_lexical_index: true,
            retention: RetentionRequest::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReflectionRequest {
    pub episode_scope: MemoryScope,
    pub semantic_scope: MemoryScope,
    pub procedural_scope: MemoryScope,
    #[serde(default = "default_min_episode_repetitions")]
    pub min_episode_repetitions: usize,
    #[serde(default)]
    pub retention: RetentionRequest,
    #[serde(default)]
    pub rebuild_lexical_index: bool,
}

impl ReflectionRequest {
    pub fn for_conversation(caller: &MemoryCaller) -> Self {
        let owner = MemoryOwner::Conversation(caller.conversation_principal());
        Self {
            episode_scope: MemoryScope::new(
                MemoryStore::Episodic,
                owner.clone(),
                MemoryVisibility::Private,
                None,
            ),
            semantic_scope: MemoryScope::new(
                MemoryStore::Semantic,
                owner.clone(),
                MemoryVisibility::Private,
                None,
            ),
            procedural_scope: MemoryScope::new(
                MemoryStore::Procedural,
                owner,
                MemoryVisibility::Private,
                None,
            ),
            min_episode_repetitions: default_min_episode_repetitions(),
            retention: RetentionRequest::default(),
            rebuild_lexical_index: true,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReflectionReport {
    pub episode_candidates: usize,
    pub promoted_records: Vec<PromotionReport>,
    pub procedural_candidates: Vec<PromotionReport>,
    pub superseded_records: Vec<RecordId>,
    pub retention: RetentionReport,
    pub index: LexicalIndexReport,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PromotionReport {
    pub record_id: RecordId,
    pub source_record_ids: Vec<RecordId>,
    pub summary: Arc<str>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LexicalIndexReport {
    pub rebuilt: bool,
    pub indexed_records: usize,
}

pub trait MemoryMaintenance: Send + Sync {
    fn mark_record_status(
        &self,
        record_id: &RecordId,
        status: &'static str,
        reason: &'static str,
    ) -> Result<(), MemoryError>;

    fn link_records(
        &self,
        from_id: &RecordId,
        to_id: &RecordId,
        relation: &'static str,
        metadata: Value,
    ) -> Result<(), MemoryError>;

    fn apply_retention(&self, request: &RetentionRequest) -> Result<RetentionReport, MemoryError>;

    fn rebuild_lexical_index(&self) -> Result<LexicalIndexReport, MemoryError>;

    /// Distinct conversation owner ids that hold active episodic records — the
    /// set of scopes [`MemoryManager::reflect_all`] sweeps on a maintenance tick.
    fn episodic_conversation_owners(&self) -> Result<Vec<Arc<str>>, MemoryError>;
}

impl MemoryManager {
    pub async fn reflect(
        &self,
        caller: &MemoryCaller,
        request: ReflectionRequest,
    ) -> Result<ReflectionReport, MemoryError> {
        tracing::info!(
            operation = "reflection",
            agent_id = caller.agent_id.as_str(),
            task_id = caller.task_id.as_str(),
            conversation_id = caller.conversation_id.as_str(),
            "memory reflection started"
        );

        let episodes = self
            .read_scoped_with_reason(
                caller,
                request.episode_scope.clone(),
                &Query::filter(usize::MAX),
                Arc::from("reflection_episode_scan"),
            )
            .await?
            .into_iter()
            .filter(is_promotable_episode)
            .collect::<Vec<_>>();
        let grouped = group_episodes_by_summary(episodes);
        let mut report = ReflectionReport {
            episode_candidates: grouped.values().map(Vec::len).sum(),
            ..ReflectionReport::default()
        };

        let min_repetitions = request.min_episode_repetitions.max(2);
        for (summary, group) in grouped {
            if group.len() < min_repetitions {
                continue;
            }
            let source_record_ids = group
                .iter()
                .filter_map(|record| record.id.clone())
                .collect::<Vec<_>>();
            if source_record_ids.is_empty() {
                continue;
            }

            let semantic_id = self
                .promote_semantic_fact(
                    caller,
                    &request.semantic_scope,
                    &summary,
                    &source_record_ids,
                )
                .await?;
            report.promoted_records.push(PromotionReport {
                record_id: semantic_id.clone(),
                source_record_ids: source_record_ids.clone(),
                summary: Arc::clone(&summary),
            });
            self.link_sources(&source_record_ids, &semantic_id, "promoted_to")?;

            let superseded = self
                .supersede_stale_semantic_records(
                    caller,
                    &request.semantic_scope,
                    &summary,
                    &semantic_id,
                )
                .await?;
            report.superseded_records.extend(superseded);

            if group.iter().all(is_successful_trajectory) && group.iter().any(episode_used_tools) {
                let procedural_id = self
                    .promote_procedural_candidate(
                        caller,
                        &request.procedural_scope,
                        &summary,
                        &source_record_ids,
                    )
                    .await?;
                report.procedural_candidates.push(PromotionReport {
                    record_id: procedural_id.clone(),
                    source_record_ids: source_record_ids.clone(),
                    summary: Arc::clone(&summary),
                });
                self.link_sources(&source_record_ids, &procedural_id, "candidate_for")?;
            }
        }

        if let Some(maintenance) = self.maintenance() {
            report.retention = maintenance.apply_retention(&request.retention)?;
            if request.rebuild_lexical_index {
                report.index = maintenance.rebuild_lexical_index()?;
            }
        }

        tracing::info!(
            operation = "reflection",
            promoted_records = report.promoted_records.len(),
            procedural_candidates = report.procedural_candidates.len(),
            superseded_records = report.superseded_records.len(),
            archived_records = report.retention.archived_records.len(),
            indexed_records = report.index.indexed_records,
            "memory reflection finished"
        );

        Ok(report)
    }

    /// Sweep every conversation that holds episodes, promoting repeated
    /// episodes to semantic facts (and successful tool trajectories to
    /// procedural candidates) and superseding contradicted facts, then rebuild
    /// the lexical index once. Returns a combined report. A no-op (empty report)
    /// when the backend has no maintenance support (e.g. the in-memory store).
    pub async fn reflect_all(
        &self,
        agent_id: &agentos_proto::AgentId,
        params: &ReflectionParams,
    ) -> Result<ReflectionReport, MemoryError> {
        let Some(maintenance) = self.maintenance() else {
            return Ok(ReflectionReport::default());
        };
        let owners = maintenance.episodic_conversation_owners()?;
        let mut combined = ReflectionReport::default();
        for owner in owners {
            // The owner id in an episodic namespace is a principal storage
            // name. Anything that does not decode was written before
            // principals existed and belongs to the `ID-002` migration, not to
            // a maintenance pass that would file its results under a
            // fabricated identity.
            let Some(principal) = agentos_proto::Principal::from_storage_name(owner.as_ref())
            else {
                tracing::warn!(
                    owner = owner.as_ref(),
                    "skipping reflection for an episodic owner that is not a principal; \
                     run the identity migration"
                );
                continue;
            };
            let caller = MemoryCaller {
                agent_id: principal.agent.clone(),
                task_id: agentos_proto::TaskId::new("memory-maintenance"),
                channel_id: principal.channel.clone(),
                conversation_id: principal.conversation.clone(),
                user_id: None,
                allowed_shared_domains: Vec::new(),
                writable_shared_domains: Vec::new(),
                audit_read_access: false,
            };
            // Per-conversation: promotion + supersession only. Retention and the
            // lexical-index rebuild are global, so run them once after the sweep.
            let mut request = ReflectionRequest::for_conversation(&caller);
            request.min_episode_repetitions = params.min_episode_repetitions;
            request.retention = RetentionRequest::default();
            request.rebuild_lexical_index = false;
            let report = self.reflect(&caller, request).await?;
            combined.episode_candidates += report.episode_candidates;
            combined.promoted_records.extend(report.promoted_records);
            combined
                .procedural_candidates
                .extend(report.procedural_candidates);
            combined
                .superseded_records
                .extend(report.superseded_records);
        }
        // Retention and the index rebuild are global, so they run once after
        // the per-conversation passes rather than once per conversation — and
        // after promotion, so a fact promoted out of episodes on this tick is
        // not archived for age on the same tick that created it.
        combined.retention = maintenance.apply_retention(&params.retention)?;
        if params.rebuild_lexical_index {
            combined.index = maintenance.rebuild_lexical_index()?;
        }
        tracing::info!(
            operation = "reflection_sweep",
            agent_id = agent_id.as_str(),
            promoted_records = combined.promoted_records.len(),
            procedural_candidates = combined.procedural_candidates.len(),
            superseded_records = combined.superseded_records.len(),
            archived_records = combined.retention.archived_records.len(),
            indexed_records = combined.index.indexed_records,
            "memory reflection sweep finished"
        );
        Ok(combined)
    }

    async fn promote_semantic_fact(
        &self,
        caller: &MemoryCaller,
        scope: &MemoryScope,
        summary: &Arc<str>,
        source_record_ids: &[RecordId],
    ) -> Result<RecordId, MemoryError> {
        let body = json!({
            "kind": "semantic_fact",
            "summary": format!("Repeated episode: {summary}"),
            "source_summary": summary.as_ref(),
            "evidence_count": source_record_ids.len(),
        });
        let mut metadata = BTreeMap::new();
        metadata.insert(Arc::from("reflection_generated"), Value::Bool(true));
        metadata.insert(
            Arc::from("source_store"),
            Value::String("episodic".to_owned()),
        );
        metadata.insert(
            Arc::from("evidence_count"),
            Value::from(source_record_ids.len() as u64),
        );
        metadata.insert(Arc::from("importance"), Value::from(0.7));
        self.write_scoped_with_reason(
            caller,
            scope.clone(),
            body,
            metadata,
            Arc::from("reflection_semantic_promotion"),
        )
        .await
    }

    async fn promote_procedural_candidate(
        &self,
        caller: &MemoryCaller,
        scope: &MemoryScope,
        summary: &Arc<str>,
        source_record_ids: &[RecordId],
    ) -> Result<RecordId, MemoryError> {
        let body = json!({
            "kind": "procedural_candidate",
            "summary": format!("Repeated successful trajectory: {summary}"),
            "source_summary": summary.as_ref(),
            "evidence_count": source_record_ids.len(),
        });
        let mut metadata = BTreeMap::new();
        metadata.insert(Arc::from("reflection_generated"), Value::Bool(true));
        metadata.insert(
            Arc::from("evidence_count"),
            Value::from(source_record_ids.len() as u64),
        );
        metadata.insert(Arc::from("importance"), Value::from(0.6));
        self.write_scoped_with_reason(
            caller,
            scope.clone(),
            body,
            metadata,
            Arc::from("reflection_procedural_candidate"),
        )
        .await
    }

    async fn supersede_stale_semantic_records(
        &self,
        caller: &MemoryCaller,
        scope: &MemoryScope,
        source_summary: &Arc<str>,
        replacement_id: &RecordId,
    ) -> Result<Vec<RecordId>, MemoryError> {
        let existing = self
            .read_scoped_with_reason(
                caller,
                scope.clone(),
                &Query::lexical(source_summary.as_ref(), usize::MAX),
                Arc::from("reflection_supersession_scan"),
            )
            .await?;
        let stale_ids = existing
            .into_iter()
            .filter(record_is_active)
            .filter(|record| record.id.as_ref() != Some(replacement_id))
            .filter(|record| {
                record.body.get("kind").and_then(Value::as_str) == Some("semantic_fact")
                    && record.body.get("source_summary").and_then(Value::as_str)
                        == Some(source_summary.as_ref())
            })
            .filter_map(|record| record.id)
            .collect::<Vec<_>>();

        if let Some(maintenance) = self.maintenance() {
            for stale_id in &stale_ids {
                maintenance.mark_record_status(stale_id, "superseded", "reflection_superseded")?;
                maintenance.link_records(
                    stale_id,
                    replacement_id,
                    "superseded_by",
                    json!({ "reason": "reflection replacement" }),
                )?;
            }
        }

        Ok(stale_ids)
    }

    fn link_sources(
        &self,
        source_record_ids: &[RecordId],
        target_id: &RecordId,
        relation: &'static str,
    ) -> Result<(), MemoryError> {
        if let Some(maintenance) = self.maintenance() {
            for source_id in source_record_ids {
                maintenance.link_records(
                    source_id,
                    target_id,
                    relation,
                    json!({ "source": "reflection" }),
                )?;
            }
        }
        Ok(())
    }
}

impl MemoryMaintenance for SqliteStore {
    fn episodic_conversation_owners(&self) -> Result<Vec<Arc<str>>, MemoryError> {
        let conn = self.memory_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT owner_id FROM memory_records \
                 WHERE store = 'episodic' AND owner_kind = 'conversation' \
                 AND status = 'active' AND owner_id IS NOT NULL AND owner_id <> ''",
            )
            .map_err(memory_sqlite_error)?;
        let owners = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(memory_sqlite_error)?
            .collect::<Result<Vec<String>, _>>()
            .map_err(memory_sqlite_error)?
            .into_iter()
            .map(Arc::from)
            .collect();
        Ok(owners)
    }

    fn mark_record_status(
        &self,
        record_id: &RecordId,
        status: &'static str,
        reason: &'static str,
    ) -> Result<(), MemoryError> {
        let conn = self.memory_conn()?;
        let metadata_json = conn
            .query_row(
                "SELECT metadata_json FROM memory_records WHERE id = ?1",
                params![record_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .map_err(memory_sqlite_error)?;
        let mut metadata: BTreeMap<Arc<str>, Value> =
            serde_json::from_str(&metadata_json).map_err(memory_json_error)?;
        metadata.insert(Arc::from("status"), Value::String(status.to_owned()));
        metadata.insert(Arc::from("status_reason"), Value::String(reason.to_owned()));
        let metadata_json = serde_json::to_string(&metadata).map_err(memory_json_error)?;
        conn.execute(
            "UPDATE memory_records \
             SET status = ?1, metadata_json = ?2, updated_at = CURRENT_TIMESTAMP \
             WHERE id = ?3",
            params![status, metadata_json, record_id.as_str()],
        )
        .map_err(memory_sqlite_error)?;
        Ok(())
    }

    fn link_records(
        &self,
        from_id: &RecordId,
        to_id: &RecordId,
        relation: &'static str,
        metadata: Value,
    ) -> Result<(), MemoryError> {
        let metadata_json = serde_json::to_string(&metadata).map_err(memory_json_error)?;
        let conn = self.memory_conn()?;
        conn.execute(
            "INSERT INTO memory_links (from_id, to_id, relation, metadata_json) \
             VALUES (?1, ?2, ?3, ?4)",
            params![from_id.as_str(), to_id.as_str(), relation, metadata_json],
        )
        .map_err(memory_sqlite_error)?;
        Ok(())
    }

    fn apply_retention(&self, request: &RetentionRequest) -> Result<RetentionReport, MemoryError> {
        self.apply_retention_budgets(request)
    }

    fn rebuild_lexical_index(&self) -> Result<LexicalIndexReport, MemoryError> {
        let conn = self.memory_conn()?;
        conn.execute_batch(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_records_fts
                USING fts5(id UNINDEXED, namespace UNINDEXED, body_text, metadata_text);
            DELETE FROM memory_records_fts;
            "#,
        )
        .map_err(memory_sqlite_error)?;

        let mut stmt = conn
            .prepare(
                "SELECT id, namespace, body_json, metadata_json \
                 FROM memory_records \
                 WHERE status = 'active'",
            )
            .map_err(memory_sqlite_error)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(memory_sqlite_error)?;
        let mut active_records = Vec::new();
        for row in rows {
            active_records.push(row.map_err(memory_sqlite_error)?);
        }
        drop(stmt);

        for (id, namespace, body_json, metadata_json) in &active_records {
            conn.execute(
                "INSERT INTO memory_records_fts (id, namespace, body_text, metadata_text) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![id, namespace, body_json, metadata_json],
            )
            .map_err(memory_sqlite_error)?;
        }

        Ok(LexicalIndexReport {
            rebuilt: true,
            indexed_records: active_records.len(),
        })
    }
}

fn is_promotable_episode(record: &Record) -> bool {
    record_is_active(record)
        && record.body.get("kind").and_then(Value::as_str) == Some("run_episode")
        && episode_summary(record).is_some()
}

fn group_episodes_by_summary(records: Vec<Record>) -> BTreeMap<Arc<str>, Vec<Record>> {
    let mut grouped = BTreeMap::<Arc<str>, Vec<Record>>::new();
    for record in records {
        if let Some(summary) = episode_summary(&record) {
            grouped.entry(summary).or_default().push(record);
        }
    }
    grouped
}

fn episode_summary(record: &Record) -> Option<Arc<str>> {
    record
        .body
        .get("summary")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|summary| !summary.is_empty())
        .map(Arc::from)
}

fn is_successful_trajectory(record: &Record) -> bool {
    record.body.get("outcome").and_then(Value::as_str) == Some("succeeded")
}

fn episode_used_tools(record: &Record) -> bool {
    record
        .body
        .get("tools_used")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
        || record
            .body
            .get("subagents_used")
            .and_then(Value::as_array)
            .is_some_and(|subagents| !subagents.is_empty())
}

fn default_min_episode_repetitions() -> usize {
    2
}
