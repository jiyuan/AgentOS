use agentos_interfaces::memory::{Memory, MemoryError, Query, Record, Selector};
use agentos_interfaces::orchestrator::MemoryFragment;
use agentos_proto::{AgentId, Namespace, RecordId};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

mod accounting;
mod authorize;
mod context;
mod hybrid;
mod in_memory;
mod lease;
pub mod migrate;
mod pool;
mod qdrant;
mod query;
mod reflection;
mod retention;
mod scope;
mod session_store;
mod sqlite;
mod sqlite_vec;

use accounting::{managed_metadata, MemoryAccessLogEntry, MemoryAccounting, MemoryOperation};
use authorize::{authorize_scope, hydration_scopes, unauthorized};
pub use context::{
    channel_id_from_context, conversation_id_from_context, memory_caller_from_context,
};
use hybrid::reciprocal_rank_fusion;
pub use hybrid::{SemanticIndex, SemanticSearchHit};
pub use in_memory::{InMemoryMemory, InMemorySession};
pub use lease::{lease_holder_id, Lease, DEFAULT_LEASE_TTL, REFLECTION_LEASE};
pub use pool::{DEFAULT_MAX_CONNECTIONS, MAX_MAX_CONNECTIONS};
pub use qdrant::{QdrantSemanticConfig, QdrantSemanticIndex};
use query::{estimate_fragment_tokens, selector_matches_record};
pub(crate) use query::{record_is_active, record_matches_query};
pub use reflection::{
    LexicalIndexReport, MemoryMaintenance, PromotionReport, ReflectionParams, ReflectionReport,
    ReflectionRequest,
};
pub use retention::{RetentionReport, RetentionRequest, StoreRetentionBudget};
pub use scope::{
    EpisodeOutcome, EpisodeRecord, HydrationRequest, HydrationResult, HydrationStats, MemoryCaller,
    MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility, RetrievalStrategy, SharedDomainGrants,
};
pub use sqlite::SqliteStore;
pub(crate) use sqlite::{memory_json_error, memory_sqlite_error};
pub use sqlite_vec::{SqliteVecConfig, SqliteVecSemanticIndex};

#[derive(Clone)]
pub struct MemoryManager {
    backend: Arc<dyn Memory>,
    accounting: Option<Arc<dyn MemoryAccounting>>,
    maintenance: Option<Arc<dyn MemoryMaintenance>>,
    semantic_index: Option<Arc<dyn SemanticIndex>>,
}

impl MemoryManager {
    pub fn new(backend: Arc<dyn Memory>) -> Self {
        Self {
            backend,
            accounting: None,
            maintenance: None,
            semantic_index: None,
        }
    }

    pub fn new_sqlite(backend: Arc<SqliteStore>) -> Self {
        Self {
            backend: backend.clone(),
            accounting: Some(backend.clone()),
            maintenance: Some(backend),
            semantic_index: None,
        }
    }

    pub fn with_semantic_index(mut self, semantic_index: Arc<dyn SemanticIndex>) -> Self {
        self.semantic_index = Some(semantic_index);
        self
    }

    pub fn backend(&self) -> &Arc<dyn Memory> {
        &self.backend
    }

    pub fn maintenance(&self) -> Option<&Arc<dyn MemoryMaintenance>> {
        self.maintenance.as_ref()
    }

    pub async fn write_scoped(
        &self,
        caller: &MemoryCaller,
        scope: MemoryScope,
        body: Value,
        metadata: BTreeMap<Arc<str>, Value>,
    ) -> Result<RecordId, MemoryError> {
        self.write_scoped_with_reason(caller, scope, body, metadata, Arc::from("managed_write"))
            .await
    }

    pub async fn write_scoped_with_reason(
        &self,
        caller: &MemoryCaller,
        scope: MemoryScope,
        body: Value,
        metadata: BTreeMap<Arc<str>, Value>,
        reason: Arc<str>,
    ) -> Result<RecordId, MemoryError> {
        authorize_scope(caller, &scope, MemoryOperation::Write)?;
        let namespace = scope.namespace();
        let metadata = managed_metadata(caller, &scope, metadata);
        let id = self
            .backend
            .write(
                &namespace,
                Record {
                    id: None,
                    namespace: namespace.clone(),
                    body: body.clone(),
                    metadata: metadata.clone(),
                },
            )
            .await?;
        let indexed_record = Record {
            id: Some(id.clone()),
            namespace: namespace.clone(),
            body,
            metadata,
        };
        self.upsert_semantic_index(&indexed_record).await;
        self.append_access_log(
            MemoryOperation::Write,
            Some(&id),
            &namespace,
            caller,
            Some(&reason),
        )?;
        tracing::info!(
            operation = "managed_write",
            record_id = id.as_str(),
            namespace = namespace.as_str(),
            caller_agent_id = caller.agent_id.as_str(),
            caller_task_id = caller.task_id.as_str(),
            caller_conversation_id = caller.conversation_id.as_str(),
            store = scope.store.as_str(),
            owner_kind = scope.owner.kind(),
            visibility = scope.visibility.as_str(),
            domain = scope.domain_name(),
            reason = reason.as_ref(),
            "memory managed write"
        );
        Ok(id)
    }

    pub async fn read_scoped(
        &self,
        caller: &MemoryCaller,
        scope: MemoryScope,
        query: &Query,
    ) -> Result<Vec<Record>, MemoryError> {
        self.read_scoped_with_reason(caller, scope, query, Arc::from("managed_read"))
            .await
    }

    pub async fn read_scoped_with_reason(
        &self,
        caller: &MemoryCaller,
        scope: MemoryScope,
        query: &Query,
        reason: Arc<str>,
    ) -> Result<Vec<Record>, MemoryError> {
        authorize_scope(caller, &scope, MemoryOperation::Read)?;
        let namespace = scope.namespace();
        let records = self.backend.read(&namespace, query).await?;
        let record_ids = records
            .iter()
            .filter_map(|record| record.id.clone())
            .collect::<Vec<_>>();
        self.record_read_access(&record_ids)?;
        if record_ids.is_empty() {
            self.append_access_log(
                MemoryOperation::Read,
                None,
                &namespace,
                caller,
                Some(&reason),
            )?;
        } else {
            self.append_access_log_for_records(
                MemoryOperation::Read,
                &record_ids,
                &namespace,
                caller,
                Some(&reason),
            )?;
        }
        tracing::info!(
            operation = "managed_read",
            namespace = namespace.as_str(),
            caller_agent_id = caller.agent_id.as_str(),
            caller_task_id = caller.task_id.as_str(),
            caller_conversation_id = caller.conversation_id.as_str(),
            store = scope.store.as_str(),
            owner_kind = scope.owner.kind(),
            visibility = scope.visibility.as_str(),
            domain = scope.domain_name(),
            record_count = records.len(),
            reason = reason.as_ref(),
            "memory managed read"
        );
        Ok(records)
    }

    pub async fn hydrate(
        &self,
        caller: &MemoryCaller,
        request: HydrationRequest,
    ) -> Result<Vec<MemoryFragment>, MemoryError> {
        Ok(self.hydrate_with_stats(caller, request).await?.fragments)
    }

    pub async fn hydrate_with_stats(
        &self,
        caller: &MemoryCaller,
        request: HydrationRequest,
    ) -> Result<HydrationResult, MemoryError> {
        tracing::info!(
            operation = "hydrate",
            caller_agent_id = caller.agent_id.as_str(),
            caller_task_id = caller.task_id.as_str(),
            caller_conversation_id = caller.conversation_id.as_str(),
            query_bytes = request.query.len(),
            max_fragments = request.max_fragments,
            max_estimated_tokens = request.max_tokens,
            store_count = request.stores.len(),
            shared_domain_count = caller.allowed_shared_domains.len(),
            "memory hydrate started"
        );
        if request.max_fragments == 0 {
            tracing::info!(
                operation = "hydrate",
                caller_agent_id = caller.agent_id.as_str(),
                caller_task_id = caller.task_id.as_str(),
                caller_conversation_id = caller.conversation_id.as_str(),
                candidate_count = 0,
                selected_count = 0,
                namespace_count = 0,
                "memory hydrate finished"
            );
            return Ok(HydrationResult::default());
        }

        let mut fragments = Vec::new();
        let mut stats = HydrationStats::default();
        let mut selected_namespaces = Vec::<Namespace>::new();
        let mut estimated_tokens = 0usize;
        for scope in hydration_scopes(caller, &request) {
            let remaining = request.max_fragments.saturating_sub(fragments.len());
            if remaining == 0 {
                break;
            }
            let candidate_limit = remaining.saturating_mul(4).max(remaining);
            for record in self
                .hydrate_scope_records(caller, scope, &request, candidate_limit)
                .await?
            {
                if !record_is_active(&record) {
                    continue;
                }
                stats.candidate_count += 1;
                let fragment = MemoryFragment {
                    id: record.id,
                    namespace: record.namespace,
                    body: record.body,
                    metadata: record.metadata,
                };
                let fragment_tokens = estimate_fragment_tokens(&fragment);
                if request.max_tokens > 0
                    && estimated_tokens.saturating_add(fragment_tokens) > request.max_tokens
                {
                    continue;
                }
                if !selected_namespaces
                    .iter()
                    .any(|namespace| namespace == &fragment.namespace)
                {
                    selected_namespaces.push(fragment.namespace.clone());
                }
                estimated_tokens = estimated_tokens.saturating_add(fragment_tokens);
                fragments.push(fragment);
                if fragments.len() >= request.max_fragments {
                    break;
                }
            }
        }
        stats.selected_count = fragments.len();
        stats.namespace_count = selected_namespaces.len();
        tracing::info!(
            operation = "hydrate",
            caller_agent_id = caller.agent_id.as_str(),
            caller_task_id = caller.task_id.as_str(),
            caller_conversation_id = caller.conversation_id.as_str(),
            candidate_count = stats.candidate_count,
            selected_count = stats.selected_count,
            namespace_count = stats.namespace_count,
            "memory hydrate finished"
        );
        Ok(HydrationResult { fragments, stats })
    }

    pub async fn record_episode(
        &self,
        episode: EpisodeRecord,
    ) -> Result<Option<RecordId>, MemoryError> {
        if !episode.should_record() {
            tracing::info!(
                operation = "episode",
                run_id = episode.run_id.as_str(),
                task_id = episode.task_id.as_str(),
                active_agent = episode.active_agent.as_str(),
                conversation_id = episode.conversation_id.as_str(),
                outcome = episode.outcome.as_str(),
                turn_count = episode.turn_count,
                tools_count = episode.tools_used.len(),
                subagents_count = episode.subagents_used.len(),
                skip_reason = "trivial_run",
                "memory episode skipped"
            );
            return Ok(None);
        }

        let run_id = episode.run_id.clone();
        let task_id = episode.task_id.clone();
        let active_agent = episode.active_agent.clone();
        let conversation_id = episode.conversation_id.clone();
        let outcome = episode.outcome;
        let tools_count = episode.tools_used.len();
        let subagents_count = episode.subagents_used.len();
        let turn_count = episode.turn_count;
        let caller = MemoryCaller {
            agent_id: episode.active_agent.clone(),
            task_id: episode.task_id.clone(),
            channel_id: episode.channel_id.clone(),
            conversation_id: episode.conversation_id.clone(),
            user_id: episode.user_id.clone(),
            allowed_shared_domains: Vec::new(),
            writable_shared_domains: Vec::new(),
            audit_read_access: false,
        };
        let scope = MemoryScope::new(
            MemoryStore::Episodic,
            MemoryOwner::Conversation(episode.conversation_principal()),
            MemoryVisibility::Private,
            None,
        );
        let body = json!({
            "kind": "run_episode",
            "run_id": episode.run_id.as_str(),
            "task_id": episode.task_id.as_str(),
            "active_agent": episode.active_agent.as_str(),
            "conversation_id": episode.conversation_id.as_str(),
            "outcome": episode.outcome.as_str(),
            "tools_used": episode
                .tools_used
                .iter()
                .map(Arc::as_ref)
                .collect::<Vec<_>>(),
            "subagents_used": episode
                .subagents_used
                .iter()
                .map(AgentId::as_str)
                .collect::<Vec<_>>(),
            "summary": episode.summary.as_ref(),
        });
        let mut metadata = episode.metadata;
        metadata.insert(
            Arc::from("source_run_id"),
            Value::String(episode.run_id.as_str().to_owned()),
        );
        metadata.insert(
            Arc::from("outcome"),
            Value::String(episode.outcome.as_str().to_owned()),
        );
        metadata.insert(
            Arc::from("tools_count"),
            Value::from(episode.tools_used.len() as u64),
        );
        metadata.insert(
            Arc::from("subagents_count"),
            Value::from(episode.subagents_used.len() as u64),
        );
        metadata.insert(
            Arc::from("turn_count"),
            Value::from(episode.turn_count as u64),
        );

        let id = self
            .write_scoped_with_reason(&caller, scope, body, metadata, Arc::from("episode_record"))
            .await?;
        tracing::info!(
            operation = "episode",
            record_id = id.as_str(),
            run_id = run_id.as_str(),
            task_id = task_id.as_str(),
            active_agent = active_agent.as_str(),
            conversation_id = conversation_id.as_str(),
            outcome = outcome.as_str(),
            turn_count = turn_count,
            tools_count = tools_count,
            subagents_count = subagents_count,
            "memory episode recorded"
        );
        Ok(Some(id))
    }

    async fn hydrate_scope_records(
        &self,
        caller: &MemoryCaller,
        scope: MemoryScope,
        request: &HydrationRequest,
        limit: usize,
    ) -> Result<Vec<Record>, MemoryError> {
        match request.strategy {
            RetrievalStrategy::Recency => {
                self.read_scoped(caller, scope, &Query::filter(limit)).await
            }
            RetrievalStrategy::Lexical => {
                let scoped_query = Query::lexical(request.query.as_ref(), limit);
                self.read_scoped(caller, scope, &scoped_query).await
            }
            RetrievalStrategy::Hybrid => {
                self.hybrid_scope_records(caller, scope, request.query.as_ref(), limit)
                    .await
            }
        }
    }

    async fn hybrid_scope_records(
        &self,
        caller: &MemoryCaller,
        scope: MemoryScope,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Record>, MemoryError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let lexical_records = self
            .read_scoped(caller, scope.clone(), &Query::lexical(query, limit))
            .await?;
        let lexical_ids = lexical_records
            .iter()
            .filter_map(|record| record.id.clone())
            .collect::<Vec<_>>();
        let mut records_by_id = lexical_records
            .into_iter()
            .filter_map(|record| record.id.clone().map(|id| (id, record)))
            .collect::<BTreeMap<_, _>>();

        let Some(index) = &self.semantic_index else {
            return Ok(lexical_ids
                .into_iter()
                .filter_map(|record_id| records_by_id.remove(&record_id))
                .take(limit)
                .collect());
        };

        let namespace = scope.namespace();
        let mut semantic_hits = match index.search(&namespace, query, limit).await {
            Ok(hits) => hits,
            Err(err) => {
                tracing::warn!(
                    operation = "semantic_index_search",
                    namespace = namespace.as_str(),
                    error = err.to_string(),
                    "semantic index search failed"
                );
                return Ok(lexical_ids
                    .into_iter()
                    .filter_map(|record_id| records_by_id.remove(&record_id))
                    .take(limit)
                    .collect());
            }
        };
        semantic_hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(left.record_id.as_str().cmp(right.record_id.as_str()))
        });
        let semantic_ids = semantic_hits
            .into_iter()
            .map(|hit| hit.record_id)
            .collect::<Vec<_>>();
        let missing_ids = semantic_ids
            .iter()
            .filter(|record_id| !records_by_id.contains_key(*record_id))
            .cloned()
            .collect::<BTreeSet<_>>();
        if !missing_ids.is_empty() {
            for record in self
                .backend
                .read(&namespace, &Query::filter(usize::MAX))
                .await?
            {
                let Some(record_id) = record.id.clone() else {
                    continue;
                };
                if missing_ids.contains(&record_id) {
                    records_by_id.insert(record_id, record);
                }
            }
        }

        Ok(reciprocal_rank_fusion(&[lexical_ids, semantic_ids], limit)
            .into_iter()
            .filter_map(|record_id| records_by_id.remove(&record_id))
            .collect())
    }

    pub async fn forget_scoped_with_reason(
        &self,
        caller: &MemoryCaller,
        scope: MemoryScope,
        selector: Selector,
        reason: Arc<str>,
    ) -> Result<usize, MemoryError> {
        authorize_scope(caller, &scope, MemoryOperation::Forget)?;
        let namespace = scope.namespace();
        if selector
            .namespace
            .as_ref()
            .is_some_and(|selected| selected != &namespace)
        {
            return Err(unauthorized(
                "selector namespace is outside the requested scope",
            ));
        }
        let candidates = self
            .backend
            .read(&namespace, &Query::filter(usize::MAX))
            .await?;
        let removed_ids = candidates
            .iter()
            .filter(|record| selector_matches_record(record, &selector))
            .filter_map(|record| record.id.clone())
            .collect::<Vec<_>>();
        let removed = self.backend.forget(&namespace, &selector).await?;
        self.delete_semantic_index_records(&namespace, &removed_ids)
            .await;
        if removed_ids.is_empty() {
            self.append_access_log(
                MemoryOperation::Forget,
                None,
                &namespace,
                caller,
                Some(&reason),
            )?;
        } else {
            self.append_access_log_for_records(
                MemoryOperation::Forget,
                &removed_ids,
                &namespace,
                caller,
                Some(&reason),
            )?;
        }
        tracing::info!(
            operation = "managed_forget",
            namespace = namespace.as_str(),
            caller_agent_id = caller.agent_id.as_str(),
            caller_task_id = caller.task_id.as_str(),
            caller_conversation_id = caller.conversation_id.as_str(),
            store = scope.store.as_str(),
            owner_kind = scope.owner.kind(),
            visibility = scope.visibility.as_str(),
            domain = scope.domain_name(),
            removed_count = removed,
            reason = reason.as_ref(),
            "memory managed forget"
        );
        Ok(removed)
    }

    fn record_read_access(&self, record_ids: &[RecordId]) -> Result<(), MemoryError> {
        if let Some(accounting) = &self.accounting {
            accounting.record_read_access(record_ids)?;
        }
        Ok(())
    }

    fn append_access_log(
        &self,
        operation: MemoryOperation,
        record_id: Option<&RecordId>,
        namespace: &Namespace,
        caller: &MemoryCaller,
        reason: Option<&Arc<str>>,
    ) -> Result<(), MemoryError> {
        if let Some(accounting) = &self.accounting {
            accounting.append_access_log(MemoryAccessLogEntry {
                operation: operation.as_str(),
                record_id,
                namespace,
                caller,
                reason: reason.map(Arc::as_ref),
            })?;
        }
        Ok(())
    }

    fn append_access_log_for_records(
        &self,
        operation: MemoryOperation,
        record_ids: &[RecordId],
        namespace: &Namespace,
        caller: &MemoryCaller,
        reason: Option<&Arc<str>>,
    ) -> Result<(), MemoryError> {
        if let Some(accounting) = &self.accounting {
            accounting.append_access_log_for_records(
                operation.as_str(),
                record_ids,
                namespace,
                caller,
                reason.map(Arc::as_ref),
            )?;
        }
        Ok(())
    }

    async fn upsert_semantic_index(&self, record: &Record) {
        let Some(index) = &self.semantic_index else {
            return;
        };
        if let Err(err) = index.upsert(record).await {
            tracing::warn!(
                operation = "semantic_index_upsert",
                namespace = record.namespace.as_str(),
                record_id = record.id.as_ref().map(RecordId::as_str),
                error = err.to_string(),
                "semantic index upsert failed"
            );
        }
    }

    async fn delete_semantic_index_records(&self, namespace: &Namespace, record_ids: &[RecordId]) {
        let Some(index) = &self.semantic_index else {
            return;
        };
        if let Err(err) = index.delete(namespace, record_ids).await {
            tracing::warn!(
                operation = "semantic_index_delete",
                namespace = namespace.as_str(),
                record_count = record_ids.len(),
                error = err.to_string(),
                "semantic index delete failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_interfaces::orchestrator::RunContext;
    use agentos_interfaces::session::Item;
    use agentos_interfaces::RunState;
    use agentos_proto::{
        AgentId, ChannelId, ConversationId, Message, MessageRole, Principal, RunId, TaskId,
    };
    use serde_json::json;

    #[test]
    fn memory_caller_uses_parent_channel_metadata_after_tool_call() {
        let mut state = RunState::new(RunId::new("run-1"), AgentId::new("default"));
        state.task_id = Some(TaskId::new("task-1"));
        let mut user_metadata = BTreeMap::new();
        user_metadata.insert(Arc::from("channel_id"), json!("telegram"));
        user_metadata.insert(Arc::from("conversation_id"), json!("chat-1"));
        user_metadata.insert(Arc::from("sender"), json!("user-1"));
        state.transcript.items.push(Item {
            message: Message::text(MessageRole::User, "Run an audit"),
            metadata: user_metadata,
        });
        let mut tool_call_metadata = BTreeMap::new();
        tool_call_metadata.insert(Arc::from("kind"), json!("tool_call"));
        tool_call_metadata.insert(Arc::from("tool_name"), json!("memory"));
        state.transcript.items.push(Item {
            message: Message::text(MessageRole::Assistant, ""),
            metadata: tool_call_metadata,
        });

        let ctx = RunContext::from_state(&state);
        let caller = memory_caller_from_context(&ctx, &SharedDomainGrants::default());

        assert_eq!(caller.conversation_id.as_str(), "chat-1");
        assert_eq!(caller.user_id.as_deref(), Some("user-1"));
        assert!(caller.audit_read_access);
    }

    #[tokio::test]
    async fn audit_store_read_requires_parent_admin_path() {
        let manager = MemoryManager::new(Arc::new(InMemoryMemory::default()));
        let scope = MemoryScope::new(
            MemoryStore::Audit,
            MemoryOwner::Conversation(Principal::conversation(
                AgentId::new("default"),
                ChannelId::new("test-channel"),
                ConversationId::new("chat-1"),
            )),
            MemoryVisibility::Private,
            None,
        );
        let query = Query::lexical("", 5);
        let caller = MemoryCaller {
            agent_id: AgentId::new("default"),
            channel_id: ChannelId::new("test-channel"),
            task_id: TaskId::new("task-1"),
            conversation_id: ConversationId::new("chat-1"),
            user_id: None,
            allowed_shared_domains: Vec::new(),
            writable_shared_domains: Vec::new(),
            audit_read_access: false,
        };
        let err = manager
            .read_scoped(&caller, scope.clone(), &query)
            .await
            .expect_err("non-admin audit read should be rejected");
        assert!(err.to_string().contains("administrative path"));

        let admin_caller = MemoryCaller {
            audit_read_access: true,
            ..caller
        };
        let records = manager
            .read_scoped(&admin_caller, scope, &query)
            .await
            .expect("admin audit read should be allowed");
        assert!(records.is_empty());
    }
}
