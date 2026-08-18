use agentos_interfaces::orchestrator::MemoryFragment;
use agentos_proto::{
    encode_base64url, AgentId, ConversationId, Namespace, PrincipalKey, RunId, TaskId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStore {
    Working,
    Episodic,
    Semantic,
    Procedural,
    Audit,
}

impl MemoryStore {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Working => "working",
            Self::Episodic => "episodic",
            Self::Semantic => "semantic",
            Self::Procedural => "procedural",
            Self::Audit => "audit",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum MemoryOwner {
    User(Arc<str>),
    Agent(AgentId),
    Task(TaskId),
    Principal(PrincipalKey),
    /// Legacy conversation-only owner. ID-002 migrates persisted rows to
    /// `Principal` and reports ambiguous legacy collisions.
    Conversation(ConversationId),
    Shared,
}

impl MemoryOwner {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::User(_) => "user",
            Self::Agent(_) => "agent",
            Self::Task(_) => "task",
            Self::Principal(_) => "principal",
            Self::Conversation(_) => "conversation",
            Self::Shared => "shared",
        }
    }

    pub(crate) fn encoded_id(&self) -> String {
        match self {
            Self::User(id) => scope_component(id, "global"),
            Self::Agent(id) => scope_component(id.as_str(), "global"),
            Self::Task(id) => scope_component(id.as_str(), "global"),
            Self::Principal(principal) => principal.storage_key(),
            Self::Conversation(id) => scope_component(id.as_str(), "global"),
            Self::Shared => "global".to_owned(),
        }
    }

    pub(crate) fn metadata_id(&self) -> String {
        match self {
            Self::User(id) => id.to_string(),
            Self::Agent(id) => id.as_str().to_owned(),
            Self::Task(id) => id.as_str().to_owned(),
            Self::Principal(principal) => principal.storage_key(),
            Self::Conversation(id) => id.as_str().to_owned(),
            Self::Shared => "global".to_owned(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryVisibility {
    Private,
    Shared,
    Public,
}

impl MemoryVisibility {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Shared => "shared",
            Self::Public => "public",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryScope {
    pub store: MemoryStore,
    pub owner: MemoryOwner,
    pub visibility: MemoryVisibility,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<Arc<str>>,
}

impl MemoryScope {
    pub fn new(
        store: MemoryStore,
        owner: MemoryOwner,
        visibility: MemoryVisibility,
        domain: Option<Arc<str>>,
    ) -> Self {
        Self {
            store,
            owner,
            visibility,
            domain,
        }
    }

    pub fn namespace(&self) -> Namespace {
        Namespace::new(format!(
            "{}/{}/{}/{}/{}",
            self.visibility.as_str(),
            self.owner.kind(),
            self.owner.encoded_id(),
            self.store.as_str(),
            self.domain_name()
        ))
    }

    pub(crate) fn domain_name(&self) -> String {
        self.domain
            .as_deref()
            .map(|domain| scope_component(domain, "general"))
            .unwrap_or_else(|| "general".to_owned())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryCaller {
    pub agent_id: AgentId,
    pub task_id: TaskId,
    pub conversation_id: ConversationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_key: Option<PrincipalKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Arc<str>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_shared_domains: Vec<Arc<str>>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub audit_read_access: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HydrationRequest {
    pub query: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<Arc<str>>,
    pub max_fragments: usize,
    pub max_tokens: usize,
    pub stores: Vec<MemoryStore>,
    pub strategy: RetrievalStrategy,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HydrationStats {
    pub candidate_count: usize,
    pub selected_count: usize,
    pub namespace_count: usize,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HydrationResult {
    pub fragments: Vec<MemoryFragment>,
    pub stats: HydrationStats,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeOutcome {
    Succeeded,
    Failed,
    Denied,
}

impl EpisodeOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Denied => "denied",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeRecord {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub active_agent: AgentId,
    pub conversation_id: ConversationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_key: Option<PrincipalKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Arc<str>>,
    pub outcome: EpisodeOutcome,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools_used: Vec<Arc<str>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subagents_used: Vec<AgentId>,
    pub summary: Arc<str>,
    pub turn_count: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<Arc<str>, Value>,
}

impl EpisodeRecord {
    pub fn should_record(&self) -> bool {
        self.outcome != EpisodeOutcome::Succeeded
            || self.turn_count > 1
            || !self.tools_used.is_empty()
            || !self.subagents_used.is_empty()
            || self.metadata_bool("explicit_user_preference")
            || self.metadata_bool("explicit_memory_write")
            || self.metadata_bool("approval_recorded")
    }

    fn metadata_bool(&self, key: &str) -> bool {
        self.metadata
            .get(key)
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalStrategy {
    Lexical,
    Recency,
    Hybrid,
}

pub(crate) fn scope_component(value: &str, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return fallback.to_owned();
    }
    format!("id_{}", encode_base64url(trimmed.as_bytes()))
}

#[cfg(test)]
mod identity_tests {
    use super::scope_component;

    #[test]
    fn namespace_components_do_not_collapse_distinct_ids() {
        assert_ne!(
            scope_component("a/b", "fallback"),
            scope_component("a_b", "fallback")
        );
    }
}
