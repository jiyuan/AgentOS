use agentos_interfaces::orchestrator::MemoryFragment;
use agentos_proto::{AgentId, ChannelId, ConversationId, Namespace, Principal, RunId, TaskId};
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum MemoryOwner {
    User(Arc<str>),
    Agent(AgentId),
    Task(TaskId),
    /// One conversation, keyed by its [`Principal`] rather than by a bare
    /// `ConversationId`.
    ///
    /// A conversation id is channel-local: `telegram:42` and `feishu:42` are
    /// both `"42"`, and so was a second agent's `telegram:42`. Keying on the
    /// id alone made all three the same memory namespace (`ID-001`,
    /// [ADR-0003](../../../../docs/adr/0003-TYPED_PRINCIPAL.md)).
    Conversation(Principal),
    Shared,
}

impl MemoryOwner {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::User(_) => "user",
            Self::Agent(_) => "agent",
            Self::Task(_) => "task",
            Self::Conversation(_) => "conversation",
            Self::Shared => "shared",
        }
    }

    /// The owner's identity, already namespace-encoded.
    ///
    /// A principal brings its own injective encoding, so it is used as-is;
    /// every other kind is a single opaque string and goes through
    /// [`scope_component`].
    pub(crate) fn namespace_id(&self) -> String {
        match self {
            Self::User(id) => scope_component(id),
            Self::Agent(id) => scope_component(id.as_str()),
            Self::Task(id) => scope_component(id.as_str()),
            Self::Conversation(principal) => principal.storage_name(),
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
            self.owner.namespace_id(),
            self.store.as_str(),
            self.domain_name()
        ))
    }

    pub(crate) fn domain_name(&self) -> String {
        self.domain
            .as_deref()
            .map(scope_component)
            .unwrap_or_else(|| "general".to_owned())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryCaller {
    pub agent_id: AgentId,
    pub task_id: TaskId,
    /// Which channel the conversation belongs to. Without it a conversation
    /// id is channel-local and two channels' conversations collide.
    #[serde(default = "unknown_channel")]
    pub channel_id: ChannelId,
    pub conversation_id: ConversationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Arc<str>>,
    /// Shared domains this caller may *read*.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_shared_domains: Vec<Arc<str>>,
    /// Shared domains this caller may *write*.
    ///
    /// Always a subset of what the deployment permits: the runtime intersects
    /// `[memory.policy].shared_writes`, the domain's own `write = true`, and
    /// the caller's own grant before this list is built, so `authorize_scope`
    /// checks one thing and the three gates cannot get out of order
    /// (M7 / `MEM-001`). Empty in every configuration that has not
    /// deliberately opened one, which is the default.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writable_shared_domains: Vec<Arc<str>>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub audit_read_access: bool,
}

/// What a deployment's `[[memory.shared_domains]]` permit, with
/// `[memory.policy].shared_writes` already folded in.
///
/// Two lists rather than a list of triples because every consumer wants one or
/// the other, and the intersection with a caller's own grant is a set
/// operation either way.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedDomainGrants {
    pub readable: Vec<Arc<str>>,
    pub writable: Vec<Arc<str>>,
}

impl SharedDomainGrants {
    /// Read-only access to the named domains, which is what a deployment that
    /// has not enabled shared writes gets.
    pub fn read_only(domains: impl IntoIterator<Item = Arc<str>>) -> Self {
        Self {
            readable: domains.into_iter().collect(),
            writable: Vec::new(),
        }
    }

    pub fn permits_write(&self, domain: &str) -> bool {
        self.writable.iter().any(|name| name.as_ref() == domain)
    }
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

/// The channel a caller deserialized from a record written before
/// `MemoryCaller` carried one. A distinct, reserved name rather than an empty
/// string, so pre-principal state is visibly pre-principal instead of
/// masquerading as a real channel.
fn unknown_channel() -> ChannelId {
    ChannelId::new("unknown-channel")
}

impl EpisodeRecord {
    /// The principal this episode belongs to.
    pub fn conversation_principal(&self) -> Principal {
        Principal::conversation(
            self.active_agent.clone(),
            self.channel_id.clone(),
            self.conversation_id.clone(),
        )
    }
}

impl MemoryCaller {
    /// The principal for this caller's own conversation.
    pub fn conversation_principal(&self) -> Principal {
        Principal::conversation(
            self.agent_id.clone(),
            self.channel_id.clone(),
            self.conversation_id.clone(),
        )
    }
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
    /// Which channel the conversation belongs to, so the episode is filed
    /// under the same principal the conversation's other memory is.
    #[serde(default = "unknown_channel")]
    pub channel_id: ChannelId,
    pub conversation_id: ConversationId,
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

/// One namespace component, encoded so that distinct inputs stay distinct.
///
/// This was `trimmed.replace('/', "_")`, which is not injective: `a/b` and
/// `a_b` produced the same namespace, so two owners shared one another's
/// memory. `channels/attachments.rs` even carried a test asserting the
/// collision (`ID-001`, [ADR-0003](../../../../docs/adr/0003-TYPED_PRINCIPAL.md)).
///
/// Values that are already unambiguous pass through unchanged, because a
/// namespace an operator can read out of the database is worth keeping.
/// Anything else — a separator, a space, an empty string, any non-ASCII — is
/// emitted as `~` followed by unpadded base64url. The marker is what makes the
/// two forms distinguishable: a plain value can never begin with `~`, since
/// `~` is not in the safe set.
pub(crate) fn scope_component(value: &str) -> String {
    // The same encoding `Principal::storage_name` uses for its own components,
    // so one namespace does not mix two escaping rules.
    agentos_proto::encode_component(value)
}
