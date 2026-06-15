use crate::run_state::RunState;
use crate::session::Transcript;
use agentos_proto::{AgentId, Message, Namespace, RecordId, TaskId, ToolCall, Usage};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("orchestrator backend failed: {0}")]
    Backend(Arc<str>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Plan {
    Reply(Message),
    CallTool(ToolCall),
    Handoff(AgentId, Option<Value>),
    Delegate(SubAgentSpec),
    Escalate(SubOrchSpec),
    ResumeSubAgent {
        spec: SubAgentSpec,
        child_channel_id: agentos_proto::ChannelId,
        child_conversation_id: agentos_proto::ConversationId,
        child_state: Box<RunState>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubAgentSpec {
    pub agent_id: AgentId,
    pub policy_id: Arc<str>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<Arc<str>, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubOrchSpec {
    pub template: OrchestratorTemplate,
    pub task_id: TaskId,
    pub policy_id: Arc<str>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<Arc<str>, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrchestratorTemplate {
    pub name: Arc<str>,
    pub stages: Vec<Stage>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Stage {
    pub name: Arc<str>,
    pub agent: SubAgentSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<Arc<str>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemContext {
    pub active_agent: AgentId,
    pub task_id: TaskId,
    pub task_description: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_point: Option<Arc<str>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<Arc<str>, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryFragment {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RecordId>,
    pub namespace: Namespace,
    pub body: Value,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<Arc<str>, Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceIndex {
    pub entries: Vec<ResourceEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceEntry {
    pub name: Arc<str>,
    pub kind: ResourceKind,
    pub summary: Arc<str>,
    pub priority: DispatchPriority,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Tool,
    Skill,
    SubAgent,
    Mcp,
    Llm,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchPriority {
    Skill,
    ToolOrMcp,
    LlmFallback,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoutingTable {
    pub rules: Vec<RoutingRule>,
    pub fallback: RoutingRule,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoutingRule {
    pub domain: TaskDomain,
    #[serde(default)]
    pub description: Arc<str>,
    #[serde(default)]
    pub examples: Vec<Arc<str>>,
    pub dispatch: DispatchTarget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum TaskDomain {
    SoftwareDev,
    ContentOps,
    Research,
    Editing,
    General,
    Custom(Arc<str>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum DispatchTarget {
    Escalate(OrchestratorTemplate),
    Delegate(SubAgentSpec),
    Direct,
}

/// Sink for incremental assistant text produced while an orchestrator streams a
/// reply. Installed by the runtime entrypoint (e.g. the CLI TUI) when it wants
/// to render tokens as they arrive; `None` — the default — means buffered
/// planning that is byte-identical to a non-streaming run. The closure receives
/// each text chunk in order; concatenating every chunk reproduces the final
/// reply's `content`. Streamed text is *provisional*: output guardrails still
/// run on the assembled message before the loop finishes, but a violation
/// surfaces after the user has already seen the tokens, so the run errors
/// instead of committing a reply.
pub type StreamSink = Arc<dyn Fn(&str) + Send + Sync>;

pub struct RunContext<'a> {
    pub state: &'a RunState,
    pub system: SystemContext,
    pub transcript: &'a Transcript,
    pub memory_fragments: Vec<MemoryFragment>,
    pub resource_index: ResourceIndex,
    /// Per-call token usage that the orchestrator observed during this `plan()`
    /// invocation. The orchestrator pushes one entry per LLM call (regardless
    /// of whether the resulting `Plan` is a `Reply`, `CallTool`, `Delegate`,
    /// etc.); the loop drains the sink after `plan()` returns and emits one
    /// `llm_token_usage` trace event plus one `agentos_llm::usage`-style log
    /// line per entry. Without this sink, tool-calling LLM responses (whose
    /// response `Message` is consumed by `Plan::CallTool` and never reaches
    /// the loop) would have their token usage silently dropped.
    pub usage_sink: Arc<Mutex<Vec<Usage>>>,
    /// Optional sink for incremental assistant text (see [`StreamSink`]). The
    /// loop installs it on the context before `plan()`; an orchestrator that
    /// supports streaming pushes each text chunk through
    /// [`RunContext::emit_stream_delta`].
    pub stream_sink: Option<StreamSink>,
}

impl std::fmt::Debug for RunContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunContext")
            .field("state", &self.state)
            .field("system", &self.system)
            .field("transcript", &self.transcript)
            .field("memory_fragments", &self.memory_fragments)
            .field("resource_index", &self.resource_index)
            .field("usage_sink", &self.usage_sink)
            // `StreamSink` is a closure (no Debug); report only its presence.
            .field("stream_sink", &self.stream_sink.is_some())
            .finish()
    }
}

impl<'a> RunContext<'a> {
    pub fn from_state(state: &'a RunState) -> Self {
        let task_description = state
            .transcript
            .items
            .last()
            .map(|item| Arc::clone(&item.message.content))
            .unwrap_or_else(|| Arc::from(""));
        Self {
            state,
            system: SystemContext {
                active_agent: state.active_agent.clone(),
                task_id: state
                    .task_id
                    .clone()
                    .unwrap_or_else(|| TaskId::new(state.run_id.as_str())),
                task_description,
                resume_point: None,
                metadata: BTreeMap::new(),
            },
            transcript: &state.transcript,
            memory_fragments: Vec::new(),
            resource_index: ResourceIndex::default(),
            usage_sink: Arc::new(Mutex::new(Vec::new())),
            stream_sink: None,
        }
    }

    pub fn with_resource_index(mut self, resource_index: ResourceIndex) -> Self {
        self.resource_index = resource_index;
        self
    }

    /// Whether a streaming sink is installed for this run. Orchestrators use
    /// this to choose between a streamed and a buffered LLM call.
    pub fn has_stream_sink(&self) -> bool {
        self.stream_sink.is_some()
    }

    /// Forward one chunk of incremental assistant text to the installed
    /// [`StreamSink`]. No-op when none is installed.
    pub fn emit_stream_delta(&self, delta: &str) {
        if let Some(sink) = &self.stream_sink {
            sink(delta);
        }
    }

    /// Push one LLM call's token usage into the sink so the loop records it as
    /// a `llm_token_usage` trace event after `plan()` returns. Orchestrators
    /// must call this once per LLM round-trip — including rounds that resolve
    /// to `Plan::CallTool`, where the response `Message` (which carries
    /// `TOKEN_USAGE_METADATA_KEY`) is otherwise discarded.
    pub fn push_llm_usage(&self, usage: Usage) {
        if let Ok(mut guard) = self.usage_sink.lock() {
            guard.push(usage);
        }
    }

    /// Extract `TOKEN_USAGE_METADATA_KEY` from an LLM response `Message` and
    /// push it onto [`Self::usage_sink`]. No-op if the metadata is missing or
    /// malformed; callers that pre-deserialize should use [`push_llm_usage`]
    /// directly.
    pub fn push_llm_usage_from_message(&self, message: &Message) {
        let Some(raw) = message
            .metadata
            .get(agentos_proto::TOKEN_USAGE_METADATA_KEY)
        else {
            return;
        };
        // Deserialize by reference — `&Value` implements `Deserializer`, so
        // there is no need to clone the metadata value first.
        if let Ok(usage) = Usage::deserialize(raw) {
            self.push_llm_usage(usage);
        }
    }
}

impl ResourceIndex {
    pub fn sorted(mut self) -> Self {
        self.entries.sort_by(|left, right| {
            left.priority
                .cmp(&right.priority)
                .then(left.name.cmp(&right.name))
        });
        self
    }

    pub fn push(&mut self, entry: ResourceEntry) {
        self.entries.push(entry);
        self.entries.sort_by(|left, right| {
            left.priority
                .cmp(&right.priority)
                .then(left.name.cmp(&right.name))
        });
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            fallback: RoutingRule {
                domain: TaskDomain::General,
                description: Arc::from("General-purpose fallback for unclassified prompts."),
                examples: Vec::new(),
                dispatch: DispatchTarget::Direct,
            },
        }
    }
}

#[async_trait]
pub trait Orchestrator: Send + Sync {
    /// Hydrate the planning context with implementation-specific memory or task
    /// fragments before a decision is made.
    async fn hydrate(&self, _ctx: &mut RunContext<'_>) -> Result<(), OrchestratorError> {
        Ok(())
    }

    /// Decide the next action for the active run.
    ///
    /// Implementations must be deterministic with respect to the supplied
    /// `RunContext` and must not execute tools directly. Tool calls, handoffs,
    /// delegation, and escalation are returned as `Plan` variants so the core
    /// loop can run guardrails and approval first.
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError>;
}
