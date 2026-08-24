use crate::approve::{
    DelegatedAuthority, DelegationGrantError, DelegationGrantTemplate, DelegationScope, Policy,
    PolicyError,
};
use crate::audit::SafetyLog;
use crate::config::CompactionConfig;
use crate::memory::{InMemorySession, MemoryManager};
use crate::prompt::Compaction;
use crate::r#loop::{InputGuardrailEntry, OutputGuardrailEntry, ResumeWitness, ToolGuardrailEntry};
use crate::runner::{
    resume_run, run_envelope, PausedRun, RunOutcome, RunnerDeps, TraceSink,
    SESSION_SCOPE_EPHEMERAL, SESSION_SCOPE_KEY,
};
use crate::spill::{ContentLimits, SpillStore, DEFAULT_TOOL_RESULT_INLINE_BYTES};
use crate::task_workspace::TaskWorkspace;
use crate::tools::ToolRegistry;
use agentos_interfaces::guardrail::{InputGuardrail, OutputGuardrail, ToolGuardrail};
use agentos_interfaces::orchestrator::{Orchestrator, SubAgentSpec};
use agentos_interfaces::session::Session;
use agentos_llm::Llm;
use agentos_proto::{
    ActorPrincipal, AgentId, ChannelId, ConversationId, ConversationPrincipal, Envelope, Message,
    RunId,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum SubAgentError {
    #[error("unknown sub-agent '{agent_id:?}' with policy '{policy_id}'")]
    Unknown {
        agent_id: AgentId,
        policy_id: Arc<str>,
    },
    #[error("child policy is not a narrowing of parent policy: {0}")]
    Policy(#[from] PolicyError),
    #[error("sub-agent channel closed")]
    ChannelClosed,
    #[error("sub-agent task failed: {0}")]
    Task(Arc<str>),
    #[error("sub-agent run failed: {0}")]
    Run(Arc<str>),
    #[error("sub-agent paused unexpectedly")]
    Paused,
    #[error("delegation grant failed: {0}")]
    Grant(#[from] DelegationGrantError),
    #[error("system clock is before the Unix epoch; delegation grants fail closed")]
    ClockUnavailable,
    #[error("delegation requires a sender-qualified initiating parent actor")]
    MissingInitiatingActor,
    #[error("paused child is missing its bound delegation scope")]
    MissingDelegationScope,
    #[error("paused child has an invalid delegation scope: {0}")]
    InvalidDelegationScope(Arc<str>),
}

pub struct SubAgentDefinition {
    pub agent_id: AgentId,
    pub policy_id: Arc<str>,
    pub orchestrator: Arc<dyn Orchestrator>,
    pub policy: Policy,
    pub tools: Option<Arc<ToolRegistry>>,
    pub memory_manager: Option<Arc<MemoryManager>>,
    pub max_turns: usize,
    pub input_guardrails: Vec<OwnedInputGuardrailEntry>,
    pub output_guardrails: Vec<OwnedOutputGuardrailEntry>,
    pub tool_guardrails: Vec<OwnedToolGuardrailEntry>,
    /// Whether this sub-agent's conversation is seeded from the parent's
    /// history the first time it is delegated to (roadmap X6).
    ///
    /// Off by default. A sub-agent exists to work on a bounded task with a
    /// narrowed policy, and handing it the whole parent conversation costs
    /// tokens on every one of its turns and widens what a weaker model can
    /// see. Turn it on for the sub-agent that needs the discussion so far —
    /// a reviewer, an editor — not for the one that fetches a URL.
    pub seed_from_parent: bool,
    /// Authority this sub-agent holds beyond its parent, from
    /// `[[subagents.delegation_grants]]`.
    ///
    /// On the definition rather than on the registry so non-transitivity is
    /// structural: `prepare` narrows against the immediate parent with only
    /// this delegatee's grants, and a sub-agent of a sub-agent is narrowed
    /// with its own, never with these.
    pub delegation_grants: Vec<DelegationGrantTemplate>,
}

impl SubAgentDefinition {
    pub fn new(
        agent_id: AgentId,
        policy_id: impl Into<Arc<str>>,
        orchestrator: Arc<dyn Orchestrator>,
        policy: Policy,
    ) -> Self {
        Self {
            agent_id,
            policy_id: policy_id.into(),
            orchestrator,
            policy,
            tools: None,
            memory_manager: None,
            max_turns: 4,
            input_guardrails: Vec::new(),
            output_guardrails: Vec::new(),
            tool_guardrails: Vec::new(),
            seed_from_parent: false,
            delegation_grants: Vec::new(),
        }
    }

    /// Authority this sub-agent holds beyond its parent. Empty by default:
    /// a sub-agent gets nothing its parent does not have unless an operator
    /// wrote down why.
    pub fn with_delegation_grants(mut self, grants: Vec<DelegationGrantTemplate>) -> Self {
        self.delegation_grants = grants;
        self
    }

    pub fn with_tools(mut self, tools: Arc<ToolRegistry>) -> Self {
        self.tools = Some(tools);
        self
    }

    pub fn with_memory_manager(mut self, memory_manager: Arc<MemoryManager>) -> Self {
        self.memory_manager = Some(memory_manager);
        self
    }

    pub fn with_max_turns(mut self, max_turns: usize) -> Self {
        self.max_turns = max_turns;
        self
    }

    pub fn with_seed_from_parent(mut self, seed_from_parent: bool) -> Self {
        self.seed_from_parent = seed_from_parent;
        self
    }

    pub fn with_input_guardrail<T>(mut self, name: impl Into<Arc<str>>, guardrail: T) -> Self
    where
        T: InputGuardrail + 'static,
    {
        self.input_guardrails.push(OwnedInputGuardrailEntry {
            name: name.into(),
            guardrail: Arc::new(guardrail),
        });
        self
    }

    pub fn with_output_guardrail<T>(mut self, name: impl Into<Arc<str>>, guardrail: T) -> Self
    where
        T: OutputGuardrail + 'static,
    {
        self.output_guardrails.push(OwnedOutputGuardrailEntry {
            name: name.into(),
            guardrail: Arc::new(guardrail),
        });
        self
    }

    pub fn with_tool_guardrail<T>(mut self, name: impl Into<Arc<str>>, guardrail: T) -> Self
    where
        T: ToolGuardrail + 'static,
    {
        self.tool_guardrails.push(OwnedToolGuardrailEntry {
            name: name.into(),
            guardrail: Arc::new(guardrail),
        });
        self
    }
}

pub struct OwnedInputGuardrailEntry {
    pub name: Arc<str>,
    pub guardrail: Arc<dyn InputGuardrail>,
}

pub struct OwnedOutputGuardrailEntry {
    pub name: Arc<str>,
    pub guardrail: Arc<dyn OutputGuardrail>,
}

pub struct OwnedToolGuardrailEntry {
    pub name: Arc<str>,
    pub guardrail: Arc<dyn ToolGuardrail>,
}

#[derive(Debug)]
pub struct SubAgentRunOutput {
    pub agent_id: AgentId,
    pub policy_id: Arc<str>,
    pub state: agentos_interfaces::RunState,
    pub message: Message,
}

#[derive(Debug)]
pub struct SubAgentPausedRun {
    pub agent_id: AgentId,
    pub policy_id: Arc<str>,
    pub channel_id: ChannelId,
    pub conversation_id: ConversationId,
    pub state: agentos_interfaces::RunState,
}

#[derive(Debug)]
pub enum SubAgentRun {
    Finished(SubAgentRunOutput),
    Paused(SubAgentPausedRun),
}

pub struct SubAgentInvocation {
    definition: Arc<SubAgentDefinition>,
    policy: Policy,
    /// The grants narrowing had to invoke to admit this child's policy. The
    /// parent records their issuance; the child's `Approve` records each use.
    delegated_authority: DelegatedAuthority,
    input: Envelope,
    run_id: RunId,
    channel_capacity: usize,
    trace_sink: Option<Arc<dyn TraceSink>>,
    task_workspace: Option<Arc<TaskWorkspace>>,
    session: Option<Arc<dyn Session>>,
    /// Owned so a child's borrowed `ContentLimits` can outlive this call and
    /// live inside the child's `LocalSet` task.
    spill: Option<Arc<SpillStore>>,
    tool_result_inline_bytes: usize,
    summarizer: Option<Arc<dyn Llm>>,
    compaction_config: CompactionConfig,
    /// Cancels this child run. Set by [`SubAgentInvocation::with_cancel`] to a
    /// *child* of the parent run's token, so stopping the parent stops the
    /// whole delegation tree while a child stopping itself leaves the parent
    /// free to use whatever it produced.
    cancel: CancellationToken,
    /// Where this child's conversation would be seeded from, when the
    /// definition asks for it (roadmap X6). Supplied by the loop, which is
    /// what holds the parent run; whether it is used is the definition's call.
    parent_seed: Option<ParentSeed>,
    /// The parent's safety log, so a child's approvals, denials, and guardrail
    /// trips land in the same durable record as the parent's (M6 / `AUD-001`).
    safety_log: Option<Arc<dyn SafetyLog>>,
}

mod branch;

const DELEGATION_SCOPE_KEY: &str = "agentos_delegation_scope";

use branch::seed_from_parent;
pub use branch::{child_input_envelope, child_run_id, parent_conversation_id, parent_principal};
pub(crate) use branch::{ChildIdentitySource, CHILD_IDENTITY_SOURCE_KEY};

/// The point in a parent conversation a child is branched from.
#[derive(Clone, Debug)]
pub struct ParentSeed {
    /// The parent conversation, as a principal rather than a bare id — the
    /// fork has to name *which* agent's `telegram:42` it is copying from
    /// (M3 deliverable 2).
    pub principal: ConversationPrincipal,
    /// Items of the parent's log to copy. Taken from the parent's *in-memory*
    /// transcript, which is the delegation point as the parent sees it; the
    /// store holds a prefix of that, because the turn in flight is not
    /// persisted until it finishes. `Session::fork` copies what exists.
    pub boundary: usize,
}

pub struct SubAgentRegistry {
    definitions: BTreeMap<(AgentId, Arc<str>), Arc<SubAgentDefinition>>,
    channel_capacity: usize,
    trace_sink: Option<Arc<dyn TraceSink>>,
    task_workspace: Option<Arc<TaskWorkspace>>,
    session: Option<Arc<dyn Session>>,
    spill: Option<Arc<SpillStore>>,
    tool_result_inline_bytes: usize,
    summarizer: Option<Arc<dyn Llm>>,
    compaction_config: CompactionConfig,
    safety_log: Option<Arc<dyn SafetyLog>>,
}

impl Default for SubAgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SubAgentRegistry {
    pub fn new() -> Self {
        Self {
            definitions: BTreeMap::new(),
            channel_capacity: 1,
            trace_sink: None,
            task_workspace: None,
            spill: None,
            tool_result_inline_bytes: DEFAULT_TOOL_RESULT_INLINE_BYTES,
            summarizer: None,
            compaction_config: CompactionConfig::default(),
            session: None,
            safety_log: None,
        }
    }

    /// Share the parent's safety-event store. Without it a sub-agent's
    /// decisions are logged and traced but not durably recorded, which is the
    /// gap M6 closes for the parent — a delegation must not be the way around
    /// it.
    pub fn with_safety_log(mut self, safety_log: Option<Arc<dyn SafetyLog>>) -> Self {
        self.safety_log = safety_log;
        self
    }

    pub fn with_trace_sink(mut self, trace_sink: Arc<dyn TraceSink>) -> Self {
        self.trace_sink = Some(trace_sink);
        self
    }

    pub fn with_task_workspace(mut self, task_workspace: Arc<TaskWorkspace>) -> Self {
        self.task_workspace = Some(task_workspace);
        self
    }

    /// Give children the parent's spill store and inline cap, so a sub-agent's
    /// oversized tool output is as recoverable as the parent's.
    pub fn with_content_limits(
        mut self,
        spill: Option<Arc<SpillStore>>,
        tool_result_inline_bytes: usize,
    ) -> Self {
        self.spill = spill;
        self.tool_result_inline_bytes = tool_result_inline_bytes;
        self
    }

    /// Give children the parent's summarizer and trigger. A sub-agent runs the
    /// same loop against the same window, so it outgrows its context the same
    /// way the parent does.
    pub fn with_compaction(
        mut self,
        summarizer: Option<Arc<dyn Llm>>,
        compaction_config: CompactionConfig,
    ) -> Self {
        self.summarizer = summarizer;
        self.compaction_config = compaction_config;
        self
    }

    /// Inject a persistent `Session` shared with the parent runtime so
    /// sub-agents accumulate their own transcript across turns. Without this,
    /// each invocation runs against a fresh `InMemorySession` and loses
    /// context as soon as the run returns.
    pub fn with_session(mut self, session: Arc<dyn Session>) -> Self {
        self.session = Some(session);
        self
    }

    pub fn register(&mut self, definition: SubAgentDefinition) {
        self.definitions.insert(
            (
                definition.agent_id.clone(),
                Arc::clone(&definition.policy_id),
            ),
            Arc::new(definition),
        );
    }

    pub fn prepare(
        &self,
        spec: &SubAgentSpec,
        parent_policy: &Policy,
        initiating_actor: ActorPrincipal,
        mut input: Envelope,
        run_id: RunId,
    ) -> Result<SubAgentInvocation, SubAgentError> {
        let definition = self
            .definitions
            .get(&(spec.agent_id.clone(), Arc::clone(&spec.policy_id)))
            .cloned()
            .ok_or_else(|| SubAgentError::Unknown {
                agent_id: spec.agent_id.clone(),
                policy_id: Arc::clone(&spec.policy_id),
            })?;
        let issued_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| SubAgentError::ClockUnavailable)?
            .as_secs();
        let scope = DelegationScope::mint(
            initiating_actor,
            definition.agent_id.clone(),
            Arc::clone(&definition.policy_id),
            issued_at,
        )?;
        input.metadata.insert(
            Arc::from(DELEGATION_SCOPE_KEY),
            serde_json::to_value(&scope).map_err(|error| {
                SubAgentError::InvalidDelegationScope(Arc::from(error.to_string()))
            })?,
        );
        input.metadata.insert(
            Arc::from("kind"),
            serde_json::Value::String("subagent_input".to_owned()),
        );
        self.prepare_bound(definition, parent_policy, input, run_id, scope, issued_at)
    }

    pub fn prepare_resume(
        &self,
        spec: &SubAgentSpec,
        parent_policy: &Policy,
        initiating_actor: ActorPrincipal,
        input: Envelope,
        run_id: RunId,
        paused_state: &agentos_interfaces::RunState,
    ) -> Result<SubAgentInvocation, SubAgentError> {
        let definition = self
            .definitions
            .get(&(spec.agent_id.clone(), Arc::clone(&spec.policy_id)))
            .cloned()
            .ok_or_else(|| SubAgentError::Unknown {
                agent_id: spec.agent_id.clone(),
                policy_id: Arc::clone(&spec.policy_id),
            })?;
        let scope_value = paused_state
            .transcript
            .items
            .iter()
            // The child input carrying the kernel-written scope follows any
            // parent history that was seeded into the conversation. Search
            // newest-first so untrusted historical metadata cannot shadow it.
            .rev()
            .find(|item| {
                item.message.role == agentos_proto::MessageRole::User
                    && item
                        .metadata
                        .get("kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("subagent_input")
            })
            .and_then(|item| item.metadata.get(DELEGATION_SCOPE_KEY))
            .cloned()
            .ok_or(SubAgentError::MissingDelegationScope)?;
        let scope: DelegationScope = serde_json::from_value(scope_value)
            .map_err(|error| SubAgentError::InvalidDelegationScope(Arc::from(error.to_string())))?;
        scope.validate_binding(
            &initiating_actor,
            &definition.agent_id,
            &definition.policy_id,
        )?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| SubAgentError::ClockUnavailable)?
            .as_secs();
        self.prepare_bound(definition, parent_policy, input, run_id, scope, now)
    }

    fn prepare_bound(
        &self,
        definition: Arc<SubAgentDefinition>,
        parent_policy: &Policy,
        input: Envelope,
        run_id: RunId,
        scope: DelegationScope,
        now: u64,
    ) -> Result<SubAgentInvocation, SubAgentError> {
        let authority =
            DelegatedAuthority::issue(&definition.delegation_grants, parent_policy, scope)?;
        let narrowed =
            Policy::narrow_with_grants(parent_policy, &definition.policy, &authority, now)?;
        // X5: state the security property over the policy the child run is
        // actually handed, rather than trusting that the call above produced
        // it. Independent of how `narrow` decides individual rules.
        #[cfg(debug_assertions)]
        crate::invariants::delegation_narrows(parent_policy, &narrowed.policy);
        Ok(SubAgentInvocation {
            definition,
            policy: narrowed.policy,
            delegated_authority: authority.restricted(narrowed.grants_relied_on),
            input,
            run_id,
            channel_capacity: self.channel_capacity,
            trace_sink: self.trace_sink.clone(),
            task_workspace: self.task_workspace.clone(),
            session: self.session.clone(),
            spill: self.spill.clone(),
            tool_result_inline_bytes: self.tool_result_inline_bytes,
            summarizer: self.summarizer.clone(),
            compaction_config: self.compaction_config,
            // A fresh token until the caller links it to a parent run; the
            // loop always does.
            cancel: CancellationToken::new(),
            parent_seed: None,
            safety_log: self.safety_log.clone(),
        })
    }
}

impl SubAgentInvocation {
    /// The grants narrowing had to invoke to admit this delegation. The
    /// caller records their issuance — it is the one that holds the parent
    /// run, and therefore the principal the grant was exercised for.
    pub fn grants_relied_on(&self) -> &[crate::approve::DelegationGrant] {
        self.delegated_authority.grants()
    }

    /// Runtime authority bound to this exact delegation generation.
    pub fn delegated_authority(&self) -> &DelegatedAuthority {
        &self.delegated_authority
    }

    /// Tie this child run to `parent`, so cancelling the parent cancels it.
    pub fn with_cancel(mut self, parent: &CancellationToken) -> Self {
        self.cancel = parent.child_token();
        self
    }

    /// Offer the parent conversation this child could branch from. Ignored
    /// unless the sub-agent's definition sets `seed_from_parent`.
    pub fn with_parent_seed(mut self, seed: ParentSeed) -> Self {
        self.parent_seed = Some(seed);
        self
    }

    pub async fn run(self) -> Result<SubAgentRun, SubAgentError> {
        let (input_tx, mut input_rx) = mpsc::channel(self.channel_capacity);
        let (output_tx, mut output_rx) = mpsc::channel(self.channel_capacity);

        input_tx
            .send(self.input)
            .await
            .map_err(|_| SubAgentError::ChannelClosed)?;

        let definition = self.definition;
        let child_policy = self.policy;
        let delegated_authority = self.delegated_authority;
        let safety_log = self.safety_log;
        let run_id = self.run_id;
        let trace_sink = self.trace_sink;
        let task_workspace = self.task_workspace;
        let injected_session = self.session;
        let spill = self.spill;
        let tool_result_inline_bytes = self.tool_result_inline_bytes;
        let summarizer = self.summarizer;
        let compaction_config = self.compaction_config;
        let cancel = self.cancel;
        let parent_seed = self.parent_seed;
        let local = LocalSet::new();
        let handle = local.spawn_local(async move {
            let Some(input) = input_rx.recv().await else {
                return Err(SubAgentError::ChannelClosed);
            };
            // Prefer the runtime-injected session (persistent, shared with the
            // parent) so sub-agents accumulate context across turns. Fall back
            // to an ephemeral in-memory session for callers that didn't wire
            // one in (tests, embedded uses).
            let fallback_session = if injected_session.is_none() {
                Some(InMemorySession::default())
            } else {
                None
            };
            let session: &dyn Session = match (&injected_session, &fallback_session) {
                (Some(persistent), _) => persistent.as_ref(),
                (None, Some(ephemeral)) => ephemeral,
                _ => unreachable!("either injected or fallback session is set"),
            };
            let child_channel_id = input.channel_id.clone();
            let child_conversation_id = input.conversation_id.clone();
            // X6: branch this conversation from the parent's before the run
            // loads it. Only ever on the first delegation — `fork` refuses a
            // target that already holds items, which is exactly the second
            // turn of a sub-agent whose conversation id is stable.
            if definition.seed_from_parent {
                seed_from_parent(session, parent_seed.as_ref(), &input, &definition.agent_id).await;
            }
            let input_guardrails = definition
                .input_guardrails
                .iter()
                .map(|entry| InputGuardrailEntry {
                    name: Arc::clone(&entry.name),
                    guardrail: entry.guardrail.as_ref(),
                })
                .collect::<Vec<_>>();
            let output_guardrails = definition
                .output_guardrails
                .iter()
                .map(|entry| OutputGuardrailEntry {
                    name: Arc::clone(&entry.name),
                    guardrail: entry.guardrail.as_ref(),
                })
                .collect::<Vec<_>>();
            let tool_guardrails = definition
                .tool_guardrails
                .iter()
                .map(|entry| ToolGuardrailEntry {
                    name: Arc::clone(&entry.name),
                    guardrail: entry.guardrail.as_ref(),
                })
                .collect::<Vec<_>>();
            let deps = RunnerDeps {
                orchestrator: definition.orchestrator.as_ref(),
                session,
                memory_manager: definition.memory_manager.as_deref(),
                hooks: None,
                max_turns: definition.max_turns,
                active_agent: definition.agent_id.clone(),
                tools: definition.tools.as_deref(),
                trace_sink: trace_sink.as_deref(),
                task_workspace: task_workspace.as_deref(),
                policy: &child_policy,
                subagents: None,
                input_guardrails: &input_guardrails,
                output_guardrails: &output_guardrails,
                tool_guardrails: &tool_guardrails,
                content_limits: ContentLimits {
                    tool_result_inline_bytes,
                    spill: spill.as_deref(),
                },
                compaction: Compaction {
                    summarizer: summarizer.as_deref(),
                    config: compaction_config,
                },
                cancel: cancel.clone(),
                // A sub-agent is not a conversation: nothing routes user input to
                // it, so there is nothing to steer it with.
                steering: None,
                // Sub-agents never stream to the parent's egress.
                stream_sink: None,
                safety_log: safety_log.as_deref(),
                delegated_authority: Some(&delegated_authority),
            };
            let result = match run_envelope(input, run_id, &deps).await {
                Ok(RunOutcome::Finished { state, output }) => {
                    Ok(SubAgentRun::Finished(SubAgentRunOutput {
                        agent_id: definition.agent_id.clone(),
                        policy_id: Arc::clone(&definition.policy_id),
                        state,
                        message: output.message,
                    }))
                }
                Ok(RunOutcome::Paused(state)) => Ok(SubAgentRun::Paused(SubAgentPausedRun {
                    agent_id: definition.agent_id.clone(),
                    policy_id: Arc::clone(&definition.policy_id),
                    channel_id: child_channel_id,
                    conversation_id: child_conversation_id,
                    state,
                })),
                Err(err) => Err(SubAgentError::Run(Arc::from(err.to_string()))),
            };
            output_tx
                .send(result)
                .await
                .map_err(|_| SubAgentError::ChannelClosed)
        });

        local
            .run_until(async move {
                let output = output_rx.recv().await.ok_or(SubAgentError::ChannelClosed)?;
                handle
                    .await
                    .map_err(|err| SubAgentError::Task(Arc::from(err.to_string())))??;
                output
            })
            .await
    }

    pub async fn resume(
        self,
        paused: SubAgentPausedRun,
        witness: ResumeWitness,
    ) -> Result<SubAgentRun, SubAgentError> {
        let definition = self.definition;
        let child_policy = self.policy;
        let delegated_authority = self.delegated_authority;
        let safety_log = self.safety_log;
        let trace_sink = self.trace_sink;
        let task_workspace = self.task_workspace;
        let injected_session = self.session;
        let spill = self.spill;
        let tool_result_inline_bytes = self.tool_result_inline_bytes;
        let summarizer = self.summarizer;
        let compaction_config = self.compaction_config;
        let cancel = self.cancel;
        let local = LocalSet::new();
        local
            .run_until(async move {
                let fallback_session = if injected_session.is_none() {
                    Some(InMemorySession::default())
                } else {
                    None
                };
                let session: &dyn Session = match (&injected_session, &fallback_session) {
                    (Some(persistent), _) => persistent.as_ref(),
                    (None, Some(ephemeral)) => ephemeral,
                    _ => unreachable!("either injected or fallback session is set"),
                };
                let input_guardrails = definition
                    .input_guardrails
                    .iter()
                    .map(|entry| InputGuardrailEntry {
                        name: Arc::clone(&entry.name),
                        guardrail: entry.guardrail.as_ref(),
                    })
                    .collect::<Vec<_>>();
                let output_guardrails = definition
                    .output_guardrails
                    .iter()
                    .map(|entry| OutputGuardrailEntry {
                        name: Arc::clone(&entry.name),
                        guardrail: entry.guardrail.as_ref(),
                    })
                    .collect::<Vec<_>>();
                let tool_guardrails = definition
                    .tool_guardrails
                    .iter()
                    .map(|entry| ToolGuardrailEntry {
                        name: Arc::clone(&entry.name),
                        guardrail: entry.guardrail.as_ref(),
                    })
                    .collect::<Vec<_>>();
                let deps = RunnerDeps {
                    orchestrator: definition.orchestrator.as_ref(),
                    session,
                    memory_manager: definition.memory_manager.as_deref(),
                    hooks: None,
                    max_turns: definition.max_turns,
                    active_agent: definition.agent_id.clone(),
                    tools: definition.tools.as_deref(),
                    trace_sink: trace_sink.as_deref(),
                    task_workspace: task_workspace.as_deref(),
                    policy: &child_policy,
                    subagents: None,
                    input_guardrails: &input_guardrails,
                    output_guardrails: &output_guardrails,
                    tool_guardrails: &tool_guardrails,
                    stream_sink: None,
                    content_limits: ContentLimits {
                        tool_result_inline_bytes,
                        spill: spill.as_deref(),
                    },
                    compaction: Compaction {
                        summarizer: summarizer.as_deref(),
                        config: compaction_config,
                    },
                    cancel: cancel.clone(),
                    steering: None,
                    safety_log: safety_log.as_deref(),
                    delegated_authority: Some(&delegated_authority),
                };
                let paused_run = PausedRun {
                    channel_id: paused.channel_id.clone(),
                    conversation_id: paused.conversation_id.clone(),
                    state: paused.state,
                };
                match resume_run(paused_run, witness, &deps).await {
                    Ok(RunOutcome::Finished { state, output }) => {
                        Ok(SubAgentRun::Finished(SubAgentRunOutput {
                            agent_id: definition.agent_id.clone(),
                            policy_id: Arc::clone(&definition.policy_id),
                            state,
                            message: output.message,
                        }))
                    }
                    Ok(RunOutcome::Paused(state)) => Ok(SubAgentRun::Paused(SubAgentPausedRun {
                        agent_id: definition.agent_id.clone(),
                        policy_id: Arc::clone(&definition.policy_id),
                        channel_id: paused.channel_id,
                        conversation_id: paused.conversation_id,
                        state,
                    })),
                    Err(err) => Err(SubAgentError::Run(Arc::from(err.to_string()))),
                }
            })
            .await
    }
}
