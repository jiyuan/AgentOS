use crate::approve::{Policy, PolicyError};
use crate::config::CompactionConfig;
use crate::memory::{InMemorySession, MemoryManager};
use crate::prompt::Compaction;
use crate::r#loop::{InputGuardrailEntry, OutputGuardrailEntry, ToolGuardrailEntry};
use crate::runner::{
    resume_run, run_envelope, PausedRun, ResumeDecision, RunOutcome, RunnerDeps, TraceSink,
};
use crate::spill::{ContentLimits, SpillStore, DEFAULT_TOOL_RESULT_INLINE_BYTES};
use crate::task_workspace::TaskWorkspace;
use crate::tools::ToolRegistry;
use agentos_interfaces::guardrail::{InputGuardrail, OutputGuardrail, ToolGuardrail};
use agentos_interfaces::orchestrator::{Orchestrator, SubAgentSpec};
use agentos_interfaces::session::Session;
use agentos_llm::Llm;
use agentos_proto::{AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::LocalSet;

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
        }
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
        }
    }

    pub fn with_channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
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
        input: Envelope,
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
        let child_policy = Policy::narrow(parent_policy, &definition.policy)?;
        Ok(SubAgentInvocation {
            definition,
            policy: child_policy,
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
        })
    }
}

impl SubAgentInvocation {
    pub async fn run(self) -> Result<SubAgentRun, SubAgentError> {
        let (input_tx, mut input_rx) = mpsc::channel(self.channel_capacity);
        let (output_tx, mut output_rx) = mpsc::channel(self.channel_capacity);

        input_tx
            .send(self.input)
            .await
            .map_err(|_| SubAgentError::ChannelClosed)?;

        let definition = self.definition;
        let child_policy = self.policy;
        let run_id = self.run_id;
        let trace_sink = self.trace_sink;
        let task_workspace = self.task_workspace;
        let injected_session = self.session;
        let spill = self.spill;
        let tool_result_inline_bytes = self.tool_result_inline_bytes;
        let summarizer = self.summarizer;
        let compaction_config = self.compaction_config;
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
                // Sub-agents never stream to the parent's egress.
                stream_sink: None,
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
        decision: ResumeDecision,
    ) -> Result<SubAgentRun, SubAgentError> {
        let definition = self.definition;
        let child_policy = self.policy;
        let trace_sink = self.trace_sink;
        let task_workspace = self.task_workspace;
        let injected_session = self.session;
        let spill = self.spill;
        let tool_result_inline_bytes = self.tool_result_inline_bytes;
        let summarizer = self.summarizer;
        let compaction_config = self.compaction_config;
        let child_approval_id = paused
            .state
            .pending_approvals
            .first()
            .map(|approval| approval.id.clone())
            .ok_or_else(|| SubAgentError::Run(Arc::from("paused child has no pending approval")))?;
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
                };
                let paused_run = PausedRun {
                    channel_id: paused.channel_id.clone(),
                    conversation_id: paused.conversation_id.clone(),
                    state: paused.state,
                };
                match resume_run(paused_run, &child_approval_id, decision, &deps).await {
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

pub fn child_input_envelope(
    spec: &SubAgentSpec,
    parent_state: &agentos_interfaces::RunState,
) -> Envelope {
    let prompt = spec.metadata.get("prompt").and_then(Value::as_str);
    let parent_last = parent_state
        .transcript
        .items
        .last()
        .map(|item| &item.message);
    let message = match (prompt, parent_last) {
        // Routing handoff supplied a prompt string, and the parent's last
        // turn is the user's inbound message — carry that message's
        // attachments forward so the sub-agent can actually read the files
        // the user just sent.
        (Some(prompt), Some(parent_msg)) if parent_msg.role == MessageRole::User => {
            Message::with_attachments(MessageRole::User, prompt, parent_msg.attachments.clone())
        }
        (Some(prompt), _) => Message::text(MessageRole::User, prompt),
        (None, Some(parent_msg)) => parent_msg.clone(),
        (None, None) => Message::text(MessageRole::User, ""),
    };
    let mut metadata = spec.metadata.clone();
    metadata.insert(
        Arc::from("kind"),
        Value::String("subagent_input".to_owned()),
    );
    metadata.insert(
        Arc::from("parent_run_id"),
        Value::String(parent_state.run_id.as_str().to_owned()),
    );

    // Stable conversation id: derived from the parent's *user-conversation*
    // id (read off the most recent transcript item's metadata, where the
    // runner stamps it on every inbound). Sticking to parent_run_id here
    // would mint a new sub-agent conversation every turn and the persistent
    // session would never accumulate history.
    let parent_conversation_id = parent_state
        .transcript
        .items
        .last()
        .and_then(|item| item.metadata.get("conversation_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| parent_state.run_id.as_str().to_owned());

    let conversation_suffix = prompt
        .filter(|prompt| !prompt.trim().is_empty())
        .map(|prompt| format!(":{}", prompt_fingerprint(prompt)))
        .unwrap_or_default();

    Envelope {
        channel_id: ChannelId::new(format!("subagent:{}", spec.agent_id.as_str())),
        conversation_id: ConversationId::new(format!(
            "{}:{}{}",
            parent_conversation_id,
            spec.agent_id.as_str(),
            conversation_suffix
        )),
        sender: Arc::from(parent_state.active_agent.as_str()),
        message,
        metadata,
    }
}

fn prompt_fingerprint(prompt: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in prompt.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub fn child_run_id(spec: &SubAgentSpec, parent_state: &agentos_interfaces::RunState) -> RunId {
    RunId::new(format!(
        "{}:{}",
        parent_state.run_id.as_str(),
        spec.agent_id.as_str()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentos_interfaces::session::Item;
    use agentos_interfaces::RunState;
    use agentos_proto::{AgentId, Attachment, AttachmentKind, MessageRole, RunId as ProtoRunId};
    use std::path::PathBuf;

    fn user_message_with_attachment() -> agentos_proto::Message {
        agentos_proto::Message::with_attachments(
            MessageRole::User,
            "look at this",
            vec![Attachment {
                kind: AttachmentKind::Document,
                name: Arc::from("SKILL.md"),
                path: PathBuf::from("workspace/attachments/telegram/1/2/SKILL.md"),
                mime: None,
                size: Some(2448),
                source: Some(Arc::from("file_xyz")),
            }],
        )
    }

    fn parent_state_with_user_message() -> RunState {
        parent_state_with_run_and_conv("parent-run", "tg-12345")
    }

    fn parent_state_with_run_and_conv(run_id: &str, conv_id: &str) -> RunState {
        let mut state = RunState::new(ProtoRunId::new(run_id), AgentId::new("parent-agent"));
        let mut metadata = BTreeMap::new();
        metadata.insert(
            Arc::from("conversation_id"),
            Value::String(conv_id.to_owned()),
        );
        state.transcript.items.push(Item {
            message: user_message_with_attachment(),
            metadata,
        });
        state
    }

    fn spec_with_prompt(prompt: &str) -> SubAgentSpec {
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(Arc::from("prompt"), Value::String(prompt.to_owned()));
        SubAgentSpec {
            agent_id: AgentId::new("worker"),
            policy_id: Arc::from("default"),
            metadata,
        }
    }

    #[test]
    fn child_envelope_carries_parent_attachments_when_prompt_present() {
        let parent = parent_state_with_user_message();
        let spec = spec_with_prompt("hi");
        let envelope = child_input_envelope(&spec, &parent);
        assert_eq!(envelope.message.content.as_ref(), "hi");
        assert_eq!(envelope.message.attachments.len(), 1);
        assert_eq!(envelope.message.attachments[0].name.as_ref(), "SKILL.md");
    }

    #[test]
    fn child_envelope_uses_parent_message_when_no_prompt() {
        let parent = parent_state_with_user_message();
        let spec = SubAgentSpec {
            agent_id: AgentId::new("worker"),
            policy_id: Arc::from("default"),
            metadata: Default::default(),
        };
        let envelope = child_input_envelope(&spec, &parent);
        assert_eq!(envelope.message.content.as_ref(), "look at this");
        assert_eq!(envelope.message.attachments.len(), 1);
    }

    #[test]
    fn child_envelope_skips_attachments_when_parent_is_not_user_turn() {
        let mut parent = RunState::new(ProtoRunId::new("parent-run"), AgentId::new("parent-agent"));
        // Assistant turn — no attachments to inherit.
        parent.transcript.items.push(Item {
            message: agentos_proto::Message::text(MessageRole::Assistant, "previous reply"),
            metadata: Default::default(),
        });
        let spec = spec_with_prompt("now do this");
        let envelope = child_input_envelope(&spec, &parent);
        assert_eq!(envelope.message.content.as_ref(), "now do this");
        assert_eq!(envelope.message.attachments.len(), 0);
    }

    #[test]
    fn child_envelope_uses_parent_conversation_id() {
        let parent = parent_state_with_user_message();
        let spec = spec_with_prompt("hi");
        let envelope = child_input_envelope(&spec, &parent);
        assert_eq!(
            envelope.conversation_id.as_str(),
            "tg-12345:worker:08ba5f07b55ec3da"
        );
    }

    #[test]
    fn child_envelope_conv_id_stable_across_parent_run_ids() {
        let turn1 = parent_state_with_run_and_conv("parent-run-1", "tg-12345");
        let turn2 = parent_state_with_run_and_conv("parent-run-2", "tg-12345");
        let spec = spec_with_prompt("hi");
        let env1 = child_input_envelope(&spec, &turn1);
        let env2 = child_input_envelope(&spec, &turn2);
        assert_eq!(env1.conversation_id, env2.conversation_id);
    }

    #[test]
    fn child_envelope_conv_id_separates_distinct_prompts() {
        let parent = parent_state_with_user_message();
        let first = child_input_envelope(&spec_with_prompt("fix audit skill"), &parent);
        let second = child_input_envelope(&spec_with_prompt("stop the approve"), &parent);

        assert_ne!(first.conversation_id, second.conversation_id);
    }

    #[test]
    fn child_envelope_falls_back_when_parent_metadata_missing() {
        // No conversation_id in the parent's transcript metadata — fall back
        // to the old run-id-derived form so embedded callers without a runner
        // continue to work.
        let mut parent = RunState::new(ProtoRunId::new("parent-run"), AgentId::new("parent-agent"));
        parent.transcript.items.push(Item {
            message: user_message_with_attachment(),
            metadata: Default::default(),
        });
        let spec = spec_with_prompt("hi");
        let envelope = child_input_envelope(&spec, &parent);
        assert_eq!(
            envelope.conversation_id.as_str(),
            "parent-run:worker:08ba5f07b55ec3da"
        );
    }
}
