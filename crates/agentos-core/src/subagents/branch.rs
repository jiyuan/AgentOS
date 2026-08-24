//! Branching a sub-agent's conversation from its parent's, and naming the
//! child's run (roadmap X6).
//!
//! Split out of `subagents/mod.rs` when M3 deliverable 2 pushed that file past
//! the module ceiling. The four things here answer one question between them —
//! *which* conversation a delegation comes from and *which* one it goes to —
//! and getting that wrong is now a keying bug rather than a naming one: a fork
//! written under a principal the child will not load with leaves the sub-agent
//! silently starting from nothing.

use super::{ParentSeed, SubAgentSpec, SESSION_SCOPE_EPHEMERAL, SESSION_SCOPE_KEY};
use agentos_interfaces::session::Session;
use agentos_proto::{
    base64url, AgentId, ChannelId, ConversationId, ConversationPrincipal, Envelope, Message,
    MessageRole, Principal, RunId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Version of the child-conversation source tuple encoded below.
///
/// This is separate from `PRINCIPAL_VERSION`: a principal and a delegation
/// branch are different stored identities and may evolve independently. The
/// rendered prefix makes the version visible in session keys and migration
/// reports instead of leaving a future reader to infer it.
const CHILD_CONVERSATION_VERSION: u8 = 1;
const CHILD_CONVERSATION_DOMAIN: &[u8] = b"agentos.child-conversation";
pub(crate) const CHILD_IDENTITY_SOURCE_KEY: &str = "subagent_identity_source";

/// The complete source tuple persisted beside a delegated input.
///
/// The rendered child id is already injective. Persisting this tuple is for
/// migration and operator diagnostics: a pre-versioned session key may be
/// rekeyed only when its rows state every input the new derivation requires.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ChildIdentitySource {
    version: u8,
    parent: String,
    child_agent: AgentId,
    policy_id: Arc<str>,
    task: ChildTaskDiscriminator,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum ChildTaskDiscriminator {
    Default,
    TaskId(Arc<str>),
    Prompt(Arc<str>),
}

impl ChildIdentitySource {
    fn from_spec(spec: &SubAgentSpec, parent_state: &agentos_interfaces::RunState) -> Self {
        let task_id = spec
            .metadata
            .get("task_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let prompt = spec
            .metadata
            .get("prompt")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let task = match (task_id, prompt) {
            (Some(task_id), _) => ChildTaskDiscriminator::TaskId(Arc::from(task_id)),
            (None, Some(prompt)) => ChildTaskDiscriminator::Prompt(Arc::from(prompt)),
            (None, None) => ChildTaskDiscriminator::Default,
        };
        Self {
            version: CHILD_CONVERSATION_VERSION,
            parent: parent_principal(parent_state).storage_name(),
            child_agent: spec.agent_id.clone(),
            policy_id: Arc::clone(&spec.policy_id),
            task,
        }
    }

    pub(crate) fn conversation_id(&self) -> Option<ConversationId> {
        if self.version != CHILD_CONVERSATION_VERSION {
            return None;
        }
        let parent =
            ConversationPrincipal::try_from(Principal::from_storage_name(&self.parent)?).ok()?;
        let mut source = Vec::new();
        push_child_component(&mut source, CHILD_CONVERSATION_DOMAIN);
        source.push(self.version);
        push_child_component(&mut source, &parent.canonical_bytes());
        push_child_component(&mut source, self.child_agent.as_str().as_bytes());
        push_child_component(&mut source, self.policy_id.as_bytes());
        match &self.task {
            ChildTaskDiscriminator::Default => source.push(0),
            ChildTaskDiscriminator::TaskId(task_id) => {
                source.push(1);
                push_child_component(&mut source, task_id.as_bytes());
            }
            ChildTaskDiscriminator::Prompt(prompt) => {
                source.push(2);
                push_child_component(&mut source, prompt.as_bytes());
            }
        }
        Some(ConversationId::new(format!(
            "child.v{CHILD_CONVERSATION_VERSION}.{}",
            base64url(&source)
        )))
    }

    pub(crate) fn child_agent(&self) -> &AgentId {
        &self.child_agent
    }
}

/// Branch a sub-agent's conversation from its parent's, once (roadmap X6).
///
/// Best effort, in the same sense as the spill store: a seed that does not
/// happen leaves the sub-agent starting from an empty conversation, which is
/// exactly the behaviour it had before this existed. Failing the delegation
/// because history could not be copied would trade a working sub-agent for no
/// sub-agent.
///
/// The two expected non-seeds are not failures and are logged as such: an
/// ephemeral input has no conversation to seed, and a target that already
/// holds items is the second and every later turn of a sub-agent whose
/// conversation id is stable across a conversation.
pub(super) async fn seed_from_parent(
    session: &dyn Session,
    seed: Option<&ParentSeed>,
    input: &Envelope,
    child_agent: &AgentId,
) {
    let Some(seed) = seed else {
        return;
    };
    if input
        .metadata
        .get(SESSION_SCOPE_KEY)
        .and_then(Value::as_str)
        == Some(SESSION_SCOPE_EPHEMERAL)
    {
        return;
    }
    // The child's key must be the one its own run will load with, which is
    // built from the sub-agent's `agent_id` — not the parent's. A mismatch
    // here would fork into a conversation nobody ever reads and leave the
    // sub-agent starting empty, silently.
    let child = ConversationPrincipal::new(
        child_agent.clone(),
        input.channel_id.clone(),
        input.conversation_id.clone(),
    );
    match session
        .fork(
            seed.principal.as_principal(),
            seed.boundary,
            child.as_principal(),
        )
        .await
    {
        Ok(items) => tracing::info!(
            parent_conversation = %seed.principal.storage_name(),
            child_conversation = %child.storage_name(),
            items,
            "sub-agent conversation seeded from parent"
        ),
        Err(error) => tracing::debug!(
            parent_conversation = %seed.principal.storage_name(),
            child_conversation = %child.storage_name(),
            error = %error,
            "sub-agent conversation not seeded; starting from its own history"
        ),
    }
}

/// The user-facing conversation a parent run belongs to.
///
/// Read off the most recent transcript item's metadata, where the runner stamps
/// it on every inbound. Falls back to the run id, which is what a run started
/// outside a channel has.
/// The parent conversation this delegation branches from, as a principal.
///
/// Agent from the parent's own `active_agent`; channel and conversation from
/// the metadata the runner stamps onto every input item. The fallbacks are the
/// ones [`parent_conversation_id`] already used — a run with no such metadata
/// is a test or an embedded caller, and keying it on the run id keeps it
/// distinct from everything else rather than merging it with everything else.
pub fn parent_principal(parent_state: &agentos_interfaces::RunState) -> ConversationPrincipal {
    let field = |name: &str| {
        parent_state
            .transcript
            .items
            .last()
            .and_then(|item| item.metadata.get(name))
            .and_then(Value::as_str)
            .map(Arc::<str>::from)
    };
    ConversationPrincipal::new(
        parent_state.active_agent.clone(),
        field("channel_id").map_or_else(|| ChannelId::new("unknown"), ChannelId::new),
        parent_conversation_id(parent_state),
    )
}

pub fn parent_conversation_id(parent_state: &agentos_interfaces::RunState) -> ConversationId {
    parent_state
        .transcript
        .items
        .last()
        .and_then(|item| item.metadata.get("conversation_id"))
        .and_then(Value::as_str)
        .map(ConversationId::new)
        .unwrap_or_else(|| ConversationId::new(parent_state.run_id.as_str()))
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
    let source = ChildIdentitySource::from_spec(spec, parent_state);
    metadata.insert(
        Arc::from(CHILD_IDENTITY_SOURCE_KEY),
        serde_json::to_value(&source)
            .expect("the child identity source contains only serializable wire values"),
    );

    Envelope {
        channel_id: ChannelId::new(format!("subagent:{}", spec.agent_id.as_str())),
        conversation_id: child_conversation_id(spec, parent_state),
        sender: Arc::from(parent_state.active_agent.as_str()),
        message,
        metadata,
    }
}

/// Derive the persistent conversation a delegated task owns (`ID-003`).
///
/// The old key was `parent_conversation:child_agent:FNV(prompt)`. It omitted
/// the parent agent, parent channel, and child policy, and the non-cryptographic
/// fixed-width prompt hash could collide. This encoding commits injectively to
/// the complete senderless parent principal, the registered child definition
/// `(agent_id, policy_id)`, and one stable task discriminator. Components are
/// length-prefixed bytes and the complete tuple is rendered as unpadded
/// base64url, so component contents cannot impersonate boundaries.
fn child_conversation_id(
    spec: &SubAgentSpec,
    parent_state: &agentos_interfaces::RunState,
) -> ConversationId {
    ChildIdentitySource::from_spec(spec, parent_state)
        .conversation_id()
        .expect("a freshly constructed child identity source is versioned and senderless")
}

fn push_child_component(into: &mut Vec<u8>, component: &[u8]) {
    let length = u64::try_from(component.len())
        .expect("a Rust slice length always fits in the u64 delegation encoding");
    into.extend_from_slice(&length.to_be_bytes());
    into.extend_from_slice(component);
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
    use std::collections::BTreeMap;
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
        parent_state_with_identity("parent-run", "parent-agent", "telegram", "tg-12345")
    }

    fn parent_state_with_run_and_conv(run_id: &str, conv_id: &str) -> RunState {
        parent_state_with_identity(run_id, "parent-agent", "telegram", conv_id)
    }

    fn parent_state_with_identity(
        run_id: &str,
        agent_id: &str,
        channel_id: &str,
        conv_id: &str,
    ) -> RunState {
        let mut state = RunState::new(ProtoRunId::new(run_id), AgentId::new(agent_id));
        let mut metadata = BTreeMap::new();
        metadata.insert(
            Arc::from("channel_id"),
            Value::String(channel_id.to_owned()),
        );
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
    fn child_envelope_uses_a_versioned_encoded_identity() {
        let parent = parent_state_with_user_message();
        let spec = spec_with_prompt("hi");
        let envelope = child_input_envelope(&spec, &parent);
        assert!(
            envelope.conversation_id.as_str().starts_with("child.v1."),
            "the stored id must say which child-source encoding it uses"
        );
        assert!(!envelope.conversation_id.as_str().contains("tg-12345"));
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
    fn identical_parent_conversation_ids_on_two_channels_are_isolated() {
        let telegram =
            parent_state_with_identity("parent-run-1", "parent-agent", "telegram", "shared-id");
        let feishu =
            parent_state_with_identity("parent-run-2", "parent-agent", "feishu", "shared-id");
        let spec = spec_with_prompt("same task");

        assert_ne!(
            child_input_envelope(&spec, &telegram).conversation_id,
            child_input_envelope(&spec, &feishu).conversation_id,
        );
    }

    #[test]
    fn identical_channel_conversations_under_two_parent_agents_are_isolated() {
        let first = parent_state_with_identity("run-1", "agent-a", "telegram", "shared-id");
        let second = parent_state_with_identity("run-2", "agent-b", "telegram", "shared-id");
        let spec = spec_with_prompt("same task");

        assert_ne!(
            child_input_envelope(&spec, &first).conversation_id,
            child_input_envelope(&spec, &second).conversation_id,
        );
    }

    #[test]
    fn two_policies_for_one_child_agent_are_isolated() {
        let parent = parent_state_with_user_message();
        let first = spec_with_prompt("same task");
        let mut second = first.clone();
        second.policy_id = Arc::from("restricted");

        assert_ne!(
            child_input_envelope(&first, &parent).conversation_id,
            child_input_envelope(&second, &parent).conversation_id,
        );
    }

    #[test]
    fn an_explicit_task_id_is_stable_and_separates_tasks() {
        let parent = parent_state_with_user_message();
        let mut first = spec_with_prompt("wording may change");
        first
            .metadata
            .insert(Arc::from("task_id"), Value::String("task-1".to_owned()));
        let mut same_task = spec_with_prompt("different wording");
        same_task
            .metadata
            .insert(Arc::from("task_id"), Value::String("task-1".to_owned()));
        let mut other_task = first.clone();
        other_task
            .metadata
            .insert(Arc::from("task_id"), Value::String("task-2".to_owned()));

        assert_eq!(
            child_input_envelope(&first, &parent).conversation_id,
            child_input_envelope(&same_task, &parent).conversation_id,
        );
        assert_ne!(
            child_input_envelope(&first, &parent).conversation_id,
            child_input_envelope(&other_task, &parent).conversation_id,
        );
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
        assert!(envelope.conversation_id.as_str().starts_with("child.v1."));
        assert_eq!(
            parent_principal(&parent)
                .as_principal()
                .conversation
                .as_str(),
            "parent-run",
            "embedded callers still fall back to the run id as the parent conversation"
        );
    }
}
