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
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, Principal, RunId,
};
use serde_json::Value;
use std::sync::Arc;

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
    let child = Principal::conversation(
        child_agent.clone(),
        input.channel_id.clone(),
        input.conversation_id.clone(),
    );
    match session.fork(&seed.principal, seed.boundary, &child).await {
        Ok(items) => tracing::info!(
            parent_conversation = %seed.principal.conversation_name(),
            child_conversation = %child.conversation_name(),
            items,
            "sub-agent conversation seeded from parent"
        ),
        Err(error) => tracing::debug!(
            parent_conversation = %seed.principal.conversation_name(),
            child_conversation = %child.conversation_name(),
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
pub fn parent_principal(parent_state: &agentos_interfaces::RunState) -> Principal {
    let field = |name: &str| {
        parent_state
            .transcript
            .items
            .last()
            .and_then(|item| item.metadata.get(name))
            .and_then(Value::as_str)
            .map(Arc::<str>::from)
    };
    Principal::conversation(
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

    // Stable conversation id: derived from the parent's *user-conversation*
    // id (read off the most recent transcript item's metadata, where the
    // runner stamps it on every inbound). Sticking to parent_run_id here
    // would mint a new sub-agent conversation every turn and the persistent
    // session would never accumulate history.
    let parent_conversation_id = parent_conversation_id(parent_state);

    let conversation_suffix = prompt
        .filter(|prompt| !prompt.trim().is_empty())
        .map(|prompt| format!(":{}", prompt_fingerprint(prompt)))
        .unwrap_or_default();

    Envelope {
        channel_id: ChannelId::new(format!("subagent:{}", spec.agent_id.as_str())),
        conversation_id: ConversationId::new(format!(
            "{}:{}{}",
            parent_conversation_id.as_str(),
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
