//! `ID-003`: delegated conversations are keyed by their complete source.
//!
//! Unit tests in `subagents::branch` pin each input to the derivation. This
//! integration test pins the property the stored identity exists to enforce:
//! transcript and memory written by one child source are invisible to child
//! sources that differ only by parent channel, parent agent, or child policy.

use agentos_core::memory::{
    InMemoryMemory, InMemorySession, MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility,
};
use agentos_core::subagents::child_input_envelope;
use agentos_interfaces::memory::{Memory, Query, Record};
use agentos_interfaces::orchestrator::SubAgentSpec;
use agentos_interfaces::session::{Item, Session};
use agentos_interfaces::RunState;
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, Namespace, Principal, RunId,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

fn parent(agent: &str, channel: &str, conversation: &str) -> RunState {
    let mut state = RunState::new(RunId::new("parent-run"), AgentId::new(agent));
    state.transcript.items.push(Item {
        message: Message::text(MessageRole::User, "delegate this task"),
        metadata: BTreeMap::from([
            (Arc::from("channel_id"), Value::String(channel.to_owned())),
            (
                Arc::from("conversation_id"),
                Value::String(conversation.to_owned()),
            ),
        ]),
    });
    state
}

fn spec(policy: &str) -> SubAgentSpec {
    SubAgentSpec {
        agent_id: AgentId::new("worker"),
        policy_id: Arc::from(policy),
        metadata: BTreeMap::from([(Arc::from("prompt"), Value::String("same task".to_owned()))]),
    }
}

fn child_principal(envelope: &Envelope) -> Principal {
    Principal::conversation(
        AgentId::new("worker"),
        envelope.channel_id.clone(),
        envelope.conversation_id.clone(),
    )
}

fn conversation_memory(principal: Principal) -> MemoryScope {
    MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Conversation(principal),
        MemoryVisibility::Private,
        Some(Arc::from("identity-test")),
    )
}

#[tokio::test]
async fn colliding_legacy_sources_do_not_share_transcript_or_memory() {
    let sources = [
        child_input_envelope(&spec("default"), &parent("agent-a", "telegram", "42")),
        child_input_envelope(&spec("default"), &parent("agent-a", "feishu", "42")),
        child_input_envelope(&spec("default"), &parent("agent-b", "telegram", "42")),
        child_input_envelope(&spec("restricted"), &parent("agent-a", "telegram", "42")),
    ];
    let principals: Vec<Principal> = sources.iter().map(child_principal).collect();
    for (index, left) in principals.iter().enumerate() {
        for right in &principals[index + 1..] {
            assert_ne!(
                left, right,
                "distinct child sources need distinct principals"
            );
        }
    }

    let sessions = InMemorySession::default();
    sessions
        .append(
            &principals[0],
            vec![Item {
                message: Message::text(MessageRole::Assistant, "private child history"),
                metadata: BTreeMap::new(),
            }],
        )
        .await
        .expect("the first child transcript is written");
    assert_eq!(
        sessions
            .load(&principals[0])
            .await
            .expect("the owner loads")
            .items
            .len(),
        1
    );
    for other in &principals[1..] {
        assert!(
            sessions
                .load(other)
                .await
                .expect("the isolated child loads")
                .items
                .is_empty(),
            "one child source read another source's transcript"
        );
    }

    let memory = InMemoryMemory::default();
    let owner_scope = conversation_memory(principals[0].clone());
    memory
        .write(
            &owner_scope.namespace(),
            Record {
                id: None,
                namespace: Namespace::new("replaced-by-store"),
                body: json!({ "fact": "private child memory" }),
                metadata: BTreeMap::new(),
            },
        )
        .await
        .expect("the first child's memory is written");
    assert_eq!(
        memory
            .read(&owner_scope.namespace(), &Query::filter(10))
            .await
            .expect("the owner reads")
            .len(),
        1
    );
    for other in &principals[1..] {
        let records = memory
            .read(
                &conversation_memory(other.clone()).namespace(),
                &Query::filter(10),
            )
            .await
            .expect("the isolated child's memory reads");
        assert!(
            records.is_empty(),
            "one child source read another source's memory"
        );
    }
}

#[test]
fn repeating_the_same_delegation_keeps_the_same_persistent_identity() {
    let first = child_input_envelope(&spec("default"), &parent("agent-a", "telegram", "42"));
    let second = child_input_envelope(&spec("default"), &parent("agent-a", "telegram", "42"));
    assert_eq!(child_principal(&first), child_principal(&second));
    assert_eq!(
        first.channel_id,
        ChannelId::new("subagent:worker"),
        "the child channel remains the registered child identity"
    );
    assert_ne!(
        first.conversation_id,
        ConversationId::new("42:worker"),
        "the legacy incomplete key must not return"
    );
}
