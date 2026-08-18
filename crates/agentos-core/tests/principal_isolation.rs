//! ID-001: persisted conversation state belongs to a complete principal.

mod support;

use agentos_core::approve::Policy;
use agentos_core::memory::{
    InMemoryMemory, InMemorySession, MemoryCaller, MemoryManager, MemoryOwner, MemoryScope,
    MemoryStore, MemoryVisibility,
};
use agentos_core::runner::{run_envelope, RunOutcome};
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_interfaces::session::Session;
use agentos_proto::{
    AgentId, ChannelId, ConversationId, Envelope, Message, MessageRole, RunId, TaskId,
};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

struct CountVisibleItems;

#[async_trait]
impl Orchestrator for CountVisibleItems {
    async fn plan(&self, ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        Ok(Plan::Reply(Message::text(
            MessageRole::Assistant,
            format!("saw {} items", ctx.state.transcript.items.len()),
        )))
    }
}

fn envelope(channel: &str, sender: &str) -> Envelope {
    Envelope {
        channel_id: ChannelId::new(channel),
        conversation_id: ConversationId::new("42"),
        sender: Arc::from(sender),
        message: Message::text(MessageRole::User, "hello"),
        metadata: BTreeMap::new(),
    }
}

#[tokio::test]
async fn agent_channel_conversation_and_sender_jointly_isolate_sessions() {
    let session = InMemorySession::default();
    let orchestrator = CountVisibleItems;
    let policy = Policy::default();
    let identities = [
        ("agent-a", "telegram", "alice"),
        ("agent-a", "feishu", "alice"),
        ("agent-b", "telegram", "alice"),
        ("agent-a", "telegram", "bob"),
    ];
    let mut keys = BTreeSet::new();

    for (agent, channel, sender) in identities {
        let input = envelope(channel, sender);
        let active_agent = AgentId::new(agent);
        let key = input.session_key(&active_agent);
        keys.insert(key.storage_key());
        let mut deps = support::runner_deps(&orchestrator, &session, &policy, None, None);
        deps.active_agent = active_agent;
        let outcome = run_envelope(input, RunId::new("same-run-id"), &deps)
            .await
            .expect("isolated run finishes");
        let RunOutcome::Finished { output, .. } = outcome else {
            panic!("no approval was expected");
        };
        assert_eq!(output.message.content.as_ref(), "saw 1 items");
        assert_eq!(
            session.load(&key).await.expect("session loads").items.len(),
            2
        );
    }

    assert_eq!(keys.len(), identities.len());

    let input = envelope("telegram", "alice");
    let active_agent = AgentId::new("agent-a");
    let key = input.session_key(&active_agent);
    let mut deps = support::runner_deps(&orchestrator, &session, &policy, None, None);
    deps.active_agent = active_agent;
    let outcome = run_envelope(input, RunId::new("same-run-id"), &deps)
        .await
        .expect("second turn finishes");
    let RunOutcome::Finished { output, .. } = outcome else {
        panic!("no approval was expected");
    };
    assert_eq!(output.message.content.as_ref(), "saw 3 items");
    assert_eq!(
        session.load(&key).await.expect("session loads").items.len(),
        4
    );
}

#[tokio::test]
async fn principal_owned_memory_rejects_another_channel_with_the_same_conversation_id() {
    let agent = AgentId::new("agent-a");
    let telegram = envelope("telegram", "alice").principal_key(&agent);
    let feishu = envelope("feishu", "alice").principal_key(&agent);
    let owner = MemoryCaller {
        agent_id: agent.clone(),
        task_id: TaskId::new("task"),
        conversation_id: ConversationId::new("42"),
        principal_key: Some(telegram.clone()),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        audit_read_access: false,
    };
    let intruder = MemoryCaller {
        principal_key: Some(feishu),
        ..owner.clone()
    };
    let scope = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Principal(telegram),
        MemoryVisibility::Private,
        None,
    );
    let manager = MemoryManager::new(Arc::new(InMemoryMemory::default()));
    manager
        .write_scoped(
            &owner,
            scope.clone(),
            serde_json::json!({"fact": "private"}),
            BTreeMap::new(),
        )
        .await
        .expect("owner writes memory");
    let error = manager
        .write_scoped(
            &intruder,
            scope,
            serde_json::json!({"fact": "crossed"}),
            BTreeMap::new(),
        )
        .await
        .expect_err("another channel must not mutate this principal's memory");
    assert!(error.to_string().contains("different caller"));
}
