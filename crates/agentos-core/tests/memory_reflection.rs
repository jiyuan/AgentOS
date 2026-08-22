//! Track B2: whole-memory reflection sweep.
//!
//! Verifies that `MemoryManager::reflect_all` discovers every conversation
//! holding episodes, promotes repeated episodes (by summary) into semantic
//! facts in each conversation's scope, and that the promoted fact is then
//! retrievable — the on-disk side of "reflection runs on a schedule and its
//! promotions show up in subsequent hydration".

use agentos_core::memory::{
    EpisodeOutcome, EpisodeRecord, MemoryCaller, MemoryManager, MemoryOwner, MemoryScope,
    MemoryStore, MemoryVisibility, ReflectionParams, SqliteStore,
};
use agentos_interfaces::memory::Query;
use agentos_proto::{AgentId, ChannelId, ConversationId, Principal, RunId, TaskId};
use std::sync::Arc;

fn episode(conversation: &str, run: &str, summary: &str) -> EpisodeRecord {
    EpisodeRecord {
        run_id: RunId::new(run),
        task_id: TaskId::new("t"),
        active_agent: AgentId::new("agent"),
        channel_id: ChannelId::new("telegram"),
        conversation_id: ConversationId::new(conversation),
        user_id: None,
        // A succeeded multi-step tool run is recordable (not trivial) and a
        // promotable successful trajectory.
        outcome: EpisodeOutcome::Succeeded,
        tools_used: vec![Arc::from("shell")],
        subagents_used: Vec::new(),
        summary: Arc::from(summary),
        turn_count: 2,
        metadata: Default::default(),
    }
}

fn caller(conversation: &str) -> MemoryCaller {
    MemoryCaller {
        agent_id: AgentId::new("agent"),
        task_id: TaskId::new("t"),
        channel_id: ChannelId::new("telegram"),
        conversation_id: ConversationId::new(conversation),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        writable_shared_domains: Vec::new(),
        audit_read_access: false,
    }
}

#[tokio::test]
async fn reflect_all_promotes_repeated_episodes_across_conversations() {
    let store = Arc::new(SqliteStore::open_in_memory().expect("sqlite opens"));
    let manager = MemoryManager::new_sqlite(store);

    // Two conversations, each with the same episode summary repeated twice.
    for conversation in ["telegram:1", "telegram:2"] {
        for run in ["r1", "r2"] {
            let recorded = manager
                .record_episode(episode(conversation, run, "deploy the service"))
                .await
                .expect("record episode");
            assert!(
                recorded.is_some(),
                "episode should be recorded, not skipped"
            );
        }
    }

    let report = manager
        .reflect_all(&AgentId::new("agent"), &ReflectionParams::default())
        .await
        .expect("reflect_all sweeps");

    // One promoted semantic fact per conversation.
    assert_eq!(
        report.promoted_records.len(),
        2,
        "expected one promotion per conversation, got {:?}",
        report.promoted_records
    );
    assert!(report
        .promoted_records
        .iter()
        .all(|promotion| promotion.summary.as_ref() == "deploy the service"));

    // The promoted fact is retrievable from the conversation's semantic scope.
    let scope = MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Conversation(Principal::conversation(
            AgentId::new("agent"),
            ChannelId::new("telegram"),
            ConversationId::new("telegram:1"),
        )),
        MemoryVisibility::Private,
        None,
    );
    let records = manager
        .read_scoped(&caller("telegram:1"), scope, &Query::filter(10))
        .await
        .expect("read semantic scope");
    assert!(
        records.iter().any(
            |record| record.body.get("source_summary").and_then(|v| v.as_str())
                == Some("deploy the service")
        ),
        "promoted semantic fact should be retrievable, got {records:?}"
    );
}

#[tokio::test]
async fn reflect_all_is_noop_without_episodes() {
    let store = Arc::new(SqliteStore::open_in_memory().expect("sqlite opens"));
    let manager = MemoryManager::new_sqlite(store);
    let report = manager
        .reflect_all(&AgentId::new("agent"), &ReflectionParams::default())
        .await
        .expect("reflect_all on empty store");
    assert!(report.promoted_records.is_empty());
    assert_eq!(report.episode_candidates, 0);
}
