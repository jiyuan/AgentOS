//! M8 / `GW-001`, deliverables 1 and 2: one database, several writers, no
//! `SQLITE_BUSY` and no duplicate maintenance.
//!
//! Two `SqliteStore`s on one file stand in for the two processes the audit
//! named — the gateway and the TUI, which both default to
//! `workspace/agentos.sqlite`. They are a fair stand-in for the part that
//! matters: separate connection pools, no shared lock, and only the file
//! itself to coordinate through.

mod support;

use agentos_core::memory::{
    MemoryCaller, MemoryManager, MemoryOwner, MemoryScope, MemoryStore, MemoryVisibility,
    SqliteStore, DEFAULT_LEASE_TTL, REFLECTION_LEASE,
};
use agentos_interfaces::memory::Query;
use agentos_proto::{AgentId, ChannelId, ConversationId, TaskId};
use serde_json::json;
use std::sync::Arc;

fn caller() -> MemoryCaller {
    MemoryCaller {
        agent_id: AgentId::new("agent"),
        task_id: TaskId::new("task"),
        channel_id: ChannelId::new("channel"),
        conversation_id: ConversationId::new("conversation"),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        writable_shared_domains: Vec::new(),
        audit_read_access: false,
    }
}

fn scope() -> MemoryScope {
    MemoryScope::new(
        MemoryStore::Semantic,
        MemoryOwner::Agent(AgentId::new("agent")),
        MemoryVisibility::Private,
        Some(Arc::from("general")),
    )
}

/// The reported symptom: the gateway and the TUI on one file, in rollback
/// journal mode with no busy timeout, where the first overlap is an immediate
/// error rather than a wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_writing_one_database_do_not_collide() {
    let tree = support::temp_tree("wal-writers");
    let path = tree.path().join("agentos.sqlite");

    let gateway = Arc::new(MemoryManager::new_sqlite(Arc::new(
        SqliteStore::open(&path).expect("the gateway store opens"),
    )));
    let tui = Arc::new(MemoryManager::new_sqlite(Arc::new(
        SqliteStore::open(&path).expect("the tui store opens"),
    )));

    let mut writers = Vec::new();
    for (label, manager) in [("gateway", &gateway), ("tui", &tui)] {
        for index in 0..25 {
            let manager = Arc::clone(manager);
            writers.push(tokio::spawn(async move {
                manager
                    .write_scoped(
                        &caller(),
                        scope(),
                        json!({ "fact": format!("{label} fact {index}") }),
                        Default::default(),
                    )
                    .await
                    .unwrap_or_else(|err| panic!("{label} write {index} failed: {err}"));
            }));
        }
    }
    for writer in writers {
        writer.await.expect("no writer panicked");
    }

    // Both wrote into the one database, and either store reads all of it.
    let visible = gateway
        .read_scoped(&caller(), scope(), &Query::filter(usize::MAX))
        .await
        .expect("the read succeeds")
        .len();
    assert_eq!(visible, 50, "one database, fifty records, both writers");
}

/// A file-backed store hands out more than one connection at a time. Under the
/// single `Mutex<Connection>` this could not be true, which is why an
/// unrelated conversation's read waited behind another one's write.
#[test]
fn a_file_backed_store_has_more_than_one_connection() {
    let tree = support::temp_tree("pool-size");
    let store = SqliteStore::open(tree.path().join("agentos.sqlite")).expect("the store opens");
    assert!(
        store.max_connections() > 1,
        "got {}",
        store.max_connections()
    );
    // An in-memory database is per connection, so this one is deliberately 1.
    let ephemeral = SqliteStore::open_in_memory().expect("the store opens");
    assert_eq!(ephemeral.max_connections(), 1);
}

/// The store knows which file it is, which is how the runtime avoids opening
/// a second pool onto the database the session store already has.
#[test]
fn a_store_reports_the_file_it_opened() {
    let tree = support::temp_tree("db-path");
    let path = tree.path().join("agentos.sqlite");
    let store = SqliteStore::open(&path).expect("the store opens");
    assert_eq!(
        store.database_path(),
        Some(path.canonicalize().expect("the file exists"))
    );
    assert_eq!(
        SqliteStore::open_in_memory()
            .expect("the store opens")
            .database_path(),
        None
    );
}

/// Deliverable 2's other half. Both channels' shard 0 ask on every idle tick;
/// exactly one sweeps.
#[test]
fn only_one_contender_holds_the_reflection_lease() {
    let tree = support::temp_tree("lease");
    let path = tree.path().join("agentos.sqlite");
    let telegram = SqliteStore::open(&path).expect("the first store opens");
    let feishu = SqliteStore::open(&path).expect("the second store opens");

    let held: Vec<&str> = [("telegram", &telegram), ("feishu", &feishu)]
        .into_iter()
        .filter(|(role, store)| {
            store
                .try_acquire_lease(REFLECTION_LEASE, role, DEFAULT_LEASE_TTL)
                .expect("the query runs")
                .is_some()
        })
        .map(|(role, _)| role)
        .collect();
    assert_eq!(
        held.len(),
        1,
        "exactly one leader across two independent stores, got {held:?}"
    );

    // And the loser still sees who won, through the file rather than through
    // any shared memory.
    let leader = feishu
        .lease(REFLECTION_LEASE)
        .expect("the query runs")
        .expect("somebody holds it");
    assert_eq!(leader.holder.as_ref(), held[0]);
}
