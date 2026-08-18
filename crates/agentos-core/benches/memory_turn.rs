//! Hydrated-memory plan turn benchmark.
//!
//! Measures a reply turn whose `Plan` state hydrates memory from a populated
//! `SqliteStore` (in-memory database, 200 records across the semantic and
//! episodic stores) before the orchestrator plans. The hydration request
//! mirrors the `MaxOrchestrator` defaults: 5 fragments max, ~1200 estimated
//! tokens max, lexical retrieval.
//!
//! Compared with `loop_overhead/reply_turn`, the delta is the cost of
//! `MemoryManager::hydrate` (scope authorization, SQLite reads, fragment
//! selection and token budgeting) inside the loop's hydrate span.
//!
//! Run with `cargo bench -p agentos-core --bench memory_turn`.

mod support;

use agentos_core::approve::Policy;
use agentos_core::memory::{
    HydrationRequest, MemoryCaller, MemoryManager, MemoryOwner, MemoryScope, MemoryStore,
    MemoryVisibility, RetrievalStrategy, SqliteStore,
};
use agentos_interfaces::orchestrator::{Orchestrator, OrchestratorError, Plan, RunContext};
use agentos_proto::{AgentId, ConversationId, Message, MessageRole};
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use support::{current_thread_runtime, drive_to_finish, fresh_state, make_deps};

const RECORDS_PER_STORE: usize = 100;
const MAX_FRAGMENTS: usize = 5;
const MAX_ESTIMATED_TOKENS: usize = 1_200;

fn bench_caller(ctx: &RunContext<'_>) -> MemoryCaller {
    MemoryCaller {
        agent_id: ctx.system.active_agent.clone(),
        task_id: ctx.system.task_id.clone(),
        conversation_id: ConversationId::new("bench-conv"),
        principal_key: ctx
            .state
            .session_key
            .as_ref()
            .map(|key| key.principal.clone()),
        user_id: None,
        allowed_shared_domains: Vec::new(),
        audit_read_access: false,
    }
}

fn hydration_request() -> HydrationRequest {
    HydrationRequest {
        query: Arc::from("deployment"),
        domain: None,
        max_fragments: MAX_FRAGMENTS,
        max_tokens: MAX_ESTIMATED_TOKENS,
        stores: vec![MemoryStore::Semantic, MemoryStore::Episodic],
        strategy: RetrievalStrategy::Lexical,
    }
}

/// Orchestrator that hydrates from the memory manager (the way
/// `MaxOrchestrator`'s hydrator does) and then replies.
struct HydratingOrchestrator {
    manager: Arc<MemoryManager>,
}

#[async_trait]
impl Orchestrator for HydratingOrchestrator {
    async fn hydrate(&self, ctx: &mut RunContext<'_>) -> Result<(), OrchestratorError> {
        let caller = bench_caller(ctx);
        let fragments = self
            .manager
            .hydrate(&caller, hydration_request())
            .await
            .map_err(|err| OrchestratorError::Backend(Arc::from(err.to_string())))?;
        ctx.memory_fragments.extend(fragments);
        Ok(())
    }

    async fn plan(&self, _ctx: &RunContext<'_>) -> Result<Plan, OrchestratorError> {
        Ok(Plan::Reply(Message::text(MessageRole::Assistant, "pong")))
    }
}

fn populated_manager(runtime: &tokio::runtime::Runtime) -> Arc<MemoryManager> {
    let store = Arc::new(SqliteStore::open_in_memory().expect("in-memory sqlite store opens"));
    let manager = Arc::new(MemoryManager::new_sqlite(store));
    let agent = AgentId::new("bench-agent");
    let writer = MemoryCaller {
        agent_id: agent.clone(),
        task_id: agentos_proto::TaskId::new("bench-run"),
        conversation_id: ConversationId::new("bench-conv"),
        principal_key: None,
        user_id: None,
        allowed_shared_domains: Vec::new(),
        audit_read_access: false,
    };

    runtime.block_on(async {
        for store_kind in [MemoryStore::Semantic, MemoryStore::Episodic] {
            for index in 0..RECORDS_PER_STORE {
                let scope = MemoryScope::new(
                    store_kind,
                    MemoryOwner::Agent(agent.clone()),
                    MemoryVisibility::Private,
                    None,
                );
                manager
                    .write_scoped(
                        &writer,
                        scope,
                        serde_json::json!({
                            "text": format!("deployment note {index}: benchmark fixture fact"),
                        }),
                        BTreeMap::new(),
                    )
                    .await
                    .expect("bench record writes");
            }
        }

        // Sanity-check the fixture: the bench is meaningless if hydration
        // selects nothing, so fail loudly here instead of measuring a no-op.
        let fragments = manager
            .hydrate(&writer, hydration_request())
            .await
            .expect("sanity hydrate succeeds");
        assert!(
            !fragments.is_empty(),
            "bench fixture produced no hydration fragments"
        );
    });
    manager
}

fn bench_hydrated_memory_plan_turn(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let manager = populated_manager(&runtime);
    let orchestrator = HydratingOrchestrator { manager };
    let policy = Policy::default();

    c.bench_function("loop_overhead/hydrated_memory_plan_turn", |b| {
        b.iter(|| {
            let deps = make_deps(&orchestrator, &policy, None);
            black_box(runtime.block_on(drive_to_finish(&deps, fresh_state())));
        });
    });
}

criterion_group!(benches, bench_hydrated_memory_plan_turn);
criterion_main!(benches);
