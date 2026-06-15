//! Loop overhead benchmark.
//!
//! Measures wall time for one full happy-path turn through `RunLoopState`:
//! `Start` → `Plan` (mock orchestrator returns `Plan::Reply`) → `Finish`.
//!
//! All "external" costs are mocked: no LLM call, no tool call, no IO. Reflects
//! the run loop's intrinsic per-turn overhead, which `BENCHMARKS.md` targets at
//! ≤ 2 ms.
//!
//! Run with `cargo bench -p agentos-core --bench loop_overhead`.

use agentos_core::approve::Policy;
use agentos_core::r#loop::{LoopDeps, RunLoopState, StartCtx};
use agentos_interfaces::session::{Item, Transcript};
use agentos_interfaces::test_support::MockOrchestrator;
use agentos_interfaces::RunState;
use agentos_proto::{AgentId, Message, MessageRole, RunId};
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::BTreeMap;
use std::hint::black_box;
use tokio::runtime::{Builder, Runtime};

fn build_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current_thread tokio runtime builds")
}

fn fresh_state() -> RunState {
    let mut state = RunState::new(RunId::new("bench-run"), AgentId::new("bench-agent"));
    state.transcript = Transcript {
        items: vec![Item {
            message: Message::text(MessageRole::User, "ping"),
            metadata: BTreeMap::new(),
        }],
    };
    state
}

async fn drive_to_finish(deps: &LoopDeps<'_>) -> RunLoopState {
    let mut current = RunLoopState::Start(StartCtx {
        state: fresh_state(),
    });
    loop {
        current = current.step(deps).await.expect("happy-path step ok");
        if matches!(current, RunLoopState::Finish(_)) {
            return current;
        }
    }
}

fn bench_reply_turn(c: &mut Criterion) {
    // Build the tokio runtime once and reuse across iterations so the
    // measurement reflects loop overhead rather than runtime construction.
    let runtime = build_runtime();
    let orchestrator = MockOrchestrator::replying("pong");
    let policy = Policy::default();

    c.bench_function("loop_overhead/reply_turn", |b| {
        b.iter(|| {
            let deps = LoopDeps {
                orchestrator: &orchestrator,
                max_turns: 4,
                hooks: None,
                tools: None,
                task_workspace: None,
                policy: &policy,
                subagents: None,
                input_guardrails: &[],
                output_guardrails: &[],
                tool_guardrails: &[],
                stream_sink: None,
            };
            black_box(runtime.block_on(drive_to_finish(&deps)));
        });
    });
}

criterion_group!(benches, bench_reply_turn);
criterion_main!(benches);
