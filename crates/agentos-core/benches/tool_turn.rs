//! Tool-call turn benchmark.
//!
//! Measures wall time for one full tool-call turn through `RunLoopState`:
//! `Start` → `Plan` (`Plan::CallTool`) → `Approve` (`PolicyDecision::Allow`)
//! → `Act` (mock tool executes) → `Observe` → `Plan` (`Plan::Reply`) →
//! `Finish`.
//!
//! All "external" costs are mocked: no LLM call, no real tool work, no IO.
//! Compared with `loop_overhead/reply_turn`, the delta is the cost of the
//! approve decision, tool dispatch through `ToolRegistry`, transcript item
//! construction, and the extra Plan/Approve/Act/Observe state transitions.
//!
//! Run with `cargo bench -p agentos-core --bench tool_turn`.

mod support;

use agentos_core::approve::Policy;
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use support::{
    current_thread_runtime, drive_to_finish, echo_tool_registry, fresh_state, make_deps,
    ScriptedToolOrchestrator, BENCH_TOOL_NAME,
};

fn bench_tool_turn_allow(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let orchestrator = ScriptedToolOrchestrator;
    let policy = Policy::allow_tools([BENCH_TOOL_NAME]);
    let tools = echo_tool_registry();

    c.bench_function("loop_overhead/tool_turn_allow", |b| {
        b.iter(|| {
            let deps = make_deps(&orchestrator, &policy, Some(&tools));
            black_box(runtime.block_on(drive_to_finish(&deps, fresh_state())));
        });
    });
}

criterion_group!(benches, bench_tool_turn_allow);
criterion_main!(benches);
