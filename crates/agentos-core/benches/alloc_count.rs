//! Allocation-count instrumentation for the loop turn shapes.
//!
//! Custom harness (not criterion): instead of wall time, this counts heap
//! allocations and allocated bytes per turn via a counting global allocator,
//! so allocation-reduction work (telemetry field maps, prelude caching,
//! metadata clones) can show a before/after number that is immune to timer
//! noise.
//!
//! Counts include everything inside `block_on(drive(..))` — loop states,
//! trace spans, telemetry fields, and tokio's per-task bookkeeping — which
//! is exactly the per-turn cost a concurrent gateway pays. Numbers are
//! averaged over many iterations and are deterministic modulo tokio
//! internals.
//!
//! Run with `cargo bench -p agentos-core --bench alloc_count`.
//! `cargo bench -- --test` runs a single small smoke pass.

mod support;

use agentos_core::approve::Policy;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use support::{
    current_thread_runtime, drive_to_finish, echo_tool_registry, fresh_state, make_deps,
    AlwaysReplyOrchestrator, ScriptedToolOrchestrator, BENCH_TOOL_NAME,
};

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates every operation to `System`; only adds relaxed counter
// increments, which are async-signal-safe and allocation-free.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn snapshot() -> (u64, u64) {
    (
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
    )
}

fn measure(label: &str, iterations: u64, mut turn: impl FnMut()) {
    // Warmup amortizes one-time lazy initialization (tracing callsite
    // registration, runtime internals) out of the measured window.
    for _ in 0..iterations.min(50) {
        turn();
    }
    let (allocs_before, bytes_before) = snapshot();
    for _ in 0..iterations {
        turn();
    }
    let (allocs_after, bytes_after) = snapshot();
    println!(
        "{label}: {} allocations/turn, {} bytes/turn (over {iterations} iterations)",
        (allocs_after - allocs_before) / iterations,
        (bytes_after - bytes_before) / iterations,
    );
}

fn main() {
    let smoke = std::env::args().any(|arg| arg == "--test");
    let iterations: u64 = if smoke { 50 } else { 2000 };

    let runtime = current_thread_runtime();

    let reply_orchestrator = AlwaysReplyOrchestrator;
    let deny_policy = Policy::default();
    measure("alloc_count/reply_turn", iterations, || {
        let deps = make_deps(&reply_orchestrator, &deny_policy, None);
        runtime.block_on(drive_to_finish(&deps, fresh_state()));
    });

    let tool_orchestrator = ScriptedToolOrchestrator;
    let allow_policy = Policy::allow_tools([BENCH_TOOL_NAME]);
    let tools = echo_tool_registry();
    measure("alloc_count/tool_turn_allow", iterations, || {
        let deps = make_deps(&tool_orchestrator, &allow_policy, Some(&tools));
        runtime.block_on(drive_to_finish(&deps, fresh_state()));
    });
}
