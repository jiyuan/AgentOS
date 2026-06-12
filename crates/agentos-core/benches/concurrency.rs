//! Tail-latency benchmark: 1000 concurrent conversations.
//!
//! Custom harness (not criterion): criterion reports mean wall time, but the
//! question here is tail latency under load, so this harness runs rounds of
//! N concurrent conversations and reports p50/p95/p99/max per-conversation
//! latency plus round throughput.
//!
//! **Architectural constraint discovered while writing this bench:** the
//! `RunLoopState::step()` future is `!Send` because sub-agent execution
//! drives a `tokio::task::LocalSet` (`subagents/mod.rs`), so runs cannot be
//! `tokio::spawn`ed onto a multi-thread work-stealing runtime at all.
//! Concurrency is therefore modeled the only way the current architecture
//! permits: conversations are sharded across one OS thread per core, each
//! running a `current_thread` runtime with a `LocalSet`. The numbers below
//! reflect that thread-per-core ceiling, which is itself a finding for the
//! optimization roadmap.
//!
//! Each conversation is one full tool-call turn (`Start → Plan → Approve →
//! Act → Observe → Plan → Finish`) against stateless shared fixtures, so the
//! numbers reflect loop behavior, not fixture lock contention.
//!
//! Run with `cargo bench -p agentos-core --bench concurrency`.
//! `cargo bench -- --test` runs a single small smoke round.

mod support;

use agentos_core::approve::Policy;
use agentos_core::tools::ToolRegistry;
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::{
    drive_to_finish, echo_tool_registry, fresh_state, make_deps, ScriptedToolOrchestrator,
    BENCH_TOOL_NAME,
};
use tokio::task::LocalSet;

const CONVERSATIONS: usize = 1000;
const ROUNDS: usize = 5;
const SMOKE_CONVERSATIONS: usize = 50;

struct Fixtures {
    orchestrator: ScriptedToolOrchestrator,
    policy: Policy,
    tools: ToolRegistry,
}

/// Run `share` conversations on this thread's own current-thread runtime,
/// interleaved on a `LocalSet` (the only scheduling shape the `!Send` loop
/// future supports). Returns per-conversation latency measured from first
/// poll to completion.
fn run_shard(fixtures: Arc<Fixtures>, share: usize) -> Vec<Duration> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current_thread tokio runtime builds");
    let local = LocalSet::new();
    let mut handles = Vec::with_capacity(share);
    for _ in 0..share {
        let fixtures = Arc::clone(&fixtures);
        handles.push(local.spawn_local(async move {
            let started = Instant::now();
            let deps = make_deps(
                &fixtures.orchestrator,
                &fixtures.policy,
                Some(&fixtures.tools),
            );
            drive_to_finish(&deps, fresh_state()).await;
            started.elapsed()
        }));
    }
    runtime.block_on(local.run_until(async move {
        let mut latencies = Vec::with_capacity(handles.len());
        for handle in handles {
            latencies.push(handle.await.expect("conversation task completes"));
        }
        latencies
    }))
}

fn run_round(fixtures: &Arc<Fixtures>, conversations: usize, workers: usize) -> Vec<Duration> {
    let mut threads = Vec::with_capacity(workers);
    for worker in 0..workers {
        // Distribute the remainder across the first shards.
        let share = conversations / workers + usize::from(worker < conversations % workers);
        if share == 0 {
            continue;
        }
        let fixtures = Arc::clone(fixtures);
        threads.push(std::thread::spawn(move || run_shard(fixtures, share)));
    }
    let mut latencies = Vec::with_capacity(conversations);
    for thread in threads {
        latencies.extend(thread.join().expect("shard thread completes"));
    }
    latencies
}

fn percentile(sorted: &[Duration], q: f64) -> Duration {
    assert!(!sorted.is_empty(), "latency sample set is non-empty");
    let index = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[index]
}

fn report(label: &str, latencies: &mut [Duration], wall: Duration) {
    latencies.sort_unstable();
    let count = latencies.len();
    let throughput = count as f64 / wall.as_secs_f64();
    println!(
        "{label}: n={count} wall={wall:?} throughput={throughput:.0}/s \
         p50={:?} p95={:?} p99={:?} max={:?}",
        percentile(latencies, 0.50),
        percentile(latencies, 0.95),
        percentile(latencies, 0.99),
        percentile(latencies, 1.0),
    );
}

fn main() {
    // `cargo bench -- --test` smoke mode: prove the harness runs, skip the
    // full measurement.
    let smoke = std::env::args().any(|arg| arg == "--test");
    let (conversations, rounds) = if smoke {
        (SMOKE_CONVERSATIONS, 1)
    } else {
        (CONVERSATIONS, ROUNDS)
    };
    let workers = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);

    let fixtures = Arc::new(Fixtures {
        orchestrator: ScriptedToolOrchestrator,
        policy: Policy::allow_tools([BENCH_TOOL_NAME]),
        tools: echo_tool_registry(),
    });

    println!(
        "concurrency bench: {conversations} concurrent tool-call conversations per round, \
         {rounds} measured round(s), {workers} shard thread(s) \
         (loop future is !Send; see module docs)"
    );

    // Warmup round (not reported): pre-faults allocator arenas so round 1 is
    // comparable to later rounds.
    run_round(&fixtures, conversations, workers);

    let mut all = Vec::with_capacity(conversations * rounds);
    for round in 1..=rounds {
        let started = Instant::now();
        let mut latencies = run_round(&fixtures, conversations, workers);
        let wall = started.elapsed();
        report(&format!("round {round}"), &mut latencies, wall);
        all.extend_from_slice(&latencies);
    }
    if rounds > 1 {
        all.sort_unstable();
        println!(
            "aggregate: n={} p50={:?} p95={:?} p99={:?} max={:?}",
            all.len(),
            percentile(&all, 0.50),
            percentile(&all, 0.95),
            percentile(&all, 0.99),
            percentile(&all, 1.0),
        );
    }
}
