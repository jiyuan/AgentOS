# Agent OS Benchmarks

Target loop overhead: ≤ 2 ms per turn excluding LLM and tool latency. This is the performance budget for the typed run loop described in [`DESIGN.md`](DESIGN.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Current results

Captured 2026-06-11 (post roadmap Phase 3) with the criterion harness
(100 samples, 3 s warmup) on `current_thread` tokio runtimes, release
profile, unless noted. Single-turn wall times move ±5–8% between runs on
this machine; treat deltas inside that band as noise.

| Bench | Mean | Verdict |
|---|---|---|
| `loop_overhead/reply_turn` | **1.19 µs** | ~1690× under the 2 ms ceiling; unchanged through Phase 3 (within noise). |
| `loop_overhead/tool_turn_allow` | **3.00 µs** | Approve + tool dispatch + extra states add ~1.8 µs over a reply turn. |
| `loop_overhead/ask_user_pause_resume` | **7.71 µs** | Full interruption cycle: pause, JSON persist round-trip, approve, resume, finish. |
| `loop_overhead/paused_state_json_round_trip` | **3.55 µs** | `RunState` serialize + deserialize alone — roughly half the pause/resume cycle. |
| `loop_overhead/hydrated_memory_plan_turn` | **273.6 µs** | SQLite-backed hydration dominates the turn (~230× a reply turn) but stays ~7× under the ceiling. |

Allocation counts (counting global allocator, 2000 iterations,
deterministic). Phase 3 (interned telemetry field keys, gated log
serialization, cached skill prelude) cut both scenarios by ~20%:

| Scenario | Allocations/turn | Bytes/turn | Phase 1 baseline |
|---|---|---|---|
| `alloc_count/reply_turn` | **37** | 3 560 | 46 / 3 936 |
| `alloc_count/tool_turn_allow` | **104** | 11 370 | 130 / 12 362 |

Concurrency / tail latency (1000 concurrent tool-call conversations per round, 5 rounds, 16 shard threads):

| Metric | Value | Phase 1 baseline |
|---|---|---|
| p50 per-conversation latency | **3.0 µs** | 3.1 µs |
| p95 | **3.8 µs** | 3.9 µs |
| p99 | **10.4 µs** | 6.1 µs |
| throughput | ~1.5 M conversations/s | ~1.3–1.5 M/s |

### Performance finding: shared interned `Arc<str>` keys contend under concurrency

The first Phase 3 implementation interned telemetry field keys in a
process-global table. Single-thread benches improved, but the
1000-conversation bench regressed ~3.5× on p50 (3.1 µs → 10.8 µs): every
clone/drop of a shared key `Arc` became a contended refcount RMW bouncing
between all 16 shard threads. The fix (current implementation,
`loop/telemetry.rs`) interns **per thread** via `thread_local!`, restoring
p50 to 3.0 µs while keeping the allocation win. Rule of thumb for this
codebase: never share refcounted hot-path constants across shard threads —
intern per thread or per runtime.

### Architectural finding: the loop future is `!Send`

Writing the concurrency bench surfaced a structural constraint:
`RunLoopState::step()` returns a `!Send` future because sub-agent execution
drives a `tokio::task::LocalSet` (`crates/agentos-core/src/subagents/mod.rs`).
Runs therefore **cannot be `tokio::spawn`ed onto a multi-thread
work-stealing runtime at all**. The concurrency bench models the only shape
the current architecture permits: conversations sharded across one OS thread
per core, each with a `current_thread` runtime plus `LocalSet`. Any future
work-stealing ambition first requires making the loop future `Send` (or
deliberately documenting thread-per-core as the scaling model).

## Bench inventory and how to reproduce

```sh
cargo bench -p agentos-core --bench loop_overhead   # reply turn (Start → Plan → Finish)
cargo bench -p agentos-core --bench tool_turn       # tool-call turn with Approve::Allow
cargo bench -p agentos-core --bench pause_resume    # ask_user pause + RunState JSON round-trip + resume
cargo bench -p agentos-core --bench memory_turn     # plan turn hydrating from a populated SqliteStore
cargo bench -p agentos-core --bench concurrency     # 1000 concurrent conversations, p50/p95/p99 (custom harness)
cargo bench -p agentos-core --bench alloc_count     # allocations + bytes per turn (counting allocator)
```

All "external" costs are mocked — no LLM, no real tool work, no network — so
the numbers isolate the run loop's intrinsic overhead (trace span allocation,
transcript handling, guardrail iteration, policy decisions, state-enum
transitions). The memory bench is the exception by design: it includes real
in-memory SQLite reads to measure hydration cost. Shared fixtures live in
`crates/agentos-core/benches/support/mod.rs` and are deliberately stateless so
the concurrency numbers reflect loop behavior, not fixture lock contention.

`cargo bench -p agentos-core -- --test` smoke-runs every bench (including the
two custom harnesses) without full measurement — CI uses this to keep benches
compiling and runnable without gating on noisy timings.

## Capture environment

- `Linux 6.12.72-linuxkit aarch64`
- `rustc 1.96.0`
- criterion `0.5`
- 16 hardware threads (`std::thread::available_parallelism`)

Note: the previously recorded `reply_turn` baseline (1.08 µs, rustc 1.95.0)
re-measured at 1.18 µs under rustc 1.96.0 on the same machine — same order of
magnitude; treat cross-toolchain deltas under ~10% as noise.

## Planned bench coverage

The four benches previously listed here (tool-call turn, `AskUser`
pause/resume, hydrated-memory turn, 1000-conversation tail latency) are now
written — see the inventory above. They were scheduled as Phase 1.1 of
[`docs/OPTIMIZATION_ROADMAP.md`](docs/OPTIMIZATION_ROADMAP.md). Remaining
candidates, in case future work needs them:

- Sub-agent delegation turn (parent `Plan::Delegate` → child loop → result
  item), once a `Send` story or thread-per-core harness pattern for nested
  `LocalSet`s is settled.
- Policy decision micro-bench under `arg_equals` constraints (only if the
  tool-turn bench flags `approve::decide` after Phase 3 changes).
