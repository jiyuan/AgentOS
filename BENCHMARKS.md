# Agent OS Benchmarks

Target loop overhead: ≤ 2 ms per turn excluding LLM and tool latency. This is the performance budget for the typed run loop described in [`DESIGN.md`](DESIGN.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Current results

Captured 2026-06-11 (post roadmap Phase 3) with the criterion harness
(100 samples, 3 s warmup) on `current_thread` tokio runtimes, release
profile, unless noted. Single-turn wall times move ±5–8% between runs on
this machine; treat deltas inside that band as noise.

| Bench | Mean | Verdict |
|---|---|---|
| `loop_overhead/reply_turn` | **2.48 µs** | ~800× under the 2 ms ceiling. 1.19 → 1.46 µs at D1 (racing the planning call against the cancellation token), then → 2.48 µs at D2 — see both notes below. |
| `loop_overhead/tool_turn_allow` | **6.50 µs** | Approve + tool dispatch + extra states add ~4 µs over a reply turn. 3.00 → 3.25 µs at C2, → 3.93 µs at D1, → 6.50 µs at D2; ~310× under the ceiling. |
| `loop_overhead/ask_user_pause_resume` | **11.0 µs** | Full interruption cycle: pause, JSON persist round-trip, approve, resume, finish. 7.71 → 8.46 µs at D1, → 11.0 µs at D2. |
| `loop_overhead/paused_state_json_round_trip` | **3.57 µs** | `RunState` serialize + deserialize alone. **The control**: it drives no loop state, and it has stayed at ~3.5 µs across C2, D1, and D2, so the other benches' movements are real rather than machine drift. |
| `loop_overhead/hydrated_memory_plan_turn` | **278 µs** | SQLite-backed hydration dominates the turn but stays ~7× under the ceiling. Unmoved by D1 or D2: a fixed microsecond disappears against the query. |

### The D1 cancellation cost

Roadmap item D1 added ~270 ns per raced await. Each
`tokio_util::sync::CancellationToken::cancelled()` future registers a waker in
the token's intrusive wait list on first poll, and the loop creates one per
planning call and one per tool call. `RunContext` also carries a token now, so
each context the loop builds costs one `Arc` clone.

Not optimised away, and not optimisable without giving up the feature: a
cancellation that only takes effect *between* awaits cannot stop an LLM call
that is thirty seconds into a response, which is the case users actually want
stopped. The absolute figure — a quarter of a microsecond against a 2 ms
budget — is the right trade. Recorded here so a future regression hunt does not
re-derive it.

### The D2 cost is codegen, not work

D2 added ~1 µs to every loop bench, and it is worth being precise about where
it does *not* come from. `reply_turn` calls no tool, builds its `LoopDeps` with
`tools: None`, and touches neither `ToolRegistry` nor `ToolSpec` — yet it moved
1.46 → 2.48 µs. Four measurements, all on the same machine in one session:

| Tree | `reply_turn` |
|---|---|
| D1 (HEAD before D2) | 1.42 µs |
| HEAD + only the `tokio` `process`/`time` features | 1.50 µs |
| D2 with `tools/exec.rs` compiled but `registry`/`shell`/`mcp` reverted | 1.47 µs |
| D2 complete | 2.48 µs |

So it is not the new tokio features (+5%, inside the noise band), and not the
executor module merely existing. It appears when the *call sites* land — the
`tokio::time::timeout` wrapper in the registry and the async subprocess paths
in `shell`/`mcp` — none of which execute on a reply turn. That leaves codegen:
pulling the timer and process machinery into `agentos-core` alongside the loop
changes what gets inlined around it.

Not chased further. It is a fixed ~1 µs against a 2 ms budget, the alternative
is a runtime with no tool deadlines at all, and the control bench confirms
nothing is wrong with the measurement. If loop overhead ever becomes the
binding constraint, the lead to pull is codegen-unit placement, not the
deadline logic.

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
