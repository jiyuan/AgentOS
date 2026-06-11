# Agent OS Benchmarks

Target loop overhead: ≤ 2 ms per turn excluding LLM and tool latency. This is the performance budget for the typed run loop described in [`DESIGN.md`](DESIGN.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Current results

| Bench | Mean | 95% CI | Verdict |
|---|---|---|---|
| `loop_overhead/reply_turn` | **1.08 µs** | [1.080, 1.084] µs | ~1850× under the 2 ms ceiling. |

Captured with the criterion harness (100 samples, 3 s warmup, ~4.6 M iterations) on a `current_thread` tokio runtime, release profile.

The bench drives one full happy-path turn through `RunLoopState`:

```
Start → Plan (mock orchestrator returns Plan::Reply) → Finish
```

All "external" costs are mocked — no LLM, no tool, no IO — so the number isolates the run loop's intrinsic per-turn overhead (trace span allocation, transcript handling, guardrail iteration over empty slices, policy decision, state-enum transitions).

## How to reproduce

```sh
cargo bench -p agentos-core --bench loop_overhead
```

Reads `crates/agentos-core/benches/loop_overhead.rs`. Re-uses the tokio runtime across iterations so the measurement reflects loop overhead, not runtime construction.

## Capture environment

- `Linux 6.12.72-linuxkit aarch64`
- `rustc 1.95.0`
- criterion `0.5`
- Mock orchestrator: `agentos_interfaces::test_support::MockOrchestrator::replying`

## Planned bench coverage

These remain unwritten — file an issue or add a `criterion_group` next to `bench_reply_turn` when you need any of them. They are scheduled as Phase 1.1 of [`docs/OPTIMIZATION_ROADMAP.md`](docs/OPTIMIZATION_ROADMAP.md):

- Tool-call turn with `Approve::Allow`.
- Tool-call turn with `Approve::AskUser` (pause + resume serialization overhead).
- Hydrated-memory plan turn (`MemoryManager::hydrate` with a non-empty `SqliteStore`).
- 1000 concurrent conversations for tail latency on the multi-thread runtime.
