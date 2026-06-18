# AgentOS Feature Roadmap

Last written: 2026-06-12

This is the forward-looking *feature* roadmap. It is distinct from
[`OPTIMIZATION_ROADMAP.md`](OPTIMIZATION_ROADMAP.md) (performance/correctness debt,
all four phases complete as of 2026-06-12) and from [`PLAN.md`](PLAN.md) (invariant
and config-authority milestones). It supersedes the net-new-feature items in
`PLAN.md` "Later Work"; that section now points here.

Scope and audience: this roadmap targets a **single-operator deployment** — the
runtime the author actually runs (TUI + Telegram/Feishu gateway), not a
third-party framework release. Items are sequenced for "things I use day-to-day"
value, not public-API surface or extension-author ergonomics.

Format follows the house style: each item carries **Files**, **Verify**, and
**Exit** lines so an implementer can execute later without re-deriving the
baseline.

## Baseline (verified 2026-06-12)

The architecture is healthy and feature-complete for its current milestones: typed
run loop, concrete `Approve`, policy narrowing, scoped memory with hydration,
import boundary, and the §16 verification matrix all pass. The gaps this roadmap
addresses:

- **No streaming.** The `Llm` trait (`crates/agentos-llm/src/lib.rs`) exposes only
  `complete()` / `complete_messages()`, each returning a single `Message`. All four
  providers (`openai`, `anthropic`, `deepseek`, `ollama`) hardcode `"stream":
  false`. The loop consumes one `Message` per `Orchestrator::plan()`.
- **Empty `extensions/`.** `extensions/orchestrators/` and `extensions/memory/`
  contain only `.gitkeep`. Per [`ARCHITECTURE.md §4`](ARCHITECTURE.md), everything
  is compiled-in and selected by `agent.toml` strings; dynamic loading is rejected
  (no stable Rust ABI, bypasses the import-boundary + authorization posture). A
  link-time factory registry is the designated upgrade path but is not built.
- **Memory infra exists but is idle.** `MemoryManager`, the `Memory`/`Session`
  traits, and `reflect()` (`crates/agentos-core/src/memory/reflection.rs`) are
  implemented with SQLite plus optional sqlite-vec / Qdrant indexes, but the
  open decisions in [`ARCHITECTURE.md §17`](ARCHITECTURE.md) — default episode
  recording, scheduled reflection, which vector backend leads — are unresolved, so
  the smart-memory paths don't run in a default deployment.

## Cross-cutting constraints

These apply to every item below; they are not repeated per-item.

- Re-run the [`ARCHITECTURE.md §16`](ARCHITECTURE.md) verification matrix before
  claiming any item done: `cargo fmt --all --check`, `cargo check --workspace`,
  `cargo test --workspace`, `bash scripts/check-import-boundaries.sh`,
  `scripts/check-module-size.sh`.
- Changes to `agentos-interfaces` (A1 trait method, C2 registry) require
  `cargo semver-checks check-release -p agentos-interfaces`. A1 is additive (a
  defaulted trait method) and should be non-breaking; note any break in the PR.
- Respect module-size governance (`PLAN.md` A6). New behavior goes in **new**
  modules — do not extend `runner.rs` (1242 LOC) or `loop/mod.rs` (686 LOC).
- Preserve loop invariants from [`DESIGN.md`](../DESIGN.md): no new state-machine
  transitions, guardrail/approval placement unchanged, all channels bounded.

## Track A — LLM Streaming

Goal: token-level streaming from provider → loop → channel, so the deployed
gateway shows incremental output instead of one blocking reply.

### A1. Streaming trait surface

Add `complete_stream()` to the `Llm` trait returning a stream of typed deltas
(text chunk, tool-call delta, usage, done). Ship a **default implementation that
calls `complete()` and emits a single terminal delta**, so non-streaming providers
and every existing call site keep working unchanged.

- Files: `crates/agentos-llm/src/lib.rs` (trait + default impl); new delta wire
  type in `crates/agentos-proto/src/` (or `agentos-llm` if it never crosses the
  wire).
- Effort: S.
- Verify: `cargo check --workspace` (default impl keeps all providers compiling);
  `cargo semver-checks check-release -p agentos-interfaces` reports additive-only.
- Exit: trait has `complete_stream()`; a non-streaming provider yields exactly one
  delta equal to its `complete()` result.
- **Status: done** (commit `d13719b`). Implemented as `CompletionEvent { Text,
  Done }` + a `CompletionStream` alias in `agentos-llm` (kept out of proto — it
  is in-process only). Default `Llm::complete_stream` adapts `complete()` via the
  public `single_message_stream()` helper. The delta type was simplified from the
  "tool-call delta / usage" sketch: tool calls and usage ride on the terminal
  `Done(Message)` rather than as separate streamed events, which keeps the loop's
  existing final-message handling unchanged.

### A2. Provider implementations

Implement real SSE streaming for the providers actually used in deployment first
— OpenAI-compatible (covers `openai` and `deepseek`), then `anthropic`. Flip
`"stream": true`, parse the SSE event stream into deltas, accumulate usage from the
terminal event. Reuse the one-`reqwest::Client`-per-provider rule and the existing
`post_json` / `ProviderError` plumbing.

- Files: `crates/agentos-llm/src/providers/{openai,deepseek,anthropic}.rs`,
  `providers/mod.rs`. `ollama` can stay on the default fallback until needed.
- Effort: M.
- Verify: integration test asserts the concatenated stream text + total usage equal
  the non-streaming `complete()` result for the same prompt (recorded fixture).
- Exit: at least the OpenAI-compatible and Anthropic providers stream natively;
  usage accounting matches the non-streaming path byte-for-byte on the final text.
- **Status: done** for openai + deepseek (`5650878`) and anthropic (`26a28d2`);
  only ollama remains on the A1 single-chunk fallback. Shared SSE machinery in
  `providers/stream.rs` behind an `SseAccumulator` trait + `run_sse_stream()`
  driver (byte-buffered line splitting so multi-byte UTF-8 across network chunks
  is never decoded mid-codepoint). `OpenAiAccumulator` handles the
  `chat.completion.chunk` shape (tool-call fragments merged by index; final
  `include_usage` chunk); `AnthropicAccumulator` handles the typed Messages event
  stream (content-block-indexed text/tool-use, `input_json_delta` argument
  assembly, usage reassembled from `message_start` + `message_delta`). Usage is
  logged + attached through the same path as the buffered response in every case.
  `post_sse()` in `providers/mod.rs` is single-attempt (no retry of a partially
  consumed stream). `EnvLlm::complete_messages_stream` dispatches
  openai/deepseek/anthropic natively.

### A3. Loop + channel plumbing

Surface streaming **without a new loop state**: streaming is a `Plan::Reply`
refinement. The orchestrator drains `complete_stream()`, the loop emits incremental
output events through the gateway/channel egress, and **output guardrails still run
on the fully assembled final text before `Finish`** — preserving the
[`DESIGN.md`](../DESIGN.md) guardrail-placement invariant. Channels gain an
optional incremental egress (edit-in-place for Telegram/Feishu, append for TUI).

- Files: `crates/agentos-core/src/loop/` (new submodule, do not grow
  `loop/mod.rs`), `runner.rs` egress path, `gateway/`,
  `channels/{telegram,feishu}`, `agentos-cli` TUI.
- Effort: M–L.
- **Risk:** the loop future is `!Send` ([`BENCHMARKS.md`](../BENCHMARKS.md)
  "loop future is `!Send`"). Streaming must not add a `Send` bound or hand the
  stream across threads — drive it on the same `current_thread` runtime as the run.
- Verify: a streamed run produces an identical final transcript and token-usage
  trace to the non-streaming path (regression against the existing usage
  accounting); the TUI shows incremental tokens; a seeded output-guardrail
  violation still trips before finish.
- Exit: streaming is opt-in via config, default-off path is byte-identical to
  today, guardrails and usage accounting unaffected.
- **Status: done** (commit `3f1e6af`). Implemented exactly as the design note
  below: `StreamSink` + optional `RunContext.stream_sink` (mirroring
  `usage_sink`), threaded through `LoopDeps`/`RunnerDeps` and installed in
  `loop/mod.rs::plan()`; `Llm::complete_messages_stream` added so `Max` streams
  with its skill prelude + tools; a shared `orchestrator/streaming.rs` helper
  drives `Min`/`Max`; `LlmOrchestrator` streams its reply. CLI TUI renders tokens
  to stdout (on by default; `AGENTOS_TUI_STREAM=0` to disable), coordinating with
  `TuiChannel::send` via a shared flag so the reply prints once. Only the TUI
  installs a sink, so gateway/one-shot paths stay byte-identical.
  **Caveats / deviations:** (1) streamed text is *provisional* — output
  guardrails still gate the finish (a violation errors the run) but tokens are
  already on screen; documented as the streaming trade-off. (2) Telegram/Feishu
  edit-in-place egress is still future work (they get the final reply, no
  regression). (3) `agentos-interfaces` gained a public `RunContext` field
  (breaking, like `usage_sink`); `cargo semver-checks` can't verify it locally —
  the crate isn't published — so it's noted rather than machine-checked.
- **Original design note (for reference):**
  - **Injection via a sink on `RunContext`**, mirroring the existing `usage_sink`
    field (added in commit `aaadb51`): add an optional
    `stream_sink: Option<Arc<dyn Fn(&str) + Send + Sync>>`. `LoopDeps` carries it
    and `loop/mod.rs::plan()` sets it on the freshly built `RunContext` (same
    spot the `usage_sink` is drained). When `None`, orchestrators call
    `complete()` exactly as today — default path stays byte-identical.
  - **Orchestrators** `LlmOrchestrator` (`agentos-llm`), `MinOrchestrator`,
    `MaxOrchestrator`: when a sink is present and no tools are in play, drain
    `complete_stream(ctx)`, push each `Text` chunk to the sink, and return
    `Plan::Reply(done_message)`.
  - **Constraint to resolve first:** `complete_stream(ctx)` carries *no tool
    specs*, but `MinOrchestrator`/`MaxOrchestrator` pass specs via
    `complete_messages(messages, tools)`. So either (a) restrict streaming to the
    no-tool reply turn (simplest; tool-deciding turns stay buffered), or (b) add a
    `complete_messages_stream(messages, tools)` trait method. Recommend (a) for
    the first cut — the visible win is the final natural-language reply, and
    tool-call turns produce little user-facing text anyway.
  - **Egress:** CLI TUI first (sink writes tokens to stdout); Telegram/Feishu
    edit-in-place followed (`9553fc6`, `a17034f`). To support async channel
    egress the `StreamSink` was made async (`Fn(&str) -> BoxFuture`) and a
    `StreamEgress` handle was added to the `Channel` trait — decoupled from the
    `&mut self` receive path, with per-conversation throttled edits and
    finalization in `Channel::send`. On by default in the gateway;
    `AGENTOS_GATEWAY_STREAM=0` disables.
  - **Interface note:** adding a field to `RunContext` is technically a breaking
    change to `agentos-interfaces`; the repo already accepts this pattern
    (`usage_sink`). Run `cargo semver-checks check-release -p agentos-interfaces`
    and note it.

## Track B — Memory Intelligence

Goal: resolve the [`ARCHITECTURE.md §17`](ARCHITECTURE.md) memory open-decisions
for a single-operator deployment and make reflection/episodes actually *run*.

### B1. Default episode recording

Decide and wire a sane default recording policy — record failures, denials,
approvals, multi-step tool workflows, sub-agent workflows, explicit corrections,
and explicit memory writes; skip trivial one-turn replies (already specified in
[`ARCHITECTURE.md §11`](ARCHITECTURE.md) "Episode Recording"). Make it config-gated
under `[memory]` so it can be disabled.

- Files: `crates/agentos-core/src/memory/`, `config/mod.rs`, `workspace/agent.toml`.
- Effort: S–M.
- Verify: `cargo test -p agentos-core memory`; a seeded denied run produces one
  episodic record; a trivial reply produces none.
- Exit: episode recording runs as a post-finish side effect by default, with a
  documented `[memory]` toggle.
- **Status: done** (already implemented; `624abf8` documents it). The recording
  policy was already wired in `runner/episodes.rs` (skips trivial runs via
  `EpisodeRecord::should_record`; records failures/denials/approvals/multi-step/
  tool/sub-agent/explicit-write runs) and gated by `[memory]
  .episode_recording_enabled`, which the deployment `agent.toml` enables. The
  struct `Default` stays `false` (conservative, matching `hydration_enabled`); a
  clarifying comment was added to `agent.toml`.

### B2. Cron-driven reflection

Wire the existing `reflect()` to the cron path so promotion (episodic → semantic),
compression of old episodes, and supersession of contradicted facts run off the hot
path on a schedule.

- Files: `crates/agentos-core/src/memory/reflection.rs`,
  `crates/agentos-core/src/crons/`.
- Effort: M.
- Verify: `cargo test -p agentos-core reflection`; on a simulated cron tick a
  seeded set of repeated episodes promotes into a semantic fact, and the
  `memory_access_log` records the promotion.
- Exit: a default deployment runs reflection on a configurable schedule; promotion
  and supersession are visible in the audit log and in subsequent hydration.
- **Status: done** (`624abf8`). `reflect()` existed but was never driven in
  production, and the existing `MemoryMaintenanceCron` was single-conversation
  while episodes are per-conversation. Added `MemoryManager::reflect_all` (sweeps
  every conversation with episodes via a new
  `MemoryMaintenance::episodic_conversation_owners`, promotes + supersedes per
  scope, rebuilds the lexical index once), reshaped `MemoryMaintenanceCron` to
  drive it, added `[memory.reflection]` config, and wired it into the gateway's
  idle tick (best-effort, one-line summary logged). **Deviation:** retention
  pruning ("compression of old episodes") is left at `RetentionRequest::default`
  for now — focus is promotion/supersession/index; wiring `[memory.retention]`
  into the sweep is a small follow-up. **Note:** each channel-gateway thread runs
  its own sweep over the shared DB (matches the existing per-channel cron model);
  fine for a single primary channel.

### B3. Vector retrieval as the deployment default

Promote semantic vector retrieval from "optional backend capability" to a
configured default for the operator's domains, behind the embeddings extension from
Track C (**this item depends on C1**). Decide the embedding source: a local
embedding model vs. a provider embeddings endpoint reached through `agentos-llm`.

- Files: `crates/agentos-core/src/memory/` (hydration wiring), the C1 extension
  crate, `workspace/agent.toml`.
- Effort: M (after C1).
- Verify: with the vector backend selected, hydration returns a semantically (not
  just lexically) matched fact for a paraphrased query; lexical+recency remains the
  fallback when embeddings are unavailable.
- Exit: vector retrieval is the configured default for chosen domains, with graceful
  fallback; the embedding source is documented.
- **Status: blocked on C1** (the vector extension crate). B1 and B2 are done, so
  this is the only remaining Track-B item.

## Track C — Extension Ecosystem (internal composition first)

Goal: prove the `extensions/` boundary works end-to-end with one *real* crate,
without building speculative plugin infrastructure a solo deployment doesn't need.
Dynamic library loading stays rejected ([`ARCHITECTURE.md §4`](ARCHITECTURE.md)).

### C1. First extension crate — `extensions/memory/agentos-memory-vector`

Implement the embeddings-backed semantic backend as a separate crate that depends
**only on `agentos-interfaces`** (the `Memory` trait), selected by the
`[memory].backend` string. This is the smallest change that exercises the whole
extension contract: a crate implementing an interface trait, a dependency edge from
`agentos-cli` (never from core), and a registration arm in `runtime` construction.
It doubles as the backend Track B3 needs.

- Files: new `extensions/memory/agentos-memory-vector/` crate; dependency edge in
  `crates/agentos-cli/Cargo.toml`; a registration arm in
  `crates/agentos-core/src/runtime/` construction (string → constructor).
- Effort: M–L.
- Verify: `cargo check --workspace` with the vector backend selected in
  `agent.toml`; `cargo tree` shows the extension depended on **only** by
  `agentos-cli`; the §16 matrix stays green.
- Exit: a real extension crate is wired and selectable by config; core has no edge
  to it.
- **Status: done** (`605f16e`). The natural extension trait was `SemanticIndex`
  (the vector index), not `Memory` — so the move was `SemanticIndex` →
  `agentos-interfaces` (decoupled from the core `MemoryScope`: `upsert` now takes
  only `&Record`, with Qdrant reading the scope fields from managed metadata).
  New `extensions/memory/agentos-memory-vector` (in-memory cosine over a local
  hashing embedder) implements it. The "registration arm" is
  `AgentRuntime::build_with(paths, semantic_factory)`: the **CLI** owns the
  `Fn(&str) -> Option<Arc<dyn SemanticIndex>>` factory (`"vector"` → the
  extension), so only `agentos-cli` depends on the extension — core never names
  it. `cargo tree` confirms the edge; the §16 matrix is green.

### C2. Link-time factory registry (conditional)

Only if C1 shows the manual registration arm is genuinely painful, add the
constructor-registry pattern in `agentos-interfaces` that
[`ARCHITECTURE.md §4`](ARCHITECTURE.md) designates, so extension crates self-register
without a core edit. This needs a short design note first (the architecture doc
requires one before this pattern lands). For a single-operator deployment with few
extensions, prefer leaving this **deferred** — the manual arm is likely fine.

- Files: `crates/agentos-interfaces/src/` (registry), extension crate registration
  call, a design note under `docs/`.
- Effort: M.
- Verify: a new extension registers without editing `agentos-core`;
  `cargo semver-checks check-release -p agentos-interfaces` documents the addition.
- Exit: deferred by default; revisit only when a second/third out-of-tree extension
  makes the manual arm a real cost.
- **Status: deferred** (as intended). C1's CLI-owned factory is a single
  `match` arm — not painful — so the link-time registry isn't warranted yet.

### C3. Boundary regression for the new edge

Extend the import-boundary self-test and integration test to cover the new
extension crate edge: `agentos-core` must never gain a dependency on the extension.

- Files: `scripts/check-import-boundaries.sh` (self-test fixtures),
  `crates/agentos-core/tests/import_boundary.rs`.
- Effort: S.
- Verify: `bash scripts/check-import-boundaries.sh`; a deliberately broken manifest
  adding the extension as a core dep is rejected.
- Exit: CI rejects any core→extension dependency for the new crate.
- **Status: done** (`605f16e`). The checker now also rejects a core/interfaces
  dependency on a *named* extension crate (catching a workspace-inherited
  `agentos-memory-vector.workspace = true` that the `extensions/` path-pattern
  can't see), with a new `--self-test` fixture exercised by
  `tests/import_boundary.rs`.

## Sequencing

```
A1 (trait + default fallback) ─► A2 (providers) ─► A3 (loop/channel)
B1 (episodes) ─► B2 (cron reflection)
C1 (vector ext crate) ──────────► B3 (vector default)   [C1 unblocks B3]
C1 ─► C3 (boundary test)   ·   C1 ─► C2 (registry, conditional/deferred)
```

Tracks A, B, and C are independent at the top and can proceed in parallel; B3 is
the only cross-track dependency (waits on C1). Recommended order for a solo
deployment:

1. **Track A** — most visible day-to-day UX win (incremental output in the gateway).
2. **C1 + Track B** — the memory you actually rely on; C1 lands the vector backend,
   B1/B2 turn on recording and reflection, B3 makes vector retrieval the default.
3. **C2** — only if a second extension makes manual registration painful.

## Status

- 2026-06-12: roadmap authored.
- 2026-06-13: **Track A1 done** (`d13719b`) — streaming trait surface + default
  fallback. **Track A2 done for openai + deepseek** (`5650878`) — native SSE
  decoding with usage parity.
- 2026-06-15: **Track A3 done** (`3f1e6af`) — streaming wired through the loop,
  the Min/Max/Llm orchestrators, and the CLI TUI. **Native Anthropic SSE done**
  (`26a28d2`), completing A2 for all HTTP providers. **Telegram + Feishu
  edit-in-place egress done** (`9553fc6`, `a17034f`) via an async `StreamSink`
  and a `Channel` `StreamEgress` handle. **Track A is fully complete** for
  openai + deepseek + anthropic across the CLI TUI and both chat channels
  (ollama uses the single-chunk fallback). No remaining Track-A follow-ups. All
  work on branch `docs/feature-roadmap`; `cargo test --workspace`, clippy
  `-D warnings`, fmt, and the import-boundary / module-size scripts are green.
- 2026-06-18: **Track B1 + B2 done** (`624abf8`). Episode recording was already
  wired (B1); added the cron-driven whole-memory reflection sweep
  (`reflect_all` over all conversations, `[memory.reflection]`, gateway idle-tick
  driver) for B2. **B3 (vector retrieval default) remains, blocked on C1.** Same
  verification matrix green.
- 2026-06-18: **Track C1 + C3 done** (`605f16e`); **C2 deferred** as intended.
  `SemanticIndex` moved to `agentos-interfaces`; first extension crate
  `agentos-memory-vector` implements it; injected via
  `AgentRuntime::build_with` + a CLI-owned factory so only `agentos-cli` depends
  on it; import-boundary checker extended to reject core→extension edges. This
  **unblocks B3**: the vector backend now exists and is selectable
  (`semantic_backend = "vector"`). The remaining B3 work is making it the
  *default* with a *real* embedder — the extension's hashing embedder is
  lexical-vector, not true paraphrase semantics (same limitation as the built-in
  `sqlite_vec`), so true semantic retrieval needs a real embedding source (local
  model or provider endpoint). Verification matrix green.
