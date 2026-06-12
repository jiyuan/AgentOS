# Agent OS Performance & Architecture Optimization Roadmap

Drafted: 2026-06-11

This roadmap is grounded in a code-level audit of all five crates plus
[`DESIGN.md`](../DESIGN.md), [`BENCHMARKS.md`](../BENCHMARKS.md), and
[`PLAN.md`](PLAN.md). Every file/line citation below was verified against the
source at drafting time.

The fact that shapes everything: the run loop already measures **1.08 µs per
happy-path turn against the 2 ms budget** (~1850× headroom, see
`BENCHMARKS.md`). Raw loop speed is not the problem. The opportunities are:

1. The four planned-but-unwritten benchmarks — without them, no optimization
   claim is verifiable.
2. Open correctness/safety debt already tracked as findings A4/A5 in
   `PLAN.md`, plus panic paths in library code.
3. Allocation-heavy and latency-heavy paths verified by direct reads:
   telemetry field maps, the per-call skill-prelude rebuild, per-retry request
   body copies, and an extra LLM routing round-trip.
4. Structural refactors that need semver and module-size care.

## Standing invariants

Every phase must respect the architecture invariants in `CLAUDE.md`:

- Approve stays a concrete engine — never a trait.
- `RunLoopState::step()` keeps consuming `self`; no transition skips a state.
- Sub-agent policies only narrow.
- All channels stay bounded.
- `Arc<str>` on hot paths; `serde_json::RawValue` for tool args.
- Any `agentos-proto` / `agentos-interfaces` change runs
  `cargo semver-checks check-release`.

---

## Phase 1 — Measurement Foundation

All three items are independent and parallelizable. Nothing in Phase 3 or 4
can demonstrate impact until these exist.

### 1.1 Write the four planned benchmarks

The "Planned bench coverage" section of `BENCHMARKS.md` lists them; the
current bench only covers `Start → Plan(Reply) → Finish`:

- Tool-call turn with `Approve::Allow` (exercises `approve::decide()`,
  `ToolRegistry`, the Act/Observe states).
- Tool-call turn with `Approve::AskUser` — pause + resume, measuring
  `RunState` JSON serialization cost.
- Hydrated-memory plan turn (`MemoryManager::hydrate` against a populated
  `SqliteStore`).
- 1000 concurrent conversations on the multi-thread runtime, reporting
  p50/p95/p99 tail latency.

Files: new bench files next to `crates/agentos-core/benches/loop_overhead.rs`
plus `[[bench]]` entries; record results in `BENCHMARKS.md`.
Effort: M. Verify: `cargo bench -p agentos-core` produces stable numbers
(criterion noise < ~5%).

### 1.2 Allocation-count instrumentation

The telemetry finding below is "~40+ heap allocations per plan turn" — make
that a measured number. Add a counting global allocator (or `dhat-rs`) behind
a bench-only feature, reporting allocations/turn alongside time/turn for the
reply turn and the tool turn.

Files: new `crates/agentos-core/benches/alloc_count.rs`.
Effort: S. Verify: deterministic allocation count on the mocked turns.

### 1.3 CI workflow wiring the existing scripts

All checks are manual today; Phases 2–4 churn enough code to need a net. Wire
exactly what already exists — no new policy:

- `cargo fmt --all --check`, `cargo check --workspace`,
  `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`
- `scripts/check-import-boundaries.sh`, `scripts/check-module-size.sh`
- `cargo semver-checks check-release -p agentos-interfaces` (advisory job)
- Bench compile smoke (`cargo bench -- --test`) — compile benches, never gate
  on timings (CI runners are too noisy).

Files: new `.github/workflows/ci.yml`.
Effort: S–M. Verify: green run on a no-op PR; the Phase 2.1 boundary fixture
fails the boundary job when wired in.

---

## Phase 2 — Correctness & Safety Debt

Cheap, and it protects the invariants every later phase refactors around.
Best landed after 1.3 so the tests run on every PR.

### 2.1 A5 regression suite — **done 2026-06-11**

The five missing tests from `PLAN.md` finding A5:

1. Adversarial max-turns: an orchestrator returning `Plan::CallTool` every
   turn must terminate with the budget-exhausted path.
2. `Policy::narrow` property tests (proptest, ≥1000 cases): `narrow(parent,
   child)` never grants anything the parent denies.
3. Paused `RunState` JSON round-trip + resume with trace continuity. This
   test also gates Phase 4.2 (wire-format change).
4. Parent/child narrowing across configured sub-agents at runner level.
5. Import-boundary negative fixture: a fixture crate that *should* fail
   `scripts/check-import-boundaries.sh`, proving the checker is not
   vacuously passing.

Files: `crates/agentos-core/tests/`, proptest dev-dependency, fixture crate;
update `PLAN.md` A5 status.
Effort: M. Verify: `cargo test --workspace`.

### 2.2 Remove panic paths from library code — **done 2026-06-11**

Outcome note: the `approve/mod.rs:527,599` citations turned out to be
`#[cfg(test)]` code (allowed); the real fixes were `approve/mod.rs:390`
(restructured away the `Option`), `loop/mod.rs:401`, and the same
poisoned-mutex pattern in `task_workspace.rs:104` (both recover via
`PoisonError::into_inner`).

Violations of the no-`unwrap()`/no-panic rule on live paths:

- `crates/agentos-core/src/approve/mod.rs:390` — restructure so the `Option`
  is unnecessary.
- `approve/mod.rs:527` and `:599` — propagate as typed error variants.
- `crates/agentos-core/src/loop/mod.rs:401` —
  `.expect("usage_sink mutex poisoned")`: recover via
  `PoisonError::into_inner()` (the sink is append-only, so a poisoned guard
  is still safe to drain).

Effort: S. Verify: grep shows no `expect(`/`unwrap(` outside tests/benches in
`agentos-core` and `agentos-llm` src; full test suite passes.

### 2.3 Close A4: a single workspace-config loader — **done 2026-06-11** (wrapper deletion pending one release)

`runtime::load_workspace_config()` is a compatibility wrapper around the
canonical `WorkspaceConfig::load()`, with the deprecation decision pending.
Decide now: add an equivalence test for both paths on a fixture config, mark
the wrapper `#[deprecated]` for one release, then delete it.

Files: `crates/agentos-core/src/runtime/`, `PLAN.md` A4.
Effort: S–M. Verify: equivalence test; workspace compiles after removal.

### 2.4 Module-size allowlist audit — **done 2026-06-11 (pulled into Phase 1)**

Audited while wiring CI: `orchestrator/max.rs` (994 raw lines) is *under* the
ceiling once `#[cfg(test)]` blocks are excluded, but
`crates/agentos-core/src/loop/mod.rs` (866 production LOC) crossed it during
the max-turns budget work. It is now allowlisted as tracked debt in
`scripts/check-module-size.sh` with the Phase 4.1 split as the exit plan;
recorded in `PLAN.md`.

---

## Phase 3 — Allocation & Hot-Path Wins

Ordered by real-world impact. Honest framing: items 3.3–3.7 matter for
allocator pressure under concurrency (visible via benches 1.1/1.2), not the
single-turn number, which is already 1.08 µs. Items 3.1–3.2 save real
wall-clock latency.

### 3.1 Cut the extra LLM classifier round-trip in routing (biggest win) — **done 2026-06-11**

Outcome: (a) lexical pre-filter — the classifier only runs when the input
shares significant vocabulary with a specialized route, and only for tables
where every rule carries examples; (c) `[routing].llm_classifier` config
flag. The classifier domain catalog JSON is also precomputed per
orchestrator instead of rebuilt per decision.

`MaxOrchestrator::route_user_text_with_llm`
(`crates/agentos-core/src/orchestrator/max.rs:245-279`) spends a full LLM
call per routing decision — hundreds of milliseconds of user-visible latency
plus token cost, dwarfing everything else here. Do, in order of preference:

- (a) Cheap heuristic pre-filter so the classifier only runs on genuinely
  ambiguous input.
- (c) Config flag in `agent.toml` to disable LLM routing entirely.
- Defer (b) — folding routing into the main `plan()` call via structured
  output — pending a behavioral eval.

Effort: M. Verify: existing routing tests plus new heuristic-path tests;
manual latency comparison (LLM-bound, no benchmark).

### 3.2 Stop copying the request body per retry attempt — **done 2026-06-11**

`send_once` (`crates/agentos-llm/src/providers/mod.rs:252-263`) does
`headers.clone()` + `body.to_vec()` on every attempt — up to 5 full-body
copies under a retry storm, exactly when the system is already stressed.
Switch the body parameter to `bytes::Bytes` (clone is a refcount bump).

Effort: S, internal-only. Verify: `cargo test -p agentos-llm`; retry/backoff
tests pass.

### 3.3 Telemetry allocation diet — **done 2026-06-11** (~20% fewer allocs/turn)

Outcome notes: keys are interned **per thread** (`thread_local!`), not in
global statics as planned below — the global table regressed concurrency
p50 ~3.5× from cross-thread `Arc` refcount contention (see the finding in
`BENCHMARKS.md`). The failure-helper map clones were kept: both emitted
events genuinely carry identical fields and the path is cold.

`crates/agentos-core/src/loop/telemetry.rs` allocates ~40+ times per plan
turn:

- Static keys (`"plan_kind"`, `"target_type"`, `"policy_id"`, …) call
  `Arc::from("literal")` per event — intern them in `LazyLock<Arc<str>>`
  statics.
- `record_telemetry_event` (line 123) eagerly runs
  `serde_json::to_string(&fields)` for an `info!` log even when the level is
  disabled — gate behind `tracing::enabled!(Level::INFO)`.
- `record_subagent_failure` / `record_suborch_failure` clone the full field
  map to emit two events — compose base + delta instead.
- `Value::String(arc.as_str().to_owned())` copies `Arc<str>` contents — keep
  the copy only where the event is actually recorded.

Effort: S–M. Verify: alloc bench (1.2) before/after — target single-digit
allocations per turn; trace-output snapshot tests unchanged.

### 3.4 Cache the skill prelude — **done 2026-06-11** (built in `with_skill_catalog`, no `OnceLock` needed)

`skill_prelude_message()` (`max.rs:221-243`) rebuilds the full concatenated
SKILL.md prelude `String` on every `plan()` call; the catalog is fixed per
runtime. Build once into `OnceLock<Option<Message>>` — `Message` content is
`Arc<str>`-backed, so the cached clone is cheap. The per-item metadata
`BTreeMap` deep-clone in the transcript copy (`max.rs:195-201`) is deferred
to Phase 4.2, which removes the empty-map case for free.

Effort: S. Verify: alloc bench delta; prelude unit tests.

### 3.5 Standardize provider tool-arg extraction — **done 2026-06-11** (`raw_args_from_json_str` / `raw_args_from_json_value`; object shape now uses `to_raw_value`, no clone or re-parse)

`openai.rs`, `anthropic.rs`, `deepseek.rs`, and `ollama.rs` each do their own
`Value → String → RawValue` round-trip with extra `to_owned()`. Add one
shared helper in `providers/mod.rs` that reaches `Box<RawValue>` with at most
one allocation; one place to fix escaping bugs.

Effort: M. Verify: provider fixture tests plus a shared round-trip test
asserting byte-identical `RawValue` output across providers.

### 3.6 Topo-sort escalation stages at template load — **done 2026-06-11**

`crates/agentos-core/src/loop/escalate.rs:102` clones
`spec.template.stages` wholesale, then resolves dependencies O(stages²) per
escalation. Resolve the order once at template load
(`workspace/suborchs/*.toml`) and store it on the template. Bonus: cyclic
templates get rejected at load time instead of at runtime.

Effort: S–M. Verify: existing escalation tests plus a
cyclic-template-rejected-at-load test.

Outcome note (done 2026-06-11): the order is computed index-based at
escalation start (no stage clones; the whole order resolves before any stage
runs, so a broken template no longer partially executes), and
`WorkspaceConfig::load` rejects cyclic/dangling `depends_on` at startup via
the shared `config::stage_execution_order`. The order is not persisted on
`OrchestratorTemplate` because that is an `agentos-interfaces` wire type —
revisit alongside 4.2 if it ever shows up in a profile.

### 3.7 Small-fry batch (opportunistic, alongside 3.3–3.6) — **done 2026-06-11**

- `crates/agentos-interfaces/src/orchestrator.rs:223` — drop the
  `raw.clone()` before `serde_json::from_value` in
  `push_llm_usage_from_message`.
- `crates/agentos-core/src/loop/items.rs` — no change needed: the
  `is_char_boundary()` walk is already bounded to ≤3 iterations (UTF-8
  boundary distance); the original finding overstated it.
- `approve/mod.rs decide()` re-parses tool args per decision but is already
  well-gated by `tool_has_arg_constraints()` — left as is; the tool-turn
  bench does not flag it.

Effort: S. Verify: full test suite; tool-turn bench unchanged or better.

---

## Phase 4 — Architectural Refactors

These need the Phase 2 safety net and the Phase 1.3 semver-checks job.

### 4.1 Split oversized modules

Pure code motion, no behavior change:

- `loop/mod.rs` (866 production LOC — over the ceiling, allowlisted
  2026-06-11) → extract the budget-exhaustion path and `record_llm_usage`
  plumbing into sibling modules next to `approval.rs`/`telemetry.rs`, then
  drop the allowlist entry.
- `orchestrator/max.rs` (994 raw lines, near-ceiling) → `max/mod.rs`
  (planner core), `max/routing.rs` (routing + 3.1 heuristics),
  `max/prelude.rs` (skill prelude + 3.4 cache). Pair with 3.1/3.4 when
  touching the file anyway.
- `approve/mod.rs` (619 lines) → extract the hand-written YAML parser to
  `approve/yaml.rs`. Keep it hand-written — do not add a `serde_yaml`
  dependency without an explicit decision; the minimal parser is likely a
  deliberate supply-chain choice.
- Shrink the allowlisted `agentos-cli/src/main.rs` and
  `bin/agentos-gateway.rs` per the existing allowlist debt.

Effort: M. Verify: `scripts/check-module-size.sh` passes with a smaller
(ideally empty) allowlist; full tests; `git diff --stat` shows moves, not
rewrites.

### 4.2 Wire format: make metadata optional — **risky**

`agentos-proto` types (`Message`, `Envelope`, `TraceEvent`, `ToolResult`)
always allocate a `BTreeMap<Arc<str>, Value>` for metadata, even when empty,
and serialize `"metadata": {}` on every wire item — multiplied by transcript
length on every LLM call. Change to `Option<BTreeMap<...>>` with
`#[serde(default, skip_serializing_if = "Option::is_none")]`.

Risk flags:

- Wire-format and public-API change — every construction/access site across
  all crates changes.
- Old persisted `RunState` blobs (paused sessions) must still deserialize.
  Commit a fixture serialized from the *pre-change* version first; the 2.1
  round-trip test is the gate.
- External consumers of the JSON wire format see fields disappear.

Effort: M–L (mechanical but wide). Verify:
`cargo semver-checks check-release -p agentos-proto -p agentos-interfaces`
(expect a major bump); old-fixture deserialization test; alloc bench delta.

### 4.3 `agentos-llm` error model: `String` → thiserror enum — **API-breaking**

Provider errors are `Result<_, String>` throughout `providers/`, violating
the project's thiserror convention and forcing retry logic to depend on
error-string formatting. Introduce an `LlmError` enum (`Http { status,
retry_after }`, `Provider { code, message }`, `Transport`, `Decode`, …) so
retry decisions match variants structurally.

Ship together with 4.2 as one major-version event, not two.

Effort: M. Verify: `cargo semver-checks check-release -p agentos-llm`
(expect major); retry tests rewritten to assert on variants.

### 4.4 Decide and document the extensions/plugin boundary

Orchestrators and memory backends are compiled-in enums selected by
`agent.toml` strings; `extensions/` exists but the plugin path is weaker than
the README structure implies. Options:

- (a) **Honest docs** — document compiled-in-enum-plus-feature-flags as the
  supported mechanism and align the README. Recommended now. Effort: S.
- (b) Link-time factory registry in `agentos-interfaces` so extension crates
  register constructors without core importing them. Write as a design doc
  first if demand materializes. Effort: L.
- (c) Dynamic library loading — recommend **against** (ABI hazard, conflicts
  with the safety posture).

Approve stays concrete regardless; the plugin surface is orchestrators,
memory, tools, and channels only.

Verify: `scripts/check-import-boundaries.sh` still passes; doc review.

---

## Sequencing

```
Phase 1 (parallel): 1.1 benches · 1.2 alloc bench · 1.3 CI
Phase 2: 2.1 A5 tests · 2.2 expect() removal · 2.3 A4 loader · 2.4 allowlist audit
         (2.1's round-trip test gates 4.2; narrow proptests gate policy-adjacent work)
Phase 3: 3.1 routing → 3.2 Bytes retry → 3.3 telemetry → 3.4 prelude cache
         → 3.5 provider args → 3.6 topo-sort → 3.7 small-fry
Phase 4: 4.1 module splits (pair with 3.1/3.4 when touching max.rs)
         4.2 + 4.3 together as one major-version event · 4.4 anytime
```

Expectation-setting: the single-turn loop number will barely move — it is
already 1.08 µs. The wins that matter are 3.1 (whole LLM round-trips of
user-visible latency and token cost), 3.2 (retry-storm resilience), allocator
pressure at 1000-conversation concurrency (3.3/3.4/4.2, visible only once
bench 1.1 exists), and the panic-path and test-coverage risk reduction in
Phase 2.
