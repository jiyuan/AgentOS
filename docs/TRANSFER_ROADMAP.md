# AgentOS Transfer Roadmap

Drafted: 2026-08-15

This roadmap executes the findings in the `agentos-transfer` review — fifteen
transferable ideas from the DeepSeek Harness, verified against the tree at
`e6e33b2`. It is distinct from [`OPTIMIZATION_ROADMAP.md`](OPTIMIZATION_ROADMAP.md)
(performance debt, all four phases complete), [`FEATURE_ROADMAP.md`](FEATURE_ROADMAP.md)
(streaming, memory intelligence, extension ecosystem), and [`PLAN.md`](PLAN.md)
(invariant and config-authority milestones).

Where this roadmap disagrees with a "done" claim in another document, this one is
current: F1 and F8 below both contradict [`ARCHITECTURE.md §15`](ARCHITECTURE.md).

Scope and audience: the same single-operator deployment `FEATURE_ROADMAP.md`
targets — TUI plus the Telegram/Feishu gateway. Items are sequenced by what
breaks a *running* deployment first, not by public-API surface.

Format follows the house style: **Files**, **Effort**, **Depends**, **Verify**,
**Exit** per item, so an implementer can execute later without re-deriving the
baseline. Findings are cited as `F1`…`F15` from the review.

## Baseline (verified 2026-08-15)

Five facts about the current tree that this roadmap exists to change:

- **Memory hydration is inert (F1).** `MaxOrchestrator::hydrate` fills
  `ctx.memory_fragments` (`orchestrator/max.rs:500`), the request is built from
  the skill prelude plus the transcript only (`orchestrator/max.rs:229-240`), and
  no reader turns fragments into a message. The retrieval stack runs, costs a
  query per turn, emits a trace count (`loop/mod.rs:271`), and is discarded.
- **The transcript only grows (F2).** `run_envelope` loads the whole conversation
  (`runner.rs:268`) and sends every item every turn. The single mitigation is
  `SESSION_SCOPE_EPHEMERAL` for cron ticks (`7b52e7d`).
- **No deadline, no cancellation (F3).** `call_isolated_subprocess`
  (`tools/registry.rs:119-143`) and `ShellTool::call`
  (`tools/builtin/shell.rs:56`) both block a Tokio worker on `std::process` with
  no timeout. No cancellation token exists in the loop, `Tool`, or `Llm`.
- **Approvals are answered by whoever speaks next (F4).**
  `bin/agentos-gateway.rs:818-845` treats the next inbound envelope as the
  decision and never matches the pending `InterruptionId`.
- **One run at a time (F5).** `run_channel_gateway` (`bin/agentos-gateway.rs:669`)
  is a serial receive → run → send loop, so F3 and F4 each stall every
  conversation on the channel.

## Cross-cutting constraints

These apply to every item; they are not repeated per-item.

- Re-run the [`ARCHITECTURE.md §16`](ARCHITECTURE.md) verification matrix before
  claiming any item done: `cargo fmt --all --check`, `cargo check --workspace`,
  `cargo test --workspace`, `bash scripts/check-import-boundaries.sh`,
  `scripts/check-module-size.sh`.
- **No new loop states and no new transitions.** Every item below attaches to an
  existing state or runs outside the loop entirely. `RunLoopState::step()` keeps
  consuming `self`.
- **Module-size governance.** New behavior goes in **new** modules. Do not extend
  `runner.rs` (1406 LOC), `orchestrator/max.rs` (1159 LOC), `loop/mod.rs`
  (691 LOC), or `config/mod.rs` (794 LOC). The 800-line ceiling excludes
  `#[cfg(test)]`; only the two CLI binaries are allowlisted.
- **The loop future is `!Send`** ([`BENCHMARKS.md`](../BENCHMARKS.md)). Runs cannot
  be `tokio::spawn`ed onto a multi-thread runtime. Anything touching concurrency
  uses thread-per-core with `current_thread` + `LocalSet`, the shape the
  concurrency bench already models.
- **`Approve` stays concrete.** F8 adds enforcement of a decision the policy
  engine already made; it does not add a policy trait.
- Changes to `agentos-interfaces` run
  `cargo semver-checks check-release -p agentos-interfaces --baseline-rev <rev>`
  and note the result in the PR. The `--baseline-rev` form compares against a
  git revision, so it works even though these crates are unpublished (which
  [`FEATURE_ROADMAP.md`](FEATURE_ROADMAP.md) A3 had recorded as a blocker). The interface-change ledger at the end of this document lists every
  item that touches it.
- Every item that adds a deployment-varying value adds it to `agent.toml` with
  loader validation, never a `DEFAULT_*` constant (F15). The consolidated config
  surface is tabulated at the end.

---

## Phase 0 — The test floor

One item, and it comes first because it is what makes every later phase
verifiable. F1 shipped for months with a fully green suite; the gap is that
nothing asserts what a complete run actually sends to a provider.

### T1. Scripted LLM fixture and golden transcripts (F12)

A `ScriptedLlm` test double implementing `Llm` that (a) returns a pre-scripted
response per call and (b) **records every request it received**. Golden
scenarios assert the recorded requests, the resulting session items, and the run
outcome as one snapshot each.

Scenarios: plain reply; skill prelude; tool call through `Approve::Allow`;
`ask_user` pause and resume; delegate to a sub-agent; cron
`SESSION_SCOPE_EPHEMERAL` scope; and a hydrated-memory turn against a populated
`SqliteStore`.

- Files: new `crates/agentos-core/tests/support/mod.rs` (harness),
  `crates/agentos-core/tests/transcripts.rs` (scenarios), goldens under
  `crates/agentos-core/tests/golden/*.json`.
- Effort: M.
- Depends: nothing.
- Verify: `cargo test -p agentos-core --test transcripts`. Introduce a
  deliberate regression (drop the skill prelude from
  `orchestrator/max.rs`), watch a golden go red, revert.
- Exit: goldens committed and reviewed; the regression proof performed; the P1
  target assertion committed as an `#[ignore]`d test.
- **Status: done** (2026-08-15). Seven goldens plus one ignored P1 target test;
  `cargo test --workspace` green, clippy clean, boundary and module-size checks
  pass. Notes and deviations:
  - **`ScriptedLlm` lives in the test harness, not `agentos-interfaces`.** The
    plan above originally placed it beside the `MockXxx` doubles in
    `test_support.rs`, which cannot work: `Llm` is defined in `agentos-llm`,
    which depends on `agentos-interfaces`, so the double would invert the
    dependency. It is a plain module under `tests/support/`, matching how
    `tests/streaming.rs` already defines its mock LLMs.
  - **A skill-prelude scenario was added after the regression proof failed.**
    The original six scenarios all ran with an empty skill catalog, so deleting
    the prelude injection changed nothing and every golden stayed green. The
    suite could not detect prompt assembly dropping a section — the exact class
    of bug it exists for. The seventh scenario populates a catalog from a temp
    workspace tree; with it, the same regression turns `skill_prelude` red.
    Re-run that proof before trusting a future scenario.
  - **The memory golden is green, not red.** It records today's behavior: the
    request contains only the user message. The F1 assertion lives in the
    `#[ignore]`d `hydrated_memory_reaches_the_model`, and the golden's diff is
    the second signal when P1 lands. Committing a red golden would have left the
    suite failing by design, which CI cannot distinguish from a real break.
  - **Recorded evidence for P1.** `golden/skill_prelude.json` shows the prelude
    reaching the model as a `system` message while `session_items` contains only
    the user turn and the reply. The request is therefore not reconstructable
    from the session — the same defect class as F1, in the one contribution that
    *does* reach the model today. P1 should treat it as a second acceptance case.
  - Re-record after reviewing a diff:
    `AGENTOS_GOLDEN=record cargo test -p agentos-core --test transcripts`.

---

## Phase 1 — One authority over the request

The root cause behind F1 is that there is no single place where a provider
request is assembled, so a contribution can be computed and then quietly not used.

### P1. Prompt assembly module (F1)

A new module that owns the **only** path from `RunState` + `RunContext` to
`Vec<Message>`. Ordered, named sections: persona, skill prelude, memory
fragments, task-workspace context, then the projected transcript. It returns the
messages plus a `PromptManifest` — section ids, item counts, and estimated tokens
— that the loop records as a trace event.

Both orchestrators and the `agentos-llm` trait defaults call it. No caller
assembles a message vector by hand afterwards.

- Files: new `crates/agentos-core/src/prompt/mod.rs` and
  `crates/agentos-core/src/prompt/sections.rs`; call sites in
  `orchestrator/max.rs:229-240`, `orchestrator/min.rs`,
  `orchestrator/streaming.rs`, and `agentos-llm/src/lib.rs:319`/`353`.
- Effort: M.
- Depends: T1.
- Risk: `agentos-llm` cannot depend on `agentos-core`. Either the assembly module
  lives in `agentos-core` and `agentos-llm`'s trait defaults keep their
  transcript-only behavior (acceptable — `EnvLlm::complete(ctx)` is only reached
  by orchestrators that have not opted in), or the section vocabulary moves to
  `agentos-interfaces`. Prefer the first for this cut; revisit if a third
  orchestrator appears.
- Verify: un-ignore `hydrated_memory_reaches_the_model` (T1's target test) and it
  passes; the `memory_hydration` golden diff shows the fragment entering the
  request; `cargo test -p agentos-core memory`; a run with
  `hydration_enabled = false` produces a request byte-identical to today's.
- Exit: every provider request in the golden suite is reproducible from the
  recorded `PromptManifest` plus `RunState` — including the skill prelude, which
  `golden/skill_prelude.json` currently shows reaching the model without
  appearing in the session; `grep` finds no remaining hand-built message vector
  outside `prompt/`.
- **Status: done, with one exit criterion not met** (2026-08-15).
  `crates/agentos-core/src/prompt/{mod,sections}.rs` own assembly; `Max` and
  `Min` both call `prompt::assemble`. `hydrated_memory_reaches_the_model` is
  un-ignored and passing, the `memory_hydration` golden now shows the recalled
  fact entering the request as a framed system message, and the other six
  goldens were **byte-identical** across the change — the pre-record run failed
  exactly one scenario — so the no-hydration path is provably unchanged.
  `cargo test --workspace` green across 24 targets, clippy clean, boundary and
  module-size checks pass. Notes and deviations:
  - **Reconstructability is NOT delivered.** A request is not byte-reproducible
    from `PromptManifest` + `RunState`: the manifest records which sections
    contributed and how much, not their content, and `memory_fragments` live
    only on the transient `RunContext`. Closing this needs a decision the
    implementation cannot make on its own — [`ARCHITECTURE.md §14`](ARCHITECTURE.md)
    forbids full memory bodies in traces, so "model-visible ⟺ logged" has to
    land in the session rather than the trace, which changes every golden and
    interacts with C3's compaction. Tracked as P3 below.
  - **The manifest records `chars`, not tokens.** Token estimation is C1's job
    and lands on this same struct; two estimators would have disagreed.
  - **No `agentos-interfaces` change.** The plan said the loop records the
    manifest as a trace event, which would have needed a `RunContext` sink field
    (the `usage_sink` pattern) and put P1 in the interface ledger. `assemble`
    emits one structured `tracing` event instead — same observability, no
    breaking change, ledger unchanged.
  - **Two LLM calls are deliberately excluded**, both documented at the code:
    the routing classifier's domain round-trip (`orchestrator/routing.rs`),
    which must not carry skills, memory, or the transcript; and
    `Llm::complete(ctx)` in `agentos-llm`, which cannot see sections because
    that crate cannot depend on core. `Llm::complete`'s doc comment now says an
    orchestrator must not build a conversation request through it. Its only
    caller, `LlmOrchestrator`, is unreferenced outside its own crate and is a
    deletion candidate.
  - Persona and task-workspace sections were not added: neither exists as a
    contribution today, so they would have been empty variants.

### P3. Log what the model saw (F1, deferred from P1)

P1 made assembly single-authority but not reconstructable. Decide where the
assembled non-transcript sections are recorded so a past request can be rebuilt:
in the session (the harness's rule, and what fork/resume/compaction need) or in
the trace (cheaper, but [`ARCHITECTURE.md §14`](ARCHITECTURE.md) forbids full
memory bodies there). Resolve that tension before C3, which rewrites the history
any reconstruction would read.

- Files: `crates/agentos-proto/src/request.rs`,
  `crates/agentos-interfaces/src/orchestrator.rs`,
  `crates/agentos-core/src/prompt/`, `crates/agentos-core/src/loop/request.rs`;
  every golden in `crates/agentos-core/tests/golden/` re-records.
- Effort: M. Depends: P1, and a decision on §14.
- Verify: every golden carries a `request_headers` entry per LLM round-trip
  naming that request's sources; no memory body appears in a header.
- Exit: the answer to "what was this request made of" is durable, and §14 still
  holds.
- **Status: done** (2026-08-15). The decision, and what changed because of it:
  - **The record goes to the run trace, not the session, and it names sources
    rather than copying them.** The standard adopted is the harness's actual
    one — *reconstructable from log + code*, not "the log holds a verbatim
    copy". A `request_header` trace event per LLM round-trip records each
    section's id, message count, char count, and its `RequestSource`s: skills by
    name, memory records by namespace and record id. The bodies stay where they
    already live — the workspace `SKILL.md` files, the memory store, and
    `RunState`'s transcript.
  - **Why not the session.** Appending prelude and memory items to the
    conversation would replay them as history next turn, or would need the
    surface/log-only split that P2 has not built yet; and the prelude is
    identical every turn, so a months-long conversation would accumulate
    hundreds of copies of it — the exact growth F2 exists to stop. These are
    per-*run* derived inputs, so they belong to the run record.
  - **Why this satisfies §14.** The prohibition is on memory *bodies* in traces.
    A namespace plus record id is an address, not content, and
    `no_memory_body_reaches_the_trace` asserts it. Nothing was weakened to fit.
  - **The stated exit criterion was wrong and is restated above.**
    `golden/skill_prelude.json` still shows a system message that reaches the
    model without appearing in `session_items`, and under this decision it
    always will. The criterion assumed the session was the answer; the analysis
    says otherwise. What the golden now carries instead is a `request_headers`
    block naming `deploy-notes` as that message's source, which is the
    reconstructability the item was actually for. If a later item needs
    verbatim session-side logging — fork replaying a request exactly, say — it
    needs P2's surface split first and should be raised on its own merits.
  - **Interface change, machine-verified.** `RunContext` gains `request_sink`,
    mirroring `usage_sink` for the same reason: something assembled inside
    `plan()` must reach the loop. `agentos-proto` gains `RequestHeader` /
    `RequestSection` / `RequestSource`, purely additive.
  - **`cargo semver-checks` does work on this repo.**
    [`FEATURE_ROADMAP.md`](FEATURE_ROADMAP.md) A3 recorded that it cannot run
    because the crates are unpublished. `--baseline-rev <rev>` compares against
    a git revision instead of crates.io and needs no publishing:
    `cargo semver-checks check-release -p agentos-interfaces --baseline-rev HEAD`
    reports the one expected major break (`constructible_struct_adds_field` on
    `RunContext.request_sink`) and `agentos-proto` as additive-only. Use this
    form for every future interface change rather than noting breaks by hand.
  - **Unplanned benefit for C1.** Each header carries `total_messages` and
    `total_chars`, so request growth is already visible per turn in the trace —
    `golden/tool_call_allowed.json` shows a run going from 1 message to 3 across
    its two round-trips. C1 can calibrate pressure thresholds against real
    recorded traffic instead of guessing, and should extend this header rather
    than introduce a parallel measurement.

### P2. Transcript projection (F2 groundwork, F10)

Split "what is in the session log" from "what the model sees". A projection
function folds the log into the model-visible item list, skipping items shadowed
by a later checkpoint. Today it is the identity function; P2 exists so C3 can
land without touching the `Session` trait.

This is the harness's core structural bet — durable state is append-only and
every view is a fold — and it is what makes plan/todo state (X7), fork (X6), and
compaction (C3) cheap instead of each being a bespoke mutation path.

- Files: new `crates/agentos-core/src/prompt/projection.rs`; consumed by
  `prompt/mod.rs`; `runner.rs` keeps loading the full transcript unchanged.
- Effort: S.
- Depends: P1.
- Verify: `cargo test -p agentos-core prompt`; projection over a log with no
  checkpoints returns the input unchanged (property test).
- Exit: `Session` trait unmodified — no `semver-checks` entry; the projection is
  the only reader that decides model visibility.
- **Status: done** (2026-08-15). `prompt/projection.rs` holds the fold;
  `prompt::assemble` builds the transcript section from `visible()` rather than
  the raw items. Both exit conditions machine-verified: the eight existing
  goldens are byte-identical (the projection is the identity function until C3
  writes a checkpoint), and `cargo semver-checks --baseline-rev HEAD` reports
  no change to `agentos-interfaces`. Design notes for C3:
  - **A checkpoint is an ordinary item carrying `agentos.transcript_shadow`**
    (`{"start", "end"}`, inclusive positions). Positions are indices into the
    loaded transcript, which is sound because the SQLite store appends with a
    dense monotonic `ordinal` and loads `ORDER BY ordinal ASC`, so an existing
    item's position never moves. No id had to be added to `Item`, so `Session`
    and its data types are untouched.
  - **Shadowing is monotonic.** Every checkpoint's range applies even when a
    later checkpoint hides that checkpoint. Skipping a hidden checkpoint's range
    could resurrect content an earlier compaction replaced, showing the model a
    summary and its originals at once.
  - **A malformed range is ignored, never clamped** — reversed, past the end, or
    reaching the checkpoint's own position. The failure mode is duplicated
    context (wasteful, correct) rather than hiding an unintended span, which
    could separate a tool call from its result and make the request invalid.
    C3 still owns keeping its ranges tool-pairing balanced; the projection will
    not rescue a badly chosen one, it will only decline to apply an impossible
    one.
  - `prompt::checkpoint()` is the writer C3 should use, so the vocabulary has
    one owner. A new golden, `compaction_checkpoint`, exercises the whole stack:
    its `session_items` keeps all five items while its `requests` carries two,
    which is the split this item exists to create.

---

## Phase 2 — Bounding a conversation's lifetime

Ordered by cost: measure, then prune for free, then spend a model call. A
deployment should reach the summarizer rarely.

### C1. Token estimation and pressure measurement (F2)

A heuristic estimator over `Vec<Message>` plus a per-provider context budget
resolved from the model id. `PromptManifest` gains the estimate, and the loop
emits it as a trace field so pressure is observable before any of C2–C4 exist.

Ship this alone first: it is one PR, it is safe, and it tells you what the real
deployment's pressure curve looks like before you tune thresholds you would
otherwise be guessing at.

- Files: new `crates/agentos-core/src/prompt/tokens.rs`;
  `crates/agentos-core/src/loop/telemetry.rs` (field key only).
- Effort: S.
- Depends: P1.
- Verify: `cargo test -p agentos-core prompt::tokens`; estimate is within ~15% of
  a provider-reported `Usage.prompt_tokens` on a recorded run.
- Exit: every turn traces `prompt_estimated_tokens` and `context_budget_tokens`.
- **Status: done, with the accuracy check outstanding** (2026-08-15). The exit
  condition holds: every golden's `request_headers` now carries
  `prompt_estimated_tokens`, `context_budget_tokens`, `tool_tokens`, and an
  integer `pressure_percent`, and each section carries its own token estimate.
  Notes:
  - **The estimator uses two rates, not one.** Four ASCII characters per token,
    but one token per non-ASCII character. The usual 4:1 rule describes English
    and under-counts Chinese by about four times — and this deployment runs
    Feishu and Telegram against DeepSeek, so mixed CJK traffic is the normal
    case. A single divisor would have made the pressure signal useless for the
    traffic it was built for.
  - **It is deliberately biased high.** One token per wide character
    over-estimates some scripts by up to half. That is the safe direction: an
    over-estimate compacts early, an under-estimate lets a request hit the
    provider's hard limit with no compaction attempted, which is the failure C4
    exists to recover from.
  - **Tool schemas are counted.** They carry no messages but occupy the same
    window, and they are not marginal: in `golden/tool_call_allowed.json` one
    `echo` tool costs 31 tokens against a 40-token first request. A section-only
    estimate would have been badly low for any tool-enabled agent.
  - **An unknown model resolves to no budget, never a default.** Budget
    resolution is a new defaulted `Llm::context_budget_tokens()` — only the
    provider adapter knows which model a request reaches — backed by a
    prefix-matched table of published windows in `agentos-llm`, longest prefix
    winning so `gpt-4o` is not budgeted as `gpt-4`. Compaction against an
    invented window would be worse than no pressure signal, so unknown models
    trace an estimate and no pressure. `AGENTOS_CONTEXT_BUDGET_TOKENS`
    overrides the table for self-hosted or proxied models; X3 should move it
    into `agent.toml` beside the compaction thresholds.
  - **The ~15% accuracy check is NOT done and cannot be done from here.** It
    needs a live provider call, which this environment has no key for. The
    mechanism to run it exists and is documented in `prompt/tokens.rs`: a
    request's estimate is on its `request_header` event and the provider's own
    `input_tokens` is on the `llm_token_usage` event under the same plan span,
    so comparing them across a deployment's traces measures the true error rate.
    Pair by span, not by position — a routing classifier round-trip records
    usage with no header. **C3 must not pick thresholds until this has been run
    against real traffic**, which was the entire argument for shipping C1 alone.
  - Interface impact, machine-verified with `--baseline-rev HEAD`:
    `agentos-interfaces` and `agentos-llm` unchanged (a defaulted trait method
    is additive); `agentos-proto` reports one major
    (`constructible_struct_adds_field`) for the token fields on `RequestHeader`
    and `RequestSection` — types introduced by P3 one commit earlier, with no
    consumers outside this workspace.

### C2. Tool-result pruning and output spill (F2, F6)

Two halves of the same problem: today an oversized tool result is cut at 64 KiB
and the remainder is destroyed (`loop/items.rs:10`).

- **Spill on write.** A `SpillStore` persists the full text under
  `workspace/spill/<conversation-hash>/`, and the inline result becomes a
  head/tail preview plus the locator and a retrieval hint in metadata. Best
  effort: a storage failure keeps today's truncated result rather than turning a
  successful call into an error. Directory `0700`, file created with
  `O_EXCL | 0600` so a planted symlink cannot redirect the write.
- **Prune under pressure.** When C1 reports pressure, replace the middle of
  already-recorded oversized tool results with an elision marker citing the spill
  locator. Deterministic, no model call.

- Files: new `crates/agentos-core/src/spill/mod.rs`; new
  `crates/agentos-core/src/prompt/prune.rs`; `loop/items.rs`
  (`bounded_tool_content` delegates to spill).
- Effort: M.
- Depends: C1.
- Verify: `cargo test -p agentos-core spill`; a 1 MiB tool result is fully
  recoverable from the locator with `file`; a symlink planted at the target path
  causes a rejected write, not a followed one; the golden for a tool turn shows
  preview + locator.
- Exit: no tool output is unrecoverable; `MAX_TOOL_RESULT_CONTENT_BYTES` is a
  config field (see X3).
- **Status: done** (2026-08-16). Both exit conditions hold. With a store
  configured, output past the cap is written to
  `workspace/spill/<run id>/<tool>-<call id>.txt` and the inline text becomes a
  preview plus the locator and a retrieval hint;
  `[limits].tool_result_inline_bytes` replaces the constant and is validated at
  load. `golden/tool_result_spilled.json` pins the preview and locator in both
  the session log and the assembled request, and the test reads the locator
  back and asserts it equals the tool's full output. Notes:
  - **Spills are scoped by run id, not by a conversation hash.** The roadmap
    said `<conversation-hash>`, but `RunState` carries no conversation id — it
    reaches the loop on the `Envelope`, not the state — so hashing one would
    have meant a breaking `agentos-interfaces` change for a directory name.
    The run id is on the state, is already the unit a trace is read by, and
    gives the same containment.
  - **Sanitization replaces the hash.** Every path segment is built by
    `safe_segment`: characters outside `[A-Za-z0-9_]` become `_`, capped at 64.
    `..`, `/`, and `\` cannot survive it, so a model- or channel-supplied id
    can only ever name a child of the root. This avoids adding a `sha2`
    dependency for what was a path-escape defence, not a collision defence.
    Files are created `O_EXCL | 0600` inside `0700` directories, so a planted
    symlink fails the write rather than redirecting it — asserted by
    `a_planted_symlink_is_rejected_not_followed`.
  - **Spilling is best effort and never fails a run.** No store, a full disk, a
    permission error: `spill_oversized` logs a warning and returns `None`, and
    the result degrades to the pre-C2 truncation notice. A storage fault must
    not turn a successful tool call into a failed run.
  - **Pruning is a view, not a rewrite** — the one design decision worth
    recording. `prompt::prune::to_fit` elides at assembly time on cloned
    messages; the session log keeps the full preview. This is the opposite
    reasoning from P2: compaction must be durable because it is expensive and
    non-deterministic, whereas elision is a pure function of the visible
    transcript and the window, so freezing it would throw bytes away that a
    later compaction could have afforded to show again.
  - **Elision only runs against a resolved context window.** No budget means no
    pruning — inventing one would silently discard output on models that had
    room for it. It triggers above `PRUNE_TRIGGER_RATIO` (0.8), keeps 2 KiB of
    head and 1 KiB of tail, walks oldest-first so the result the model just
    asked for is the last to lose its middle, and stops as soon as the request
    fits. Results under 5 KiB are left whole: the marker would replace a span
    barely larger than itself.
  - **The elision test is not a golden.** Pinning it would mean storing
    kilobytes of filler to assert one marker, so
    `elision_reaches_the_model_but_not_the_log` asserts the marker reaches the
    provider and does *not* reach the log. Both it and the spill golden were
    checked against a deliberately broken implementation before acceptance.
  - **`temp_tree` hashes its discriminator.** The first recording of
    `tool_result_spilled` was unstable: the golden redacts the temp path but
    still pins the `chars` count of the text that carried it, and a `ThreadId`
    rendering as one digit on one run and two on the next moved that count. The
    discriminator is now a fixed-width hash; the golden was verified stable
    across five consecutive runs.
  - **Nothing prunes spilled artifacts — deliberately deferred.** The config
    table listed `[spill].root` and `[spill].retention_days`; C2 implements
    neither. The root is fixed at `<workspace>/spill`, and artifacts accumulate
    for the life of the workspace. That is the right trade for now — losing a
    referenced artifact mid-run is worse than disk use — but a long-lived
    deployment needs retention, and it belongs with X3's other bounds rather
    than bolted onto this item.
  - **Benchmark impact:** `loop_overhead/tool_turn_allow` 3.00 µs → 3.25 µs
    (size check plus a tool-name `Arc` clone per tool turn), ~615× under the
    2 ms ceiling. Other benches within noise. `BENCHMARKS.md` updated.
  - Interface impact, machine-verified with `--baseline-rev HEAD`:
    `agentos-interfaces` and `agentos-llm` unchanged; `agentos-proto` reports
    one major (`constructible_struct_adds_field`) for `elided_messages` and
    `elided_chars` on `RequestHeader` — the same P3-introduced type C1 extended,
    with no consumers outside this workspace.

### C3. Span summarization (F2)

When pruning is not enough, summarize the oldest balanced span into a single
checkpoint item appended to the log. **Append-only** — nothing in the session is
mutated or deleted. The checkpoint carries the shadowed item ids in metadata, and
P2's projection folds them out of the model-visible list.

Span edges must keep tool-call/tool-result pairs together: a span may not end
between an assistant item carrying a `tool_call_id` and its result. This is the
one rule whose violation produces a hard provider 400 rather than a degraded
answer.

- Files: new `crates/agentos-core/src/prompt/compact.rs`; invoked from
  `loop/mod.rs::plan()` entry (one call, no new state); config in
  `config/mod.rs` via a new `config/compaction.rs`.
- Effort: L.
- Depends: C2, P2.
- Risk: the summarizer is itself an LLM call inside the planning path. It must
  respect the run's cancellation token (D1) and must not recurse — a compaction
  request never triggers compaction.
- Verify: `cargo test -p agentos-core prompt::compact`; a property test asserts
  every produced span is tool-pairing balanced; a golden replays a 200-turn
  session and asserts the request stays under budget while the full log is
  retained on disk; resume-after-compaction produces the same trace.
- Exit: a conversation can run indefinitely without exceeding the provider's
  context limit; the session log still contains every original item.

### C4. Context-overflow recovery (F2)

The one trigger that is never a guess: the provider itself rejecting the request
for length. Classify that error in the provider adapters, and on it force one
compaction pass and retry the step exactly once.

- Files: `crates/agentos-llm/src/providers/*.rs` (error classification beside the
  existing `insufficient_quota` handling); `loop/mod.rs::plan()` retry arm.
- Effort: S.
- Depends: C3.
- Verify: a stubbed provider returning a context-length error produces one
  compaction, one retry, and a successful turn; a second consecutive overflow
  finishes the run with a truncation notice rather than looping.
- Exit: a context-length rejection is recoverable without operator intervention.

---

## Phase 3 — Deadlines and control

### D1. Cancellation token (F3)

Thread a `CancellationToken` through `RunnerDeps` → `LoopDeps` → `RunContext` →
tool calls and LLM requests. Use `tokio_util::sync::CancellationToken` rather
than hand-rolling — it composes child tokens, which is exactly what sub-agent
delegation needs.

- Files: `Cargo.toml` (`tokio-util` workspace dep; `tokio` gains `time` and
  `process` features in `crates/agentos-core/Cargo.toml`);
  `crates/agentos-interfaces/src/orchestrator.rs` (`RunContext` field, mirroring
  `usage_sink`/`stream_sink`); `crates/agentos-interfaces/src/tool.rs`;
  `crates/agentos-llm/src/lib.rs`; `runner.rs`, `loop/mod.rs`,
  `subagents/mod.rs` (child token per delegation).
- Effort: M.
- Depends: nothing (parallel with Phase 2).
- Risk: adding a `RunContext` field is a breaking `agentos-interfaces` change.
  The repo already accepts this pattern twice (`usage_sink`, `stream_sink`) —
  note it in the PR.
- Verify: `cargo semver-checks check-release -p agentos-interfaces`; a test
  cancels a run mid-tool and asserts the run terminates with a cancellation
  outcome and the child process is reaped; cancelling a parent cancels its
  sub-agent.
- Exit: every blocking call in a run observes one token.

### D2. Tool deadlines and async subprocess execution (F3)

Give `ToolSpec` a `timeout_ms` read from `[resources.tools]`. Convert
`call_isolated_subprocess` and `ShellTool` to `tokio::process` under
`tokio::time::timeout`; wrap any remaining synchronous work in `spawn_blocking`.
Cap captured child output.

A timeout returns a **failed `ToolResult`**, never a `RunError` — the loop
already recovers from failed results (`loop/mod.rs:587`), and raising would kill
the parent run and, today, the gateway.

- Files: `crates/agentos-interfaces/src/tool.rs` (`ToolSpec`, `#[serde(default)]`);
  new `crates/agentos-core/src/tools/exec.rs` (async spawn + deadline, keeping
  `registry.rs` under its ceiling); `tools/builtin/shell.rs`; `tools/mcp.rs`
  (drop the hardcoded 10 s in favour of the same field); `runtime/tools_config.rs`.
- Effort: M.
- Depends: D1.
- Verify: `cargo test -p agentos-core tools::exec`; a tool sleeping past its
  deadline returns a failed result within the deadline and leaves no orphan
  process (`ps` assertion or PID reap check); the loop continues and the model
  sees the failure; no `std::process` call remains inside an `async fn`
  (`grep -rn "std::process::Command" crates/agentos-core/src`).
- Exit: no tool call can occupy a worker thread indefinitely.

### D3. Background job registry (F9)

An owner-fenced registry: a producer declares kind, label, output cap, and an
owning conversation, and returns cancel/done/read-output hooks. Model-facing
`job_start` / `job_status` / `job_output` / `job_kill` tools. A call that exceeds
its D2 deadline is **promoted** to a job rather than lost.

- Files: new `crates/agentos-core/src/jobs/mod.rs` and `jobs/registry.rs`; new
  `crates/agentos-core/src/tools/builtin/jobs.rs`; registration in
  `runtime/tools_config.rs`.
- Effort: L.
- Depends: D2.
- Risk: jobs outlive a run, so their handles cannot live in `RunState`. They
  belong to the conversation actor (G1) — build D3 after G1 if the ordering
  allows, or park handles in a runtime-owned registry keyed by conversation.
- Verify: `cargo test -p agentos-core jobs`; a job survives its originating run,
  reports output incrementally, is killable, and is cancelled when its owning
  conversation is disposed; a job owned by conversation A is not visible to B.
- Exit: long work is observable and cancellable instead of blocking or failing.

---

## Phase 4 — Gateway concurrency and approval correlation

### G1. Per-conversation actors (F5)

Replace the serial receive → run → send loop with a shard set: N OS threads, each
a `current_thread` runtime plus `LocalSet`, conversations routed by a stable hash
of `conversation_id`. Concurrent across conversations, strictly serialized within
one — which also removes the session write-back race that two overlapping runs on
one conversation would otherwise have.

Each conversation owns a bounded inbox with two lists (`next-turn`, `next-step`).
Maintenance work — cron scan, reflection — runs from a shard's idle phase instead
of competing with the receive loop.

This is not new architecture: it is exactly the shape
[`BENCHMARKS.md`](../BENCHMARKS.md)'s concurrency bench already models, promoted
from bench harness to product.

- Files: new `crates/agentos-core/src/gateway/shard.rs` and
  `gateway/inbox.rs`; `gateway/mod.rs`; `bin/agentos-gateway.rs:669`
  (the loop becomes a router).
- Effort: L.
- Depends: D1 (an actor without cancellation cannot be steered or stopped).
- Risk: the `!Send` constraint is load-bearing here. Do not attempt
  `tokio::spawn`; the shard owns its runtime. Shard count belongs in config.
- Verify: `cargo bench -p agentos-core --bench concurrency` (unchanged tail
  latencies); a test drives two conversations where A's tool sleeps and asserts
  B completes first; a second message on A during A's run lands in the inbox and
  is claimed at the next `Plan` rather than starting a second run.
- Exit: one slow conversation cannot stall another; the gateway has a `/stop`
  that cancels the active run.

### G2. Correlated approvals (F4)

Carry the `InterruptionId` into the approval prompt and require it in the answer:
inline-keyboard callback data on Telegram, card actions on Feishu, explicit
`/approve <id>` and `/deny <id>` in text-only channels. A message that does not
carry a matching id is ordinary input and queues on the inbox.

Model the outcome as a closed enum — approved, rejected, cancelled, unavailable —
and fail closed: an unparseable or expired answer resolves to a distinct outcome
that the audit trail can tell apart from a deliberate refusal. Add an expiry.

- Files: new `crates/agentos-core/src/loop/approval_route.rs` (matching and
  outcome vocabulary); `runner.rs::approval_prompt_envelope`;
  `channels/telegram.rs`, `channels/feishu/mod.rs` (callback payloads);
  `bin/agentos-gateway.rs:818-845`; `agentos-cli/src/slash.rs`.
- Effort: M.
- Depends: G1 (non-matching input needs an inbox to queue on).
- Verify: `cargo test -p agentos-core approve`; a golden for the pause/resume
  scenario asserts an unrelated message does not decide the pending approval; an
  expired prompt records `cancelled`, not `rejected`.
- Exit: no envelope can answer an approval it does not name; every decision has a
  correlated audit pair.

---

## Phase 5 — Correctness and hygiene

Independent items. Each is small enough to slot between the larger phases.

### X1. Parallel tool calls (F7)

`orchestrator/max.rs:250` takes `response.tool_calls.first()` and discards the
rest; the Anthropic and DeepSeek adapters parse the full vector, so on those
providers the model's other calls are paid for and dropped. Pick one and make it
uniform:

- **(a) Serialize explicitly.** Send the parallel-calls-off flag on every
  provider that has one and emit a trace event when a response still carries
  extra calls, so the loss is visible. Effort S.
- **(b) Batch.** Extend the plan to carry a call batch where each call crosses
  `Approve` individually and results append in order. The batch loop lives inside
  `Act` — no new state, no new transition. Effort M.

Recommend (a) now and (b) after Phase 4: batching is most valuable once tool
calls have deadlines (D2) and can run concurrently without risking the gateway.

- Files: `orchestrator/max.rs` call site, `providers/{anthropic,deepseek,ollama}.rs`;
  for (b) also `agentos-interfaces/src/orchestrator.rs` (`Plan`) and
  `loop/mod.rs::act`.
- Verify: a scripted response with three tool calls produces three results in
  order (b) or one result plus one trace event (a); no orphaned `tool_call_id`
  reaches a provider.
- Exit: extra tool calls are never silently dropped.

### X2. Real sandboxing (F8)

`requires_isolation` yields a subprocess as the same user with the same
filesystem, network, and environment. Replace the bool with a mode
(`read_only` / `workspace_write` / `full_access`) on `ToolSpec` and add a
`Sandbox` provider: Landlock on Linux (no extra process), Seatbelt on macOS.

Two smaller things to do first, either as part of this item or immediately:
`ShellTool`'s description claims "an allowlisted shell command"
(`tools/builtin/shell.rs:31`) — the allowlist lives in `[guardrails]` and is
enforced by a guardrail, so the tool description should say so rather than imply
its own enforcement; and `DESIGN.md`'s safety ring 4 should describe a process
boundary until this item lands.

- Files: new `crates/agentos-core/src/sandbox/mod.rs` (+ `linux.rs`, `macos.rs`);
  `agentos-interfaces/src/tool.rs`; `tools/exec.rs`; `DESIGN.md`.
- Effort: L. Depends: D2.
- Verify: a `read_only` tool cannot write to the workspace; the enforcement test
  is skipped with an explicit reason on unsupported platforms rather than passing
  vacuously.
- Exit: ring 4 in `DESIGN.md` describes what the code enforces.

### X3. One config pass (F15)

Move every deployment-varying constant into `agent.toml` with loader validation,
in one PR rather than four: tool-result inline cap (`loop/items.rs:10`), MCP
timeout (`tools/mcp.rs:80`), hydration budgets (`orchestrator/max.rs:24-25`), and
the directory listing limit. Fold in the new keys from C1–C3, D2, and G1.

- Files: new `crates/agentos-core/src/config/limits.rs` (keeps `config/mod.rs`
  under its ceiling); `runtime/mod.rs`; `workspace/agent.toml`.
- Effort: S. Depends: whichever of C1–C3, D2, G1 have landed.
- Verify: `cargo test -p agentos-core --test config_loader`; an out-of-range
  value fails loud at load; `agentos-gateway config` prints every new key.
- Exit: no `DEFAULT_*` constant governs a value an operator has reason to change.

### X4. Generated catalogs (F13)

Generate `docs/config-catalog.md` from the config structs and a tool catalog from
registered `ToolSpec`s, between edit markers, and verify freshness in CI beside
`check-import-boundaries.sh`. This roadmap's own opening — two documented
baselines that the tree contradicts — is the argument.

- Files: new `scripts/gen-catalogs.sh` (or a `--dump` subcommand on the existing
  `agentos-gateway config`); `.github/workflows/ci.yml`.
- Effort: M. Depends: X3.
- Verify: editing a config field without regenerating fails CI.
- Exit: config and tool surfaces are derived, not maintained by hand.

### X5. Runtime invariants (F14)

Debug-build assertions over relationships that already have prose rules: every
provider message derives from `RunState` (the F1 class); every delegation's
effective policy narrows its parent's; every tool result in the transcript
follows an assistant item carrying its call id (the C3 class). Assert
relationships over authoritative state, never that a type or method exists.

- Files: new `crates/agentos-core/src/invariants.rs`, called under
  `debug_assertions` from `loop/mod.rs` and `subagents/mod.rs`.
- Effort: S. Depends: P1.
- Verify: each invariant has a test that violates it and observes the panic.
- Exit: the three named classes are machine-checked in debug builds.

### X6. Session fork (F11)

`fork(source, boundary, child_id)` seeds a child conversation from a prefix of
the parent log. Nearly free on the SQLite store once P2's projection exists, and
it is the natural seeding primitive for a sub-agent that should start from the
parent's context at the delegation point rather than a summary of it.

- Files: `agentos-interfaces/src/session.rs` (defaulted trait method — additive);
  `memory/sqlite.rs`; `subagents/mod.rs`.
- Effort: S. Depends: P2.
- Verify: `cargo semver-checks check-release -p agentos-interfaces` reports
  additive-only; a forked child's projection equals the parent's prefix; spill
  locators in the seeded prefix resolve from the child without copying.
- Exit: a conversation can be branched.

### X7. Folded collaboration state (F10)

If todo/plan/goal state is added, add it as transcript items with a fold — never
as parallel mutable state. State that is a pure function of the log survives
resume, fork, and having its history compacted; a live mirror does not.

- Files: new `crates/agentos-core/src/tools/builtin/todo.rs`; fold in
  `prompt/projection.rs`.
- Effort: M. Depends: P2, C3.
- Verify: todo state after a resume equals the fold of the log; a compaction that
  shadows a todo write preserves the folded value.
- Exit: no collaboration state exists outside the session log.

---

## Sequencing

```text
T1 ──┬─> P1 ──┬─> P2 ──> C3 ──> C4
     │        ├─> P3 ──────┘
     │        ├─> C1 ──> C2 ──┘
     │        └─> X5
     └─> D1 ──> D2 ──┬─> D3
                     ├─> G1 ──> G2
                     └─> X2
P2 ──> X6, X7        X3 ──> X4
```

P3 lands before C3: compaction rewrites the history that any reconstruction
reads, so decide what is logged before deciding what may be shadowed.

Three tracks run in parallel after T1: the **request track** (P1, P2, C1–C4), the
**control track** (D1, D2, D3), and the **hygiene track** (X1, X3, X4, X5). They
converge at G1, which needs D1 for cancellation and benefits from C3 having
already bounded per-run cost.

Two ordering rules are load-bearing:

- **P1 before C3.** Compaction rewrites what the request reads. Establish one
  authority over the request before you start rewriting its inputs.
- **D1 before G1.** An actor you cannot cancel is a worse serial loop, not a
  better one.

If only one phase gets built: **T1 + P1**. That is the smallest change that turns
a silently inert subsystem into a machine-checked one, and it is the prerequisite
for everything else being verifiable.

## Config surface added

Land these together in X3 rather than piecemeal.

| Key | Item | Purpose |
|---|---|---|
| `[limits].tool_result_inline_bytes` | C2, X3 | Inline cap before spill. **Landed** in C2; default `64 KiB`, floor `512` |
| `[limits].directory_list_entries` | X3 | Directory listing cap |
| `[spill].root`, `[spill].retention_days` | C2 → X3 | Spill artifact storage. C2 fixed the root at `<workspace>/spill` and left retention unimplemented — nothing prunes old artifacts yet |
| `[compaction].enabled`, `.pressure_ratio`, `.retain_tail_turns`, `.model` | C3 | Compaction policy and summarizer route |
| `[resources.tools].timeout_ms` (per tool, with a default) | D2 | Tool deadlines; replaces the MCP 10 s constant |
| `[jobs].max_concurrent`, `.output_limit_bytes` | D3 | Job registry bounds |
| `[gateway].shards`, `.inbox_capacity` | G1 | Conversation sharding |
| `[approval].expiry_seconds` | G2 | Approval prompt expiry |
| `[memory].hydrate_*` | X3 | Already config; move the remaining constants beside them |

## Interface-change ledger

Run `cargo semver-checks check-release -p agentos-interfaces` on each and record
the result in the PR.

| Item | Change | Expected |
|---|---|---|
| P3 | `RunContext` gains `request_sink`; `agentos-proto` gains `RequestHeader` | **Verified**: interfaces major (`constructible_struct_adds_field`), proto additive |
| C1 | `RequestHeader`/`RequestSection` gain token fields; `Llm` gains a defaulted `context_budget_tokens()` | **Verified**: proto major (`constructible_struct_adds_field`), interfaces and llm unchanged |
| C2 | `RequestHeader` gains `elided_messages`/`elided_chars` | **Verified**: proto major (`constructible_struct_adds_field`), interfaces and llm unchanged |
| D1 | `RunContext` gains a cancellation field | Breaking; same pattern as `usage_sink`, `stream_sink`, `request_sink` |
| D2 | `ToolSpec` gains `timeout_ms` (`#[serde(default)]`) | Breaking on struct literals; additive on wire |
| X1 (b) | `Plan::CallTool` carries a batch | Breaking |
| X2 | `ToolSpec.requires_isolation` → sandbox mode | Breaking |
| X6 | `Session::fork` as a defaulted method | Additive |

`Approve`, `RunLoopState`, and the guardrail traits are unchanged by every item
in this roadmap.

## Status

Not started. Update each item with a **Status** line and its commit as it lands,
matching the convention in [`FEATURE_ROADMAP.md`](FEATURE_ROADMAP.md).
