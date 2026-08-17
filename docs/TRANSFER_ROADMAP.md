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
    caller, `LlmOrchestrator`, was unreferenced outside its own crate and has
    since been deleted; `Llm::complete(ctx)` and `Llm::complete_stream(ctx)`
    now have no caller at all and are candidates for removal from the trait.
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
- **Status: done** (2026-08-15; accuracy check run 2026-08-17). The exit
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
  - **The accuracy check has now been run** (2026-08-17), against
    `openai:gpt-5.4-mini`. `agentos-gateway calibrate` sends the fixed corpus in
    `prompt::calibration` through the ordinary `Llm::complete_messages` path and
    records the provider's own `input_tokens` per case; the result is
    `docs/token-calibration.md` and `tests/golden/token_calibration.json`.
    Measured error, estimate against provider count:

    | Class | Error |
    |---|---|
    | Chinese prose | **+32%** |
    | Tool schemas (JSON) | **+38%** |
    | English prose | **+22%** |
    | Symbol-dense ASCII (code) | **−19%** |
    | Realistic mixed requests | **+1% to +16%** |

    Two findings, and neither is what C1 predicted:

    - **The estimator is not safe by construction.** `prompt/tokens.rs` claimed
      it was "never dangerously low". It is: code tokenizes at ~3.2 characters
      per token, not 4, so a request made mostly of code reads 19% *under* the
      truth. The claim has been removed and replaced with the measurements.
    - **No single divisor fixes it.** Code is denser than 4:1 while JSON schemas
      are sparser, and both are "symbol-heavy ASCII" to any character-class
      rule. Fitting a more elaborate heuristic to one provider's tokenizer would
      buy accuracy on that provider and unknown behaviour elsewhere, so the
      error is carried by the threshold instead — see C3's status.

    Only 2 of 8 cases fall inside the ~15% target this item asked for, so on
    the letter of the Verify line the estimator fails it. On the cases that
    decide anything it passes: a request large enough to matter for pressure is
    a transcript, not one character class, and the three composite cases land at
    +1%, +7%, and +16%. The single-class figures are the ingredient bounds. The
    `minimal` case's −29% is the provider's fixed per-request framing (two
    tokens) and means nothing at scale.
  - **This is one tokenizer, and not the deployment's.** The .env in this
    checkout points at OpenAI; the machine described in `[channels]` runs
    DeepSeek, whose key is not here, so the CJK figure in particular is
    unverified for the traffic that motivated the two-rate design. **Re-run
    `agentos-gateway calibrate` on the deployment.** The offline half
    (`tests/token_calibration.rs`, `calibrate --check`) then re-scores the
    estimator against whatever that run recorded, spending nothing.
  - **The passive trace-pairing route is weaker than C1 assumed.** A request's
    estimate is on its `request_header` event and the provider's `input_tokens`
    on `llm_token_usage` under the same plan span, but the two sinks are drained
    separately (`loop/planning.rs`), so their interleaving is lost. A span whose
    plan made several calls — the routing classifier records usage with no
    header — cannot be paired at all. Usable only for spans holding exactly one
    of each, which is why the corpus route exists.
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
- **Status: done** (2026-08-16; trigger measured 2026-08-17). Both exit
  conditions are demonstrated by
  `tests/compaction.rs::a_long_conversation_stays_within_the_window_and_keeps_its_log`:
  200 turns against a 2 000-token window, 27 compactions, peak request 1 790
  tokens, and the session log holds all 400 originals plus 27 checkpoints.
  Notes:
  - **The threshold is now measured, and it moved: 90 → 84.** C3 shipped with a
    reasoned 90 because C1's accuracy check had not been run; it has now been
    run (see C1's status), and 90 does not survive it. The estimator's worst
    meaningful under-estimate is 18.8%, so a request estimated at 90% of the
    window is at 107% of it in truth — a provider rejection that C4 then has to
    recover from, on exactly the conversations compaction exists to protect.
    `100 / 1.188 = 84.2`, floored to **84**: the largest trigger at which a
    request that reads as under the window still is one. It stays above C2's 80%
    elision trigger, so the ladder is intact — the two constraints leave a
    four-point window and this is the top of it. The arithmetic is enforced by
    `tests/token_calibration.rs::the_default_pressure_threshold_leaves_room_for_the_measured_error`,
    which fails with the correct value named if either the estimator or the
    threshold moves. **This figure is derived from an OpenAI-family tokenizer;
    a deployment on another provider should re-run `agentos-gateway calibrate`,
    and that test will then tell it what its own threshold should be.**
  - **Compaction is on by default.** The failure it prevents is a hard provider
    400 that nothing recovers from until C4; the failure it risks is a
    degraded answer on a conversation that was already near its limit. Given
    that asymmetry, defaulting off would have left the exit condition unmet for
    every deployment that never reads this file. `enabled = false` restores the
    pre-C3 behaviour exactly, and C2's elision is unaffected either way.
  - **The trigger reads the larger of two pressure figures**, and this was a
    defect caught during implementation. Reading only the last `request_header`
    (the exact, C1-measured figure) looked right and passed its unit test, but
    the gateway starts a *fresh run per user message*, so the first — usually
    only — plan of each run has no header of its own and a chat would never
    have compacted at all. The transcript estimate is always available but low:
    it omits the tool schemas and skill prelude the orchestrator contributes.
    Taking `max` of the two never reads lower than either source says.
  - **A span always starts at 0.** Each pass summarizes from the beginning, so
    a second pass subsumes the first checkpoint along with the turns since,
    leaving exactly one summary in the projection rather than a chain of them.
    P2's monotonic shadowing is what makes re-covering an already-hidden range
    a no-op.
  - **The pairing rule is a property test**, not a spot check:
    `every_selected_span_is_tool_pairing_balanced` generates arbitrary
    histories — including calls whose results never arrive — and asserts no
    selected span ever ends with a call outstanding. This is the one rule whose
    violation is a hard 400.
  - **The span is rendered as text, not replayed as messages.** A message list
    carrying tool calls must satisfy every provider's pairing rules or the
    compaction request itself 400s, which is precisely the failure compaction
    exists to prevent. Oversized tool results are elided through C2's pruner
    first, and the span is cut to fit half the window, so the summarization
    call cannot itself overflow.
  - **Recursion is impossible structurally, not by a flag.** `compact` calls
    `Llm::complete_messages` directly, never the orchestrator, so it assembles
    no prompt, pushes no request header, and records no pressure.
  - **A summarizer failure is not a run failure.** An error or an empty summary
    logs a warning and leaves the turn uncompacted, covered by
    `a_failing_summarizer_leaves_the_run_uncompacted_rather_than_broken`.
  - **The 200-turn replay is not a golden**, contrary to the Verify line above.
    Pinning it would put a megabyte of filler in `tests/golden/`, and the
    property worth asserting — every request fits — holds over all 200 requests
    rather than the exact bytes of one. The test also asserts the peak request
    actually approached the window, so it cannot go vacuous if the fixture
    shrinks.
  - **`[compaction].model` is not implemented.** Summaries are written by the
    run's own high-tier provider. Routing a separate, cheaper model would let a
    weaker summary lower the ceiling on every later turn, and the wiring
    belongs with X3's other config work. *Landed in X3, as a model tier.*
  - **The D1 cancellation requirement is not met** — D1 does not exist yet, so
    there is no token to respect. A compaction call is one non-streaming
    round-trip at the start of a plan; when D1 lands it must be threaded
    through here.
  - **Cost on the hot path:** with a summarizer configured, every plan entry
    estimates the visible transcript. That is the same scan prompt assembly
    already performs, so it doubles an existing cost rather than adding a new
    order of magnitude. Benchmarks are within noise
    (`tool_turn_allow` 3.10 µs, `reply_turn` 1.20 µs); with no summarizer the
    check short-circuits before touching the transcript.
  - Interface impact, machine-verified with `--baseline-rev HEAD`:
    `agentos-interfaces`, `agentos-proto`, and `agentos-llm` all report **no
    semver update required**. C3 is contained entirely in `agentos-core`.

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
- **Status: done** (2026-08-16). Both Verify cases hold end to end in
  `tests/compaction.rs`: a stubbed provider returning a length rejection
  produces one forced compaction, one retry, and a finished turn; a provider
  that rejects every attempt finishes with a truncation notice and exactly two
  attempts. Notes:
  - **The class stays typed the whole way.** `LlmError::ContextLengthExceeded`
    and `OrchestratorError::ContextLengthExceeded` are new variants, and
    `ProviderError::ContextLength` is split out of `Api`. The loop branches on
    a type, never on a substring of a provider's prose — which is what this
    project's typed-error convention is for. `orchestrator::planning_error` is
    the single shared conversion, because collapsing the class into `Backend`
    is an omission the compiler cannot catch.
  - **Classification is prose matching, and that is unavoidable.** Only OpenAI
    and its compatibles set `code = "context_length_exceeded"`; Anthropic and
    self-hosted runtimes say it in the message and nowhere else. The phrase
    list is deliberately narrow and each entry is a phrase its provider emits
    verbatim, because a false positive costs a pointless summarization call and
    a retry of a request that was never too long.
    `unrelated_failures_stay_unrecoverable` pins quota, rate-limit, auth, and
    `max_tokens` errors as *not* recoverable.
  - **The routing classifier is deliberately excluded.** Its prompt is two
    fixed messages carrying none of the conversation, so compaction could not
    shorten it; it keeps mapping to `Backend`. Marking it recoverable would
    spend a summarizer call and a retry on a request that fails identically.
  - **Giving up returns a `Plan::Reply`, not an error or a `Finish`.** The
    truncation notice therefore goes through the loop's normal reply handling,
    so the output guardrails still run on it — the alternative,
    `budget_exhausted_finish`'s shape, had to re-implement that check inline.
    The notice is fixed text with no model call: asking the provider that just
    rejected the request to write the apology would fail the same way.
  - **`compact_now` respects `[compaction].enabled`.** An operator who turned
    summarization off did not ask for it back on a bad turn; such a deployment
    answers with the notice instead. It does bypass the *pressure* check, which
    is the whole point — the provider's rejection is the one trigger that is
    never an estimate.
  - **No retry when there is no span.** A conversation shorter than the
    retained tail has nothing to summarize, so the loop answers immediately
    rather than re-sending an identical request.
  - **`plan()` shrank rather than grew.** The roadmap's constraint forbids
    extending `loop/mod.rs`, so the attempt, both compaction triggers, and the
    give-up path moved into a new `loop/planning.rs` (509 LOC) and C3's inline
    compaction block moved with them. `loop/mod.rs` is 725 LOC, under the
    ceiling.
  - **Incidental fix:** a hydration failure now closes its `orchestrator.plan`
    span and drains the request sink before propagating. Previously it used
    `?` and left the span open, so a failed hydrate produced a trace with no
    `hydrate_finished` event.
  - Interface impact, machine-verified with `--baseline-rev HEAD`:
    `agentos-interfaces` major (`enum_variant_added` on `OrchestratorError`),
    `agentos-llm` major (same, on `LlmError` and `ProviderError`),
    `agentos-proto` unchanged. Both enums are matched exhaustively inside this
    workspace only.

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
- **Status: done, except the subprocess half of the Verify** (2026-08-16). One
  `tokio_util::sync::CancellationToken` per run, carried on `RunnerDeps` →
  `LoopDeps` → every `RunContext` the loop builds, with a child token per
  delegation. `tests/cancellation.rs` covers the four cases end to end. Notes:
  - **Cancellation finishes the run; it does not fail it.** The same choice
    `max_turns` and C4 already make, but it matters more here: the runner only
    appends a run's new transcript items to the session on the success path, so
    returning `Err` would have discarded the tool results the run had already
    produced — exactly the work a user who pressed stop most wants kept.
    `cancelling_mid_tool_stops_the_run_and_keeps_its_work` asserts the earlier
    tool output survives in the session.
  - **The race is `biased`.** Without it `select!` polls a ready branch at
    random, so an already-cancelled run could still dispatch one more tool call.
    `an_already_cancelled_token_never_starts_the_work` pins it.
  - **Sub-agents get a child token, not the parent's.** Cancelling a parent
    stops the whole delegation tree; a sub-agent stopping itself leaves the
    parent free to use what it got. Both directions are asserted.
  - **C3's outstanding promise is now kept.** The summarizer call is an LLM
    round-trip inside the planning path, and it is raced against the token, so
    a run cancelled mid-summarization drops it rather than paying for a summary
    nobody will read.
  - **The `Tool` trait did not need to change**, contrary to the Files list
    above. `call_with_context` already hands tools a `RunContext`, so putting
    the token there gives every tool a path to observe it without breaking a
    single implementation. `RunContext::is_cancelled()` is the cooperative
    check for tools that do several units of work per call.
  - **`RunError::Cancelled` never reaches a caller.** It exists only to carry
    cancellation up from the tool path, which is several frames deep; `act`
    converts it into the terminal stop notice.
  - **The subprocess half of the Verify is NOT met.** "the child process is
    reaped" cannot hold yet: `ShellTool` and the isolation worker both block a
    Tokio worker on `std::process`, so `select!` never gets to poll the
    cancellation branch until they return. Cancellation is real for anything
    that *awaits* — an HTTP request, a `tokio::process` child — and inert for
    anything that blocks. That is precisely what **D2** (async subprocess
    execution) exists to fix, and D2 now has a token to hang off. The test
    suite uses an awaiting tool for this reason, and `loop/cancel.rs` documents
    the limitation where someone debugging it would look.
  - **Benchmark impact is real and was not optimised away:** `reply_turn`
    1.19 → 1.46 µs (+23%), `tool_turn_allow` 3.25 → 3.93 µs (+21%),
    `ask_user_pause_resume` 7.71 → 8.46 µs. `paused_state_json_round_trip` is
    flat at 3.54 µs, which is the control — it touches no loop state, so its
    flatness confirms the others moved for a real reason. The cost is ~270 ns
    per raced await, from `CancellationToken::cancelled()` registering a waker.
    A cancellation that only takes effect *between* awaits cannot stop an LLM
    call thirty seconds into a response, which is the case users actually want
    stopped, so the race stays. `BENCHMARKS.md` records the figures and the
    reasoning.
  - Interface impact, machine-verified with `--baseline-rev HEAD`:
    `agentos-interfaces` major (`constructible_struct_adds_field` for
    `RunContext.cancel`) — the same pattern as `usage_sink`, `stream_sink`, and
    `request_sink`, exactly as the Risk line predicted. `agentos-proto` and
    `agentos-llm` unchanged.

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
- **Status: done, with one documented exception to the Verify grep**
  (2026-08-16). Every tool call now runs under a resolved deadline, and a
  deadline that expires produces a failed `ToolResult` the model reads and
  replans around. Notes:
  - **Deadline resolution is most-specific-wins**, and the tool gets a say.
    `[limits].tool_timeout_overrides.<tool>` beats `ToolSpec::timeout_ms` beats
    `[limits].tool_timeout_ms` (default 60 s). The middle term is the one the
    roadmap did not spell out and is worth having: `ShellTool` declares five
    minutes because a build or a test suite legitimately takes that, and an
    unconfigured deployment should not hold it to a budget that suits an HTTP
    call. An operator who disagrees still has the last word.
  - **The deadline is enforced in the registry, not per tool**, so it covers
    MCP tools and any third-party `Tool` impl without their cooperation.
    `ShellTool` keeps its own inner deadline as a belt to that brace, so using
    it directly still cannot hang a caller.
  - **Capping output is not the same as not reading it** — this was a real bug
    caught by its own test. The first `exec::capped` stopped reading at the cap;
    the child's pipe then filled, it blocked in `write`, and it never exited, so
    a chatty-but-healthy tool came back as a *deadline failure* instead of a
    truncated success. It now keeps the first `max_output_bytes` and drains the
    rest to EOF.
  - **Reaping is `kill_on_drop` and nothing else.** Both ways a child outlives
    its usefulness end in a dropped future — the deadline drops it, and D1's
    cancellation drops the whole call — so there is no path where a caller must
    remember to clean up. Disabling `kill_on_drop` turns exactly one test red.
  - **The orphan test checks a pid, not `ps` output.** The first version grepped
    `ps -eo args` for a nonce, which matched *itself*: the checking pipeline
    necessarily contains the pattern it looks for. It now has the child record
    its pid and polls `kill -0`.
  - **D1's limitation is lifted.** `loop/cancel.rs` documented that cancellation
    was inert against `ShellTool` and the isolation worker because they blocked
    the thread. Both are `tokio::process` now, so a cancelled run really does
    drop the future and kill the child.
  - **The Verify grep does not come back empty, deliberately.**
    `tools/mcp.rs` still holds `std::process::{Child, Command, Stdio}` for the
    *persistent* stdio MCP worker. What remains there is a `spawn` — a one-off
    syscall that returns as soon as the child is forked — and the blocking
    request/response loop runs on a dedicated OS thread, never on a Tokio
    worker. The per-call wait that *did* block a Tokio worker (a
    `recv_timeout` of up to 10 s) is now behind `spawn_blocking`, and its
    hardcoded 10 s is now `[limits].tool_timeout_ms`. Rebuilding the whole
    persistent worker on `tokio::process` is a change with its own risk and no
    benefit to the exit condition, so it was not attempted.
  - **`test_support.rs` was split** because these changes pushed it to 801 LOC,
    one over the ceiling. `MockMemory` and `MockSession` moved to
    `test_support/storage.rs` with their tests — they are the two mocks that
    are real stores rather than canned responses, so they were the natural cut.
  - **Benchmark impact is large and is codegen, not work:** `reply_turn`
    1.46 → 2.48 µs, `tool_turn_allow` 3.93 → 6.50 µs,
    `ask_user_pause_resume` 8.46 → 11.0 µs, with the JSON round-trip control
    flat at 3.57 µs. A reply turn calls no tool and touches neither
    `ToolRegistry` nor `ToolSpec`, and a tree with `exec.rs` compiled but the
    call sites reverted measures 1.47 µs — so the cost appears when the
    `tokio::time`/`tokio::process` machinery lands next to the loop, not from
    anything the loop now does. `BENCHMARKS.md` records all four measurements.
    Still ~800× under the 2 ms ceiling, and the alternative is a runtime with
    no tool deadlines at all.
  - Interface impact, machine-verified with `--baseline-rev HEAD`:
    `agentos-interfaces` major (`constructible_struct_adds_field` for
    `ToolSpec.timeout_ms`), `agentos-proto` and `agentos-llm` unchanged. The
    field is `#[serde(default)]`, so the wire form is additive.

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
- **Status: done, with `job_start` deliberately omitted** (2026-08-16). An
  owner-fenced registry, three model-facing tools, and promotion from D2's
  deadline. `tests/jobs.rs` covers the exit condition end to end: a tool that
  outruns its deadline keeps running after the run returns, and the model is
  told where it went. Notes:
  - **Promotion starts the job first and waits inline, rather than racing a
    call and promoting on expiry.** Racing cannot work: the loser of a
    `timeout` is *dropped*, so there would be nothing left to promote and the
    work would have to be restarted — for a tool with side effects, done twice.
    Starting as a job makes the deadline a question of how long to wait inline,
    and a call that finishes in time is indistinguishable from one that never
    involved a job (`a_promotable_tool_that_finishes_in_time_looks_like_an_ordinary_call`).
  - **Promotion is an operator allowlist** (`[jobs].promotable`, default
    `["shell"]`), not automatic. A promoted call is re-issued through
    `Tool::call` rather than `call_with_context`, because the job outlives the
    borrow the context is built from. Only `MemoryTool` uses that context
    today, but silently dropping caller identity for a *third-party* tool that
    authorises on it would be a security bug rather than a degraded result, so
    the substitution is named rather than inferred.
  - **There is no `job_start`, and the reason is an invariant.** A `job_start`
    that runs another tool would dispatch that inner call from inside the tool
    layer — below the point where the loop applies tool guardrails and the
    approval engine — so `job_start {tool: "shell", …}` would be a way to run a
    shell command the shell guardrail never sees. That is the same shortcut
    `ARCHITECTURE.md` forbids for MCP-originated calls, which must re-enter the
    loop at `Approve`. Starting a job safely means re-entering the loop, which
    is a planning concern (a new `Plan` variant) rather than a tool one.
    Promotion delivers the same capability with none of the risk: the call has
    already passed guardrails and approval as an ordinary tool call. **If a
    future item wants an explicit `job_start`, it belongs with G1 or as a
    `Plan` variant, not here.**
  - **Fencing does not distinguish "missing" from "not yours."** Both are
    `JobError::Unknown`, because saying which one it is tells a caller that
    another conversation's job exists. The conversation is resolved through the
    *same* helper the memory tool uses — `conversation_id_from_context`, moved
    to one definition — since two copies of a security boundary are two chances
    to drift.
  - **A job cancelled before its first poll never runs at all.** `start` queues
    work on the executor rather than running it, so killing a job in the turn
    that started it means the work never executed a line. Pinned by
    `a_job_cancelled_before_its_first_poll_never_runs`: it is the difference
    between "stopped early" and "never happened", which a producer with side
    effects needs to know.
  - **Work may be dropped at any await point.** Cancellation races the future
    and drops it, because that is the only way to stop arbitrary async work.
    Code after an await in a job is not guaranteed to run; anything that must
    happen on the way out belongs in a `Drop` impl, which is how `tools::exec`
    reaps its child. The first version of the disposal test asserted cleanup
    *after* `cancelled().await` and failed for exactly this reason.
  - **A full job table degrades to D2 rather than refusing.** Out of slots, the
    call runs inline under its deadline. A busy conversation gets the previous
    behaviour instead of losing the tool.
  - **The concurrency cap is per conversation**, not per process: the failure
    it prevents is one model spawning work in a loop, and a global cap would
    let that starve every *other* conversation instead of only its own.
  - **`dispose_conversation` exists, is tested, and has no caller.** The
    roadmap's Risk line is real — jobs belong to the conversation actor, and G1
    does not exist — so the registry is runtime-owned and keyed by
    conversation, which is the fallback the item sanctions. **G1 must call
    `AgentRuntime::jobs().dispose_conversation()` when a conversation ends**, or
    a long-lived gateway leaks cancelled-but-unreaped job entries.
    *Resolved by G1:* the sharded gateway calls it on `/clear`.
  - **`runtime/mod.rs` was split**: the job registry pushed it to 813 LOC, so
    MCP registration moved to `runtime/mcp_config.rs` — the most self-contained
    thing in the file, reading one config section and producing tool specs.
  - Interface impact, machine-verified with `--baseline-rev HEAD`:
    `agentos-interfaces`, `agentos-proto`, and `agentos-llm` all report **no
    semver update required**. D3 is contained entirely in `agentos-core`.
    Benchmarks within noise (`reply_turn` 2.42 µs, `tool_turn_allow` 6.17 µs).

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

**Status: done.** `gateway/inbox.rs` (per-conversation mailbox, `next-turn` and
`next-step`), `gateway/shard.rs` (stable-hash routing, one `current_thread`
runtime per shard thread, an idle phase for maintenance), `loop/steering.rs`
(mid-run input, claimed at `Plan`), `config/gateway.rs` (`[gateway].shards`,
`.inbox_capacity`), and the gateway binary split into
`bin/agentos-gateway/{main.rs,shard.rs}` — the router and what the shards run.
Verified: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --
-D warnings`, 456 tests, import boundaries, module sizes. Both halves proven
load-bearing: removing the `Plan`-time steering claim turns
`mid_run_input_is_claimed_at_the_next_plan` red, and running a shard's turns
serially instead of interleaved turns
`one_slow_conversation_does_not_stall_another` plus both shard scheduling tests
red.

  - **`Channel` gained a required `egress()` and this is the enabling change.**
    `Channel::receive` takes `&mut self`, so whoever receives holds the channel
    exclusively — fine for a serial receive-run-send loop, fatal for a sharded
    one where turns run on other threads while the router is parked in a
    40-second long poll. Draining replies between polls would have made a reply
    wait up to a poll cycle. So the send half is now a shareable `Arc<dyn
    Egress>`, exactly as `stream_egress` already was for streaming deltas, and
    for the same stated reason. Telegram and Feishu already had a struct holding
    precisely what `send` needs; their `Channel::send` bodies moved onto it and
    `send` became a provided method that delegates. **This is a breaking change
    to `agentos-interfaces`**, machine-confirmed by `cargo semver-checks
    --baseline-rev HEAD`: one major failure, `Channel::egress` added without a
    default. `agentos-proto` and `agentos-llm` report no update required.
  - **Turns are polled on a `FuturesUnordered`, not `spawn_local`.** A spawned
    task must be `'static`, which would force every shard to build its own
    `AgentRuntime` — separate session stores, separate memory, separate job
    registries for what is one agent. Polling borrowed futures in place lets all
    shards share one `Arc<AgentRuntime>` while each `!Send` run stays pinned to
    the thread that started it. This is a departure from the concurrency bench's
    `spawn_local` shape, and the reason is the borrow, not the scheduling.
  - **Routing is a stable hash, never round-robin.** A conversation that could
    migrate between shards could have two runs overlap on two threads, which is
    the one thing sharding must not allow. Serialization itself is enforced in
    `Inbox::claim`, which returns `None` while a run is in flight, rather than
    trusted to the shard loop's control flow.
  - **The two inbox lists shed load from opposite ends.** A full steer queue
    drops its *oldest* entry (the newest instruction is the one the user means);
    a full turn queue refuses the *newest* (a queued run is work already
    promised, and dropping it for a later message would lose the request
    silently). Pinned by `the_two_lists_shed_load_from_opposite_ends`.
  - **Steering is folded in before compaction and hydration**, so the new input
    is part of what gets summarized and recalled against rather than arriving
    after the context for the turn was already assembled. Claimed messages
    become ordinary user items, so they persist with the rest of the run and the
    next run sees them.
  - **An approval no longer freezes the gateway.** The serial loop answered an
    approval by blocking on `channel.receive()` — one user's unanswered prompt
    stopped every conversation. The paused run is now parked per conversation on
    its shard and the next message on that conversation decides it. Correlating
    an answer to a specific prompt is G2's job; until then "the next message
    decides" is the same rule as before, minus the freeze. *Resolved by G2: only
    an answer carrying the prompt's ticket decides it, and an unanswered prompt
    expires.*
  - **Cron and reflection run from shard 0's idle phase, not through the
    router.** Firing them on every shard would fire every task N times, so
    `TurnHandler::idle` carries the shard index and the handler guards on it.
    Cron runs go straight through the runner rather than the inbox, which is
    safe *because* a cron envelope carries `session_scope = ephemeral`: it
    neither loads nor writes back the conversation transcript, so it cannot race
    a user's run for the state sharding exists to protect. A cron that ever
    stops being ephemeral must be routed instead.
  - **The gateway binary was split** into `bin/agentos-gateway/main.rs` (1187
    LOC, allowlisted) and `bin/agentos-gateway/shard.rs` (361). Adding ~350
    lines of new behaviour to a file already at 1320 is exactly what the module
    rule forbids; the router half is now smaller than before this item.
  - **Not done, deliberately:** the router still parses only `/stop` itself.
    Every other slash command is delivered to the shard so it runs in
    conversation order — `/clear` mutates the session, and answering it on the
    router thread could race the run it is clearing.

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

**Status: done.** `loop/approval_route.rs` (tickets, matching, the closed
outcome enum), `config/approval.rs` (`[approval].expiry_seconds`),
`ApprovalStatus::Unanswered` and `ResumeDecision::{Cancel, Unavailable}`,
`runner.rs::approval_prompt_envelope` rebuilt around the ticket, Telegram inline
keyboards and `callback_query` handling, `/approve` and `/deny` on every
channel, and expiry sweeping in the gateway shard. Verified: `cargo fmt --all
--check`, `cargo clippy --workspace --all-targets -- -D warnings`, 482 tests,
import boundaries, module sizes. Both halves proven load-bearing: restoring the
`y`-approves fallback turns `an_unrelated_message_does_not_decide_a_pending_approval`,
the pause/resume golden, and two unit tests red; recording an expired prompt as
a rejection turns `an_expired_prompt_records_cancelled_not_rejected` and
`a_run_with_nobody_to_ask_records_unavailable` red.

  - **The correlation token is a per-prompt ticket, not the `InterruptionId`.**
    The item says to carry the `InterruptionId` and require it back, and that is
    not sufficient on its own: the id is *derived from the action*
    (`approval-<tool call id>`), so a model retrying the same call, or a user
    ignoring the first ask, produces two prompts with the same name — and a
    stale button carrying it would decide the later one. `ApprovalTicket` is
    minted per prompt and pinned by
    `a_stale_ticket_does_not_decide_the_current_prompt`, which asserts that two
    prompts share an `InterruptionId` and differ in ticket. The ticket is also
    short by design: Telegram caps `callback_data` at 64 bytes, which the
    action-derived id can exceed. The `InterruptionId` still travels on the
    prompt as `approval_id` — one says which asking, the other says what it
    authorises, and that pair is the correlated audit record the exit condition
    asks for.
  - **`y` no longer approves anything, anywhere.** That is the hole the item
    exists to close, and it is a deliberate break in behaviour: previously *any*
    next message resumed a paused run, so "yes, go ahead" about something else
    entirely authorised a tool call. The CLI TUI was changed too rather than
    left on the old rule — two authorization models is one more than is worth
    reasoning about — so it now loops on input until something names the ticket.
  - **Four outcomes, and the split that matters is not approve/deny.** It is
    `Rejected` against `Cancelled`/`Unavailable`: a refusal is a decision
    somebody made, and the other two are the absence of one. They fail closed
    identically and are recorded differently — `ApprovalStatus::Unanswered`,
    `RunError::ApprovalUnanswered`, and `EpisodeOutcome::Failed` rather than
    `Denied`. Recalling an expired prompt as a refusal would teach the agent
    that this user says no to things they never saw.
  - **`Unavailable` has a real caller.** A cron tick that hits an `ask_user`
    policy has nobody behind it, so its prompt can never be answered. It used to
    be logged and left parked; it is now resolved as `Unavailable`, which ends
    the run and reclaims it.
  - **An unattributable answer does not resolve the prompt.** A stale ticket is
    reported to the sender and the prompt stays pending until it expires or is
    answered properly. Reading "unparseable answers resolve to a distinct
    outcome" as *cancel the prompt* would hand anyone who can send a malformed
    payload a way to cancel someone else's approval.
  - **Expiry is swept on every shard, not just shard 0.** Pending prompts are
    per-shard state (each shard holds its own conversations'), unlike cron,
    which is process-wide and guarded. Expiry is also re-checked when a message
    arrives for that conversation, because a busy shard may not reach its idle
    phase.
  - **Not done: Feishu card actions.** Telegram gets inline keyboards and
    `callback_query` handling, unit-tested over recorded update shapes. Feishu
    does not: its long connection filters to `type == "event"` frames, and card
    actions arrive as `type == "card"` with a response frame whose shape I
    cannot verify without a live tenant. Shipping buttons that silently do
    nothing is worse than shipping none, so Feishu gets the same correlated
    text path — the prompt spells out `/approve <ticket>`, which fails closed
    identically. **This is the one part of the item's file list not delivered.**

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

**Status: done — (b), batching.** The roadmap recommended (a) now and (b) after
Phase 4; Phase 4 has landed, so (b) is what shipped and (a) is superseded rather
than deferred. New `loop/batch.rs` (queue and drain), `Plan::CallTools`,
`RunState::queued_tool_calls`, `orchestrator::plan_from_response` shared by both
orchestrators, and the OpenAI Responses adapter no longer forcing
`parallel_tool_calls: false`. Verified: `cargo fmt --all --check`, `cargo clippy
--workspace --all-targets -- -D warnings`, 492 tests, import boundaries, module
sizes. Both halves proven load-bearing: taking `first()` in
`plan_from_response` again turns `several_calls_are_all_kept_in_order` and the
`tool_call_batch` golden red; refusing to drain the queue turns four of the six
batch tests red.

  - **The batch loop lives in `Plan`, not `Act`.** The item's sketch put it in
    `Act`, and that cannot preserve per-call approval: `Act` runs what `Approve`
    already decided, so a batch executed there would have crossed the policy
    engine once, as a unit. Approving "these five calls" is not a decision a
    user was shown enough to make. Instead the batch is queued on the run state
    and drained one call per turn, each going back through `Plan → Approve →
    Act → Observe`. No new state, no new transition — as the item asked — and
    the loop never carries more than one call past `Plan`.
  - **The queue lives in `RunState`, not the loop's frame**, because a batch can
    pause for approval halfway through and what is left has to survive being
    written to disk. Pinned by
    `a_batch_pausing_for_approval_keeps_the_rest_queued`, which round-trips the
    paused state through JSON.
  - **A batch is replayed to the provider as paired single-call turns.** The
    transcript records one assistant turn per call, each followed by its own
    result, rather than one assistant turn with three calls and three results.
    That is not cosmetic: every provider rejects a tool result whose call has no
    preceding assistant turn, and it means no provider is ever shown a
    half-answered batch while the loop works through one. The `tool_call_batch`
    golden pins the assembled requests, and shows the other half of the win —
    three tool calls cost **two** LLM round trips, not four.
  - **Draining costs turns.** A five-call batch spends five of `max_turns`, so a
    model emitting a hundred calls hits the same budget as one that loops a
    hundred times. It costs no extra LLM round trips: the model already said
    what it wanted.
  - **Execution stays sequential, deliberately.** Concurrency is the larger
    prize and a separate decision. Calls in one batch routinely touch the same
    files, and interleaving two `file` writes because a model emitted them
    together would be a bug introduced by an optimisation nobody asked for.
    Sequential is also what makes "results append in order" free rather than a
    reordering step. The exit condition — no call silently dropped — does not
    require concurrency.
  - **The policy engine decides a batch by its strictest member**, and `Act`
    refuses one outright. Both are unreachable today because `Plan` splits every
    batch first — but "unreachable" is not a security property, and a future
    caller that routed a batch past the split must not get a weaker decision
    than its most dangerous call would have received.
  - **`parallel_tool_calls: false` is gone from the OpenAI Responses adapter.**
    It was the only place forcing serialization, which is exactly the
    non-uniformity the item complains about. Safe because that adapter already
    parsed every `function_call` item, and because its stateful
    `previous_response_id` resume never anchors on a tool-calling turn:
    `assistant_tool_call_item` builds its message with empty metadata, so the
    session pointer is not carried and every tool turn replays the full
    stateless prefix.
  - **Two files were split to stay under the ceiling**: tool execution moved
    from `loop/mod.rs` to `loop/tool_call.rs`, and the golden suite's context
    scenarios (memory hydration, spill/elision) from `tests/transcripts.rs` to
    `tests/transcripts_context.rs`, with the shared tool fixtures moving to
    `tests/support/fixtures.rs`.

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

**Status: done.** New `sandbox/{mod,linux,macos}.rs`, `SandboxMode` replacing
`ToolSpec::requires_isolation`, the sandbox applied in `tools/exec.rs` — the
single choke point every subprocess the runtime starts goes through — and ring
4 in `DESIGN.md` rewritten to describe enforcement rather than a process
boundary. Both prerequisites done: the `shell` description now credits
`[guardrails].shell_allowlist` for the allowlist instead of implying its own,
and ring 4 no longer overstates what isolation buys. Verified: `cargo fmt --all
--check`, `cargo clippy --workspace --all-targets -- -D warnings`, 512 tests,
import boundaries, module sizes. **The enforcement suite really ran here** —
this kernel reports Landlock ABI 6 — and is proven load-bearing: making
`Sandbox::harden` a no-op turns six of its nine tests red.

  - **Landlock is applied in `pre_exec`, between fork and exec.**
    `landlock_restrict_self` restricts the calling thread irreversibly and every
    descendant inherits it, so applying it anywhere in the agent would sandbox
    the agent for the life of the process. The closure it runs is subject to the
    usual post-fork rules — no allocation, no locks — so the directory
    descriptors and rule structs are built in the parent and only syscalls
    happen in the child.
  - **Every enforcement test carries a control.** Each asserts first that the
    write succeeds *unsandboxed*, then that it fails sandboxed. Without that,
    `a_read_only_child_cannot_write_outside_it_either` passes on any machine
    where the target was unwritable anyway — and the first version of the
    `workspace_write` test did exactly that, "proving" the sandbox by trying to
    write to `/` as a non-root user. The item warns about vacuous passes and
    that is the shape they take.
  - **`full_access` is the default, and it is a claim about enforcement rather
    than a grant.** It is exactly what `requires_isolation: false` meant. Most
    built-in tools do their work in-process, where the only sandbox available
    would restrict the whole agent — so they say `full_access` and are bounded
    by rings 2 and 3. Declaring `read_only` on a tool that never spawns a child
    would be a claim the kernel is not making, and `DESIGN.md` now says which
    ring covers which.
  - **A sandbox that cannot be built fails the call.** Not a warning: the
    alternative to a failed sandbox is running the tool with everything it asked
    not to have. `agentos-gateway config` prints
    `sandbox.enforcement=landlock|seatbelt|<reason>` so an operator learns this
    at startup rather than from a failed tool call.
  - **The macOS backend is compiled and unit-tested on Linux.** It is string
    building and one `Path::exists`, so gating it on `target_os` would have left
    the profile builder and its quote escaping unchecked on the machine that
    runs CI. The escaping test matters: a workspace path containing a quote
    would otherwise close the profile literal and let the rest of the path be
    read as profile syntax. What genuinely cannot be checked off-macOS is
    whether Seatbelt honours the profile, and that test skips with a reason.
  - **`/dev/null` stays writable in every sandboxed mode.** `2>/dev/null` is
    ordinary shell, and a sandbox that refuses it reads as the tool being broken
    rather than as the sandbox working. Writing to the null device changes
    nothing.
  - **`workspace_write` grants the temp directory too.** Compilers, package
    managers and anything using `mkstemp` write there; refusing it would make
    the mode mean "cannot run a build" and push operators to `full_access`,
    which is worse than granting a directory the machine already treats as
    scratch.
  - **Landlock rights are masked to the kernel's ABI.** `create_ruleset` rejects
    a ruleset naming a right the kernel does not know (`REFER` is ABI 2,
    `TRUNCATE` ABI 3), so asking for everything would fail closed on an older
    kernel — a sandboxed tool that stops running rather than a tool that runs
    unsandboxed, but still a regression against no reason.
  - **Not done: network and read restrictions.** The mode bounds filesystem
    writes only. Landlock gained network scoping in ABI 4 and Seatbelt can deny
    sockets, but the two do not line up, and a tool that must not read a
    particular path is a policy question rather than a sandbox one. Ring 4 in
    `DESIGN.md` states both limits rather than leaving them to be discovered.

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

**Status: done.** New `config/spill.rs` (`[spill].root`, `.retention_days` —
the keys C2 deferred here) and `[compaction].model` (deferred by C3);
`[limits]` gained `directory_list_entries`, `file_read_bytes`,
`file_read_max_bytes` and `tool_output_bytes`. `agentos-gateway config` prints
every one. Verified: `cargo fmt --all --check`, `cargo clippy --workspace
--all-targets -- -D warnings`, 530 tests, import boundaries, module sizes.

  - **Two of the four constants the item names were already gone.** The MCP
    timeout moved to `[limits].tool_timeout_ms` with a per-server override in
    D2, and the hydration budgets were already `[memory].hydrate_*` — the
    constants in `orchestrator/max.rs` are the `Default` impl behind them, not a
    second source of truth. Only the directory listing limit and the inline cap
    (done in C2) were outstanding, so this item was smaller than written and the
    deferred keys were the real work.
  - **Both wiring gaps were found by breaking the code, not by reading it.**
    Making `register_builtin_tool` ignore the configured file limits, and the
    summarizer ignore its configured tier, left the whole suite green: the
    tests covered `FileTool::with_limits` and `CompactionConfig` parsing, and
    nothing asserted the runtime *used* either. A key that reads back correctly
    and changes nothing is the exact failure mode this item exists to prevent.
    Both now have tests that go config → registry → tool output, and config →
    the client actually built; each turns red when its wiring is removed.
  - **`[compaction].model` is a tier, not a `provider:model` string.** Every
    other model in this runtime is chosen by `AGENTOS_LLM_MODEL_<TIER>` and by
    `/model`; a second spelling would be a second thing to keep in sync, and a
    typo'd tier now fails the load rather than the turn that would have
    compacted. The `high` default reuses the conversation's own client rather
    than building a second one for the same model — a separate client means a
    separate connection pool and a separate `/model` override state.
  - **The elide-then-compact ladder is now enforced, not commented.**
    `pressure_percent` had to sit above the 80% at which tool output is elided
    for free; that was a comment and a test on the *default*. A deployment
    setting `pressure_percent = 70` would have inverted it silently, spending a
    summarizer call on a request free pruning would have fixed. It is a load
    error now.
  - **Retention sweeps whole runs, from the gateway's idle phase.** A run's
    artifacts are referenced together by the transcript that produced them, so
    removing half of one leaves a conversation with locators that resolve and
    locators that do not — worse than removing all of it. The default is `0`,
    "keep everything", which is what C2 shipped; the floor when set is one day,
    because a shorter window would race the run that wrote it and turn a
    recoverable result back into the destroyed one spill exists to replace.
  - **The `file` tool's JSON schema is built from the configured bounds.** It
    advertised `maximum: 262144` as a literal. A schema naming a ceiling the
    tool no longer enforces has the model asking for reads it cannot get, or
    not asking for ones it could.
  - **Deliberately left as constants**, because the exit condition is about
    values an operator has reason to change and these are not:
    - `prune::PRUNE_TRIGGER_RATIO` (0.8) is not a policy trigger like
      `pressure_percent`. The remaining fifth of the window is headroom for the
      *reply*, which the estimator does not measure; changing it changes how
      much room the model has to answer, not a cost/quality trade.
    - `http::DEFAULT_TIMEOUT` (30 s) bounds a process-wide `OnceLock` client
      shared by the `http` tool, Qdrant, and Feishu — it cannot see per-
      deployment config without being rebuilt, and the bound an operator
      actually cares about for the tool is `[limits].tool_timeout_ms`, which is
      configurable and wins.
    - `compact::SPAN_SHARE_OF_WINDOW`, `tokens::ASCII_CHARS_PER_TOKEN`,
      `hybrid::RRF_K`, `task_workspace::SESSION_QUEUE_CAPACITY`, and the
      Landlock ABI bits are algorithm and protocol shape. Exposing them would
      invite tuning that changes results without changing a decision.
  - **Not closed: the isolation worker's own output cap.**
    `agentos-tool-worker` is a separate process that does not read
    `agent.toml`, so its inner `exec` capture stays at the compiled-in default.
    The registry that spawned it applies the configured cap to what it reads
    back, which is the bound that reaches the model; the inner one only bounds
    the worker's own memory. Stated in the worker rather than left to be found.

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

**Status: done.** New `config/catalog.rs` (the renderer),
`config/undocumented.txt` (the debt ratchet), `bin/agentos-gateway/catalog.rs`
(`agentos-gateway catalog [--check]`), `scripts/check-catalogs.sh`, a CI step
beside the boundary and module-size checks, and `tests/catalog_freshness.rs`.
Generated: `docs/config-catalog.md` (148 keys) and `docs/tool-catalog.md`.
Verified: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --
-D warnings`, 545 tests, import boundaries, module sizes, catalogs current.
Semver: nothing public changed shape — all six crates report no update
required.

  - **Three things are derived, from three sources that cannot disagree with
    the code.** Keys, types and nesting come from the config structs' own
    source, `include_str!`-ed at compile time — not read from disk, because the
    catalog a binary produces has to describe *that binary*, not whatever tree
    it is run from. Defaults come from serializing `WorkspaceConfig::default()`,
    so a changed default changes the catalog. Prose comes from each field's
    `///` comment, which is where it already lives.
  - **The first thing the catalog did was prove the config surface is mostly
    undocumented.** 121 of 148 keys had no doc comment. That is the finding, not
    a blocker: writing 121 descriptions in this item would mean inventing
    semantics for subsystems I had not verified, which is how documentation
    becomes wrong rather than absent. Instead the 21 section-level fields on
    `WorkspaceConfig` were written (the rows an operator reads first), and the
    remaining 102 went on `config/undocumented.txt` as an acknowledged debt.
  - **The debt list is a ratchet that turns both ways.** A key with no doc that
    is *not* listed fails the check, so a new config key cannot be added without
    saying what it does. A key that *is* listed but has since gained a doc also
    fails, so the list cannot go stale and can only shrink. Both directions are
    proven by deliberately breaking them.
  - **Undocumented rows say so, and the count is rendered into the file.** A
    blank description cell reads as "this key does nothing"; `*(undocumented)*`
    plus a footer counting them reads as a debt. A debt nobody can see is a debt
    nobody pays.
  - **Markers, not whole-file generation.** Each catalog carries an
    introduction the generator does not own, so regenerating cannot wipe prose
    somebody wrote. A missing marker is an error rather than a silent no-op that
    would report success while leaving the file stale.
  - **The parser is string matching, not a syntax tree**, which is a deliberate
    trade for one dependency-free file. It handles the shape every config struct
    in this crate is written in and would silently miss anything exotic — so the
    guard is that a field it fails to pair with a doc is *reported*, and
    `every_section_is_reachable` pins that every top-level key of the serialized
    default is one the walk actually reached. The failure mode is a loud gap.
  - **The built-in tool list is now one constant.** `BUILTIN_TOOL_NAMES` is
    shared by `register_builtin_tool` and the catalog, with a test that every
    name in it registers — a catalog listing a different set from the one the
    runtime builds would read as "this deployment does not have that tool".
  - **Freshness is checked twice, on purpose.** `scripts/check-catalogs.sh` is
    what CI runs, and `tests/catalog_freshness.rs` is what `cargo test` runs. A
    check that only exists in CI drifts locally until a commit later; both are
    proven to fail on a changed default, an added key, and a documented key
    still on the debt list.

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
- **Status: done** (2026-08-17). All three classes are asserted in
  `crates/agentos-core/src/invariants.rs`, and each has a test that violates it
  and observes the panic (14 unit tests, 7 of them `#[should_panic]`). The
  module and every call site are behind `#[cfg(debug_assertions)]` at the *call*
  rather than only inside the function, so a release build compiles them away
  entirely — verified with `cargo check --release --workspace`. Notes:
  - **The call sites are proven live, not assumed.** A passing suite proves only
    that an invariant does not misfire; it says nothing about whether the check
    runs at all. Each of the three was probed by making it panic
    unconditionally and re-running the workspace suite: 20 tests reach the
    assembly check, 9 the delegation check, 10 the pairing check. Without that
    probe the whole item could have shipped as dead code with green tests, which
    is the failure X3 already found once in this repo.
  - **One call site is not where the roadmap said.** The assembly invariant
    lives in `prompt::assemble`, not `loop/mod.rs`. The loop records the
    request header but never sees the assembled message vector — `assemble` is
    the only place both sides of the relationship exist at once. The other two
    are where the roadmap put them (`loop/mod.rs`, `subagents/mod.rs`).
  - **The delegation check does not re-run `Policy::narrow`.** Asserting
    `narrow(parent, child).is_ok()` immediately after calling it is a tautology.
    The invariant instead states the coarser security property directly: for
    every action the child can reach, the parent must have some non-`Deny` path
    to it. That survives a rewrite of narrowing's rule-covering logic, and
    fires if the rewrite lets something through.
  - **Argument constraints are deliberately out of scope for it.** A child is
    allowed to be *more specific* about arguments than its parent — that is what
    an explicit sub-agent tool allowlist does, and `narrow` documents it. An
    argument-level comparison would fire on configurations that are correct by
    design, so the check compares actions only.
  - **The pairing rule has a real exemption.** `Delegate` and `Escalate` results
    are `Tool`-role items with no `tool_call_id` (`loop/items.rs`), because they
    answer a delegation rather than a tool call. They carry nothing to pair and
    are exempt; asserting over them would fire on every correct sub-agent run.
  - **The pairing check runs at the append, not over loaded history.** Checking
    a whole transcript would assert over items written by past builds, and a
    debug-build gateway would then panic on legacy session data it did not
    create. Checking the item just pushed catches the same defect class against
    state this build produced.
  - Interface impact: none. `agentos-interfaces` and `agentos-proto` unchanged
    (`--baseline-rev HEAD`: no semver update required). The one visibility
    change is `PolicyRule::label`, private → `pub(crate)`, so a violation names
    its action the same way `PolicyError::Widened` does.
  - Loop overhead is unaffected: benchmarks build in release, where these do not
    exist. p50 4.83µs against the ≤2ms target.

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
- **Status: done** (2026-08-17). `Session::fork(source, boundary, child_id)` is a
  defaulted trait method returning how many items landed;
  `cargo semver-checks check-release -p agentos-interfaces --baseline-rev HEAD`
  reports no semver update required. SQLite overrides it with one
  `INSERT … SELECT`, so the rows never leave the database. `tests/session_fork.rs`
  covers both (12 tests). Notes:
  - **`boundary` is a length, not a range, and that is correctness rather than
    convenience.** A compaction checkpoint names the span it hides by *absolute*
    position, so a fork that dropped a head would leave every checkpoint in the
    copied tail pointing at the wrong items — the child would hide text the
    parent showed, or show text the parent had folded away. Copying from 0 keeps
    positions identical, which is what makes the child's projection equal the
    projection of the parent's prefix. This is the whole content of the P2
    dependency, and the test that pins it fails if the prefix becomes a suffix.
  - **A boundary past the end seeds what exists rather than failing**, and this
    is load-bearing for the delegation path rather than leniency. The parent
    names a point in the transcript it holds *in memory*; the store holds a
    prefix of that, because the turn in flight is not persisted until the run
    finishes. So a seeded sub-agent gets the parent's history up to the previous
    turn, plus the delegation prompt as its own first input — the current ask
    reaches it as a message, not as history. Worth knowing before relying on it:
    earlier tool results from the *same* parent turn are not in the seed.
  - **Forking onto a conversation that already has items is refused**, in both
    implementations, inside SQLite's transaction so a concurrent append cannot
    slip between the check and the copy. Merging two logs would invalidate every
    checkpoint position in both. This refusal is also what makes sub-agent
    seeding idempotent: a sub-agent's conversation id is stable, so the second
    delegation finds history and leaves it alone.
  - **The refusal uses `SessionError::Backend`, not a new variant.** A new
    variant on an exhaustive public enum is a major bump, and this item's Verify
    line requires additive-only. The message names the target and its item
    count, so the condition is still diagnosable; a typed variant is worth doing
    the next time `agentos-interfaces` takes a deliberate break.
  - **Spilled output is not copied and does not need to be.** A locator is an
    absolute path into a run-keyed store, so a seeded item resolves to the
    artifact the parent's run wrote. The test asserts the child reads the
    content back *and* that the run directory still holds exactly one file.
  - **The sub-agent wiring is opt-in and off by default**
    (`[[subagents]] seed_from_parent`, plumbed config → `SubAgentDefinition` →
    `SubAgentInvocation` → `fork` before the child run loads its transcript).
    Handing a sub-agent the whole parent conversation costs tokens on every turn
    it takes and shows a possibly weaker model everything the parent has seen.
    **No shipped sub-agent enables it** — turning it on for the code-review or
    edit sub-agent is a deployment's call, not this item's.
  - **Seeding is best effort.** A fork that fails leaves the sub-agent starting
    from an empty conversation, which is exactly its pre-X6 behaviour; failing
    the delegation because history could not be copied would trade a working
    sub-agent for none. The two expected non-seeds — an ephemeral input, and a
    target that already has history — log at debug rather than warn.
  - Proven load-bearing by breaking each half: ignoring the definition flag
    reddens the seeded-delegation test; an off-by-one in the SQLite boundary
    reddens 2, including the one that asserts the override and the default agree;
    copying a suffix instead of a prefix reddens 2; dropping the emptiness
    refusal reddens 2.
  - Interface impact: one defaulted trait method on `Session`. `agentos-proto`
    unchanged.

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
| `[limits].directory_list_entries`, `.file_read_bytes`, `.file_read_max_bytes`, `.tool_output_bytes` | X3 | **Landed** in X3. The `file` tool's JSON schema is built from the read bounds, so it cannot advertise a ceiling the tool does not enforce |
| `[spill].root`, `[spill].retention_days` | C2 → X3 | Spill artifact storage. **Landed** in X3; retention defaults to `0` (keep everything, C2's behaviour) and sweeps whole run directories from the gateway's idle phase when set |
| `[compaction].enabled`, `.pressure_percent`, `.retain_tail_turns`, `.model` | C3, X3 | Compaction policy. **Landed** in C3; `.pressure_ratio` shipped as an integer `pressure_percent`, the unit C1 already traces. `.model` **landed** in X3 as a model *tier* (`high`/`medium`/`low`), the same vocabulary `AGENTOS_LLM_MODEL_<TIER>` and `/model` use, defaulting to the conversation's own model. X3 also made "compact above the elision trigger" a load-time check rather than a comment |
| `[limits].tool_timeout_ms`, `[limits].tool_timeout_overrides` | D2 | Tool deadlines. **Landed** in D2, in `[limits]` rather than `[resources.tools]`: that section says *which* tools are enabled, and `ResourceSection` is shared with skills/mcp/llm where a timeout is meaningless. Replaces the MCP 10 s constant |
| `[jobs].max_concurrent`, `.output_limit_bytes`, `.promotable` | D3 | Job registry bounds. **Landed** in D3; `.promotable` added — the allowlist of tools that become a job instead of failing at their deadline |
| `[gateway].shards`, `.inbox_capacity` | G1 | Conversation sharding. **Landed** in G1; `shards = 0` means one per core, capped at 64. `.inbox_capacity` bounds both lists — envelopes waiting for a run of their own, and messages steering the one in flight |
| `[approval].expiry_seconds` | G2 | Approval prompt expiry. **Landed** in G2; default 900s, `0` means no expiry, otherwise 30–86400. An expired prompt records `cancelled`, not `rejected` |
| `[memory].hydrate_*` | X3 | Already config; move the remaining constants beside them |

## Interface-change ledger

Run `cargo semver-checks check-release -p agentos-interfaces` on each and record
the result in the PR.

| Item | Change | Expected |
|---|---|---|
| P3 | `RunContext` gains `request_sink`; `agentos-proto` gains `RequestHeader` | **Verified**: interfaces major (`constructible_struct_adds_field`), proto additive |
| C1 | `RequestHeader`/`RequestSection` gain token fields; `Llm` gains a defaulted `context_budget_tokens()` | **Verified**: proto major (`constructible_struct_adds_field`), interfaces and llm unchanged |
| C2 | `RequestHeader` gains `elided_messages`/`elided_chars` | **Verified**: proto major (`constructible_struct_adds_field`), interfaces and llm unchanged |
| C3 | None — compaction is entirely inside `agentos-core` | **Verified**: interfaces, proto, and llm all report no semver update required |
| C4 | `OrchestratorError` and `LlmError` gain a `ContextLengthExceeded` variant; `ProviderError` gains `ContextLength` | **Verified**: interfaces and llm major (`enum_variant_added`), proto unchanged |
| D1 | `RunContext` gains a cancellation field | **Verified**: interfaces major (`constructible_struct_adds_field`), proto and llm unchanged. `Tool` was *not* changed — `call_with_context` already carries the context |
| D2 | `ToolSpec` gains `timeout_ms` (`#[serde(default)]`) | **Verified**: interfaces major (`constructible_struct_adds_field`), proto and llm unchanged; additive on wire |
| D3 | None — the job registry is entirely inside `agentos-core` | **Verified**: interfaces, proto, and llm all report no semver update required |
| G1 | `Channel` gains a required `egress() -> Arc<dyn Egress>`; `send` becomes a provided method delegating to it | **Verified**: interfaces major (`trait_method_added` without default), proto and llm unchanged. Required: a sharded gateway cannot hold `&mut self` for `receive` and `&self` for `send` at once |
| G2 | `ApprovalStatus` gains an `Unanswered` variant | **Verified**: interfaces major (`enum_variant_added`), proto and llm unchanged. `agentos-core` is also major (`ResumeDecision` and `RunError` variants, `GatewayRun::Paused` fields, `approval_prompt_envelope` arity) but has no external consumers |
| X1 (b) | `Plan` gains `CallTools`; `RunState` gains `queued_tool_calls` | **Verified**: interfaces major (`enum_variant_added`, `constructible_struct_adds_field`), proto and llm unchanged |
| X2 | `ToolSpec.requires_isolation: bool` → `sandbox: SandboxMode` | **Verified**: interfaces major (`struct_pub_field_missing` plus the new field), proto and llm unchanged. `agentos-core` also major (`McpToolConfig.sandbox`, `Exec.sandbox`, `ExecError::Sandbox`) with no external consumers |
| X6 | `Session::fork` as a defaulted method | Additive |

`Approve`, `RunLoopState`, and the guardrail traits are unchanged by every item
in this roadmap.

## Status

Not started. Update each item with a **Status** line and its commit as it lands,
matching the convention in [`FEATURE_ROADMAP.md`](FEATURE_ROADMAP.md).
