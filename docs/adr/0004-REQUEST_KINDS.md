# ADR-0004 — Every provider call has a kind and a recorded manifest

- Status: accepted
- Date: 2026-08-21
- Milestone: M5 (`REQ-001`)

## Context

`AGENTS.md` states that `prompt/` is the only path from a `RunContext` to the
messages a provider sees, and that every request records what it was made of
in a `RequestHeader`. Two production calls do not:

- the routing classifier, `orchestrator/max.rs`
- the compaction summarizer, `prompt/compact.rs`

**Their separation from prompt assembly is correct and must survive this
work.** `prompt/mod.rs` argues that assembling the classifier's request "would
spend the skill prelude and recalled memory on a question that has no use for
either, and would let a stored fact steer routing." That is a prompt-injection
defence. Routing a stored memory into the classifier would let a
memory-poisoning attack choose which orchestrator handles the next turn.

The genuine defect is narrower: neither call records a header, so "what did
the model see" cannot be answered from the trace for either one; and
compaction records no usage at all, so summarizer tokens are missing from run
totals. The obstacle is structural — `compact()` holds `&mut RunState`, not a
`RunContext`, so it has nowhere to put a header.

## Decision

**One provider-call gateway, taking a request *kind* rather than a message
vector.** Planning, routing, compaction, and any future model request go
through it.

**A kind determines the section set, and some kinds legitimately contribute no
transcript.** This is what lets the classifier keep its deliberate isolation
while still being recorded: it is not an exception to the manifest rule, it is
a kind whose manifest names a small, fixed section set. The invariant to
enforce is *"every provider call records what it was made of"*, not *"every
provider call is assembled from the transcript"* — conflating the two is what
would break the injection defence.

**Every call records a `RequestHeader`, a usage record, and a trace link.**
Compaction gets a header sink, either on `Compaction<'a>` (a two-field `Copy`
struct) or by returning the header alongside `Compacted` for
`loop/planning.rs` to emit. Compaction usage reaches run totals.

**`invariants.rs::request_derives_from_state` branches per kind**, since the
classifier and summarizer carry no transcript and would otherwise trip it.

**CI fails when a direct provider call is added outside the gateway.** The
rule is only as good as the thing that notices it being broken.

## Consequences

- Adding a new kind of model request means declaring a kind. That is friction,
  and it is the mechanism: an undeclared request is a request nobody can
  replay.
- Run token totals increase, because compaction's tokens were being spent and
  not counted. Reported figures will move; the old ones were wrong.

## Verification

- Exactly one manifest is recorded per provider invocation, routing and
  compaction included.
- A replay can identify the inputs to every provider request in a run.
- A test asserts the routing classifier's manifest contains **no** skill
  prelude and **no** recalled-memory section. This is the regression guard on
  the injection defence, and it must fail if a later refactor "unifies" the
  classifier into full assembly.
- Run totals after a compaction include the summarizer's tokens.
- A deliberately added direct provider call fails the CI check.
