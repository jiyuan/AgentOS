# ADR-0007 — Stable channels buffer; streaming is opt-in until guardrails are incremental

- Status: accepted; implemented by M6 / `STATE-001`
- Date: 2026-08-21
- Milestone: M6

## Context

Output guardrails exist to stop the runtime from saying something it should
not. They run in the loop at the end of a turn. Streaming forwards each chunk
to the sink during `plan()` — before those guardrails have run. The TUI sink
does `print!` followed by `flush` per chunk and has no way to retract what it
already wrote.

So on a streaming channel, an output guardrail is a check performed after the
user has already read the output. The guarantee the guardrail advertises is
not one it can deliver there.

This is not an argument against streaming. It is an argument that streaming
and end-of-turn guardrails are incompatible, and that today's default resolves
that incompatibility in favour of latency without saying so.

## Decision

**Stable channels default to buffered, guardrail-checked output.** A terminal
reply is emitted after the output policy has passed it, not before.

**Provisional streaming stays available and opt-in, and is labelled
provisional** — in config, in the catalog, and wherever capability maturity is
reported. A deployment that prefers latency to the guarantee may have it,
knowingly.

**The blocker is named, not worked around.** Streaming becomes eligible for
stable default when an *incremental* output-guardrail interface exists — one
that can inspect a chunk before it is forwarded and can refuse the remainder.
Until then, no amount of care at the sink fixes it, because the sink is
downstream of the decision.

**Cancellation and every other terminal reply route through the declared
output policy.** Cancellation currently does not, which makes it a second path
around the same check.

## Consequences

- Perceived latency on stable channels increases by roughly one turn's
  generation time. That is the cost of the guarantee, and it is stated rather
  than silently traded away.
- Anyone relying on streaming today must opt in explicitly. The opt-in is the
  record that they accepted the exposure.

## Verification

- A seeded output-policy violation emits **zero** user-visible bytes in stable
  mode.
- The same violation in provisional streaming mode emits bytes and is reported
  as a provisional-mode exposure, so the difference is tested rather than
  assumed.
- A cancelled run's terminal reply passes through the output policy.
- The capability matrix labels streaming `preview`, and a test fails if a
  `preview` capability is enabled by default.

## Implementation notes

`[channels] provisional_streaming`, off by default;
`crates/agentos-core/src/loop/output.rs` is the gate every synthesized
terminal reply passes through. Verified by `tests/output_gate.rs` and the
default assertion in `tests/capability_matrix.rs`.

One thing the decision left implicit. A model reply that trips an output
guardrail **fails the run**; a notice the *loop* wrote — cancellation, budget
exhaustion — is replaced by a shorter fixed one instead. Those have to differ:
the run must terminate at a cancellation, and re-raising would resurrect the
hard-failure path the safeguard exists to remove, throwing away the tool
results a user who pressed stop most wants kept. Either way the policy decides
what leaves, which is the property the ADR is about.
