# ADR-0001 — Strict policy narrowing, with delegation as a separate grant

- Status: accepted
- Date: 2026-08-21
- Milestone: M3 (`AUTH-002`)
- Supersedes: the informal reading of "sub-agent policies only narrow" in
  `AGENTS.md`, `README.md`, `DESIGN.md`, and `docs/ARCHITECTURE.md`

## Context

`Policy::narrow(&parent, &child)` is supposed to guarantee that a sub-agent
cannot do anything its parent could not. It does not. `parent_exposes_tool`
(`approve/mod.rs`) accepts a child `Allow` whenever the parent holds *any*
rule for the same tool name under `Allow` or `AskUser`. Two consequences
follow, and both are widening:

- A parent rule constrained by `arg_equals` — `file` limited to
  `{operation: "read"}` — legitimises an unconstrained child `Allow` that
  reaches `write`.
- A parent `AskUser` becomes a child `Allow`, so a sub-agent performs
  unattended what the parent would have stopped to ask about.

This was not an oversight. The doc comment states the intent: "an explicitly
allowlisted sub-agent tool needs no approval." That is a real product need —
an unattended sub-agent that pauses for approval is not unattended — but it
is being met by weakening the authorization check for every sub-agent rather
than by granting the exception to the one that needs it.

Three places currently bless the weakening and must move together with the
code: `approve/mod.rs` unit tests, `tests/runner_narrowing.rs`, and
`invariants.rs`, whose comment reads "Argument constraints are ignored here."
The 1000-case proptest suite scopes itself to "tool-granular, not
argument-granular" and therefore cannot see the defect.

## Decision

**Narrowing is exact, over actions *and* arguments.** For every possible tool
call, the child's effective decision must be no more permissive than the
parent's effective decision for that same call. `Allow > AskUser > Deny`.
A child rule is accepted only when every call it admits is admitted at least
as permissively by some parent rule, `arg_equals` included. `narrow` returns
`PolicyError::Widened` otherwise, at startup, with the offending rule labelled.

**Unattended delegation becomes a separate, explicit object — a delegation
grant — not an exception inside `narrow`.** A grant is:

- issued against a principal (ADR-0003), not a tool name;
- constrained to exact actions and exact argument constraints;
- bounded by an expiry;
- non-transitive: a sub-agent holding a grant cannot mint one for its own
  sub-agent;
- durable, with issuance *and* each use recorded as a safety event
  (ADR-0005).

`narrow` stays a pure lattice check. The grant is consulted at `Approve`,
alongside the policy decision, never folded into it.

## Consequences

- Configurations that rely on today's promotion break at startup rather than
  silently. This is intended; the diagnostic names the rule and the parent
  rule that fails to cover it.
- A sub-agent that genuinely needs unattended use of a gated tool needs a
  grant written for it. That is more configuration, and it is the point: the
  authority becomes visible and auditable instead of implicit in an allowlist.
- `invariants.rs` loses its argument-blind exemption.

## Verification

- Property test: for randomized parent/child rule sets **and randomized tool
  calls**, `child_decision(call) <= parent_decision(call)`. The generator must
  produce `arg_equals` constraints, not only tool names.
- A parent constrained to `{operation: "read"}` with an unconstrained child
  `Allow` fails `narrow`.
- A parent `AskUser` with a child `Allow` and no grant fails `narrow`.
- The same pair with a matching, unexpired grant is admitted, and both the
  issuance and the use appear in the safety-event store.
- An expired grant, a grant for a different principal, and a grant used to
  mint a second grant are each refused.
