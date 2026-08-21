# ADR-0005 — Safety decisions are immutable events, not absence of state

- Status: accepted
- Date: 2026-08-21
- Milestone: M6

## Context

The runtime makes decisions that a later reader needs to reconstruct: an
approval was requested and resolved, a guardrail tripped, a sandbox refused a
call, a run was cancelled, a delegation grant was issued and used. Today most
of these are inferred from *mutable* state, and the strongest signal for a
successful approval is a **deletion**: the pending record is removed. An
absence is indistinguishable from a record that was never written, one that
was cleaned up, and one that was deleted to hide something.

`memory_access_log` is the in-tree precedent for doing this properly — an
append-only table — but it covers memory access only. Approval *requested* is
not recorded there at all. Error paths return before persisting state, so the
runs most worth reconstructing are the ones that leave the least behind.

## Decision

**Every safety-boundary decision emits an immutable, durably stored event.**
The set: approval requested, approval resolved, approval denied, guardrail
trip, sandbox refusal, cancellation, delegation-grant issuance, delegation-grant
use, and terminal error. The store is modelled on `memory_access_log`:
append-only, never updated, never deleted on the normal path.

**Deletion is never the record of an outcome.** A resolved interruption is
retained and marked resolved; it is not removed. This is the same principle as
the append-only session log (ADR-0006) applied to the authorization surface.

**Error paths persist before returning.** A run that fails is a run whose
safety events must still be readable.

**Events carry a principal (ADR-0003) and carry digests, not payloads.**
Sensitive tool arguments are redacted or hashed rather than duplicated into a
store that is, by construction, never cleaned up. An audit log that copies the
secrets it is auditing is a second copy of the secret with a longer retention
policy.

**Recoverable tool denial and fatal structural denial are distinct types.** A
policy `deny` on one tool call is something the model reads and replans
around; a structural denial ends the run. Collapsing them makes the state
diagram wrong and the events unreadable.

## Consequences

- The store grows monotonically and needs a retention story that is an
  explicit, authorized operation rather than a background cleanup.
- Redaction has to be decided per event type at the point of emission. A
  generic "log the arguments" helper is exactly what this ADR prohibits.
- Some existing code paths that signal by deleting need a second field
  instead.

## Verification

- Approval and guardrail history survives pause, resume, restart, and a
  terminal error.
- A successful approval leaves a resolved event, not a missing row.
- Canary secrets planted in tool arguments do not appear in the event store.
- A recoverable denial and a structural denial produce distinct event types
  and distinct run outcomes.
