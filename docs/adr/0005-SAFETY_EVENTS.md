# ADR-0005 — Safety decisions are immutable events, not absence of state

- Status: accepted; implemented by M6 / `AUD-001`
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
  explicit, authorized operation rather than a background cleanup. **Settled
  by M7 / `QUOTA-001`:** `agentos-gateway purge --audit --before YYYY-MM-DD`,
  covering `safety_events` and `memory_access_log` together. It reports the
  counts and changes nothing until `--apply --yes N` names the number it
  printed, and the date is absolute rather than "N days ago" so the count an
  operator was shown is the count the apply computes. The delete writes an
  `audit_purged` event afterwards, into the store it just shortened — the
  first row of the remaining history says how the preceding history ended,
  which is this ADR's own rule applied to itself. Nothing else in the tree
  deletes from either table, and the background retention sweep
  (`crates/agentos-core/src/retention/`) deliberately does not touch them.
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

## Implementation notes

`crates/agentos-core/src/audit/`, verified by `tests/safety_events.rs`, which
reopens the store file before reading so nothing passes on writer-side state.

Two things the decision above did not settle, resolved during implementation:

- **Superseded by R1 / `AUD-002`: a failed required append fails the affected
  run closed.** It is still logged at `error!` as
  `safety_log_write_failed`, but the failure is also returned as the typed
  `RunError::SafetyEvidence`. Approval requests and resolutions, grant
  issuance and use, and other protected transitions do not proceed without
  their evidence. A denial, refusal, or cancellation remains the safer
  outcome, then the run stops before further mutable work. Database-trigger
  failpoints in `tests/audit_failpoints.rs` enforce this contract.
- **A reason is bounded, not banned.** Several guardrails interpolate a
  model-chosen fragment — a program name, a path — into the sentence they
  refuse with, and a log that could not say *why* would not be readable. The
  fragment is already in the transcript and the trace; this store outlives
  both, so the reason gets a 512-byte ceiling rather than the guardrail's word
  for it. Arguments remain digest-only, which is the line that does not move.
