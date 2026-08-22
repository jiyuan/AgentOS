# Architecture decision records

Each record fixes **one testable definition** for an invariant the audit found
disputed — stated in the shipped documentation, contradicted by the code, or
resolved differently in different places. A record is the contract the
implementing milestone is measured against; the plan slice is how it gets
built.

These were written as `ADR-001`, a deliverable of M0 in
[`../AUDIT_REMEDIATION_PLAN.md`](../AUDIT_REMEDIATION_PLAN.md). They describe
target behavior. Where the current code differs, the record says so and names
the milestone that closes the gap.

| ADR | Decision | Milestone |
|-----|----------|-----------|
| [0001](0001-POLICY_NARROWING.md) | Narrowing is exact over actions *and* arguments; unattended delegation is a separate, expiring, principal-bound grant | M3 (`AUTH-002`) |
| [0002](0002-FAIL_CLOSED_ISOLATION.md) | A sandboxed tool runs under a compatible executor or does not run; executor capability is negotiated, availability is probed | M4 (`SBX-001`), M2 (`CI-001`) |
| [0003](0003-TYPED_PRINCIPAL.md) | One versioned principal keys every isolated resource; encoding is injective, and channels fail closed on identity | M3 (`ID-001`) |
| [0004](0004-REQUEST_KINDS.md) | Every provider call has a kind and a recorded manifest — including kinds that contribute no transcript, which is what preserves routing's injection defence | M5 (`REQ-001`) |
| [0005](0005-SAFETY_EVENTS.md) | Safety decisions are immutable append-only events; a deletion is never the record of an outcome | M6 (`AUD-001`) |
| [0006](0006-CLEAR_EPOCH.md) | `/clear` writes an epoch marker; irreversible purge is a distinct authorized operation | M6 (`STATE-001`) |
| [0007](0007-BUFFERED_OUTPUT.md) | Stable channels buffer behind output guardrails; streaming is opt-in and labelled provisional until guardrails are incremental | M6 (`STATE-001`) |

## Writing a new one

Number sequentially, keep the filename uppercase after the number, and include
Context / Decision / Consequences / **Verification**. The verification section
is not optional: an invariant without a named way to test it is the failure
mode this whole set exists to correct.
