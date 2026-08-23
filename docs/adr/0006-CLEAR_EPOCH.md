# ADR-0006 — `/clear` starts a new epoch; it does not delete

- Status: accepted; implemented by M6 / `STATE-001` except principal keying
- Date: 2026-08-21
- Milestone: M6

## Context

`AGENTS.md`, `DESIGN.md`, and `docs/ARCHITECTURE.md` all state that the
session log is append-only and that compaction, elision, and fork are
projections over it rather than mutations of it. That is true of those three.
It is not true of `/clear`, which runs:

```sql
DELETE FROM session_items WHERE conversation_id = ?1
```

So the one operation a user reaches for most casually is the only one that
destroys the record irreversibly, and the documented invariant was false at
exactly the point where it mattered. (The wording was corrected in M0; this
ADR decides the behavior.)

`prompt/projection.rs` is the in-tree precedent: compaction already hides an
entire span from the model without deleting a single item.

## Decision

**`/clear` writes an epoch marker. It removes nothing.** The projection
excludes items before the current epoch, so the model sees a fresh
conversation, and the log still holds what happened. This is the same
mechanism as a compaction checkpoint, applied to the whole history rather
than a span.

**Irreversible purge remains available, as a distinct and explicitly
authorized operation.** Deletion is a legitimate requirement — a user asking
to be forgotten is not asking for a projection. It is simply not what `/clear`
means, and it must not be reachable by the casual path. The purge crosses
`Approve` and emits a safety event (ADR-0005).

**The epoch is keyed by principal** (ADR-0003), so one participant clearing a
group conversation cannot clear another's state.

## Consequences

- Storage grows where it previously shrank. Bounded by the purge operation and
  by retention policy, not by `/clear`. **Settled by M7 / `QUOTA-001`:** there
  is deliberately no background sweep over `session_items`. A timer that
  trimmed old items off a live conversation would leave a history beginning
  mid-sentence, with compaction summaries citing items that are gone — a
  mutation of the log wearing retention's name. What bounds it instead is
  `agentos-gateway purge --sessions --before YYYY-MM-DD`, which surveys whole
  conversations that have been idle since that date, reports them, and applies
  only when the operator types the count back. It removes each one through
  `SqliteStore::purge_session`, so this ADR's "one deletion path, one safety
  event" still holds exactly — a bulk `DELETE` would have been the second
  path.
- Every reader of `session_items` must respect the epoch. A query that forgets
  to is a correctness bug that shows up as resurrected history, so the epoch
  filter belongs in the projection layer, not at call sites.
- The invariant in `AGENTS.md` becomes true without qualification once this
  lands, and its M6 caveat can be removed.

## Verification

- A normal `/clear` performs no `DELETE` against `session_items`.
- After `/clear`, the projection returns no pre-epoch items, and the store
  still contains them.
- A second group participant's `/clear` does not affect the initiator's
  visible history.
- The explicit purge does delete, requires approval, and leaves a safety
  event.

## Implementation notes

`crates/agentos-core/src/memory/session_store.rs`, verified by
`tests/session_epoch.rs`, which reads the log underneath the projection so
"hidden" and "deleted" are actually distinguishable to the test.

Two departures from the decision above:

- **The epoch is keyed by conversation, not by principal.** `Session::load`
  takes a bare `ConversationId`, so a per-principal epoch would be unreadable
  by the one query that has to apply it. Principal keying arrives with M3
  deliverable 2, which is where the `Session` semver break lives. Until then,
  one participant's `/clear` in a group conversation clears the shared view —
  stated in the capability matrix rather than left to be discovered.
- **The purge does not cross `Approve`.** `Approve` gates what the *model* may
  do; a purge is an operator's decision about their own records, with no run to
  pause and no model asking, so routing it through the run policy would be a
  category error. It is gated instead on shell access to the host and on naming
  the conversation twice (`agentos-gateway purge --conversation ID --yes ID`),
  which is what keeps it off the casual path — the point the decision was
  actually making. The safety event is recorded as required.
