# ADR-0006 — `/clear` starts a new epoch; it does not delete

- Status: accepted
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
  by retention policy, not by `/clear`.
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
