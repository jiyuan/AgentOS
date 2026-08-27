# Observability

What a running Agent OS deployment emits, and what an operator can build an
alert on. Written as the last acceptance criterion of M9 / `CI-002`, which asks
for dashboards covering seven conditions.

**Dashboards are not in this repository, and should not be.** Which metrics
system a deployment runs, where its logs go, and what an on-call rotation
considers page-worthy are all decisions this project cannot make for it. What
*is* this project's job is that the signal exists at all and is machine-readable
— an operator cannot alert on a condition the runtime never mentions. So this
file is the inventory: one row per condition, naming the signal and, where the
signal is weak or missing, saying so plainly rather than describing an intent.

## The three sources

- **`safety_events`**, a table in the session database. Every authorization
  boundary decision, typed by `kind` and `outcome`, carrying a `Principal` and
  a timestamp ([ADR-0005](adr/0005-SAFETY_EVENTS.md)). This is the source worth
  alerting on: it is durable, structured, and complete by construction — a
  decision that reached no row is a bug the invariant forbids.
- **The gateway log**, `logs/agentos-gateway.log`, one line per notable event
  with a Unix timestamp. Human-facing, rotated by `[retention]`.
- **`tracing` output** on the process's stderr, structured fields on spans.
  Everything in the core emits here; the gateway log is the operator-facing
  subset.

## Alertable conditions

| Condition | Signal | Strength |
|---|---|---|
| Queue saturation | `tracing` warn `inbox full; message refused`, with `conversation_id`, `ingress_id`, and `inbox_capacity`; the gateway sends a terminal refusal before settling that exact event | Good |
| Sandbox denial | `safety_events` where `kind = 'sandbox_refusal'` | Good |
| Approval failure | `safety_events` where `kind = 'approval_resolved'` and `outcome` in (`rejected`, `unanswered`, `unavailable`) | Good |
| Approval recovery | Gateway log `restored N pending approval(s)`; startup refuses corrupt approval identity/scope or ambiguous prompt/action delivery | Good |
| DB contention | Errors carrying `database is locked`; the `sqlite refused WAL` warn at startup | **Weak.** No counter — see below |
| Retention backlog | The gateway log's `retention: …` line, which names what each sweep removed | **Partial.** It reports what went, not what is still there |
| Maintenance lag | Gateway log `maintenance sequence=… started_at=… completed_at=… lag_ms=… duration_ms=…`, with separate cron/reflection/retention/ingress/job status | Good |
| Process leaks | Gateway log `process shutdown deadline reached; abandoning channel worker(s) …`, per-channel `shard(s) … did not drain`, and `JobRegistry::len` | **Partial** |
| Delivery lag | `report_abandoned_work` at startup and shutdown, naming every accepted-and-unsettled ingress event with its `attempts` and `accepted_at` | Good |
| Ambiguous external outcome | Gateway startup refusal plus `agentos-gateway migrate` rows naming `event`, `action`, `delivery`, state, and bounded reason | Good |

## The three gaps, named

These are real and are not closed by M9. Each is written here rather than left
for an operator to discover by not being paged.

**Contention has no counter.** A statement that waits out `busy_timeout`
succeeds and says nothing, and one that exceeds it fails with `database is
locked` wherever the caller logs. So the observable is the *failure*, not the
pressure before it — an operator learns about contention when it has already
cost a turn. A counter of waits, exported from the pool, is the fix.

**Retention reports flow, not stock.** The sweep says what it removed. A store
growing faster than its ceiling removes it looks identical to a quiet one:
both report small numbers. What would answer the question is the size of each
store after the sweep, which the sweep already computes and discards.

**Nothing counts child processes.** `tools/exec.rs` puts every child in its own
process group and signals the group on a deadline or cancellation, and
`a_killed_child_leaves_no_grandchild` tests that it works. But no running total
is emitted, so a leak is visible only in the host's process table.

## Useful queries

The session database is SQLite; `safety_events` is meant to be read directly.

```sql
-- Denials and refusals in the last day, by kind.
SELECT kind, outcome, COUNT(*)
  FROM safety_events
 WHERE recorded_at > datetime('now', '-1 day')
   AND outcome IN ('denied', 'refused', 'tripped', 'rejected', 'unanswered')
 GROUP BY kind, outcome
 ORDER BY 3 DESC;

-- Approvals nobody answered, newest first.
SELECT recorded_at, principal, subject
  FROM safety_events
 WHERE kind = 'approval_resolved' AND outcome = 'unanswered'
 ORDER BY row_id DESC LIMIT 50;

-- Accepted messages that never settled: safe pending stages and explicit
-- action/delivery ambiguity are distinguishable.
SELECT channel_id, event_id, conversation_id, attempts, accepted_at,
       delivery_state, action_id, delivery_id, state_reason
  FROM ingress_events
 WHERE settlement IS NULL
 ORDER BY accepted_at ASC;

-- Active approval pauses and their restart stage. `delivery_started` or
-- `delivery_ambiguous` requires operator reconciliation rather than resend.
SELECT approval_instance_id, channel_id, conversation_id, status, expires_at,
       resolution_event_id, updated_at
  FROM pending_approvals
 ORDER BY updated_at ASC;
```

`agentos-gateway status` answers whether a gateway is serving, by holding a
lock rather than by guessing from a pid — so it is safe to poll.
