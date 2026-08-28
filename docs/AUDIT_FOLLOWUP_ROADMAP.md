# AgentOS Audit Follow-up Roadmap

Drafted: 2026-08-23

Status: **R0 implemented; R1 underway with `ID-003`, `AUTH-003`, `AUTH-004`, `AUTH-005`, and `AUD-002` complete; R2 complete with `GW-002`, `GW-003`, `GW-004`, `GW-005`, `STATE-002`, `MAINT-002`, and `CTRL-002` complete; R3 complete with `FS-002`, `FS-003`, `PROC-002`, `SBX-002`, `ING-002`, and `NET-002` complete; R4 complete with `MCP-002`, `MCP-003`, and `MCP-004` complete**

Baseline: branch `docs/audit-remediation-plan`, commit `d4b2e94`

This is the forward-looking corrective roadmap produced from the implementation
audit of [`AUDIT_REMEDIATION_PLAN.md`](AUDIT_REMEDIATION_PLAN.md). The earlier
plan remains the historical record of the intended work, but its M3–M9 status
claims are not release evidence. Acceptance now comes from the regression tests,
runtime behavior, migration rehearsals, and exit gates below.

## Audit verdict and release posture

The implementation is **not ready for sign-off**. Useful controls exist, but
several are incomplete at the point where the security or durability property
must actually hold:

- persistent child identity can collide across parent agents and channels;
- transport acknowledgement, durable ingress, replay, steering, and settlement
  do not yet form one crash-consistent protocol;
- authorization witnesses, approval tickets, and delegation grants do not bind
  every decision to the complete plan and principal;
- rooted filesystem, native sandbox, and MCP controls are present but are not
  wired end to end;
- failed provider attempts, output-policy fallbacks, purge evidence, and remote
  approval resolution are not fully reconstructible from durable records; and
- the shipped policy, installer, build scripts, and current lint baseline do not
  meet the claimed release gate.

Do not publish or promote a stable release while any P0 or P1 finding in the
coverage table remains open. The final release candidate also closes every P2
listed here; deferral requires a new accepted ADR that states the residual risk,
owner, expiry, and compensating control.

## Current verification baseline

The audit established this starting point:

- Pass: `cargo fmt --all -- --check` and
  `cargo check --workspace --locked`.
- Pass: import boundaries, module size, catalog freshness, provider-call check,
  and release-surface check.
- Fail: `cargo clippy --workspace --all-targets --locked -- -D warnings` at
  `crates/agentos-core/src/channels/admission.rs`.
- Fail on native macOS execution: two sandbox regressions —
  `workspace_write` cannot write inside its workspace, and a missing workspace
  root does not fail setup.
- Pass outside the managed test sandbox: the audited egress and MCP integration
  suites. They remain subject to the new end-to-end regressions below.
- Timing-sensitive: the exact gateway lifecycle test passes outside the managed
  sandbox but exposes a startup window between publishing the control lock and
  installing the shutdown handler.

## Delivery rules

These rules apply to every slice:

1. Land one reviewable invariant at a time. Do not combine a persistence
   migration, policy-semantics change, and unrelated feature work in one PR.
2. Add the regression that reproduces the finding in the same PR as the fix.
   A status moves from `Open` only when the test and implementation are
   both present and the named exit gate passes.
3. Persistence changes include versioned forward migration, crash-point tests,
   rollback rehearsal, and explicit behavior for ambiguous legacy records.
   Never infer an identity that was not stored.
4. Security failures are fail-closed and produce a bounded safety event. A
   logging failure is not permission to continue.
5. Keep new behavior in new modules where needed. Do not further grow the
   existing oversized runner, orchestrator, or gateway entry-point files.
6. Use `--locked` for every Cargo command that contributes to an artifact or a
   gate. Public interface/proto changes run blocking semver checks against the
   merge base.
7. Every platform claim is proved on that platform. Landlock evidence comes
   from Linux; Seatbelt evidence comes from macOS; neither may skip on a
   supported runner.

Effort labels are relative engineering size, not calendar commitments: S is up
to three focused days, M is roughly three to seven, L is one to two weeks, and
XL should be split before implementation.

## Dependency and parallelization plan

| Phase | Track | Depends on | Parallel work | Effort |
|---|---|---|---|---:|
| R0 | Re-establish a trustworthy baseline | — | None; land first | S |
| R1 | Principal and authorization integrity | R0 | Rooted I/O primitives and durable ingress | L |
| R2 | Durable ingress, approvals, and gateway authority | R0; only approval recovery waits for R1 | R1, R3, and MCP request cancellation | XL |
| R3 | Filesystem, process, sandbox, and input containment | R0 | R1; primitives before consumers | XL |
| R4 | MCP isolation and lifecycle | R0; native/process proof waits for R3, final drain for R2 | R1 and R2 schema work | L |
| R5 | Request, output, and audit completeness | R1 actor model; R2 delivery states | Late R3/R4 tests | L |
| R6 | Installer, deterministic release inputs, and acceptance | R0–R5 | Documentation preparation only | M |

There are three joining critical paths:

- `R0 → GW-002 → GW-004 → STATE-002 → R6` for crash consistency;
- `R0 → R1 → STATE-002/R5 → R6` for identity and authorization; and
- `R0 → PROC-002 → MCP-002/MCP-004 → R6` for native containment.

`MCP-003` and most of `MCP-002` can start directly after R0. Only their native
proof waits for `PROC-002`, while final MCP drain wiring waits for the process-wide
runtime and shutdown supervisor in R2.

---

## R0 — Re-establish a trustworthy baseline

Goal: make the branch green enough that later failures are meaningful, apply the
one immediately exploitable shipped-policy correction, and establish a stable
finding-to-test register.

### `BASE-001` — Restore the blocking local gate

- Priority: P1.
- Files: `crates/agentos-core/src/channels/admission.rs`, CI only if the local
  and required commands differ.
- Deliver:
  - fix the current Clippy error without weakening or suppressing the lint;
  - make the documented local command identical to the required CI command;
  - record toolchain and platform in failure artifacts.
- Verify: `cargo fmt --all -- --check`, `cargo check --workspace --locked`, and
  `cargo clippy --workspace --all-targets --locked -- -D warnings`.
- Exit: all three commands pass from a clean checkout on Linux and macOS.
- Effort: S.

### `CFG-002` — Remove implicit cron mutation authority

- Priority: P1.
- Files: `workspace/agent.toml`,
  `crates/agentos-core/src/runtime/tools_config.rs`,
  `crates/agentos-core/tests/shipped_config_policy.rs`.
- Deliver:
  - remove `cron_create` and `cron_remove` from the blanket shipped allowlist;
  - keep read-only cron/job inspection non-interactive where appropriate;
  - require `AskUser` or an exact principal-bound grant for persistent or
    cross-conversation mutation;
  - add side-effect/persistence-scope metadata to `ToolSpec`, generate it into the
    catalog, and make policy construction reject blanket `Allow` for that class;
  - make the structural test enumerate metadata, not a hand-maintained tool list.
- Verify: shipped-config policy tests exercise real calls and their arguments,
  not only rule names.
- Exit: the default configuration cannot return unconditional `Allow` for a
  persistent or cross-conversation mutation, including a newly registered tool.
- Effort: M.

### `TEST-002` — Ratchet the audit finding register

- Priority: P1.
- Files: focused unit/integration tests named by R1–R6; this document's coverage
  table.
- Deliver:
  - give every finding a stable identifier and a named regression test;
  - add a CI check that fails when a roadmap finding marked complete lacks its
    referenced test artifact;
  - keep environment-induced failures distinct from native-platform failures.
- Exit: no finding can disappear from the work queue through a documentation-only
  status change.
- Effort: S.

---

## R1 — Principal and authorization integrity

Goal: no persistent state or authorized action can be selected using an
incomplete principal, incomplete plan, ambiguous ticket, or unscoped grant.

### `ID-003` — Collision-free subagent principals

- Priority: P0.
- Files: `crates/agentos-core/src/subagents/branch.rs`,
  `crates/agentos-core/src/subagents/mod.rs`, principal persistence/migration
  code, and session/memory integration tests.
- Deliver:
  - derive a child conversation from the canonical senderless parent
    conversation principal — agent, channel, and conversation — plus the child
    identity, policy/definition identity, and stable task discriminator;
  - prefer a versioned injective length-prefixed encoding; if a fixed-width
    identifier is required, use a named domain-separated cryptographic digest,
    persist the source tuple, and reject a detected collision;
  - expose distinct constructors/types for senderless conversation identity and
    sender-qualified actor identity so authorization cannot accidentally reuse
    the session namespace helper;
  - rekey unambiguous legacy records and quarantine/report ambiguous records.
- Verify:
  - identical conversation IDs on Telegram and Feishu produce different child
    principals;
  - identical channel/conversation IDs under two parent agents remain isolated;
  - two policies registered under the same child agent ID remain isolated;
  - repeat delegation of the same task is stable;
  - transcript and memory written by one source principal are invisible to the
    others.
- Exit: no bare parent conversation ID is sufficient to derive persistent child
  state, and no legacy collision is silently merged.
- Effort: M.

### `AUTH-005` — Authorize an immutable, complete plan

- Priority: P1.
- Files: `crates/agentos-core/src/loop/authorization.rs`, loop plan/act modules,
  `crates/agentos-core/tests/loop_transitions.rs`.
- Deliver:
  - replace partial hand-maintained fingerprints with a canonical commitment to
    every security-relevant field of every `Plan` variant;
  - make the plan inside `ActCtx` private and immutable, or introduce a private
    `AuthorizedPlan` consumed by `act()`;
  - keep the live-policy re-decision immediately before action execution.
- Verify: mutation tests change each field of `CallTool`, `Delegate`, `Escalate`,
  `Handoff`, and `ResumeSubAgent`; every altered plan is rejected by the original
  witness. A hand-built act context remains a compile-fail test.
- Exit: possession of a genuine authorization cannot authorize a different
  action, payload, route, child, policy, or resume state.
- Effort: M.

### `AUTH-003` — Unique tickets and actor-bound approval resolution

- Priority: P1.
- Status: Complete (2026-08-23).
- Files: `crates/agentos-core/src/loop/approval_route.rs`, approval/run-state
  modules, `crates/agentos-core/src/runner.rs`, runner audit helpers, gateway
  approval routing, approval and safety-event tests.
- Deliver:
  - mint one approval-instance ID before emitting `ApprovalRequested`, place it
    on the interruption and every requested/resolved event, and use it as or bind
    it one-to-one with the channel ticket;
  - mint tickets from a once-generated 128-bit OS-CSPRNG nonce plus a checked
    atomic counter, within channel callback limits; entropy failure or counter
    exhaustion fails ticket creation;
  - bind a private resume witness to approval instance, ticket, interruption,
    prompting principal, resolver principal, expiry, and outcome;
  - mutate interruptions by exact pending ID/ticket, never by an oldest-match
    scan that can select a consumed entry;
  - replace bare sender-ID administrator entries with principal-qualified
    selectors; reject ambiguous legacy entries;
  - record who was asked and who answered, including an authorized administrator.
- Verify: an injectable nonce source and synchronized first-initialization test
  deterministically exercise the old collision; ticket serialization round-trips;
  entropy failure and counter exhaustion fail closed; wrong ticket, principal,
  cross-channel administrator, or consumed interruption fails; authorized admin
  resolution identifies both actors.
- Exit: no public resume path accepts a bare interruption ID and decision, and
  requested/resolved events correlate by stable identifiers and principals.
- Effort: M.

### `AUTH-004` — Principal-bound delegation grants

- Priority: P1.
- Files: `crates/agentos-core/src/approve/mod.rs`, subagent config/runtime/loop
  modules, narrowing property tests, and safety-event tests.
- Deliver:
  - separate configured grant templates from runtime grant instances;
  - bind every runtime grant to the sender-qualified initiating parent actor,
    delegatee, tool, optional exact arguments, mandatory `expires_at`, and stable
    grant ID;
  - cap expiry by a named maximum lifetime and define wall-clock rollback/forward
    behavior; a standing grant requires a replacement ADR rather than
    `expires_at = None`;
  - validate expiry at delegation start and at each covered action;
  - reject unscoped legacy configuration with an actionable migration error.
- Verify: grants do not cross two users in one group, channels, parent agents,
  arguments, expiry, clock changes, or delegation generations; grants remain
  non-transitive; issuance and use events share the grant ID.
- Exit: a grant cannot exist or be used without exact principal and action scope.
- Effort: M.

### `AUD-002` — Make required safety evidence a first-class result

- Priority: P1.
- Files: safety journal API, loop/runner audit helpers, approval/delegation,
  policy/guardrail/sandbox/cancellation paths, and database failpoint tests.
- Deliver:
  - make safety-event append fallible and handle its result at every required
    event site; no append result may be discarded;
  - persist approval-requested before exposing a prompt, and approval-resolved,
    grant-issued, and grant-used before the protected transition proceeds;
  - on an evidence failure before a protected action, stop the affected run
    without executing; when the decision already denies/cancels/refuses, preserve
    that safer outcome, surface a typed operational failure, and admit no further
    mutable work until the journal is healthy.
- Verify: inject append failure at approval request/resolution and grant
  issuance/use, plus representative policy denial, sandbox refusal, cancellation,
  and terminal error sites; assert no protected action runs and no event failure
  is converted into logging-and-continue.
- Exit: required evidence is either durable or the runtime enters an explicit
  fail-closed operational state; it is never an ignored best-effort append.
- Effort: M.

R1 release gate: run all approval, loop-transition, runner-narrowing,
policy-property, session-fork, memory-authority, migration, and safety-event tests,
plus blocking semver checks for changed public types.

---

## R2 — Durable ingress, approvals, and gateway authority

Goal: an accepted transport event is never silently lost, executed without a
durable identity, settled under another event's identity, or blindly re-executed
after an ambiguous crash.

Implementation order inside the phase is `GW-002 → GW-003/GW-004 → STATE-002`.
`GW-005` can start once the ingress ownership API stabilizes; `MAINT-002` and
`CTRL-002` then build on its process supervisor.

### `GW-002` — Make durable admission the transport checkpoint

- Priority: P0.
- Files: `crates/agentos-interfaces/src/channel.rs`, channel adapters,
  `crates/agentos-core/src/gateway/ingress.rs`, gateway main, SQLite migration,
  and channel pipeline fixtures.
- Deliver:
  - make receive return a bounded envelope plus an event-bound ACK/checkpoint
    receipt; adapters do not ACK or mutate a global cursor while parsing;
  - persist a replayable envelope and checkpoint before transport
    acknowledgement, then route it;
  - move Feishu ACK out of `long_connection.rs`; make Telegram return the
    update-specific offset instead of advancing it during receive;
  - fail closed on ledger or checkpoint errors;
  - drive routing from a continuous durable dispatcher that retries accepted rows
    during normal operation and startup while preserving per-conversation order;
  - send every transport input, including `/stop`, through the ledger; an
    `AlreadySettled` redelivery still ACKs/advances its event-bound checkpoint
    before suppression;
  - migrate legacy unsettled rows by quarantining/reporting them and refusing to
    serve until operator adjudication; never synthesize missing envelopes, drop
    rows, or resume a cursor already beyond unrecoverable content.
- Required order: `persist → ACK/checkpoint → route`. ACK need not wait for a
  complete turn, but it must never get ahead of recoverable state.
- Verify: crash tests at before persist, after persist/before ACK, after
  ACK/before route, mid-turn, after terminal result persistence, and after
  settlement; forced SQLite write failure; Feishu and Telegram mock transport
  ordering; queue saturation without restart; `/stop` ordering; a pre-roadmap
  database with an advanced cursor and non-replayable unsettled row.
- Exit: every acknowledged event is recoverable, a ledger failure causes no ACK,
  cursor movement, or execution, accepted rows cannot strand until restart, and
  settled replay is suppressed only after its transport checkpoint is handled.
- Interface note: this changes `agentos-interfaces`; run the blocking merge-base
  semver check and version the crate if the contract is breaking.
- Effort: L.

### `GW-003` — Preserve complete envelope identity through steering

- Priority: P1.
- Depends on: `GW-002`.
- Files: `crates/agentos-core/src/gateway/inbox.rs`, gateway shard/router code,
  and steering/admission tests.
- Deliver:
  - carry full envelopes, not bare messages, through queueing, steering,
    displacement, and requeue;
  - preserve each event's principal, channel metadata, transport ID, ingress ID,
    and ledger key; the live ACK/checkpoint receipt remains owned and consumed by
    the ingress layer and never enters shard or steering state;
  - return explicit outcomes for full, displaced, refused, and requeued input;
  - settle only the row belonging to the input actually consumed; a displaced
    accepted steer is requeued or durably superseded only after a sender-facing
    terminal outcome.
- Verify: interleave multiple senders and ingress IDs, steer twice, displace an
  input, and saturate a queue; assert exact envelope and settlement identity at
  every step.
- Exit: no metadata is reconstructed from another message and no accepted input
  disappears without a durable outcome and sender-facing disposition.
- Effort: M.

### `GW-004` — Model delivery and settlement as durable states

- Priority: P1.
- Depends on: `GW-002`.
- Files: ingress schema/API, gateway shard/egress path, runner task-session code,
  and crash/egress tests.
- Deliver:
  - define explicit states such as `accepted`, `running`, `paused`,
    `action_started`, `reply_pending`, `delivery_started`, `handled`, `rejected`,
    `action_ambiguous`, and `delivery_ambiguous`;
  - persist `action_started` before invoking a tool and classify an incomplete
    action as ambiguous on restart before any rerun;
  - persist a terminal reply before delivery and use a stable transport
    idempotency/edit key where supported;
  - never mark `Handled` when terminal egress fails;
  - if a crash can fall after transport acceptance but before settlement, require
    reconciliation or a configured recovery decision instead of automatic resend.
- Verify: inject failure at every state transition and egress call; assert no
  handled-without-delivery state and no automatic repeat of an ambiguous action
  or delivery. Include the exact points after tool side effect/before completion
  commit and after transport accepts bytes/before settlement commit.
- Exit: restart resumes the correct stage — route, approval, reply delivery, or
  explicit ambiguity — and never claims exactly-once semantics where the external
  system supplies no idempotency/reconciliation mechanism.
- Effort: L.

### `STATE-002` — Persist restart-safe remote approvals

- Priority: P1.
- Status: Complete (2026-08-26).
- Depends on: `AUTH-003`, `GW-002`, and the action/delivery-state contract in
  `GW-004`.
- Files: gateway shard/main, runner pause/resume code, durable path or database
  store, lifecycle/state-durability/approval tests.
- Deliver:
  - atomically persist a versioned paused run, ticket, interruption, prompting
    principal, original approval scope, expiry, and ingress identity before
    sending the approval prompt;
  - restore live approvals and shard ownership at startup;
  - resolve expired approvals as unanswered; reject corrupt or identity-less
    legacy records;
  - intersect persisted approval scope with current live policy before resolution
    so revocation takes effect and newly added authority is not retroactive;
  - consume each resolution once; after an ambiguous action boundary, never
    claim or attempt automatic exactly-once recovery.
- Verify: pause/terminate/restart/approve consumes the resolution once; wrong
  sender remains `NotYours`; administrator revocation/addition across restart is
  honored conservatively; downtime expiry never executes; crashes immediately
  before and after prompt delivery or resolution enter the correct durable state.
- Exit: every delivered live approval has durable state, restart preserves all
  bindings, one terminal resolution is recorded, and any uncertain protected
  action is marked ambiguous rather than replayed.
- Effort: L.

### `GW-005` — Establish one process-wide runtime authority

- Priority: P1.
- Files: gateway main/shard/supervisor, runtime construction, and multi-channel
  lifecycle/pipeline tests.
- Deliver:
  - build one process-wide `AgentRuntime`, SQLite pool, `JobRegistry`, session
    usage authority, and MCP lifecycle registry;
  - pass per-channel ingress ledgers and egress handles into that shared
    authority instead of constructing one runtime per channel.
- Verify: enable Telegram and Feishu together; assert one configured pool/job
  limit, cross-channel job visibility with correct principal fencing, and one
  runtime resource shutdown.
- Exit: exactly one runtime, database pool, job registry, and lifecycle authority
  exists per serving process.
- Effort: M.

### `MAINT-002` — Drive maintenance independently of traffic

- Priority: P1.
- Depends on: `GW-005`.
- Files: core gateway maintenance, gateway supervisor, retention/reflection/cron
  leases, and deterministic-clock tests.
- Deliver:
  - drive cron, reflection, retention, and ingress sweeping from bounded timers,
    not shard-0 idle time;
  - cover every channel ledger and process-wide job registry under the existing
    lease rules;
  - expose bounded lag/last-run evidence for operational diagnosis.
- Verify: keep shard 0 continuously saturated and assert each maintenance class
  runs once within the configured test interval and never acquires duplicate
  leadership.
- Exit: maintenance has a deterministic upper-bound lag independent of traffic
  and covers every process-owned growing store.
- Effort: M.

### `CTRL-002` — Make control signalling and shutdown race-free

- Priority: P1.
- Status: Complete (2026-08-28). Startup publication, exact-holder control,
  shared-deadline cancellation, bounded async Telegram transport, and retained
  active-MCP group shutdown all use the process-wide drain authority.
- Depends on: `GW-005`; real Telegram/process-backed cancellation also depends on
  `PROC-002`; final MCP drain proof depends on `MCP-004`.
- Files: gateway control/supervisor, channel receive loops, process/runtime
  shutdown, and process-level lifecycle fixtures.
- Deliver:
  - install shutdown handling before publishing the serving lock;
  - make every blocking receive cancellation-aware, stop admission immediately,
    drain to one process-wide grace deadline, and report abandoned ingress IDs;
  - include channel tasks, Tokio/spawn-blocking teardown, maintenance, MCP, and
    shard draining inside that same deadline;
  - replace holder-lookup-then-`kill(pid)` with an authenticated holder-directed
    control socket or kernel-stable process handle; fail safely where neither is
    available, while retaining the held-lock liveness contract.
- Verify: a forever-blocked receiver, wedged turn, active hung MCP call, and two
  channels exit within the grace bound; a barrier-controlled holder exit and
  replacement start can never signal the replacement process.
- Exit: SIGTERM completes within `shutdown_grace_secs` plus scheduler tolerance,
  reports abandoned IDs, releases the lock, and cannot target a reused PID.
- Effort: M–L.

R2 release gate: versioned migration and rollback, the complete crash matrix,
both mock-channel pipelines, remote-approval restart tests, multi-channel
authority/maintenance tests, and real process-level shutdown/control tests pass
on Linux and macOS.

---

## R3 — Filesystem, process, sandbox, and input containment

Goal: every untrusted identifier, write, allocation, network destination, and
child process is constrained by the boundary the runtime claims to enforce.

### `FS-002` — Complete descriptor-relative rooted writes

- Priority: P1.
- Status: Complete (2026-08-27). Finding closure remains in `FS-003`, which
  migrates task, attachment, skill, and other untrusted consumers onto these
  primitives.
- Files: `crates/agentos-core/src/paths/`.
- Deliver: add private descriptor-relative directory creation, create/append,
  temporary publication/rename, and parent-directory fsync; propagate open/fsync
  failures.
- Verify: planted symlink at every component, concurrent symlink swaps,
  permissive umask, and injected fsync failure.
- Exit: no operation alters an outside canary, files/directories are `0600`/`0700`,
  and reported durable success satisfies the fsync contract.
- Effort: M.

### `FS-003` — Migrate every untrusted filesystem consumer

- Priority: P1.
- Status: Complete (2026-08-27). Task/session, attachment, skill, spill, and
  cron persistence now enter retained descriptor roots with validated relative
  names; paused-run and trace paths were audited as runtime/operator-selected
  paths rather than model-selected consumers.
- Depends on: `FS-002`.
- Files: `crates/agentos-core/src/task_workspace.rs`, attachment/channel writers,
  `crates/agentos-core/src/tools/builtin/skill_validate.rs`, and an audit of spill,
  cron, session, and task writers.
- Deliver:
  - store a `RootDir` plus validated relative segments instead of host paths;
  - re-root the special `main`/`min` task layout at a common descriptor, or retain
    separate trusted root handles; do not preserve the current `root.parent()`
    traversal;
  - publish attachment downloads through rooted temporary files;
  - require skill names to be one valid segment and inventory with bounded,
    no-follow traversal;
  - apply private modes to task, session, attachment, and state artifacts.
- Verify: traversal, absolute path, symlink escape/race, `main`/`min` planted
  symlinks and outside canaries, inventory cycle/excessive depth/count, and modes
  under a permissive umask.
- Exit: every escape fails before an outside object is opened and no sensitive
  artifact is group/world accessible.
- Effort: L.

### `PROC-002` / `SBX-002` — One child boundary and correct native enforcement

- Priority: P1.
- Status: Complete (2026-08-28). Shell, isolation workers, and stdio MCP servers
  share one environment-clearing, process-group-owning, sandbox-hardening spawn
  path; Telegram no longer spawns `curl`; Seatbelt grants canonical roots and
  both native backends reject a missing writable root before spawn. CI rejects
  new runtime spawn bypasses.
- Files: tool exec/child-env/isolation modules, macOS sandbox profile, MCP process
  startup, Telegram process use, and a new
  `scripts/check-subprocess-boundaries.sh` ratchet.
- Deliver:
  - provide one child builder/guard that clears the environment, applies the
    allowlist, creates a process group, caps pipes, hardens before spawn, and
    kills/reaps the group on timeout, cancellation, shutdown, and drop;
  - fix Seatbelt `workspace_write` so the declared workspace is writable and a
    missing workspace root fails before spawn;
  - keep unsupported sandbox backends fail-closed;
  - replace Telegram `curl` children with the bounded shared HTTP client for its
    operator-configured endpoint where practical, otherwise route them through
    the common child boundary;
  - reject direct `std::process::Command`/`tokio::process::Command` use outside
    the approved child module and narrowly documented bootstrap binaries.
- Verify: credential canary, child-plus-grandchild cleanup, read-only and
  workspace-write enforcement, nonexistent root, bounded stdout/stderr, and
  unsupported-backend tests on native Linux/macOS.
- Exit: every runtime child uses the common boundary and no descendant survives
  cancellation or gateway shutdown.
- Effort: L.

### `ING-002` / `NET-002` — Bound before allocation and complete address policy

- Priority: P1 for unbounded ingress; P2 for IPv6 site-local coverage.
- Status: Complete (2026-08-28). Tungstenite rejects Feishu frames at a named
  transport ceiling; protobuf lengths, headers, every event shape, fragment
  aggregates, and channel JSON responses are bounded before accumulation.
  Telegram shares the JSON cap without a child process, and literal or resolved
  `fec0::/10` destinations are refused.
- Files: Feishu WebSocket/proto/fragment code, Telegram protocol/HTTP body
  handling, and `crates/agentos-core/src/egress/policy.rs`.
- Deliver:
  - set a named WebSocket frame ceiling and reject oversized payloads before
    cloning or allocating from declared lengths;
  - apply the event-byte limit to unfragmented, missing-sum, malformed-sum, and
    fragmented Feishu input;
  - bound channel HTTP response bodies before accumulation; generic child-pipe
    caps belong to `PROC-002`, skill traversal to `FS-003`, and MCP stderr to
    `MCP-004`;
  - reject deprecated IPv6 site-local `fec0::/10` alongside other non-public
    destination classes.
- Verify: oversized single frame/event/HTTP response, malformed fragment
  metadata, aggregate overflow, and literal/DNS-resolved IPv6 site-local targets.
- Exit: adversarial input is rejected at the first framing boundary, before an
  allocation proportional to attacker-controlled size, and no prohibited address
  can be reached by representation changes.
- Effort: M.

R3 release gate: all rooted-I/O, attachment, skill, task/session, egress, channel,
subprocess, and native sandbox regressions pass. The platform jobs must report the
actual Landlock/Seatbelt mechanism and fail rather than skip.

---

## R4 — MCP isolation and lifecycle

Goal: a sandboxed MCP server starts under the declared native boundary, remains
bounded under failure and cancellation, and leaves no process or pending slot
behind at shutdown.

### `MCP-002` — Bind tool registration to the server sandbox witness

- Priority: P1.
- Status: Complete (2026-08-28). Dynamic stdio tools register as self-hardened
  only through the retained client that constructs their server sandbox.
- Depends on: R0 for implementation; `PROC-002` only for native end-to-end proof.
- Files: MCP protocol/tool modules, tool registry, runtime isolation wiring, and
  runtime startup integration tests.
- Deliver:
  - expose MCP tools as `SelfHardened` only when the retained connection carries
    the matching server-sandbox witness;
  - do not route a dynamic MCP tool through a generic executor that cannot invoke
    it;
  - reject startup if the declared server boundary cannot be established.
- Verify: construct a full runtime with a sandboxed MCP server and no generic
  worker; list and call a tool successfully inside the boundary; force an
  unsupported backend and assert startup refusal.
- Exit: sandboxed MCP registration and invocation work end to end without
  weakening the declared isolation mode.
- Effort: M.

### `MCP-003` — Make requests cancellation- and drop-safe

- Priority: P1.
- Status: Complete (2026-08-28). A drop guard releases pending IDs and the
  bounded connection writer orders one cancellation notice per abandoned call.
- Files: `crates/agentos-core/src/tools/mcp/connection.rs`, loop tool-call
  cancellation, and MCP interoperability fixtures.
- Deliver:
  - introduce an RAII pending-request guard that removes its ID on every return,
    cancellation, or dropped future;
  - route drop/outer cancellation through the connection actor so it sends
    exactly one `notifications/cancelled` when this side abandons a call;
  - make late replies harmless and preserve request-ID correlation.
- Verify: set the outer loop/registry deadline shorter than the MCP timeout,
  cancel more than the configured in-flight limit against a server that never
  replies, and have the independent fixture observe exactly one cancellation per
  request before a later normal call succeeds. Exercise late replies and
  connection death.
- Exit: cancellation never leaks capacity or shifts a later response onto the
  wrong request.
- Effort: M.

### `MCP-004` — Bound process, restart, stderr, and shutdown lifecycle

- Priority: P1.
- Status: Complete (2026-08-28). Runtime-retained clients shut down active
  process groups by the gateway deadline; failed opens, stderr, and tool pages
  consume their named bounds before further work or allocation.
- Depends on: `PROC-002` and `GW-005`; acceptance joins `CTRL-002` for final
  deadline/drain integration.
- Files: MCP connection/process modules and runtime lifecycle registry.
- Deliver:
  - use the common process-group guard and retain MCP clients as process-wide
    runtime resources;
  - retain a shutdown/group-kill handle that does not depend on unique `Arc`
    ownership and remains effective while active calls hold connection references;
  - gracefully shut them down during gateway drain, then kill/reap the group at
    the deadline;
  - consume restart budget on every post-crash open attempt, including failed
    opens;
  - replace line-based stderr collection with fixed-size chunk/ring-buffer logic;
  - check each `tools/list` page against the remaining cumulative tool budget
    before `Vec::with_capacity`, schema cloning, or other page-sized allocation.
- Verify: failed-open restart loop, no-newline stderr flood, forked descendant,
  compact oversized tools array, cancellation storm, and gateway shutdown while
  an MCP call never replies; assert the server and grandchild are reaped before
  the grace deadline.
- Exit: restart attempts stop at the configured ceiling, stderr stays bounded,
  and no MCP child or grandchild survives runtime shutdown.
- Effort: M.

R4 release gate: three suites pass on Linux and macOS: the independent protocol
fixture (interoperability, pagination, out-of-order response, cancellation and
bounds), a full-runtime sandboxed registration/invocation suite without a generic
worker, and a process-level gateway shutdown suite with active MCP work. Native
jobs fail rather than skip when Landlock/Seatbelt is unavailable.

---

## R5 — Request, output, and audit completeness

Goal: every provider attempt and safety-sensitive outcome is durably attributable,
and no fallback path bypasses the policy it is meant to enforce.

### `REQ-002` — Record one manifest per provider attempt

- Priority: P1.
- Files: prompt request builder/gateway, loop/orchestrator/compaction call sites,
  provider-call enforcement script, request-header persistence, and golden tests.
- Deliver:
  - persist a request header before each provider invocation and append its
    success/failure/usage outcome afterward;
  - fail closed before provider I/O if the pre-attempt manifest cannot be
    persisted;
  - assign a stable logical request ID and a unique attempt number so a streaming
    retry produces two attempt records;
  - retain failed turn, routing, and compaction attempts;
  - replace grep-only enforcement with compile-time API/module visibility where
    possible, otherwise an AST-aware check that covers UFCS and multiline calls.
- Verify: provider error before response, compaction failure, streaming retry,
  cancellation, and successful call all produce the expected ordered manifests;
  a journal-write failpoint proves no request leaves without its pre-attempt
  record; negative fixtures prove direct provider calls fail the gate.
- Exit: the durable record can answer what was sent, why, which attempt ran, and
  how it ended for every provider invocation.
- Effort: L.

### `OUT-001` — Keep every terminal byte behind output policy

- Priority: P1.
- Files: `crates/agentos-core/src/loop/output.rs`, terminal error/cancellation
  paths, and output-policy tests.
- Deliver:
  - run any fallback/refusal through the same output gate;
  - when the policy rejects both the original and fallback, emit no channel bytes
    and record a bounded safety outcome;
  - preserve provisional-streaming's explicit, default-off exception without
    creating another terminal bypass.
- Verify: a deny-all output policy suppresses replies, cancellation notices,
  budget notices, and fallback text; allow/refusal policies emit only their
  authorized result.
- Exit: there is one terminal output authority and no synthesized message is
  self-exempting.
- Effort: S–M.

### `AUD-003` — Make purge evidence transactional

- Priority: P1.
- Files: `crates/agentos-core/src/audit/purge.rs`, session purge/store code, CLI
  purge flow, and SQLite failpoint tests.
- Deliver:
  - pass the operator-confirmed expected count into the transaction, re-count
    under that transaction, and abort on mismatch before deletion;
  - delete and insert `audit_purged`/`session_purged` in that same transaction
    through a fallible journal API;
  - return failure and roll back deletion if marker insertion fails;
  - retain count-confirmation and principal/reason fields.
- Verify: race the report/apply boundary and force marker failure; both cases
  leave row counts and events unchanged. A successful purge commits the confirmed
  deletion plus exactly one marker.
- Exit: deletion can never commit without its durable safety evidence.
- Effort: M.

R5 release gate: request golden artifacts include success and failure attempts;
output deny-all tests pass; approval-resolution and purge events identify their
actors and correlate to their initiating records; all safety-event fields remain
bounded and argument-redacted.

---

## R6 — Installer, deterministic release inputs, and acceptance

Goal: upgrades preserve operator state, every artifact resolves from the
committed lockfile and exact reviewed toolchain/tool versions, and every
corrective regression is a required release gate. This is an input-reproducibility
claim, not yet a claim that Rust binaries or archives are bit-for-bit reproducible.

### `REL-003` — Make install and upgrade non-destructive

- Priority: P1.
- Files: `scripts/install-agentos.sh`, release archive rehearsal, installer docs.
- Deliver:
  - separate immutable distribution assets from the mutable operator workspace;
  - seed `agent.toml`, skills, subagents, and sub-orchestrators only when absent,
    or install versioned `.dist` defaults for explicit merge;
  - require an explicit replace/reset option and create a recoverable backup;
  - exercise the same rules for source and archive installs.
- Verify: install, modify config, add a custom skill/sentinel, reinstall and
  upgrade; operator files remain byte-for-byte unchanged while new defaults are
  available for review.
- Exit: ordinary install/reinstall/upgrade never deletes or overwrites operator
  workspace content.
- Effort: M.

### `REL-004` — Lock release dependency and tool resolution

- Priority: P2, release blocking.
- Files: `scripts/package-release.sh`, `scripts/install-agentos.sh`,
  `scripts/check-catalogs.sh`, remaining scripted Cargo invocations, CI toolchain
  setup, and an exact `rust-toolchain.toml`.
- Deliver:
  - add `--locked` to every artifact-producing or catalog Cargo command;
  - pin one exact Rust toolchain and make local scripts and CI consume/report it;
  - pin exact reviewed `cargo-deny` and `cargo-semver-checks` versions rather than
    resolving floating tool names;
  - add a static gate for unlocked release-critical commands and toolchain drift;
  - keep third-party actions pinned by immutable digest.
- Verify: two clean-checkout builds emit the same dependency/toolchain/tool input
  manifest; a negative fixture with an unlocked command, changed lockfile, or
  floating tool version fails.
- Exit: no moving `stable` branch or unlocked Cargo resolution can produce a
  release artifact.
- Effort: S–M.

### `CI-003` — Promote the corrective matrix to one required check

- Priority: P1.
- Depends on: all previous slices.
- Files: `.github/workflows/ci.yml`, `.github/workflows/nightly.yml`, test/release
  scripts, capability matrix, observability and release notes.
- Deliver:
  - add the R1–R5 targeted suites, crash matrix, reinstall preservation, and
    native sandbox/MCP tests to required CI;
  - provide executable gates named
    `scripts/check-audit-regressions.sh`,
    `scripts/check-gateway-crash-matrix.sh`,
    `scripts/check-install-preservation.sh`,
    `scripts/check-release-locking.sh`, and
    `scripts/check-subprocess-boundaries.sh`;
  - keep one aggregate `ci` job that depends on every required job;
  - retain long property/Miri/loop-budget runs nightly, but do not leave a
    release-blocking regression nightly-only;
  - update capability status only after the corresponding exit gate passes.
- Exit: Linux and macOS required CI, clean-room release/install/upgrade/rollback,
  both gateway pipelines, dependencies, semver, catalogs, and release surface are
  all green from a clean checkout.
- Effort: M.

---

## Audit finding coverage

`Regression artifact` is the exact test or executable gate that must exist before
the row can move to `Complete`. `Execution` separates portable evidence from
claims that are meaningful only on native Linux or macOS runners; a managed-host
failure is recorded as environment-induced and never substitutes for a native
platform result. `scripts/check-audit-regressions.sh` enforces the schema, stable
ID sequence, and complete-row artifact resolution.

| ID | Finding | Priority | Corrective slice | Acceptance evidence | Regression artifact | Execution | Status |
|---|---|---:|---|---|---|---|---|
| `AF-001` | Child session namespace omits parent channel/agent/policy | P0 | `ID-003` | Cross-channel, cross-agent, cross-policy fork/memory isolation | `crates/agentos-core/tests/subagent_identity.rs::colliding_legacy_sources_do_not_share_transcript_or_memory` | `portable` | Complete |
| `AF-002` | Feishu ACK precedes durable ingress | P0 | `GW-002` | ACK-order crash fixture | `crates/agentos-core/tests/gateway_ingress_crash.rs::feishu_ack_follows_durable_ingress_commit` | `native-linux+macos` | Complete |
| `AF-003` | Telegram checkpoint advances without recoverable replay | P0 | `GW-002` | Cursor/restart/replay fixture | `crates/agentos-core/tests/gateway_ingress_crash.rs::telegram_cursor_never_advances_past_unrecoverable_input` | `native-linux+macos` | Complete |
| `AF-004` | Ingress ledger error logs and continues | P0 | `GW-002` | SQLite failpoint proves no ACK or execution | `crates/agentos-core/tests/gateway_ingress_crash.rs::ledger_failure_prevents_ack_and_execution` | `portable` | Complete |
| `AF-005` | ACKed rows can strand until restart; `/stop` bypasses admission | P0 | `GW-002` | Live dispatcher, saturation, and `/stop` ordering | `crates/agentos-core/tests/gateway_ingress_crash.rs::live_dispatcher_drains_acked_rows_and_stop_obeys_admission` | `native-linux+macos` | Complete |
| `AF-006` | Legacy unsettled rows lack replay payload behind an advanced cursor | P0 | `GW-002` | Pre-roadmap migration quarantine fixture | `crates/agentos-core/tests/gateway_ingress_crash.rs::legacy_unsettled_rows_are_quarantined_before_cursor_advance` | `portable` | Complete |
| `AF-007` | Steering/requeue loses envelope metadata and ingress ID | P1 | `GW-003` | Multi-sender steering/displacement | `crates/agentos-core/tests/gateway_steering.rs::displaced_envelope_preserves_principal_and_ingress_id` | `portable` | Complete |
| `AF-008` | Remote paused approvals exist only in memory | P1 | `STATE-002` | Pause/restart/policy-change/resolve matrix | `crates/agentos-core/tests/approval_recovery.rs::paused_approval_survives_restart_and_policy_change` | `native-linux+macos` | Complete |
| `AF-009` | Authorization commitment omits plan fields; plan is swappable | P1 | `AUTH-005` | Per-field mutation and compile-fail tests | `crates/agentos-core/src/loop/authorization.rs::mutating_any_plan_field_invalidates_authorization` | `portable` | Complete |
| `AF-010` | Public resume path accepts bare interruption/decision identifiers | P1 | `AUTH-003` | Opaque actor-bound witness negative tests | `crates/agentos-core/tests/approval_witness.rs::resume_rejects_bare_or_wrong_actor_witness` | `portable` | Complete |
| `AF-011` | Approval ticket initialization can collide and lacks one prompt ID | P1 | `AUTH-003` | Deterministic synchronized initialization test | `crates/agentos-core/src/loop/approval_route.rs::synchronized_prompts_receive_distinct_ids` | `portable` | Complete |
| `AF-012` | Interruption resolution can select an old non-pending entry | P1 | `AUTH-003` | Exact pending-instance lifecycle | `crates/agentos-core/tests/approval_witness.rs::resolution_selects_only_the_exact_pending_instance` | `portable` | Complete |
| `AF-013` | Bare administrator sender IDs can cross channel namespaces | P1 | `AUTH-003` | Telegram/Feishu selector and migration test | `crates/agentos-core/tests/channel_admin_identity.rs::same_sender_id_on_two_channels_is_not_one_administrator` | `portable` | Complete |
| `AF-014` | Delegation grants are not actor-bound or mandatorily expiring | P1 | `AUTH-004` | Actor/channel/agent/expiry/non-transitive matrix | `crates/agentos-core/tests/delegation_grants.rs::grant_is_actor_bound_expiring_and_non_transitive` | `portable` | Complete |
| `AF-015` | Approval resolution audit omits the resolver | P1 | `AUTH-003` | Requested/resolved actor correlation | `crates/agentos-core/tests/safety_events.rs::approval_resolution_records_requester_and_resolver` | `portable` | Complete |
| `AF-016` | Best-effort journal writes can leave required safety evidence absent | P1 | `AUD-002` | Approval/grant/denial/refusal append failpoints | `crates/agentos-core/tests/audit_failpoints.rs::required_safety_event_failure_aborts_the_action` | `portable` | Complete |
| `AF-017` | Failed/retried provider calls lose attempt manifests | P1 | `REQ-002` | Failure/retry/compaction/cancellation goldens | `crates/agentos-core/tests/request_attempts.rs::every_failed_retried_or_cancelled_attempt_is_durable` | `portable` | Open |
| `AF-018` | Task/attachment writes and skill traversal bypass rooted I/O | P1 | `FS-002`, `FS-003` | Symlink/race/main-min/outside-canary matrix | `crates/agentos-core/tests/rooted_io.rs::all_model_selected_paths_refuse_symlink_escape_and_swap` | `native-linux+macos` | Complete |
| `AF-019` | Task/session modes rely on umask; fsync errors are ignored | P2 | `FS-002`, `FS-003` | Mode and injected-fsync tests | `crates/agentos-core/tests/state_durability.rs::fsync_failure_aborts_state_commit` | `native-linux+macos` | Complete |
| `AF-020` | Native macOS workspace-write/setup semantics are wrong | P1 | `SBX-002` | Enforcing Seatbelt tests on macOS | `crates/agentos-core/tests/sandbox.rs::a_workspace_write_child_writes_inside_and_not_outside`, `crates/agentos-core/tests/sandbox.rs::an_unbuildable_sandbox_fails_the_call` | `native-macos` | Complete |
| `AF-021` | Sandboxed MCP tools cannot register/invoke correctly | P1 | `MCP-002` | Full-runtime sandboxed MCP call | `crates/agentos-core/tests/mcp_runtime.rs::sandboxed_mcp_tool_registers_and_runs_through_runtime` | `native-linux+macos` | Complete |
| `AF-022` | MCP cancellation leaks pending slots/notification | P1 | `MCP-003` | Observed cancellation storm beyond limit | `crates/agentos-core/tests/mcp_interop.rs::cancellation_storm_releases_slots_and_notifies_server` | `native-linux+macos` | Complete |
| `AF-023` | MCP group shutdown, stderr, and restart accounting are incomplete | P1 | `MCP-004` | Descendant/flood/failed-open/active-drain tests | `crates/agentos-core/tests/mcp_process_lifecycle.rs::shutdown_drains_active_calls_and_kills_the_process_group` | `native-linux+macos` | Complete |
| `AF-024` | MCP tool-page count sizes allocation before the cumulative cap | P1 | `MCP-004` | Oversized compact `tools/list` page | `crates/agentos-core/tests/mcp_interop.rs::oversized_compact_tool_page_is_refused_before_allocation` | `portable` | Complete |
| `AF-025` | Gateway lock is visible before shutdown handling is ready | P1 | `CTRL-002` | Process-level startup/SIGTERM barrier | `crates/agentos-cli/tests/gateway_lifecycle.rs::startup_lock_is_published_after_signal_handlers_are_ready` | `native-linux+macos` | Complete |
| `AF-026` | Channel receive can block shutdown indefinitely | P1 | `CTRL-002`, `PROC-002` | Forever-blocked receiver | `crates/agentos-core/tests/gateway_shutdown.rs::forever_blocked_channel_is_abandoned_at_shutdown_deadline` | `native-linux+macos` | Complete |
| `AF-027` | Numeric PID holder lookup can signal a replacement process | P1 | `CTRL-002` | Barrier-controlled PID-reuse regression | `crates/agentos-cli/tests/gateway_lifecycle.rs::stop_never_signals_a_reused_pid` | `native-linux+macos` | Complete |
| `AF-028` | Channels construct independent runtime authorities | P1 | `GW-005` | Two-channel shared-authority test | `crates/agentos-core/tests/runtime_authority.rs::two_channels_share_one_runtime_authority` | `native-linux+macos` | Complete |
| `AF-029` | Maintenance depends on shard-0 idle time | P1 | `MAINT-002` | Saturated-shard deterministic-clock test | `crates/agentos-core/tests/maintenance.rs::maintenance_runs_while_shard_zero_is_saturated` | `portable` | Complete |
| `AF-030` | Failed/ambiguous egress is settled as handled or blindly retried | P1 | `GW-004` | Delivery/action crash-state matrix | `crates/agentos-core/tests/delivery_crash.rs::ambiguous_delivery_is_neither_silently_settled_nor_blindly_retried` | `native-linux+macos` | Complete |
| `AF-031` | Shipped cron mutations are blanket-allowed | P1 | `CFG-002` | Metadata-driven mutation policy ratchet | `crates/agentos-core/tests/shipped_config_policy.rs::metadata_drives_the_blanket_allow_ratchet_for_every_shipped_tool` | `portable` | Open |
| `AF-032` | Reinstall deletes operator configuration and extensions | P1 | `REL-003` | Modify-and-reinstall preservation | `scripts/check-install-preservation.sh` | `native-linux+macos` | Open |
| `AF-033` | Purge commits before best-effort audit marker/count recheck | P1 | `AUD-003` | Transaction count-race/failpoint rollback | `crates/agentos-core/tests/audit_purge.rs::purge_count_race_or_marker_failure_rolls_back_everything` | `portable` | Open |
| `AF-034` | Feishu single-part and other protocol bodies allocate before caps | P1 | `ING-002` | Oversize/malformed input matrix | `crates/agentos-core/tests/ingress_limits.rs::all_protocol_bodies_are_bounded_before_allocation` | `portable` | Complete |
| `AF-035` | Terminal fallback bypasses output policy | P1 | `OUT-001` | Deny-all terminal-path matrix | `crates/agentos-core/tests/output_gate.rs::every_terminal_fallback_crosses_the_output_policy` | `portable` | Open |
| `AF-036` | Provider-call checker is grep-bypassable | P2 | `REQ-002`, `CI-003` | Compile-time or AST negative fixture | `tests/test_provider_call_checker.py::test_syntactic_bypasses_are_rejected` | `portable` | Open |
| `AF-037` | Release/catalog/source builds are unlocked; tool versions float | P2 | `REL-004` | Static gate and clean input-manifest test | `scripts/check-release-locking.sh` | `native-linux+macos` | Open |
| `AF-038` | IPv6 site-local egress remains allowed | P2 | `NET-002` | Literal/DNS-resolved policy tests | `crates/agentos-core/tests/egress.rs::ipv6_site_local_is_refused_by_literal_and_resolution` | `portable` | Complete |
| `AF-039` | Telegram child bypasses common env/group/output controls | P2 | `PROC-002`, `ING-002` | No-child source ratchet and shared response cap | `crates/agentos-core/tests/subprocess_boundaries.rs::telegram_transport_has_no_child_and_uses_bounded_http` | `portable` | Complete |
| `AF-040` | Current all-target Clippy gate fails | P1 | `BASE-001` | Required command green on both platforms | `scripts/check-baseline-gates.sh` | `native-linux+macos` | Open |

## Final release gate

Once the named slices have added their new gate scripts, run the following from
a clean checkout with the pinned toolchain:

```sh
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo bench -p agentos-core --locked -- --test
bash scripts/check-import-boundaries.sh
bash scripts/check-module-size.sh
bash scripts/check-provider-calls.sh
bash scripts/check-audit-regressions.sh
bash scripts/check-subprocess-boundaries.sh
bash scripts/check-catalogs.sh
bash scripts/check-release-surface.sh
bash scripts/check-release-locking.sh
bash scripts/check-dependencies.sh
bash scripts/rehearse-upgrade.sh
bash scripts/check-gateway-pipeline.sh
bash scripts/check-gateway-crash-matrix.sh
bash scripts/check-install-preservation.sh
bash scripts/check-release-archive.sh
git diff --check
```

Additionally:

- run real Landlock and Seatbelt tests on Linux and macOS respectively;
- run the new ingress/approval crash matrix and two-channel shared-runtime test;
- run `cargo semver-checks check-release -p agentos-interfaces --baseline-rev <merge-base>`
  and the equivalent `agentos-proto` command when their public surface changes;
  a breaking channel contract requires the matching version bump rather than a
  waived check;
- verify upgrade and rollback against representative pre-roadmap databases,
  including ambiguous child identities, live approvals, and unsettled ingress;
- inspect the durable request/safety record for a failed provider call, denied
  output, administrator-resolved approval, ambiguous interrupted action, and
  purge; and
- build, install, modify, reinstall, start, stop, and complete one offline turn
  from the packaged archive with no source checkout reachable.

## Definition of done

This roadmap is complete only when:

1. every coverage-table row has a merged regression and is marked complete with
   a commit/reference to its evidence;
2. all P0, P1, and P2 findings are closed or an explicit time-bounded ADR has
   been accepted under the release posture above;
3. all versioned migrations succeed, fail closed on ambiguous data, and pass
   rollback rehearsal;
4. the full required matrix is green on Linux and macOS from a clean checkout;
5. the shipped configuration, installed workspace, generated catalogs,
   capability matrix, observability guide, release notes, and actual runtime
   describe the same behavior; and
6. a fresh independent audit can reconstruct admission, authorization,
   execution, delivery, failure, and purge outcomes from durable evidence without
   relying on process memory or log-message inference.
