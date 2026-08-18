# ADR-001: Release-remediation contracts

- Status: Accepted
- Date: 2026-08-18
- Owners: Core authorization, runtime security, identity/persistence, and
  prompt/orchestration maintainers
- Supersedes: weaker or ambiguous claims in `DESIGN.md`, `ARCHITECTURE.md`, and
  historical roadmaps where those claims conflict with this decision
- Drives: `TEST-001`, `ID-001`, `AUTH-002`, `SBX-001`, `REQ-001`, `AUD-001`,
  and `STATE-001` in `AUDIT_REMEDIATION_PLAN.md`

## Context

The 2026-08-18 audit found that several intended invariants were described more
strongly than the implementation enforced. Remediation needs stable contracts
before compatibility behavior or regression tests can be judged correct. These
decisions define the rescue-release boundary; they do not claim that the current
implementation already satisfies it.

## Decisions

### 1. Policy narrowing is evaluated per call

For any concrete action, including its operation and arguments, authority is
ordered as `Deny < AskUser < Allow`. A child policy is valid only when its
decision is less than or equal to the effective parent decision for every call
the child can express.

- Parent `Deny` can only remain `Deny`.
- Parent `AskUser` can remain `AskUser` or become `Deny`; it cannot become an
  unattended `Allow`.
- Parent `Allow` may be narrowed by operation, argument constraint, resource,
  deadline, or sandbox mode. A child may not remove any parent constraint.
- Comparing only tool names is insufficient. Constraint implication must be
  proven by the policy implementation; an unprovable comparison fails startup.
- Every child call, including an MCP-originated call, still crosses `Approve`.

Unattended authority that is unavailable to the parent policy is not narrowing.
If it is required, it must be represented by a separate delegation grant. A
grant is explicitly approved by an authorized principal, scoped to exact
actions and constraints, time bounded, non-transitive by default, and recorded
as an immutable safety event. `Policy::narrow` never consults a compatibility
exception that silently widens authority.

Test oracle: for generated parent policies, child policies, operations, and
arguments, `decision(child, call) <= decision(parent, call)` unless a separately
authorized grant covers that exact call.

### 2. Sandboxing fails closed

`full_access` is an explicit declaration that the tool is not kernel isolated.
Every other `SandboxMode` requires an available executor that supports the
current operating system, the requested mode, and the tool's execution
protocol.

- Compatibility is checked before the tool implementation starts.
- Missing backends, unsupported protocols, profile-application failures, and
  worker failures are typed refusals. None may fall back to in-process or
  unsandboxed execution.
- Executor availability means a real probe successfully applied the requested
  restriction, not merely that a binary exists.
- For subprocess and stdio MCP tools, the isolated subject is the actual process
  tree that exercises the capability. Wrapping only a proxy client is not
  containment.
- Cancellation and deadlines terminate the process group governed by the
  executor.

Test oracle: a sandboxed mock tool records zero body invocations when no
compatible executor exists, and platform enforcement tests prove forbidden
writes fail for the actual descendant process.

### 3. Persistence is keyed by a typed principal

The authorization boundary is a versioned `PrincipalKey`, initially
`PrincipalKeyV1` with these required components:

1. agent or tenant id;
2. channel id;
3. conversation id;
4. sender id where an action is attributable to a sender.

An absent sender is a distinct typed value and is accepted only for trusted
local/system ingress. It is never represented by an empty string. Components
are serialized injectively as a version tag plus length-prefixed UTF-8 bytes;
filesystem-safe persistence names use unpadded base64url of those canonical
bytes. Replacement-based sanitization is not an identity encoding.

`SessionKey` is `(PrincipalKey, epoch)`. Sessions, memory scopes, approval
tickets, jobs, task sessions, audit events, and clear operations use this key or
an explicitly documented coarser scope that cannot authorize a finer-scoped
action. Remote approval and clear commands additionally bind to the initiating
sender or an explicitly configured administrator.

Test oracle: equal canonical keys imply equal typed components. Cross-agent,
cross-channel, cross-conversation, and cross-sender fixtures cannot observe or
mutate one another's state.

### 4. Every provider call uses one gateway and one manifest

All provider invocations pass through the prompt provider-call gateway. Initial
request kinds are `Plan`, `Route`, and `Compact`; adding a model-backed feature
requires a new exhaustive kind before it can call a provider.

Each invocation durably records exactly one request header and one immutable
manifest before dispatch. The record contains:

- request, run, trace, and session identifiers plus `RequestKind`;
- provider/model selection and tool-schema digest;
- ordered provider-visible messages and their named section provenance, or
  immutable content-addressed references sufficient to reproduce those exact
  bytes;
- projection/compaction inputs, token estimate, configured context limit, and
  pressure decision;
- completion status and provider usage, linked back to run totals.

Provider-visible content is sensitive session data and follows the session's
access and retention controls. General audit events refer to request and content
digests instead of copying prompts or unrestricted tool arguments.

Test oracle: scripted planning, routing, and compaction calls each produce one
replayable manifest, and the sum of their usage equals the run total.

### 5. Safety decisions are immutable events

The append-only safety-event schema has typed variants for at least:

- approval requested, resolved, denied, expired, and cancelled;
- input, tool, and output guardrail trips;
- sandbox refusal;
- run cancellation;
- terminal structural or provider error;
- delegation grant issued, used, expired, and revoked.

Every event includes an event id, schema version, event time, principal/session
key, run and trace link, actor when applicable, outcome, and redacted structured
details. Raw credentials, unrestricted tool arguments, and unrestricted memory
bodies are prohibited; use classified fields and stable digests. Events are
appended before the corresponding externally visible state transition is
acknowledged. Resolving approval may update a derived pending-work index, but it
never deletes or rewrites the event history.

Test oracle: pause, resume, denial, restart, and error fixtures retain the same
ordered safety history without exposing seeded canary secrets.

### 6. `/clear` starts a new epoch

Normal `/clear` atomically advances the caller's `SessionKey` epoch and records
an immutable event. Old session items remain append-only and are excluded from
ordinary hydration and planning by the new epoch. Memory outside the session
scope is unaffected unless a separately authorized memory operation says
otherwise.

Irreversible removal is a distinct purge operation with its own authorization,
scope preview, retention/legal constraints, audit event, and restart-safe
execution. `/clear` is never an alias for purge.

Test oracle: normal clear performs no session-item `DELETE`, subsequent input
uses the new epoch, and another principal or sender cannot advance it.

### 7. Stable output is buffered until accepted

Stable delivery buffers the complete candidate response, applies the output
guardrail and channel policy, and only then emits user-visible bytes. This rule
also covers cancellation notices, truncation notices, and synthesized terminal
replies.

Provisional streaming is Preview and explicit opt-in. A supporting channel must
identify provisional content and implement its documented replacement or
retraction behavior. It cannot be enabled by a stable default or presented as
equivalent to policy-accepted output. Promotion requires an incremental
guardrail or another staging interface that proves rejected content emits zero
user-visible bytes.

Test oracle: a seeded output-policy violation produces zero delivered bytes in
stable mode; preview streaming requires explicit configuration and is marked
provisional at the channel boundary.

## Consequences

- Some existing configurations will fail rather than retain permissive
  compatibility behavior. Diagnostics and migrations are part of the owning
  implementation slices.
- Subagent delegation, remote-channel production use, sandboxed mutation tools,
  and provisional streaming cannot be classified Stable until their referenced
  acceptance tests pass.
- Provider manifests and safety events increase durable storage. Their content
  is separately classified and subject to retention rather than weakened into
  non-reconstructible telemetry.
- `PrincipalKey`, `SessionKey`, `RequestKind`, safety-event variants, sandbox
  refusal errors, and delegation grants are versioned persisted types and need
  migration and compatibility tests.

## Rejected alternatives

- Tool-name-only policy narrowing: argument and operation constraints can still
  be widened.
- Treating `AskUser` as delegable `Allow`: this changes attended authority into
  unattended authority without consent.
- Best-effort sandbox fallback: a declared kernel boundary would not exist.
- Sanitized concatenated identity strings: distinct principals can collide.
- Separate ad hoc routing or compaction calls: request inputs and usage become
  incomplete.
- Deleting pending approval or session rows as the audit record: absence cannot
  reconstruct the decision.
- Best-effort streaming followed by a final guardrail: rejected bytes may
  already have reached the user.
