# Release Notes

## v0.7.0

The current development version. It is one thing:
[`AUDIT_REMEDIATION_PLAN.md`](AUDIT_REMEDIATION_PLAN.md), M0 through M9, worked
as twenty-six named slices against the findings in
[`AUDIT_BASELINE_2026-08-18.md`](AUDIT_BASELINE_2026-08-18.md). Each heading
below names the slices behind it, so the plan can be read for the detail and
the reasoning.

Nothing here is a feature. Almost all of it is the difference between a claim
this project made and what the code did.

### Read this before upgrading

- **The shipped policy is conservative now** (`CFG-000`). `shell`, `file` and
  `memory` are no longer in `[policy] allowlist`, and `python3` is no longer in
  the shell allowlist. A deployment that relied on those running unprompted
  will start asking. `[[guardrails.shell_profiles]]` is how a command is
  allowed back, per command and with its arguments validated.
- **`[memory.policy]` is memory's authority** (`MEM-001`). Naming `memory` in
  `[policy] allowlist` is now a load-time error rather than a silent override,
  so a config that set both fails until one is removed.
- **Unknown config keys fail the load** (`CFG-001`). Every section rejects a
  typo instead of defaulting past it, which will reject an `agent.toml` that
  loads today. Two keys are accepted-but-deprecated and warn: `agent.memory`
  and `[resources.llm]`.
- **`agent.id` is live** (`CFG-001`). It was read by nothing and the runtime
  hardcoded `main-agent`. It now keys every trace, episode, memory record and
  safety event — so setting it on an existing deployment orphans memory written
  under the old id. There is no rename path.
- **Memory *and the session log* written before v0.7.0 are under old keys**
  (`ID-001`, `ID-002`, M3 deliverable 2). `agentos-gateway migrate` reports
  what it would move and applies with `--apply --backup PATH`. It needs
  `--channel NAME`: the old rows do not record one and it cannot be inferred.
  **The gateway refuses to start until this has run** — an unmigrated session
  log reads as empty, and every conversation would silently start over.
  `agentos-gateway purge --conversation ID` needs `--channel NAME` now too,
  for the same reason.
- **A stdio MCP server must speak MCP** (`MCP-001`). The old client was not a
  JSON-RPC client; a server built against it will no longer work. See below.
- **Stop the old gateway with the old binary before upgrading** (`GW-001`).
  Liveness is now a held `flock`, and a gateway started by an older build holds
  no lock, so the new `stop` reads its control file as stale and refuses to
  signal it.
- **`[[mcp_servers]] sandbox` now means something** and defaults to
  `full_access`, which is an honest statement of the old behaviour rather than
  a new grant. A server that only reads should say `read_only` — and will then
  refuse to start on a host with no sandbox backend.

### Authorization and identity

**Sessions and jobs are keyed by principal** (M3 deliverable 2). This was the
last thing keyed on the number the transport chose, so Telegram's chat `42`,
Feishu's chat `42` and a second agent sharing the database were one transcript.
`Session::{load, append, fork}` now take a `Principal` — the semver break the
milestone was waiting on, and why this release is 0.7.0. The store reads two
names out of it: the conversation names the *transcript*, which its
participants share, and the full principal names the *`/clear` epoch*, which is
theirs alone. So one member of a group chat clearing their view no longer
clears anybody else's, and two participants can legitimately see different
prefixes of one log. `JobRegistry` fences on the same key, which matters
because that fence decides who may read a job's output.

**Principals are typed and namespaces are injective** (`ID-001`, `ID-002`,
`ID-003`). Two conversations whose ids differed only in punctuation could land
in one memory namespace and read each other's records.
`ConversationPrincipal` and `ActorPrincipal` distinguish shared conversation
state from a sender-qualified action. Delegated conversations commit to the
complete parent principal, child definition and policy, and task discriminator;
their source tuple lets `agentos-gateway migrate` rekey complete legacy rows
while reporting incomplete or colliding rows rather than guessing.

**A remote sender is authenticated, and an approval is answered by name**
(`AUTH-001`). Both channels fail closed on an unrecognised sender. An approval
prompt issues a ticket, and an envelope that does not carry it back is ordinary
input however affirmative it sounds — in a group conversation, a prompt belongs
to the person it was put to.

**Approval resolution is unique and actor-bound** (`AUTH-003`). Every asking
now mints one approval instance from a once-generated 128-bit OS-CSPRNG nonce
and a checked atomic counter. The persisted interruption, channel ticket, and
requested/resolved safety events share that identity. Resume APIs accept only
an opaque witness bound to the exact pending ticket, requester, resolver,
expiry, and outcome; consumed or mismatched interruptions fail closed.
Approval administrators are complete agent/channel/conversation/sender
principals, so a sender id configured for one channel has no authority in
another, and both actors survive in the durable audit record.

**Sub-agent policies only narrow** (`AUTH-002`). `Policy::narrow` decides a
finite set of witness calls with both policies instead of comparing rules, so
first-match ordering is respected and arguments are covered. A sub-agent that
must exceed its parent needs an explicit `[[subagents.delegation_grants]]`
naming one tool.

**Delegation grants are principal-bound and expiring** (`AUTH-004`). Configured
grant entries are lifetime-bounded templates, not reusable authority. Each
delegation mints a runtime instance bound to the sender-qualified parent actor,
delegatee agent and policy, exact tool and optional arguments, and a fresh
delegation generation. The one-hour kernel maximum is checked at load and
issuance; rollback before issuance and forward movement to expiry both fail
closed. Paused children retain the original generation and expiry, while grant
issuance and use events share one stable grant ID. Legacy `expires_at` and
standing entries now fail load with `lifetime_secs` migration guidance.

**Required safety evidence fails closed** (`AUD-002`). Safety-journal writes
are now fallible results handled at every event site. Approval prompts and
resolutions, delegation-grant issuance and use, and protected actions cannot
proceed when their evidence is unavailable. Denials, guardrail and sandbox
refusals, cancellation, and terminal errors preserve the safer outcome and
surface a typed `RunError::SafetyEvidence` instead of logging and continuing.
SQLite trigger failpoints cover each class and assert that no protected tool
body runs.

**`Approve` is re-asserted where the action happens** (`STATE-001`, `AUTH-005`).
`ActCtx` privately owns an immutable plan together with a domain-separated
commitment to its complete serialized form, including payloads, metadata,
routes, policies, and resumed child state. A hand-built context does not
compile, the plan or denied disposition cannot be replaced after approval, and
`act()` still re-decides against the live policy immediately before execution.

### Isolation, filesystem, process, network

**A declared sandbox mode is enforced or the tool is refused** (`SBX-001`). A
tool declares `read_only`, `workspace_write` or `full_access`; anything but the
last runs in a child under Landlock or Seatbelt. The registry discovers whether
an executor is compatible through a `--capabilities` handshake rather than
assuming it, and a tool that declares a mode with no way to apply it is refused
at startup and again at every call.

**Filesystem containment is directory-relative** (`FS-001`). `paths::RootDir`
walks to the target one component at a time with `openat(O_NOFOLLOW)`, so a
symlink is refused rather than followed and there is no window between the
check and the open. Identifiers that become a path component go through
`path_segment`, which rejects rather than rewrites.

**A child process starts from nothing** (`PROC-001`). Every subprocess —
isolation worker, `shell`, stdio MCP servers — gets `env_clear` and a named
allowlist, so credentials are never inherited; `[isolation].env_passthrough`
names anything else a deployment needs. Each child gets its own process group,
and a deadline or cancellation signals the group rather than the direct child.

**Tool egress is judged by address, not by name** (`NET-001`). A request whose
destination the model chose resolves through `GuardedResolver`, which drops
loopback, private, link-local, CGNAT and cloud-metadata addresses before a
connection exists — so a hostname pointed at `127.0.0.1` and a DNS rebind are
the same non-case. Redirect hops are checked too, and bodies stream to
`[limits].http_response_bytes`.

**Untrusted input never sizes an allocation** (`ING-001`). Feishu's fragment
reassembly bounds fragments per event, partially-received events and total
bytes; attachment downloads stream to `[limits].attachment_bytes` and stop;
provider response bodies are capped rather than read to end.

### The record

**Safety decisions are events, never absences** (`AUD-001`). Approval requested
and resolved, policy denial, guardrail trip, sandbox refusal, cancellation,
delegation-grant issuance and use, and terminal error each append a row to
`safety_events`. A resolved interruption is marked consumed and retained rather
than deleted. Arguments enter only as an `ArgumentDigest` —
[ADR-0005](adr/0005-SAFETY_EVENTS.md) — and `RunLoopState::step` hands the
state back out on failure, so a run that ends badly still persists its trace.

**One authority over the provider request** (`REQ-001`). Every call declares a
`RequestKind` and records a `RequestHeader` saying what it was made of, usage
included, and `prompt::gateway` is the only path from a built request to a
provider — `scripts/check-provider-calls.sh` fails CI when a direct call is
added elsewhere. Two kinds carry no transcript on purpose: the routing
classifier and the compaction summarizer are isolated from recalled memory as a
prompt-injection defence, now asserted rather than left to convention
([ADR-0004](adr/0004-REQUEST_KINDS.md)).

**`/clear` stops deleting** (`STATE-001`). It writes an epoch marker and
`Session::load` returns only items at or after it, so the model sees a fresh
conversation while the log still holds what happened. Irreversible removal is
`agentos-gateway purge`, which names the conversation twice and records a
safety event ([ADR-0006](adr/0006-CLEAR_EPOCH.md)).

**Every terminal reply crosses the output policy** (`STATE-001`). Including the
loop's own cancellation and budget notices, which used to be emitted directly.
Provisional streaming is off by default, because a chunk forwarded before the
check cannot be retracted ([ADR-0007](adr/0007-BUFFERED_OUTPUT.md)).

### Configuration and storage

**A config key does something, or the load fails** (`CFG-001`, `MEM-001`).
`deny_unknown_fields` on every deserializable struct; every key documented and
ratcheted both ways; `config/maturity.txt` naming the keys that are deprecated
or preview. `agentos-gateway config` is derived from the same walk that
generates the catalogs and prints every key with its value, whether it was set,
its default and its maturity.

**Oversized tool output is behind an opaque locator** (`SPILL-001`).
`spill:<run>/<artifact>` replaced an absolute host path embedded in the
transcript. `spill_read` is the only retrieval path, contained by `RootDir` and
authorized by the calling run's own transcript citing the locator.

**Every store that grows has a bound** (`QUOTA-001`). Traces, attachments, the
gateway log, spill, settled ingress rows and finished jobs are swept from the
gateway's idle phase under a `gateway.retention` lease, each with an age
ceiling and — where the store is a directory this runtime owns — a byte quota,
because one busy afternoon outweighs a quiet month. New keys: `[retention]`,
`[spill].max_bytes`, `[jobs].completed_retention_secs`. The session log and the
two audit stores are never swept; `agentos-gateway purge --sessions --before
DATE` and `--audit --before DATE` report first and apply only against the count
typed back. `agentos-gateway purge` also became runnable: `--conversation` and
`--yes` were never accepted by the option parser.

### Gateway and MCP

**Persistence is durable** (`GW-001`). SQLite runs in WAL with a busy timeout
behind a bounded connection pool (`[memory] max_connections`), instead of one
mutex serialising every shard. State files are written temp-fsync-rename-fsync
and at mode 0600. An ingress ledger keyed by `(channel, transport event id)`
answers fresh / retry / suppress, with the row written before the turn and
settled after, so a crash falls inside *retry*. Liveness is a held `flock`
rather than `kill -0` on a pid read from a file, and `SIGTERM` is a drain
bounded by `[gateway] shutdown_grace_secs`.

**MCP is MCP** (`MCP-001`). The previous client sent an AgentOS `ToolCall` and
expected an AgentOS `ToolResult` — no `initialize`, no version negotiation, no
capability check, no `nextCursor`, and a reply matched to a request by arrival
order, so one stray line shifted every later answer onto the wrong question.
Rewritten as JSON-RPC 2.0 against MCP `2025-06-18` with id correlation, content
blocks, `notifications/cancelled`, named bounds on frames, in-flight requests,
stderr, pages, tools and restarts, and the server child sandboxed by
`[[mcp_servers]] sandbox`. Interoperability is tested against a fixture that
imports nothing from this repository. Still Preview: stdio only, and this
client declines `roots`, `sampling` and `elicitation`.

### Release qualification

**The archive is self-contained and the install path is one path** (`REL-001`,
`REL-002`). `scripts/check-release-archive.sh` packages an archive, installs it
into a temporary prefix under a redirected HOME, validates every packaged
documentation link, starts and stops the packaged gateway, and completes one
offline turn with no repository reachable.

**CI is a gate rather than a report** (`CI-001`, `CI-002`). Both supported
platforms, with a sandbox skip counted as a failure. Actions pinned by commit
SHA and every Cargo command `--locked`. `cargo-deny` for advisories, licences,
duplicates and sources, with exceptions that expire. Upgrade, restart,
migration and rollback rehearsed against a real database by
`scripts/rehearse-upgrade.sh`, and the channel pipeline driven end to end
against mock transports. `cargo-semver-checks` is blocking rather than
suppressed twice over.

**Documented claims match the code** (`ADR-001`, `DOC-001`, `TEST-001`). Seven
ADRs under [`docs/adr/`](adr/README.md) for the disputed invariants,
[`docs/CAPABILITY_MATRIX.md`](CAPABILITY_MATRIX.md) with a status and a named
limitation per capability, and an `enforced_or_fail!` that no longer passes by
skipping.

## v0.6.0

Two roadmaps landed since v0.5.0:
[`FEATURE_ROADMAP.md`](FEATURE_ROADMAP.md) in full, and
[`TRANSFER_ROADMAP.md`](TRANSFER_ROADMAP.md) except its last item.

**Streaming.** Native SSE decoding for OpenAI, Anthropic, and DeepSeek, wired
through the run loop, both orchestrators, the TUI, and edit-in-place message
egress on Telegram and Feishu. Ollama uses the single-chunk fallback.

**One authority over the provider request.** A new `prompt` module is the only
path from a run's context to the messages a provider sees. Every contribution is
a named section and every request records what it was made of, which closed a
bug where hydrated memory was retrieved, counted, and then never sent.

**Bounded conversations.** The session log became append-only, with what the
model sees computed as a projection over it. Oversized tool output spills to a
file behind a locator instead of being truncated away; under context pressure
recorded results are elided; beyond that the oldest span is summarized into a
checkpoint; and a provider that rejects a request for length triggers one forced
compaction and one retry. Token pressure is estimated per request and the
estimator has been calibrated against a live provider.

**Deadlines and control.** Every tool call has a deadline and runs without
blocking a Tokio worker. Every run carries a cancellation token, and a sub-agent
tree cancels with its parent. A tool that would exceed its deadline can be
promoted to a background job that outlives the turn, with `job_status`,
`job_output`, and `job_kill` to manage it. `/stop` abandons a turn while keeping
the work it already finished.

**Gateway concurrency.** Conversations are now actors: each is assigned by stable
hash to one of `[gateway].shards` worker threads, so a conversation waiting on a
slow tool no longer stalls everybody else. Within a conversation everything stays
serial. A message sent mid-run steers the run at its next planning step instead
of queueing behind it.

**Approvals must be answered by name.** An approval prompt issues a ticket, and
only an answer carrying that ticket decides it — `/approve <ticket>`,
`/deny <ticket>`, or an inline button. Prompts expire (default 15 minutes) and
an expired prompt is recorded as cancelled, not refused.

**Real sandboxing.** `requires_isolation` was replaced by a `SandboxMode`
(`read_only`, `workspace_write`, `full_access`) that the kernel enforces —
Landlock on Linux, Seatbelt on macOS — inherited by every descendant process.
Where no backend exists, a sandboxed tool fails rather than running unrestricted.

**Correctness and hygiene.** Every tool call in a multi-call response is now
kept and drained one per turn instead of the first being run and the rest
dropped. One config pass moved the remaining hardcoded limits into `agent.toml`.
The config and tool catalogs are generated from the code and checked in CI.
Three load-bearing invariants are asserted in debug builds. A conversation can be
forked from a prefix of another. Memory gained reflection sweeps, real
embeddings, and the first extension crate, `agentos-memory-vector`.

**Other.** OpenAI migrated to the Responses API exclusively; Anthropic replies
are capped at each model's published output limit; reply-integrity checks reject
provider responses that lost information; the email PII guardrail no longer
matches URLs; Feishu reconnects back off; trace records carry `emitted_unix`.

## v0.5.0

Released 2026-05-19.

- Reworked the sub-agent and skill mechanism.
- Added new bundled workspace skills.
- Documentation refresh across the `docs/` set.
- Test fixes and `.gitignore` cleanup.

## v0.4.0

- Reworked skill creation: rebuilt `skill-creator` planner logic and the
  `agentos skill create` flow, including bundle completeness handling.
- Cron tooling: refactored `cron-create` and fixed cron scheduling issues.
- Sub-agent improvements: tool calling for sub-agents, output-length limits,
  per-task session isolation, and attachment propagation from the main agent.
- Multimodal support: attachment handling for Telegram and Feishu, and
  multimodal requests for the OpenAI and Anthropic providers.
- Fixed `MaxOrchestrator` not invoking tools.
- Telegram/Feishu message chunking limit fixes.
- Feishu reliability: proxy fix and openssl stderr capture on websocket
  failures.

## v0.3.0

- Unified slash commands across the TUI, Telegram, and Feishu surfaces.
- Added TUI list commands (`/skills`, `/crons`, `/tools`, `/memory`, `/usage`).
- Phase 1–5 milestones completed (typed loop, approval, tools, memory,
  channels).
- Bash 3.2 CLI argument-expansion fix.

## v0.2.0

Baseline packaged AgentOS workspace:

- interactive TUI support
- persistent Telegram and Feishu gateway support
- pluggable LLM providers: OpenAI, Anthropic, DeepSeek, Ollama, and a
  `builtin.echo` offline fallback
- a typed run loop with concrete approval policy and content guardrails
- SQLite-backed sessions and scoped long-term memory
- configured sub-agents and sub-orchestrator templates
- static and stdio MCP-backed tools, with subprocess isolation
- workspace skills in the Anthropic `SKILL.md` format
- user-facing install and startup scripts
- source and bundle installation paths
- release packaging automation

Release contents:

- `agentos-cli`
- `agentos-gateway`
- `agentos-tool-worker`
- `agentos-mcp-stdio-worker`
- default `workspace/agent.toml`
- `.env.example`
- install/startup scripts
- user documentation
