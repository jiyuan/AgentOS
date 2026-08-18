# Release Notes

> **Audit status (2026-08-18):** v0.6.0 is not release-qualified. The
> [`capability matrix`](CAPABILITY_MATRIX.md) classifies the features below as
> Stable, Preview, or Deferred and names their promotion gates. The
> [`remediation plan`](AUDIT_REMEDIATION_PLAN.md) supersedes unqualified
> completion claims in these development notes.

## v0.6.0

The current development version. Two roadmaps landed since v0.5.0:
[`FEATURE_ROADMAP.md`](FEATURE_ROADMAP.md) in full, and
[`TRANSFER_ROADMAP.md`](TRANSFER_ROADMAP.md) except its last item.

**Streaming (Preview).** Native SSE decoding for OpenAI, Anthropic, and DeepSeek, wired
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

**Sandbox modes (Preview pending platform and MCP qualification).** `requires_isolation` was replaced by a `SandboxMode`
(`read_only`, `workspace_write`, `full_access`) that the kernel enforces —
Landlock on Linux, Seatbelt on macOS — inherited by every descendant process.
The registry now refuses missing executors, unavailable backends, unsupported
modes or protocols, and worker failures without invoking the in-process tool
body. macOS availability now requires `sandbox-exec` to apply a permissive
profile and a read-only profile to deny a control write; binary presence alone
does not qualify the backend. `SBX-001` still tracks isolation of the actual
stdio MCP server process.

**Correctness and hygiene.** Every tool call in a multi-call response is now
kept and drained one per turn instead of the first being run and the rest
dropped. One config pass moved the remaining hardcoded limits into `agent.toml`.
The config and tool catalogs are generated from the code and checked in CI.
Three load-bearing invariants are asserted in debug builds. A conversation can be
forked from a prefix of another. Memory gained reflection sweeps, real
embeddings, and the first extension crate, `agentos-memory-vector`.

**Self-contained release archive.** Release bundles now include the enabled
skills, subagents, sub-orchestrator templates, empty runtime directories, and
the complete linked documentation set without copying session or trace state.
An artifact check extracts the archive outside the checkout, resolves workspace
and documentation references, constructs the runtime, and completes an offline
`builtin.echo` turn. `REL-002` extends that check through source and packaged
installs: the XDG layout, wrapper configuration diagnostics, stopped gateway
status, pathless resume, and an installed offline turn are now exercised on
Linux and macOS.

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
