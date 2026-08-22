# AgentOS Architecture Design Document

Status: active design baseline
Last consolidated: 2026-08-17

This document is the unified architecture reference for AgentOS. It supersedes
the older standalone extension-boundary, memory-system, and development-plan
documents while preserving their current design decisions.

Sequencing and per-item rationale live in the roadmaps:
[`TRANSFER_ROADMAP.md`](TRANSFER_ROADMAP.md) (request authority, context
management, deadlines, gateway concurrency — the most recent work),
[`FEATURE_ROADMAP.md`](FEATURE_ROADMAP.md) (streaming, memory intelligence,
extension ecosystem), [`OPTIMIZATION_ROADMAP.md`](OPTIMIZATION_ROADMAP.md)
(performance debt), and [`PLAN.md`](PLAN.md) (invariant and config-authority
milestones). This document describes what is built; those describe why and in
what order.

## 1. Purpose

AgentOS is a Rust agent runtime with a typed run loop, concrete approval policy,
pluggable extension traits, persistent sessions, scoped memory, tool execution,
workspace skills, sub-agent orchestration, MCP-backed tools, and channel
gateways.

The core design goal is simple: every run follows one auditable loop, and every
external capability crosses a narrow trait or policy boundary.

Not every capability listed above is equally finished.
[`CAPABILITY_MATRIX.md`](CAPABILITY_MATRIX.md) gives each one a Stable /
Preview / Deferred status with its security limitations and test level; read it
before relying on anything here in production.
[`adr/`](adr/README.md) records the decisions behind the invariants below, and
each invariant that the code does not yet enforce says so with the milestone
that closes it.

## 2. Core Invariants

- The run loop owns control flow.
- `Approve` is concrete policy, not an extension trait.
- Guardrails inspect content; approval checks permission.
- One module assembles every provider request, and every request records what
  it was made of.
- The session log is append-only, and what the model sees is a projection over
  that log rather than a rewrite of it — `/clear` included, which appends an
  epoch marker ([ADR-0006](adr/0006-CLEAR_EPOCH.md)). The one deletion is the
  `agentos-gateway purge` operator command.
- Passive memory retrieval hydrates planning context; explicit memory mutation
  goes through tools and approval.
- Sub-agent permissions can only narrow parent permissions, over actions and
  arguments; an exception is an explicit delegation grant
  ([ADR-0001](adr/0001-POLICY_NARROWING.md)).
- External implementation contracts live in `agentos-interfaces`.
- Workspace-owned content is data, not a dependency of core crates.
- Runtime paths are injected by CLI/gateway construction, not inferred from
  scattered core defaults.
- Every `tokio::mpsc` channel is bounded.

## 3. Crate Layout

Active crates:

- `agentos-proto`: serializable wire and domain types (`Message`, `ToolCall`,
  `Envelope`, `Span`, `RequestHeader`, `Usage`).
- `agentos-interfaces`: public extension traits and shared run-state types.
- `agentos-core`: run loop, runner, gateway service, approval engine,
  guardrails, prompt assembly, reference tools, memory/session stores,
  sub-agent execution, channel adapters, sandboxing, background jobs, runtime
  construction, and config parsing.
- `agentos-llm`: provider-neutral LLM facade and provider adapters (`openai`
  Responses API, `anthropic`, `deepseek`, `ollama`, plus a `builtin.echo`
  offline fallback).
- `agentos-cli`: TUI, one-shot channel entry points, persistent gateway,
  slash commands, startup environment loading, and runtime path construction.

Extension crates (compiled in, selected by config — see §4):

- `extensions/memory/agentos-memory-vector`: `SemanticIndex` implementation with
  a pluggable `Embedder` (OpenAI-compatible `/embeddings`, with an offline
  hashing fallback).
- `extensions/orchestrators/`: empty placeholder. No orchestrator extension
  exists; both orchestrators are built into `agentos-core`.

`agentos-core` module map, since the crate is where most of the runtime lives:

| Module | Owns |
|---|---|
| `loop/` | `RunLoopState` and its transitions, planning, tool calls, batches, delegation, escalation, approval routing, cancellation, steering, turn budget, telemetry. |
| `prompt/` | The single authority over a provider request: assembly, sections, transcript projection, token estimation, calibration, elision, span compaction. |
| `orchestrator/` | `MaxOrchestrator` (tool-selecting) and `MinOrchestrator` (LLM fallback), routing, streaming, slash-command handling. |
| `approve/` | The concrete policy engine. Not a trait. |
| `guardrails/` | Input, tool, and output content checks. |
| `gateway/` | Bounded ingress queue, per-conversation inbox, the shard threads that run conversations concurrently, the durable ingress ledger, the locked control file, and the shutdown flag. |
| `runner/` | Envelope-to-run driving, task sessions, episode recording. |
| `runtime/` | `AgentRuntime` construction, `RuntimePaths`, tool/MCP wiring, policy build. |
| `config/` | `agent.toml` parsing, normalization, validation, and catalog rendering. |
| `memory/` | `MemoryManager`, scope/authorization, SQLite, sqlite-vec, Qdrant, hybrid retrieval, reflection. |
| `tools/` | Registry, execution, deadlines, built-ins, the MCP client (protocol, stdio transport), the `memory` tool. |
| `jobs/` | Background work that outlives the turn that started it. |
| `spill/` | Oversized tool output persisted to disk behind an opaque locator. |
| `sandbox/` | Kernel-enforced write restrictions (Landlock, Seatbelt). |
| `skills/` | Workspace skill loading, validation, and deterministic planners. |
| `subagents/` | Sub-agent execution. |
| `channels/` | Telegram and Feishu reference adapters, attachments, text chunking. |
| `crons/` | Persisted scheduled tasks. |
| `hooks/` | Lifecycle hook dispatch. |
| `invariants.rs` | Debug-build assertions over load-bearing relationships (§10). |

Workspace-owned content lives under `workspace/` and is loaded through config:

- `agent.toml`
- `skills/`
- `subagents/`
- `suborchs/`
- `crons/`
- `tasks/`
- runtime state such as session DBs, traces, run snapshots, spill artifacts, and
  attachments

## 4. Public Extension Boundary

Extension authors should implement traits from `agentos-interfaces`:

- `Channel` for ingress and egress adapters, with `Egress` and `StreamEgress`
  handles for reply and streamed reply delivery.
- `Tool` for callable capabilities.
- `Skill` for provider-neutral skill execution.
- `McpClient` for MCP transports and servers.
- `Memory` and `Session` for storage implementations.
- `SemanticIndex` for vector or other similarity retrieval behind a `Memory`
  backend.
- `InputGuardrail`, `OutputGuardrail`, and `ToolGuardrail` for safety checks.
- `Orchestrator` for planning and dispatch behavior.

`agentos-core` contains reference implementations. New integrations should
prefer trait implementations or MCP tools before adding core dependencies.

Current reference-implementation decisions:

- Telegram and Feishu adapters remain in `agentos-core` as reference channels.
  Future extension crates can replace them by implementing `Channel`.
- SQLite, sqlite-vec, and Qdrant remain core reference memory implementations.
  Alternative storage backends should implement `Memory`/`Session` or expose
  behavior through MCP.
- Built-in deterministic skill planners remain core reference planners.
  Workspace skill content remains workspace-owned data.

How extensions are wired today (decided 2026-06-12, roadmap Phase 4.4):

- Implementations are **compiled in**, selected by `agent.toml` strings:
  `[agent].orchestrator` picks `builtin.max`/`builtin.min`, `[memory].backend`
  and `[memory].semantic_backend` pick the built-in stores or the vector
  extension, channels and tools are enabled by name. There is no plugin loader.
- The one extension crate that exists, `agentos-memory-vector`, shows the shape:
  it implements `SemanticIndex` from `agentos-interfaces`, only `agentos-cli`
  depends on it, and it reaches the runtime through the factory argument of
  `AgentRuntime::build_with`. Core never imports it.
- Adding an implementation therefore means: a new crate that implements the
  `agentos-interfaces` traits, a dependency edge from `agentos-cli` (never
  from core), a registration arm in `agentos-core::runtime` construction or
  the CLI wiring, and a rebuild.
- Dynamic library loading was considered and **rejected**: Rust has no stable
  ABI, and a `dlopen`-style surface would bypass both the import-boundary
  check and the supply-chain posture of the authorization layer.
- A link-time factory registry in `agentos-interfaces` (extension crates
  register constructors; core stays free of extension imports) is the
  designated upgrade path if out-of-tree extensions materialize — design doc
  required first. Deferred while a single extension exists.
- `Approve` stays a concrete engine regardless; the extension surface is
  orchestrators, memory, semantic indexes, tools, channels, skills, and
  guardrails only.

Boundary enforcement:

- `scripts/check-import-boundaries.sh` enforces compile-time dependency
  boundaries (with a `--self-test` negative-fixture mode). It rejects both
  core → workspace and core/interfaces → extension edges.
- Runtime path strings in docs, comments, tests, and user examples are not
  dependency-boundary violations.

## 5. Runtime Path Ownership

The CLI and gateway derive paths from the selected `agent.toml` and pass them to
core through `RuntimePaths`:

- `agent_config_path`
- `session_db_path`
- `trace_dir`
- `workspace_root`
- `skills_dir`
- `cron_dir`

`AgentRuntime::build()` pins these for legacy built-in tools:

- `AGENTOS_WORKSPACE_ROOT`
- `AGENTOS_SKILLS_DIR`
- `AGENTOS_CRON_DIR`

Spill artifacts resolve `[spill].root` against the workspace root unless it is
absolute. Channel attachment directories are selected by the CLI or gateway and
passed to reference channel adapters. Standalone channel construction still
supports `AGENTOS_ATTACHMENTS_DIR` as an explicit override.

All workspace paths ultimately anchor on `$AGENTOS_HOME`, which the gateway
resolves from the environment or the loaded `.env` file. This keeps path
decisions at runtime entrypoints while preserving compatibility with built-in
tools that resolve paths from environment variables.

## 6. Configuration Authority

`workspace/agent.toml` describes the effective runtime. The canonical loader is
`WorkspaceConfig::load()`, which:

- parses the main TOML;
- resolves relative paths against the config directory;
- loads `subagents/*.toml`;
- loads `suborchs/*.toml`;
- validates memory, policy, channels, resources, limits, sub-agents, templates,
  and routing — including cross-section checks such as compaction triggering
  above the elision threshold, and contradictions such as `[policy] allowlist`
  naming `memory` when `[memory.policy]` is the authority over it.

Every section rejects an unknown key, so a typo is a load-time error rather
than a silent default — `[memory.polcy]` used to load with defaults and say
nothing (M7 / `CFG-001`).

### Every key is one of three things

| Maturity | Meaning | Where recorded |
|---|---|---|
| effective | decides something, no caveat | the default; nothing to record |
| deprecated | still parses so an older `agent.toml` loads, warns, decides nothing | `config/maturity.txt` |
| preview | decides something, with a stated exposure; off by default | `config/maturity.txt` and `CAPABILITY_MATRIX.md` |

`config/maturity.txt` is ratcheted both ways: a listed key the config no longer
has fails the build, and a doc comment announcing `**Deprecated.**` without a
matching line fails too — a deprecation cannot be declared in prose alone.
There is deliberately no way to *mark* a key effective; effective is what a key
is when nobody had to make an excuse for it.

`agentos-gateway config` prints the resolved deployment — paths, sandbox
mechanism, the channels that will be served — and then every key with its
value, whether it was set, its default, and its maturity. It is derived from
the same walk that generates `docs/CONFIG_CATALOG.md`, so a key present in the
structs cannot be missing from it; it used to be fifty-five hand-written
`println!`s over a subset. `source` reads `file` when the effective value
differs from the default, so a config that writes a default explicitly reports
`default` — distinguishing those needs presence tracking against the raw TOML.

Effective config sections:

| Section | Governs |
|---|---|
| `[agent]` | id, orchestrator, memory id, `max_turns`. |
| `[policy]` | parent policy default decision. |
| `[memory]` | backend, semantic backend, hydration, retention, reflection, shared-domain policy. |
| `[channels.*]` | TUI and persistent-channel enablement and modes. |
| `[resources.skills]`, `[resources.tools]`, `[resources.mcp]`, `[resources.llm]` | which skills, built-in tools, MCP tools, and LLM fallbacks are enabled. |
| `[[mcp_servers]]`, `[[mcp_tools]]` | static or stdio MCP declarations. |
| `[[routing.rules]]`, `[orchestrator_templates]` | routing table and sub-orchestrator templates. |
| `[[subagents]]` | sub-agent declarations (also loaded from `subagents/*.toml`). |
| `[limits]` | tool deadlines and per-tool overrides, inline tool-result cap, file/listing/output byte bounds. |
| `[spill]` | oversized tool-output artifact root and retention. |
| `[compaction]` | whether, when, and with which model tier a run summarizes its own history. |
| `[jobs]` | background job concurrency, output cap, and the promotable-tool allowlist. |
| `[gateway]` | shard-thread count and per-conversation inbox capacity. |
| `[approval]` | approval prompt expiry. |
| `[guardrails]` | shell allowlist and content-check settings. |
| `[isolation]` | sandbox worker binary discovery. |
| `[task_workspace]` | task-state root. |

Every key, with its type, default, and description, is generated from the config
structs into [`CONFIG_CATALOG.md`](CONFIG_CATALOG.md). Edit the doc comment on
the field, never that table; `scripts/check-catalogs.sh` fails CI when they
diverge.

Gateway persistent channel precedence:

1. `AGENTOS_ENABLED_CHANNELS=telegram,feishu` overrides workspace channel
   enablement.
2. Without that override, `[channels.telegram].enabled` and
   `[channels.feishu].enabled` control persistent gateway channels.
3. `channels.tui` is for the interactive TUI and is never started by the
   persistent gateway.

Diagnostic commands:

```sh
agentos-gateway config --config workspace/agent.toml   # effective config
agentos-gateway catalog --check                        # catalogs still match the code
```

## 7. Run Loop Model

AgentOS uses one typed run loop:

```text
Start -> Plan -> Approve -> Act -> Observe -> Plan ...
                   |         |
                   v         v
                 Paused    Finish
```

The core state enum is:

- `Start`
- `Plan`
- `Approve`
- `Act`
- `Observe`
- `Paused`
- `Finish`

Every transition consumes the previous state and returns the next state, or
returns a `StepFailure` that carries the state back out so a failed run still
persists its trace. Invalid transitions are not representable by the public
API. No feature since the original loop has added a state or a transition;
everything below attaches to an existing state or runs outside the loop
entirely.

The *ordering* used to be a review rule rather than a property. `ActCtx` had
all-public fields, so a caller could build one carrying a tool call and hand it
to `step`, and `Act` ran it with no policy decision behind it. It now carries a
private `Authorization` with no public constructor — a hand-built `ActCtx` does
not compile — and the witness names the plan it was issued for, so a caller
holding a legitimate one cannot assign a different plan over the approved one.
`Act` also re-decides against the live policy before running anything,
accepting `ask_user` only when a human has actually answered. The transition
table itself is pinned in `crates/agentos-core/tests/loop_transitions.rs`.

Plan variants:

- `Reply`: terminal assistant output.
- `CallTool`: execute a local or MCP-backed tool.
- `CallTools`: a batch of calls the model asked for in one response. The batch
  is queued on `RunState.queued_tool_calls` and drained one call per turn, so
  every call crosses `Approve` individually and results append in the order the
  model asked for them. Draining costs turns deliberately; it does not cost
  extra LLM round trips. Execution stays sequential.
- `Handoff`: switch active agent and continue planning.
- `Delegate`: run a sub-agent and return the child result to the parent.
- `Escalate`: execute a configured sub-orchestrator template.

Guardrail placement:

- Input guardrails run in `Start`.
- Tool guardrails run in `Act`.
- Output guardrails run before terminal replies finish — including the replies
  the loop synthesizes itself for budget exhaustion, cancellation, and context
  overflow. `loop/output.rs` is the single gate for the synthesized ones. A
  model reply that trips fails the run; a notice the loop wrote is replaced by
  a shorter fixed one, because the run has to terminate here and re-raising
  would throw away the tool results a user who pressed stop most wants kept.
  Streaming is the one way to put bytes past a sink before the check runs, and
  is off unless `[channels] provisional_streaming` says otherwise
  ([ADR-0007](adr/0007-BUFFERED_OUTPUT.md)).

Approval placement:

- Every non-terminal action crosses `Approve`, and `Act` re-asserts that it did.
- `allow` proceeds to `Act`.
- `deny` on a **tool call** is *recoverable*: `Act` records a `Denied`
  `ToolResult` rather than running the tool, the model reads it on the next turn
  and replans, and the run continues. It is never a `RunError`.
- `deny` on a **handoff, delegation, or escalation** is *structural*: there is
  no tool-call id to attach a result to and nothing to retry around, so the run
  ends with `RunError::StructuralDenial`. That is a separate variant from
  `RunError::ApprovalDenied`, which means a human was asked and said no — the
  same distinction that already separates both from `ApprovalUnanswered`.
- `ask_user` serializes a paused `RunState` and mints an approval ticket (§9).

`Plan` is also the one point where new input is folded in. A message that
arrives mid-run is queued on the run's steering handle and claimed at the next
`Plan`, where the previous turn has been observed and the next request has not
been assembled yet. From the model's side it is an ordinary user turn.

## 8. Request Assembly and Context Management

`prompt/` is the single authority over what a provider request contains
(roadmap P1–P3, C1–C4). Before it, every LLM-backed orchestrator built its own
message vector at the call site, and hydrated memory was computed, counted in
telemetry, and silently never sent.

- **Assembly.** `prompt::assemble` is the only path from a `RunContext` to the
  messages a provider sees. Every contribution is a named `SectionId`.
- **Kinds.** Every provider call declares a `RequestKind`, and the kind decides
  the section set. Two kinds legitimately carry no transcript: the routing
  classifier and the compaction summarizer. That is not an exception to the
  recording rule but the reason the rule can be stated precisely — *every
  provider call records what it was made of*, not *every provider call derives
  from the transcript*. Conflating the two would mean assembling the
  classifier, which would let a poisoned memory record choose which
  orchestrator handles the next turn.
  `invariants::request_derives_from_state` branches on the kind and refuses a
  non-turn request that reaches the transcript, the skill prelude, or recalled
  memory, so the injection defence is a compiled assertion rather than an
  intention. See
  [`ADR-0004`](adr/0004-REQUEST_KINDS.md).
- **Gateway.** `prompt::gateway` is the only path from a built `Request` to a
  provider, and it is what attaches the header and the usage record.
  `prompt::call_with_context` records onto the run context's sinks;
  `prompt::call_detached` hands the record back to a caller that has no context
  — compaction holds `&mut RunState`, which is why its tokens went uncounted
  for as long as they did. `scripts/check-provider-calls.sh` fails CI when a
  direct provider call is added outside the gateway, because the rule is only
  as good as the thing that notices it being broken.
- **Manifest.** Each call returns a `PromptManifest`, recorded as a
  `RequestHeader` in the trace, so "what did the model see" is answered from the
  trace rather than by re-reading the code. `RequestBuilder` is the single path
  from sections to messages *and* manifest, so a manifest cannot describe
  content that was not sent.
- **Projection.** The session log is append-only. `prompt::projection` computes
  the model-visible view over it. A *checkpoint* is an ordinary transcript item
  that summarizes an inclusive range of earlier positions; the projection folds
  that range out. This is what lets compaction, fork, and elision each be a read
  rather than a bespoke mutation path.

  `/clear` is the same idea one level down, in the store rather than in the
  projection module: it appends to `session_epochs`, and `Session::load`
  returns only items at or after the newest epoch. It lives there because the
  epoch has to constrain every reader of `session_items` — `fork` included,
  which would otherwise copy hidden history into a child — and a filter at the
  call sites is a filter somebody forgets. The epoch is keyed by conversation
  rather than by principal, which ADR-0006 asks for; that waits on `Session`
  being principal-keyed (M3 deliverable 2), so in a group conversation one
  participant's `/clear` still clears the shared view.
- **Pressure.** `prompt::tokens` estimates how much of the context window a
  request occupies. It is a heuristic, not a tokenizer, and uses two rates
  because a 4:1 characters-per-token divisor badly under-counts CJK text.
  `prompt::calibration` measures it against a live provider's own
  `Usage.input_tokens`; `agentos-gateway calibrate` runs it and
  `--check` re-scores offline. See [`TOKEN_CALIBRATION.md`](TOKEN_CALIBRATION.md).
- **Spill.** A tool result over `[limits].tool_result_inline_bytes` is written
  whole to a spill artifact and replaced inline by a bounded preview plus an
  opaque locator, so the model can read the rest on demand and an operator can
  still see what a tool produced. Artifact directories are `0700`, files are
  created `O_EXCL` at `0600`, and every path segment is sanitized by replacement
  so a model-supplied id cannot escape the root.

  The locator is `spill:<run>/<artifact>` and is deliberately *not* a path. It
  used to be the absolute host path, handed to the model with an instruction to
  open it with the `file` tool — which put the host's layout into a durable
  transcript, made the locator authorize nothing (anything that could read one
  path could read any path), and left a spill root outside the workspace
  unreadable, since `file` is contained to the workspace. Retrieval is now the
  `spill_read` tool: it resolves against the configured store through
  `paths::RootDir`, and refuses a locator the calling run's own transcript does
  not cite. That last check is the authorization — the model may re-read what
  it was told about and nothing else — and it spans turns for free, because a
  conversation's transcript does.
- **Elision.** Under pressure, the middle of an already-recorded tool result is
  replaced by a marker citing its locator. Free, deterministic, and a view
  rather than a rewrite.
- **Compaction.** When elision is not enough, one model call summarizes the
  oldest span into a checkpoint. Governed by `[compaction]`; the tail
  (`retain_tail_turns`) stays verbatim. The summarizer is a provider call like
  any other: it goes through the gateway, records a `compaction` header, and
  its tokens reach the run's totals. They did not before M5, so run totals
  across a compaction were understated.
- **Overflow recovery.** A provider that rejects a request for length is the one
  trigger that is never an estimate. The class stays typed
  (`ProviderError::ContextLength` → `LlmError::ContextLengthExceeded` →
  `OrchestratorError::ContextLengthExceeded`), and the loop forces one
  compaction and retries the step exactly once. A second consecutive overflow
  finishes the run with a truncation notice delivered as a normal `Plan::Reply`,
  so output guardrails still run on it.
- **Fork.** `Session::fork(source, boundary, child_id)` branches a conversation
  from a prefix of another. `boundary` is a length, not a range: checkpoints name
  the span they hide by absolute position, so copying from 0 is what makes the
  child's projection equal the projection of the parent's prefix. The SQLite
  store implements it as one `INSERT … SELECT`.

## 9. Concurrency, Deadlines, and Control

Roadmap D1–D3 and G1–G2. All of this sits around the loop, not inside it.

- **Cancellation.** One `CancellationToken` per run, carried on `LoopDeps` and
  installed on every `RunContext`. Sub-agents get a *child* token, so cancelling
  a parent cancels its whole delegation tree while a sub-agent cancelling itself
  leaves the parent alone. A cancelled run *finishes* with a stop notice rather
  than erroring — the runner only appends a run's transcript items on the
  success path, so failing would discard the tool results the user most wants
  kept.
- **Deadlines.** Every tool call has a deadline: its own `ToolSpec::timeout_ms`,
  the `[limits].tool_timeout_ms` default, or a `[limits].tool_timeout_overrides`
  entry, which wins over both so an operator has the last word. Subprocess
  execution is async; nothing blocks a Tokio worker on `std::process`.
- **Background jobs.** A tool on the `[jobs].promotable` allowlist becomes a
  background job instead of failing at its deadline. Jobs report progress, can be
  killed, and outlive the turn that started them. Every job belongs to exactly
  one `ConversationId` and every registry operation is fenced by the asking
  conversation, so a model that can name an id still cannot reach across
  conversations. Surfaced by the `job_status`, `job_output`, and `job_kill`
  tools.
- **Conversations as actors.** The gateway no longer runs one serial
  receive → run → send loop. Each conversation has an `Inbox` and is assigned by
  stable hash to one of `[gateway].shards` OS threads, each with its own
  `current_thread` runtime. Within a conversation everything stays strictly
  serial, because two runs on one conversation would both load the transcript and
  the second write-back would clobber the first. Within a shard, turns run
  interleaved on a `FuturesUnordered` rather than `spawn_local`: a spawned task
  must be `'static`, which would force each shard to build its own
  `AgentRuntime`. The run-loop future is `!Send` (sub-agent execution drives a
  `LocalSet`), so thread-per-core is the scaling model, adopted deliberately —
  see [`../BENCHMARKS.md`](../BENCHMARKS.md).
- **Inbox routing.** An inbound envelope either waits for a run of its own
  (*next-turn*) or steers the run in flight (*next-step*). Both lists are
  bounded by `[gateway].inbox_capacity`: the steer queue drops its oldest entry
  (the newest instruction is what the user means), the turn queue refuses the
  newest (queued work is work the user asked for).
- **Correlated approvals.** An approval answer must name what it answers. An
  `ApprovalTicket` is minted per *prompt*; the `InterruptionId` (derived as
  `approval-<tool call id>`) still travels with it to say *what* was approved.
  Anything that is not an answer stays ordinary input. Prompts expire after
  `[approval].expiry_seconds` (default 900) and an expired prompt records
  `ApprovalStatus::Unanswered` — cancelled, not rejected.
- **Durable ingress.** The transports AgentOS speaks are at-least-once, and no
  acknowledgement a client can send makes a redelivery impossible — so the
  redelivery has to be *recognised*. `gateway/ingress.rs` keys on
  `(channel_id, transport event id)` and answers three ways: fresh (run it),
  accepted-but-never-settled (run it again — the user asked and nobody
  answered), and settled (suppress it). The row is written before the envelope
  reaches a shard and settled once the turn ends, whatever the turn's outcome,
  which is what puts the crash window inside the retry case rather than between
  the two. A channel publishes its transport's own id under `INGRESS_ID_KEY`;
  one that publishes none is delivered and not recorded, because the gateway
  cannot dedupe what it cannot recognise and an invented id would either merge
  two identical messages or call every redelivery new. A resumable cursor —
  Telegram's `getUpdates` offset — is persisted only after the event at that
  position is in the ledger; the reverse ordering is the one that cannot be
  recovered from.
- **One serving process, one sweeper.** Which process is the gateway is
  established by `flock` on the control file, held for the life of that
  process: locked means it is alive, unlocked means nobody is. There is no
  `kill -0` liveness guess, because a pid is recycled and a stale record then
  names a stranger. `SIGTERM` sets a flag — the only work safe inside a signal
  handler — the router stops accepting, in-flight turns drain within
  `[gateway] shutdown_grace_secs`, and past that the gateway reports the shards
  that would not drain and every accepted-but-unsettled event rather than
  hanging. Separately, reflection and retention run under a `memory.reflection`
  lease so the one database gets one sweep, across every channel's shard set
  and every process on the file.

## 10. Safety Architecture

AgentOS uses nine independent safety rings:

1. **Type system.** Loop states and plan variants enumerate valid control flow.
2. **Guardrails.** Input, output, and tool checks inspect content and halt with
   typed errors.
3. **Approve.** Concrete policy allows, denies, or pauses boundary actions.
   `Approve` is not an extension trait.
4. **Isolation.** A tool declares a `SandboxMode` — `read_only`,
   `workspace_write`, or `full_access`. Anything but `full_access` runs the tool
   in a child process whose filesystem writes the kernel refuses: Landlock on
   Linux, Seatbelt on macOS, inherited by every descendant. `full_access` is the
   default and means no sandbox — the honest declaration for a tool that does its
   work in-process, where the only available restriction would apply to the whole
   agent permanently; such tools are bounded by rings 2 and 3 instead. Reads and
   network are not restricted. Where no backend exists, a sandboxed tool fails
   rather than running unsandboxed, and `agentos-gateway config` prints which
   mechanism (if any) this machine enforces with.

   Two things can carry the mode, and a tool says which. The registry runs the
   tool inside an isolated executor, whose `--capabilities` handshake reports
   the tools it can execute and the modes its machine can enforce — asked, not
   assumed, because the executor is a separate binary that may be older or on a
   different kernel. Or the tool spawns and hardens its own child, declared as
   `Isolation::SelfHardened`; `shell` is the only shipped instance. A tool that
   declares a mode and offers neither is refused when the runtime starts and
   again on every call, with a typed error naming which of the three
   conditions applies: no executor, an incompatible one, or an isolated run
   that failed. See
   [`docs/adr/0002-FAIL_CLOSED_ISOLATION.md`](adr/0002-FAIL_CLOSED_ISOLATION.md).

5. **Filesystem containment.** A model-supplied path never reaches the
   filesystem as a string. `crates/agentos-core/src/paths/RootDir` holds a
   descriptor for the root and walks to the target with `openat(O_NOFOLLOW)`,
   one component at a time, then acts on the descriptor it arrived at. A
   symlink anywhere on the path is refused rather than resolved — including
   one whose target is inside the root, because deciding that safely means
   re-resolving, which is the race this replaces. Identifiers that become a
   path component (task ids, sub-agent names, orchestrator template names,
   session ids) are validated as single names by `paths::path_segment` and
   rejected if they are not; they are never rewritten, because whoever chose
   the name will use it again to find what was written under it.

6. **Subprocess environment and lifetime.** Every child the runtime spawns —
   the isolation worker, a `shell` command, a stdio MCP server — starts from
   `env_clear` and is given back a named allowlist
   (`crates/agentos-core/src/tools/child_env.rs`): `PATH`, `HOME`, `TMPDIR`,
   the locale, proxy settings, and `AGENTOS_HOME`. No provider or channel
   credential is inherited, so a tool the model drives cannot read the keys
   the gateway holds. `[isolation].env_passthrough` names anything else a
   deployment needs, by name and never by value. Each child is spawned into
   its own process group; a deadline or a cancellation signals the *group*, so
   a shell that forks does not leave a grandchild behind. A command that exits
   cleanly is left alone, because something it started on purpose is its
   business.

7. **Tool egress.** A URL the model chose is fetched through
   `crates/agentos-core/src/egress/`, never through the shared client. The
   decision is made on the *resolved address*, inside the DNS resolver: a
   hostname is not a destination, `localtest.me` resolves to `127.0.0.1`, and
   filtering after resolution and before the connection means there is no
   second lookup for a rebinding attack to win. Everything that is not
   globally routable unicast is refused, plus the cloud metadata addresses,
   which are globally routable and are the highest-value SSRF target there is.
   A literal-address URL and every redirect hop are checked separately,
   because neither reaches DNS. The ambient HTTP proxy is deliberately off for
   this client: a proxy resolves the destination itself, so every request
   would connect to one always-allowed address and hand it a hostname the
   policy never sees — on a machine behind a corporate proxy, which is exactly
   where reaching `http://internal.corp/` matters most.
   `AGENTOS_TOOL_EGRESS_PROXY=1` takes the proxy back and logs that the proxy,
   not this policy, now decides. Bodies are streamed and stop at
   `[limits].http_response_bytes`. Endpoints the operator configured — a local
   Qdrant, an Ollama, a loopback proxy — go through `http::shared_client` and
   are deliberately not policed: an address written into `agent.toml` is the
   decision.

8. **Bounded ingress.** Nothing that arrived over the wire is allowed to
   choose how much memory this process spends. Feishu's fragment reassembly
   (`channels/feishu/fragments.rs`) took `sum` straight from a frame header
   into `vec![None; sum]`, which a peer could turn into an allocation failure
   before the event was parsed, let alone admitted; it now has ceilings on
   fragments per event, on partially-received events held at once, and on the
   reassembled total. Attachment downloads stream to
   `[limits].attachment_bytes` and abandon what exceeds it, because the size a
   channel reports is the sender's claim rather than a limit the sender is
   held to, and `[limits].attachments_per_message` bounds how many downloads
   one message can cause. Provider response bodies are read to a ceiling
   rather than buffered whole.

9. **Durable safety events.** Every decision the rings above make appends a
   row to `safety_events` through `crates/agentos-core/src/audit/`: approval
   requested and resolved, policy denial, guardrail trip, sandbox refusal,
   cancellation, delegation-grant issuance and use, terminal error. The store
   is modelled on `memory_access_log` — append-only, never updated, never
   deleted on the normal path — and the events go in at the moment of the
   decision, so a run that fails still leaves them behind.

   This ring exists because the strongest signal for a *successful* approval
   used to be a deletion: `take_approved_action` removed the pending
   interruption and the CLI unlinked the paused-run file. An absence cannot be
   told apart from a record that was never written, one that was cleaned up,
   and one that was removed to hide something. A resolved interruption is now
   marked `consumed` and retained.

   What a row may carry is decided per event kind at the point of emission. A
   subject (a tool name, a guardrail name, a sub-agent id), a `Principal`, and
   a reason bounded at 512 bytes; a tool's arguments only as
   `ArgumentDigest`, which is enough to tell two events apart and to spot a
   retried call, and not enough to recover what was in it. `SafetyEvent` has
   no field that would accept the arguments, which is what makes that
   structural rather than a habit. `SafetyLog` is defined in the core, not in
   `agentos-interfaces`, for the same reason `approve` is: an extension that
   can replace the audit sink decides what the audit trail says.

   The trace is a weaker record than the store — diagnostics rather than an
   audit trail — but it used to disappear entirely on the path that needed it
   most. `RunLoopState::step` consumes the state, so a failing step dropped it
   and the runner had nothing to persist. `StepFailure` carries it back out,
   and a failed run's records are written under the `failed` phase. See
   [`docs/adr/0005-SAFETY_EVENTS.md`](adr/0005-SAFETY_EVENTS.md).

### Checked invariants

Three relationships the rings depend on are asserted in debug builds
(`crates/agentos-core/src/invariants.rs`):

- every provider message derives from `RunState` and is accounted for in the
  manifest that claims to describe the request;
- every delegation's effective policy reaches no action its parent cannot;
- every tool result in the transcript follows an assistant item carrying its
  call id.

These are **not a ring of their own**. They compile out of release builds entirely, so
they protect development rather than deployment: a violation is a bug in the
core, not a state a deployment can reach by configuration. Each states a
relationship between two pieces of authoritative state rather than re-running the
code it checks, and each has a test that violates it and observes the panic.

### Sub-agent safety

- Child policy is checked with `Policy::narrow(parent, child)`, which returns
  `Err` on any widening attempt.
- Child tools and skills are opt-in; the `memory` tool is opt-in per operation
  (`read`, `write`, `forget`).
- Child memory views are opt-in and scoped.
- Child guardrails can be inherited from the parent runtime.
- Every sub-agent tool call re-enters the loop at `Approve`, including
  MCP-originated calls.

## 11. Tools, Skills, MCP, and Resources

Tools are registered in `ToolRegistry` and surfaced as `ToolSpec`s. Parent
built-in tools are derived from `[resources.tools].enabled`.

Reference built-in tools:

| Tool | Notes |
|---|---|
| `file` | Read/write UTF-8 files; its JSON schema is built from `[limits]` so it cannot advertise a ceiling it does not enforce. |
| `http` | GET an HTTP(S) URL. |
| `shell` | Structured shell execution, gated by `[guardrails].shell_allowlist` and sandboxed `workspace_write`. |
| `memory` | Scoped read/write/forget over `MemoryManager`. |
| `skill_validate` | Validate a workspace skill bundle. |
| `cron_create`, `cron_list`, `cron_remove` | Persisted scheduled tasks. |
| `job_status`, `job_output`, `job_kill` | Background jobs (§9). |

Every tool's sandbox mode and deadline is generated into
[`TOOL_CATALOG.md`](TOOL_CATALOG.md) from its `ToolSpec`.

MCP-backed tools adapt remote tool specs into ordinary `Tool` implementations.
The orchestrator does not need to know whether a tool is local or MCP-backed.
Only MCP tools listed in `[resources.mcp].enabled` are registered.

Two clients sit behind that. `StaticMcpClient` answers from a table in
`agent.toml` and is not a protocol implementation. `StdioMcpClient` speaks the
Model Context Protocol over stdio: JSON-RPC 2.0, pinned to revision
`2025-06-18` and able to read `2025-03-26` and `2024-11-05`. It sends
`initialize` before anything else, refuses a server that offers an unreadable
revision or no `tools` capability, walks `tools/list` to the end of its
`nextCursor` chain, and maps MCP content blocks into a `ToolResult` —
`isError` becoming a failed *tool* rather than a failed call, because the model
reads it and replans.

A reply is matched to its request by JSON-RPC id. That is the load-bearing
detail: before M8 the reply was whatever line came back next, so one stray line
shifted every later answer onto the wrong question, permanently and silently.
Correlation is also what makes concurrent calls, out-of-order replies, and a
timeout that cancels one request rather than killing the server possible at
all.

Every bound is named in `tools/mcp/connection.rs`: frame bytes, in-flight
requests, stderr retained, pages walked, tools per server, restarts per window.
The server process itself runs under `[[mcp_servers]] sandbox` — the child that
touches the filesystem is the one restricted, rather than a shell-only worker
in front of it that could not run MCP calls at all. Interoperability is tested
against `tests/fixtures/mcp_server.py`, which imports nothing from this
repository.

Workspace skills are loaded from the configured `skills_dir` and filtered by
`[resources.skills].enabled`. Built-in deterministic planners can short-circuit
known workflows, but the skill files remain workspace-owned content. See
[`SKILLS.md`](SKILLS.md).

## 12. Sub-Agents and Routing

Sub-agents are configured in `subagents/*.toml` and referenced by routing rules
or sub-orchestrator templates. Each sub-agent declares:

- id and policy id;
- orchestrator and model tier;
- allowed tools;
- allowed skills and memory operations;
- memory view;
- memory domains;
- max turns;
- guardrail inheritance.

Routing rules can dispatch directly, delegate to a sub-agent, or escalate to a
template. Templates live in `suborchs/*.toml` and define ordered stages that
reference configured sub-agents.

Sub-agent execution uses bounded task communication on a `LocalSet`. Parent
trace and approval paths remain visible: delegation is an action, not a second
hidden runtime.

## 13. Memory Architecture

### Who decides what memory may do

`[memory.policy]`, and nothing else. Its three verbs — `reads`, `writes`,
`forgets` — become the `memory` tool's policy rules directly, rather than the
hardcoded allow-read/ask-write/ask-forget they used to be pre-empted by. A
`[policy] allowlist` entry naming `memory` would turn all three into a blanket
`Allow`; it used to, silently, and is now a load-time error that names both
settings so the operator says which they meant.

A write into a shared domain needs **three** permissions, and losing any one of
them refuses the write: `[memory.policy].shared_writes` for the deployment, the
`[[memory.shared_domains]]` entry's own `write = true` for the domain, and a
caller holding it — a sub-agent acting under a `shared_readwrite` memory view
that names the domain. The runtime intersects all three into
`MemoryCaller::writable_shared_domains` before `authorize_scope` runs, so the
check is one comparison and the gates cannot be applied out of order by a call
site. A top-level run declares no memory view and therefore writes to no shared
domain.

### Layers

Memory has three layers:

1. Session memory: exact transcript continuity, owned by `Session`.
2. Working memory: task-local active context, owned by `RunState`,
   `RunContext.memory_fragments`, and `TaskWorkspace`.
3. Long-term memory: scoped records, owned by `Memory` backends and mediated by
   `MemoryManager`.

The low-level `Memory` trait stays backend-oriented:

```text
write(namespace, record)
read(namespace, query)
forget(namespace, selector)
```

Scope, authorization, hydration, retention, and reflection belong above that
trait in `MemoryManager`. Similarity retrieval is a separate `SemanticIndex`
trait, which is what the vector extension implements.

### Store Types

| Store | Owner | Purpose |
|---|---|---|
| Working | `RunState`, `RunContext`, `TaskWorkspace` | Active task facts, observations, checkpoints, resume context. |
| Episodic | `Memory` backend | Completed run/session events with outcome and provenance. |
| Semantic | `Memory` backend | Durable domain facts learned from explicit writes or episodes. |
| Procedural | Skills/resource registry plus memory metadata | Repeatable workflows and skill success history. |
| Audit | Memory backend or SQLite audit table | Who read, wrote, forgot, promoted, or pruned memory. |

Sessions are source material for memory, but exact chat transcripts are not
long-term memory by themselves.

### Scope Model

Memory namespaces are derived from typed scope rather than arbitrary strings:

```text
{visibility}/{owner_kind}/{owner_id}/{store}/{domain}
```

Examples:

- `private/user/terminal/episodic/general`
- `private/agent/main-agent/semantic/agentos`
- `private/task/main/working/general`
- `shared/shared/global/semantic/agentos`
- `shared/shared/global/procedural/general`
- `private/conversation/terminal/episodic/general`

Caller view includes:

- active agent private memory;
- active task memory;
- current conversation/user memory;
- allowed shared domains;
- explicitly delegated parent fragments.

The manager rejects cross-user, cross-agent, and cross-task access unless
explicitly delegated.

### Hydration

Passive retrieval occurs in `Orchestrator::hydrate()`:

```text
RunLoopState::Plan
  -> RunContext::from_state
  -> Orchestrator::hydrate
  -> MemoryManager::hydrate
  -> RunContext.memory_fragments
  -> prompt::assemble  (memory section)
  -> Orchestrator::plan
```

Hydration is read-only, bounded, and scoped by caller view; its fragments reach
the model through a named prompt section, and the manifest records whether they
did. Budgets come from `[memory].hydrate_*`; the defaults are 5 fragments,
~1200 estimated tokens, semantic and episodic stores.

Hydration does not put full memory bodies into trace fields. Traces carry
counts, record ids, namespaces, and compact summaries.

### Explicit Memory Tool

Explicit model- or user-requested memory actions use the `memory` tool:

```text
Plan -> Approve -> Act -> ToolGuardrails -> MemoryTool -> Observe
```

Default parent policy shape:

- allow `memory` reads;
- ask user for `memory` writes;
- ask user or deny `memory` forgets.

### Episode Recording

Episode recording is a post-finish persistence side effect, not a loop state.
It runs after final output and transcript/task persistence.

Record episodes for:

- failures and denials;
- approvals;
- multi-step tool workflows;
- sub-agent workflows;
- explicit user corrections or preferences;
- explicit memory writes.

Skip trivial one-turn replies unless the user explicitly asks to remember.

### Reflection, Retention, and Indexing

Reflection runs outside the hot path, driven by `[memory.reflection]` from the
gateway's idle tick (a whole-memory `reflect_all` sweep over conversations), and
also on task-completion thresholds, contradiction detection, or an
administrative path.

Reflection can:

- promote repeated episodes into semantic facts;
- promote repeated successful workflows into procedural candidates;
- compress old episodes;
- supersede contradicted facts;
- archive low-value records by policy.

Promotion runs per conversation; **retention** and the index rebuild run once
per sweep, after it — so a fact promoted out of episodes on a tick is not
archived for age on the same tick that created it. `[memory.retention]` offers
three independent ceilings, and `memory/retention.rs` applies them in the order
age, count, bytes: a record past its age is gone whatever the other two say,
and dropping it may bring the other two back under on its own. Records are
**archived, not deleted** (`status = 'archived'`), which drops them out of
every read path while the row stays — the same choice `/clear` makes for
session items, and for the same reason: an operator who set a budget too low
should be able to see what it cost them. Retention rides the reflection sweep,
so a deployment with `[memory.reflection] enabled = false` prunes nothing.

All three ceilings, `[memory].default_domain`, and `[[memory.shared_domains]]`
parsed, validated, appeared in `docs/CONFIG_CATALOG.md`, and changed no
behaviour before M7 / `MEM-001`.

Retrieval is deterministic by default: lexical plus recency, with SQLite FTS
where available. Vector retrieval is available through `[memory].semantic_backend`
— the built-in `sqlite_vec` index or the `agentos-memory-vector` extension,
which can embed against an OpenAI-compatible `/embeddings` endpoint and falls
back to a hashing embedder offline.

## 14. Memory Record Schema

Managed records keep `Record.body` as JSON and standardize metadata.

Common metadata:

```json
{
  "store": "episodic",
  "owner_kind": "user",
  "owner_id": "terminal",
  "visibility": "private",
  "domain": "agentos",
  "source_agent_id": "main-agent",
  "source_task_id": "main",
  "source_run_id": "cli-run-1",
  "conversation_id": "terminal",
  "importance": 0.0,
  "confidence": 1.0,
  "status": "active",
  "schema": "agentos.memory.v1"
}
```

Semantic facts are revisable. Contradicted facts should be marked
`superseded` and linked to replacements instead of overwritten.

Forget, prune, supersede, and archive are distinct:

- Forget: explicit deletion or suppression request.
- Prune: budget management.
- Supersede: keep provenance while replacing stale knowledge.
- Archive: remove from default retrieval without deleting.

## 15. SQLite Memory Layout

The SQLite reference backend uses additive schema evolution. The base table is:

```sql
memory_records(row_id, id, namespace, body_json, metadata_json, created_at)
```

Managed memory adds columns such as:

- `updated_at`
- `last_accessed_at`
- `access_count`
- `status`
- `store`
- `owner_kind`
- `owner_id`
- `visibility`
- `domain`
- `source_run_id`
- `source_task_id`
- `source_agent_id`

Supporting tables:

- `memory_links` for provenance, supersession, promotion, and compression
  relationships.
- `memory_access_log` for reads, writes, forgets, promotions, and prunes.
- `safety_events` for every safety-boundary decision (§10, ring 9). Append-only
  and never updated. The principal is stored twice — as `Principal::storage_name`
  for exact lookup and as its components, so "everything this channel was
  allowed to do" is a query a human can write.
- `session_items` and `session_epochs` for the append-only session log and the
  `/clear` projection over it (ADR-0006).
- `ingress_events` and `ingress_cursors` for the gateway's durable record of
  what it has accepted from a transport and how far it got (§9).
- `maintenance_leases` for the single-sweeper election. One row, taken with a
  single `INSERT … ON CONFLICT DO UPDATE … WHERE`, so SQLite's own write
  serialization picks the winner across threads *and* across processes.
- Optional FTS/vector tables for retrieval acceleration.

Existing databases open through idempotent migrations.

### Connections

The database is opened in WAL mode with a five-second busy timeout, behind a
fixed-size connection pool (`[memory] max_connections`, default 4). WAL is what
makes concurrent access work at all: a reader never blocks a writer, and — the
half no in-process lock can fix — the gateway and the TUI are two processes on
one file. Connections are created up front rather than on demand, because a
pool that grows under load is an unbounded queue with a different name.

These are still synchronous calls made from async code. A pooled connection is
never held across an `.await`, but a slow statement occupies the calling
thread; moving the store behind `spawn_blocking` is a separate change with its
own `!Send` consequences for the run loop.

## 16. Observability

Trace spans and audit logs cover:

- run start/finish;
- planning and LLM calls, with per-call and per-run token usage
  (`call_*_tokens`, `run_*_tokens`);
- the request header for every provider call: sections included, estimated
  tokens, context budget, pressure percent, elided messages and characters;
- tool start/end, including spill locators and truncation counts;
- approval decisions, prompts, tickets, and pauses;
- guardrail trips;
- handoffs, delegation, and escalation;
- memory hydrate started/finished, candidate and selected counts;
- managed writes/forgets;
- episode recorded/skipped;
- reflection started/finished;
- compaction passes and context-overflow retries;
- cancellation and steering;
- cron ingress and persistent-channel loops.

Trace records carry `emitted_unix` so an audit can window by event time. Full
sensitive memory bodies are never written to traces.

## 17. Current State

Completed baselines:

- typed loop states and trace shape;
- concrete approval, serializable paused runs, and correlated approval tickets
  with expiry;
- reference tools and guardrails, tool deadlines, and background jobs;
- kernel sandboxing (Landlock/Seatbelt) for tools declaring a `SandboxMode`;
- one prompt-assembly authority, request manifests, transcript projection,
  token estimation with live calibration, spill, elision, span compaction,
  context-overflow recovery, and session fork;
- run cancellation and mid-run steering;
- per-conversation actors sharded across threads;
- SQLite session and memory backend, memory manager, hydration, scoped memory
  tool, episodes, sub-agent views, reflection sweeps, and vector retrieval
  through the first extension crate;
- streaming across the OpenAI, Anthropic, and DeepSeek providers, wired through
  the loop, both orchestrators, the TUI, and edit-in-place Telegram/Feishu
  egress (Ollama uses the single-chunk fallback);
- Telegram and Feishu reference channels;
- configured sub-agents and sub-orchestrator templates;
- static MCP declarations and a stdio MCP client speaking the pinned protocol
  revision, with the server process sandboxed;
- config authority with generated catalogs, effective-config diagnostics, and
  debug-build invariant assertions;
- runtime path injection and an enforced extension boundary.

Remaining architecture work:

- **X7, folded collaboration state** — the one open item in
  [`TRANSFER_ROADMAP.md`](TRANSFER_ROADMAP.md).
- An MCP transport other than stdio (HTTP with SSE), and the client-side
  capabilities — `roots`, `sampling`, `elicitation` — this build declines in
  `initialize` and therefore refuses when a server asks.
- Retention and quotas across sessions, traces, attachments, gateway logs, and
  jobs, plus an authorized purge for the two append-only audit stores
  (`QUOTA-001`).
- Decide when channel and memory reference implementations should move into
  separate extension crates.
- Make real embeddings the persistent default by injecting an embedder into the
  built-in `sqlite_vec` index (deferred `FEATURE_ROADMAP.md` follow-up).
- Design the link-time extension registry if out-of-tree extensions materialize
  (deferred `FEATURE_ROADMAP.md` C2).
- Publish stable extension-author docs when `agentos-interfaces` reaches a
  release boundary.

## 18. Verification Matrix

Run before claiming architecture milestone completion — this is what CI runs:

```sh
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace -- -D warnings
cargo test --workspace
bash scripts/check-import-boundaries.sh
bash scripts/check-module-size.sh
bash scripts/check-catalogs.sh
cargo bench -p agentos-core -- --test    # smoke: benches still compile and run
```

Run when public interfaces change:

```sh
cargo semver-checks check-release -p agentos-interfaces --baseline-rev HEAD
```

The interface crates are unpublished, so `--baseline-rev` is required; CI runs
this advisory (non-blocking) against the PR base commit for both
`agentos-interfaces` and `agentos-proto`.

Run when memory behavior changes:

```sh
cargo test -p agentos-core memory
cargo test -p agentos-core memory_tool
cargo test -p agentos-core reflection
```

Run when approval or sub-agent policy behavior changes:

```sh
cargo test -p agentos-core approve
cargo test -p agentos-core subagents
cargo test -p agentos-core runner
```

Run when request assembly, projection, or compaction changes:

```sh
cargo test -p agentos-core --test transcripts          # golden transcripts
cargo test -p agentos-core --test transcripts_context
cargo test -p agentos-core --test compaction
cargo test -p agentos-core --test session_fork
```

## 19. Open Decisions

- Whether to introduce first-class `UserId` in `agentos-proto`.
- Whether session and memory should split into separate SQLite files by default.
- Whether automatic episode recording should be enabled by default for all
  deployments.
- How much memory access logging is required for the target deployment threat
  model.
- When to split reference channel adapters from `agentos-core`.
- Whether the token estimator should become a real tokenizer once calibration
  data shows where the heuristic costs the most.

Resolved since the last consolidation: the extension mechanism (compiled-in,
config-selected; dynamic loading rejected), the first vector backend
(`sqlite_vec` built in, `agentos-memory-vector` as the extension), the scaling
model (thread-per-core, adopted deliberately in G1), and the shape of ring 4
(kernel sandbox, not a bare subprocess).
