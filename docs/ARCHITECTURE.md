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
  that log rather than a rewrite of it — except for `/clear`, which still
  deletes items outright ([ADR-0006](adr/0006-CLEAR_EPOCH.md), M6).
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
| `gateway/` | Bounded ingress queue, per-conversation inbox, and the shard threads that run conversations concurrently. |
| `runner/` | Envelope-to-run driving, task sessions, episode recording. |
| `runtime/` | `AgentRuntime` construction, `RuntimePaths`, tool/MCP wiring, policy build. |
| `config/` | `agent.toml` parsing, normalization, validation, and catalog rendering. |
| `memory/` | `MemoryManager`, scope/authorization, SQLite, sqlite-vec, Qdrant, hybrid retrieval, reflection. |
| `tools/` | Registry, execution, deadlines, built-ins, MCP adapters, the `memory` tool. |
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
  above the elision threshold.

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

Every transition consumes the previous state and returns the next state. Invalid
transitions are not representable by the public API. No feature since the
original loop has added a state or a transition; everything below attaches to an
existing state or runs outside the loop entirely.

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
  overflow.

Approval placement:

- Every non-terminal action crosses `Approve`.
- `allow` proceeds to `Act`.
- `deny` terminates with a policy error.
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
- **Manifest.** Each call returns a `PromptManifest`, recorded as a
  `RequestHeader` in the trace, so "what did the model see" is answered from the
  trace rather than by re-reading the code.
- **Projection.** The session log is append-only — `/clear` excepted, see M6 in
  [`AUDIT_REMEDIATION_PLAN.md`](AUDIT_REMEDIATION_PLAN.md).
  `prompt::projection` computes the model-visible view over it. A *checkpoint* is an ordinary transcript item
  that summarizes an inclusive range of earlier positions; the projection folds
  that range out. This is what lets compaction, fork, and elision each be a read
  rather than a bespoke mutation path.
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
- **Elision.** Under pressure, the middle of an already-recorded tool result is
  replaced by a marker citing its locator. Free, deterministic, and a view
  rather than a rewrite.
- **Compaction.** When elision is not enough, one model call summarizes the
  oldest span into a checkpoint. Governed by `[compaction]`; the tail
  (`retain_tail_turns`) stays verbatim.
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

## 10. Safety Architecture

AgentOS uses four independent safety rings:

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

### Checked invariants

Three relationships the rings depend on are asserted in debug builds
(`crates/agentos-core/src/invariants.rs`):

- every provider message derives from `RunState` and is accounted for in the
  manifest that claims to describe the request;
- every delegation's effective policy reaches no action its parent cannot;
- every tool result in the transcript follows an assistant item carrying its
  call id.

These are **not a fifth ring**. They compile out of release builds entirely, so
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
- archive or prune low-value records by policy.

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
- Optional FTS/vector tables for retrieval acceleration.

Existing databases open through idempotent migrations.

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
- static and stdio MCP registration;
- config authority with generated catalogs, effective-config diagnostics, and
  debug-build invariant assertions;
- runtime path injection and an enforced extension boundary.

Remaining architecture work:

- **X7, folded collaboration state** — the one open item in
  [`TRANSFER_ROADMAP.md`](TRANSFER_ROADMAP.md).
- Harden stdio MCP toward the selected production protocol.
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
