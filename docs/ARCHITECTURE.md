# AgentOS Architecture Design Document

Status: active design baseline
Last consolidated: 2026-05-13

This document is the unified architecture reference for AgentOS. It supersedes
the older standalone extension-boundary, memory-system, and development-plan
documents while preserving their current design decisions.

## 1. Purpose

AgentOS is a Rust agent runtime with a typed run loop, concrete approval policy,
pluggable extension traits, persistent sessions, scoped memory, tool execution,
workspace skills, sub-agent orchestration, MCP-backed tools, and channel
gateways.

The core design goal is simple: every run follows one auditable loop, and every
external capability crosses a narrow trait or policy boundary.

## 2. Core Invariants

- The run loop owns control flow.
- `Approve` is concrete policy, not an extension trait.
- Guardrails inspect content; approval checks permission.
- Passive memory retrieval hydrates planning context; explicit memory mutation
  goes through tools and approval.
- Sub-agent permissions can only narrow parent permissions.
- External implementation contracts live in `agentos-interfaces`.
- Workspace-owned content is data, not a dependency of core crates.
- Runtime paths are injected by CLI/gateway construction, not inferred from
  scattered core defaults.

## 3. Crate Layout

Active crates:

- `agentos-proto`: serializable wire and domain types.
- `agentos-interfaces`: public extension traits and shared run-state types.
- `agentos-core`: run loop, runner, gateway service, approval engine,
  guardrails, reference tools, memory/session stores, sub-agent execution,
  channel adapters, runtime construction, and config parsing.
- `agentos-llm`: provider-neutral LLM facade and provider adapters.
- `agentos-cli`: TUI, one-shot channel entry points, persistent gateway,
  slash commands, startup environment loading, and runtime path construction.

Workspace-owned content lives under `workspace/` and is loaded through config:

- `agent.toml`
- `skills/`
- `subagents/`
- `suborchs/`
- `crons/`
- `tasks/`
- runtime state such as session DBs, traces, run snapshots, and attachments

## 4. Public Extension Boundary

Extension authors should implement traits from `agentos-interfaces`:

- `Channel` for ingress and egress adapters.
- `Tool` for callable capabilities.
- `Skill` for provider-neutral skill execution.
- `McpClient` for MCP transports and servers.
- `Memory` and `Session` for storage implementations.
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
  picks the built-in stores, channels and tools are enabled by name. There is
  no plugin loader; the `extensions/` directory is a home for future
  first-party extension crates, not a drop-in mechanism.
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
  required first.
- `Approve` stays a concrete engine regardless; the extension surface is
  orchestrators, memory, tools, channels, skills, and guardrails only.

Boundary enforcement:

- `scripts/check-import-boundaries.sh` enforces compile-time dependency
  boundaries (with a `--self-test` negative-fixture mode).
- Core crates must not import workspace-owned code or extension crates.
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

Channel attachment directories are selected by the CLI or gateway and passed to
reference channel adapters. Standalone channel construction still supports
`AGENTOS_ATTACHMENTS_DIR` as an explicit override.

This keeps path decisions at runtime entrypoints while preserving compatibility
with built-in tools that resolve paths from environment variables.

## 6. Configuration Authority

`workspace/agent.toml` is intended to describe the effective runtime. The
canonical loader is `WorkspaceConfig::load()`, which:

- parses the main TOML;
- resolves relative paths against the config directory;
- loads `subagents/*.toml`;
- loads `suborchs/*.toml`;
- validates memory, policy, channels, resources, sub-agents, templates, and
  routing.

Effective config areas:

- `[agent]`: id, orchestrator, memory id, and `max_turns`.
- `[policy]`: parent policy default decision.
- `[memory]`: backend, hydration, retention, and shared-domain policy.
- `[channels.*]`: TUI and persistent-channel enablement/modes.
- `[resources.skills]`: enabled workspace skills.
- `[resources.tools]`: parent built-in tools.
- `[resources.mcp]`: MCP tools that can be registered and surfaced.
- `[resources.llm]`: LLM fallback resource labels.
- `[[mcp_servers]]` and `[[mcp_tools]]`: static or stdio MCP declarations.
- `[[routing.rules]]` and sub-orchestrator templates: routing table.
- `[task_workspace]`: task-state root.

Gateway persistent channel precedence:

1. `AGENTOS_ENABLED_CHANNELS=telegram,feishu` overrides workspace channel
   enablement.
2. Without that override, `[channels.telegram].enabled` and
   `[channels.feishu].enabled` control persistent gateway channels.
3. `channels.tui` is for the interactive TUI and is never started by the
   persistent gateway.

Diagnostic command:

```sh
agentos-gateway config --config workspace/agent.toml
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
transitions become compile-time or explicit runtime errors.

Plan variants:

- `Reply`: terminal assistant output.
- `CallTool`: execute a local or MCP-backed tool.
- `Handoff`: switch active agent and continue planning.
- `Delegate`: run a sub-agent and return the child result to the parent.
- `Escalate`: execute a configured sub-orchestrator template.

Guardrail placement:

- Input guardrails run in `Start`.
- Tool guardrails run in `Act`.
- Output guardrails run before terminal replies finish.

Approval placement:

- Every non-terminal action crosses `Approve`.
- `allow` proceeds to `Act`.
- `deny` terminates with a policy error.
- `ask_user` serializes a paused `RunState`.

## 8. Safety Architecture

AgentOS uses four independent safety rings:

1. Type system: loop states and plan variants enumerate valid control flow.
2. Guardrails: input, output, and tool checks inspect content.
3. Approve: concrete policy allows, denies, or pauses boundary actions.
4. Isolation: tools marked `requires_isolation` run through a subprocess worker.

Sub-agent safety:

- Child policy is checked with `Policy::narrow(parent, child)`.
- Child tools and skills are opt-in.
- Child memory views are opt-in and scoped.
- Child guardrails can be inherited from the parent runtime.

## 9. Tools, Skills, MCP, and Resources

Tools are registered in `ToolRegistry` and surfaced as `ToolSpec`s. Parent
built-in tools are derived from `[resources.tools].enabled`.

Reference built-in tools include:

- `file`
- `http`
- `shell`
- `memory`
- `skill_validate`
- cron creation/list/removal tools

MCP-backed tools adapt remote tool specs into ordinary `Tool` implementations.
The orchestrator should not need to know whether a tool is local or MCP-backed.
Only MCP tools listed in `[resources.mcp].enabled` are registered as available.

Workspace skills are loaded from the configured `skills_dir` and filtered by
`[resources.skills].enabled`. Built-in deterministic planners can short-circuit
known workflows, but the skill files remain workspace-owned content.

## 10. Sub-Agents and Routing

Sub-agents are configured in `subagents/*.toml` and referenced by routing rules
or sub-orchestrator templates. Each sub-agent declares:

- id and policy id;
- orchestrator and model tier;
- allowed tools;
- allowed skills;
- memory view;
- memory domains;
- max turns;
- guardrail inheritance.

Routing rules can dispatch directly, delegate to a sub-agent, or escalate to a
template. Templates live in `suborchs/*.toml` and define ordered stages that
reference configured sub-agents.

Sub-agent execution uses bounded task communication. Parent trace and approval
paths remain visible: delegation is an action, not a second hidden runtime.

## 11. Memory Architecture

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
trait in `MemoryManager`.

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
  -> Orchestrator::plan
```

Hydration is read-only, bounded, and scoped by caller view. Initial budgets:

- max fragments: 5
- max estimated tokens: 1200
- stores: semantic and episodic by default

Hydration should not put full memory bodies into trace fields. Use counts,
record ids, namespaces, and compact summaries.

### Explicit Memory Tool

Explicit model or user requested memory actions use the `memory` tool:

```text
Plan -> Approve -> Act -> ToolGuardrails -> MemoryTool -> Observe
```

Default parent policy shape:

- allow `memory` reads;
- ask user for `memory` writes;
- ask user or deny `memory` forgets.

The `remember:` and `recall:` deterministic commands are smoke-test paths over
this model, not the architecture itself.

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

Reflection runs outside the hot path:

- on cron;
- after task completion thresholds;
- after contradiction detection;
- manually through an administrative path.

Reflection can:

- promote repeated episodes into semantic facts;
- promote repeated successful workflows into procedural candidates;
- compress old episodes;
- supersede contradicted facts;
- archive or prune low-value records by policy.

Retrieval starts deterministic: lexical plus recency, with SQLite FTS where
available. Vector embeddings remain optional backend capability, not a hot-path
requirement.

## 12. Memory Record Schema

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

## 13. SQLite Memory Layout

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

Existing databases should open through idempotent migrations.

## 14. Observability

Trace spans and audit logs should cover:

- run start/finish;
- planning and LLM calls;
- tool start/end;
- approval decisions and pauses;
- guardrail trips;
- handoffs, delegation, and escalation;
- memory hydrate started/finished;
- memory candidate and selected counts;
- managed writes/forgets;
- episode recorded/skipped;
- reflection started/finished;
- cron ingress and persistent-channel loops.

Avoid full sensitive memory bodies in traces.

## 15. Development Roadmap

The original phase roadmap remains useful as historical sequencing, but the
current architecture is milestone-driven.

Completed or active baselines:

- typed loop states and trace shape;
- concrete approval and serializable paused runs;
- reference tools and guardrails;
- SQLite session and memory backend;
- memory manager, hydration, scoped memory tool, episodes, sub-agent views, and
  reflection/indexing paths;
- Telegram and Feishu reference channels;
- configured sub-agents and sub-orchestrator templates;
- static and stdio MCP registration;
- subprocess isolation for marked tools;
- config authority and effective-config diagnostics;
- runtime path injection and extension-boundary documentation.

Remaining architecture work:

- harden stdio MCP toward the selected production protocol;
- broaden subprocess isolation beyond the reference shell worker if needed;
- continue shrinking legacy comments/examples that imply hardcoded workspace
  paths;
- decide when channel and memory reference implementations should move into
  separate extension crates;
- publish stable extension-author docs when `agentos-interfaces` reaches a
  release boundary.

## 16. Verification Matrix

Run before claiming architecture milestone completion:

```sh
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
bash scripts/check-import-boundaries.sh
scripts/check-module-size.sh
```

Run when public interfaces change:

```sh
cargo semver-checks check-release -p agentos-interfaces
```

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

## 17. Open Decisions

- Whether to introduce first-class `UserId` in `agentos-proto`.
- Whether session and memory should split into separate SQLite files by default.
- Whether automatic episode recording should be enabled by default for all
  deployments.
- Which vector backend should become the first memory extension target if
  embeddings are added.
- How much memory access logging is required for the target deployment threat
  model.
- When to split reference channel adapters from `agentos-core`.
