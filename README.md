# AgentOS

AgentOS is an agent-agnostic Rust agent runtime built around one auditable run
loop. Every run follows the same typed state machine, and every external
capability crosses a narrow trait or policy boundary.

Highlights:

- a typed run loop (`Start → Plan → Approve → Act → Observe`) with serializable
  paused runs
- a concrete approval policy plus input/output/tool guardrails, with approval
  prompts that only the ticket they issued can answer
- one module that assembles every provider request, and a manifest on each
  request recording what went into it
- a session log that is append-only under tool-output spill, elision under
  context pressure, span compaction, and conversation fork — each a projected
  view rather than a rewrite (`/clear` is the one path that still deletes)
- pluggable extension traits (`Channel`, `Tool`, `Skill`, `McpClient`,
  `Memory`, `Session`, `SemanticIndex`, `Orchestrator`)
- sub-agents and routing that cannot reach a tool the parent does not hold
  (a tool the parent *does* hold is currently granted to the sub-agent
  unconditionally — see `AUTH-002` in `docs/AUDIT_REMEDIATION_PLAN.md`)
- scoped three-layer memory (session, working, long-term) on a SQLite reference
  backend, with optional vector retrieval
- static and stdio MCP-backed tools, kernel-sandboxed (Landlock/Seatbelt) for
  tools that declare a `SandboxMode`
- per-tool deadlines, run cancellation, mid-run steering, and background jobs
  for work that outlives a turn
- an interactive TUI and a persistent gateway for Telegram and Feishu that runs
  conversations concurrently

## Quick start

From a source checkout:

```sh
cp .env.example .env
scripts/install-agentos.sh --from-source
~/.local/bin/agentos tui
```

From a packaged release bundle:

```sh
tar -xzf agentos-v<version>-<platform>-<arch>.tar.gz
cd agentos-v<version>-<platform>-<arch>
scripts/install-agentos.sh
~/.local/bin/agentos tui
```

## Architecture overview

AgentOS is layered into an immutable core engine, an agent-owned workspace, and
extension implementations behind the `agentos-interfaces` traits. Extensions
are compiled in and selected through `agent.toml` (there is no dynamic plugin
loading — see the extension boundary section of the architecture doc). Core
crates must never depend on workspace or extension content.

- `agentos-proto`: serializable wire and domain types.
- `agentos-interfaces`: public extension traits and shared run-state types.
- `agentos-core`: run loop, prompt assembly, runner, gateway service, approval
  engine, guardrails, reference tools, memory/session stores, sub-agent
  execution, sandboxing, background jobs, channel adapters, and config parsing.
- `agentos-llm`: provider-neutral LLM facade and provider adapters.
- `agentos-cli`: TUI, one-shot channel entry points, persistent gateway, and
  runtime path construction.

Four independent safety rings protect every run: the type system enumerates
valid control flow, guardrails inspect content, the concrete `Approve` engine
allows/denies/pauses boundary actions, and a kernel sandbox (Landlock on Linux,
Seatbelt on macOS) contains the child processes of tools that declare a
`SandboxMode`. Three further relationships the rings depend on are asserted in
debug builds and compiled out of release ones.

Workspace-owned content (`agent.toml`, `skills/`, `subagents/`, `suborchs/`,
`crons/`, `tasks/`, runtime state) is loaded as data through config, never
linked as a dependency.

See the [Architecture Design Document](docs/ARCHITECTURE.md) for the full run
loop model, memory architecture, and extension boundary.

## Main scripts

- `scripts/install-agentos.sh`: install AgentOS from source or a release bundle
- `scripts/start-agentos.sh`: start the TUI, one-shot channel runs, or the persistent gateway
- `scripts/package-release.sh`: build and package a release archive
- `scripts/bootstrap-agentos.sh`: developer-oriented source checkout bootstrap

## Documentation

- [Install Guide](docs/INSTALL.md)
- [User Guide](docs/USER_GUIDE.md)
- [Architecture Design Document](docs/ARCHITECTURE.md)
- [Skills Guide](docs/SKILLS.md)
- [Config catalog](docs/CONFIG_CATALOG.md) and [tool catalog](docs/TOOL_CATALOG.md) — generated from the code
- [Release Notes](docs/RELEASE_NOTES.md)

## Release artifacts

Packaged releases are written to `dist/` by `scripts/package-release.sh`.
