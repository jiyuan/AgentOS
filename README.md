# AgentOS

AgentOS is an agent-agnostic Rust agent runtime built around one auditable run
loop. Every run follows the same typed state machine, and every external
capability crosses a narrow trait or policy boundary.

Highlights:

- a typed run loop (`Start → Plan → Approve → Act → Observe`) with serializable
  paused runs
- a concrete approval policy plus input/output/tool guardrails
- pluggable extension traits (`Channel`, `Tool`, `Skill`, `McpClient`,
  `Memory`, `Session`, `Orchestrator`)
- sub-agents and routing whose permissions can only narrow the parent's
- scoped three-layer memory (session, working, long-term) on a SQLite reference
  backend
- static and stdio MCP-backed tools, with subprocess isolation for tools marked
  `requires_isolation`
- an interactive TUI and a persistent gateway for Telegram and Feishu

## Quick start

From a source checkout:

```sh
cp .env.example .env
scripts/install-agentos.sh --from-source
~/.local/bin/agentos tui
```

From a packaged release bundle:

```sh
tar -xzf agentos-v0.2.0-<platform>-<arch>.tar.gz
cd agentos-v0.2.0-<platform>-<arch>
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
- `agentos-core`: run loop, runner, gateway service, approval engine,
  guardrails, reference tools, memory/session stores, sub-agent execution,
  channel adapters, and config parsing.
- `agentos-llm`: provider-neutral LLM facade and provider adapters.
- `agentos-cli`: TUI, one-shot channel entry points, persistent gateway, and
  runtime path construction.

Four independent safety rings protect every run: the type system enumerates
valid control flow, guardrails inspect content, the concrete `Approve` engine
allows/denies/pauses boundary actions, and subprocess isolation contains tools
that request it. Workspace-owned content (`agent.toml`, `skills/`,
`subagents/`, `suborchs/`, `crons/`, `tasks/`, runtime state) is loaded as data
through config, never linked as a dependency.

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
- [Release Notes](docs/RELEASE_NOTES.md)

## Release artifacts

Packaged releases are written to `dist/` by `scripts/package-release.sh`.
