# Agent OS Development Plan

Last reconstructed: 2026-05-13

This plan reflects the current codebase and the architectural audit performed on
2026-05-13. It intentionally replaces earlier phase-status claims that no longer
match the implementation.

## Current Snapshot

AgentOS is a Rust workspace with these active crates:

- `agentos-proto`: wire/data types.
- `agentos-interfaces`: public extension traits and shared run-state types.
- `agentos-core`: run loop, runner, gateway service, concrete approve engine,
  built-in tools, guardrails, memory/session stores, sub-agent execution,
  channel adapters, runtime construction, and workspace config parsing.
- `agentos-llm`: provider-neutral LLM facade and provider adapters.
- `agentos-cli`: TUI, one-shot channel entry points, persistent gateway binary,
  slash commands, and user-facing startup flows.

The runtime currently supports:

- Typed loop states: `Start`, `Plan`, `Approve`, `Act`, `Observe`, `Paused`,
  and `Finish`.
- Concrete approval policy with `allow`, `deny`, and `ask_user`.
- Tool execution through `ToolRegistry`, including built-in `shell`, `http`,
  `file`, `memory`, cron, skill validation, and MCP-backed tools.
- Reference guardrails: `PiiFilter`, `MaxOutputLength`, and
  `ShellCommandAllowlist`.
- SQLite-backed session and memory storage, plus optional semantic indexes.
- Workspace skills loaded from `workspace/skills`.
- Configured sub-agents and sub-orchestrator templates loaded from
  `workspace/subagents/*.toml` and `workspace/suborchs/*.toml`.
- Telegram and Feishu channel adapters.
- Static and stdio MCP tool registration.
- Subprocess isolation support for tools marked `requires_isolation`.
- Packaging and install scripts for source and release bundles.

## Verification Baseline

Verified after initial invariant work on 2026-05-13:

- `cargo fmt --all --check` passes.
- `cargo check --workspace` passes.
- `cargo test --workspace` passes.
- `cargo test -p agentos-core approve` passes.
- `scripts/check-import-boundaries.sh` passes.
- `scripts/check-module-size.sh` passes.

Known verification warning:

- `scripts/check-module-size.sh` still reports the existing
  `crates/agentos-cli/src/bin/agentos-gateway.rs` size as allowlisted legacy
  debt.

## Architectural Findings To Resolve

### A1. Parent Policy Is Widened By Child Tool Declarations

Current behavior after first remediation pass:

- Parent policy is derived from parent-owned `resources.tools.enabled`, plus
  configured MCP tool specs.
- Sub-agent tool declarations no longer add parent tool permissions.
- `file` reads are allowed when `file` is enabled; `file` writes require user
  approval.
- Sub-agent file policy uses the same read/approval split, so it can narrow the
  parent policy instead of requesting broad file access.

Why this matters:

- It contradicts the invariant that sub-agent policies only narrow parent
  capability.
- It makes a child declaration a parent permission grant.

Remaining target:

- Extend coverage from unit policy tests into runner/config smoke tests.
- Decide how nested sub-agent approval should behave when a child asks to write
  a file.

Exit checks:

- Unit tests proving parent policy does not inherit child tool permissions.
- Unit tests proving parent file writes require approval even when a child
  declares `file`.
- Unit tests proving a file-capable child policy narrows parent file policy.
- Runner-level tests for the same invariants.

### A2. Workspace Config Is Not The Runtime Source Of Truth

Current behavior:

- `workspace/agent.toml` includes `[policy]`, `[channels.*]`,
  `[resources.tools]`, and `[agent].max_turns`.
- Runtime applies `[policy].default` to the parent approval policy.
- Runtime channel selection defaults to `[channels.*]`, with
  `AGENTOS_ENABLED_CHANNELS` as an explicit override.
- Runtime now registers parent built-in tools from `[resources.tools].enabled`.
- Runtime registers MCP tools only when listed in `[resources.mcp].enabled`.
- Runtime exposes configured skills, tools, MCP tools, and LLM fallback through
  the effective resource index.
- Main runner `max_turns` now comes from `[agent].max_turns`.

Why this matters:

- Operators cannot reason from `workspace/agent.toml` to effective runtime
  behavior.
- Documentation and config imply a declarative runtime model that does not
  exist yet.

Remaining target:

- Extend smoke coverage around installed CLI wrapper paths.

Exit checks:

- Add config-loading tests that assert effective `max_turns`, enabled tools,
  channel enablement, and policy behavior.
- Add a CLI/gateway smoke test that proves `workspace/agent.toml` controls the
  expected runtime paths.

### A3. Import-Boundary Rule And Codebase Have Diverged

Current behavior after first remediation pass:

- Project docs say `agentos-core` must not depend on `workspace/` or
  `extensions/`.
- The import-boundary script now enforces the compile-time dependency boundary
  and no longer flags runtime path strings.
- The script is executable and passes locally.
- Runtime-owned roots for file tools, skills, and crons are injected through
  `RuntimePaths`.
- CLI/gateway-owned attachment roots are passed into channel adapters.
- `agentos-core` still knows the configured sub-agent and sub-orchestrator
  directory names relative to `agent.toml`; that is now the canonical config
  loader contract rather than an extension dependency.

Why this matters:

- CI cannot enforce the stated invariant as written.
- The boundary between immutable core and agent-owned workspace is unclear.

Remaining target:

- Continue shrinking legacy comments and examples that mention old
  `workspace/...` paths directly.

Exit checks:

- `scripts/check-import-boundaries.sh` passes.
- Add a negative fixture or script mode proving actual dependency/path imports
  are rejected without flagging harmless docs/comments.

### A4. Two Workspace Config Loaders Produce Different Results

Current behavior:

- `WorkspaceConfig::load()` parses the main TOML, resolves paths, loads
  `subagents/*.toml` and `suborchs/*.toml`, and validates the effective config.
- `runtime::load_workspace_config()` is a compatibility wrapper around
  `WorkspaceConfig::load()`.

Why this matters:

- Callers using the public-looking loader get a different runtime model than
  `AgentRuntime::build`.
- Tests can pass against one loader while production uses another.

Resolution target:

- Decide whether the compatibility wrapper should remain public. Scheduled as
  Phase 2.3 of [`OPTIMIZATION_ROADMAP.md`](OPTIMIZATION_ROADMAP.md).

Exit checks:

- Add tests proving sub-agent and sub-orchestrator file loading works through the
  canonical loader.
- Remove or restrict the divergent loader.

### A5. Required Regression Tests Are Missing Or Stale

Current state:

- The workspace test suite passes, but observed tests do not cover several
  documented load-bearing scenarios:
  - adversarial max-turn loop,
  - `Policy::narrow` property-style coverage,
  - paused `RunState` JSON round trip through resume with trace continuity,
  - parent/child policy narrowing across configured sub-agents,
  - import-boundary negative fixture.

Resolution target:

- Rebuild the test plan around architectural invariants, not phase history.
- Keep tests targeted and deterministic.
- Scheduled as Phase 2.1 of [`OPTIMIZATION_ROADMAP.md`](OPTIMIZATION_ROADMAP.md).

Exit checks:

- `cargo test --workspace` includes invariant coverage for run loop, approve,
  resume, config, sub-agent narrowing, and boundary scripts.

### A6. Module-Size Governance Is Not Enforced

Current behavior after first remediation pass:

- The module-size script runs on the default macOS Bash.
- The new runtime tool/policy configuration code lives in
  `runtime/tools_config.rs` instead of further enlarging `runtime/mod.rs`.
- The gateway binary remains an allowlisted legacy-size offender.

Remaining target:

- Allowlist only entry-point binaries or explicitly tracked legacy files.
- Split high-churn core modules before adding new behavior.

Exit checks:

- `scripts/check-module-size.sh` runs successfully.
- New code does not enlarge already-over-budget modules.

## Immediate Milestone: Stabilize Invariants

Goal: make the implemented architecture match the safety invariants before
adding new capabilities.

Tasks:

1. Done: repair parent policy derivation so child declarations cannot widen
   parent permissions.
2. Done: make `agent.max_turns` effective and remove hardcoded main-run turn
   limits.
3. Done: reconcile parent built-in tool registration and parent policy with
   `resources.tools.enabled`.
4. Done: choose and document the real import-boundary invariant in the checker.
5. Done: fix `check-import-boundaries.sh` permissions and behavior.
6. Done: fix `check-module-size.sh`.
7. Add invariant tests for policy narrowing, max turns, and pause/resume.

Exit criteria:

- `cargo check --workspace` passes.
- `cargo test --workspace` passes.
- `bash scripts/check-import-boundaries.sh` passes.
- `scripts/check-module-size.sh` passes.
- A parent configured for read-only file access cannot write files directly or
  indirectly through child tool declarations.

## Next Milestone: Config Authority

Goal: make `workspace/agent.toml` a reliable description of the effective
runtime.

Tasks:

1. Done: define the effective config schema in docs and tests.
2. Done: consolidate workspace config loading.
3. Done: implement `[policy].default`.
4. Done: implement `[channels.*]` gateway enablement.
5. Done: make `[resources.tools]`, `[resources.skills]`, `[resources.mcp]`, and
   `[resources.llm]` reflect actual runtime availability.
6. Done: add an effective-config diagnostic command.

Exit criteria:

- A test can load `workspace/agent.toml` and assert the exact effective runtime
  resources.
- Gateway channel selection has a documented precedence order.
- Default config contains no inert keys unless they are explicitly marked
  reserved/future.

## Following Milestone: Extension Boundary Cleanup

Goal: restore a clean extension model without losing current functionality.

Tasks:

1. Done: document that channel adapters remain in `agentos-core` as reference
   adapters until separate extension crates exist.
2. Done: document Qdrant/sqlite-vec as core reference memory implementations.
3. Done: move runtime-owned workspace paths toward `RuntimePaths` and
   CLI/gateway path construction.
4. Done: document `agentos-interfaces` as the only trait boundary for external
   implementations.
5. Not required: no `agentos-interfaces` public API changes were made.

Exit criteria:

- Core has a documented and enforced dependency boundary.
- Extension authors can identify which traits to implement without reading CLI
  or workspace internals.
- Workspace paths are injected through runtime configuration rather than implied
  by current working directory.

## Later Work

These items are useful but should wait until the invariant and config milestones
are complete:

- Harden stdio MCP toward the selected production MCP protocol, including
  initialization, shutdown, stderr propagation, and lifecycle observability.
- Broaden subprocess isolation beyond the reference shell worker if more tools
  require isolation.
- Decide whether built-in deterministic skill planners should remain in core or
  move behind a skill-extension surface.
- Revisit semantic memory defaults after config authority is stable.
- Refresh release notes and user docs once the effective runtime contract is
  settled.

## Ongoing Verification Matrix

Run before claiming a milestone complete:

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

Run when tool, approval, or sub-agent policy behavior changes:

```sh
cargo test -p agentos-core approve
cargo test -p agentos-core subagents
cargo test -p agentos-core runner
```
