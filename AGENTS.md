# AGENTS.md

Agent OS is an agent-agnostic runtime written in Rust. It has three layers: an immutable core engine (`crates/`), an agent-modifiable workspace (`workspace/`), and compiled-in extensions selected via `agent.toml` (`extensions/`). The core engine must never depend on workspace or extensions.

## Project structure

```
agentos/
├── crates/
│   ├── agentos-interfaces/   # Public traits — the extension API. Zero internal deps.
│   ├── agentos-proto/        # Wire types (Message, ToolCall, Envelope, Span). Serde only.
│   ├── agentos-core/         # Kernel: run loop, gateway, orchestrator, approve, hooks.
│   │   ├── loop/             # RunLoopState enum and transitions.
│   │   ├── gateway/          # Channel multiplexer (bounded mpsc).
│   │   ├── orchestrator/     # Default orchestrator impl.
│   │   ├── approve/          # Concrete policy engine (NOT a trait).
│   │   ├── runtime/          # Tokio setup, tracing, error types.
│   │   └── hooks/            # Lifecycle hook dispatch.
│   ├── agentos-llm/          # LLM client trait + provider adapters.
│   │   └── providers/        # openai/, anthropic/, ollama/
│   └── agentos-cli/          # Binary entry point.
├── workspace/                # Agent-owned config and data. Not a Cargo crate.
│   └── agent.toml            # Agent wiring: orchestrator, memory, channels, policy.
├── extensions/               # Compiled-in extension crates (selected via agent.toml).
│   ├── orchestrators/
│   └── memory/
├── DESIGN.md                 # Loop state machine diagram and safety architecture.
└── BENCHMARKS.md             # Performance targets and results.
```

Crate names are prefixed with `agentos-`. For example, the `core` folder's crate is `agentos-core`.

## Build and test commands

Install prerequisites if not already available:

```sh
rustup component add clippy
cargo install cargo-semver-checks
```

Core commands:

```sh
cargo check --workspace                                   # Type-check everything.
cargo clippy --workspace -- -D warnings                   # Lint. Warnings are errors.
cargo test -p agentos-interfaces                          # Interface contract tests.
cargo test -p agentos-core                                # Core unit + integration tests.
cargo test --workspace                                    # Full test suite.
cargo bench -p agentos-core                               # Loop overhead benchmarks.
cargo semver-checks check-release -p agentos-interfaces   # Catch breaking interface changes.
```

Run the most targeted test first. For example, if you changed `crates/agentos-core/loop/`, run `cargo test -p agentos-core` before running the full workspace suite.

After finishing Rust code changes, run `cargo fmt --all`. Do not ask for approval to run the formatter.

## Architecture invariants

These rules are load-bearing. Violating any of them breaks the extension model or the safety architecture.

- **Import boundary.** Nothing in `crates/agentos-core` or `crates/agentos-interfaces` may depend on `workspace/` or `extensions/`. This is enforced in CI by scanning `Cargo.toml` and `cargo tree`. If you need a type from the workspace, the type belongs in `agentos-interfaces` or `agentos-proto` instead.
- **Approve is concrete, not a trait.** The policy engine in `agentos-core/approve/` must not be defined as a trait in `agentos-interfaces`. Extensions must not be able to replace the authorization layer. If you need to add policy verbs, add them to the concrete engine.
- **Run loop is a typed state machine.** `RunLoopState` is an enum (`Start`, `Plan`, `Approve`, `Act`, `Observe`, `Paused`, `Finish`). `step()` consumes `self` and returns the next state. Invalid transitions are compile errors. Never add a transition that skips a state.
- **Guardrails ≠ Approve.** Guardrails check content (input/tool/output). Approve checks permission (allow/deny/ask_user). Both are required at their designated points in the loop.
- **Sub-agent policies only narrow.** `Policy::narrow(&parent, &child)` returns `Err` on any widening attempt. Every tool call in a sub-agent re-enters the loop at the Approve state — no shortcuts, including for MCP-originated calls.
- **All channels are bounded.** Every `tokio::mpsc` channel must use `channel(cap)`, never `unbounded_channel()`.

## Code conventions

- Use `Arc<str>` for hot strings (agent names, tool names, channel ids). Never clone `String` on the loop hot path.
- Use `serde_json::RawValue` for tool arguments — parse only when the tool needs them.
- One `reqwest::Client` per LLM provider, not per request.
- `thiserror` for all error enums. Domain errors are typed enums, not `String` or `anyhow`.
- `#[async_trait]` on all trait definitions in `agentos-interfaces`. All public traits require `Send + Sync`.
- Structured `tracing` fields on spans. Never use format strings in span fields.
- When using `format!()`, always inline variables into `{}` when possible.
- Make `match` arms exhaustive. Avoid wildcard `_` catch-all arms where the compiler can check variants.
- No `unwrap()` in library crates. Use `expect("reason")` only with a message explaining the invariant.
- Prefer private modules with explicitly re-exported public API.
- Keep modules under 500 lines of code (excluding tests). If a file exceeds ~800 lines, add new functionality in a new module rather than extending the existing one.
- Do not create single-use helper functions. Inline short logic at the call site.
- When adding a new trait to `agentos-interfaces`, include doc comments that explain its role and how implementations should behave.

## Testing

- Every trait in `agentos-interfaces` has a `MockXxx` in `crates/agentos-interfaces/src/test_support.rs`. Use these in integration tests.
- Unit tests go in the same file under `#[cfg(test)] mod tests`. Integration tests go in the `tests/` directory.
- Prefer `assert_eq!` on entire structs over individual fields for clearer diffs.
- Key test scenarios that must always pass:
  - `RunState` round-trip: serialize → disk → deserialize → resume → assert same trace.
  - `Policy::narrow` property test: random parent/child pairs, verify no widening.
  - `max_turns`: adversarial orchestrator returning `Plan::CallTool` every turn → assert `MaxTurnsExceeded`.
  - Import boundary: a deliberately broken `Cargo.toml` adding `workspace` as a dep → CI rejects.

## PR conventions

- Title format: `[crate-name] short description` (e.g. `[agentos-core] add tool guardrail pipeline`).
- Run `cargo fmt --all` and `cargo clippy --workspace -- -D warnings` before committing.
- If you changed `agentos-interfaces`, run `cargo semver-checks check-release -p agentos-interfaces` and note any breaking changes in the PR body.
- If you changed wire types in `agentos-proto`, verify downstream crates still compile with `cargo check --workspace`.

## Key files

| File | Purpose |
|------|---------|
| `workspace/agent.toml` | Agent wiring: which orchestrator, memory, channels, and policy to use. |
| `DESIGN.md` | Loop state machine diagram and four-ring safety architecture. |
| `BENCHMARKS.md` | Loop overhead target: ≤2ms per turn excluding LLM/tool latency. |
| `crates/agentos-interfaces/src/` | Every public trait. Read this first when writing an extension. |
| `crates/agentos-core/loop/` | `RunLoopState` enum and `step()` transitions. The heart of the system. |
| `crates/agentos-core/approve/` | Policy engine. YAML policy with `allow`, `deny`, `ask_user` verbs. |
