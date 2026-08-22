# AGENTS.md

Agent OS is an agent-agnostic runtime written in Rust. It has three layers: an immutable core engine (`crates/`), an agent-modifiable workspace (`workspace/`), and compiled-in extensions selected via `agent.toml` (`extensions/`). The core engine must never depend on workspace or extensions.

## Project structure

```
agentos/
├── crates/
│   ├── agentos-interfaces/   # Public traits — the extension API. Zero internal deps.
│   ├── agentos-proto/        # Wire types (Message, ToolCall, Envelope, Span, RequestHeader). Serde only.
│   ├── agentos-core/         # Kernel: run loop, prompt assembly, gateway, orchestrators, approve.
│   │   ├── loop/             # RunLoopState enum and transitions; cancellation, steering, batches.
│   │   ├── prompt/           # The one authority over a provider request: assembly, projection,
│   │   │                     #   token estimation, elision, compaction.
│   │   ├── gateway/          # Bounded ingress queue, per-conversation inbox, shard threads.
│   │   ├── orchestrator/     # builtin.max (tool-selecting) and builtin.min (LLM fallback).
│   │   ├── approve/          # Concrete policy engine (NOT a trait).
│   │   ├── guardrails/       # Input, tool, and output content checks.
│   │   ├── runner/           # Envelope → run driving, task sessions, episode recording.
│   │   ├── runtime/          # AgentRuntime construction, RuntimePaths, tool/MCP wiring.
│   │   ├── config/           # agent.toml parse/validate + generated catalog rendering.
│   │   ├── memory/           # MemoryManager, scope, SQLite/sqlite-vec/Qdrant, reflection.
│   │   ├── tools/            # Registry, deadlines, built-ins, MCP adapters, memory tool.
│   │   ├── jobs/             # Background work outliving the turn that started it.
│   │   ├── spill/            # Oversized tool output persisted behind an opaque locator.
│   │   ├── sandbox/          # Kernel write limits: Landlock (Linux), Seatbelt (macOS).
│   │   ├── skills/           # Workspace skill loading, validation, deterministic planners.
│   │   ├── subagents/        # Sub-agent execution (LocalSet).
│   │   ├── channels/         # Telegram and Feishu reference adapters.
│   │   ├── crons/            # Persisted scheduled tasks.
│   │   ├── hooks/            # Lifecycle hook dispatch.
│   │   └── invariants.rs     # Debug-build assertions over load-bearing relationships.
│   ├── agentos-llm/          # LLM client trait + provider adapters.
│   │   └── providers/        # openai/ (Responses API), anthropic, deepseek, ollama
│   └── agentos-cli/          # TUI + one-shot channel entry points.
│       └── bin/agentos-gateway/  # Persistent gateway: start/stop/status/config/catalog/calibrate.
├── workspace/                # Agent-owned config and data. Not a Cargo crate.
│   └── agent.toml            # Agent wiring: orchestrator, memory, channels, policy, limits.
├── extensions/               # Compiled-in extension crates (selected via agent.toml).
│   ├── orchestrators/        # (empty placeholder — no orchestrator extension yet)
│   └── memory/
│       └── agentos-memory-vector/  # SemanticIndex impl; wired in by agentos-cli only.
├── docs/                     # ARCHITECTURE.md, roadmaps, generated catalogs, user docs.
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
bash scripts/check-import-boundaries.sh                    # Core must not depend on workspace/extensions.
bash scripts/check-module-size.sh                         # 800-LOC ceiling (cfg(test) excluded).
bash scripts/check-catalogs.sh                            # docs/*-catalog.md still match the code.
bash scripts/check-release-archive.sh                     # Clean-room: package, install, run one offline turn.
cargo semver-checks check-release -p agentos-interfaces --baseline-rev HEAD
```

The interface crates are unpublished, so `semver-checks` needs `--baseline-rev`;
without it the crates.io baseline lookup fails. CI runs every command above.

Run the most targeted test first. For example, if you changed `crates/agentos-core/loop/`, run `cargo test -p agentos-core` before running the full workspace suite.

After finishing Rust code changes, run `cargo fmt --all`. Do not ask for approval to run the formatter.

## Architecture invariants

These rules are load-bearing. Violating any of them breaks the extension model or the safety architecture.

- **Import boundary.** Nothing in `crates/agentos-core` or `crates/agentos-interfaces` may depend on `workspace/` or `extensions/`. This is enforced in CI by scanning `Cargo.toml` and `cargo tree`. If you need a type from the workspace, the type belongs in `agentos-interfaces` or `agentos-proto` instead.
- **Approve is concrete, not a trait.** The policy engine in `agentos-core/approve/` must not be defined as a trait in `agentos-interfaces`. Extensions must not be able to replace the authorization layer. If you need to add policy verbs, add them to the concrete engine.
- **Run loop is a typed state machine.** `RunLoopState` is an enum (`Start`, `Plan`, `Approve`, `Act`, `Observe`, `Paused`, `Finish`). `step()` consumes `self` and returns the next state, and its `match` is exhaustive, so a state it fails to handle is a compile error. The *ordering* is not compiler-enforced: the variants and their context structs have public fields, so a caller can hand-build an `ActCtx` and skip `Approve` entirely. Never add a transition that skips a state — review enforces that today, not the type system. Closing it is M6 in `docs/AUDIT_REMEDIATION_PLAN.md`.
- **Guardrails ≠ Approve.** Guardrails check content (input/tool/output). Approve checks permission (allow/deny/ask_user). Both are required at their designated points in the loop.
- **Sub-agent policies only narrow.** `Policy::narrow(&parent, &child)` returns `Err` when the child would decide any call more permissively than the parent, arguments included. It checks this by *deciding* a finite set of witness calls with both policies rather than by comparing rules, so first-match ordering is respected for free. Every tool call in a sub-agent re-enters the loop at the Approve state — no shortcuts, including for MCP-originated calls. A sub-agent that must exceed its parent needs an explicit `DelegationGrant` (`[[subagents.delegation_grants]]`), which names one tool, may pin exact arguments, and applies only against the immediate parent.
- **All channels are bounded.** Every `tokio::mpsc` channel must use `channel(cap)`, never `unbounded_channel()`.
- **One authority over the request.** `crates/agentos-core/src/prompt/` is the only path from a `RunContext` to the messages a provider sees, and `prompt::gateway` is the only path from a built request to a provider — `scripts/check-provider-calls.sh` fails CI when a direct call is added elsewhere. Never build a message vector at a call site; add a named `SectionId` instead. Every request declares a `RequestKind` and records what it was made of in a `RequestHeader`, usage included. A kind determines the section set, and two kinds legitimately carry no transcript: the routing classifier and the compaction summarizer are isolated from recalled memory on purpose, and the invariant refuses a non-turn request that reaches turn context.
- **The session log is append-only.** Compaction, elision, and fork are all *projections* over the log (`prompt/projection.rs`), never mutations of it. One path breaks this: `/clear` runs `DELETE FROM session_items` (`memory/sqlite.rs`), an irreversible purge rather than a projection. Do not add a second such path; M6 replaces this one with a session epoch.
- **Untrusted input never sizes an allocation.** A length, a count, or a fragment total that arrived over the wire is checked against a named ceiling before it reaches `Vec::with_capacity`, `vec![_; n]`, or a read-to-end. Feishu's fragment reassembly (`channels/feishu/fragments.rs`) bounds fragments per event, partially-received events, and total bytes; attachment downloads stream to `[limits].attachment_bytes` and stop; provider response bodies are capped rather than `.bytes()`-ed.
- **Tool egress is judged by address, not by name.** A request whose destination the *model* chose goes through `crates/agentos-core/src/egress/`: `tool_client()` resolves every name through `GuardedResolver`, which drops loopback, private, link-local, CGNAT and cloud-metadata addresses before a connection exists — so a hostname pointed at `127.0.0.1` and a DNS rebind are the same non-case. Literal-address URLs and every redirect hop go through `check_url`. Bodies are streamed to `[limits].http_response_bytes`. Endpoints the *operator* configured keep using `http::shared_client`, which is unpoliced on purpose: an address in `agent.toml` is already a decision.
- **A child process starts from nothing.** Every subprocess the runtime spawns — the isolation worker, `shell`, stdio MCP servers — goes through `tools/exec.rs` or the same `child_env::minimal` allowlist: `env_clear`, then `PATH`, `HOME`, `TMPDIR`, the locale, proxy settings, and `AGENTOS_HOME`. Credentials are never inherited; `[isolation].env_passthrough` names anything else a deployment needs. Each child gets its own process group, and a deadline or cancellation signals the group rather than the direct child.
- **Filesystem containment is directory-relative.** Anything that reaches the filesystem from a model-supplied path goes through `crates/agentos-core/src/paths/`: `RootDir` walks to the target one component at a time with `openat(O_NOFOLLOW)` and operates on the resulting descriptor, so a symlink is refused rather than followed and there is no window between the check and the open. Lexical helpers like `safe_workspace_path` classify a path; they do not contain it. Identifiers that become one component of a path — task ids, sub-agent names, template names, session ids — go through `path_segment`, which rejects rather than rewrites.
- **Isolation is a kernel boundary.** A tool declares a `SandboxMode` (`read_only`, `workspace_write`, `full_access`). Anything but `full_access` runs in a child process under Landlock/Seatbelt. Declaring a narrower mode on a tool that never spawns a child would be a claim the kernel is not making. Where no backend exists, a sandboxed tool fails rather than running unsandboxed. Enforcement is either the registry's — a compatible isolated executor, discovered through the worker's `--capabilities` handshake rather than assumed — or the tool's own, declared as `Isolation::SelfHardened` and true only when the tool applies the sandbox to its own child before spawning it. There is no third case: a tool that declares a mode and provides neither is refused at startup and again at every call.

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
  - Golden transcripts (`tests/transcripts.rs`, `tests/golden/`): a scripted LLM fixture drives a complete run and the assembled requests, session items, and outcome are pinned as one artifact. Re-record deliberately (see `tests/support/mod.rs`), never to make a test pass.
  - Generated catalogs (`tests/catalog_freshness.rs`): a config field or `ToolSpec` change without a regenerated catalog fails.

## PR conventions

- Title format: `[crate-name] short description` (e.g. `[agentos-core] add tool guardrail pipeline`).
- Run `cargo fmt --all` and `cargo clippy --workspace -- -D warnings` before committing.
- If you changed `agentos-interfaces`, run `cargo semver-checks check-release -p agentos-interfaces --baseline-rev HEAD` and note any breaking changes in the PR body.
- If you changed a config field's type, default, or doc comment, or a built-in `ToolSpec`, run `cargo run -p agentos-cli --bin agentos-gateway -- catalog` and commit the regenerated `docs/CONFIG_CATALOG.md` and `docs/TOOL_CATALOG.md`.
- If you changed wire types in `agentos-proto`, verify downstream crates still compile with `cargo check --workspace`.

## Key files

| File | Purpose |
|------|---------|
| `workspace/agent.toml` | Agent wiring: which orchestrator, memory, channels, and policy to use. |
| `DESIGN.md` | Loop state machine diagram and four-ring safety architecture. |
| `BENCHMARKS.md` | Loop overhead target: ≤2ms per turn excluding LLM/tool latency. |
| `crates/agentos-interfaces/src/` | Every public trait. Read this first when writing an extension. |
| `crates/agentos-core/src/loop/` | `RunLoopState` enum and `step()` transitions. The heart of the system. |
| `crates/agentos-core/src/approve/` | Policy engine: `Policy` rules with `allow`, `deny`, `ask_user` verbs. Built from `[policy]` in `agent.toml` by `runtime::phase5_policy`. |
| `crates/agentos-core/src/prompt/` | The single authority over what a provider request contains. |
| `crates/agentos-core/src/invariants.rs` | Debug-build assertions over three load-bearing relationships. Compiled out of release builds. |
| `docs/ARCHITECTURE.md` | Full architecture reference: extension boundary, config authority, memory model, verification matrix. |
| `docs/adr/` | Architecture decision records. One testable definition per disputed invariant, each naming the milestone that closes the gap. Read before changing narrowing, isolation, identity, provider requests, safety events, `/clear`, or streaming. |
| `docs/CAPABILITY_MATRIX.md` | Every capability with a Stable / Preview / Deferred status, its platforms, required config, security limitations, and test level. Ratcheted by `tests/capability_matrix.rs`: a new tool or provider without a row fails the build. |
| `docs/TRANSFER_ROADMAP.md` | Most recent work, with per-item rationale and status. |
| `docs/CONFIG_CATALOG.md`, `docs/TOOL_CATALOG.md` | Generated from the code by `agentos-gateway catalog`. Edit the doc comment on the field, not the table. |
