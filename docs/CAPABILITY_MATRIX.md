# Capability matrix

Every capability AgentOS exposes, with the status that decides whether it may
be enabled in the shipped config. Written as M0 deliverable 3 of
[`AUDIT_REMEDIATION_PLAN.md`](AUDIT_REMEDIATION_PLAN.md); the classification
follows its §6 release-scope policy.

| Status | Meaning | Enablement rule |
|---|---|---|
| **Stable** | Supported security and recovery contract; package- and platform-tested | May be enabled in the shipped config |
| **Preview** | Implemented, with an explicit limitation or a missing production contract | Disabled by default; requires an explicit opt-in |
| **Deferred** | Not part of the release commitment | Not exposed as a completed capability |

**Test level** is what actually exists in the tree, not what is planned:

- `unit` — assertions in the module's own `#[cfg(test)]`
- `integration` — a test under `crates/*/tests/`
- `golden` — pinned end-to-end transcript
- `none` — no automated coverage

A **Preview** row's Limitations column names the exposure and the milestone
that closes it. That is the whole point of the row: nothing here should be
enabled by someone who has not read the limitation.

This file is checked by `crates/agentos-core/tests/capability_matrix.rs`, which
fails both when a capability in the code is missing a row and when a row names
something that no longer exists — the two-way ratchet
`crates/agentos-core/src/config/undocumented.txt` established.

## Run loop and safety

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Typed run loop (`Start`→`Plan`→`Approve`→`Act`→`Observe`→`Finish`) | Stable | all | — | State *ordering* is not compiler-enforced; a hand-built `ActCtx` reaches `Act` with no `Approve` pass (M6) | integration, golden |
| Pause / resume over a serialized `RunState` | Stable | all | — | Paused-run state is written with plain `fs::write`: no temp+rename, no fsync, mode 0644 (M8) | integration, golden |
| Ticketed approval | Stable | all | `[policy]` | A prompt is answerable only by the sender it was put to, or a `[policy] approval_administrators` entry. Tickets are still minted from a clock-seeded counter rather than a CSPRNG | unit, integration |
| Policy engine (`allow` / `deny` / `ask_user`) | Stable | all | `[policy]` | — | unit, integration |
| Sub-agent policy narrowing | Stable | all | `[[subagents]]` | Exact over actions *and* arguments, decided by witness calls rather than by comparing rules. An elevation needs a declared `[[subagents.delegation_grants]]` entry, whose issuance and every use are safety events ([ADR-0001](adr/0001-POLICY_NARROWING.md), closed by M3 / `AUTH-002` and M6 / `AUD-001`) | unit, integration, property |
| Input / tool / output guardrails | Stable | all | `[guardrails]` | Output guardrails run after streaming has already forwarded chunks; see *Provisional streaming* | unit, integration |
| Shell command profiles (argument constraints) | Stable | all | `[[guardrails.shell_profiles]]` | Matches literal argument tokens; no shell-metacharacter parsing, because the tool takes structured argv rather than a command string | unit, integration |
| Run cancellation and mid-run steering | Stable | all | — | Cancellation does not route through the declared output policy (M6) | integration |
| `max_turns` budget | Stable | all | `[agent] max_turns` | — | integration |
| Tool batches | Stable | all | — | — | integration |
| Per-tool deadlines | Stable | all | `[limits]` | A deadline bounds the direct child only; a grandchild survives it (M4) | integration |
| Durable safety events (`safety_events`) | Stable | all | a SQLite session/memory store | Nine event kinds, append-only, principal-stamped. Tool arguments enter only as a SHA-256 digest and reasons are capped at 512 bytes. A failed append is logged as `safety_log_write_failed` and does not fail the run; an entrypoint with no store records nothing ([ADR-0005](adr/0005-SAFETY_EVENTS.md)) | unit, integration |

## Prompt and session

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Single request authority (`prompt::assemble`) with `RequestHeader` | Stable | all | — | Every provider call declares a `RequestKind` and records a header through `prompt::gateway`; `scripts/check-provider-calls.sh` fails CI on a direct call added elsewhere ([ADR-0004](adr/0004-REQUEST_KINDS.md)) | unit, integration, golden |
| Append-only session log with projection | Stable | all | — | `/clear` deletes rather than projecting ([ADR-0006](adr/0006-CLEAR_EPOCH.md), M6) | integration, golden |
| Span compaction | Stable | all | `[limits]` | The summarizer records a `compaction` header and its tokens reach run totals; totals across a compaction are larger than before M5 because the old ones omitted them | integration, golden |
| Tool-output elision under context pressure | Stable | all | `[limits]` | — | integration, golden |
| Conversation fork | Stable | all | — | — | integration |
| Tool-output spill | Stable | all | `[limits]` | The locator is an absolute host path embedded in a model-visible retrieval hint, and lands in the durable transcript (M7) | integration |
| Token estimation and calibration | Stable | all | — | Heuristic, not a tokenizer; two rates, calibrated per provider | integration |

## Tools

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| `tool:shell` | Stable | all | `[resources.tools]`, `[guardrails] shell_allowlist` | Sandboxed to `workspace_write`; the deployment's allowlist is the whole boundary on which programs run | unit, integration |
| `tool:file` | Stable | all | `[resources.tools]` | `full_access`: an in-process tool the kernel sandbox cannot bound. Contained by `paths::RootDir` instead — every open is an `openat(O_NOFOLLOW)` walk from a workspace-root descriptor, so symlinks are refused rather than followed | unit, integration |
| `tool:http` | Stable | all | `[resources.tools]`, `[limits] http_response_bytes` | Destinations are judged by resolved address inside the DNS resolver, so names and redirects are covered; bodies are streamed to a cap. The ambient HTTP proxy is off — a proxy resolves the destination itself, so the policy would not see it; `AGENTOS_TOOL_EGRESS_PROXY=1` takes it back and gives the decision to the proxy. Does **not** bound a `shell` command that runs `curl` — the sandbox restricts writes, not the network | unit, integration |
| `tool:skill_validate` | Stable | all | `[resources.tools]` | — | unit |
| `tool:cron_create` | Stable | all | `[resources.tools]`, gateway running | Writes a TOML file the scheduler replays; the prompt it stores is executed later with no further approval | unit, integration |
| `tool:cron_list` | Stable | all | `[resources.tools]` | — | unit |
| `tool:cron_remove` | Stable | all | `[resources.tools]` | — | unit |
| `tool:memory` | Stable | all | `[resources.tools]`, `[memory]` | `[memory.policy]` is pre-empted by a coarse `[policy] allowlist` entry (M7) | unit, integration |
| `tool:job_status` | Stable | all | `[resources.tools]`, `[jobs]` | — | integration |
| `tool:job_output` | Stable | all | `[resources.tools]`, `[jobs]` | — | integration |
| `tool:job_kill` | Stable | all | `[resources.tools]`, `[jobs]` | — | integration |
| Background jobs outliving their turn | Stable | all | `[jobs]` | Fenced to the conversation that started them | integration |
| Static (in-process) MCP tools | Stable | all | `[[mcp_servers]]` | — | unit |
| stdio MCP transport | **Preview** | all | `[[mcp_servers]]` | Not MCP: no `initialize`, no `protocolVersion`, no capability negotiation, no request-id correlation — one stray line desynchronizes every later call. Unbounded channel and `read_line`. **No test module and no integration test anywhere** (M8) | none |

## Isolation

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Landlock sandbox | Stable | Linux 5.13+ with the LSM enabled | tool declares a `SandboxMode` | Bounds filesystem **writes** only; reads and network are unrestricted | unit, integration |
| Seatbelt sandbox | Stable | macOS | tool declares a `SandboxMode` | Bounds filesystem **writes** only; reads and network are unrestricted, as on Linux | unit, integration, CI |
| Registry enforcement of `SandboxMode` | Stable | all | — | A declared mode is carried by an isolated executor or by the tool's own `Isolation::SelfHardened` child; anything else is refused at startup and at every call | unit, integration |
| Isolation worker subprocess | Stable | Linux, macOS | `[isolation]` | Carries `shell` only; the `--capabilities` handshake reports that, so an unsupported tool is refused before dispatch rather than after a non-zero exit | unit, integration |
| Minimal subprocess environment | Stable | all | `[isolation].env_passthrough` | Every child starts from `env_clear` and an allowlist. Proxy variables are kept, so a proxy URL carrying userinfo credentials still reaches tools | unit, integration |
| Process-group termination | Stable | Unix | — | The group is signalled on deadline and cancellation only; a command that exits cleanly leaves its deliberate background children running | unit |
| Network egress policy | **Deferred** | — | — | Not implemented; no SSRF protection anywhere (M4) | none |

## Memory

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Three-layer scoped memory (session, working, long-term) | Stable | all | `[memory]` | Namespaces are keyed by `Principal` and encoded injectively ([ADR-0003](adr/0003-TYPED_PRINCIPAL.md)). Memory written before this is under the old keys and needs the `ID-002` migration | unit, integration |
| SQLite reference backend | Stable | all | `[memory] backend = "sqlite"` | One `Mutex<Connection>` shared across all shard threads — a global lock, not a pool. No WAL, no busy timeout (M8) | unit, integration |
| In-memory backend | Stable | all | `[memory] backend = "in_memory"` | Not durable; for tests and ephemeral runs | unit |
| Passive retrieval into planning context | Stable | all | `[memory]` | — | integration |
| Reflection | Stable | all | `[memory.reflection]` | Overwrites the retention request with `RetentionRequest::default()`, discarding configured budgets (M7) | integration |
| `sqlite-vec` semantic index | **Preview** | all | `[memory] semantic_backend = "sqlite_vec"` | Requires the sqlite backend; no end-to-end contract test | unit |
| Qdrant semantic index | **Preview** | all | `[memory] semantic_backend = "qdrant"` | Requires an external service; not exercised in CI | unit |
| `agentos-memory-vector` extension | **Preview** | all | wired by `agentos-cli` | Hashing fallback when no embedder is configured, which ranks token overlap rather than meaning | unit |
| Memory retention (count / age / bytes) | **Deferred** | — | `[memory.retention]` | Parses, validates, appears in the catalog, and is never read outside `config/` (M7) | none |
| Shared memory domains | **Deferred** | — | `[memory.shared_domains]` | Same: inert (M7) | none |

## LLM providers

Every adapter is enabled by a `provider:model` identifier. None is exercised
against a live service in CI; coverage is over request construction, response
parsing, streaming assembly, and retry behavior.

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| `provider:openai` (Responses API) | Stable | all | `OPENAI_API_KEY` | — | unit |
| `provider:anthropic` | Stable | all | `ANTHROPIC_API_KEY` | — | unit |
| `provider:deepseek` | Stable | all | `DEEPSEEK_API_KEY` | — | unit |
| `provider:ollama` | Stable | all | local Ollama service | No API key; whatever the local service exposes | unit |
| Buffered replies | Stable | all | — | — | unit, golden |
| Provisional streaming | **Preview** | all | opt-in | Chunks are forwarded during `plan()`, before output guardrails run, and the TUI sink cannot retract what it printed ([ADR-0007](adr/0007-BUFFERED_OUTPUT.md), M6) | integration |

## Channels

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| TUI | Stable | all | — | — | integration |
| One-shot CLI | Stable | all | — | — | integration |
| Telegram | **Preview** | all | `[[channels]]`, bot token, an allowlist | Fails closed on identity, with a sender allowlist. The update offset still lives only in process memory and advances *before* the run, so a crash both replays and loses (M8) | unit |
| Feishu | **Preview** | all | `[[channels]]`, app credentials, an allowlist | Fails closed on identity; fragment reassembly is bounded by count, pending events, and bytes. Still reads `event_id` and never dedupes on it (M8) | unit |
| Attachments | Stable | all | `[limits] attachment_bytes`, `attachments_per_message` | Downloads are streamed and stop at the byte cap; per-message count is bounded and the surplus is reported, not silently dropped. Path components are injectively encoded, so two conversations cannot share a directory | unit |

## Orchestration and workspace

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| `builtin.max` (tool-selecting) | Stable | all | `[agent] orchestrator` | — | integration, golden |
| `builtin.min` (LLM fallback) | Stable | all | `[agent] orchestrator` | — | integration, golden |
| LLM routing classifier | Stable | all | `[routing]` | Records a `routing` header carrying its two own sections and none of the turn's. Its isolation from prompt assembly is a prompt-injection defence, now enforced by `invariants::request_derives_from_state` rather than by convention ([ADR-0004](adr/0004-REQUEST_KINDS.md)) | unit, integration |
| Sub-agents | Stable | all | `[[subagents]]` | See *Sub-agent policy narrowing* | integration |
| Workspace skills | Stable | all | `[skills]` | Loaded from workspace content, which is data the agent can modify | unit, integration |
| Deterministic skill planners | Stable | all | `[skills]` | — | unit |
| Lifecycle hooks | Stable | all | `[hooks]` | — | unit |
| Persisted crons | Stable | all | gateway running | The scheduler replays a stored prompt with no further approval | integration |

## Gateway

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Bounded ingress queue and per-conversation inbox | Stable | all | — | — | integration |
| Shard threads | Stable | all | — | — | integration |
| `start` / `status` / `config` / `catalog` / `calibrate` | Stable | all | — | — | integration |
| `stop` | **Preview** | all | — | Shells out to `kill(1)` on a pid read from a plain `fs::write` pidfile, with no verification that the pid is still the gateway (M8) | none |
| Durable ingress ledger | **Deferred** | — | — | Not implemented: a crash between accept and enqueue loses the message (M8) | none |

## Release and packaging

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Release archive | Stable | Linux, macOS | — | — | script (`check-release-archive.sh`), CI |
| Generated catalogs | Stable | all | — | — | integration |
| Import-boundary check | Stable | all | — | Self-testing (`--self-test`) | script, integration |
| Module-size check | Stable | all | — | Two allowlisted files exceed the 800-LOC budget | script |
