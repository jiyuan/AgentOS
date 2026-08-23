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
| Typed run loop (`Start`→`Plan`→`Approve`→`Act`→`Observe`→`Finish`) | Stable | all | — | `ActCtx` carries a private authorization witness, so a hand-built one does not compile and a swapped plan is refused in `Act`; the transition table is pinned by `tests/loop_transitions.rs` | integration, golden |
| Pause / resume over a serialized `RunState` | Stable | all | — | Paused-run state is written with plain `fs::write`: no temp+rename, no fsync, mode 0644 (M8) | integration, golden |
| Ticketed approval | Stable | all | `[policy]` | A prompt is answerable only by the sender it was put to, or a `[policy] approval_administrators` entry. Tickets are still minted from a clock-seeded counter rather than a CSPRNG | unit, integration |
| Policy engine (`allow` / `deny` / `ask_user`) | Stable | all | `[policy]` | — | unit, integration |
| Sub-agent policy narrowing | Stable | all | `[[subagents]]` | Exact over actions *and* arguments, decided by witness calls rather than by comparing rules. An elevation needs a declared `[[subagents.delegation_grants]]` entry, whose issuance and every use are safety events ([ADR-0001](adr/0001-POLICY_NARROWING.md), closed by M3 / `AUTH-002` and M6 / `AUD-001`) | unit, integration, property |
| Input / tool / output guardrails | Stable | all | `[guardrails]` | Every terminal reply passes the output policy, including the notices the loop synthesizes for cancellation and budget exhaustion. Streaming is the one way to emit bytes ahead of the check, and is off by default; see *Provisional streaming* | unit, integration |
| Shell command profiles (argument constraints) | Stable | all | `[[guardrails.shell_profiles]]` | Matches literal argument tokens; no shell-metacharacter parsing, because the tool takes structured argv rather than a command string | unit, integration |
| Run cancellation and mid-run steering | Stable | all | — | Cancellation does not route through the declared output policy (M6) | integration |
| `max_turns` budget | Stable | all | `[agent] max_turns` | — | integration |
| Tool batches | Stable | all | — | — | integration |
| Per-tool deadlines | Stable | all | `[limits]` | A deadline bounds the direct child only; a grandchild survives it (M4) | integration |
| Durable safety events (`safety_events`) | Stable | all | a SQLite session/memory store | Eleven event kinds, append-only, principal-stamped. Tool arguments enter only as a SHA-256 digest and reasons are capped at 512 bytes. A failed append is logged as `safety_log_write_failed` and does not fail the run; an entrypoint with no store records nothing ([ADR-0005](adr/0005-SAFETY_EVENTS.md)) | unit, integration |

## Prompt and session

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Single request authority (`prompt::assemble`) with `RequestHeader` | Stable | all | — | Every provider call declares a `RequestKind` and records a header through `prompt::gateway`; `scripts/check-provider-calls.sh` fails CI on a direct call added elsewhere ([ADR-0004](adr/0004-REQUEST_KINDS.md)) | unit, integration, golden |
| Append-only session log with projection | Stable | all | — | `/clear` appends an epoch marker and deletes nothing; the epoch is per conversation rather than per principal until `Session` is principal-keyed (M3 deliverable 2). Deletion is `agentos-gateway purge` ([ADR-0006](adr/0006-CLEAR_EPOCH.md)) | integration, golden |
| Span compaction | Stable | all | `[limits]` | The summarizer records a `compaction` header and its tokens reach run totals; totals across a compaction are larger than before M5 because the old ones omitted them | integration, golden |
| Tool-output elision under context pressure | Stable | all | `[limits]` | — | integration, golden |
| Conversation fork | Stable | all | — | — | integration |
| Tool-output spill | Stable | all | `[limits]`, `[spill]` | The locator is opaque (`spill:<run>/<artifact>`) and carries no host path into the transcript; retrieval is `spill_read`, authorized by the transcript that cites it | integration |
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
| `tool:memory` | Stable | all | `[resources.tools]`, `[memory]` | `[memory.policy]` decides read, write, and forget; a `[policy] allowlist` entry naming `memory` is a load-time error rather than a silent override | unit, integration |
| `tool:job_status` | Stable | all | `[resources.tools]`, `[jobs]` | — | integration |
| `tool:job_output` | Stable | all | `[resources.tools]`, `[jobs]` | — | integration |
| `tool:job_kill` | Stable | all | `[resources.tools]`, `[jobs]` | — | integration |
| `tool:spill_read` | Stable | all | `[resources.tools]`, `[spill]` | Reads back a tool result that outgrew the inline cap. A `spill:` locator is not a path; it resolves only inside the configured store, through an `openat(O_NOFOLLOW)` walk, and only when the calling run's own transcript cites it. Registered only when a spill store is configured | unit, integration |
| Background jobs outliving their turn | Stable | all | `[jobs]` | Fenced to the conversation that started them | integration |
| Static (in-process) MCP tools | Stable | all | `[[mcp_servers]]` | — | unit |
| stdio MCP transport | **Preview** | all | `[[mcp_servers]]` | Speaks MCP `2025-06-18` (accepts `2025-03-26` and `2024-11-05`): `initialize`, capability negotiation, paginated `tools/list`, content blocks, request-id correlation, `notifications/cancelled`, structured errors, graceful shutdown, bounded restart. Preview because only the stdio transport exists — no HTTP/SSE — and this client claims no `roots`, `sampling`, or `elicitation` capability, so a server needing one is refused rather than served | unit, integration (against an independent fixture) |
| MCP server isolation | Stable | Linux, macOS | `[[mcp_servers]] sandbox` | The mode restricts the *server child*, which is the process that touches the filesystem. Defaults to `full_access`, an honest statement of what happened before M8 rather than a new grant; a `read_only` server on a host with no sandbox backend refuses to start | unit, integration |

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
| SQLite reference backend | Stable | all | `[memory] backend = "sqlite"`, `[memory] max_connections` | WAL with a 5s busy timeout, behind a fixed-size connection pool. Still synchronous calls from async code: a pooled connection is never held across an `.await`, but a slow statement occupies the calling thread | unit, integration |
| Single maintenance leader | Stable | all | — | Memory reflection runs under a `memory.reflection` lease and the store retention sweep under a `gateway.retention` lease, so each gets one sweep across every channel's shard set and every process on the file. A lease expires rather than locks, so a leader that wedges without dying can be overlapped after 5 minutes | unit, integration |
| In-memory backend | Stable | all | `[memory] backend = "in_memory"` | Not durable; for tests and ephemeral runs | unit |
| Passive retrieval into planning context | Stable | all | `[memory]` | — | integration |
| Reflection | Stable | all | `[memory.reflection]`, `[memory.retention]` | Promotion runs per conversation; retention and the index rebuild run once per sweep, after it | integration |
| `sqlite-vec` semantic index | **Preview** | all | `[memory] semantic_backend = "sqlite_vec"` | Requires the sqlite backend; no end-to-end contract test | unit |
| Qdrant semantic index | **Preview** | all | `[memory] semantic_backend = "qdrant"` | Requires an external service; not exercised in CI | unit |
| `agentos-memory-vector` extension | **Preview** | all | wired by `agentos-cli` | Hashing fallback when no embedder is configured, which ranks token overlap rather than meaning | unit |
| Memory retention (count / age / bytes) | Stable | all | `[memory.retention]`, `[memory.reflection] enabled` | Applied by the reflection sweep, so a deployment that has not enabled reflection prunes nothing. Records are archived, not deleted | unit, integration |
| Shared memory domains | Stable | all | `[[memory.shared_domains]]`, `[memory.policy] shared_writes` | A write needs the global switch, the domain's own `write`, and a caller holding it — a top-level run declares no `memory_view`, so it writes to no shared domain | unit, integration |

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
| Provisional streaming | **Preview** | all | `[channels] provisional_streaming = true` | Off by default. Chunks are forwarded during `plan()`, before output guardrails run, and the TUI sink cannot retract what it printed; a chat channel edits in place but the bytes reached a device first. Eligible for a stable default once an incremental output-guardrail interface exists ([ADR-0007](adr/0007-BUFFERED_OUTPUT.md)) | integration |

## Channels

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| TUI | Stable | all | — | — | integration |
| One-shot CLI | Stable | all | — | — | integration |
| Telegram | **Preview** | all | `[[channels]]`, bot token, an allowlist | Fails closed on identity, with a sender allowlist. The `getUpdates` offset is persisted in the ingress ledger after the update is recorded, and every update carries its `update_id` as the dedupe key. Preview because the transport still shells out to `curl` | unit |
| Feishu | **Preview** | all | `[[channels]]`, app credentials, an allowlist | Fails closed on identity; fragment reassembly is bounded by count, pending events, and bytes. `event_id` is now the ingress dedupe key, with `message_id` standing in when a payload carries no header id. Preview because the long-connection reconnect path has no end-to-end test | unit |
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
| Strict config schema | Stable | all | — | Every section rejects an unknown key at load time, so a typo is an error rather than a silent default. Two keys are accepted-but-deprecated and warn: `agent.memory` (use `[memory].backend`) and `[resources.llm]` (use `[resources].priority`) | unit, integration |
| Configured agent identity | Stable | all | `[agent] id` | Stamped on every trace, episode, memory record, and safety event, and part of the principal keying them. Changing it on an existing deployment orphans memory written under the old id — there is no rename path | unit, integration |
| Persisted crons | Stable | all | gateway running | The scheduler replays a stored prompt with no further approval | integration |

## Gateway

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Bounded ingress queue and per-conversation inbox | Stable | all | — | — | integration |
| Shard threads | Stable | all | — | — | integration |
| `start` / `status` / `config` / `catalog` / `calibrate` | Stable | all | — | — | integration |
| `stop` | Stable | Unix | — | Signals the holder of an `flock`ed control file, never a pid read out of a file: a stale record names nobody and is not signalled. Escalates to `SIGKILL` after 45s | unit, integration |
| Graceful shutdown | Stable | Unix | `[gateway] shutdown_grace_secs` | `SIGTERM` stops the router accepting and drains in-flight turns within the grace period, then reports the shards that would not drain and every accepted-but-unsettled event | unit, integration |
| Durable ingress ledger | Stable | all | — | `(channel_id, transport event id)` decides fresh / retry / suppress. A channel that publishes no `INGRESS_ID_KEY` is delivered and not recorded — the gateway cannot dedupe what it cannot recognise | unit, integration |
| Store retention sweep | Stable | all | `[retention]`, `[spill]`, `[jobs] completed_retention_secs` | Traces, attachments, the gateway log, spill, settled ingress rows and finished jobs, from the idle phase every 15 minutes under a `gateway.retention` lease. Runs only while a channel is served: a gateway with no channels enabled rotates nothing. The trace and attachment ceilings are off by default | unit, integration |
| Authorized purge (`purge --conversation` / `--sessions` / `--audit`) | Stable | all | — | The only deletion path for the session log and the two audit stores; no timer touches either ([ADR-0005](adr/0005-SAFETY_EVENTS.md), [ADR-0006](adr/0006-CLEAR_EPOCH.md)). The bulk modes report first and apply only against the printed count typed back. Gated on shell access to the host, not on `Approve` | unit, integration |

## Release and packaging

| Capability | Status | Platforms | Required config | Limitations | Test level |
|---|---|---|---|---|---|
| Release archive | Stable | Linux, macOS | — | — | script (`check-release-archive.sh`), CI |
| Generated catalogs | Stable | all | — | — | integration |
| Import-boundary check | Stable | all | — | Self-testing (`--self-test`) | script, integration |
| Module-size check | Stable | all | — | Two allowlisted files exceed the 800-LOC budget | script |
