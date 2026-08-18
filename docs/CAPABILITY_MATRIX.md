# AgentOS Capability Matrix

Status date: 2026-08-18  
Authority: [`AUDIT_REMEDIATION_PLAN.md`](AUDIT_REMEDIATION_PLAN.md) and
[`ADR-001`](adr/ADR-001-remediation-contracts.md)

This matrix describes the audited source baseline, not merely whether code is
present. A capability is Stable only when its security and recovery contract is
supported and exercised end to end. Preview capabilities require an explicit
operator choice and are not release guarantees. Deferred capabilities are not
part of the rescue release.

Platform labels mean supported release targets: Linux requires Landlock where
isolation is claimed; macOS requires a successful Seatbelt enforcement probe.
"Portable" means both Linux and macOS without a platform-specific security
claim.

| Capability | Maturity | Default/example config | Platforms | Required config | Current limitation or security assumption | Test level / promotion gate |
|---|---|---|---|---|---|---|
| Typed run loop and bounded turn execution | Stable | Enabled | Portable | `[agent].max_turns` | Compile-time state types do not by themselves make persistence or policy correct | Core unit, integration, golden transcript, and max-turn adversarial tests |
| Input/tool/output guardrail pipeline | Stable for buffered local runs | Enabled | Portable | `[guardrails]` | Shell allowlisting and streamed output have separate Preview constraints below | Unit and complete-run integration tests; stable streaming remains gated by M4 |
| Concrete ticketed approval | Stable for one trusted local principal | Enabled as policy requires | Portable | `[policy]`, `[approval]` | Remote/group binding and durable decision history are not yet Stable | Pause/resume tests; promotion to remote use requires `ID-001`, `AUTH-001`, and `AUD-001` |
| Append-only session log, projection, and fork | Stable within one typed principal | Enabled | Portable | Session backend | Principal isolation and versioned legacy migration are enforced; `/clear` epoch advancement remains open | Projection/fork, cross-principal isolation, and crash/restart/rollback migration tests; reset promotion requires `STATE-001` |
| Tool-result spill and elision | Preview | Enabled above inline limit | Portable | `[limits].tool_result_inline_bytes`, `[spill]` | Locator ownership/retrieval and absolute-path portability are incomplete | Unit/golden coverage; promotion requires `SPILL-001` and package-path tests |
| Context compaction and overflow recovery | Preview | Enabled | Portable | `[compaction]` | Compaction provider calls do not yet satisfy the unified request-manifest contract | Integration/golden coverage; promotion requires `REQ-001` |
| SQLite memory in a trusted single-principal deployment | Stable within that scope | Enabled | Portable | `[memory]` | Several policy, domain, and retention keys are not yet authoritative | Unit/integration tests; broader promotion requires `CFG-001` and `MEM-001` |
| Shared or multi-principal memory | Preview | Shared writes disabled | Portable | `[memory.policy]`, `[[memory.shared_domains]]` | Private memory is principal-isolated and legacy ambiguity is quarantined; shared write authority and retention remain incomplete | Principal isolation and migration/quarantine regressions; promotion requires `MEM-001` behavior tests |
| Hashing-based `sqlite_vec` retrieval | Stable as lexical-vector retrieval | Enabled | Portable | `[memory].semantic_backend = "sqlite_vec"` | It is persistent but is not represented as real semantic embeddings | Persistence/restart tests; documentation must retain the lexical-vector qualifier |
| Real-embedding vector retrieval | Preview | Disabled unless selected/configured | Portable plus provider | `semantic_backend = "vector"` or `"qdrant"`, embedding credentials | Built-in real-vector persistence/backfill/restart and paraphrase-quality contracts are incomplete | Promotion requires persistence, backfill, restart, and retrieval-quality tests |
| TUI and one-shot local operation | Stable from source | TUI enabled | Portable | `[channels.tui]` and provider credentials, or offline fixture | Packaged-install qualification awaits required CI results | Core/CLI tests plus the `REL-001`/`REL-002` source and artifact smoke matrix |
| Release archive and installed wrapper | Preview pending CI qualification | Self-contained archive; XDG wrapper install | Linux/macOS targets | Default XDG paths or explicit install overrides | Archive, source install, packaged install, wrapper diagnostics, pathless resume, and offline echo are covered; required platform runs have not yet qualified the release | `REL-001`/`REL-002` clean-room matrix is configured on Linux and macOS; promote after both required jobs pass |
| Gateway actor/shard core | Stable for tested in-process envelopes | Configured | Portable | `[gateway]` | Durable remote ingress, process-wide DB authority, and principal identity are outside this scope | Bounded-ingress and shard tests; production gateway promotion requires M2 and M6 |
| Telegram and Feishu production channels | Preview | Disabled | Portable plus remote service | `[channels.telegram]` or `[channels.feishu]`, credentials, explicit sender policy | Sender authentication, principal binding, durable ACK/replay, and stable buffered output are incomplete | Promotion requires `AUTH-001`, `ING-001`, `STATE-001`, and `GW-001` plus live fixtures |
| Background jobs | Stable within one typed session principal | Tools enabled | Portable | `[jobs]`, `job_*` tools | Complete principal ownership is enforced; descendant process cleanup remains incomplete | Job integration and cross-channel ownership tests; process guarantees require `PROC-001` |
| Static MCP adapter | Stable for configured static fixtures | Example enabled | Portable | `[resources.mcp]`, `[[mcp_servers]]`, `[[mcp_tools]]` | This does not imply stdio protocol interoperability or remote-process isolation | Adapter/config tests; only static endpoints are covered by this row |
| Stdio MCP servers | Preview | Not configured in example | Linux/macOS with compatible sandbox | Explicit stdio server config and Preview opt-in | Protocol lifecycle, bounds, restart, and isolation of the actual server process are incomplete | Promotion requires `SBX-001` and `MCP-001` interoperability/enforcement tests |
| Sandboxed subprocess tools | Preview | Shell is enabled in example | Linux Landlock; macOS Seatbelt | `[isolation]`, compatible worker/backend | Registry invocation is fail-closed and protocol-checked; macOS availability now requires a real profile/write-denial probe; actual-process MCP isolation remains incomplete | No-body-invocation tests pass and required Landlock/Seatbelt CI jobs are configured; promotion awaits qualifying runs and `MCP-001` |
| File, shell, and HTTP mutation/network profiles | Preview | Enabled in example | Platform/tool dependent | `[resources.tools]`, policy and guardrails | Traversal/symlink, inherited-secret, descendant, SSRF, and argument-profile gaps remain | Promotion requires `FS-001`, `PROC-001`, `NET-001`, and conservative defaults in `AUTH-003` |
| Subagent delegation and sub-orchestrator templates | Preview | Reachable from example routing | Portable, subject to tool backends | Routing, subagent/template files, child policies | Strict per-call narrowing and principal-keyed child sessions/forks are enforced; explicit delegation grants remain incomplete | Strict policy-lattice and principal-keyed fork tests; promotion still requires the explicit grant model |
| Provider routing classifier | Preview | Enabled | Portable plus provider | `[routing].llm_classifier` | Routing calls do not yet pass through the unified request gateway or contribute complete usage | Promotion requires `REQ-001` manifest and accounting tests |
| Buffered final delivery | Stable where the channel buffers | Channel dependent | Portable | No provisional streaming | Stability assumes no bytes are emitted before output policy accepts the candidate | Seeded reject-to-zero-bytes test required for each Stable channel in `STATE-001` |
| Provisional token streaming | Preview | Implementation may stream; no stable opt-in contract yet | Channel/provider dependent | Future explicit Preview flag | Output later rejected by policy may already be visible; retraction semantics vary | Promotion requires explicit opt-in now and an incremental guardrail/staging contract later |
| Folded todo/plan/goal collaboration state | Deferred | Not enabled | — | — | Outside release-rescue scope | Reconsider after M7 |
| Dynamic plugin loading and new orchestrator extensions | Deferred | Not available | — | — | Compiled-in, config-selected extension model remains authoritative | Reconsider only with an approved extension-boundary design |
| New channel integrations | Deferred | Not available | — | — | Principal, containment, and durable ingress must close first | Reconsider after M2, M3, and M6 |

## Release rules

- A Preview row must be disabled by the shipped release config or guarded by an
  explicit Preview opt-in before release qualification. Rows that say the audit
  example currently enables a Preview feature identify remediation work; they
  are not exceptions to this rule.
- A capability changes maturity only when the named end-to-end gate passes on
  every claimed platform. Code presence, unit coverage, or an old roadmap
  completion marker is insufficient.
- README, example config, generated catalogs, release notes, and packaged
  documentation must agree with this matrix at release-candidate cut.
