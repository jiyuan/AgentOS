# Audit Remediation Baseline — 2026-08-18

This is the M0 measurement snapshot for the remediation baseline at `224f070`
plus the documentation-only `ADR-001` slice. It records observed failures as
well as passes; it is not a release qualification report.

## Environment

- Host: Apple arm64, macOS 15.2 (Darwin 24.2.0)
- Rust: `rustc 1.94.1 (e408947bf 2026-03-25)`
- Cargo: `cargo 1.94.1`
- Source branch: `main`, baseline commit `224f070`

## Verification floor

| Check | Result | Observation |
|---|---|---|
| `cargo fmt --all -- --check` | Pass | No formatting drift |
| `cargo check --workspace --locked` | Pass | Completed from the existing build cache |
| `cargo clippy --workspace --locked -- -D warnings` | Pass | No warnings |
| `cargo test --workspace --locked` | Fail | Core library reached 366 pass / 2 fail before Cargo stopped the workspace run |
| `bash scripts/check-import-boundaries.sh` | Pass | Import boundaries intact |
| `bash scripts/check-module-size.sh` | Pass with existing warnings | Gateway `main.rs` is 1327 LOC and CLI `main.rs` is 864 LOC; both are allowlisted over the 800-LOC budget |
| `bash scripts/check-catalogs.sh` | Pass | Generated catalog blocks are current |
| `git diff --check` | Pass | No whitespace errors in the remediation slice |

The two test failures are named work rather than new hidden exceptions:

1. `config::tests::repository_workspace_config_declares_effective_resources`
   still expects the three skills removed from the shipped enabled list by
   `224f070`. The same names remain in `general-subagent.toml`, so clean startup
   warns that the child references skills absent from the parent catalog. This
   belongs to M1's complete-workspace resolution test.
2. `spill::tests::a_sweep_removes_expired_runs_and_keeps_fresh_ones` invokes
   GNU `touch -d`; macOS `touch` rejects that syntax. This is M1/`CI-001`'s
   portable timestamp-fixture item.

Because `cargo bench -p agentos-core --locked -- --test` runs library tests
before the bench binaries, the same two failures also prevent the required
aggregate bench-smoke command from reaching its harnesses.

## Performance

The direct intrinsic-loop benchmark bypassed the unrelated failing library
fixtures and passed:

```text
cargo bench -p agentos-core --locked --bench loop_overhead
loop_overhead/reply_turn time: [3.6014 µs 3.6088 µs 3.6159 µs]
```

This is comfortably below the documented 2 ms intrinsic-turn target. It is a
local baseline, not a cross-platform release result.

## Clean temporary startup

A temporary `AGENTOS_HOME` was populated with only `agent.toml`, `skills/`,
`subagents/`, and `suborchs/`. With all live-provider credentials blanked,
`builtin.echo`, and TUI streaming disabled, the CLI:

- constructed the runtime;
- accepted `M0 startup smoke`;
- returned the offline echo response;
- exited after `/exit`;
- reported 0.02 seconds wall time.

Startup emitted three resolution warnings: `general-subagent` names
`email-digest`, `rss-digest`, and `fetch-arxiv-paper-list`, none of which is in
the parent's enabled skill catalog. No repository state was used by this
temporary startup fixture.

## Gateway smoke

`tests/gateway_pipeline.py` ran the 50-task Telegram fixture against local mock
servers and the debug gateway with `builtin.echo`:

- elapsed: 69 seconds (`07:27:40Z` to `07:28:49Z`);
- 50 task entries across smoke, slash, reasoning, tool, skill, guardrail, edge,
  and multi-turn categories;
- 0 timeouts;
- 0 task error arrays;
- 2 expected notes for edge-case handling.

The sandboxed command runner could not bind loopback, so the smoke required the
approved local-network test permission. The harness does not set a temporary
`AGENTOS_HOME`; it wrote ignored test artifacts and session data under the
checkout. This is a hermeticity gap for `CI-002`, not a clean-room result.

## Storage snapshot

Measured after the gateway smoke, so these numbers include its session writes:

| Store | Size / rows |
|---|---|
| `workspace/` | 572 KiB |
| `workspace/agentos.sqlite` | 217,088 bytes (53 pages × 4096 bytes) |
| `session_items` | 82 rows |
| `memory_records` | 0 rows |
| `memory_access_log` | 422 rows |
| `tests/_artifacts/` | 8 KiB |
| `tests/issues.json` | 27,241 bytes, ignored |
| gateway test log | 3,771 bytes, ignored |

The database reports `PRAGMA user_version = 0`; M2's versioned persistence
migration must not assume an existing nonzero schema marker.

## Next measurement

Re-run this snapshot after `TEST-001`, then require a green workspace test and
aggregate bench smoke after M1/`CI-001`. M7 replaces this local snapshot with
required Linux and macOS artifact, gateway, migration, sandbox, and semver jobs.
