# Audit baseline

M0 deliverable 2 of [`AUDIT_REMEDIATION_PLAN.md`](AUDIT_REMEDIATION_PLAN.md).
What the system measured *before* the remediation milestones changed it, so a
later regression can be told apart from a later improvement.

The filename carries the audit date, 2026-08-18. The figures were captured on
**2026-08-21** at commit `a7a0217`, which is the M0 tree: `CFG-000`, `DOC-001`,
`ADR-001`, and half of `TEST-001` had landed, none of which touches a hot path.
Where the audit's own count differs from a measurement here, both are given.

## Measurement environment

Every figure below is from one machine, and none of it is a cross-machine
comparison. Re-measure before drawing a conclusion from a delta.

| | |
|---|---|
| Platform | Linux 6.12.72-linuxkit, aarch64 (containerized) |
| Cores | 16 |
| Memory | 7 GiB total |
| Toolchain | rustc 1.97.1 (8bab26f4f 2026-07-14) |
| Profile | `release` for benchmarks, `debug` for tests |
| Sandbox | Landlock **available and enforcing** — every sandbox test ran, none skipped |
| Build parallelism | `-j 1`; the linker is OOM-killed at default parallelism on this machine |

That last row is itself a finding: `cargo test --workspace` at default
parallelism fails with `ld terminated with signal 9` on a 7 GiB machine. It is
a local build-ergonomics problem, not a code defect, but it will bite any
contributor on a similar box.

## Loop overhead

Criterion, 100 samples, 3 s warmup, `current_thread` runtimes, release profile.
Budget from [`BENCHMARKS.md`](../BENCHMARKS.md): **≤ 2 ms per turn** excluding
LLM and tool latency.

| Bench | 2026-06-11 (BENCHMARKS.md) | 2026-08-21 | Δ | Headroom vs budget |
|---|---|---|---|---|
| `loop_overhead/reply_turn` | 2.48 µs | **2.59 µs** | +4.4% | ~770× |
| `loop_overhead/tool_turn_allow` | 6.50 µs | **6.22 µs** | −4.3% | ~320× |
| `loop_overhead/ask_user_pause_resume` | 11.0 µs | **11.18 µs** | +1.6% | ~180× |
| `loop_overhead/paused_state_json_round_trip` | 3.57 µs | **3.66 µs** | +2.5% | control |
| `loop_overhead/hydrated_memory_plan_turn` | 278 µs | **269 µs** | −3.2% | ~7× |

All five moves are inside or near the ±5–8% band `BENCHMARKS.md` documents for
this class of machine, and the control bench moved with them, so this reads as
machine difference rather than a code change. **The loop is nowhere near its
budget, and no remediation milestone is performance-constrained.**

### Allocations per turn

| Bench | Allocations | Bytes |
|---|---|---|
| `alloc_count/reply_turn` | 45 | 42,920 |
| `alloc_count/tool_turn_allow` | 120 | 51,370 |

### Concurrency

1000 concurrent tool-call conversations per round, 5 measured rounds, 16 shard
threads. (The loop future is `!Send`, so the harness uses shard threads rather
than a multi-thread runtime — see the bench module docs.)

| Round | Wall | Throughput | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| 1 | 1.450 ms | 689,477/s | 4.75 µs | 5.79 µs | 21.92 µs | 102.75 µs |
| 2 | 1.241 ms | 806,018/s | 4.75 µs | 6.00 µs | 20.00 µs | 32.75 µs |
| 3 | 1.221 ms | 818,694/s | 4.79 µs | 5.67 µs | 22.08 µs | 49.71 µs |
| 4 | 1.385 ms | 721,891/s | 4.75 µs | 5.42 µs | 21.54 µs | 40.58 µs |
| 5 | 1.314 ms | 760,818/s | 4.75 µs | 5.46 µs | 18.13 µs | 39.08 µs |
| **aggregate** | — | — | **4.75 µs** | **5.71 µs** | **20.29 µs** | **102.75 µs** |

p50 is flat to three digits across all five rounds, so the tail — p99 at ~4×
p50, and a first-round max 2–3× every later round — is warmup and scheduling
noise rather than contention. Worth recording because M8 changes the
persistence concurrency model (one global `Mutex<Connection>` today), and this
is the "before" it must not regress.

## Storage

From the reference deployment's live database, `workspace/agentos.sqlite`.
This is real accumulated use, not a synthetic fixture, which is what makes it
worth recording.

| | |
|---|---|
| File size | **142.1 MB** (34,697 pages × 4096 B) |
| Freelist | 0 pages |
| Journal mode | **`delete`** — not WAL, confirming the M8 finding by measurement |
| Tables | 18 |

| Table | Rows | Size |
|---|---|---|
| `memory_records_vec_vector_chunks00` | — | **127.7 MB** |
| `memory_access_log` | 17,443 | 2.0 MB |
| `session_items` | 3,548 | 1.9 MB |
| `memory_records_vec_metadatachunks00` | — | 1.4 MB |
| `idx_memory_access_log_namespace_row` | — | 1.0 MB |
| `memory_records_vec_chunks` | 85 | 0.7 MB |
| `memory_records` | **189** | 0.3 MB |
| `memory_records_fts_content` | 189 | 0.2 MB |
| `memory_links` | 0 | — |

**Two things stand out and both belong to M7 deliverable 10 (retention and
quotas):**

1. **189 memory records occupy 127.7 MB of vector chunks** — roughly 676 KB
   per record. `sqlite-vec` allocates fixed-size chunks, so the index cost is
   driven by chunk geometry rather than by record count. Any retention policy
   that prunes records without reclaiming chunks will not move this number.
2. **`memory_access_log` has 17,443 rows against 189 records** — a ~92:1 ratio,
   growing monotonically with no retention. It is also the model M6 adopts for
   the safety-event store, so its growth rate is a preview of that store's.

## Configuration surface

| | Count |
|---|---|
| Keys accepted by `agent.toml` | 153 |
| Keys with no doc comment (`config/undocumented.txt`) | **102** (67%) |
| Config structs | 32 |
| Structs with `deny_unknown_fields` | **7** (22%) |

The audit reported "6 of 36" structs with `deny_unknown_fields`; the count here
is 7 of 32 at this commit, one of which (`ShellProfileConfig`) is new in
`CFG-000`. The finding is unchanged: a typo such as `[memory.polcy]` still
loads silently with defaults.

## Test and verification floor

| | Count |
|---|---|
| Tests passing, `cargo test --workspace` | 647 |
| Tests failing | 0 |
| Tests ignored | **8** — all deliberate red regression tests (7 in `audit_regressions.rs`, 1 in `exec.rs`) |
| Integration test files (`agentos-core`) | 24 |
| Golden transcripts | 11 |
| Rust source files | 180 across 5 crates |
| Lines of Rust | 56,736 |
| `unbounded_channel()` uses | **0** |
| `std_mpsc::channel()` (unbounded variant) uses | **3** — all in `tools/mcp.rs` (M8) |

Green at this commit: `cargo clippy --workspace --all-targets -- -D warnings`,
`check-import-boundaries.sh`, `check-module-size.sh`, `check-catalogs.sh`,
`git diff --check`.

Two files exceed the 800-LOC module budget under an explicit allowlist:
`agentos-gateway/main.rs` (1,327) and `agentos-cli/src/main.rs` (864).

## Coverage gaps this baseline records

Not figures, but the state a later run should be compared against:

- **No CI job runs on macOS.** The Seatbelt backend has unit coverage only, and
  its `availability()` probe is a `Path::exists` rather than a real check.
- **stdio MCP has no test module and no integration test anywhere** —
  `tools/mcp.rs` is 392 LOC with zero coverage.
- **No package-install, gateway-pipeline, or migration test runs in CI.**
- **The 1000-case narrowing proptest scopes itself to "tool-granular, not
  argument-granular"**, so it cannot observe the `AUTH-002` defect. It passes
  today and will keep passing after the fix; it is not evidence either way.

## How to re-measure

```sh
cargo bench -p agentos-core -j 1                    # loop overhead, allocations
cargo test --workspace --locked -j 1                # test floor
grep -c '^| `' docs/CONFIG_CATALOG.md               # accepted config keys
grep -vc '^#' crates/agentos-core/src/config/undocumented.txt
python3 -c "import sqlite3;c=sqlite3.connect('file:workspace/agentos.sqlite?mode=ro',uri=True);\
print(sorted(c.execute('SELECT name,SUM(pgsize) s FROM dbstat GROUP BY name ORDER BY s DESC').fetchall(),key=lambda r:-r[1])[:8])"
```
