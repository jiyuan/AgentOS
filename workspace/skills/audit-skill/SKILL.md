---
name: audit-skill
description: Reconstruct and summarize AgentOS activity from trace JSONL, session JSONL, and gateway logs. Use when the user asks for an audit, usage report, token consumption, model call count, task executions, task success rate, operational health summary, review, or ranking.
---

# Audit Skill

Produce an AgentOS operational audit for a requested time window, defaulting to the trailing 24 hours. The audit must use the best available local evidence from trace JSONL and session JSONL files, and gateway log files.

## When To Use

Trigger this skill when the user asks any of:

- "audit" or "usage report"
- "token consumption" or "how many tokens"
- "model calls" or "how many LLM calls"
- "task executions" or "task success rate"
- "operational health" or "activity summary"
- "review" or "rank"

## Mandatory Evaluation Dimensions

Whenever the user asks for a **review** or **ranking**, you must evaluate all four dimensions below. Do not omit any dimension, and do not treat them as optional or implied:

1. **Correctness**
2. **Completeness**
3. **Clarity**
4. **Actionability**

If the user does not specify a rubric, apply these four dimensions by default and present each one explicitly.

## Scope

Report these metrics every time:

| Metric | Source |
|---|---|
| Total tokens consumed (input/output/cache hit/miss/write) | Trace JSONL `event.name="llm_token_usage"` records (one per LLM call) with `call_input_tokens`, `call_output_tokens`, `call_cache_read_tokens` (hit), `call_cache_miss_tokens`, `call_cache_write_tokens`; extracted by `scripts/audit_tokens.py`. Gateway log `agentos_llm::usage` lines are a fallback when traces are unavailable. |
| Total model calls | Trace JSONL: count of span records with `kind: "llm"` |
| Task executions | Trace JSONL: `run_started` events, or Session JSONL: `session_started` events |
| Task success rate | Trace JSONL: `run_finished` events, or Session JSONL: `session_finished` events |
| Cron job execution | Gateway log lines containing cron run/finish records (if available) |
| Model invocations by provider/model | Gateway log lines containing `llm provider=`, `model=` |
| Failed tasks with timestamps and details | Gateway log lines containing `"gateway run failed"`, `"resume failed"`, `"maximum turn count exceeded"`, `"guardrail"`, or `"approval denied"` |

**Token data is exact and lives in trace JSONL.** Each LLM call emits an `event.name="llm_token_usage"` record into the per-run trace file at `$AGENTOS_HOME/workspace/traces/<run-id>.jsonl`. The record carries `call_input_tokens`, `call_output_tokens`, `call_total_tokens`, `call_cache_read_tokens` (hit), `call_cache_miss_tokens`, `call_cache_write_tokens` plus rolling `run_*_tokens` snapshots. Attribution is by `run_id` (e.g. `telegram-gateway`, `telegram-gateway:edit-subagent`, `cli-run-3`), which self-identifies channel and sub-agent. The emitter is at `crates/agentos-core/src/loop/mod.rs:523`.

The gateway log also receives an `agentos_llm::usage` `tracing::info!` event for the same data (`crates/agentos-core/src/loop/mod.rs:525`), but only when `RUST_LOG=agentos_llm::usage=info` is set. This is treated as a fallback. `scripts/audit_tokens.py` prefers the trace JSONL automatically.

## Workspace Resolution

Every path this skill touches anchors on `$AGENTOS_HOME`, the **only** workspace knob. There are no per-path env overrides (`AGENTOS_AGENT_CONFIG_PATH`, `AGENTOS_CRON_DIR`, `AGENTOS_SKILLS_DIR`, etc. have all been removed).

Resolution cascade (matches `crates/agentos-interfaces/src/paths.rs`):

1. `AGENTOS_HOME` env var (set by user shell or loaded from `.env`).
2. Parent dir of the discovered `.env` file (for the Rust binaries).
3. `Path(__file__).resolve().parents[4]` (for `scripts/audit_tokens.py`, which lives at `$AGENTOS_HOME/workspace/skills/audit-skill/scripts/audit_tokens.py`).
4. CWD as last resort.

All path references below — `workspace/traces`, `workspace/main/sessions`, `logs/`, `workspace/crons` — are implicitly `$AGENTOS_HOME/...`. `scripts/audit_tokens.py` prints the resolved `AGENTOS_HOME` to stderr at startup.

## Data Sources

Read sources in this order until enough evidence exists. Every directory listing must filter by mtime (24-hour default), and every file read must pull only the tail — see [Temporal Filtering](#temporal-filtering) and [Incremental Retrieval](#incremental-retrieval) for the two equivalent execution paths.

1. List `$AGENTOS_HOME/workspace/traces` filtered to the last 24 hours.
2. List `$AGENTOS_HOME/workspace/main/sessions` filtered to the last 24 hours.
3. List `$AGENTOS_HOME/logs` filtered to the last 24 hours.

Then for each file returned by those listings, retrieve the tail only:

4. Trace JSONL: `tail -n 500 <trace-jsonl>` (preferred) or `file(... max_bytes=65536, tail=true)`.
5. Session JSONL: `tail -n 500 <session-jsonl>` (preferred) or the file-tool tail fallback.
6. Gateway log: `tail -n 500 <log-file>` (preferred) or the file-tool tail fallback.

Files whose mtime falls outside the window must not appear in steps 4–6 at all.

## Temporal Filtering

Filter files based on their last modification time and **exclude any file whose mtime is older than the audit window**. The default window is 24 hours, so any file not modified in the last 24 hours must be skipped entirely — do not open it, do not tail it, do not parse it.

Two equivalent ways to enforce the cutoff, in order of preference:

1. **File tool directory listing** (preferred): pass `modified_within_hours=24` so the listing returns only files within the window:

   ```
   file(operation="read", path="workspace/traces", include_metadata=true, modified_within_hours=24)
   ```

2. **Shell `find` with mtime** (fallback when the directory listing path is unavailable): `find` is in the shell allowlist; use `-mmin -1440` (minutes) for a 24-hour window:

   ```
   shell(command="find", args=["workspace/traces", "-type", "f", "-mmin", "-1440"])
   ```

If the user requests a different window, convert it to hours for `modified_within_hours` (or to minutes for `find -mmin`). If a known directory has no files inside the window, skip that source entirely and mention the omission in the data-source note. Never widen the window silently to "find something" — an empty window is a valid finding.

## Incremental Retrieval

Large trace, session, and gateway log files must never be loaded in full. Read only the tail — the most recent entries — to keep latency bounded and to stay inside the bounded-evidence contract of this audit.

Two equivalent paths, in order of preference:

1. **`tail` shell command** (preferred for large log files): `tail` is in the shell allowlist; pull only the last N lines (default 500 — adjust upward only if a metric is still under-evidenced):

   ```
   shell(command="tail", args=["-n", "500", "<path>"])
   ```

   For very large gateway logs, prefer `tail -n 500` over a byte-bounded read because line-aligned output is cheaper to parse and never produces a truncated leading JSON record.

2. **File tool tail mode** (fallback when shell is unavailable or the file is small): `tail=true` with `max_bytes=65536`:

   ```
   file(operation="read", path="<path>", max_bytes=65536, tail=true)
   ```

If any retrieved sample is marked truncated, compute the audit from that bounded tail and state in the report that it is based on bounded recent evidence. Do not page backwards through history unless the user explicitly asks for a forensic full-history audit.

## Extraction Rules

Parse trace JSONL and session JSONL by reading each line as a JSON record. Build counters by scanning for specific record shapes.

### Generate Timestamp

Use the current UTC time in `YYYY-MM-DD HH:MM` format for the `Generated` field. Compute the window start as now minus the requested hours (default 24).

### Model Calls (LLM invocations)

Count trace JSONL records where:

- `record_type` is `"span"` AND
- `span.kind` is `"llm"`

These represent LLM invocations (orchestrator planning calls). In the current trace files, spans named `"orchestrator.plan"` with `kind: "llm"` are the LLM calls.

### Token Consumption

**Primary path: `scripts/audit_tokens.py`.** This pure-Python helper auto-selects the canonical source — trace JSONL first, gateway log as fallback — extracts every `llm_token_usage` event, aggregates totals, breaks them down by run_id / channel / sub-agent / cron, and emits a Markdown report. Always prefer it over hand-parsing.

```
shell(command="python3", args=[
  "$AGENTOS_HOME/workspace/skills/audit-skill/scripts/audit_tokens.py",
  "--hours", "24"
])
```

The helper resolves `$AGENTOS_HOME` itself and derives `--traces-dir`, `--log`, and `--crons-dir` from it. Pin `--source=traces` or `--source=log` to force one path. Pass `--json` for machine-readable output, `-o <path>` to write to a file.

**Source A — trace JSONL (canonical).** Per-LLM-call events at `$AGENTOS_HOME/workspace/traces/<run-id>.jsonl` with `record_type="event"` and `event.name="llm_token_usage"`. Each event carries:
- `call_input_tokens`, `call_output_tokens`, `call_total_tokens`
- `call_cache_read_tokens` (= **hit**), `call_cache_miss_tokens` (= **miss**), `call_cache_write_tokens`
- Rolling `run_*_tokens` snapshots and `run_llm_calls` count

Attribution is exact, by `run_id`:
- **By Run ID**: one row per trace file with non-zero token activity.
- **By Channel**: derived from `run_id` prefix — `telegram-gateway*` → telegram, `feishu-gateway*` → feishu, `cli-run-*` → cli, `tui*` → tui.
- **By Sub-Agent**: derived from the suffix after `:` in `run_id` (e.g. `telegram-gateway:edit-subagent` → `edit-subagent`).
- **By Cron**: best-effort overlay — if a trace file's mtime falls within `[last_fired_unix, last_fired_unix + --cron-window-secs]` of a cron, that file's tokens are also attributed to the cron. Trace events have no per-record timestamp, so cron attribution is file-granular.

**Source B — gateway log (fallback).** Same data emitted as `tracing::info!(target: "agentos_llm::usage", ...)` at `crates/agentos-core/src/loop/mod.rs:525`. Requires `RUST_LOG=agentos_llm::usage=info` (set in `.env`). When this source is used, attribution falls back to timestamp-window matching for cron (`--cron-window-secs`) and channel (`--channel-window-secs`) since the line itself carries no `run_id`. Used only when trace JSONL is empty or `--source=log` is forced.

If both sources are empty, the helper says so plainly — do not fabricate estimates. The skill no longer maintains an "estimated from character counts" fallback.

### Model Invocations (Provider / Model)

Extract the provider and model from gateway log startup lines:

- `llm provider=<provider>, model=<model>`

Group LLM calls by the combination of provider and model observed during the audit window. If no provider/model line is found, use the most recent startup line's values and note the assumption. Show each provider/model pair with its call count in the Model Invocations table.

### Task Executions

Count `event.name` is `"run_started"` in trace JSONL, or `event` is `"session_started"` in session JSONL. Avoid double-counting the same run when both trace and session evidence point to the same `run_id`.

### Successful Tasks

Count `event.name` is `"run_finished"` in trace JSONL, or `event` is `"session_finished"` in session JSONL. A task is successful when its final recorded outcome does not contain a failure marker.

### Failed / Aborted Tasks

Count gateway log lines containing `"gateway run failed"`, `"resume failed"`, `"maximum turn count exceeded"`, `"guardrail"`, or `"approval denied"`. For each failed task, extract:

- **Timestamp**: The bracketed epoch timestamp `[NNNNNNNNNN]` on the failure line. Convert to `YYYY-MM-DD HH:MM UTC`.
- **Status**: Use `aborted` for approval denials, `failed` for run failures, `error` for guardrail/other.
- **Details**: The message text after the timestamp, summarized to one line.

Currently trace JSONL and session JSONL may show failed tool calls (e.g. `tool_finished` with `status: "failed"`) but these are tool failures, not necessarily task failures.

### Task Success Rate

`succeeded_task_count / task_execution_count * 100`, rounded to one decimal place. Display as `XX.X% (succeeded/total)`. If task executions is zero, report `0% (0/0)`.

### Cron Job Execution

Look for gateway log lines containing `"cron"` or `"cron_run"` or `"cron_finished"`. If cron-related data is present, report:

- **Finished runs**: count of completed cron runs
- **Successes**: count of cron runs that succeeded
- **Errors**: count of cron runs that failed
- **Deliveries succeeded**: count of successful cron message deliveries

If no cron data is found in the gateway log or any other source, report `0` for all cron metrics and add a note: _No cron execution records found in the audit window._

### Gateway Log Trace Summary Lines

Gateway log lines like `trace: run=1, plan=10, llm=10` provide a quick summary of run/plan/LLM counts per batch. These are useful as a cross-check but less reliable than structured JSONL parsing. When the gateway log is available, extract these lines and compare against your JSONL counts.

## Workflow

1. List traces directory (`workspace/traces`), filtered to mtime within the audit window (24h default). Drop any file outside the window.
2. List sessions directory (`workspace/main/sessions`) under the same mtime filter.
3. List logs directory under the same mtime filter.
4. For each in-window trace JSONL file, `tail -n 500` to retrieve only the most recent records.
5. For each in-window session JSONL file, `tail -n 500` (same rule).
6. For each in-window gateway log file, `tail -n 500` (same rule). Never read the full file.
7. Run `scripts/audit_tokens.py --hours 24` to extract token totals plus by-run-id / by-channel / by-sub-agent / by-cron breakdowns from `$AGENTOS_HOME/workspace/traces/*.jsonl` (canonical) or the gateway log (fallback). Use its output directly in the Token Consumption and Token Consumption by Cron/Channel/Sub-Agent sections.
8. Parse each line of the retrieved trace/session/log samples to extract the remaining metrics:
   - Count LLM spans for model calls.
   - If the helper printed "Source: none" both sources were empty — report zeros and surface the diagnostic. Do not invent estimates.
   - Count `run_started`/`session_started` events for task executions.
   - Count `run_finished`/`session_finished` events for successful tasks.
   - Extract provider/model from gateway startup lines (the helper also surfaces them from usage lines).
   - Extract failed task timestamps and details from gateway log.
   - Extract cron execution records if present.
   - Extract gateway log trace summary lines.
9. Compute success rate.
10. When the user asked for a review or ranking, include the four mandatory evaluation dimensions explicitly in the response.
11. Write the report using the output format below.

## Output Format

Return a structured Markdown report using this exact template:

```markdown
# Audit Report

Generated: YYYY-MM-DD HH:MM UTC
Window: last 24 hours

***

## Overview

| Metric                | Value    |
| --------------------- | -------- |
| Total tokens consumed | 0        |
| Total model calls     | 0        |
| Task executions       | 0        |
| Task success rate     | 0% (0/0) |

***

## Token Consumption

| Type        | Tokens |
| ----------- | ------ |
| Input       | 0      |
| Output      | 0      |
| Cache hit   | 0      |
| Cache miss  | 0      |
| Cache write | 0      |
| **Total**   | **0**  |

_All token values from `scripts/audit_tokens.py` are exact when sourced from trace JSONL or the gateway log. If both sources are empty, report zeros and add: "No `llm_token_usage` events in `$AGENTOS_HOME/workspace/traces/` and no usage lines in the gateway log for this window."_

***

## Token Consumption by Cron Task

One row per cron defined in `$AGENTOS_HOME/workspace/crons/` that registered token activity. When sourced from trace JSONL (canonical), cron attribution uses each trace file's mtime within `[last_fired_unix, last_fired_unix + --cron-window-secs]` (default 30 min). When sourced from the gateway log fallback, attribution uses each token line's epoch within the same window.

| Cron ID | Last Fired (UTC) | Schedule | Calls | Input | Output | Hit | Miss |
| ------- | ---------------- | -------- | ----- | ----- | ------ | --- | ---- |
| _cron-id_ | YYYY-MM-DD HH:MM | `m h dom mon dow` | 0 | 0 | 0 | 0 | 0 |

***

## Token Consumption by Channel (Non-Cron Task Types)

One row per channel (`telegram`, `feishu`, `tui`, `cli`) whose marker preceded a non-cron token line within `--channel-window-secs` (default 5 min). This is the breakdown of non-cron task types — i.e., interactive runs initiated from each channel.

| Channel | Calls | Input | Output | Hit | Miss |
| ------- | ----- | ----- | ------ | --- | ---- |
| _channel_ | 0 | 0 | 0 | 0 | 0 |

## Token Consumption — Uncategorized

Token activity that matched neither a cron's run window nor any channel marker.

| Calls | Input | Output | Hit | Miss |
| ----- | ----- | ------ | --- | ---- |
| 0 | 0 | 0 | 0 | 0 |

_If both trace JSONL and the gateway log are empty for the window, leave all per-task-type tables at zero and add: "No `llm_token_usage` events in trace JSONL and no usage lines in the gateway log for this window. If runs did happen, the trace directory may have been pruned or the events were never emitted."_

***

## Task Completion

| Outcome          | Count |
| ---------------- | ----- |
| Succeeded        | 0     |
| Failed / aborted | 0     |
| **Total**        | **0** |

***

## Cron Job Execution

| Metric               | Value |
| -------------------- | ----- |
| Finished runs        | 0     |
| Successes            | 0     |
| Errors               | 0     |
| Deliveries succeeded | 0     |

_If no cron data is found, add: "No cron execution records found in the audit window."_

***

## Model Invocations

| Provider / Model           | Calls |
| -------------------------- | ----- |
| provider-name / model-name | 0     |

_Extract provider and model from gateway log startup lines. If no line is found, use the most recent startup line available and note the assumption._

***

## Recent Failed Tasks

| Timestamp (UTC)   | Status  | Details                              |
| ----------------- | ------- | ------------------------------------ |
| YYYY-MM-DD HH:MM  | aborted | Brief description of failure context |

_If no failed tasks, leave the table body empty and add: "No failed tasks in the audit window." below the table._

***

## Definitions

*   **Total tokens consumed**: summed from trace JSONL `event.name="llm_token_usage"` records' `call_*_tokens` fields within the audit window (via `scripts/audit_tokens.py`). Falls back to gateway log `agentos_llm::usage` lines if trace JSONL is empty. Both sources are exact; there is no estimation path.
*   **Cache hit / miss / write tokens**: `cache_read_tokens`, `cache_miss_tokens`, `cache_write_tokens` fields on the same log event.
*   **Token consumption by cron task**: tokens attributed to a given cron via timestamp correlation — line at epoch `t` belongs to cron `c` iff `c.last_fired_unix ≤ t ≤ c.last_fired_unix + --cron-window-secs` (default 1800s); overlapping windows resolve to the most recently fired cron.
*   **Token consumption by channel / task type (non-cron)**: any in-window token line not attributed to a cron is checked against the latest gateway-log line whose payload starts with `telegram`/`feishu`/`tui`/`cli`; if that marker is within `--channel-window-secs` (default 300s) before the token line, the line is attributed to that channel. This produces the non-cron task-type breakdown.
*   **Token consumption uncategorized**: token activity that matched neither a cron window nor a channel marker.
*   **Total model calls**: number of `kind: "llm"` span records in trace JSONL within the audit window.
*   **Task executions**: `run_started` events in trace JSONL or `session_started` events in session JSONL within the audit window.
*   **Task success rate**: share of task executions whose final recorded outcome was `run_finished`/`session_finished` without a gateway failure marker (`gateway run failed`, `maximum turn count exceeded`, `guardrail`, `approval denied`).
*   **Cron job execution**: finished cron-run records within the audit window.
*   **Model invocations**: call counts grouped by combined provider and model identifier from gateway startup lines.
```

Always populate every section with the best available data. Do not omit sections — if a section has no data, show zeros and add a note explaining the absence.

## Validation

Run `skill_validate("audit-skill")` after editing this skill. The validator checks frontmatter shape and body presence. Validation passes if the bundle contains valid `SKILL.md` with correct frontmatter.

## Known Limitations

- Trace JSONL `kind: "llm"` span records (e.g. `orchestrator.plan`) do not themselves carry token counts. The exact counts are in sibling records with `record_type: "event"` and `event.name: "llm_token_usage"` (emitted at `crates/agentos-core/src/loop/mod.rs:523`). `scripts/audit_tokens.py` parses those.
- The session JSONL files contain `transcript_item` records with `message.role: "assistant"` but their `metadata` object is often empty (`{}`) — no token tracking data is attached.
- `agentos_llm::usage` gateway-log emission is gated by `RUST_LOG=agentos_llm::usage=info` (set in `.env`). This only affects the fallback source; trace JSONL events are always emitted regardless of `RUST_LOG`.
- Cron attribution from trace JSONL is **file-granular** (uses the trace file's mtime as a proxy for run end time), not per-event. A cron and a channel run that overlap within `--cron-window-secs` may attribute the same trace to both buckets. The proper fix is propagating `cron_id` / `sender` into the `llm_token_usage` event fields in `crates/agentos-core/src/loop/mod.rs:523`.
- Channel attribution depends on gateway-log lines whose payload starts with a known channel word. If a channel's only activity in the window is the LLM call itself (no preceding marker), it lands in **Uncategorized**, not **By Channel**.
- The `content_bytes` field on `orchestrator_task_assigned` events provides output/plan byte sizes (37–164 bytes for tool plans, up to ~4000 bytes for reply plans) but does not include any input token counts.
- Cron job execution data may not be present in the gateway log; report zeros with a note when absent.
