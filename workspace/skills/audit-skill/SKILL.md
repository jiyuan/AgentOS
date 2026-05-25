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
| Total tokens consumed (input/output/cache read/cache write) | Session JSONL: `llm_token_usage` events with `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens`; fall back to `content_bytes` on `orchestrator_task_assigned` events |
| Total model calls | Trace JSONL: count of span records with `kind: "llm"` |
| Task executions | Trace JSONL: `run_started` events, or Session JSONL: `session_started` events |
| Task success rate | Trace JSONL: `run_finished` events, or Session JSONL: `session_finished` events |
| Cron job execution | Gateway log lines containing cron run/finish records (if available) |
| Model invocations by provider/model | Gateway log lines containing `llm provider=`, `model=` |
| Failed tasks with timestamps and details | Gateway log lines containing `"gateway run failed"`, `"resume failed"`, `"maximum turn count exceeded"`, `"guardrail"`, or `"approval denied"` |

**Important: The actual trace and session JSONL files in this workspace do not contain `token_counts` or `usage` fields on most records.** The `content_bytes` field on `orchestrator_task_assigned` events provides plan output sizes only, not input token counts. The `llm_token_usage` event (when present in session JSONL) is the only source of exact token data including `cache_read_tokens` and `cache_write_tokens`. All other token metrics must be **estimated** from visible text character counts unless exact fields are verified present.

## Data Sources

Read sources in this order until enough evidence exists. Every directory listing must filter by mtime (24-hour default), and every file read must pull only the tail — see [Temporal Filtering](#temporal-filtering) and [Incremental Retrieval](#incremental-retrieval) for the two equivalent execution paths.

1. List `workspace/traces` filtered to the last 24 hours.
2. List `workspace/main/sessions` filtered to the last 24 hours.
3. List `logs` filtered to the last 24 hours.

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

Collect token data from multiple sources, preferring exact fields when available:

1. **Exact (preferred)**: Session JSONL `llm_token_usage` events contain `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens`, and `total_tokens`. Sum these across all sessions within the audit window.

2. **Estimation fallback — session assistant transcript text**: For each `transcript_item` where `message.role` is `"assistant"`, count the characters in `message.content`. Estimate tokens as `ceil(character_count / 4)`.

3. **Estimation fallback — `orchestrator_task_assigned`**: The `fields.content_bytes` field on `orchestrator_task_assigned` events (where `plan_kind` is `"reply"` and `target_type` is `"assistant"`) contains the plan output size in bytes. Use this as the output token estimate.

4. **Estimation fallback — input token estimation**: Count characters in the corresponding user message from the same session turn. Estimate as `ceil(char_count / 4)`.

Report exact token totals when `llm_token_usage` events exist and cover all sessions. When only partial or no exact data is available, label token values as **estimated** and prefix them with `~`.

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
7. Parse each line of the retrieved trace/session/log samples to extract metrics:
   - Count LLM spans for model calls.
   - Sum `llm_token_usage` events for exact token counts (including cache read/write).
   - Estimate remaining tokens from `content_bytes` and character counts when exact data is partial.
   - Count `run_started`/`session_started` events for task executions.
   - Count `run_finished`/`session_finished` events for successful tasks.
   - Extract provider/model from gateway startup lines.
   - Extract failed task timestamps and details from gateway log.
   - Extract cron execution records if present.
   - Extract gateway log trace summary lines.
8. Compute success rate.
9. When the user asked for a review or ranking, include the four mandatory evaluation dimensions explicitly in the response.
10. Write the report using the output format below.

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
| Cache read  | 0      |
| Cache write | 0      |
| **Total**   | **0**  |

_If any token values are estimated rather than exact, add a note after the table: "~ denotes estimated values. Estimation method: ..."_

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

*   **Total tokens consumed**: summed from `llm_token_usage` events within the audit window, across all token types. When exact data is unavailable, estimated from `content_bytes` on `orchestrator_task_assigned` events and character counts in transcript text (`ceil(char_count / 4)`).
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

- The trace JSONL files contain `kind: "llm"` span records (e.g. `orchestrator.plan`) but these spans do not carry `fields.token_counts` or `fields.usage` dicts. Token metrics are therefore always estimated unless `llm_token_usage` events are present in session JSONL.
- The session JSONL files contain `transcript_item` records with `message.role: "assistant"` but their `metadata` object is often empty (`{}`) — no token tracking data is attached.
- The `llm_token_usage` event, when present, is the only source of exact token data (including `cache_read_tokens` and `cache_write_tokens`). Its presence is inconsistent across sessions.
- The `content_bytes` field on `orchestrator_task_assigned` events provides output/plan byte sizes (37–164 bytes for tool plans, up to ~4000 bytes for reply plans) but does not include any input token counts.
- Cron job execution data may not be present in the gateway log; report zeros with a note when absent.
