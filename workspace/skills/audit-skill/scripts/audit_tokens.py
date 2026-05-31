#!/usr/bin/env python3
"""
Audit AgentOS LLM token consumption from trace JSONL (canonical) or gateway
log (fallback).

Trace JSONL events are emitted by `crates/agentos-core/src/loop/mod.rs`
(`record_llm_usage`) once per LLM round-trip — including tool-calling rounds.
Gateway-log `"llm token usage"` lines are emitted by
`crates/agentos-llm/src/providers/mod.rs` (`log_token_usage`) at the provider
layer for the same calls. The two emitters live in different layers, so
`--cross-check` validates that they agree. Pure Python stdlib — no third-party
deps.

Source-of-truth event fields:
    provider, model,
    input_tokens, output_tokens, total_tokens,
    cache_read_tokens, cache_write_tokens, cache_miss_tokens

Note: "cache hit" = cache_read_tokens; "cache miss" = cache_miss_tokens.

For the token lines to appear in the log, the gateway must be started with
`RUST_LOG=agentos_llm::usage=info` (or any RUST_LOG that includes this target
at >= info). If the log has zero matching lines, the report will say so.

Task-type attribution (timestamp correlation, in priority order):
  1. cron:<id>   — token line falls in [fired, fired + --cron-window-secs]
                   of some cron's last_fired_unix in workspace/crons/<id>.toml.
  2. channel:<n> — token line is within --channel-window-secs after a
                   gateway-log line whose payload starts with a known channel
                   word (telegram, feishu, tui, cli).
  3. uncategorized — neither matches.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def resolve_agentos_home() -> Path:
    """Resolve the workspace anchor with the same cascade the Rust binaries use.

    1. AGENTOS_HOME env var (set by user shell or by .env load).
    2. Repo-root anchor via this file's location in the canonical bundle layout:
       $AGENTOS_HOME/workspace/skills/audit-skill/scripts/audit_tokens.py
       so Path(__file__).resolve().parents[4] is AGENTOS_HOME.
    3. CWD as last resort.
    """
    val = os.environ.get("AGENTOS_HOME")
    if val:
        return Path(val).resolve()
    try:
        return Path(__file__).resolve().parents[4]
    except IndexError:
        return Path.cwd()

# Gateway log line: leading bracket-epoch + free-form payload.
#   [1779160076] trace: run=1, plan=1, llm=1
#   [1779160076] llm token usage provider=openai model=gpt-5.4 input_tokens=...
EPOCH_RE = re.compile(r"^\[(\d{10,11})\]\s*(.*)$")

# Channel attribution: gateway log payloads that begin with one of these words
# (followed by whitespace) signal activity on the named channel — e.g.
#   "telegram conversation 5480467472 cleared"
#   "feishu long connection receive failed"
#   "telegram approval prompt sent"
# Startup/banner lines like "orchestrator=max..." or "trace: run=1..." are
# not channel activity and are filtered out by the strict prefix match.
KNOWN_CHANNELS = ("telegram", "feishu", "tui", "cli")
CHANNEL_MARKER_RE = re.compile(
    rf"^(?P<chan>{'|'.join(KNOWN_CHANNELS)})\b"
)

# Tracing default formatter renders fields as `name=value` (no quoting for ints,
# quoted for strings). The message text "llm token usage" is the anchor.
USAGE_ANCHOR = "llm token usage"

# Per-field regex — applied to the post-anchor segment of a matching line.
# Tolerant of order and of extra surrounding fields.
INT_FIELDS = (
    "input_tokens", "output_tokens", "total_tokens",
    "cache_read_tokens", "cache_write_tokens", "cache_miss_tokens",
)
STR_FIELDS = ("provider", "model")


def _int_field_re(name):
    return re.compile(rf"\b{re.escape(name)}=(\d+)\b")


def _str_field_re(name):
    # Either bare token (alnum/dash/dot/underscore) or quoted.
    return re.compile(rf'\b{re.escape(name)}=(?:"([^"]*)"|([\w.\-]+))')


INT_RES = {n: _int_field_re(n) for n in INT_FIELDS}
STR_RES = {n: _str_field_re(n) for n in STR_FIELDS}


def parse_usage_line(payload):
    """Extract token-usage fields from the post-bracket payload.

    Returns dict with provider/model + int fields, or None if not a usage line.
    """
    if USAGE_ANCHOR not in payload:
        return None
    out = {}
    for name, pat in STR_RES.items():
        m = pat.search(payload)
        if m:
            out[name] = m.group(1) if m.group(1) is not None else m.group(2)
    for name, pat in INT_RES.items():
        m = pat.search(payload)
        out[name] = int(m.group(1)) if m else 0
    out.setdefault("provider", "unknown")
    out.setdefault("model", "unknown")
    return out


def tail_bytes(path, max_bytes):
    """Seek to the last `max_bytes` of the file and return decoded text.

    Drops a partial leading line so the caller never has to parse a half-record.
    """
    size = os.path.getsize(path)
    start = max(0, size - max_bytes)
    with open(path, "rb") as f:
        f.seek(start)
        data = f.read()
    text = data.decode("utf-8", errors="replace")
    if start > 0:
        nl = text.find("\n")
        if nl >= 0:
            text = text[nl + 1:]
    return text


def load_crons(crons_dir):
    """Read cron TOMLs with the stdlib tomllib (3.11+) or a tiny fallback.

    Returns [{id, last_fired_unix, schedule_expression}].
    """
    crons = []
    if not crons_dir.exists():
        return crons
    try:
        import tomllib
        for path in sorted(crons_dir.glob("*.toml")):
            with path.open("rb") as f:
                doc = tomllib.load(f)
            crons.append({
                "id": doc.get("id", path.stem),
                "last_fired_unix": int(doc.get("last_fired_unix", 0) or 0),
                "schedule_expression": (doc.get("schedule", {}) or {}).get("expression", ""),
            })
    except ImportError:
        # Minimal fallback for Python < 3.11: extract just the three fields we need.
        for path in sorted(crons_dir.glob("*.toml")):
            text = path.read_text(encoding="utf-8")
            def _grab(key, default=""):
                m = re.search(rf'^\s*{re.escape(key)}\s*=\s*"?([^"\n]+?)"?\s*$', text, re.M)
                return m.group(1).strip() if m else default
            def _grab_int(key, default=0):
                m = re.search(rf'^\s*{re.escape(key)}\s*=\s*(\d+)\s*$', text, re.M)
                return int(m.group(1)) if m else default
            schedule_section = re.search(r'\[schedule\](.*?)(?:\n\[|\Z)', text, re.S)
            sched_expr = ""
            if schedule_section:
                me = re.search(r'expression\s*=\s*"([^"]+)"', schedule_section.group(1))
                if me:
                    sched_expr = me.group(1)
            crons.append({
                "id": _grab("id", path.stem),
                "last_fired_unix": _grab_int("last_fired_unix"),
                "schedule_expression": sched_expr,
            })
    return crons


def attribute_to_cron(epoch, crons, window_secs):
    """Return cron id whose last_fired_unix falls within `window_secs` before
    `epoch`, picking the most recent fire that still covers `epoch`. None if
    no cron matches.
    """
    best_id = None
    best_delta = window_secs + 1
    for c in crons:
        fired = c["last_fired_unix"]
        if fired <= 0:
            continue
        delta = epoch - fired
        if 0 <= delta <= window_secs and delta < best_delta:
            best_id = c["id"]
            best_delta = delta
    return best_id


def attribute_to_channel(epoch, markers, window_secs):
    """Return channel name from the latest gateway-log marker at `t' <= epoch`
    with `epoch - t' <= window_secs`. `markers` must be sorted by epoch.
    Returns None if no marker within the window.
    """
    # Linear scan from the end is fine — call rate per audit is low and
    # marker count stays in the hundreds even on busy days.
    best = None
    for t, chan in markers:
        if t > epoch:
            break
        if epoch - t <= window_secs:
            best = chan
    return best


def empty_bucket():
    return {
        "calls": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "cache_miss_tokens": 0,
    }


def fold(bucket, rec):
    bucket["calls"] += 1
    for k in INT_FIELDS:
        bucket[k] += rec.get(k, 0)


# Mapping from trace event field names (call_*_tokens) → bucket keys.
TRACE_CALL_FIELDS = {
    "input_tokens": "call_input_tokens",
    "output_tokens": "call_output_tokens",
    "total_tokens": "call_total_tokens",
    "cache_read_tokens": "call_cache_read_tokens",
    "cache_write_tokens": "call_cache_write_tokens",
    "cache_miss_tokens": "call_cache_miss_tokens",
}


def fold_trace_event(bucket, fields):
    """Fold one trace `llm_token_usage` event's call_* fields into a bucket."""
    bucket["calls"] += 1
    for bucket_key, trace_key in TRACE_CALL_FIELDS.items():
        bucket[bucket_key] += int(fields.get(trace_key, 0) or 0)


def classify_run_id(run_id):
    """Derive (parent_run, channel, subagent) from a run_id.

    Conventions observed in workspace/traces/:
      telegram-gateway                                 → (telegram-gateway, telegram, None)
      telegram-gateway:edit-subagent                   → (telegram-gateway, telegram, edit-subagent)
      feishu-gateway                                   → (feishu-gateway,   feishu,   None)
      cli-run-3                                        → (cli-run-3,        cli,      None)
      cli-run-2:general-subagent                       → (cli-run-2,        cli,      general-subagent)
      other                                            → (other,            unknown,  None)
    """
    parent, _, sub = run_id.partition(":")
    subagent = sub or None
    if parent.startswith("telegram-gateway"):
        channel = "telegram"
    elif parent.startswith("feishu-gateway"):
        channel = "feishu"
    elif parent.startswith("cli-run-"):
        channel = "cli"
    elif parent.startswith("tui"):
        channel = "tui"
    else:
        channel = "unknown"
    return parent, channel, subagent


def quick_log_totals(log_path, cutoff, max_bytes):
    """Tail the gateway log and sum every in-window `llm token usage` line into
    a totals bucket. Cheaper than the full log path because it ignores cron /
    channel attribution. Used only by `--cross-check` to validate trace JSONL
    against the provider-layer log emission — divergence between the two
    indicates a layer is silently dropping events.
    """
    totals = empty_bucket()
    if not log_path.exists():
        return totals, "missing"
    mtime = int(log_path.stat().st_mtime)
    if mtime < cutoff:
        return totals, "stale"
    text = tail_bytes(log_path, max_bytes)
    for line in text.splitlines():
        m = EPOCH_RE.match(line)
        if not m:
            continue
        epoch = int(m.group(1))
        if epoch < cutoff:
            continue
        rec = parse_usage_line(m.group(2))
        if rec is None:
            continue
        fold(totals, rec)
    return totals, "ok"


def emit_cross_check(report, log_totals, tolerance):
    """Compare trace-JSONL totals against gateway-log totals; warn on stderr
    when call count or total tokens diverge by more than `tolerance`
    (relative).
    """
    trace_totals = report["totals"]
    if trace_totals["calls"] == 0 and log_totals["calls"] == 0:
        return
    keys = ("calls", "input_tokens", "output_tokens", "cache_read_tokens", "cache_miss_tokens")
    divergences = []
    for key in keys:
        t = trace_totals[key]
        l = log_totals[key]
        if t == 0 and l == 0:
            continue
        ref = max(t, l)
        rel = abs(t - l) / ref if ref > 0 else 0.0
        if rel > tolerance:
            divergences.append((key, t, l, rel))
    if not divergences:
        return
    print(
        "[audit_tokens] cross-check WARN: trace JSONL and gateway log disagree "
        f"by more than {tolerance:.0%} on these metrics:",
        file=sys.stderr,
    )
    for key, t, l, rel in divergences:
        print(
            f"    {key}: trace={t} log={l} (rel diff {rel:.1%})",
            file=sys.stderr,
        )
    print(
        "    One emitter is dropping events. Trace events are written by "
        "`crates/agentos-core/src/loop/mod.rs::record_llm_usage`; the gateway "
        "log line is written by `crates/agentos-llm/src/providers/mod.rs::log_token_usage`. "
        "Re-run with `--source=log` to inspect the log-derived totals.",
        file=sys.stderr,
    )


def aggregate_from_traces(traces_dir, crons, cron_window_secs, cutoff):
    """Read every in-window trace JSONL file and aggregate `llm_token_usage`
    events. Returns a dict with totals + per-run/channel/subagent/cron buckets.

    Attribution:
      - run_id, channel, subagent come from the file's records (canonical).
      - cron is a best-effort overlay using file mtime as a proxy for run end:
        if mtime falls in [fired, fired + cron_window_secs] for some cron, the
        whole file's tokens are attributed to that cron in addition to its
        channel bucket.
    """
    result = {
        "files_scanned": 0,
        "files_in_window": 0,
        "events_found": 0,
        "run_started_count": 0,
        "totals": empty_bucket(),
        "by_run_id": {},
        "by_channel": {},
        "by_subagent": {},
        "by_cron": {c["id"]: empty_bucket() for c in crons},
    }
    if not traces_dir.exists():
        return result
    for path in sorted(traces_dir.glob("*.jsonl")):
        result["files_scanned"] += 1
        try:
            mtime = int(path.stat().st_mtime)
        except OSError:
            continue
        if mtime < cutoff:
            continue
        result["files_in_window"] += 1
        # Cron attribution by mtime (one match wins, closest).
        cron_id = None
        best_delta = cron_window_secs + 1
        for c in crons:
            fired = c["last_fired_unix"]
            if fired <= 0:
                continue
            delta = mtime - fired
            if 0 <= delta <= cron_window_secs and delta < best_delta:
                cron_id = c["id"]
                best_delta = delta
        try:
            with path.open("r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if rec.get("record_type") != "event":
                        continue
                    event = rec.get("event") or {}
                    event_name = event.get("name")
                    if event_name == "run_started":
                        result["run_started_count"] += 1
                        continue
                    if event_name != "llm_token_usage":
                        continue
                    fields = event.get("fields") or {}
                    run_id = rec.get("run_id") or "?"
                    parent, channel, subagent = classify_run_id(run_id)
                    result["events_found"] += 1
                    fold_trace_event(result["totals"], fields)
                    result["by_run_id"].setdefault(run_id, empty_bucket())
                    fold_trace_event(result["by_run_id"][run_id], fields)
                    result["by_channel"].setdefault(channel, empty_bucket())
                    fold_trace_event(result["by_channel"][channel], fields)
                    if subagent:
                        result["by_subagent"].setdefault(subagent, empty_bucket())
                        fold_trace_event(result["by_subagent"][subagent], fields)
                    if cron_id is not None:
                        fold_trace_event(result["by_cron"][cron_id], fields)
        except OSError:
            continue
    return result


def render_markdown(report):
    now = report["generated_at"]
    cutoff = report["window_start"]
    lines = []
    lines.append("# LLM Token Audit")
    lines.append("")
    lines.append(f"Generated: {now}")
    lines.append(f"Window: last {report['window_hours']}h (since {cutoff})")
    src = report["source"]
    if src == "traces":
        lines.append(
            f"Source: trace JSONL — `{report['traces_path']}` "
            f"({report['trace_files_in_window']} file(s) in window, "
            f"{report['trace_events_found']} `llm_token_usage` event(s))"
        )
    elif src == "log":
        lines.append(f"Source: gateway log — `{report['log_path']}`")
    else:
        lines.append(
            f"Source: none — checked traces (`{report['traces_path']}`) and log (`{report['log_path']}`)"
        )
    lines.append("")
    lines.append("---")
    lines.append("")
    if src is None:
        # Nothing was found in either source.
        if report["trace_files_in_window"] == 0:
            lines.append(
                "_No trace JSONL files were modified in the audit window. "
                "No gateway-log usage lines either._ "
                "If runs did happen, check `$AGENTOS_HOME/workspace/traces/` mtimes "
                "and the `RUST_LOG=agentos_llm::usage=info` env var."
            )
        else:
            lines.append(
                "_Trace files were present but contained no `llm_token_usage` events, "
                "and the gateway log had no usage lines either. "
                "Token recording may have been disabled when the runs executed._"
            )
        return "\n".join(lines) + "\n"

    t = report["totals"]
    lines.append("## Totals")
    lines.append("")
    lines.append("| Metric                    | Value |")
    lines.append("| ------------------------- | ----- |")
    lines.append(f"| LLM API invocations (compare to remote billed requests) | {t['calls']} |")
    lines.append(f"| User-facing tasks (`run_started`, context only) | {report['user_facing_tasks']} |")
    lines.append(f"| LLM calls (with usage)    | {t['calls']} |")
    lines.append(f"| Input tokens              | {t['input_tokens']} |")
    lines.append(f"| Output tokens             | {t['output_tokens']} |")
    lines.append(f"| Total tokens              | {t['total_tokens']} |")
    lines.append(f"| Cache hit  (read tokens)  | {t['cache_read_tokens']} |")
    lines.append(f"| Cache miss tokens         | {t['cache_miss_tokens']} |")
    lines.append(f"| Cache write tokens        | {t['cache_write_tokens']} |")
    hit_rate = ""
    denom = t["cache_read_tokens"] + t["cache_miss_tokens"]
    if denom > 0:
        hit_rate = f"{t['cache_read_tokens'] / denom * 100:.1f}%"
    lines.append(f"| Cache hit rate            | {hit_rate or '-'} |")
    lines.append("")

    if src == "log":
        lines.append("## By Provider / Model")
        lines.append("")
        lines.append("| Provider / Model | Calls | Input | Output | Hit | Miss |")
        lines.append("| ---------------- | ----- | ----- | ------ | --- | ---- |")
        for key, b in sorted(report["by_provider_model"].items()):
            lines.append(
                f"| {key} | {b['calls']} | {b['input_tokens']} | {b['output_tokens']} "
                f"| {b['cache_read_tokens']} | {b['cache_miss_tokens']} |"
            )
        lines.append("")

    if src == "traces" and report["by_run_id"]:
        lines.append("## By Run ID")
        lines.append("")
        lines.append(
            "Token usage attributed by `run_id` (the canonical identifier from each trace JSONL record). "
            "Sub-agent runs use the `<parent-run>:<subagent>` form."
        )
        lines.append("")
        lines.append("| Run ID | Calls | Input | Output | Hit | Miss |")
        lines.append("| ------ | ----- | ----- | ------ | --- | ---- |")
        for rid, b in sorted(report["by_run_id"].items()):
            lines.append(
                f"| {rid} | {b['calls']} | {b['input_tokens']} | {b['output_tokens']} "
                f"| {b['cache_read_tokens']} | {b['cache_miss_tokens']} |"
            )
        lines.append("")

    lines.append(f"## By Cron Task (run window: {report['cron_window_secs']}s)")
    lines.append("")
    lines.append("| Cron ID | Last Fired (UTC) | Schedule | Calls | Input | Output | Hit | Miss |")
    lines.append("| ------- | ---------------- | -------- | ----- | ----- | ------ | --- | ---- |")
    any_cron = False
    for entry in report["by_cron"]:
        b = entry["bucket"]
        if b["calls"] == 0:
            continue
        any_cron = True
        lines.append(
            f"| {entry['id']} | {entry['last_fired_iso']} | `{entry['schedule_expression']}` "
            f"| {b['calls']} | {b['input_tokens']} | {b['output_tokens']} "
            f"| {b['cache_read_tokens']} | {b['cache_miss_tokens']} |"
        )
    if not any_cron:
        lines.append("| _(no cron token activity in window)_ | - | - | 0 | 0 | 0 | 0 | 0 |")
    lines.append("")

    if src == "traces":
        lines.append("## By Channel")
        lines.append("")
        lines.append(
            "Token usage attributed by `run_id` prefix: "
            "`telegram-gateway*` → telegram, `feishu-gateway*` → feishu, "
            "`cli-run-*` → cli, `tui*` → tui."
        )
    else:
        lines.append(
            f"## By Channel (non-cron, marker window: {report['channel_window_secs']}s)"
        )
        lines.append("")
        lines.append(
            "Token usage attributed to a channel via the latest gateway-log line "
            "mentioning that channel (telegram / feishu / tui / cli) within the marker "
            "window before each token line. Cron-attributed lines are excluded here."
        )
    lines.append("")
    lines.append("| Channel | Calls | Input | Output | Hit | Miss |")
    lines.append("| ------- | ----- | ----- | ------ | --- | ---- |")
    if report["by_channel"]:
        for chan, b in sorted(report["by_channel"].items()):
            lines.append(
                f"| {chan} | {b['calls']} | {b['input_tokens']} | {b['output_tokens']} "
                f"| {b['cache_read_tokens']} | {b['cache_miss_tokens']} |"
            )
    else:
        lines.append("| _(no channel-attributed token activity)_ | 0 | 0 | 0 | 0 | 0 |")
    lines.append("")

    if src == "traces" and report["by_subagent"]:
        lines.append("## By Sub-Agent")
        lines.append("")
        lines.append(
            "Token usage attributed by sub-agent name (the segment after `:` in `run_id`). "
            "Top-level runs without a sub-agent are excluded here — see **By Channel** for those."
        )
        lines.append("")
        lines.append("| Sub-Agent | Calls | Input | Output | Hit | Miss |")
        lines.append("| --------- | ----- | ----- | ------ | --- | ---- |")
        for name, b in sorted(report["by_subagent"].items()):
            lines.append(
                f"| {name} | {b['calls']} | {b['input_tokens']} | {b['output_tokens']} "
                f"| {b['cache_read_tokens']} | {b['cache_miss_tokens']} |"
            )
        lines.append("")

    if src == "log":
        u = report["uncategorized_bucket"]
        lines.append("## Uncategorized")
        lines.append("")
        lines.append(
            "Token usage from log lines that matched neither a cron's run window "
            "nor any channel marker. Often background tasks or activity before the "
            "first channel marker in the audit window."
        )
        lines.append("")
        lines.append("| Calls | Input | Output | Hit | Miss |")
        lines.append("| ----- | ----- | ------ | --- | ---- |")
        lines.append(
            f"| {u['calls']} | {u['input_tokens']} | {u['output_tokens']} "
            f"| {u['cache_read_tokens']} | {u['cache_miss_tokens']} |"
        )
        lines.append("")

    return "\n".join(lines) + "\n"


def main():
    home = resolve_agentos_home()
    print(f"[audit_tokens] AGENTOS_HOME = {home}", file=sys.stderr)
    parser = argparse.ArgumentParser(description="LLM token audit from gateway log")
    parser.add_argument("--log", default=str(home / "logs" / "agentos-gateway.log"),
                        help="Path to gateway log file (default: $AGENTOS_HOME/logs/agentos-gateway.log)")
    parser.add_argument("--hours", type=float, default=24.0,
                        help="Audit window in hours (default 24)")
    parser.add_argument("--crons-dir", default=str(home / "workspace" / "crons"),
                        help="Directory of cron TOML files (default: $AGENTOS_HOME/workspace/crons)")
    parser.add_argument("--traces-dir", default=str(home / "workspace" / "traces"),
                        help="Directory of trace JSONL files (default: $AGENTOS_HOME/workspace/traces). "
                             "Trace JSONL is the canonical token source — exact per-call totals attributed "
                             "by run_id. Gateway log is only used as fallback when traces are unavailable.")
    parser.add_argument("--source", choices=["auto", "traces", "log"], default="auto",
                        help="Token data source: traces (canonical), log (gateway log fallback), or auto "
                             "(traces if non-empty, else log). Default: auto.")
    parser.add_argument("--cross-check", action="store_true",
                        help="If both trace JSONL and gateway-log sources have data, cross-check their "
                             "call counts and totals; print a stderr warning when they disagree by more "
                             "than --cross-check-tolerance. The trace and log emitters live at different "
                             "layers of the loop (`crates/agentos-core/src/loop/mod.rs` vs "
                             "`crates/agentos-llm/src/providers/mod.rs`), so a divergence here signals that "
                             "one layer is silently dropping events.")
    parser.add_argument("--cross-check-tolerance", type=float, default=0.05,
                        help="Relative-difference threshold (0.0–1.0) for --cross-check warnings on "
                             "call counts and total tokens. Default: 0.05 (5%%).")
    parser.add_argument("--cron-window-secs", type=int, default=1800,
                        help="Seconds after a cron fire that count as that cron's run (default 1800)")
    parser.add_argument("--channel-window-secs", type=int, default=300,
                        help="Seconds after a channel marker (telegram/feishu/tui/cli) that "
                             "count as that channel's activity for non-cron token lines (default 300)")
    parser.add_argument("--max-bytes", type=int, default=2_000_000,
                        help="Tail bytes to read from the log (default 2 MB)")
    parser.add_argument("--output", "-o", default=None, help="Write Markdown to file")
    parser.add_argument("--json", action="store_true", help="Emit raw JSON instead of Markdown")
    args = parser.parse_args()

    log_path = Path(args.log)
    crons_dir = Path(args.crons_dir)
    traces_dir = Path(args.traces_dir)
    now = time.time()
    cutoff = int(now - args.hours * 3600)
    crons = load_crons(crons_dir)

    report = {
        "generated_at": datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "window_start": datetime.fromtimestamp(cutoff, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "window_hours": args.hours,
        "source": None,  # filled in below: "traces" or "log"
        "log_path": str(log_path),
        "traces_path": str(traces_dir),
        "log_missing": False,
        "log_outside_window": False,
        "log_mtime": None,
        "cron_window_secs": args.cron_window_secs,
        "channel_window_secs": args.channel_window_secs,
        "usage_lines_found": 0,
        "trace_events_found": 0,
        "trace_files_in_window": 0,
        "user_facing_tasks": 0,
        "totals": empty_bucket(),
        "by_provider_model": {},
        "by_cron": [],
        "by_channel": {},
        "by_run_id": {},
        "by_subagent": {},
        "uncategorized_bucket": empty_bucket(),
    }

    # --- Primary path: trace JSONL ---
    if args.source in ("auto", "traces"):
        trace_result = aggregate_from_traces(traces_dir, crons, args.cron_window_secs, cutoff)
        report["trace_files_in_window"] = trace_result["files_in_window"]
        report["trace_events_found"] = trace_result["events_found"]
        report["user_facing_tasks"] = trace_result["run_started_count"]
        if trace_result["events_found"] > 0:
            report["source"] = "traces"
            report["totals"] = trace_result["totals"]
            report["by_run_id"] = trace_result["by_run_id"]
            report["by_channel"] = trace_result["by_channel"]
            report["by_subagent"] = trace_result["by_subagent"]
            # Convert by_cron dict into the list-of-entries shape the renderer uses.
            for c in crons:
                report["by_cron"].append({
                    "id": c["id"],
                    "last_fired_unix": c["last_fired_unix"],
                    "last_fired_iso": (
                        datetime.fromtimestamp(c["last_fired_unix"], tz=timezone.utc)
                        .strftime("%Y-%m-%d %H:%M UTC")
                        if c["last_fired_unix"] > 0 else "never"
                    ),
                    "schedule_expression": c["schedule_expression"],
                    "bucket": trace_result["by_cron"][c["id"]],
                })
    # --- Fallback path: gateway log ---
    # Only runs when trace source returned nothing AND user didn't pin --source=traces.
    use_log_path = (
        args.source == "log"
        or (args.source == "auto" and report["source"] is None)
    )
    if use_log_path and not log_path.exists():
        report["log_missing"] = True
    elif use_log_path:
        mtime = int(log_path.stat().st_mtime)
        report["log_mtime"] = datetime.fromtimestamp(mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if mtime < cutoff:
            report["log_outside_window"] = True
        else:
            report["source"] = "log"
            cron_buckets = {c["id"]: empty_bucket() for c in crons}
            text = tail_bytes(log_path, args.max_bytes)

            # First pass: collect channel-activity markers (cheap regex, no
            # token parse). Skip lines outside the audit window.
            channel_markers = []
            for line in text.splitlines():
                m = EPOCH_RE.match(line)
                if not m:
                    continue
                epoch = int(m.group(1))
                if epoch < cutoff:
                    continue
                cm = CHANNEL_MARKER_RE.match(m.group(2))
                if cm:
                    channel_markers.append((epoch, cm.group("chan")))
            channel_markers.sort(key=lambda p: p[0])

            # Second pass: aggregate token-usage lines.
            for line in text.splitlines():
                m = EPOCH_RE.match(line)
                if not m:
                    continue
                epoch = int(m.group(1))
                if epoch < cutoff:
                    continue
                rec = parse_usage_line(m.group(2))
                if rec is None:
                    continue
                report["usage_lines_found"] += 1
                fold(report["totals"], rec)
                pm_key = f"{rec['provider']} / {rec['model']}"
                report["by_provider_model"].setdefault(pm_key, empty_bucket())
                fold(report["by_provider_model"][pm_key], rec)

                cron_id = attribute_to_cron(epoch, crons, args.cron_window_secs)
                if cron_id is not None:
                    fold(cron_buckets[cron_id], rec)
                    continue
                channel = attribute_to_channel(epoch, channel_markers, args.channel_window_secs)
                if channel is not None:
                    report["by_channel"].setdefault(channel, empty_bucket())
                    fold(report["by_channel"][channel], rec)
                    continue
                fold(report["uncategorized_bucket"], rec)

            for c in crons:
                report["by_cron"].append({
                    "id": c["id"],
                    "last_fired_unix": c["last_fired_unix"],
                    "last_fired_iso": (
                        datetime.fromtimestamp(c["last_fired_unix"], tz=timezone.utc)
                        .strftime("%Y-%m-%d %H:%M UTC")
                        if c["last_fired_unix"] > 0 else "never"
                    ),
                    "schedule_expression": c["schedule_expression"],
                    "bucket": cron_buckets[c["id"]],
                })

    # --- Cross-check trace vs log when requested ---
    # Only meaningful when traces are the active source; the inverse direction
    # (--source=log) doesn't have an independent trace bucket to compare to.
    if args.cross_check and report["source"] == "traces":
        log_totals, status = quick_log_totals(log_path, cutoff, args.max_bytes)
        if status == "ok":
            emit_cross_check(report, log_totals, args.cross_check_tolerance)
        else:
            print(
                f"[audit_tokens] cross-check skipped: gateway log {status} "
                f"({log_path})",
                file=sys.stderr,
            )

    if args.json:
        out = json.dumps(report, indent=2)
    else:
        out = render_markdown(report)

    if args.output:
        Path(args.output).write_text(out, encoding="utf-8")
        print(f"wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(out)


if __name__ == "__main__":
    main()
