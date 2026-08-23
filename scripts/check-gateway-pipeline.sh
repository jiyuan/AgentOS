#!/usr/bin/env bash
# Drive the gateway end to end against mock Telegram and Feishu transports
# (M9 / `CI-002`, deliverable 3).
#
# `tests/gateway_pipeline.py` has been in the tree since before the audit and
# was, in the plan's words, "invoked by nothing". Wiring it up is most of the
# value: on its first run under this script it found two defects that every
# other test missed, because both need a whole gateway and a transport that
# behaves like a transport.
#
#   * The pipeline still asked for an open channel by *blanking*
#     `AGENTOS_TELEGRAM_CHAT_ID`, which M3 / `AUTH-001` had reversed to mean
#     "admit nothing". Every task timed out against a channel loop that had
#     already failed to start.
#   * With that fixed, a two-channel gateway on a database that does not exist
#     yet lost one of its channels to "database is locked" — `PRAGMA
#     journal_mode = WAL` is not covered by `busy_timeout`. Fixed in
#     `memory/pool.rs` and pinned by `tests/persistence_concurrency.rs`.
#
# Neither is reachable from a unit test. That is the argument for the whole
# job: the pipeline is the only thing here that starts the real binary, speaks
# a real transport protocol at it, and reads what comes back.
#
# Hermetic. Mock HTTP and WebSocket servers on loopback, `builtin.echo` as the
# provider, and a session database of the cycle's own — no network, no
# credentials, no touching the developer's workspace.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

gateway="target/debug/agentos-gateway"
tasks="tests/ci_tasks.json"
timeout_secs=45

usage() {
  cat <<'USAGE'
Usage: scripts/check-gateway-pipeline.sh [OPTIONS]

Run one pipeline cycle against mock channels and fail on any task issue.

Options:
  --gateway PATH   agentos-gateway binary. Default: target/debug/agentos-gateway.
  --tasks PATH     Task file. Default: tests/ci_tasks.json (the full set is
                   tests/test_tasks.json).
  --timeout N      Seconds to wait for each reply. Default: 45.
  -h, --help       Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gateway) gateway="$2"; shift 2 ;;
    --tasks) tasks="$2"; shift 2 ;;
    --timeout) timeout_secs="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -x "$gateway" ]] || {
  echo "no gateway binary at $gateway (cargo build -p agentos-cli --bin agentos-gateway)" >&2
  exit 1
}
[[ -f "$tasks" ]] || { echo "no task file at $tasks" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }

python3 tests/gateway_pipeline.py --once \
  --gateway-bin "$gateway" \
  --tasks "$tasks" \
  --per-task-timeout "$timeout_secs"

# The pipeline exits 0 whether or not the tasks passed — it is a reporter, and
# `--cron` has to survive a bad cycle. The gate is the report.
python3 - <<'PY'
import json, pathlib, sys

path = pathlib.Path("tests/issues.json")
if not path.is_file():
    print("  FAIL  the pipeline wrote no issues.json", file=sys.stderr)
    sys.exit(1)
report = json.loads(path.read_text(encoding="utf-8"))
tasks = report.get("tasks", [])
if not tasks:
    print("  FAIL  the pipeline ran no tasks", file=sys.stderr)
    sys.exit(1)

bad = [task for task in tasks if task.get("timed_out") or task.get("errors")]
for task in bad:
    reason = "; ".join(task.get("errors") or ["timed out"])
    print(f"  FAIL  [{task['channel']}] {task['spec_id']}: {reason}", file=sys.stderr)
if bad:
    print(f"\n{len(bad)} of {len(tasks)} task(s) failed; see tests/issues.json", file=sys.stderr)
    sys.exit(1)
channels = sorted({task["channel"] for task in tasks})
print(f"  ok    {len(tasks)} task(s) answered across {', '.join(channels)}")
PY

echo "gateway pipeline ok"
