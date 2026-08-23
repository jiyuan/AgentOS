#!/usr/bin/env bash
# The loop-overhead budget in BENCHMARKS.md is a number the build can check
# (M9 / `CI-002`, deliverable 6).
#
# `BENCHMARKS.md` has promised "≤ 2 ms per turn excluding LLM and tool latency"
# since the beginning, and nothing ever compared a measurement to it. The
# recorded per-bench figures are not the gate and must not be: they are
# microseconds on one developer's machine, and a shared CI runner is noisy
# enough that gating on them would fail on scheduling rather than on code. The
# *budget* is different — it is three orders of magnitude above the current
# figures, so crossing it means something structural happened, not that the
# runner was busy.
#
# Run nightly rather than per-push, because a release-profile bench build is
# minutes and the signal does not change between two commits.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

run_bench=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-run) run_bench=0; shift ;;
    -h|--help)
      cat <<'USAGE'
Usage: scripts/check-loop-budget.sh [--no-run]

Run the loop-overhead benches and check every mean against the budget stated in
BENCHMARKS.md.

Options:
  --no-run   Score the results already in target/criterion instead of
             re-running the benches.
USAGE
      exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ "$run_bench" == "1" ]]; then
  cargo bench --locked -p agentos-core --bench loop_overhead
fi

python3 - <<'PY'
import json, pathlib, re, sys

# The budget is read from the document that states it, so the two cannot
# disagree — and a rewording that loses the number fails here rather than
# quietly disabling the gate.
text = pathlib.Path("BENCHMARKS.md").read_text(encoding="utf-8")
match = re.search(r"Target loop overhead:\s*[≤<=]+\s*([0-9.]+)\s*ms", text)
if not match:
    print(
        "  FAIL  BENCHMARKS.md no longer states a 'Target loop overhead: ≤ N ms' budget",
        file=sys.stderr,
    )
    sys.exit(1)
budget_ns = float(match.group(1)) * 1_000_000

results = sorted(pathlib.Path("target/criterion").glob("loop_overhead_*/new/estimates.json"))
if not results:
    print("  FAIL  no loop_overhead results under target/criterion", file=sys.stderr)
    sys.exit(1)

over = []
for path in results:
    name = path.parents[1].name
    mean = json.loads(path.read_text(encoding="utf-8"))["mean"]["point_estimate"]
    share = mean / budget_ns
    verdict = "ok  " if mean <= budget_ns else "OVER"
    print(f"  {verdict}  {name}: {mean / 1000:.2f} µs ({share:.1%} of the budget)")
    if mean > budget_ns:
        over.append(name)

if over:
    print(
        f"\n{len(over)} bench(es) over the {budget_ns / 1_000_000:.0f} ms budget: "
        f"{', '.join(over)}",
        file=sys.stderr,
    )
    sys.exit(1)
print(f"\nevery loop bench is inside the {budget_ns / 1_000_000:.0f} ms budget")
PY
