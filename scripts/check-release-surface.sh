#!/usr/bin/env bash
# The artifact, the catalogs, the capability matrix and the release notes all
# describe one feature surface (M9 / `CI-002`).
#
# Each of those four documents is already ratcheted against the *code*:
# `check-catalogs.sh` regenerates the catalogs from the config structs,
# `tests/capability_matrix.rs` fails when a tool or provider has no row, and
# `check-release-archive.sh` validates every link in what is packaged. What
# nothing checked is whether they agree with *each other* — and they did not.
# Its first run found the capability matrix requiring `[skills]` and `[hooks]`,
# neither of which is a config section: skills are `[resources.skills]`, and
# hooks have no configuration at all because every entrypoint passes `None`.
#
# Three questions, all answerable from the tree:
#
#   1. Does the release note the tree ships name every slice the plan calls
#      done? A shipped change nobody can read about is a change the operator
#      upgrading into it will meet by surprise.
#   2. Does every config key the capability matrix requires exist in the
#      generated config catalog?
#   3. Does the packaged documentation closure reach the catalogs, the matrix,
#      and the release notes at all?
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

plan="docs/AUDIT_REMEDIATION_PLAN.md"
notes="docs/RELEASE_NOTES.md"
matrix="docs/CAPABILITY_MATRIX.md"
catalog="docs/CONFIG_CATALOG.md"

for file in "$plan" "$notes" "$matrix" "$catalog"; do
  [[ -f "$file" ]] || { echo "missing $file" >&2; exit 1; }
done

failures=0

echo "== every completed slice is in the release notes"
python3 - "$plan" "$notes" <<'PY' || failures=$((failures + 1))
import re, sys

plan, notes = (open(path, encoding="utf-8").read() for path in sys.argv[1:3])
# The slice table: | `ID` | milestone | scope | depends | status |
rows = re.findall(r"^\|\s*`([A-Z]+-\d+)`\s*\|.*\|\s*([^|]*)\|\s*$", plan, re.M)
done = [slice_id for slice_id, status in rows if "done" in status.lower()]
if not done:
    print("  FAIL  the plan's slice table parsed to nothing; the format changed", file=sys.stderr)
    sys.exit(1)
missing = [slice_id for slice_id in done if slice_id not in notes]
for slice_id in missing:
    print(f"  FAIL  {slice_id} is done in the plan and unmentioned in the release notes", file=sys.stderr)
if missing:
    sys.exit(1)
print(f"  ok    {len(done)} completed slice(s) all named")
PY

echo "== every config key the matrix requires is in the catalog"
python3 - "$matrix" "$catalog" <<'PY' || failures=$((failures + 1))
import re, sys

matrix, catalog = (open(path, encoding="utf-8").read() for path in sys.argv[1:3])
known = set(re.findall(r"^\|\s*`([A-Za-z_][A-Za-z0-9_.]*)`", catalog, re.M))
if not known:
    print("  FAIL  the config catalog parsed to nothing; the format changed", file=sys.stderr)
    sys.exit(1)

# Only tokens shaped like a config reference. The column also carries env var
# names, binaries, and prose, none of which the catalog describes.
reference = re.compile(r"^\[\[?([a-z0-9_.]+)\]?\]\s*\.?\s*([a-z0-9_.]+)?$")
problems = []
checked = 0
for row in matrix.splitlines():
    if not row.startswith("|"):
        continue
    cells = [cell.strip() for cell in row.strip().strip("|").split("|")]
    if len(cells) < 5:
        continue
    for token in re.findall(r"`([^`]+)`", cells[3]):
        if not token.startswith("["):
            continue
        match = reference.match(token.split("=")[0].strip())
        if not match:
            problems.append(f"  FAIL  {cells[0]!r}: cannot parse required config {token!r}")
            continue
        section, key = match.group(1), match.group(2)
        wanted = f"{section}.{key}" if key else section
        checked += 1
        if wanted not in known:
            problems.append(
                f"  FAIL  {cells[0]!r} requires `{token}`, and `{wanted}` is not a config key"
            )

for problem in problems:
    print(problem, file=sys.stderr)
if problems:
    sys.exit(1)
print(f"  ok    {checked} required-config reference(s) all resolve")
PY

echo "== the packaged documentation reaches the catalogs, matrix, and notes"
python3 - <<'PY' || failures=$((failures + 1))
import os, re, sys
from collections import deque

# The same transitive walk package-release.sh performs, so this answers "is it
# in the archive" rather than "is it in the repository".
queue = deque(["README.md", "docs/INSTALL.md", "docs/USER_GUIDE.md", "docs/RELEASE_NOTES.md"])
seen = set()
while queue:
    path = queue.popleft()
    if path in seen or not os.path.isfile(path):
        continue
    seen.add(path)
    base = os.path.dirname(path)
    for link in re.findall(r"\]\(([^)#]+\.md)(?:#[^)]*)?\)", open(path, encoding="utf-8").read()):
        if link.startswith(("http://", "https://")):
            continue
        queue.append(os.path.normpath(os.path.join(base, link)))

required = [
    "docs/CONFIG_CATALOG.md",
    "docs/TOOL_CATALOG.md",
    "docs/CAPABILITY_MATRIX.md",
    "docs/RELEASE_NOTES.md",
]
missing = [path for path in required if path not in seen]
for path in missing:
    print(f"  FAIL  {path} is not reachable from the packaged documentation roots", file=sys.stderr)
if missing:
    sys.exit(1)
print(f"  ok    {len(seen)} packaged document(s) include all four")
PY

if [[ "$failures" -gt 0 ]]; then
  echo "release surface FAILED" >&2
  exit 1
fi
echo "release surface ok"
