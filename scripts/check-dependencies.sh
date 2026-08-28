#!/usr/bin/env bash
# Dependency advisory, licence, duplicate and source policy (M9 / `CI-002`).
#
# Two things happen here. `cargo deny check` applies `deny.toml`; then this
# script applies the one rule `cargo-deny` has no notion of — that every
# suppression in that file carries an expiry, and an expired one fails.
#
# Without the second half the first half decays into a list. A suppression is
# added on the day something is urgent and is never revisited, because nothing
# ever asks again; six months later the policy file describes a graph that no
# longer exists and the check it replaced is not running. The same two-way
# ratchet as `config/undocumented.txt`, applied to dependencies.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
policy="$root/deny.toml"
# shellcheck source=release-versions.env
source "$root/scripts/release-versions.env"

usage() {
  cat <<'USAGE'
Usage: scripts/check-dependencies.sh [OPTIONS]

Run the dependency policy in deny.toml and verify no exception has expired.

Options:
  --expiry-only   Check the exception expiries without running cargo-deny.
                  Useful when the crates.io advisory database is unreachable.
  -h, --help      Show this help.

Requires the exact cargo-deny version pinned in scripts/release-versions.env.
USAGE
}

expiry_only=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --expiry-only) expiry_only=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -f "$policy" ]] || { echo "missing $policy" >&2; exit 1; }

failures=0

# Every suppression must be annotated. A suppression is a line inside `skip`,
# `ignore`, `deny`, `allow-git`, or `allow-registry` beyond crates.io — in
# practice, any `{ crate = ... }` or bare advisory id. The annotation is the
# comment line immediately above it, and it must start with `# expires:` or
# `# permanent:`.
echo "== exception annotations"
python3 - "$policy" <<'PY'
import datetime, re, sys

path = sys.argv[1]
lines = open(path, encoding="utf-8").read().splitlines()
today = datetime.date.today()
problems = []
checked = 0

# A suppression entry: a table line naming a crate, or a quoted advisory id.
entry = re.compile(r'^\s*(\{\s*crate\s*=|"RUSTSEC-)')
expires = re.compile(r'^\s*#\s*expires:\s*(\d{4}-\d{2}-\d{2})\b')
permanent = re.compile(r'^\s*#\s*permanent:\s*\S')

for index, line in enumerate(lines):
    if not entry.match(line):
        continue
    checked += 1
    # Walk back over continuation comments to the annotation.
    annotation = None
    cursor = index - 1
    while cursor >= 0 and lines[cursor].lstrip().startswith("#"):
        if expires.match(lines[cursor]) or permanent.match(lines[cursor]):
            annotation = lines[cursor]
            break
        cursor -= 1
    if annotation is None:
        problems.append(
            f"{path}:{index + 1}: suppression has no '# expires: YYYY-MM-DD' or "
            f"'# permanent: <reason>' annotation above it\n    {line.strip()}"
        )
        continue
    match = expires.match(annotation)
    if match:
        when = datetime.date.fromisoformat(match.group(1))
        if when < today:
            problems.append(
                f"{path}:{cursor + 1}: exception expired on {when} "
                f"({(today - when).days} days ago); re-check whether it is still "
                f"needed and either remove it or set a new date\n    {line.strip()}"
            )

for problem in problems:
    print(f"  FAIL  {problem}", file=sys.stderr)
if problems:
    sys.exit(1)
print(f"  ok    {checked} suppression(s), all annotated and unexpired")
PY
annotations=$?
[[ "$annotations" -eq 0 ]] || failures=$((failures + 1))

if [[ "$expiry_only" == "1" ]]; then
  [[ "$failures" -eq 0 ]] || exit 1
  echo "dependency exceptions ok"
  exit 0
fi

cargo_deny_bin="$(command -v cargo-deny || true)"
if [[ -z "$cargo_deny_bin" && -x "${CARGO_HOME:-$HOME/.cargo}/bin/cargo-deny" ]]; then
  cargo_deny_bin="${CARGO_HOME:-$HOME/.cargo}/bin/cargo-deny"
fi
if [[ -z "$cargo_deny_bin" ]]; then
  echo "cargo-deny is not installed: scripts/install-toolchain.sh" >&2
  exit 1
fi
if [[ "$($cargo_deny_bin --version)" != "cargo-deny $AGENTOS_CARGO_DENY_VERSION" ]]; then
  echo "cargo-deny version does not match scripts/release-versions.env" >&2
  exit 1
fi

echo "== cargo deny check"
# `--locked` so the audit describes the committed lockfile rather than whatever
# resolution today's index happens to produce.
if ( cd "$root" && "$cargo_deny_bin" --locked check ); then
  echo "  ok    advisories, licences, bans, sources"
else
  failures=$((failures + 1))
fi

if [[ "$failures" -gt 0 ]]; then
  echo "dependency policy FAILED" >&2
  exit 1
fi
echo "dependency policy ok"
