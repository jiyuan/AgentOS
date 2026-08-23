#!/usr/bin/env bash
# Upgrade, restart, migration and rollback, rehearsed against a real database
# (M9 / `CI-002`, deliverable 3).
#
# `check-release-archive.sh` proves a fresh install works. Nothing proved the
# other three transitions, which are the ones an existing deployment actually
# performs:
#
#   * restart — the gateway comes back on state it wrote itself,
#   * upgrade — today's binary opens a database written before it,
#   * rollback — yesterday's data goes back and today's binary still runs.
#
# The last is the one worth having. `agentos-gateway migrate` writes a backup
# and says so, but "there is a backup" and "restoring the backup leaves a
# working deployment" are different claims, and only the second is what an
# operator needs at 3am. A migration is also `schema_version`-gated, so a
# restored database is *older* than the binary reading it — exactly the
# situation nothing had exercised.
#
# Everything runs against a temporary AGENTOS_HOME with no channels enabled,
# so it needs no network, no provider, and no credentials.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gateway="$root/target/debug/agentos-gateway"
keep=0

usage() {
  cat <<'USAGE'
Usage: scripts/rehearse-upgrade.sh [OPTIONS]

Rehearse restart, upgrade, legacy-data migration, and rollback.

Options:
  --gateway PATH  agentos-gateway binary. Default: target/debug/agentos-gateway.
  --keep          Leave the scratch directory in place for inspection.
  -h, --help      Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gateway) gateway="$2"; shift 2 ;;
    --keep) keep=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ! -x "$gateway" ]]; then
  echo "no gateway binary at $gateway (cargo build -p agentos-cli --bin agentos-gateway)" >&2
  exit 1
fi

scratch="$(mktemp -d)"
cleanup() {
  # Never leave a gateway running behind a failed rehearsal.
  run stop >/dev/null 2>&1 || true
  if [[ "$keep" == "1" ]]; then
    echo "scratch kept at $scratch"
  else
    rm -rf "$scratch"
  fi
}

failures=0
check() {
  local what="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "  ok    $what"
  else
    echo "  FAIL  $what" >&2
    failures=$((failures + 1))
  fi
}

home="$scratch/home"
mkdir -p "$home/workspace"
# No channels: the gateway reaches its idle loop with no provider and no
# network, which is the narrowest configuration in which the lifecycle is real.
cat >"$home/workspace/agent.toml" <<'TOML'
[agent]
id = "rehearsal"

[gateway]
shutdown_grace_secs = 5
TOML
# An env file of its own, so a .env in the checkout cannot redirect AGENTOS_HOME
# at the developer's real install — which is a live database.
printf 'AGENTOS_HOME=%s\n' "$home" >"$home/.env"

db="$home/workspace/agentos.sqlite"
log="$home/logs/agentos-gateway.log"
pid="$home/workspace/run/agentos-gateway.pid"

run() {
  "$gateway" "$@" --env-file "$home/.env" --pid-path "$pid" --log-path "$log" \
    --config "$home/workspace/agent.toml" --session-db-path "$db"
}
trap cleanup EXIT

# `start` returns once the control file names the serving process, so there is
# nothing to poll for. Reported anyway if it ever stops being true.
started() {
  run status 2>/dev/null | grep -q "status: running"
}
stopped() {
  run status 2>/dev/null | grep -q "status: stopped"
}

echo "== source startup"
check "the gateway starts" run start
check "status reports running" started
check "the log was created" test -s "$log"
check "the control file exists" test -f "$pid"
check "the gateway stops" run stop
check "status reports stopped" stopped

echo "== restart on its own state"
check "it starts again on the state it wrote" run start
check "status reports running" started
check "restart is a single command" run restart
check "status still reports running after restart" started
check "the gateway stops" run stop
check "status reports stopped" stopped

echo "== a database written before this binary"
if [[ ! -f "$db" ]]; then
  # The idle gateway has no reason to open the session store, so create it the
  # way any real deployment would — by running the migrate command, which
  # refuses on a missing file and tells us so.
  run purge --conversation rehearsal-seed --yes rehearsal-seed >/dev/null 2>&1 || true
fi
check "a session database exists to migrate" test -f "$db"

# A record under the pre-`ID-001` namespace shape: five segments with a bare
# conversation id where a principal now goes. Written with python's sqlite3
# rather than by hand-rolling the schema, so the table is the real one the
# runtime created and only the row is synthetic.
python3 - "$db" <<'PY'
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute(
    "INSERT INTO memory_records (id, namespace, body_json, metadata_json) VALUES (?, ?, ?, ?)",
    ("legacy-1", "private/conversation/42/semantic/general", '{"text":"remember"}', "{}"),
)
conn.commit()
conn.close()
PY
legacy_count() {
  python3 - "$db" <<'PY'
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
print(conn.execute(
    "SELECT COUNT(*) FROM memory_records WHERE namespace = ?",
    ("private/conversation/42/semantic/general",),
).fetchone()[0])
PY
}
check "the legacy row is there before migrating" test "$(legacy_count)" = "1"

echo "== migration reports before it changes anything"
run migrate --channel telegram >"$scratch/plan.log" 2>&1 || true
check "the plan names the legacy namespace" grep -q "private/conversation/42" "$scratch/plan.log"
check "the plan says nothing was changed" grep -q "Nothing was changed" "$scratch/plan.log"
check "the legacy row survives a plan-only run" test "$(legacy_count)" = "1"

echo "== migration applies, with a backup"
backup="$scratch/before-migration.sqlite"
run migrate --channel telegram --apply "--backup=$backup" >"$scratch/apply.log" 2>&1 || {
  cat "$scratch/apply.log" >&2
  echo "migrate --apply failed" >&2
  exit 1
}
check "the backup was written" test -s "$backup"
check "the legacy namespace is gone" test "$(legacy_count)" = "0"
check "the record is now principal-keyed" bash -c \
  'python3 - "$1" <<PY
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
row = conn.execute("SELECT namespace FROM memory_records WHERE id = ?", ("legacy-1",)).fetchone()
sys.exit(0 if row and row[0].startswith("private/conversation/v1.") else 1)
PY' _ "$db"

echo "== rollback"
# What an operator does when a migration turns out to have been wrong: put the
# backup back and carry on. The claim under test is not that `cp` works, it is
# that the current binary runs on the restored, older data.
cp "$backup" "$db"
rm -f "$db-wal" "$db-shm"
check "the legacy row is back" test "$(legacy_count)" = "1"
check "the current binary starts on rolled-back data" run start
check "status reports running" started
check "and stops cleanly" run stop
check "status reports stopped" stopped
check "the migration can be replanned after a rollback" bash -c \
  '"$1" migrate --channel telegram --env-file "$2" --pid-path "$3" --log-path "$4" \
     --config "$5" --session-db-path "$6" | grep -q "private/conversation/42"' \
  _ "$gateway" "$home/.env" "$pid" "$log" "$home/workspace/agent.toml" "$db"

if [[ "$failures" -gt 0 ]]; then
  echo >&2
  echo "$failures rehearsal check(s) failed" >&2
  echo "gateway log:" >&2
  sed -n '1,40p' "$log" >&2 || true
  exit 1
fi

echo
echo "upgrade, restart, migration and rollback rehearsed"
