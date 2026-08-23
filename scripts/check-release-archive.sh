#!/usr/bin/env bash
# Clean-room check that a release archive is self-contained.
#
# M2 (`REL-001`/`REL-002`). The failure this exists to catch was end to end and
# untested by anything: the archive shipped a workspace holding only
# agent.toml, the installer then copied that empty workspace over the install,
# and startup died in WorkspaceSkillCatalog::load_enabled on a skill that was
# never packaged. Meanwhile the documented install path disagreed with the
# installer's own default, so following the README gave "command not found".
#
# Everything below runs against the extracted archive in a temporary prefix
# with no access to the repository, which is the only way to tell "it works"
# apart from "it works because the checkout is right there".
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
keep=0
archive=""

usage() {
  cat <<'USAGE'
Usage: scripts/check-release-archive.sh [OPTIONS]

Verify that a packaged release archive installs and runs with no repository.

Options:
  --archive PATH   Check this archive. Default: build one with package-release.sh.
  --keep           Leave the scratch directory in place for inspection.
  -h, --help       Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --archive) archive="$2"; shift 2 ;;
    --keep) keep=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

scratch="$(mktemp -d)"
cleanup() {
  if [[ "$keep" == "1" ]]; then
    echo "scratch kept at $scratch"
  else
    rm -rf "$scratch"
  fi
}
trap cleanup EXIT

failures=0
check() {
  local what="$1"
  shift
  if "$@"; then
    echo "  ok    $what"
  else
    echo "  FAIL  $what" >&2
    failures=$((failures + 1))
  fi
}

if [[ -z "$archive" ]]; then
  echo "== building the release archive"
  DIST_DIR="$scratch/dist" "$root/scripts/package-release.sh" >"$scratch/package.log" 2>&1 || {
    cat "$scratch/package.log" >&2
    echo "packaging failed" >&2
    exit 1
  }
  archive="$(find "$scratch/dist" -maxdepth 1 -name '*.tar.gz' | head -1)"
fi
[[ -f "$archive" ]] || { echo "no archive at $archive" >&2; exit 1; }
echo "== archive: $archive"

echo "== extracting"
extract_dir="$scratch/extract"
mkdir -p "$extract_dir"
tar -C "$extract_dir" -xzf "$archive"
bundle="$(find "$extract_dir" -maxdepth 1 -mindepth 1 -type d | head -1)"
echo "   $bundle"

echo "== archive contents"
# The three failures the audit found, asserted directly on the staged tree.
check "workspace/skills is packaged and non-empty" \
  bash -c '[[ -d "$1/workspace/skills" ]] && [[ -n "$(ls -A "$1/workspace/skills")" ]]' _ "$bundle"
check "workspace/subagents is packaged" test -d "$bundle/workspace/subagents"
check "workspace/suborchs is packaged" test -d "$bundle/workspace/suborchs"

# Every skill the shipped config enables must be in the bundle, because
# load_enabled is a hard error on a missing one.
enabled_skills="$(sed -n '/^\[resources\.skills\]/,/^\[/p' "$bundle/workspace/agent.toml" \
  | sed -n 's/^enabled *= *\[\(.*\)\]/\1/p' | tr -d '" ' | tr ',' '\n' | grep -v '^$' || true)"
[[ -n "$enabled_skills" ]] || { echo "  FAIL  could not read [resources.skills] enabled" >&2; failures=$((failures + 1)); }
while IFS= read -r skill; do
  [[ -z "$skill" ]] && continue
  check "enabled skill '$skill' is packaged" test -f "$bundle/workspace/skills/$skill/SKILL.md"
done <<<"$enabled_skills"

echo "== runtime state is excluded"
for unwanted in workspace/agentos.sqlite workspace/traces workspace/runs workspace/tasks workspace/attachments workspace/main workspace/min; do
  check "no $unwanted" bash -c '[[ ! -e "$1/$2" ]]' _ "$bundle" "$unwanted"
done
# Nothing anywhere in the bundle should be a database or a session log.
check "no *.sqlite anywhere" bash -c '[[ -z "$(find "$1" -name "*.sqlite*" -print -quit)" ]]' _ "$bundle"
check "no *.jsonl anywhere" bash -c '[[ -z "$(find "$1" -name "*.jsonl" -print -quit)" ]]' _ "$bundle"

echo "== documentation links resolve"
# Every relative Markdown link in every packaged document must point at a file
# that is also in the bundle. The README linked four documents that were not.
missing_links=0
while IFS= read -r doc; do
  doc_dir="$(dirname "$doc")"
  while IFS= read -r link; do
    [[ -z "$link" ]] && continue
    [[ "$link" == *://* ]] && continue
    link="${link%%#*}"
    [[ -z "$link" ]] && continue
    [[ "$link" == *.md ]] || continue
    if [[ ! -f "$doc_dir/$link" ]]; then
      echo "  FAIL  ${doc#"$bundle"/} links $link, which is not in the bundle" >&2
      missing_links=$((missing_links + 1))
    fi
  done < <(grep -oE '\]\([^)]+\)' "$doc" | sed -E 's/^\]\((.*)\)$/\1/')
done < <(find "$bundle" -name '*.md')
if [[ "$missing_links" == "0" ]]; then
  echo "  ok    every packaged Markdown link resolves"
else
  failures=$((failures + missing_links))
fi

echo "== installing into a clean prefix"
install_prefix="$scratch/prefix"
home_dir="$scratch/fakehome"
mkdir -p "$home_dir"
# HOME is redirected so a default-path install cannot touch the real one, and
# so nothing can quietly read the developer's ~/.agentos.
HOME="$home_dir" "$bundle/scripts/install-agentos.sh" --prefix "$install_prefix" --skip-build \
  >"$scratch/install.log" 2>&1 || {
  cat "$scratch/install.log" >&2
  echo "install failed" >&2
  exit 1
}
agentos="$install_prefix/bin/agentos"
check "the wrapper is installed and executable" test -x "$agentos"
check "the installed workspace has skills" bash -c '[[ -n "$(ls -A "$1/share/agentos/workspace/skills")" ]]' _ "$install_prefix"
check "the install carries no database" bash -c '[[ -z "$(find "$1" -name "*.sqlite*" -print -quit)" ]]' _ "$install_prefix"

echo "== the installed command works with no repository"
# Run from `/` so nothing can resolve a path relative to the checkout, with a
# redirected HOME so nothing reads the developer's real install.
run_installed() {
  ( cd / && env HOME="$home_dir" AGENTOS_LLM_PROVIDER=builtin.echo "$@" )
}

run_installed "$agentos" config >"$scratch/config.log" 2>&1 && config_ok=0 || config_ok=$?
check "agentos config reports the effective configuration" test "$config_ok" -eq 0
check "agentos config names the installed workspace" grep -q "share/agentos" "$scratch/config.log"

run_installed "$agentos" gateway-status >"$scratch/status.log" 2>&1 && status_code=0 || status_code=$?
check "agentos gateway-status exits deterministically" test "$status_code" -le 1

# The pathless form must reach the CLI's typed error naming the default path,
# not die in the wrapper on an unbound variable.
run_installed "$agentos" resume >"$scratch/resume.log" 2>&1 || true
check "pathless resume does not fail on an unbound variable" \
  bash -c '! grep -q "unbound variable" "$1"' _ "$scratch/resume.log"
check "pathless resume names the default state path" \
  grep -q "cli-run-1.json" "$scratch/resume.log"

run_installed "$agentos" definitely-not-a-command >/dev/null 2>&1 && unknown_code=0 || unknown_code=$?
check "an unknown command is rejected with exit 2" test "$unknown_code" -eq 2

echo "== one offline builtin.echo turn"
# The definition of done in §5 of the remediation plan: a release archive
# installed into a temporary prefix runs a turn using only files it contains.
if printf 'hello from the clean room\n' | run_installed "$agentos" tui >"$scratch/turn.log" 2>&1; then
  check "the turn produced output" bash -c '[[ -s "$1/turn.log" ]]' _ "$scratch"
  check "the turn echoed the input" grep -q "hello from the clean room" "$scratch/turn.log"
else
  echo "  FAIL  the offline turn did not complete" >&2
  sed -n '1,40p' "$scratch/turn.log" >&2
  failures=$((failures + 1))
fi

echo "== the packaged gateway starts and stops"
# The other half of "installed and working": a turn proves the CLI runs, and
# this proves the *service* does — that the wrapper's paths exist, that the
# gateway can take its control-file lock under the install prefix, and that
# `stop` finds and ends it (M9 / `CI-002`). No channels are enabled in the
# shipped config by default, so this needs no network and no credentials.
if run_installed "$agentos" gateway-start >"$scratch/gw-start.log" 2>&1; then
  check "the packaged gateway reports running" bash -c \
    'cd / && HOME="$1" AGENTOS_LLM_PROVIDER=builtin.echo "$2" gateway-status | grep -q "status: running"' \
    _ "$home_dir" "$agentos"
  run_installed "$agentos" gateway-stop >"$scratch/gw-stop.log" 2>&1 || true
  check "the packaged gateway reports stopped after gateway-stop" bash -c \
    'cd / && HOME="$1" AGENTOS_LLM_PROVIDER=builtin.echo "$2" gateway-status | grep -q "status: stopped"' \
    _ "$home_dir" "$agentos"
else
  echo "  FAIL  the packaged gateway did not start" >&2
  sed -n '1,40p' "$scratch/gw-start.log" >&2
  failures=$((failures + 1))
fi

echo "== installing from the source checkout"
# The other half of the same contract, and the one that used to copy the
# developer's runtime state: `cp -r "$root/workspace"` pulled agentos.sqlite,
# traces/, runs/ and the task directories into every source install.
if [[ -x "$root/target/release/agentos-cli" ]]; then
  source_prefix="$scratch/source-prefix"
  HOME="$home_dir" "$root/scripts/install-agentos.sh" \
    --from-source --skip-build --prefix "$source_prefix" \
    >"$scratch/source-install.log" 2>&1 || {
    cat "$scratch/source-install.log" >&2
    echo "source install failed" >&2
    exit 1
  }
  check "the source install carries no database" \
    bash -c '[[ -z "$(find "$1" -name "*.sqlite*" -print -quit)" ]]' _ "$source_prefix"
  check "the source install carries no traces" \
    bash -c '[[ ! -e "$1/share/agentos/workspace/traces" ]]' _ "$source_prefix"
  check "the source install carries no session logs" \
    bash -c '[[ -z "$(find "$1" -name "*.jsonl" -print -quit)" ]]' _ "$source_prefix"
  check "the source install has skills" \
    bash -c '[[ -n "$(ls -A "$1/share/agentos/workspace/skills")" ]]' _ "$source_prefix"
  if printf 'hello from source\n' | ( cd / && env HOME="$home_dir" AGENTOS_LLM_PROVIDER=builtin.echo \
      "$source_prefix/bin/agentos" tui ) >"$scratch/source-turn.log" 2>&1; then
    check "the source install completes a turn" grep -q "hello from source" "$scratch/source-turn.log"
  else
    echo "  FAIL  the source install's offline turn did not complete" >&2
    sed -n '1,40p' "$scratch/source-turn.log" >&2
    failures=$((failures + 1))
  fi
else
  echo "  skip  no target/release binaries; run a release build to cover the source install"
fi

echo
if [[ "$failures" == "0" ]]; then
  echo "release archive is self-contained"
else
  echo "$failures check(s) failed" >&2
  exit 1
fi
