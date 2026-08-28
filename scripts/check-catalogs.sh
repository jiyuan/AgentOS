#!/usr/bin/env bash
# Fail when the generated catalogs no longer match the code (roadmap X4).
#
# `docs/CONFIG_CATALOG.md` and `docs/TOOL_CATALOG.md` are derived from the
# config structs and the built-in `ToolSpec`s. Adding a config key, changing a
# default, or editing a tool's description without regenerating leaves the docs
# describing a tree that no longer exists — which is the failure this roadmap
# opened by finding.
#
# Usage: scripts/check-catalogs.sh
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# `--check` renders and compares without writing, so this is safe to run on a
# clean tree and in CI.
if cargo run --locked --quiet -p agentos-cli --bin agentos-gateway -- catalog --check --root="$root"; then
  echo "catalogs ok"
  exit 0
fi

echo
echo "The generated catalogs are out of date."
echo "Run: cargo run --locked -p agentos-cli --bin agentos-gateway -- catalog"
echo "and commit docs/CONFIG_CATALOG.md and docs/TOOL_CATALOG.md."
exit 1
