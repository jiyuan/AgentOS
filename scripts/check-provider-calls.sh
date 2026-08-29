#!/usr/bin/env bash
# AST-aware lexical enforcement for the single provider-call gateway.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The checker's own regression (`AF-036`) runs here rather than under pytest,
# which no workflow installs and no script invoked — the named artifact existed,
# passed, and gated nothing. Running it before the scan also means a checker
# broken badly enough to miss every call fails this gate instead of reporting a
# clean tree.
python3 "$root/tests/test_provider_call_checker.py"
python3 "$root/scripts/check_provider_calls.py" "$root"
