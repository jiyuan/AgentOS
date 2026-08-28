#!/usr/bin/env bash
# AST-aware lexical enforcement for the single provider-call gateway.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 "$root/scripts/check_provider_calls.py" "$root"
