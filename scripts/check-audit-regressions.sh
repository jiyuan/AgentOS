#!/usr/bin/env bash
# Ensure audit findings cannot be closed or removed through documentation alone.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

python3 scripts/check_audit_findings.py --self-test
python3 scripts/check_audit_findings.py --root "$root"
