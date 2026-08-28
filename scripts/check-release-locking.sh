#!/usr/bin/env bash
# AF-037: exact toolchain/tool versions, a committed lock digest, locked
# artifact commands, and immutable action pins are one executable release gate.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 "$root/scripts/check_release_locking.py" --root "$root" --self-test
