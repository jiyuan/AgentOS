#!/usr/bin/env bash
# AF-040: the release baseline includes all-target Clippy with warnings denied.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings

echo "baseline gates ok"
