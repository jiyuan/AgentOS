#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# These suites cover the accepted/ACKed/replayed/delivered state transitions
# and the process-control crash edges. They run on native Linux and macOS in
# required CI because sockets, signals, flock, Landlock, and Seatbelt cannot be
# meaningfully qualified by a managed sandbox.
cargo test --locked -p agentos-core --test gateway_ingress_crash
cargo test --locked -p agentos-core --test delivery_crash
cargo test --locked -p agentos-core --test approval_recovery
cargo test --locked -p agentos-core --test state_durability
cargo test --locked -p agentos-cli --test gateway_lifecycle

echo "gateway crash matrix ok"
