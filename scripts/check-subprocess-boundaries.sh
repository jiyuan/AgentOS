#!/usr/bin/env bash
set -euo pipefail

# Runtime children belong behind tools/exec.rs. The only production exceptions
# are the Seatbelt availability probe (which establishes whether that boundary
# can enforce at all) and the gateway's operator-driven detached bootstrap.
violations="$({
  rg -n '(^|[^[:alnum:]_])(std::process::Command|tokio::process::Command|Command)::new' \
    crates/agentos-core/src crates/agentos-cli/src \
    --glob '*.rs' || true
} | rg -v \
  '^(crates/agentos-core/src/tools/exec.rs|crates/agentos-core/src/sandbox/macos.rs|crates/agentos-core/src/sandbox/mod.rs|crates/agentos-cli/src/bin/agentos-gateway/main.rs):' \
  || true)"

if [[ -n "$violations" ]]; then
  echo "runtime subprocess bypasses the common tools/exec.rs boundary:" >&2
  echo "$violations" >&2
  exit 1
fi

echo "subprocess boundaries ok"
