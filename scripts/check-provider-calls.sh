#!/usr/bin/env bash
# Fail when a provider call is added outside the request gateway.
#
# M5 / `REQ-001`, the enforcement half of
# `docs/adr/0004-FAIL_CLOSED_ISOLATION.md`'s sibling ADR-0004: "every provider
# call records what it was made of" is only as good as the thing that notices
# it being broken. Two production calls — the routing classifier and the
# compaction summarizer — drifted outside the rule and stayed there, because
# nothing was watching.
#
# The rule: in `crates/*/src/`, `.complete_messages(` and
# `.complete_messages_stream(` may only appear in the files listed below.
# Everything else must build a `prompt::Request` and go through
# `prompt::call_with_context` or `prompt::call_detached`, which is what
# attaches a kind, a manifest, and a usage record.
#
# Usage: scripts/check-provider-calls.sh
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Files permitted to call a provider directly, each for a stated reason.
#
#   prompt/gateway.rs         — is the gateway.
#   orchestrator/streaming.rs — the streamed transport the gateway delegates to
#                               for `RequestKind::Turn`; reached only from
#                               there.
#   agentos-gateway/calibrate.rs — the token-estimator calibration harness. It
#                               sends a fixed corpus to measure estimator error
#                               and is not part of a run: there is no
#                               `RunState`, no transcript, and nothing to
#                               record a header against.
allowed=(
  "crates/agentos-core/src/prompt/gateway.rs"
  "crates/agentos-core/src/orchestrator/streaming.rs"
  "crates/agentos-cli/src/bin/agentos-gateway/calibrate.rs"
)

is_allowed() {
  local candidate="$1"
  local entry
  for entry in "${allowed[@]}"; do
    [[ "$candidate" == "$entry" ]] && return 0
  done
  return 1
}

# The provider adapters themselves are the implementation, not a caller.
violations=0
while IFS= read -r line; do
  file="${line%%:*}"
  rel="${file#"$root"/}"
  case "$rel" in
    crates/agentos-llm/*) continue ;;
  esac
  if is_allowed "$rel"; then
    continue
  fi
  echo "error: $rel calls a provider directly"
  echo "       ${line#*:}"
  violations=$((violations + 1))
done < <(
  grep -rn --include='*.rs' \
    -e '\.complete_messages(' \
    -e '\.complete_messages_stream(' \
    "$root/crates"/*/src 2>/dev/null || true
)

if (( violations > 0 )); then
  cat <<'MSG'

provider-call lint failed.

Every provider call must carry a request kind, a recorded manifest, and a
usage record. Build a `prompt::Request` — `prompt::assemble` for a turn,
`prompt::routing_request` or `prompt::summary_request` otherwise — and send it
with `prompt::call_with_context` (when you hold a `RunContext`) or
`prompt::call_detached` (when you do not).

See docs/adr/0004-REQUEST_KINDS.md. If a new call genuinely cannot go through
the gateway, add it to the allowlist in this script with the reason.
MSG
  exit 1
fi

echo "provider calls ok"
