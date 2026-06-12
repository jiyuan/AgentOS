#!/usr/bin/env bash
# Fail when any production Rust source file in `crates/*/src/` exceeds the
# CLAUDE.md ceiling of ~800 lines (excluding `#[cfg(test)]` blocks).
#
# Usage: scripts/check-module-size.sh
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Hard ceiling per CLAUDE.md: "If a file exceeds ~800 lines, add new
# functionality in a new module rather than extending the existing one."
limit=800

# Pre-existing offenders tracked separately from the agentos-core split work.
# Keep this POSIX-ish instead of using associative arrays so the script works on
# the older Bash shipped by default on macOS.
# Per PLAN.md A6, only entry-point binaries may stay allowlisted.
is_allowlisted() {
  case "$1" in
    crates/agentos-cli/src/main.rs) return 0 ;;
    crates/agentos-cli/src/bin/agentos-gateway.rs) return 0 ;;
    *) return 1 ;;
  esac
}

# Count `cfg(test)`-stripped lines in a file by tracking brace depth.
# Lines from a `#[cfg(test)]` annotation through the matching closing brace
# (inclusive) are skipped from the count.
count_production_lines() {
  awk '
    BEGIN { lines = 0; pending = 0; depth = 0 }
    {
      line = $0

      if (depth > 0) {
        opens = gsub(/[{]/, "{", line)
        closes = gsub(/[}]/, "}", line)
        depth += opens - closes
        next
      }

      if (pending) {
        opens = gsub(/[{]/, "{", line)
        closes = gsub(/[}]/, "}", line)
        delta = opens - closes
        pending = 0
        if (delta > 0) {
          depth = delta
          next
        }
        # `#[cfg(test)] use ...;` or single-line item: drop just this line.
        next
      }

      if (line ~ /^[[:space:]]*#\[cfg\(test\)\]/) {
        pending = 1
        next
      }

      lines++
    }
    END { print lines }
  ' "$1"
}

violations=0
while IFS= read -r -d "" file; do
  rel="${file#"$root"/}"
  effective=$(count_production_lines "$file")
  if (( effective <= limit )); then
    continue
  fi

  if is_allowlisted "$rel"; then
    echo "warn: $rel is $effective LOC (allowlisted; budget $limit)"
    continue
  fi

  echo "error: $rel is $effective LOC (limit $limit, excluding cfg(test) blocks)"
  violations=$((violations + 1))
done < <(find "$root/crates" -type f -name "*.rs" -not -path "*/target/*" -print0)

if (( violations > 0 )); then
  echo
  echo "module-size lint failed: $violations file(s) exceed the $limit LOC ceiling."
  echo "Split new functionality into a new module rather than extending the existing one (see CLAUDE.md)."
  exit 1
fi

echo "module sizes ok"
