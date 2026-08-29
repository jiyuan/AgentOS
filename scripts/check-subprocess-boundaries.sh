#!/usr/bin/env bash
set -euo pipefail

# Runtime children belong behind tools/exec.rs. The only production exceptions
# are the Seatbelt availability probe (which establishes whether that boundary
# can enforce at all) and the gateway's operator-driven detached bootstrap.
#
# The scanner is `grep`, not `rg`: CI provisions no ripgrep, and an earlier
# revision piped `rg ... || true`, so a missing scanner produced an empty
# violation list and this gate printed "ok" having read nothing. A gate whose
# silence is indistinguishable between "clean tree" and "never ran" is not a
# gate, so this one separates *no matches* from *scanner failed*, refuses to
# report on an empty file list, and runs `self_test` — which plants a spawn and
# requires the scanner to still find it — before any green result.
#
# Exclusions are per *scope*, not per file: `#[cfg(test)]` blocks are blanked
# before matching (line numbers preserved), so a test that shells out needs no
# entry here and a new production spawn in an allowlisted file is still caught.
#
# Like check-module-size.sh, this stays within the Bash 3.2 that macOS ships:
# no namerefs, no associative arrays, no `mapfile`.

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

PATTERN='(^|[^[:alnum:]_])(std::process::Command|tokio::process::Command|Command)::new'
ALLOWED='^(crates/agentos-core/src/tools/exec\.rs|crates/agentos-core/src/sandbox/macos\.rs|crates/agentos-cli/src/bin/agentos-gateway/main\.rs):'

# Set by scan_or_abort to the matching production `Command::new` sites.
SCAN_RESULT=""

# Every Rust source compiled into a crate or extension. Integration tests under
# `tests/` are deliberately out of scope: a fixture may spawn what it likes.
production_sources() {
  find crates extensions -type f -name '*.rs' -path '*/src/*' -not -path '*/target/*' | sort
}

# Blank `#[cfg(test)]` items and blocks, preserving line numbering so a reported
# match still points at the real line.
strip_tests() {
  awk '
    BEGIN { pending = 0; depth = 0 }
    {
      line = $0
      if (depth > 0) {
        opens = gsub(/[{]/, "{", line); closes = gsub(/[}]/, "}", line)
        depth += opens - closes
        print ""; next
      }
      if (pending) {
        opens = gsub(/[{]/, "{", line); closes = gsub(/[}]/, "}", line)
        delta = opens - closes
        pending = 0
        if (delta > 0) { depth = delta }
        print ""; next
      }
      if ($0 ~ /^[[:space:]]*#\[cfg\(test\)\]/) { pending = 1; print ""; next }
      print
    }
  ' "$1"
}

# Read file paths on stdin, print `path:line:text` for each production match.
# Returns 0 with matches, 1 with none, 2 when the scan itself failed.
scan() {
  local file hits status found seen
  found=""
  seen=0
  while IFS= read -r file; do
    seen=$((seen + 1))
    status=0
    hits="$(strip_tests "$file" | grep -nE "$PATTERN")" || status=$?
    if ((status > 1)); then
      echo "error: the subprocess scanner failed on $file (exit $status)" >&2
      return 2
    fi
    if [[ -n "$hits" ]]; then
      while IFS= read -r hit; do
        found="$found$file:$hit"$'\n'
      done <<<"$hits"
    fi
  done
  if ((seen == 0)); then
    echo "error: the subprocess scanner was given no sources to read" >&2
    return 2
  fi
  if [[ -z "$found" ]]; then
    return 1
  fi
  printf '%s' "$found"
  return 0
}

# Run `scan` over the file paths on stdin into SCAN_RESULT, aborting when the
# scanner could not do its job. Callers must redirect rather than pipe: a pipe
# would run this in a subshell, discarding both SCAN_RESULT and a fatal exit.
scan_or_abort() {
  local status=0
  SCAN_RESULT="$(scan)" || status=$?
  if ((status > 1)); then
    exit 2
  fi
}

# Prove the scanner still sees a spawn, still ignores a test-only one, and
# still reports nothing for clean source, before trusting that it found none.
self_test() {
  local fixture
  fixture="$(mktemp -d)"

  printf 'fn spawn() { let _ = std::process::Command::new("sh"); }\n' \
    >"$fixture/planted.rs"
  scan_or_abort <<<"$fixture/planted.rs"
  case "$SCAN_RESULT" in
    *planted.rs*) ;;
    *)
      rm -rf "$fixture"
      echo "error: scanner self-test failed — a planted child spawn went undetected" >&2
      exit 2
      ;;
  esac

  printf '#[cfg(test)]\nmod tests {\n fn t() { std::process::Command::new("sh"); }\n}\n' \
    >"$fixture/planted.rs"
  scan_or_abort <<<"$fixture/planted.rs"
  case "$SCAN_RESULT" in
    *planted.rs*)
      rm -rf "$fixture"
      echo "error: scanner self-test failed — a cfg(test) spawn was reported as production" >&2
      exit 2
      ;;
  esac

  printf 'fn safe() { let _ = 1; }\n' >"$fixture/planted.rs"
  scan_or_abort <<<"$fixture/planted.rs"
  case "$SCAN_RESULT" in
    *planted.rs*)
      rm -rf "$fixture"
      echo "error: scanner self-test failed — clean source reported a violation" >&2
      exit 2
      ;;
  esac

  rm -rf "$fixture"
}

self_test

scan_or_abort < <(production_sources)
violations="$(grep -Ev "$ALLOWED" <<<"$SCAN_RESULT" || true)"

if [[ -n "$violations" ]]; then
  echo "runtime subprocess bypasses the common tools/exec.rs boundary:" >&2
  echo "$violations" >&2
  exit 1
fi

echo "subprocess boundaries ok"
