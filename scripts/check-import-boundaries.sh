#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Boundary invariant: core and interfaces may use runtime-supplied workspace
# paths, but they must not compile against workspace-owned or extension crates.
pattern='(\.\./\.\./workspace|\.\./\.\./extensions|workspace/|extensions/)'

# First-party extension crate names. Core/interfaces must not depend on these
# even via a workspace-inherited edge (`name.workspace = true`), which carries
# no `extensions/` path for the pattern above to catch. Pipe-separated for the
# alternation; add new extension crates here.
ext_names='agentos-memory-vector'

# Returns 0 (success) when the manifest contains a boundary violation: either a
# workspace/extension path dependency, or a dependency on a named extension crate.
manifest_violates() {
  grep -Eq "$pattern" "$1" || grep -Eq "(^|[^[:alnum:]_-])($ext_names)([^[:alnum:]_-]|\$)" "$1"
}

# Self-test: prove the checker actually rejects violating manifests instead of
# vacuously passing. Exercises the same `pattern` the real check uses.
# Invoked by `cargo test -p agentos-core` (tests/import_boundary.rs) and CI.
if [[ "${1:-}" == "--self-test" ]]; then
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT

  cat > "$tmp/violating-extensions.toml" <<'EOF'
[dependencies]
custom-orchestrator = { path = "../../extensions/orchestrators/custom" }
EOF
  cat > "$tmp/violating-workspace.toml" <<'EOF'
[dependencies]
agent-config = { path = "../../workspace" }
EOF
  cat > "$tmp/violating-extension-name.toml" <<'EOF'
[dependencies]
agentos-memory-vector.workspace = true
EOF
  cat > "$tmp/clean.toml" <<'EOF'
[dependencies]
serde = { version = "1", features = ["derive"] }
agentos-proto = { version = "0.5", path = "../agentos-proto" }
EOF

  if ! manifest_violates "$tmp/violating-extensions.toml"; then
    echo "self-test FAILED: extensions path dependency was not rejected"
    exit 1
  fi
  if ! manifest_violates "$tmp/violating-workspace.toml"; then
    echo "self-test FAILED: workspace path dependency was not rejected"
    exit 1
  fi
  if ! manifest_violates "$tmp/violating-extension-name.toml"; then
    echo "self-test FAILED: workspace-inherited extension dependency was not rejected"
    exit 1
  fi
  if manifest_violates "$tmp/clean.toml"; then
    echo "self-test FAILED: clean manifest was flagged as a violation"
    exit 1
  fi

  echo "import boundary self-test ok"
  exit 0
fi

for manifest in "$root"/crates/agentos-core/Cargo.toml "$root"/crates/agentos-interfaces/Cargo.toml; do
  if manifest_violates "$manifest"; then
    echo "import boundary violation in $manifest"
    exit 1
  fi
done

if command -v cargo >/dev/null 2>&1; then
  cargo tree -p agentos-core --manifest-path "$root/Cargo.toml" >/dev/null
  cargo tree -p agentos-interfaces --manifest-path "$root/Cargo.toml" >/dev/null
fi

echo "import boundaries ok"
