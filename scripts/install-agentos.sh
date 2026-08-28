#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# XDG paths, which is what docs/INSTALL.md, README.md, and docs/USER_GUIDE.md
# have always told users to run. The installer previously defaulted to
# ~/.agentos, so following the documentation gave "command not found"
# (M2 deliverable 2). One layout, and the documents are the definition of it.
xdg_data_home="${XDG_DATA_HOME:-$HOME/.local/share}"
prefix="${PREFIX:-$HOME/.local}"
bindir="${BINDIR:-$prefix/bin}"
agentos_home="${AGENTOS_HOME:-$xdg_data_home/agentos}"
from_source=0
skip_build=0
replace_workspace=0
# shellcheck source=release-versions.env
source "$root/scripts/release-versions.env"
rust_toolchain="${AGENTOS_RUST_TOOLCHAIN}"

usage() {
  cat <<'USAGE'
Usage: scripts/install-agentos.sh [OPTIONS]

Install AgentOS from a source checkout or a packaged release bundle.

Options:
  --from-source       Build binaries from the current source checkout.
  --skip-build        Do not build source binaries; require existing artifacts.
  --replace-workspace Replace shipped workspace entries after making a backup.
  --reset-workspace   Alias for --replace-workspace.
  --prefix PATH       Installation prefix. Default: ~/.local
  --bindir PATH       Binary install directory. Default: <prefix>/bin
  --home PATH         AgentOS runtime home. Default: $XDG_DATA_HOME/agentos
                      (~/.local/share/agentos)
  -h, --help          Show this help.

Environment:
  PREFIX              Installation prefix override.
  BINDIR              Binary install directory override.
  XDG_DATA_HOME       Base for the runtime home. Default: ~/.local/share
  AGENTOS_HOME        AgentOS runtime home override.
  AGENTOS_RUST_TOOLCHAIN  Exact Rust toolchain for source builds.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from-source)
      from_source=1
      shift
      ;;
    --skip-build)
      skip_build=1
      shift
      ;;
    --replace-workspace|--reset-workspace)
      replace_workspace=1
      shift
      ;;
    --prefix)
      # An explicit prefix relocates the whole install, which is what a
      # clean-room smoke test into a temporary directory needs.
      prefix="$2"
      bindir="$prefix/bin"
      agentos_home="$prefix/share/agentos"
      shift 2
      ;;
    --bindir)
      bindir="$2"
      shift 2
      ;;
    --home)
      agentos_home="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

bin_source_dir="$root/bin"

build_from_source() {
  if ! command -v rustup >/dev/null 2>&1; then
    echo "rustup is required for --from-source installs" >&2
    exit 1
  fi
  if ! rustup show >/dev/null 2>&1; then
    cat >&2 <<ERROR
A rustup proxy is on PATH but rustup is not initialized for this user
(\$HOME=$HOME). This commonly happens when running as root with a
distribution rustup package that was never set up.

Initialize it for the current user, then rerun this script:

  rustup-init -y            # or: install from https://rustup.rs
  rustup default ${rust_toolchain}

If a working toolchain lives under another user's home, re-run with
RUSTUP_HOME and CARGO_HOME pointing at it.
ERROR
    exit 1
  fi
  "$root/scripts/install-toolchain.sh" --skip-semver-checks --skip-cargo-deny
  rustup run "$rust_toolchain" cargo build \
    --locked \
    --release \
    --manifest-path "$root/Cargo.toml" \
    -p agentos-cli \
    -p agentos-core \
    --bins
  bin_source_dir="$root/target/release"
}

if [[ "$from_source" == "1" ]]; then
  if [[ "$skip_build" != "1" ]]; then
    build_from_source
  else
    bin_source_dir="$root/target/release"
  fi
elif [[ ! -x "$bin_source_dir/agentos-cli" ]]; then
  if [[ "$skip_build" == "1" ]]; then
    echo "release binaries are missing under $bin_source_dir" >&2
    exit 1
  fi
  build_from_source
fi

for binary in agentos-cli agentos-gateway agentos-tool-worker agentos-mcp-stdio-worker; do
  if [[ ! -x "$bin_source_dir/$binary" ]]; then
    echo "missing binary: $bin_source_dir/$binary" >&2
    exit 1
  fi
done

install -d "$bindir" "$agentos_home/bin" "$agentos_home/scripts" "$agentos_home/docs" "$agentos_home/workspace" "$agentos_home/logs"

for binary in agentos-cli agentos-gateway agentos-tool-worker agentos-mcp-stdio-worker; do
  install -m 755 "$bin_source_dir/$binary" "$agentos_home/bin/$binary"
done

install -m 755 "$root/scripts/start-agentos.sh" "$agentos_home/scripts/start-agentos.sh"
install -m 644 "$root/.env.example" "$agentos_home/.env.example"
install -m 644 "$root/README.md" "$agentos_home/README.md"
install -m 644 "$root/LICENSE" "$agentos_home/LICENSE"
if [[ -f "$root/docs/RELEASE_INPUTS.json" ]]; then
  install -m 644 "$root/docs/RELEASE_INPUTS.json" "$agentos_home/RELEASE_INPUTS.json"
elif [[ -f "$root/RELEASE_INPUTS.json" ]]; then
  install -m 644 "$root/RELEASE_INPUTS.json" "$agentos_home/RELEASE_INPUTS.json"
fi

# Whatever documentation the source is carrying. From a release bundle this is
# the closure package-release.sh computed; from a source checkout it is docs/.
# Either way the links inside the installed README have somewhere to point.
for doc in "$root"/docs/*.md; do
  [[ -e "$doc" ]] || continue
  install -m 644 "$doc" "$agentos_home/docs/$(basename "$doc")"
done

if [[ ! -f "$agentos_home/.env" ]]; then
  cp "$agentos_home/.env.example" "$agentos_home/.env"
fi

# Distribution defaults and operator state have different owners. Every
# release installs its declarative workspace into a versioned, immutable
# review location. The live workspace is seeded only when an entry is absent.
# An explicit reset replaces only these four declarative entries and first
# copies their current contents to a recoverable backup; runtime databases,
# traces, logs, and unrecognised workspace sentinels are never touched.
if [[ -f "$root/VERSION" ]]; then
  version="$(tr -d '[:space:]' < "$root/VERSION")"
else
  version="$(awk -F'"' '/^version = / { print $2; exit }' "$root/Cargo.toml")"
fi
if [[ -z "$version" || ! "$version" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  echo "invalid release version: $version" >&2
  exit 1
fi

defaults_root="$agentos_home/dist/$version/workspace"
install -d "$defaults_root"

backup_root=""
if [[ "$replace_workspace" == "1" ]]; then
  backup_root="$agentos_home/backups/workspace-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  install -d "$backup_root"
fi

for entry in agent.toml skills subagents suborchs; do
  source_path="$root/workspace/$entry"
  if [[ ! -e "$source_path" ]]; then
    echo "workspace entry is missing from the source tree: workspace/$entry" >&2
    exit 1
  fi
  default_path="$defaults_root/$entry"
  if [[ ! -e "$default_path" ]]; then
    if [[ -d "$source_path" ]]; then
      cp -RL "$source_path" "$default_path"
    else
      install -m 644 "$source_path" "$default_path"
    fi
  fi

  live_path="$agentos_home/workspace/$entry"
  if [[ "$replace_workspace" == "1" && ( -e "$live_path" || -L "$live_path" ) ]]; then
    cp -Rp "$live_path" "$backup_root/$entry"
    rm -rf "$live_path"
  fi
  if [[ ! -e "$live_path" && ! -L "$live_path" ]]; then
    if [[ -d "$default_path" ]]; then
      cp -Rp "$default_path" "$live_path"
    else
      install -m 644 "$default_path" "$live_path"
    fi
  fi
done

cat >"$bindir/agentos" <<EOF
#!/usr/bin/env bash
exec "$agentos_home/scripts/start-agentos.sh" "\$@"
EOF
chmod 755 "$bindir/agentos"

echo "Installed AgentOS"
echo "  home:    $agentos_home"
echo "  command: $bindir/agentos"
echo "  defaults: $defaults_root"
if [[ -n "$backup_root" ]]; then
  echo "  backup:  $backup_root"
fi
echo "Next steps:"
echo "  1. Edit $agentos_home/.env"
echo "  2. Run $bindir/agentos tui"
