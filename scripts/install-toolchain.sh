#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=release-versions.env
source "$root/scripts/release-versions.env"
rust_toolchain="${AGENTOS_RUST_TOOLCHAIN}"
install_semver_checks="${AGENTOS_INSTALL_SEMVER_CHECKS:-1}"
install_cargo_deny="${AGENTOS_INSTALL_CARGO_DENY:-1}"

cargo_for_toolchain() {
  rustup run "$rust_toolchain" cargo "$@"
}

usage() {
  cat <<'USAGE'
Usage: scripts/install-toolchain.sh [--skip-semver-checks] [--skip-cargo-deny]

Configures the local AgentOS Rust toolchain.

Environment:
  AGENTOS_RUST_TOOLCHAIN          Exact Rust toolchain to install/use.
  AGENTOS_INSTALL_SEMVER_CHECKS  Install cargo-semver-checks when set to 1. Default: 1
  AGENTOS_INSTALL_CARGO_DENY     Install cargo-deny when set to 1. Default: 1
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-semver-checks)
      install_semver_checks=0
      shift
      ;;
    --skip-cargo-deny)
      install_cargo_deny=0
      shift
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

if ! command -v rustup >/dev/null 2>&1; then
  cat >&2 <<'ERROR'
rustup is required but was not found.
Install it from https://rustup.rs, then rerun this script.
ERROR
  exit 1
fi

if ! rustup show >/dev/null 2>&1; then
  cat >&2 <<ERROR
A rustup proxy is on PATH but rustup is not initialized for this user
(\$HOME=$HOME). Initialize it, then rerun this script:

  rustup-init -y            # or: install from https://rustup.rs
  rustup default ${rust_toolchain}
ERROR
  exit 1
fi

echo "Installing Rust toolchain: ${rust_toolchain}"
rustup toolchain install "$rust_toolchain"
rustup component add clippy rustfmt --toolchain "$rust_toolchain"

if [[ "$install_semver_checks" == "1" ]]; then
  if [[ "$(cargo_for_toolchain semver-checks --version 2>/dev/null || true)" == "cargo-semver-checks $AGENTOS_CARGO_SEMVER_CHECKS_VERSION" ]]; then
    echo "cargo-semver-checks is already installed"
  else
    echo "Installing cargo-semver-checks $AGENTOS_CARGO_SEMVER_CHECKS_VERSION"
    cargo_for_toolchain install cargo-semver-checks --version "$AGENTOS_CARGO_SEMVER_CHECKS_VERSION" --locked
  fi
else
  echo "Skipping cargo-semver-checks installation"
fi

if [[ "$install_cargo_deny" == "1" ]]; then
  if [[ "$(cargo_for_toolchain deny --version 2>/dev/null || true)" == "cargo-deny $AGENTOS_CARGO_DENY_VERSION" ]]; then
    echo "cargo-deny is already installed"
  else
    echo "Installing cargo-deny $AGENTOS_CARGO_DENY_VERSION"
    cargo_for_toolchain install cargo-deny --version "$AGENTOS_CARGO_DENY_VERSION" --locked
  fi
else
  echo "Skipping cargo-deny installation"
fi

echo "Checking workspace toolchain"
cargo_for_toolchain fmt --all --check --manifest-path "$root/Cargo.toml"
cargo_for_toolchain check --workspace --locked --manifest-path "$root/Cargo.toml"

echo "AgentOS toolchain is ready"
