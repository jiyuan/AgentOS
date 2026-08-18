#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dist_dir="${DIST_DIR:-$root/dist}"
rust_toolchain="${AGENTOS_RUST_TOOLCHAIN:-stable}"

version="$(awk -F'"' '/^version = / { print $2; exit }' "$root/Cargo.toml")"
platform="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"

case "$platform" in
  darwin) platform="darwin" ;;
  linux) platform="linux" ;;
esac

case "$arch" in
  arm64) arch="arm64" ;;
  aarch64) arch="arm64" ;;
  x86_64) arch="x86_64" ;;
esac

bundle_name="agentos-v${version}-${platform}-${arch}"
stage_dir="$dist_dir/$bundle_name"
archive_path="$dist_dir/$bundle_name.tar.gz"
checksum_path="$archive_path.sha256"

mkdir -p "$dist_dir"
rm -rf "$stage_dir" "$archive_path" "$checksum_path"

rustup run "$rust_toolchain" cargo build \
  --locked \
  --release \
  --manifest-path "$root/Cargo.toml" \
  -p agentos-cli \
  -p agentos-core \
  --bins

install -d \
  "$stage_dir/bin" \
  "$stage_dir/scripts" \
  "$stage_dir/docs" \
  "$stage_dir/workspace/skills" \
  "$stage_dir/workspace/subagents" \
  "$stage_dir/workspace/suborchs" \
  "$stage_dir/workspace/crons" \
  "$stage_dir/workspace/tasks" \
  "$stage_dir/workspace/runs" \
  "$stage_dir/workspace/traces"

for binary in agentos-cli agentos-gateway agentos-tool-worker agentos-mcp-stdio-worker; do
  install -m 755 "$root/target/release/$binary" "$stage_dir/bin/$binary"
done

install -m 755 "$root/scripts/install-agentos.sh" "$stage_dir/scripts/install-agentos.sh"
install -m 755 "$root/scripts/start-agentos.sh" "$stage_dir/scripts/start-agentos.sh"
install -m 644 "$root/.env.example" "$stage_dir/.env.example"
install -m 644 "$root/workspace/agent.toml" "$stage_dir/workspace/agent.toml"
cp -R "$root/workspace/skills/." "$stage_dir/workspace/skills/"
cp -R "$root/workspace/subagents/." "$stage_dir/workspace/subagents/"
cp -R "$root/workspace/suborchs/." "$stage_dir/workspace/suborchs/"
install -m 644 "$root/README.md" "$stage_dir/README.md"
install -m 644 "$root/LICENSE" "$stage_dir/LICENSE"
install -m 644 "$root/DESIGN.md" "$stage_dir/DESIGN.md"
install -m 644 "$root/BENCHMARKS.md" "$stage_dir/BENCHMARKS.md"
cp -R "$root/docs/." "$stage_dir/docs/"
printf '%s\n' "$version" >"$stage_dir/VERSION"

COPYFILE_DISABLE=1 LC_ALL=C LANG=C tar -C "$dist_dir" -czf "$archive_path" "$bundle_name"

write_checksum() {
  local target="$1"
  if command -v shasum >/dev/null 2>&1; then
    LC_ALL=C LANG=C shasum -a 256 "$target" >"$checksum_path" && return 0
    rm -f "$checksum_path"
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    LC_ALL=C LANG=C sha256sum "$target" >"$checksum_path" && return 0
    rm -f "$checksum_path"
  fi
  return 1
}

if ! write_checksum "$archive_path"; then
  echo "warning: unable to generate release checksum" >&2
fi

echo "Release bundle created:"
echo "  $archive_path"
if [[ -f "$checksum_path" ]]; then
  echo "  $checksum_path"
fi
