#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dist_dir="${DIST_DIR:-$root/dist}"
# shellcheck source=release-versions.env
source "$root/scripts/release-versions.env"
rust_toolchain="${AGENTOS_RUST_TOOLCHAIN}"

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

install -d "$stage_dir/bin" "$stage_dir/scripts" "$stage_dir/docs" "$stage_dir/workspace"

for binary in agentos-cli agentos-gateway agentos-tool-worker agentos-mcp-stdio-worker; do
  install -m 755 "$root/target/release/$binary" "$stage_dir/bin/$binary"
done

install -m 755 "$root/scripts/install-agentos.sh" "$stage_dir/scripts/install-agentos.sh"
install -m 755 "$root/scripts/start-agentos.sh" "$stage_dir/scripts/start-agentos.sh"
install -m 644 "$root/scripts/release-versions.env" "$stage_dir/scripts/release-versions.env"
install -m 644 "$root/.env.example" "$stage_dir/.env.example"
install -m 644 "$root/README.md" "$stage_dir/README.md"
install -m 644 "$root/LICENSE" "$stage_dir/LICENSE"
install -m 644 "$root/docs/RELEASE_INPUTS.json" "$stage_dir/docs/RELEASE_INPUTS.json"

# Documentation: the transitive closure of relative Markdown links reachable
# from what the bundle already ships, rather than a hand-listed three. The
# previous list packaged INSTALL, USER_GUIDE, and RELEASE_NOTES while README
# linked ARCHITECTURE, SKILLS, and both generated catalogs, so a third of the
# bundle's own links were dead on arrival. Computed rather than enumerated so
# adding a link to a packaged document cannot silently break the bundle.
doc_queue=(README.md docs/INSTALL.md docs/USER_GUIDE.md docs/RELEASE_NOTES.md)
doc_seen=()

doc_already_seen() {
  local candidate="$1" seen
  for seen in ${doc_seen[@]+"${doc_seen[@]}"}; do
    [[ "$seen" == "$candidate" ]] && return 0
  done
  return 1
}

# Resolve "link as written in $1" to a repo-relative path.
resolve_doc_link() {
  local from_dir="$1" link="$2" combined
  combined="$from_dir/$link"
  # Collapse ./ and x/../ without touching the filesystem, so a link that does
  # not resolve is reported by the caller rather than silently skipped.
  printf '%s' "$combined" | awk -F/ '{
    n = 0
    for (i = 1; i <= NF; i++) {
      if ($i == "" || $i == ".") continue
      if ($i == "..") { if (n > 0) n--; continue }
      parts[++n] = $i
    }
    out = ""
    for (i = 1; i <= n; i++) out = out (i > 1 ? "/" : "") parts[i]
    print out
  }'
}

while [[ ${#doc_queue[@]} -gt 0 ]]; do
  doc="${doc_queue[0]}"
  doc_queue=("${doc_queue[@]:1}")
  doc_already_seen "$doc" && continue
  if [[ ! -f "$root/$doc" ]]; then
    echo "packaged document is missing from the repository: $doc" >&2
    exit 1
  fi
  doc_seen+=("$doc")

  doc_dir="$(dirname "$doc")"
  while IFS= read -r link; do
    [[ -z "$link" ]] && continue
    # Only relative in-repo Markdown; anything with a scheme is external.
    [[ "$link" == *://* ]] && continue
    link="${link%%#*}"
    [[ "$link" == *.md ]] || continue
    resolved="$(resolve_doc_link "$doc_dir" "$link")"
    doc_already_seen "$resolved" || doc_queue+=("$resolved")
  done < <(grep -oE '\]\([^)]+\)' "$root/$doc" | sed -E 's/^\]\((.*)\)$/\1/')
done

for doc in "${doc_seen[@]}"; do
  install -d "$stage_dir/$(dirname "$doc")"
  install -m 644 "$root/$doc" "$stage_dir/$doc"
done

# The runtime workspace, by allowlist. An allowlist rather than "copy the
# workspace minus some exclusions": the workspace also accumulates task
# directories, session logs, a multi-hundred-megabyte SQLite file, traces, and
# attachments, and a denylist silently ships whatever kind of state gets added
# next. What a fresh install needs is the declarative content.
for entry in agent.toml skills subagents suborchs; do
  source_path="$root/workspace/$entry"
  if [[ ! -e "$source_path" ]]; then
    echo "workspace entry is missing from the repository: workspace/$entry" >&2
    exit 1
  fi
  if [[ -d "$source_path" ]]; then
    # -L to materialise any symlinked skill content into the bundle.
    cp -RL "$source_path" "$stage_dir/workspace/$entry"
  else
    install -m 644 "$source_path" "$stage_dir/workspace/$entry"
  fi
done

printf '%s\n' "$version" >"$stage_dir/VERSION"

LC_ALL=C LANG=C tar -C "$dist_dir" -czf "$archive_path" "$bundle_name"

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
