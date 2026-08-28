#!/usr/bin/env bash
# AF-032: ordinary archive and source reinstalls preserve operator workspace
# content; explicit replacement is backed up and separately exercised.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/agentos-install-preservation.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

fixture="$scratch/bundle"
mkdir -p "$fixture/bin" "$fixture/target/release" "$fixture/scripts" \
  "$fixture/docs" "$fixture/workspace"
cp "$root/scripts/install-agentos.sh" "$fixture/scripts/install-agentos.sh"
cp "$root/scripts/start-agentos.sh" "$fixture/scripts/start-agentos.sh"
cp "$root/scripts/release-versions.env" "$fixture/scripts/release-versions.env"
cp "$root/.env.example" "$fixture/.env.example"
cp "$root/README.md" "$fixture/README.md"
cp "$root/LICENSE" "$fixture/LICENSE"
cp -R "$root/workspace/agent.toml" "$root/workspace/skills" \
  "$root/workspace/subagents" "$root/workspace/suborchs" "$fixture/workspace/"
cp "$root/docs/INSTALL.md" "$fixture/docs/INSTALL.md"
printf '0.7.0\n' > "$fixture/VERSION"

for binary in agentos-cli agentos-gateway agentos-tool-worker agentos-mcp-stdio-worker; do
  printf '#!/usr/bin/env sh\nexit 0\n' > "$fixture/bin/$binary"
  chmod 755 "$fixture/bin/$binary"
  cp "$fixture/bin/$binary" "$fixture/target/release/$binary"
done

install_once() {
  local prefix="$1"
  shift
  "$fixture/scripts/install-agentos.sh" --prefix "$prefix" --skip-build "$@" >/dev/null
}

assert_preserved() {
  local home="$1" expected_config="$2"
  cmp "$expected_config" "$home/workspace/agent.toml"
  test -f "$home/workspace/skills/operator-custom/SKILL.md"
  test "$(cat "$home/workspace/skills/operator-custom/SKILL.md")" = "operator skill"
  test "$(cat "$home/workspace/operator.sentinel")" = "operator sentinel"
}

# Release-bundle install and same-version reinstall preserve every mutable
# byte, while defaults remain available outside the live workspace.
archive_prefix="$scratch/archive-prefix"
install_once "$archive_prefix"
archive_home="$archive_prefix/share/agentos"
printf 'operator configuration\n' > "$archive_home/workspace/agent.toml"
mkdir -p "$archive_home/workspace/skills/operator-custom"
printf 'operator skill\n' > "$archive_home/workspace/skills/operator-custom/SKILL.md"
printf 'operator sentinel\n' > "$archive_home/workspace/operator.sentinel"
cp "$archive_home/workspace/agent.toml" "$scratch/archive-config"
install_once "$archive_prefix"
assert_preserved "$archive_home" "$scratch/archive-config"
test -f "$archive_home/dist/0.7.0/workspace/agent.toml"

# A new bundle version publishes new defaults for review without merging or
# replacing the operator's live files.
printf '0.7.1\n' > "$fixture/VERSION"
printf '\n# upgraded distribution default\n' >> "$fixture/workspace/agent.toml"
install_once "$archive_prefix"
assert_preserved "$archive_home" "$scratch/archive-config"
rg -q '^# upgraded distribution default$' \
  "$archive_home/dist/0.7.1/workspace/agent.toml"

# Replacement is explicit, affects only the shipped declarative allowlist,
# and leaves a recovery copy containing the overwritten configuration and
# extension.
install_once "$archive_prefix" --replace-workspace
rg -q '^# upgraded distribution default$' "$archive_home/workspace/agent.toml"
test ! -e "$archive_home/workspace/skills/operator-custom"
test "$(cat "$archive_home/workspace/operator.sentinel")" = "operator sentinel"
backup_count="$(find "$archive_home/backups" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
test "$backup_count" = "1"
backup="$(find "$archive_home/backups" -mindepth 1 -maxdepth 1 -type d)"
cmp "$scratch/archive-config" "$backup/agent.toml"
test "$(cat "$backup/skills/operator-custom/SKILL.md")" = "operator skill"

# Source installs use the identical preservation path; --skip-build points at
# controlled fixture binaries so this regression is fast and network-free.
source_prefix="$scratch/source-prefix"
install_once "$source_prefix" --from-source
source_home="$source_prefix/share/agentos"
printf 'source operator configuration\n' > "$source_home/workspace/agent.toml"
mkdir -p "$source_home/workspace/skills/operator-custom"
printf 'operator skill\n' > "$source_home/workspace/skills/operator-custom/SKILL.md"
printf 'operator sentinel\n' > "$source_home/workspace/operator.sentinel"
cp "$source_home/workspace/agent.toml" "$scratch/source-config"
install_once "$source_prefix" --from-source
assert_preserved "$source_home" "$scratch/source-config"

echo "install preservation ok"
