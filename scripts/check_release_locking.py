#!/usr/bin/env python3
"""Reject moving release inputs and stale dependency manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import tempfile
from pathlib import Path


EXACT_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
EXACT_NIGHTLY = re.compile(r"^nightly-[0-9]{4}-[0-9]{2}-[0-9]{2}$")
RELEASE_SCRIPTS = (
    "scripts/install-agentos.sh",
    "scripts/package-release.sh",
    "scripts/check-catalogs.sh",
    "scripts/bootstrap-agentos.sh",
)
WORKFLOWS = (".github/workflows/ci.yml", ".github/workflows/nightly.yml")


def read_versions(root: Path) -> dict[str, str]:
    versions: dict[str, str] = {}
    for raw in (root / "scripts/release-versions.env").read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            versions[key] = value
    return versions


def input_manifest(root: Path, versions: dict[str, str]) -> dict[str, str]:
    return {
        "cargo_deny": versions["AGENTOS_CARGO_DENY_VERSION"],
        "cargo_lock_sha256": hashlib.sha256((root / "Cargo.lock").read_bytes()).hexdigest(),
        "cargo_semver_checks": versions["AGENTOS_CARGO_SEMVER_CHECKS_VERSION"],
        "miri_toolchain": versions["AGENTOS_MIRI_TOOLCHAIN"],
        "rust_toolchain": versions["AGENTOS_RUST_TOOLCHAIN"],
    }


def logical_lines(text: str) -> list[str]:
    return [re.sub(r"\s+", " ", item).strip() for item in text.replace("\\\n", " ").splitlines()]


def check(root: Path) -> list[str]:
    errors: list[str] = []
    versions = read_versions(root)
    rust = versions.get("AGENTOS_RUST_TOOLCHAIN", "")
    deny = versions.get("AGENTOS_CARGO_DENY_VERSION", "")
    semver = versions.get("AGENTOS_CARGO_SEMVER_CHECKS_VERSION", "")
    miri = versions.get("AGENTOS_MIRI_TOOLCHAIN", "")
    for name, value in (("Rust", rust), ("cargo-deny", deny), ("cargo-semver-checks", semver)):
        if not EXACT_VERSION.fullmatch(value):
            errors.append(f"{name} version is not exact: {value!r}")
    if not EXACT_NIGHTLY.fullmatch(miri):
        errors.append(f"Miri toolchain is not date-pinned: {miri!r}")

    toolchain = (root / "rust-toolchain.toml").read_text()
    match = re.search(r'^channel\s*=\s*"([^"]+)"', toolchain, re.MULTILINE)
    if match is None or match.group(1) != rust:
        errors.append("rust-toolchain.toml drifts from release-versions.env")

    expected = input_manifest(root, versions)
    recorded = json.loads((root / "docs/RELEASE_INPUTS.json").read_text())
    if recorded != expected:
        errors.append("docs/RELEASE_INPUTS.json is stale; release inputs changed")

    for relative in RELEASE_SCRIPTS:
        for line in logical_lines((root / relative).read_text()):
            if "echo " in line or line.startswith(("#", "--")):
                continue
            if re.search(r"\bcargo (build|run)\b", line) and "--locked" not in line:
                errors.append(f"unlocked release-critical command in {relative}: {line}")

    for relative in WORKFLOWS:
        text = (root / relative).read_text()
        for line in logical_lines(text):
            if re.search(r"\bcargo (build|run)\b", line) and "--locked" not in line:
                errors.append(f"unlocked artifact command in {relative}: {line}")
        for action in re.findall(r"uses:\s*([^\s#]+)", text):
            if "@" not in action or not re.fullmatch(r"[^@]+@[0-9a-f]{40}", action):
                errors.append(f"action is not pinned by immutable digest in {relative}: {action}")
        blocks = text.split("uses: dtolnay/rust-toolchain@")
        for block in blocks[1:]:
            setup = "\n".join(block.splitlines()[:6])
            if f"toolchain: {rust}" not in setup and f"toolchain: {miri}" not in setup:
                errors.append(f"Rust action has no reviewed exact toolchain in {relative}")
        if re.search(r"toolchain:\s*(stable|nightly)\s*$", text, re.MULTILINE):
            errors.append(f"moving Rust toolchain in {relative}")

    ci = (root / WORKFLOWS[0]).read_text()
    nightly_text = (root / WORKFLOWS[1]).read_text()
    if f"tool: cargo-deny@{deny}" not in ci or f"tool: cargo-deny@{deny}" not in nightly_text:
        errors.append("cargo-deny CI installation is not pinned to the reviewed version")
    if f"tool: cargo-semver-checks@{semver}" not in ci:
        errors.append("cargo-semver-checks CI installation is not pinned to the reviewed version")

    combined = "\n".join((root / path).read_text() for path in (*RELEASE_SCRIPTS, *WORKFLOWS))
    if "AGENTOS_RUST_TOOLCHAIN:-stable" in combined:
        errors.append("moving stable toolchain fallback remains")
    return errors


def self_test(root: Path) -> None:
    selected = (
        "Cargo.lock",
        "rust-toolchain.toml",
        "docs/RELEASE_INPUTS.json",
        "scripts/release-versions.env",
        *RELEASE_SCRIPTS,
        *WORKFLOWS,
    )
    mutations = (
        ("scripts/package-release.sh", "\ncargo build --release\n", "unlocked release-critical"),
        ("Cargo.lock", "\n# changed lockfile\n", "RELEASE_INPUTS.json is stale"),
    )
    for relative, suffix, expected in mutations:
        with tempfile.TemporaryDirectory(prefix="agentos-release-locking-") as temp:
            fixture = Path(temp)
            for selected_path in selected:
                source = root / selected_path
                target = fixture / selected_path
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
            with (fixture / relative).open("a") as handle:
                handle.write(suffix)
            found = check(fixture)
            if not any(expected in error for error in found):
                raise RuntimeError(f"negative fixture was accepted ({relative}): {found}")

    with tempfile.TemporaryDirectory(prefix="agentos-release-locking-") as temp:
        fixture = Path(temp)
        for selected_path in selected:
            source = root / selected_path
            target = fixture / selected_path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        versions_path = fixture / "scripts/release-versions.env"
        versions_path.write_text(
            re.sub(
                r"(?m)^AGENTOS_CARGO_DENY_VERSION=.*$",
                "AGENTOS_CARGO_DENY_VERSION=latest",
                versions_path.read_text(),
            )
        )
        found = check(fixture)
        if not any("cargo-deny version is not exact" in error for error in found):
            raise RuntimeError(f"floating tool fixture was accepted: {found}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test(args.root)
    errors = check(args.root)
    if errors:
        for error in errors:
            print(f"release locking: {error}")
        return 1
    print(json.dumps(input_manifest(args.root, read_versions(args.root)), sort_keys=True))
    print("release locking ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
