#!/usr/bin/env python3
"""Ratchet the audit finding table against named regression artifacts."""

from __future__ import annotations

import argparse
import re
import stat
import tempfile
from pathlib import Path


MINIMUM_FINDING = 40
EXPECTED_HEADER = [
    "ID",
    "Finding",
    "Priority",
    "Corrective slice",
    "Acceptance evidence",
    "Regression artifact",
    "Execution",
    "Status",
]
EXECUTION_CLASSES = {
    "portable",
    "native-linux",
    "native-macos",
    "native-linux+macos",
}
ARTIFACT = re.compile(r"`([^`]+)`")
RUST_TEST = re.compile(r"^(?P<path>.+\.rs)::(?P<name>[a-z][a-z0-9_]*)$")
PYTHON_TEST = re.compile(r"^(?P<path>.+\.py)::(?P<name>test_[a-z0-9_]*)$")


def table_rows(markdown: str) -> list[dict[str, str]]:
    lines = markdown.splitlines()
    headings = [
        index
        for index, line in enumerate(lines)
        if line.strip() == "## Audit finding coverage"
    ]
    if len(headings) != 1:
        raise ValueError("expected exactly one 'Audit finding coverage' heading")
    tables = [
        index
        for index, line in enumerate(lines[headings[0] + 1 :], headings[0] + 1)
        if line.strip().startswith("| ID | Finding | Priority |")
    ]
    if len(tables) != 1:
        raise ValueError("expected exactly one audit finding coverage table")
    start = tables[0]

    header = [cell.strip() for cell in lines[start].strip().strip("|").split("|")]
    if header != EXPECTED_HEADER:
        raise ValueError(
            "audit finding table header changed; expected " + " | ".join(EXPECTED_HEADER)
        )
    rows: list[dict[str, str]] = []
    for line in lines[start + 2 :]:
        if not line.startswith("|"):
            break
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != len(header):
            raise ValueError(f"malformed audit finding row: {line}")
        rows.append(dict(zip(header, cells)))
    if not rows:
        raise ValueError("audit finding coverage table has no rows")
    return rows


def artifact_exists(root: Path, reference: str, finding: str) -> str | None:
    rust = RUST_TEST.fullmatch(reference)
    python = PYTHON_TEST.fullmatch(reference)
    if rust or python:
        match = rust or python
        assert match is not None
        path = root / match.group("path")
        if not path.is_file():
            return f"artifact file does not exist: {reference}"
        body = path.read_text(encoding="utf-8")
        keyword = "fn" if rust else "def"
        test = re.search(
            rf"(?m)^\s*(?:async\s+)?{keyword}\s+{re.escape(match.group('name'))}\s*\(",
            body,
        )
        if not test:
            return f"named regression does not exist: {reference}"
        marker_window = body[max(0, test.start() - 500) : test.start() + 200]
        if finding not in marker_window:
            return f"named regression is not marked {finding}: {reference}"
        return None

    path = root / reference
    if reference.startswith("scripts/") and reference.endswith(".sh"):
        if not path.is_file():
            return f"artifact script does not exist: {reference}"
        if not path.stat().st_mode & stat.S_IXUSR:
            return f"artifact script is not executable: {reference}"
        if finding not in path.read_text(encoding="utf-8"):
            return f"artifact script is not marked {finding}: {reference}"
        return None
    return f"unsupported regression artifact reference: {reference}"


def safe_reference(reference: str) -> bool:
    path_text = reference.split("::", 1)[0]
    path = Path(path_text)
    return not path.is_absolute() and ".." not in path.parts


def validate(root: Path, roadmap: Path) -> list[str]:
    try:
        rows = table_rows(roadmap.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        return [str(error)]

    errors: list[str] = []
    ids: list[int] = []
    artifact_owners: dict[str, str] = {}
    for row in rows:
        match = re.fullmatch(r"`AF-(\d{3})`", row["ID"])
        if not match:
            errors.append(f"invalid finding ID: {row['ID']}")
            continue
        finding_id = int(match.group(1))
        ids.append(finding_id)
        label = f"AF-{finding_id:03d}"

        if row["Priority"] not in {"P0", "P1", "P2"}:
            errors.append(f"{label}: invalid priority {row['Priority']!r}")
        for column in ["Finding", "Corrective slice", "Acceptance evidence"]:
            if not row[column]:
                errors.append(f"{label}: {column} must not be empty")
        execution = row["Execution"].strip("`")
        if execution not in EXECUTION_CLASSES:
            errors.append(f"{label}: invalid execution class {row['Execution']!r}")
        elif row["Execution"] != f"`{execution}`":
            errors.append(f"{label}: execution class must be wrapped in backticks")

        references = ARTIFACT.findall(row["Regression artifact"])
        if not references:
            errors.append(f"{label}: no named regression artifact")
        elif row["Regression artifact"] != ", ".join(
            f"`{reference}`" for reference in references
        ):
            errors.append(f"{label}: regression artifacts must be comma-separated code spans")
        for reference in references:
            if not safe_reference(reference):
                errors.append(f"{label}: artifact escapes the repository: {reference!r}")
            if not (RUST_TEST.fullmatch(reference) or PYTHON_TEST.fullmatch(reference)) and not (
                reference.startswith("scripts/") and reference.endswith(".sh")
            ):
                errors.append(f"{label}: unsupported artifact syntax {reference!r}")
            previous = artifact_owners.setdefault(reference, label)
            if previous != label:
                errors.append(
                    f"{label}: artifact {reference!r} is already assigned to {previous}"
                )

        status = row["Status"]
        if status not in {"Open", "Complete"}:
            errors.append(f"{label}: status must be Open or Complete, got {status!r}")
        if status == "Complete":
            for reference in references:
                if not safe_reference(reference):
                    continue
                problem = artifact_exists(root, reference, label)
                if problem:
                    errors.append(f"{label}: marked complete but {problem}")

    if len(ids) != len(set(ids)):
        errors.append("finding IDs are not unique")
    if ids:
        expected_max = max(max(ids), MINIMUM_FINDING)
        expected = list(range(1, expected_max + 1))
        if sorted(ids) != expected:
            missing = sorted(set(expected) - set(ids))
            unexpected = sorted(set(ids) - set(expected))
            if missing:
                errors.append(
                    "finding register lost stable IDs: "
                    + ", ".join(f"AF-{number:03d}" for number in missing)
                )
            if unexpected:
                errors.append(
                    "finding register has unexpected IDs: "
                    + ", ".join(f"AF-{number:03d}" for number in unexpected)
                )
    return errors


def self_test() -> None:
    heading = "## Audit finding coverage\n\n"
    header = "| " + " | ".join(EXPECTED_HEADER) + " |\n"
    divider = "|" + "|".join("---" for _ in EXPECTED_HEADER) + "|\n"
    rows = []
    for number in range(1, MINIMUM_FINDING + 1):
        rows.append(
            f"| `AF-{number:03d}` | finding | P1 | `TEST-002` | evidence | "
            f"`tests/future.rs::test_{number:03d}` | `portable` | Open |\n"
        )
    with tempfile.TemporaryDirectory(prefix="agentos-audit-ratchet-") as directory:
        root = Path(directory)
        roadmap = root / "roadmap.md"
        valid = heading + header + divider + "".join(rows)
        roadmap.write_text(valid, encoding="utf-8")
        if validate(root, roadmap):
            raise RuntimeError("open findings must be allowed to name planned artifacts")

        complete_without_test = valid.replace(
            "`tests/future.rs::test_001` | `portable` | Open",
            "`tests/future.rs::test_001` | `portable` | Complete",
        )
        roadmap.write_text(complete_without_test, encoding="utf-8")
        if not any("marked complete" in error for error in validate(root, roadmap)):
            raise RuntimeError("a complete row without its test was accepted")

        tests = root / "tests"
        tests.mkdir()
        (tests / "future.rs").write_text("fn test_001() {}\n", encoding="utf-8")
        roadmap.write_text(complete_without_test, encoding="utf-8")
        if not any("is not marked AF-001" in error for error in validate(root, roadmap)):
            raise RuntimeError("a complete row accepted an unrelated existing test")

        (tests / "future.rs").write_text("// AF-001\nfn test_001() {}\n", encoding="utf-8")
        roadmap.write_text(complete_without_test, encoding="utf-8")
        if validate(root, roadmap):
            raise RuntimeError("a complete row did not resolve its named test")

        roadmap.write_text(valid.replace(rows[0], ""), encoding="utf-8")
        if not any("lost stable IDs" in error for error in validate(root, roadmap)):
            raise RuntimeError("deleting a stable finding ID was accepted")

        roadmap.write_text(valid.replace("`portable`", "`managed-host`", 1), encoding="utf-8")
        if not any("invalid execution class" in error for error in validate(root, roadmap)):
            raise RuntimeError("an unknown execution class was accepted")

        duplicate = valid.replace(
            "`tests/future.rs::test_002`",
            "`tests/future.rs::test_001`",
        )
        roadmap.write_text(duplicate, encoding="utf-8")
        if not any("already assigned" in error for error in validate(root, roadmap)):
            raise RuntimeError("one regression artifact was assigned to two findings")

        escaping = valid.replace(
            "`tests/future.rs::test_001`",
            "`../outside.rs::test_escape`",
        )
        roadmap.write_text(escaping, encoding="utf-8")
        if not any("escapes the repository" in error for error in validate(root, roadmap)):
            raise RuntimeError("a repository-escaping artifact was accepted")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--roadmap",
        type=Path,
        default=Path("docs/AUDIT_FOLLOWUP_ROADMAP.md"),
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        print("audit finding checker self-test ok")
        return 0

    root = args.root.resolve()
    roadmap = args.roadmap
    if not roadmap.is_absolute():
        roadmap = root / roadmap
    errors = validate(root, roadmap)
    for error in errors:
        print(f"FAIL: {error}")
    if errors:
        print(f"audit finding register FAILED ({len(errors)} problem(s))")
        return 1
    rows = table_rows(roadmap.read_text(encoding="utf-8"))
    complete = sum(row["Status"] == "Complete" for row in rows)
    print(
        f"audit finding register ok: {len(rows)} stable IDs, "
        f"{complete} complete, {len(rows) - complete} open"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
