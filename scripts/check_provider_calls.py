#!/usr/bin/env python3
"""Reject Rust provider calls outside the request gateway.

The scanner tokenizes Rust-like syntax after removing comments and every
string form. Unlike the old grep it recognizes whitespace/newline-separated
method calls and UFCS (`Llm::complete_messages`) without treating examples in
comments or literals as executable code.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

CALLS = {"complete_messages", "complete_messages_stream"}
ALLOWED = {
    Path("crates/agentos-core/src/prompt/gateway.rs"),
    Path("crates/agentos-cli/src/bin/agentos-gateway/calibrate.rs"),
}
TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|::|.", re.DOTALL)


def code_only(source: str) -> str:
    out = list(source)
    i = 0
    while i < len(source):
        if source.startswith("//", i):
            end = source.find("\n", i)
            end = len(source) if end < 0 else end
            out[i:end] = " " * (end - i)
            i = end
        elif source.startswith("/*", i):
            depth, j = 1, i + 2
            while j < len(source) and depth:
                if source.startswith("/*", j):
                    depth += 1
                    j += 2
                elif source.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            out[i:j] = " " * (j - i)
            i = j
        elif source[i] == '"' or (
            source[i] in "br" and i + 1 < len(source) and source[i + 1] == '"'
        ):
            start = i
            if source[i] in "br":
                i += 1
            i += 1
            while i < len(source):
                if source[i] == "\\":
                    i += 2
                elif source[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
            out[start:i] = " " * (i - start)
        elif source[i] in "br" and i + 1 < len(source) and source[i + 1] == "#":
            match = re.match(r"[br]+(#+)\"", source[i:])
            if match:
                start = i
                hashes = match.group(1)
                i += match.end()
                end_marker = '"' + hashes
                end = source.find(end_marker, i)
                i = len(source) if end < 0 else end + len(end_marker)
                out[start:i] = " " * (i - start)
            else:
                i += 1
        else:
            i += 1
    return "".join(out)


def direct_calls(source: str) -> list[tuple[int, str]]:
    tokens = [
        (match.group(), match.start())
        for match in TOKEN.finditer(code_only(source))
        if not match.group().isspace()
    ]
    violations = []
    for index, (token, offset) in enumerate(tokens):
        if token in CALLS and index and tokens[index - 1][0] in {".", "::"}:
            line = source.count("\n", 0, offset) + 1
            violations.append((line, token))
    return violations


def scan(root: Path) -> list[str]:
    violations: list[str] = []
    for path in sorted((root / "crates").glob("*/src/**/*.rs")):
        rel = path.relative_to(root)
        if rel in ALLOWED or rel.parts[1] == "agentos-llm":
            continue
        for line, call in direct_calls(path.read_text(encoding="utf-8")):
            violations.append(f"{rel}:{line}: direct provider call `{call}`")
    return violations


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parents[1])
    violations = scan(root)
    if violations:
        print("provider-call lint failed:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        print("Every run-time provider call must go through prompt/gateway.rs.", file=sys.stderr)
        return 1
    print("provider calls ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
