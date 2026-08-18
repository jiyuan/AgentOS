#!/usr/bin/env python3
"""Verify that an AgentOS release archive is self-contained and runnable."""

from __future__ import annotations

import argparse
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import tempfile
import tomllib
from urllib.parse import unquote


MARKDOWN_LINK = re.compile(r"!?\[[^]]*\]\(([^)]+)\)")


def fail(message: str) -> None:
    raise SystemExit(f"release archive check failed: {message}")


def extract_archive(archive: Path, destination: Path) -> Path:
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                fail(f"unsafe archive member: {member.name}")
        roots = {PurePosixPath(member.name).parts[0] for member in members if member.name}
        if len(roots) != 1:
            fail(f"expected one archive root, found {sorted(roots)}")
        bundle.extractall(destination)
    root = destination / roots.pop()
    if not root.is_dir():
        fail("archive root is not a directory")
    return root


def require_paths(root: Path) -> None:
    required = [
        "bin/agentos-cli",
        "bin/agentos-gateway",
        "bin/agentos-tool-worker",
        "bin/agentos-mcp-stdio-worker",
        "scripts/install-agentos.sh",
        "scripts/start-agentos.sh",
        "workspace/agent.toml",
        "workspace/skills",
        "workspace/subagents",
        "workspace/suborchs",
        "workspace/crons",
        "workspace/tasks",
        "workspace/runs",
        "workspace/traces",
        "README.md",
        "DESIGN.md",
        "BENCHMARKS.md",
        "docs/INSTALL.md",
        "docs/USER_GUIDE.md",
    ]
    missing = [path for path in required if not (root / path).exists()]
    if missing:
        fail(f"missing required paths: {', '.join(missing)}")

    executables = [
        "bin/agentos-cli",
        "bin/agentos-gateway",
        "bin/agentos-tool-worker",
        "bin/agentos-mcp-stdio-worker",
        "scripts/install-agentos.sh",
        "scripts/start-agentos.sh",
    ]
    non_executable = [path for path in executables if not os.access(root / path, os.X_OK)]
    if non_executable:
        fail(f"paths are not executable: {', '.join(non_executable)}")

    forbidden = [
        "workspace/agentos.sqlite",
        "workspace/main",
        "workspace/traces/telegram-gateway.jsonl",
    ]
    leaked = [path for path in forbidden if (root / path).exists()]
    if leaked:
        fail(f"archive contains runtime state: {', '.join(leaked)}")


def load_toml(path: Path) -> dict:
    with path.open("rb") as source:
        return tomllib.load(source)


def verify_workspace_references(root: Path) -> None:
    workspace = root / "workspace"
    config = load_toml(workspace / "agent.toml")
    enabled_skills = set(config.get("resources", {}).get("skills", {}).get("enabled", []))
    for skill in sorted(enabled_skills):
        if not (workspace / "skills" / skill / "SKILL.md").is_file():
            fail(f"enabled skill does not resolve: {skill}")

    subagents = {
        entry["id"]: entry for entry in config.get("subagents", [])
    }
    for path in sorted((workspace / "subagents").glob("*.toml")):
        entry = load_toml(path)
        subagents[entry["id"]] = entry
    for agent_id, entry in sorted(subagents.items()):
        for skill in entry.get("skills", []):
            if skill not in enabled_skills:
                fail(f"subagent {agent_id} references unavailable skill: {skill}")

    templates = {
        entry["name"]: entry for entry in config.get("templates", [])
    }
    for path in sorted((workspace / "suborchs").glob("*.toml")):
        entry = load_toml(path)
        templates[entry["name"]] = entry
    for template_name, entry in sorted(templates.items()):
        for stage in entry.get("stages", []):
            if stage["agent_id"] not in subagents:
                fail(
                    f"template {template_name} references unknown subagent: "
                    f"{stage['agent_id']}"
                )

    routing = config.get("routing", {})
    routes = [*routing.get("rules", []), routing.get("fallback", {})]
    for route in routes:
        if route.get("dispatch") == "delegate" and route.get("agent_id") not in subagents:
            fail(f"route {route.get('domain')} references unknown subagent")
        if route.get("dispatch") == "escalate" and route.get("template") not in templates:
            fail(f"route {route.get('domain')} references unknown template")


def verify_document_links(root: Path) -> None:
    for document in sorted(root.rglob("*.md")):
        text = document.read_text(encoding="utf-8")
        for match in MARKDOWN_LINK.finditer(text):
            raw_target = match.group(1).strip()
            if raw_target.startswith("<") and raw_target.endswith(">"):
                raw_target = raw_target[1:-1]
            target = unquote(raw_target.split("#", 1)[0].split("?", 1)[0])
            if not target or "://" in target or target.startswith("mailto:"):
                continue
            linked = (document.parent / target).resolve()
            if linked.suffix.lower() != ".md":
                continue
            relative_document = document.relative_to(root)
            try:
                linked.relative_to(root.resolve())
            except ValueError:
                fail(f"{relative_document} links outside the archive: {raw_target}")
            if not linked.is_file():
                fail(f"{relative_document} links missing document: {raw_target}")


def smoke_offline_echo(root: Path) -> None:
    marker = "release archive offline echo smoke"
    env = os.environ.copy()
    env.update(
        {
            "AGENTOS_HOME": str(root),
            "AGENTOS_AGENT_CONFIG_PATH": str(root / "workspace/agent.toml"),
            "AGENTOS_SESSION_DB_PATH": str(root / "workspace/smoke.sqlite"),
            "AGENTOS_RUN_STATE_PATH": str(root / "workspace/runs/smoke.json"),
            "AGENTOS_TOOL_WORKER_PATH": str(root / "bin/agentos-tool-worker"),
            "AGENTOS_NO_ENV_OVERRIDE": "1",
            "AGENTOS_LLM_PROVIDER": "builtin.echo",
            "AGENTOS_LLM_MODEL": "builtin.echo",
            "AGENTOS_LLM_MODEL_HIGH": "",
            "AGENTOS_LLM_MODEL_MEDIUM": "",
            "AGENTOS_LLM_MODEL_LOW": "",
            "AGENTOS_TUI_STREAM": "0",
            "OPENAI_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
            "DEEPSEEK_API_KEY": "",
            "OLLAMA_HOST": "",
        }
    )
    result = subprocess.run(
        [str(root / "scripts/start-agentos.sh"), "tui"],
        input=f"{marker}\n/exit\n",
        text=True,
        cwd=root.parent,
        env=env,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        fail(
            f"offline echo exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    if marker not in result.stdout:
        fail(
            f"offline echo response did not contain marker\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def clean_runtime_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in [
        "PREFIX",
        "BINDIR",
        "XDG_BIN_HOME",
        "XDG_DATA_HOME",
        "AGENTOS_HOME",
        "AGENTOS_ENV_FILE",
        "AGENTOS_AGENT_CONFIG_PATH",
        "AGENTOS_SESSION_DB_PATH",
        "AGENTOS_RUN_STATE_PATH",
        "AGENTOS_TOOL_WORKER_PATH",
        "AGENTOS_GATEWAY_PID_PATH",
        "AGENTOS_GATEWAY_LOG_PATH",
    ]:
        env.pop(name, None)
    env.update(
        {
            "AGENTOS_NO_ENV_OVERRIDE": "1",
            "AGENTOS_LLM_PROVIDER": "builtin.echo",
            "AGENTOS_LLM_MODEL": "builtin.echo",
            "AGENTOS_LLM_MODEL_HIGH": "",
            "AGENTOS_LLM_MODEL_MEDIUM": "",
            "AGENTOS_LLM_MODEL_LOW": "",
            "AGENTOS_TUI_STREAM": "0",
            "OPENAI_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
            "DEEPSEEK_API_KEY": "",
            "OLLAMA_HOST": "",
        }
    )
    return env


def run_wrapper(wrapper: Path, home: Path, *args: str, input_text: str | None = None):
    env = clean_runtime_env()
    env["HOME"] = str(home)
    return subprocess.run(
        [str(wrapper), *args],
        input=input_text,
        text=True,
        cwd=home.parent,
        env=env,
        capture_output=True,
        timeout=30,
        check=False,
    )


def smoke_installed_wrapper(
    root: Path,
    temporary: Path,
    label: str,
    install_args: tuple[str, ...] = (),
) -> None:
    fake_home = temporary / f"{label}-home"
    fake_home.mkdir()
    install_env = clean_runtime_env()
    install_env["HOME"] = str(fake_home)
    install = subprocess.run(
        [str(root / "scripts/install-agentos.sh"), *install_args],
        text=True,
        cwd=root,
        env=install_env,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if install.returncode != 0:
        fail(
            f"clean-room install exited {install.returncode}\n"
            f"stdout:\n{install.stdout}\nstderr:\n{install.stderr}"
        )

    wrapper = fake_home / ".local/bin/agentos"
    installed_home = fake_home / ".local/share/agentos"
    if not os.access(wrapper, os.X_OK):
        fail(f"installer did not create the documented wrapper: {wrapper}")
    if not (installed_home / "workspace/agent.toml").is_file():
        fail(f"installer did not create the documented runtime home: {installed_home}")
    leaked_state = [
        path
        for path in [
            installed_home / "workspace/agentos.sqlite",
            installed_home / "workspace/main",
            installed_home / "workspace/traces/telegram-gateway.jsonl",
        ]
        if path.exists()
    ]
    if leaked_state:
        fail(f"{label} install copied runtime state: {', '.join(map(str, leaked_state))}")

    config = run_wrapper(wrapper, fake_home, "config")
    expected_config_lines = [
        f"config.path={installed_home / 'workspace/agent.toml'}",
        f"paths.home={installed_home}",
    ]
    if config.returncode != 0 or any(
        line not in config.stdout.splitlines() for line in expected_config_lines
    ):
        fail(
            f"installed `agentos config` contract failed\n"
            f"stdout:\n{config.stdout}\nstderr:\n{config.stderr}"
        )

    status = run_wrapper(wrapper, fake_home, "gateway-status")
    if status.returncode != 0 or status.stdout.strip() != "AgentOS gateway status: stopped":
        fail(
            f"installed `agentos gateway-status` contract failed\n"
            f"stdout:\n{status.stdout}\nstderr:\n{status.stderr}"
        )

    resume = run_wrapper(wrapper, fake_home, "resume")
    default_state = installed_home / "workspace/runs/cli-run-1.json"
    resume_output = f"{resume.stdout}\n{resume.stderr}"
    if resume.returncode == 0 or str(default_state) not in resume_output:
        fail(
            f"pathless resume did not fail deterministically at {default_state}\n"
            f"stdout:\n{resume.stdout}\nstderr:\n{resume.stderr}"
        )
    if "unbound variable" in resume_output:
        fail(f"pathless resume triggered a shell nounset failure: {resume_output}")

    marker = "installed release offline echo smoke"
    echo = run_wrapper(wrapper, fake_home, "tui", input_text=f"{marker}\n/exit\n")
    if echo.returncode != 0 or marker not in echo.stdout:
        fail(
            f"installed offline echo contract failed\n"
            f"stdout:\n{echo.stdout}\nstderr:\n{echo.stderr}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument(
        "--source-root",
        type=Path,
        help="also smoke a source install using its existing release binaries",
    )
    args = parser.parse_args()
    archive = args.archive.resolve()
    if not archive.is_file():
        fail(f"archive not found: {archive}")

    with tempfile.TemporaryDirectory(prefix="agentos-release-check-") as temporary:
        temporary_path = Path(temporary)
        root = extract_archive(archive, temporary_path)
        require_paths(root)
        verify_workspace_references(root)
        verify_document_links(root)
        smoke_offline_echo(root)
        smoke_installed_wrapper(root, temporary_path, "archive")
        if args.source_root is not None:
            source_root = args.source_root.resolve()
            smoke_installed_wrapper(
                source_root,
                temporary_path,
                "source",
                ("--from-source", "--skip-build"),
            )
    print(f"release archive is self-contained: {archive}")


if __name__ == "__main__":
    main()
