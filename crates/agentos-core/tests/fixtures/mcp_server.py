#!/usr/bin/env python3
"""An MCP server written against the specification, not against AgentOS.

M8 / `MCP-001`, deliverable 9. The point of this file is what it does *not*
import: no agentos types, no shared schema, no knowledge of how the client is
implemented. It reads the Model Context Protocol's stdio transport —
newline-delimited JSON-RPC 2.0 — and answers `initialize`, `tools/list` with
`nextCursor`, and `tools/call` with content blocks. If the client can talk to
this, the client is speaking MCP; the old one could not, because it sent a
serialized AgentOS `ToolCall` and expected a serialized `ToolResult` back.

The modes exist so the failure paths are testable. A protocol client is mostly
its behaviour when the peer misbehaves, and none of that had a test before.

Usage: mcp_server.py [MODE]

  well-behaved   the happy path (default)
  out-of-order   answers two in-flight calls newest-first
  noisy          emits a junk line and an unsolicited notification per reply
  crash          exits after the first tools/call
  bad-version    answers initialize with a protocol revision from 1999
  no-tools       answers initialize without a tools capability
  oversized      answers tools/call with a line past any sane frame limit
  silent         accepts tools/call and never answers it
  demanding      sends the client a request it does not implement
  cancellation-storm records cancellation notices and sends late replies
  oversized-tools returns more tools than the cumulative catalog budget
  process-lifecycle forks a descendant and leaves tool calls active
  stderr-flood    emits bounded-diagnostic stress without a newline
"""

import json
import os
import signal
import subprocess
import sys
import time

PROTOCOL_VERSION = "2025-06-18"

MODE = sys.argv[1] if len(sys.argv) > 1 else "well-behaved"
MODE_ARG = sys.argv[2] if len(sys.argv) > 2 else None

# Three pages, so the client has to follow `nextCursor` twice to see them all.
PAGES = {
    None: (
        [
            {
                "name": "echo",
                "description": "Return the text it was given.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                },
            }
        ],
        "page-2",
    ),
    "page-2": (
        [
            {
                "name": "picture",
                "description": "Return an image block.",
                "inputSchema": {"type": "object", "properties": {}},
            }
        ],
        "page-3",
    ),
    "page-3": (
        [
            {
                "name": "explode",
                "description": "Return isError.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "write_file",
                "description": "Write a file at an absolute path.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                },
            },
        ],
        None,
    ),
}


def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def note(message):
    sys.stderr.write(message + "\n")
    sys.stderr.flush()


def result(request_id, body):
    return {"jsonrpc": "2.0", "id": request_id, "result": body}


def call_result(name, arguments):
    if name == "echo":
        return {"content": [{"type": "text", "text": arguments.get("text", "")}]}
    if name == "picture":
        return {
            "content": [
                {"type": "text", "text": "here it is"},
                {"type": "image", "mimeType": "image/png", "data": "iVBORw0KGgo="},
            ]
        }
    if name == "explode":
        return {
            "content": [{"type": "text", "text": "the tool failed on purpose"}],
            "isError": True,
        }
    if name == "write_file":
        path = arguments.get("path", "")
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("the sandbox did not stop me\n")
            return {"content": [{"type": "text", "text": f"wrote {path}"}]}
        except OSError as err:
            return {
                "content": [{"type": "text", "text": f"refused: {err}"}],
                "isError": True,
            }
    return {
        "content": [{"type": "text", "text": f"no such tool: {name}"}],
        "isError": True,
    }


def main():
    if MODE == "stderr-then-exit":
        note("configuration error: MCP_TOKEN is not set")
        sys.exit(3)
    if MODE == "stderr-flood":
        sys.stderr.write("x" * (2 * 1024 * 1024) + "stderr-flood-tail")
        sys.stderr.flush()
        sys.exit(3)

    if MODE == "process-lifecycle":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        descendant = subprocess.Popen(["/bin/sleep", "300"])
        with open(MODE_ARG, "w", encoding="utf-8") as handle:
            handle.write(f"{os.getpid()} {descendant.pid}\n")

    deferred = []
    abandoned = {}
    calls_served = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        message = json.loads(line)
        method = message.get("method")
        request_id = message.get("id")

        if method is None:
            # A response to something we asked. Only `demanding` mode asks.
            continue
        if method.startswith("notifications/"):
            if method == "notifications/cancelled":
                cancelled_id = message.get("params", {}).get("requestId")
                note(f"cancelled {cancelled_id}")
                if MODE == "cancellation-storm":
                    with open(MODE_ARG, "a", encoding="utf-8") as handle:
                        handle.write(f"{cancelled_id}\n")
                    pending = abandoned.pop(cancelled_id, None)
                    if pending is not None:
                        name, arguments = pending
                        send(result(cancelled_id, call_result(name, arguments)))
            continue

        if method == "initialize":
            if MODE == "bad-version":
                send(result(request_id, {"protocolVersion": "1999-01-01",
                                         "capabilities": {"tools": {}}}))
                continue
            if MODE == "no-tools":
                send(result(request_id, {"protocolVersion": PROTOCOL_VERSION,
                                         "capabilities": {}}))
                continue
            send(
                result(
                    request_id,
                    {
                        "protocolVersion": PROTOCOL_VERSION,
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {"name": "interop-fixture", "version": "1.0.0"},
                    },
                )
            )
            if MODE == "demanding":
                # A request the client never claimed the capability for. It
                # must be answered rather than left hanging, or this server
                # would block here forever.
                send({"jsonrpc": "2.0", "id": "srv-1", "method": "sampling/createMessage"})
            continue

        if method == "tools/list":
            if MODE == "oversized-tools":
                tools = [{"name": f"tool-{index}"} for index in range(257)]
                send(result(request_id, {"tools": tools}))
                continue
            cursor = (message.get("params") or {}).get("cursor")
            tools, next_cursor = PAGES.get(cursor, ([], None))
            body = {"tools": tools}
            if next_cursor:
                body["nextCursor"] = next_cursor
            send(result(request_id, body))
            continue

        if method == "tools/call":
            params = message.get("params") or {}
            name = params.get("name")
            arguments = params.get("arguments") or {}
            calls_served += 1

            if MODE == "silent":
                continue
            if MODE == "process-lifecycle":
                continue
            if MODE == "cancellation-storm" and arguments.get("text") != "normal":
                abandoned[request_id] = (name, arguments)
                continue
            if MODE == "crash":
                note("crashing after one call, as asked")
                sys.exit(1)
            if MODE == "oversized":
                huge = "x" * (8 * 1024 * 1024)
                send(result(request_id, {"content": [{"type": "text", "text": huge}]}))
                continue
            if MODE == "noisy":
                sys.stdout.write("this line is not JSON at all\n")
                send({"jsonrpc": "2.0", "method": "notifications/message",
                      "params": {"level": "info", "data": "unsolicited"}})
                # A reply to an id nobody is waiting on. Under the old client
                # this became the answer to the *next* question.
                send(result(999999, {"content": [{"type": "text", "text": "stray"}]}))
            if MODE == "out-of-order":
                deferred.append((request_id, name, arguments))
                if len(deferred) < 2:
                    continue
                for pending_id, pending_name, pending_args in reversed(deferred):
                    send(result(pending_id, call_result(pending_name, pending_args)))
                deferred = []
                continue

            send(result(request_id, call_result(name, arguments)))
            continue

        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"no such method: {method}"},
            }
        )


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
