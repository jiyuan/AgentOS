#!/usr/bin/env python3
"""tests/gateway_pipeline.py

Automated test pipeline that mimics Telegram and Feishu channel traffic against
the AgentOS gateway. Each cycle spins up local mock servers (HTTP for Telegram
and Feishu, plus a minimal WebSocket server for Feishu long-connection), spawns
``agentos-gateway serve`` pointed at the mocks, dispatches arbitrary test
tasks, captures gateway responses and runtime log errors, and persists a
structured per-task issue map to ``tests/issues.json``.

Run modes:
  --once             Run a single cycle (default).
  --cron             Run cycles every 5 minutes until SIGINT/SIGTERM.
  --interval N       Override cron interval (seconds). Useful for debugging.
  --stop             Send SIGTERM to the running --cron pipeline (uses PID file).
  --tasks PATH       JSON file with a top-level list of task prompts.
  --channels c,c     Subset of channels to exercise (telegram,feishu).
  --gateway-bin PATH Path to the agentos-gateway binary (default: target/debug).

Outputs:
  tests/issues.json   Structured per-task issue map for the latest cycle.
  tests/pipeline.log  Combined diagnostic log for the pipeline driver.
  tests/pipeline.pid  PID file for the --cron loop (used by --stop).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.server
import json
import os
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

REPO = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
ISSUES_PATH = TESTS_DIR / "issues.json"
PID_PATH = TESTS_DIR / "pipeline.pid"
LOG_PATH = TESTS_DIR / "pipeline.log"
DEFAULT_TASKS_PATH = TESTS_DIR / "test_tasks.json"
GATEWAY_LOG = REPO / "tests" / "_artifacts" / "agentos-gateway.log"
GATEWAY_STDIO = REPO / "tests" / "_artifacts" / "agentos-gateway.stdio.log"
GATEWAY_PID = REPO / "tests" / "_artifacts" / "agentos-gateway.pid"
# A session database of the cycle's own, replaced on every run.
#
# Two reasons, and the second is why the fix is a fresh file rather than a
# fixed one. First, without this the pipeline writes into
# `workspace/agentos.sqlite` — the developer's live database. Second, M8 /
# `GW-001` made the Telegram channel persist its `getUpdates` offset as an
# ingress cursor, and the mock restarts its update ids at 1 every cycle. A
# gateway resuming from the previous cycle's offset therefore skipped the first
# few tasks of the next one, which is correct behaviour against a real
# transport (ids are globally increasing) and wrong against a mock that rewinds
# (M9 / `CI-002`).
GATEWAY_DB = REPO / "tests" / "_artifacts" / "agentos-pipeline.sqlite"
ARTIFACTS_DIR = REPO / "tests" / "_artifacts"

DEFAULT_TASKS = [
    "Reply with the literal string OK so we can confirm the gateway round-trip.",
    "List three risks of running an agent gateway behind a corporate proxy.",
    "Summarize what an AgentOS skill is in one sentence.",
]

# Synthetic credentials understood only by the local mock servers.
TELEGRAM_BOT_TOKEN = "TEST-BOT-TOKEN"
TELEGRAM_CHAT_ID = "100200300"
TELEGRAM_USER_ID = "999111222"
FEISHU_APP_ID = "cli_test_app"
FEISHU_APP_SECRET = "test-secret"
FEISHU_CHAT_ID = "oc_test_chat"
FEISHU_SENDER_OPEN_ID = "ou_test_user"

CYCLE_INTERVAL_DEFAULT = 5 * 60  # 5 minutes per spec.

# Whitespace-only prompts are dropped by the gateway's channel parsers and
# never get a reply. Confirm that quickly instead of waiting the full
# per-task timeout (which otherwise wastes minutes per cycle).
BLANK_PROMPT_WAIT = 5.0

# Broad substrings used to flag runtime *log* lines as errors. The gateway
# emits these from its own user_facing_error_message + persistent gateway loop
# helpers. Appropriate for log scanning, too noisy for reply text.
ERROR_PATTERNS = (
    "failed",
    "error",
    "panicked",
    "exited",
    "could not complete",
    "tripped",
    "denied",
)

# Specific markers the gateway puts in a *reply* envelope when a request could
# not be served (see crates/agentos-cli/.../user_facing_error_message and the
# failure_envelope path). Deterministic command output (e.g. `/tools` listing a
# tool whose description contains the word "error") must not false-trip, so the
# reply scan only looks for these exact gateway-emitted phrases.
REPLY_ERROR_MARKERS = (
    "agentos could not complete this request",
    "insufficient_quota",
    "gateway run failed",
    "gateway loop failed",
    "gateway loop panicked",
)


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------

_LOG_LOCK = threading.Lock()


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%S')}] {msg}\n"
    with _LOG_LOCK:
        try:
            LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(line)
        except OSError:
            pass
        sys.stderr.write(line)
        sys.stderr.flush()


# ---------------------------------------------------------------------------
# Telegram mock HTTP server
# ---------------------------------------------------------------------------


class TelegramMock:
    """In-process HTTP server that emulates the subset of the Telegram Bot API
    consumed by the gateway. Inbound messages are queued via ``enqueue_user``;
    outbound replies are captured into ``self.sent``.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._updates: list[dict[str, Any]] = []
        self._next_update_id = 1
        self._reply_events: dict[str, threading.Event] = {}
        # chat_id (== gateway conversation_id) -> task_id. Replies are
        # correlated by conversation, not by an in-band text marker, because
        # the gateway owns the [task:...] prefix namespace (extract_reply_prefix).
        self._task_by_chat: dict[str, str] = {}
        self.sent: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.httpd: socketserver.TCPServer | None = None
        self.thread: threading.Thread | None = None
        self.port = 0

    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        mock = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # quiet
                pass

            def _read_body(self) -> bytes:
                length = int(self.headers.get("Content-Length", "0") or "0")
                return self.rfile.read(length) if length else b""

            def _form(self, body: bytes) -> dict[str, str]:
                if not body:
                    return {}
                if self.headers.get("Content-Type", "").startswith("multipart/"):
                    return _parse_multipart(body, self.headers.get("Content-Type", ""))
                from urllib.parse import parse_qs

                parsed = parse_qs(body.decode("utf-8", errors="replace"))
                return {k: v[0] for k, v in parsed.items() if v}

            def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
                data = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
                path = self.path
                body = self._read_body()
                form = self._form(body)
                # Bot path is /bot{token}/{method}.
                if not path.startswith(f"/bot{TELEGRAM_BOT_TOKEN}/"):
                    self._send_json({"ok": False, "description": "bad token"}, 404)
                    return
                method = path.split("/")[-1]
                if method == "getUpdates":
                    self._send_json(mock._handle_get_updates(form))
                elif method in ("sendMessage", "sendPhoto", "sendDocument"):
                    mock._handle_send(method, form)
                    self._send_json({"ok": True, "result": {"message_id": 1}})
                elif method == "getFile":
                    self._send_json(
                        {
                            "ok": True,
                            "result": {"file_path": f"documents/{form.get('file_id', 'x')}.bin"},
                        }
                    )
                else:
                    self._send_json({"ok": True, "result": {}})

            def do_GET(self) -> None:  # noqa: N802
                if self.path.startswith(f"/file/bot{TELEGRAM_BOT_TOKEN}/"):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(404)
                self.end_headers()

        # Pick an unused localhost port.
        self.httpd = socketserver.TCPServer(("127.0.0.1", 0), Handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, name="telegram-mock", daemon=True
        )
        self.thread.start()
        log(f"telegram mock listening on {self.base_url()}")

    def stop(self) -> None:
        if self.httpd is not None:
            try:
                self.httpd.shutdown()
                self.httpd.server_close()
            except OSError:
                pass

    # Public injection API ---------------------------------------------------
    def enqueue_user(self, text: str, *, task_id: str, chat_id: str | None = None) -> str:
        """Queue an incoming user message that the gateway will fetch via
        getUpdates. ``chat_id`` becomes the gateway's conversation_id; pass a
        unique value per task to keep session/transcript state isolated.
        """
        effective_chat_id = chat_id or TELEGRAM_CHAT_ID
        with self._lock:
            update_id = self._next_update_id
            self._next_update_id += 1
            message_id = update_id
            event = threading.Event()
            self._reply_events[task_id] = event
            self._task_by_chat[str(effective_chat_id)] = task_id
            # Tasks are strictly sequential (enqueue -> wait -> next), so any
            # leftover update is from a prior task. Drop it: a gateway that
            # restarts mid-suite polls getUpdates with no offset and would
            # otherwise replay every past update (e.g. a `/orchestrator min`
            # that re-poisons the fresh process).
            self._updates.clear()
            self._updates.append(
                {
                    "update_id": update_id,
                    "message": {
                        "message_id": message_id,
                        "date": int(time.time()),
                        "chat": {"id": int(effective_chat_id), "type": "private"},
                        "from": {
                            "id": int(TELEGRAM_USER_ID),
                            "is_bot": False,
                            "first_name": "Pipeline",
                            "username": "pipeline_user",
                        },
                        "text": text,
                    },
                }
            )
        return task_id

    def wait_for_reply(self, task_id: str, timeout: float) -> bool:
        event = self._reply_events.get(task_id)
        if event is None:
            return False
        return _stop_aware_wait(event, timeout)

    def reply_text_for(self, task_id: str) -> str | None:
        for item in reversed(self.sent):
            if item.get("task_id") == task_id:
                return item.get("text")
        return None

    # Internal handlers ------------------------------------------------------
    def _handle_get_updates(self, form: dict[str, str]) -> dict[str, Any]:
        offset = int(form.get("offset", "0") or "0")
        with self._lock:
            if offset:
                self._updates = [u for u in self._updates if u["update_id"] >= offset]
            updates = list(self._updates)
        # Long-poll with a short delay if empty, to keep CPU low without
        # blocking the gateway shutdown.
        if not updates:
            time.sleep(0.2)
        return {"ok": True, "result": updates}

    def _handle_send(self, method: str, form: dict[str, str]) -> None:
        text = form.get("text") or form.get("caption") or ""
        chat_id = str(form.get("chat_id", ""))
        with self._lock:
            task_id = self._task_by_chat.get(chat_id)
            item = {"method": method, "form": form, "text": text, "task_id": task_id}
            self.sent.append(item)
            event = self._reply_events.get(task_id) if task_id else None
        if event is not None:
            event.set()


# ---------------------------------------------------------------------------
# Feishu mock servers (HTTP + WebSocket long-connection)
# ---------------------------------------------------------------------------


class FeishuMock:
    """Mocks Feishu Open Platform endpoints used by the gateway:

    - HTTP ``/auth/v3/tenant_access_token/internal`` — returns a fake token.
    - HTTP ``/callback/ws/endpoint`` — returns the URL of our local WS server.
    - HTTP ``/im/v1/messages`` — captures outbound reply payloads.
    - WebSocket — streams Feishu-framed ``im.message.receive_v1`` events.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending_events: list[bytes] = []
        self._reply_events: dict[str, threading.Event] = {}
        # chat_id (== gateway conversation_id / Feishu receive_id) -> task_id.
        self._task_by_chat: dict[str, str] = {}
        self.sent: list[dict[str, Any]] = []
        self.http: socketserver.TCPServer | None = None
        self.http_thread: threading.Thread | None = None
        self.http_port = 0
        self.ws_server: _FeishuWebsocketServer | None = None
        self.ws_thread: threading.Thread | None = None
        self.ws_port = 0

    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.http_port}/open-apis"

    def start(self) -> None:
        self.ws_server = _FeishuWebsocketServer(self)
        self.ws_server.start()
        self.ws_port = self.ws_server.port

        mock = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # quiet
                pass

            def _read_body(self) -> bytes:
                length = int(self.headers.get("Content-Length", "0") or "0")
                return self.rfile.read(length) if length else b""

            def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
                data = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self) -> None:  # noqa: N802
                body = self._read_body()
                payload: dict[str, Any] = {}
                if body and self.headers.get("Content-Type", "").startswith("application/json"):
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except json.JSONDecodeError:
                        payload = {}
                path = self.path.split("?", 1)[0]
                if path.endswith("/auth/v3/tenant_access_token/internal"):
                    self._send_json(
                        {
                            "code": 0,
                            "msg": "ok",
                            "tenant_access_token": "t-test-token",
                            "expire": 7200,
                        }
                    )
                    return
                if path.endswith("/callback/ws/endpoint"):
                    self._send_json(
                        {
                            "code": 0,
                            "msg": "ok",
                            "data": {
                                "URL": f"ws://127.0.0.1:{mock.ws_port}/feishu",
                                "ClientID": "test-client",
                                "ServiceID": "test-service",
                            },
                        }
                    )
                    return
                if path.endswith("/im/v1/messages"):
                    mock._capture_send(payload)
                    self._send_json(
                        {
                            "code": 0,
                            "msg": "ok",
                            "data": {"message_id": f"om_{uuid.uuid4().hex[:12]}"},
                        }
                    )
                    return
                # Anything else (images/files) — accept.
                self._send_json({"code": 0, "msg": "ok", "data": {}})

            def do_GET(self) -> None:  # noqa: N802
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.http = socketserver.TCPServer(("127.0.0.1", 0), Handler)
        self.http_port = self.http.server_address[1]
        self.http_thread = threading.Thread(
            target=self.http.serve_forever, name="feishu-http-mock", daemon=True
        )
        self.http_thread.start()
        log(f"feishu http mock listening on {self.base_url()}")
        log(f"feishu ws mock listening on ws://127.0.0.1:{self.ws_port}/feishu")

    def stop(self) -> None:
        if self.http is not None:
            try:
                self.http.shutdown()
                self.http.server_close()
            except OSError:
                pass
        if self.ws_server is not None:
            self.ws_server.stop()

    def enqueue_user(self, text: str, *, task_id: str, chat_id: str | None = None) -> str:
        message_id = f"om_{uuid.uuid4().hex[:12]}"
        effective_chat_id = chat_id or FEISHU_CHAT_ID
        event_payload = {
            "schema": "2.0",
            "header": {
                "event_id": f"ev_{uuid.uuid4().hex[:12]}",
                "event_type": "im.message.receive_v1",
                "create_time": str(int(time.time() * 1000)),
                "tenant_key": "tenant-test",
                "app_id": FEISHU_APP_ID,
            },
            "event": {
                "sender": {
                    "sender_id": {"open_id": FEISHU_SENDER_OPEN_ID},
                    "sender_type": "user",
                    "tenant_key": "tenant-test",
                },
                "message": {
                    "message_id": message_id,
                    "root_id": message_id,
                    "parent_id": message_id,
                    "create_time": str(int(time.time() * 1000)),
                    "chat_id": effective_chat_id,
                    "chat_type": "p2p",
                    "message_type": "text",
                    "content": json.dumps({"text": text}),
                },
            },
        }
        frame = _encode_feishu_frame(
            method=1,
            headers=[("type", "event"), ("message_id", message_id)],
            payload=json.dumps(event_payload).encode("utf-8"),
        )
        event = threading.Event()
        with self._lock:
            self._reply_events[task_id] = event
            self._task_by_chat[str(effective_chat_id)] = task_id
            # Drop any stale frame from a prior task so a reconnecting gateway
            # (after a restart) doesn't replay an earlier event.
            self._pending_events.clear()
            self._pending_events.append(frame)
        if self.ws_server is not None:
            self.ws_server.notify()
        return task_id

    def wait_for_reply(self, task_id: str, timeout: float) -> bool:
        event = self._reply_events.get(task_id)
        if event is None:
            return False
        return _stop_aware_wait(event, timeout)

    def reply_text_for(self, task_id: str) -> str | None:
        for item in reversed(self.sent):
            if item.get("task_id") == task_id:
                return item.get("text")
        return None

    def pop_pending_frame(self) -> bytes | None:
        with self._lock:
            if self._pending_events:
                return self._pending_events.pop(0)
        return None

    def _capture_send(self, payload: dict[str, Any]) -> None:
        content_raw = payload.get("content")
        text = ""
        if isinstance(content_raw, str):
            try:
                content_json = json.loads(content_raw)
                text = content_json.get("text") or ""
            except json.JSONDecodeError:
                text = content_raw
        receive_id = str(payload.get("receive_id", ""))
        with self._lock:
            task_id = self._task_by_chat.get(receive_id)
            self.sent.append(
                {"payload": payload, "text": text, "task_id": task_id}
            )
            event = self._reply_events.get(task_id) if task_id else None
        if event is not None:
            event.set()


# ---------------------------------------------------------------------------
# Minimal WebSocket server for Feishu long-connection (RFC 6455 binary frames)
# ---------------------------------------------------------------------------


class _FeishuWebsocketServer:
    def __init__(self, mock: "FeishuMock") -> None:
        self._mock = mock
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(("127.0.0.1", 0))
        self._socket.listen(4)
        self.port = self._socket.getsockname()[1]
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._accept_thread: threading.Thread | None = None
        self._client_threads: list[threading.Thread] = []

    def start(self) -> None:
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name="feishu-ws-accept", daemon=True
        )
        self._accept_thread.start()

    def notify(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        try:
            self._socket.close()
        except OSError:
            pass

    def _accept_loop(self) -> None:
        self._socket.settimeout(0.5)
        while not self._stop.is_set():
            try:
                client, _addr = self._socket.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            t = threading.Thread(
                target=self._serve_client,
                args=(client,),
                name="feishu-ws-client",
                daemon=True,
            )
            t.start()
            self._client_threads.append(t)

    def _serve_client(self, client: socket.socket) -> None:
        try:
            client.settimeout(5.0)
            if not _ws_handshake(client):
                client.close()
                return
            client.settimeout(0.5)
            while not self._stop.is_set():
                self._wake.wait(timeout=0.5)
                self._wake.clear()
                while not self._stop.is_set():
                    frame = self._mock.pop_pending_frame()
                    if frame is None:
                        break
                    _ws_send_binary(client, frame)
                # Drain any acks from the gateway side; we don't care about
                # their contents.
                try:
                    while True:
                        opcode, data = _ws_read_frame(client, timeout=0.05)
                        if opcode is None:
                            break
                        if opcode == 0x8:  # close
                            return
                except (OSError, ConnectionError):
                    return
        finally:
            try:
                client.close()
            except OSError:
                pass


def _ws_handshake(sock: socket.socket) -> bool:
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            return False
        buf += chunk
        if len(buf) > 65536:
            return False
    headers: dict[str, str] = {}
    for line in buf.split(b"\r\n")[1:]:
        if not line:
            break
        if b":" in line:
            k, _, v = line.partition(b":")
            headers[k.decode("ascii").strip().lower()] = v.decode("ascii").strip()
    key = headers.get("sec-websocket-key")
    if not key:
        return False
    digest = hashlib.sha1(
        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
    ).digest()
    accept = base64.b64encode(digest).decode("ascii")
    response = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
    )
    sock.sendall(response.encode("ascii"))
    return True


def _ws_send_binary(sock: socket.socket, payload: bytes) -> None:
    header = bytearray([0x82])  # FIN | binary
    length = len(payload)
    if length < 126:
        header.append(length)
    elif length < 65536:
        header.append(126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", length))
    sock.sendall(bytes(header) + payload)


def _ws_read_frame(sock: socket.socket, *, timeout: float) -> tuple[int | None, bytes]:
    sock.settimeout(timeout)
    try:
        b1 = sock.recv(1)
    except socket.timeout:
        return None, b""
    if not b1:
        return None, b""
    opcode = b1[0] & 0x0F
    b2 = sock.recv(1)
    if not b2:
        return opcode, b""
    masked = bool(b2[0] & 0x80)
    length = b2[0] & 0x7F
    if length == 126:
        length = struct.unpack("!H", _recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _recv_exact(sock, 8))[0]
    mask = _recv_exact(sock, 4) if masked else b""
    data = _recv_exact(sock, length) if length else b""
    if masked and data:
        data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    return opcode, data


def _recv_exact(sock: socket.socket, count: int) -> bytes:
    out = bytearray()
    while len(out) < count:
        chunk = sock.recv(count - len(out))
        if not chunk:
            raise ConnectionError("socket closed mid-frame")
        out.extend(chunk)
    return bytes(out)


# ---------------------------------------------------------------------------
# Feishu protobuf-like frame encoder (mirrors crates/agentos-core/.../proto.rs)
# ---------------------------------------------------------------------------


def _encode_feishu_frame(
    *,
    method: int,
    headers: Iterable[tuple[str, str]],
    payload: bytes,
    seq_id: int = 1,
) -> bytes:
    out = bytearray()
    if seq_id:
        _write_varint_field(out, 1, seq_id)
    if method:
        _write_varint_field(out, 4, method)
    for key, value in headers:
        encoded = bytearray()
        _write_bytes_field(encoded, 1, key.encode("utf-8"))
        _write_bytes_field(encoded, 2, value.encode("utf-8"))
        _write_bytes_field(out, 5, bytes(encoded))
    _write_bytes_field(out, 6, b"json")
    _write_bytes_field(out, 7, b"application/json")
    if payload:
        _write_bytes_field(out, 8, payload)
    return bytes(out)


def _write_varint_field(out: bytearray, field: int, value: int) -> None:
    _write_varint(out, field << 3)
    _write_varint(out, value)


def _write_bytes_field(out: bytearray, field: int, value: bytes) -> None:
    _write_varint(out, (field << 3) | 2)
    _write_varint(out, len(value))
    out.extend(value)


def _write_varint(out: bytearray, value: int) -> None:
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value & 0x7F)


# ---------------------------------------------------------------------------
# Task/result helpers
# ---------------------------------------------------------------------------


@dataclass
class TaskSpec:
    prompt: str
    id: str | None = None
    category: str | None = None


@dataclass
class TaskResult:
    task_id: str
    prompt: str
    channel: str
    started_at: str
    finished_at: str
    reply: str | None
    errors: list[str] = field(default_factory=list)
    timed_out: bool = False
    spec_id: str | None = None
    category: str | None = None
    note: str | None = None


# ---------------------------------------------------------------------------
# Gateway lifecycle
# ---------------------------------------------------------------------------


def gateway_binary(custom: str | None) -> Path:
    if custom:
        path = Path(custom)
        if not path.is_absolute():
            path = REPO / path
        return path
    for candidate in ("debug", "release"):
        bin_path = REPO / "target" / candidate / "agentos-gateway"
        if bin_path.is_file():
            return bin_path
    raise FileNotFoundError(
        "agentos-gateway binary not found; build with `cargo build -p agentos-cli`"
    )


def _preflight_binary(binary: Path) -> str | None:
    """Return None if the binary looks runnable on this host, or a human
    explanation of what's wrong otherwise. Catches the common case where the
    binary was built for a different OS/arch (Linux container vs. macOS host).
    """
    if not binary.exists():
        return f"gateway binary not found at {binary}; run `cargo build -p agentos-cli` on this host"
    if not os.access(binary, os.X_OK):
        return f"gateway binary {binary} is not executable; chmod +x or rebuild"
    try:
        proc = subprocess.run(
            [str(binary), "--help"],
            cwd=str(REPO),
            capture_output=True,
            timeout=5,
        )
    except OSError as err:
        if err.errno == 8:  # ENOEXEC
            return (
                f"gateway binary {binary} was built for a different OS/arch "
                f"(Exec format error). Rebuild on this host: `cargo build -p agentos-cli`."
            )
        return f"failed to probe gateway binary {binary}: {err}"
    except subprocess.TimeoutExpired:
        # --help should return instantly; a hang here usually means the binary
        # is wedged, not a platform mismatch.
        return f"gateway binary {binary} hung on --help; rebuild or inspect manually"
    if proc.returncode not in (0, 1, 2):
        return (
            f"gateway binary {binary} exited {proc.returncode} on --help; "
            f"stderr={proc.stderr.decode('utf-8', errors='replace')[:200]}"
        )
    return None


def _build_gateway_env(
    telegram: TelegramMock, feishu: FeishuMock
) -> dict[str, str]:
    env = os.environ.copy()
    # Every task uses a unique chat_id so it gets a fresh conversation, which
    # means the gateway has to accept chats it was not told about in advance.
    #
    # Blanking AGENTOS_TELEGRAM_CHAT_ID used to be how that was asked for, and
    # M3 / `AUTH-001` reversed it: an empty allowlist now admits *nothing*,
    # because "unset means open" is how a deployment that forgot the variable
    # ended up exposed to anyone who could find the bot. The pipeline was
    # invoked by nothing, so it went on asking the old way and every task
    # timed out at 90 seconds against a channel loop that had already failed
    # to start (M9 / `CI-002`). ALLOW_ALL is the explicit form of the same
    # request. It is set (not just popped) because AGENTOS_NO_ENV_OVERRIDE=1
    # below would otherwise let the on-disk .env re-introduce a real
    # allowlist, which would make the gateway silently drop every test
    # message.
    env.pop("AGENTOS_TELEGRAM_CHAT_ID", None)
    env.update(
        {
            "AGENTOS_TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
            "AGENTOS_TELEGRAM_CHAT_ID": "",
            "AGENTOS_TELEGRAM_ALLOW_ALL": "1",
            "AGENTOS_TELEGRAM_API_BASE": telegram.base_url(),
            "AGENTOS_TELEGRAM_FILE_BASE": telegram.base_url(),
            "AGENTOS_FEISHU_APP_ID": FEISHU_APP_ID,
            "AGENTOS_FEISHU_APP_SECRET": FEISHU_APP_SECRET,
            "AGENTOS_FEISHU_ALLOWED_ID": FEISHU_SENDER_OPEN_ID,
            "AGENTOS_FEISHU_ALLOW_ALL": "1",
            "AGENTOS_FEISHU_RECEIVE_ID_TYPE": "chat_id",
            "AGENTOS_FEISHU_API_BASE": feishu.base_url(),
            "AGENTOS_ENABLED_CHANNELS": "telegram,feishu",
            "AGENTOS_GATEWAY_PID_PATH": str(GATEWAY_PID),
            "AGENTOS_GATEWAY_LOG_PATH": str(GATEWAY_LOG),
            # The persistent gateway requires a writable owner token slot. The
            # `serve` subcommand reads this when pid-file ownership is checked.
            "AGENTOS_GATEWAY_OWNER_TOKEN": f"pipeline-{os.getpid()}-{int(time.time())}",
            # Hermetic LLM: this pipeline mocks Telegram/Feishu but NOT any LLM
            # provider, and `_classify_reply` is written against the
            # builtin.echo orchestrator (which echoes the prompt verbatim).
            # workspace/agent.toml pins orchestrator = "builtin.max"; with no
            # usable LLM selection the Max orchestrator deterministically
            # echoes instead of calling a provider, which is exactly the
            # behaviour the harness expects. Force that here:
            #   * AGENTOS_NO_ENV_OVERRIDE=1 makes these explicit values win
            #     over the on-disk .env (the gateway loads .env at startup and,
            #     by default, .env *overrides* the process environment).
            #   * AGENTOS_LLM_PROVIDER=builtin.echo yields no LLM selection.
            #   * Blanking the keys/models stops .env from re-introducing a
            #     live provider that would hang on a network-less test host.
            "AGENTOS_NO_ENV_OVERRIDE": "1",
            "AGENTOS_LLM_PROVIDER": "builtin.echo",
            "AGENTOS_LLM_MODEL": "",
            "AGENTOS_LLM_MODEL_HIGH": "",
            "AGENTOS_LLM_MODEL_MEDIUM": "",
            "AGENTOS_LLM_MODEL_LOW": "",
            "OPENAI_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
            "DEEPSEEK_API_KEY": "",
            "OLLAMA_HOST": "",
        }
    )
    return env


def _start_gateway(
    binary: Path,
    env: dict[str, str],
) -> subprocess.Popen[bytes]:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    GATEWAY_LOG.touch(exist_ok=True)
    GATEWAY_PID.unlink(missing_ok=True)
    for suffix in ("", "-wal", "-shm"):
        Path(f"{GATEWAY_DB}{suffix}").unlink(missing_ok=True)
    # Pre-populate the pid file so the gateway accepts ownership immediately.
    GATEWAY_PID.write_text(f"0 {env['AGENTOS_GATEWAY_OWNER_TOKEN']}\n", encoding="utf-8")
    cmd = [
        str(binary),
        "serve",
        "--pid-path",
        str(GATEWAY_PID),
        "--log-path",
        str(GATEWAY_LOG),
        "--session-db-path",
        str(GATEWAY_DB),
    ]
    log(f"starting gateway: {' '.join(cmd)}")
    # Redirect the gateway's stdout/stderr (eprintln! channel diagnostics,
    # tracing output) to a file. Leaving it on an unread PIPE both hides the
    # diagnostics and risks stalling the gateway once the 64KB pipe buffer
    # fills under a long task sweep.
    stdio = open(GATEWAY_STDIO, "ab", buffering=0)  # noqa: SIM115 (closed by caller)
    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO),
        env=env,
        stdout=stdio,
        stderr=subprocess.STDOUT,
    )
    proc._pipeline_stdio = stdio  # type: ignore[attr-defined]
    # The serve subcommand expects its own pid in the pid file before it
    # starts running. Write the real pid in the same format.
    GATEWAY_PID.write_text(
        f"{proc.pid} {env['AGENTOS_GATEWAY_OWNER_TOKEN']}\n", encoding="utf-8"
    )
    return proc


def _wait_for_gateway_ready(
    proc: subprocess.Popen[bytes], timeout: float, log_offset: int = 0
) -> bool:
    """Wait until the gateway logs a readiness marker *after* ``log_offset``.

    The gateway log is append-only and survives restarts, so scanning from
    byte 0 would match a previous boot's "telegram channel enabled" line and
    declare a freshly-restarted gateway ready before it actually is. Callers
    pass the log size captured just before spawning the new process.
    """
    deadline = time.monotonic() + timeout
    last_size = log_offset
    while time.monotonic() < deadline and not _STOP.is_set():
        if proc.poll() is not None:
            log("gateway exited during startup")
            return False
        if GATEWAY_LOG.exists():
            try:
                with GATEWAY_LOG.open("rb") as fh:
                    fh.seek(last_size)
                    chunk = fh.read()
                    last_size = fh.tell()
            except OSError:
                chunk = b""
            text = chunk.decode("utf-8", errors="replace")
            if "gateway loop started" in text or "telegram channel enabled" in text:
                return True
        time.sleep(0.25)
    return False


def _stop_gateway(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _gateway_log_errors(start_size: int) -> list[str]:
    if not GATEWAY_LOG.exists():
        return []
    try:
        with GATEWAY_LOG.open("rb") as fh:
            fh.seek(start_size)
            blob = fh.read()
    except OSError as err:
        return [f"gateway log unreadable: {err}"]
    text = blob.decode("utf-8", errors="replace")
    errors = []
    for line in text.splitlines():
        lower = line.lower()
        if any(pattern in lower for pattern in ERROR_PATTERNS):
            errors.append(line.strip())
    return errors


# ---------------------------------------------------------------------------
# Cycle execution
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _classify_reply(reply: str | None, prompt: str) -> list[str]:
    if reply is None:
        return ["no reply received from gateway"]
    # The builtin.echo orchestrator echoes the prompt verbatim, so a prompt
    # that mentions "error"/"failed" would self-trip the keyword scan. Only
    # treat the *residual* (reply minus the echoed prompt) as signal.
    residual = reply
    p = prompt.strip()
    if p and p.lower() in residual.lower():
        idx = residual.lower().index(p.lower())
        residual = (residual[:idx] + residual[idx + len(p):]).strip()
    lower = residual.lower()
    matches = [marker for marker in REPLY_ERROR_MARKERS if marker in lower]
    if not matches:
        return []
    return [f"reply contains gateway failure marker: {reply.strip()[:240]}"]


def _global_state_restore_commands(prompt: str) -> list[str]:
    """If `prompt` is a slash command that mutates process-global gateway
    state (orchestrator strategy / model selection — shared across every
    channel and conversation), return the command(s) that restore the
    workspace defaults. Empty list means the prompt is state-neutral.

    Without this, a command like `/orchestrator min` (which switches to an
    orchestrator that needs a real LLM) would make the builtin.echo test
    gateway fail every subsequent task for the rest of its lifetime. Sending
    the inverse command in-place is deterministic and race-free, unlike
    restarting the shared gateway process mid-sweep.
    """
    p = prompt.strip().lower()
    restores: list[str] = []
    if p == "/orchestrator" or p.startswith("/orchestrator "):
        arg = p[len("/orchestrator"):].strip()
        if arg and arg != "status":
            # workspace/agent.toml pins orchestrator = "builtin.max".
            restores.append("/orchestrator max")
    if p == "/model" or p.startswith("/model "):
        arg = p[len("/model"):].strip()
        if arg and arg != "status":
            restores.append("/model reset")
    return restores


@dataclass
class GatewayHandle:
    """Carries gateway lifecycle context (currently just the running proc) so
    helpers can reference the active process.
    """

    binary: Path
    env: dict[str, str]
    startup_timeout: float
    proc: subprocess.Popen[bytes]


def _per_task_chat_id(channel: str, index: int) -> str:
    """Stable per-task conversation id. Each task gets a fresh session so the
    35 prompts don't all pile up in the same transcript.
    """
    if channel == "telegram":
        # Telegram chat IDs are integers; allocate from a high range that
        # won't collide with anything the gateway might persist for the
        # AGENTOS_TELEGRAM_CHAT_ID default.
        return str(900_000_000 + index)
    return f"oc_test_chat_{index:04d}"


def _run_channel_tasks(
    channel: str,
    mock: TelegramMock | FeishuMock,
    tasks: list[TaskSpec],
    timeout: float,
    log_start_size: int,
    gateway: "GatewayHandle | None" = None,
) -> list[TaskResult]:
    results: list[TaskResult] = []
    total = len(tasks)
    for index, spec in enumerate(tasks, start=1):
        if _STOP.is_set():
            log(f"  [{channel}] stop requested; skipping remaining {total - index + 1} task(s)")
            break
        task_id = uuid.uuid4().hex[:10]
        chat_id = _per_task_chat_id(channel, index)
        label = spec.id or task_id
        log(f"  [{channel}] task {index}/{total} ({label}) -> chat={chat_id}")
        task_log_start = GATEWAY_LOG.stat().st_size if GATEWAY_LOG.exists() else log_start_size
        started = _now_iso()
        # Whitespace-only input is intentionally suppressed by both channel
        # parsers (Telegram skips empty content; Feishu parse_event rejects
        # empty text), so the gateway never replies. Don't burn the full
        # per-task timeout proving that — use a short confirmation wait and
        # record it as an expected outcome, not an error/timeout.
        blank_prompt = spec.prompt.strip() == ""
        wait_budget = min(timeout, BLANK_PROMPT_WAIT) if blank_prompt else timeout
        mock.enqueue_user(spec.prompt, task_id=task_id, chat_id=chat_id)
        replied = mock.wait_for_reply(task_id, timeout=wait_budget)
        finished = _now_iso()
        reply = mock.reply_text_for(task_id)
        note: str | None = None
        errors = _classify_reply(reply, spec.prompt)
        if blank_prompt and not replied and reply is None:
            # Expected: gateway suppressed empty input. Not an error.
            errors = []
            note = (
                "expected: gateway suppresses whitespace-only input "
                f"(no reply within {wait_budget:.0f}s)"
            )
        elif not replied and reply is None:
            errors.append(f"timed out waiting for reply after {wait_budget:.0f}s")
        # Per-task runtime log slice. Each task gets its own window so errors
        # are attributed to the actual task that caused them rather than the
        # last one in the channel sweep.
        runtime_errors = _gateway_log_errors(task_log_start)
        if runtime_errors:
            errors.extend(f"runtime log: {line}" for line in runtime_errors)
        results.append(
            TaskResult(
                task_id=task_id,
                prompt=spec.prompt,
                channel=channel,
                started_at=started,
                finished_at=finished,
                reply=reply,
                errors=errors,
                timed_out=(not replied) and not blank_prompt,
                spec_id=spec.id,
                category=spec.category,
                note=note,
            )
        )
        # A slash command that mutated process-global state would break every
        # subsequent task in this shared gateway. Restore defaults in-place by
        # sending the inverse command(s) and waiting for their acks.
        if not _STOP.is_set():
            restore_cmds = _global_state_restore_commands(spec.prompt)
            for r, restore_cmd in enumerate(restore_cmds):
                restore_task = uuid.uuid4().hex[:10]
                # Keep restore conversations off the per-task id space but
                # channel-shaped (Telegram chat ids must be integers).
                if channel == "telegram":
                    restore_chat = str(800_000_000 + index * 10 + r)
                else:
                    restore_chat = f"{chat_id}-restore-{r}"
                log(f"  [{channel}] restoring global state via {restore_cmd!r}")
                mock.enqueue_user(restore_cmd, task_id=restore_task, chat_id=restore_chat)
                if not mock.wait_for_reply(restore_task, timeout=timeout):
                    log(f"  [{channel}] WARNING: restore {restore_cmd!r} got no ack")
    return results


def _cycle_failure_result(reason: str) -> TaskResult:
    """A single placeholder result describing why no tasks ran this cycle.
    Used in place of N_tasks × N_channels phantom timeouts when the gateway
    cannot start (e.g. binary built for a different platform).
    """
    now = _now_iso()
    return TaskResult(
        task_id="cycle-failure",
        prompt="(no tasks dispatched)",
        channel="-",
        started_at=now,
        finished_at=now,
        reply=None,
        errors=[reason],
        timed_out=False,
        spec_id="cycle-failure",
        category="pipeline-error",
    )


def run_cycle(
    *,
    tasks: list[TaskSpec],
    channels: list[str],
    gateway_bin: Path,
    per_task_timeout: float,
    startup_timeout: float,
) -> list[TaskResult]:
    preflight_err = _preflight_binary(gateway_bin)
    if preflight_err is not None:
        log(f"gateway preflight failed: {preflight_err}")
        return [_cycle_failure_result(preflight_err)]

    results: list[TaskResult] = []
    telegram_mock = TelegramMock()
    feishu_mock = FeishuMock()
    telegram_mock.start()
    feishu_mock.start()
    env = _build_gateway_env(telegram_mock, feishu_mock)
    try:
        proc = _start_gateway(gateway_bin, env)
    except Exception as err:  # noqa: BLE001
        log(f"failed to start gateway: {err}")
        telegram_mock.stop()
        feishu_mock.stop()
        return [_cycle_failure_result(f"gateway failed to start: {err}")]
    _register_proc(proc)
    gateway = GatewayHandle(
        binary=gateway_bin,
        env=env,
        startup_timeout=startup_timeout,
        proc=proc,
    )
    try:
        ready = _wait_for_gateway_ready(gateway.proc, timeout=startup_timeout)
        if not ready:
            log("gateway did not signal readiness; continuing optimistically")
        # Record where each per-channel run starts in the gateway log so we
        # can attribute runtime errors back to the channel sweep.
        log_size_before = GATEWAY_LOG.stat().st_size if GATEWAY_LOG.exists() else 0
        for channel in channels:
            if _STOP.is_set():
                log(f"stop requested; skipping remaining channels")
                break
            mock: TelegramMock | FeishuMock
            mock = telegram_mock if channel == "telegram" else feishu_mock
            log(f"dispatching {len(tasks)} task(s) on channel={channel}")
            channel_results = _run_channel_tasks(
                channel,
                mock,
                tasks,
                timeout=per_task_timeout,
                log_start_size=log_size_before,
                gateway=gateway,
            )
            results.extend(channel_results)
            log_size_before = GATEWAY_LOG.stat().st_size if GATEWAY_LOG.exists() else 0
    finally:
        _stop_gateway(gateway.proc)
        _register_proc(None)
        telegram_mock.stop()
        feishu_mock.stop()
    return results


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def write_issues(results: list[TaskResult], *, cycle_id: str, cycle_started: str) -> None:
    ISSUES_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "cycle_id": cycle_id,
        "cycle_started_at": cycle_started,
        "cycle_finished_at": _now_iso(),
        "tasks": [
            {
                "task_id": r.task_id,
                "spec_id": r.spec_id,
                "category": r.category,
                "channel": r.channel,
                "prompt": r.prompt,
                "reply": r.reply,
                "timed_out": r.timed_out,
                "errors": r.errors,
                "note": r.note,
                "started_at": r.started_at,
                "finished_at": r.finished_at,
            }
            for r in results
        ],
    }
    ISSUES_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"wrote {ISSUES_PATH} with {len(results)} task entries")


def load_tasks(path: Path | None) -> list[TaskSpec]:
    target = path or DEFAULT_TASKS_PATH
    if not target.exists():
        return [TaskSpec(prompt=p) for p in DEFAULT_TASKS]
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        log(f"invalid {target}: {err}; falling back to defaults")
        return [TaskSpec(prompt=p) for p in DEFAULT_TASKS]
    raw_tasks: list[Any]
    if isinstance(data, list):
        raw_tasks = data
    elif isinstance(data, dict) and isinstance(data.get("tasks"), list):
        raw_tasks = data["tasks"]
    else:
        log(f"{target} has unexpected shape; falling back to defaults")
        return [TaskSpec(prompt=p) for p in DEFAULT_TASKS]
    specs: list[TaskSpec] = []
    for entry in raw_tasks:
        if isinstance(entry, str):
            specs.append(TaskSpec(prompt=entry))
        elif isinstance(entry, dict):
            prompt = entry.get("prompt")
            if not isinstance(prompt, str):
                continue
            specs.append(
                TaskSpec(
                    prompt=prompt,
                    id=entry.get("id") if isinstance(entry.get("id"), str) else None,
                    category=entry.get("category") if isinstance(entry.get("category"), str) else None,
                )
            )
    if not specs:
        log(f"{target} contained no usable prompts; falling back to defaults")
        return [TaskSpec(prompt=p) for p in DEFAULT_TASKS]
    return specs


# ---------------------------------------------------------------------------
# Termination & multipart helper
# ---------------------------------------------------------------------------


def _parse_multipart(body: bytes, content_type: str) -> dict[str, str]:
    boundary = ""
    for part in content_type.split(";"):
        part = part.strip()
        if part.startswith("boundary="):
            boundary = part[len("boundary=") :].strip('"')
            break
    if not boundary:
        return {}
    delim = b"--" + boundary.encode("ascii")
    out: dict[str, str] = {}
    for section in body.split(delim):
        section = section.strip(b"\r\n-")
        if not section:
            continue
        head, _, payload = section.partition(b"\r\n\r\n")
        headers = head.decode("utf-8", errors="replace")
        if "name=" not in headers:
            continue
        name = headers.split("name=", 1)[1].split(";", 1)[0].split("\r\n", 1)[0]
        name = name.strip().strip('"')
        out[name] = payload.rstrip(b"\r\n").decode("utf-8", errors="replace")
    return out


def _existing_pid() -> int | None:
    if not PID_PATH.exists():
        return None
    try:
        pid = int(PID_PATH.read_text(encoding="utf-8").strip().split()[0])
    except (OSError, ValueError):
        return None
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return None
    return pid


def _write_pid() -> None:
    PID_PATH.write_text(f"{os.getpid()}\n", encoding="utf-8")


def _remove_pid() -> None:
    PID_PATH.unlink(missing_ok=True)


_STOP = threading.Event()
_CURRENT_PROC: subprocess.Popen[bytes] | None = None
_PROC_LOCK = threading.Lock()


def _register_proc(proc: subprocess.Popen[bytes] | None) -> None:
    """Track the gateway subprocess so the signal handler can kill it
    immediately. Pass ``None`` once the proc has been reaped.
    """
    global _CURRENT_PROC
    with _PROC_LOCK:
        _CURRENT_PROC = proc


def _stop_aware_wait(event: threading.Event, timeout: float) -> bool:
    """Wait for ``event`` up to ``timeout`` seconds, returning early if the
    pipeline-level ``_STOP`` event is set. Keeps Ctrl-C / SIGTERM responsive
    inside long per-task waits.
    """
    deadline = time.monotonic() + max(0.0, timeout)
    while not _STOP.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if event.wait(timeout=min(0.25, remaining)):
            return True
    return False


def _install_signal_handlers() -> None:
    def handler(signum: int, _frame: Any) -> None:
        log(f"received signal {signum}; aborting current cycle")
        _STOP.set()
        with _PROC_LOCK:
            proc = _CURRENT_PROC
        if proc is not None and proc.poll() is None:
            try:
                proc.send_signal(signal.SIGTERM)
            except OSError:
                pass

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    parser.add_argument("--cron", action="store_true", help="loop on a 5-minute schedule")
    parser.add_argument("--interval", type=float, default=CYCLE_INTERVAL_DEFAULT, help="cron interval seconds")
    parser.add_argument("--stop", action="store_true", help="terminate the running cron pipeline")
    parser.add_argument("--tasks", type=Path, default=None, help="JSON file of task prompts")
    parser.add_argument(
        "--channels",
        type=str,
        default="telegram,feishu",
        help="comma-separated channel subset",
    )
    parser.add_argument("--gateway-bin", type=str, default=None, help="path to agentos-gateway")
    parser.add_argument(
        "--per-task-timeout",
        type=float,
        default=90.0,
        help="seconds to wait for each gateway reply",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=20.0,
        help="seconds to wait for gateway readiness",
    )
    args = parser.parse_args(argv)

    if args.stop:
        pid = _existing_pid()
        if pid is None:
            log("no running pipeline found")
            return 0
        log(f"sending SIGTERM to pipeline pid {pid}")
        os.kill(pid, signal.SIGTERM)
        return 0

    if args.once and args.cron:
        parser.error("--once and --cron are mutually exclusive")

    channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    for c in channels:
        if c not in ("telegram", "feishu"):
            parser.error(f"unsupported channel: {c}")

    tasks = load_tasks(args.tasks)
    if not tasks:
        log("no tasks to run")
        return 1

    try:
        binary = gateway_binary(args.gateway_bin)
    except FileNotFoundError as err:
        log(str(err))
        return 1

    _install_signal_handlers()

    def one_cycle() -> None:
        cycle_id = uuid.uuid4().hex[:12]
        cycle_started = _now_iso()
        log(f"cycle {cycle_id} starting at {cycle_started}")
        results = run_cycle(
            tasks=tasks,
            channels=channels,
            gateway_bin=binary,
            per_task_timeout=args.per_task_timeout,
            startup_timeout=args.startup_timeout,
        )
        write_issues(results, cycle_id=cycle_id, cycle_started=cycle_started)

    if args.cron:
        existing = _existing_pid()
        if existing:
            log(f"another pipeline is already running (pid {existing}); abort")
            return 1
        _write_pid()
        try:
            while not _STOP.is_set():
                one_cycle()
                if _STOP.is_set():
                    break
                # Sleep in small chunks so signals interrupt promptly.
                slept = 0.0
                while slept < args.interval and not _STOP.is_set():
                    time.sleep(min(1.0, args.interval - slept))
                    slept += 1.0
        finally:
            _remove_pid()
        return 0

    # Default: --once
    one_cycle()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
