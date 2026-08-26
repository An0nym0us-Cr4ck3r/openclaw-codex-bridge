"""Minimal WebSocket client for Codex app-server.sock (UDS + WS handshake)."""
from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
from typing import Any


class AppServerError(RuntimeError):
    pass


class WSClient:
    """Single WS owner. Call connect() once, then request()/run_turn()."""

    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._buf = b""
        self._next_id = 1
        self._recv_lock = asyncio.Lock()
        # pending request futures keyed by id
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._bg_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        self.reader, self.writer = await asyncio.open_unix_connection(self.socket_path)
        key = base64.b64encode(os.urandom(16)).decode()
        handshake = (
            "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
        ).encode()
        self.writer.write(handshake)
        await self.writer.drain()
        # read HTTP 101
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = await self.reader.read(4096)
            if not chunk:
                raise AppServerError("handshake closed")
            resp += chunk
        if not resp.startswith(b"HTTP/1.1 101"):
            raise AppServerError(resp.split(b"\r\n", 1)[0].decode(errors="replace"))
        # leftover after header is first WS bytes
        self._buf = resp.split(b"\r\n\r\n", 1)[1]
        self._bg_task = asyncio.create_task(self._recv_loop())
        # initialize
        await self.request("initialize", {"clientInfo": {"name": "codex-bridge-daemon", "version": "0.2.0"}, "capabilities": {"experimentalApi": True}})
        await self._send_obj({"method": "initialized", "params": {}})

    async def close(self) -> None:
        if self._bg_task is not None:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
            self._bg_task = None
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        self.writer = None
        self.reader = None

    # ---- low-level WS framing ----

    async def _read_exact(self, n: int) -> bytes:
        out = bytearray()
        while len(out) < n:
            if self._buf:
                take = min(n - len(out), len(self._buf))
                out += self._buf[:take]
                self._buf = self._buf[take:]
            else:
                assert self.reader is not None
                chunk = await self.reader.read(n - len(out))
                if not chunk:
                    raise AppServerError("socket closed")
                out += chunk
        return bytes(out)

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        assert self.writer is not None
        mask = os.urandom(4)
        size = len(payload)
        if size < 126:
            header = bytes([0x80 | opcode, 0x80 | size])
        elif size < 65536:
            header = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack(">H", size)
        else:
            header = bytes([0x80 | opcode, 0x80 | 127]) + struct.pack(">Q", size)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.writer.write(header + mask + masked)
        await self.writer.drain()

    async def _send_obj(self, obj: dict[str, Any]) -> None:
        await self._send_frame(0x1, json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode())

    async def _recv_loop(self) -> None:
        try:
            while True:
                first, second = await self._read_exact(2)
                opcode = first & 0x0F
                size = second & 0x7F
                if size == 126:
                    size = struct.unpack(">H", await self._read_exact(2))[0]
                elif size == 127:
                    size = struct.unpack(">Q", await self._read_exact(8))[0]
                masked = bool(second & 0x80)
                mask = await self._read_exact(4) if masked else None
                payload = await self._read_exact(size)
                if mask is not None:
                    payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
                if opcode == 0x9:
                    await self._send_frame(0xA, payload)
                    continue
                if opcode == 0x8:
                    raise AppServerError("app-server sent close")
                if opcode != 0x1:
                    continue
                msg = json.loads(payload.decode("utf-8"))
                mid = msg.get("id")
                if isinstance(mid, int) and mid in self._pending:
                    fut = self._pending.pop(mid)
                    if not fut.done():
                        if "error" in msg:
                            fut.set_exception(AppServerError(f"{msg['error']}"))
                        else:
                            fut.set_result(msg.get("result") or {})
                else:
                    # push to event queue
                    await self._event_q.put(msg)  # type: ignore[attr-defined]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # fail all pending
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(exc)
            self._pending.clear()
            # put error sentinel to event queue so run_turn can surface
            try:
                await self._event_q.put({"_error": str(exc)})  # type: ignore[attr-defined]
            except Exception:
                pass

    _event_q: asyncio.Queue[dict[str, Any]]  # set in __init__-like place

    def _ensure_event_q(self) -> None:
        if not hasattr(self, "_event_q"):
            self._event_q = asyncio.Queue()

    async def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self._ensure_event_q()
        rid = self._next_id
        self._next_id += 1
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        await self._send_obj({"id": rid, "method": method, "params": params})
        return await fut

    async def ensure_thread_resumed(self, thread_id: str) -> None:
        await self.request("thread/resume", {"threadId": thread_id, "excludeTurns": True})

    async def thread_read(self, thread_id: str) -> dict[str, Any]:
        return await self.request("thread/read", {"threadId": thread_id, "includeTurns": True})

    async def thread_fork(self, thread_id: str) -> str:
        res = await self.request("thread/fork", {"threadId": thread_id})
        new_id = ((res.get("thread") or {}).get("id")) or res.get("id") or ""
        if not new_id:
            raise AppServerError("fork returned no thread id")
        return str(new_id)

    async def inject_items(self, thread_id: str, text: str) -> None:
        # chunk to avoid huge payload
        CHUNK = 32000
        for i in range(0, len(text), CHUNK):
            part = text[i : i + CHUNK]
            await self.request("thread/inject_items", {"threadId": thread_id, "items": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": part}]}]})

    async def run_turn(self, thread_id: str, text: str) -> tuple[str, str]:
        """Start or steer a turn and return (reply, turnId)."""
        self._ensure_event_q()
        # check active turn
        read = await self.thread_read(thread_id)
        thread = read.get("thread") or {}
        active: str | None = None
        for t in reversed(thread.get("turns") or []):
            if t.get("status") in {"inProgress", "pending", "started", "running"}:
                active = t.get("id")
                break
        rid = self._next_id
        self._next_id += 1
        if active:
            await self._send_obj({"id": rid, "method": "turn/steer", "params": {"threadId": thread_id, "input": [{"type": "text", "text": text}], "expectedTurnId": active}})
            expected = active
        else:
            await self._send_obj({"id": rid, "method": "turn/start", "params": {"threadId": thread_id, "input": [{"type": "text", "text": text}]}})
            expected = None

        turn_id: str | None = expected
        output: list[str] = []

        # helper to extract text from content
        def content_text(c: Any) -> str:
            if isinstance(c, str):
                try:
                    c = json.loads(c)
                except Exception:
                    return c
            if isinstance(c, list):
                return "\n".join(str(x.get("text", "")) for x in c if isinstance(x, dict) and "text" in x)
            if isinstance(c, dict) and "text" in c:
                return str(c["text"])
            return ""

        while True:
            msg = await self._event_q.get()
            if "_error" in msg:
                raise AppServerError(str(msg["_error"]))
            if msg.get("id") == rid:
                if "error" in msg:
                    raise AppServerError(f"turn: {msg['error']}")
                turn = (msg.get("result") or {}).get("turn") or {}
                if turn.get("id"):
                    turn_id = turn["id"]
                continue
            method = msg.get("method", "")
            params = msg.get("params") or {}
            if method == "item/agentMessage/delta":
                delta = params.get("delta") or params.get("text") or ""
                if isinstance(delta, str):
                    output.append(delta)
            elif method == "item/completed" and not output:
                item = params.get("item") or {}
                if item.get("type") == "agentMessage":
                    v = content_text(item.get("content"))
                    if v:
                        output.append(v)
            elif method == "turn/completed":
                completed = params.get("turn") or {}
                cid = completed.get("id") or params.get("turnId")
                if turn_id is None or cid in {None, turn_id}:
                    return ("".join(output).strip(), turn_id or "")
