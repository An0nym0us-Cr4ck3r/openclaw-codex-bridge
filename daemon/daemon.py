#!/usr/bin/env python3
"""codex-bridge-daemon — single owner of app-server.sock (WS 1本), ThreadStore, UDS fanout."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from thread_store import ThreadStore
from ws_client import WSClient, AppServerError

APP_SOCK = Path("/home/s0u7a/.codex/app-server-control/app-server-control.sock")
DEFAULT_UDS = Path("/run/user/1000/codex-bridge.sock")
DEFAULT_STATE = Path.home() / ".local/state/codex-bridge/thread-state.json"
DEFAULT_OFFSET = Path.home() / ".local/state/codex-bridge/offset.json"
TG_SINK = Path("/home/s0u7a/.local/bin/codex-tg-send")
MIKU_SESSION_KEY = "agent:miku:telegram:direct:7536160870"
TELEGRAM_TARGET = "7536160870"
OPENCLAW_BIN = Path("/home/s0u7a/.local/bin/openclaw")


def log(msg: str) -> None:
    print(f"[daemon] {msg}", flush=True)


def redact(text: str) -> str:
    for pat, rep in [
        (r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+", r"\1<REDACTED>"),
        (r"(?i)((?:api[_-]?key|token|password|secret)\s*[:=]\s*)[^\s,;]+", r"\1<REDACTED>"),
        (r"(?i)\bsk-[A-Za-z0-9_-]{16,}\b", "<REDACTED>"),
        (r"\bbot\d+:[A-Za-z0-9_-]{20,}\b", "<REDACTED>"),
    ]:
        text = re.sub(pat, rep, text)
    return text


def chunk_text(text: str, limit: int):
    if len(text) <= limit:
        yield text
        return
    start = 0
    while start < len(text):
        end = min(start + limit, len(text))
        if end < len(text):
            split = text.rfind("\n\n", start, end)
            if split > start + limit // 2:
                end = split
        yield text[start:end]
        start = end


class OffsetStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = {}
        try:
            self.data = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            self.data = {"delivered": [], "pending": []}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)

    def is_delivered(self, h: str) -> bool:
        return h in set(self.data.get("delivered") or [])

    def mark_delivered(self, h: str) -> None:
        lst = self.data.setdefault("delivered", [])
        if h not in lst:
            lst.append(h)
            # keep last 2000
            if len(lst) > 2000:
                self.data["delivered"] = lst[-2000:]
            self.save()

    def push_pending(self, reply: str, thread_id: str) -> None:
        self.data.setdefault("pending", []).append({"reply": reply, "threadId": thread_id, "ts": int(time.time())})
        self.save()

    def pop_pending(self) -> list[dict[str, Any]]:
        p = list(self.data.get("pending") or [])
        self.data["pending"] = []
        self.save()
        return p


def deliver_telegram(text: str) -> None:
    text = redact(text).strip()
    if not text:
        return
    for part in chunk_text(text, 3500):
        try:
            subprocess.run([str(TG_SINK), part], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=False)
        except Exception:
            pass


def deliver_miku_async(text: str) -> None:
    text = redact(text).strip()[:12000]
    if not text:
        return
    try:
        cmd = [str(OPENCLAW_BIN), "agent", "--session-key", MIKU_SESSION_KEY, "--deliver",
               "--reply-channel", "telegram", "--reply-to", TELEGRAM_TARGET,
               "--thinking", "off", "--timeout", "60", "--message", f"Codex: {text}"]
        try:
            subprocess.Popen(
                ["timeout", "45"] + cmd,
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except FileNotFoundError:
            proc = subprocess.Popen(
                cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            def _kill():
                import time as _t
                _t.sleep(50)
                try:
                    proc.terminate()
                    _t.sleep(2)
                    proc.kill()
                except Exception:
                    pass
            import threading
            threading.Thread(target=_kill, daemon=True).start()
    except Exception:
        pass


class Daemon:
    def __init__(self, uds: Path, state_path: Path, offset_path: Path) -> None:
        self.uds = uds
        self.store = ThreadStore(state_path)
        self.offset = OffsetStore(offset_path)
        self.ws = WSClient(str(APP_SOCK))
        self.queue: asyncio.Queue[tuple[dict[str, Any], asyncio.Future[dict[str, Any]]]] = asyncio.Queue()
        self._server: asyncio.AbstractServer | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._ws_connected = False

    async def ensure_ws(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self.ws.connect()
                # resume active thread if known
                tid = self.store.active_thread_id
                if tid:
                    try:
                        await self.ws.ensure_thread_resumed(tid)
                        log(f"resumed thread {tid}")
                    except Exception as e:
                        log(f"resume {tid} failed: {e} — will try fork/list")
                        # try to pick a fresh thread
                        try:
                            found = await self._pick_thread()
                            if found and found != tid:
                                self.store.set_active(found)
                                tid = found
                                log(f"picked thread {tid}")
                        except Exception as e2:
                            log(f"pick thread failed: {e2}")
                else:
                    # no active thread — pick one
                    try:
                        found = await self._pick_thread()
                        if found:
                            self.store.set_active(found)
                            log(f"picked initial thread {found}")
                    except Exception as e:
                        log(f"pick initial thread failed: {e}")
                # replay pending fanout
                for item in self.offset.pop_pending():
                    h = hashlib.sha256(item["reply"].encode()).hexdigest()[:16]
                    if not self.offset.is_delivered(h):
                        deliver_telegram(item["reply"])
                        deliver_miku_async(item["reply"])
                        self.offset.mark_delivered(h)
                self._ws_connected = True
                log("WS connected")
                return
            except Exception as e:
                log(f"WS connect failed: {e} — retry in {backoff:.1f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _pick_thread(self) -> str | None:
        res = await self.ws.request("thread/list", {})
        threads = res.get("data") or []
        # Prefer smallest legacy thread (fewest turns) that is not systemError, to avoid the 16384 item chain.
        # Score by recency within the smallest-turn bucket.
        legacy = [t for t in threads if t.get("historyMode") == "legacy"]
        best: str | None = None
        best_turns: int | None = None
        best_recency: int = -1
        for t in legacy:
            st = (t.get("status") or {}).get("type")
            if st == "systemError":
                continue
            try:
                info = await self.ws.thread_read(t["id"])
                n = len(((info.get("thread") or {}).get("turns")) or [])
            except Exception:
                continue
            rec = int(t.get("recencyAt") or t.get("createdAt") or 0)
            if best_turns is None or n < best_turns or (n == best_turns and rec > best_recency):
                best_turns = n
                best_recency = rec
                best = t["id"]
        if best is not None:
            return best
        # Fallback: any non-systemError legacy newest
        for t in sorted(legacy, key=lambda x: int(x.get("recencyAt") or 0), reverse=True):
            if (t.get("status") or {}).get("type") != "systemError":
                return t["id"]
        # Last resort: fork the newest non-systemError (or the newest at all)
        candidates = [t for t in legacy if (t.get("status") or {}).get("type") != "systemError"]
        if not candidates:
            candidates = legacy
        if candidates:
            newest = sorted(candidates, key=lambda x: int(x.get("recencyAt") or 0), reverse=True)[0]
            return await self.ws.thread_fork(newest["id"])
        return None

    async def ensure_active_thread(self) -> str:
        tid = self.store.active_thread_id
        if tid is None:
            found = await self._pick_thread()
            if not found:
                raise AppServerError("no thread available")
            self.store.set_active(found)
            tid = found
        try:
            info = await self.ws.thread_read(tid)
            thread = info.get("thread") or {}
            status_type = (thread.get("status") or {}).get("type")
            turns = thread.get("turns") or []
            item_count = sum(len(t.get("items") or []) for t in turns)
            turn_count = len(turns)
            self.store.record_usage(item_count, turn_count)
            if self.store.needs_rotation(status_type=status_type, item_count=item_count, turn_count=turn_count):
                log(f"rotation needed: status={status_type} items={item_count} turns={turn_count} — tid={tid}")
                # Forking a huge thread copies the huge history and the child still fails (array too long).
                # So for systemError or excessive turns we pick a fresh small thread instead of forking the big one.
                if status_type == "systemError" or turn_count >= self.store.limit_turns:
                    found = await self._pick_thread()
                    if found and found != tid:
                        # verify it is actually usable (not systemError)
                        try:
                            chk = await self.ws.thread_read(found)
                            if (chk.get("thread") or {}).get("status", {}).get("type") != "systemError":
                                self.store.set_active(found, forked_from=tid)
                                log(f"rotated systemError {tid} -> picked fresh {found}")
                                return found
                        except Exception:
                            pass
                    # fallback: try compact then fail
                    try:
                        await self.ws.request("thread/compact/start", {"threadId": tid})
                        log(f"compact started for {tid}")
                    except Exception as e2:
                        log(f"compact failed: {e2}")
                    if status_type == "systemError":
                        # create a fresh thread by re-picking (will fork smallest)
                        found2 = await self._pick_thread()
                        if found2:
                            self.store.set_active(found2, forked_from=tid)
                            return found2
                        raise AppServerError(f"systemError on {tid} and no alternative thread")
                # Normal rotation (not systemError): fork
                try:
                    new_id = await self.ws.thread_fork(tid)
                    # Verify child is not immediately systemError (fork of huge history)
                    try:
                        child_info = await self.ws.thread_read(new_id)
                        if (child_info.get("thread") or {}).get("status", {}).get("type") == "systemError":
                            log(f"forked child {new_id} is still systemError — discarding, picking fresh")
                            found = await self._pick_thread()
                            if found and found != tid:
                                self.store.set_active(found, forked_from=tid)
                                return found
                    except Exception:
                        pass
                    self.store.set_active(new_id, forked_from=tid)
                    log(f"forked {tid} -> {new_id}")
                    return new_id
                except Exception as e:
                    log(f"fork failed: {e} — trying compact")
                    try:
                        await self.ws.request("thread/compact/start", {"threadId": tid})
                        log(f"compact started for {tid}")
                    except Exception as e2:
                        log(f"compact failed: {e2}")
                    if status_type == "systemError":
                        found = await self._pick_thread()
                        if found and found != tid:
                            self.store.set_active(found)
                            return found
                        raise
        except AppServerError:
            raise
        except Exception as e:
            log(f"ensure_active_thread check failed: {e}")
        return tid

    async def _reconnect_ws(self) -> None:
        self._ws_connected = False
        try:
            await self.ws.close()
        except Exception:
            pass
        # reset WSClient state so _pending doesn't leak
        self.ws._pending.clear()  # type: ignore[attr-defined]
        await self.ensure_ws()

    async def handle_submit(self, text: str, source: str) -> dict[str, Any]:
        if not self._ws_connected:
            await self.ensure_ws()
        try:
            tid = await self.ensure_active_thread()
        except (AppServerError, OSError, BrokenPipeError, ConnectionResetError) as e:
            log(f"ensure_active_thread failed ({type(e).__name__}): {e} — reconnecting")
            await self._reconnect_ws()
            tid = await self.ensure_active_thread()
        # prefix source
        if source == "telegram":
            payload = f"Codex: Telegramからの指示\n{text}"
        elif source == "miku":
            payload = f"Miku: {text}"
        else:
            payload = text
        log(f"submit source={source} thread={tid} len={len(payload)}")
        # run turn with reconnect retry once on any transport failure
        try:
            reply, turn_id = await self.ws.run_turn(tid, payload)
        except (AppServerError, OSError, BrokenPipeError, ConnectionResetError, asyncio.CancelledError) as e:
            log(f"run_turn failed ({type(e).__name__}): {e} — reconnecting once")
            await self._reconnect_ws()
            tid = await self.ensure_active_thread()
            reply, turn_id = await self.ws.run_turn(tid, payload)
        # fanout (best-effort, with offset for crash)
        h = hashlib.sha256(reply.encode()).hexdigest()[:16] if reply else ""
        if reply and not self.offset.is_delivered(h):
            # push pending before fanout for crash safety
            self.offset.push_pending(reply, tid)
            deliver_telegram(reply)
            deliver_miku_async(reply)
            self.offset.mark_delivered(h)
            # clear pending entry
            # (already marked delivered, pending was the crash buffer)
            self.offset.data["pending"] = [p for p in self.offset.data.get("pending", []) if hashlib.sha256(p.get("reply","").encode()).hexdigest()[:16] != h]
            self.offset.save()
        return {"reply": reply, "threadId": tid, "turnId": turn_id}

    async def worker(self) -> None:
        while True:
            req, fut = await self.queue.get()
            try:
                method = req.get("method")
                params = req.get("params") or {}
                if method == "submit":
                    text = str(params.get("text") or "")
                    source = str(params.get("source") or "unknown")
                    if not text.strip():
                        fut.set_result({"error": {"code": -32602, "message": "text required"}})
                    else:
                        res = await self.handle_submit(text, source)
                        fut.set_result({"result": res})
                elif method == "status":
                    tid = self.store.active_thread_id
                    info: dict[str, Any] = {"activeThreadId": tid, "store": self.store.to_dict(), "wsConnected": self._ws_connected}
                    if tid:
                        try:
                            r = await self.ws.thread_read(tid)
                            thread = r.get("thread") or {}
                            info["threadStatus"] = thread.get("status")
                            info["turnCount"] = len(thread.get("turns") or [])
                        except Exception as e:
                            info["threadStatusError"] = str(e)
                    fut.set_result({"result": info})
                elif method == "ping":
                    fut.set_result({"result": {"ok": True, "activeThreadId": self.store.active_thread_id}})
                else:
                    fut.set_result({"error": {"code": -32601, "message": f"unknown method {method}"}})
            except Exception as e:
                if not fut.done():
                    fut.set_exception(e)
            finally:
                self.queue.task_done()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    req = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError as e:
                    writer.write((json.dumps({"error": {"code": -32700, "message": f"parse error: {e}"}}) + "\n").encode())
                    await writer.drain()
                    continue
                rid = req.get("id")
                fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
                await self.queue.put((req, fut))
                try:
                    res = await asyncio.wait_for(fut, timeout=1800)
                except asyncio.TimeoutError:
                    res = {"error": {"code": -32000, "message": "timeout"}}
                except Exception as e:
                    res = {"error": {"code": -32000, "message": str(e)}}
                out: dict[str, Any] = {}
                if rid is not None:
                    out["id"] = rid
                out.update(res)
                writer.write((json.dumps(out, ensure_ascii=False) + "\n").encode())
                await writer.drain()
        except Exception as e:
            log(f"client handler error: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def run(self) -> None:
        # stale sock cleanup
        if self.uds.exists():
            # probe if live
            try:
                r, w = await asyncio.open_unix_connection(str(self.uds))
                w.close()
                await w.wait_closed()
                log(f"UDS {self.uds} already owned — exiting")
                sys.exit(1)
            except (ConnectionRefusedError, FileNotFoundError, OSError):
                try:
                    self.uds.unlink()
                except FileNotFoundError:
                    pass
        self.uds.parent.mkdir(parents=True, exist_ok=True)
        await self.ensure_ws()
        self._worker_task = asyncio.create_task(self.worker())
        self._server = await asyncio.start_unix_server(self.handle_client, path=str(self.uds))
        # restrict to owner only (0700 dir already, but be explicit)
        try:
            os.chmod(self.uds, 0o700)
        except Exception:
            pass
        log(f"UDS listening on {self.uds}")
        async with self._server:
            await self._server.serve_forever()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uds", default=str(DEFAULT_UDS))
    ap.add_argument("--state", default=str(DEFAULT_STATE))
    ap.add_argument("--offset", default=str(DEFAULT_OFFSET))
    args = ap.parse_args()
    daemon = Daemon(Path(args.uds), Path(args.state), Path(args.offset))
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
