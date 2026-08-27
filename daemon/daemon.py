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
import uuid
from pathlib import Path
from typing import Any

from offset_store import OffsetStore, RequestConflict, request_fingerprint, request_key
from thread_store import ThreadStore
from verification import VerificationLog
from ws_client import WSClient, AppServerError

APP_SOCK = Path("/home/s0u7a/.codex/app-server-control/app-server-control.sock")
DEFAULT_UDS = Path("/run/user/1000/codex-bridge.sock")
DEFAULT_STATE = Path.home() / ".local/state/codex-bridge/thread-state.json"
DEFAULT_OFFSET = Path.home() / ".local/state/codex-bridge/offset.json"
DEFAULT_VERIFICATION_LOG = Path.home() / ".local/state/codex-bridge/verification.jsonl"
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


def deliver_telegram(text: str) -> bool:
    """Deliver to Telegram and report whether every chunk was accepted."""

    text = redact(text).strip()
    if not text:
        return True
    delivered = True
    for part in chunk_text(text, 3500):
        try:
            result = subprocess.run(
                [str(TG_SINK), part],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
                start_new_session=True,
            )
            if result.returncode != 0:
                delivered = False
        except Exception:
            delivered = False
    return delivered


def deliver_miku(text: str) -> bool:
    """Deliver to Miku with a hard process bound.

    This function is called through ``asyncio.to_thread`` by the daemon's
    fanout worker, so waiting here does not block UDS request handling.
    """

    text = redact(text).strip()[:12000]
    if not text:
        return True
    cmd = [
        str(OPENCLAW_BIN),
        "agent",
        "--session-key",
        MIKU_SESSION_KEY,
        "--deliver",
        "--reply-channel",
        "telegram",
        "--reply-to",
        TELEGRAM_TARGET,
        "--thinking",
        "off",
        "--timeout",
        "60",
        "--message",
        f"Codex: {text}",
    ]
    try:
        try:
            result = subprocess.run(
                ["timeout", "--kill-after=5", "45"] + cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                start_new_session=True,
            )
            return result.returncode == 0
        except FileNotFoundError:
            result = subprocess.run(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=45,
                check=False,
                start_new_session=True,
            )
            return result.returncode == 0
    except Exception:
        return False


class Daemon:
    def __init__(
        self,
        uds: Path,
        state_path: Path,
        offset_path: Path,
        verification_log_path: Path = DEFAULT_VERIFICATION_LOG,
        limit_items: int = 12000,
        limit_turns: int = 50,
    ) -> None:
        self.uds = uds
        self.store = ThreadStore(state_path, limit_items=limit_items, limit_turns=limit_turns)
        self.offset = OffsetStore(offset_path)
        self.verification = VerificationLog(verification_log_path)
        self.ws = WSClient(str(APP_SOCK))
        self.queue: asyncio.Queue[tuple[dict[str, Any], asyncio.Future[dict[str, Any]]]] = asyncio.Queue()
        self._server: asyncio.AbstractServer | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._fanout_task: asyncio.Task[None] | None = None
        self._fanout_event = asyncio.Event()
        self._ws_connected = False
        self._stale_task: asyncio.Task[None] | None = None
        self._active_request_ids: set[str] = set()

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
                self._ws_connected = True
                self.verification.append("ws.connected")
                self._fanout_event.set()
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
        # Last resort: fork the newest healthy legacy thread.  Forking a
        # systemError thread reproduces the oversized/broken history and is
        # worse than returning no candidate.
        candidates = [t for t in legacy if (t.get("status") or {}).get("type") != "systemError"]
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
            self.verification.append(
                "thread.usage",
                threadId=tid,
                itemCount=item_count,
                turnCount=turn_count,
                status=status_type,
            )
            if not self.store.needs_rotation(status_type=status_type, item_count=item_count, turn_count=turn_count):
                return tid

            log(f"rotation needed: status={status_type} items={item_count} turns={turn_count} — tid={tid}")
            self.verification.append(
                "thread.rotation_needed",
                threadId=tid,
                itemCount=item_count,
                turnCount=turn_count,
                status=status_type,
            )

            # A systemError or turn-heavy thread should not be forked: its
            # child may inherit the same broken/oversized history.  Prefer a
            # separately small, healthy legacy thread and verify it again.
            if status_type == "systemError" or turn_count >= self.store.limit_turns:
                found = await self._pick_thread()
                if found and found != tid:
                    chk = await self.ws.thread_read(found)
                    if (chk.get("thread") or {}).get("status", {}).get("type") != "systemError":
                        self.store.set_active(found, forked_from=tid)
                        self.verification.append("thread.rotated", fromThreadId=tid, toThreadId=found, reason=status_type or "limit")
                        log(f"rotated {tid} -> picked fresh {found}")
                        return found

                try:
                    await self.ws.request("thread/compact/start", {"threadId": tid})
                    log(f"compact started for {tid}")
                except Exception as compact_error:
                    log(f"compact failed: {compact_error}")
                if status_type == "systemError":
                    found = await self._pick_thread()
                    if found and found != tid:
                        chk = await self.ws.thread_read(found)
                        if (chk.get("thread") or {}).get("status", {}).get("type") != "systemError":
                            self.store.set_active(found, forked_from=tid)
                            self.verification.append("thread.rotated", fromThreadId=tid, toThreadId=found, reason="systemError")
                            return found
                    raise AppServerError(f"systemError on {tid} and no healthy alternative thread")

            # Item-heavy threads use the existing fork path, but the child is
            # read and validated before the pointer is persisted.
            try:
                new_id = await self.ws.thread_fork(tid)
                child_info = await self.ws.thread_read(new_id)
                child_status = (child_info.get("thread") or {}).get("status", {}).get("type")
                if child_status == "systemError":
                    log(f"forked child {new_id} is still systemError — discarding, picking fresh")
                    found = await self._pick_thread()
                    if found and found not in {tid, new_id}:
                        found_info = await self.ws.thread_read(found)
                        if (found_info.get("thread") or {}).get("status", {}).get("type") != "systemError":
                            self.store.set_active(found, forked_from=tid)
                            self.verification.append("thread.rotated", fromThreadId=tid, toThreadId=found, reason="limit-fallback")
                            return found
                    raise AppServerError(f"forked child {new_id} is systemError and no healthy thread is available")
                self.store.set_active(new_id, forked_from=tid)
                self.verification.append("thread.rotated", fromThreadId=tid, toThreadId=new_id, reason="limit")
                log(f"forked {tid} -> {new_id}")
                return new_id
            except AppServerError:
                raise
            except (OSError, BrokenPipeError, ConnectionResetError):
                raise
            except Exception as fork_error:
                log(f"fork failed: {fork_error} — trying compact")
                try:
                    await self.ws.request("thread/compact/start", {"threadId": tid})
                    log(f"compact started for {tid}")
                except Exception as compact_error:
                    log(f"compact failed: {compact_error}")
                raise
        except AppServerError:
            raise
        except (OSError, BrokenPipeError, ConnectionResetError):
            raise
        except Exception as error:
            log(f"ensure_active_thread check failed: {error}")
            raise

    async def _reconnect_ws(self) -> None:
        self._ws_connected = False
        try:
            await self.ws.close()
        except Exception:
            pass
        # reset WSClient state so _pending doesn't leak
        self.ws._pending.clear()  # type: ignore[attr-defined]
        await self.ensure_ws()

    async def handle_submit(self, text: str, source: str, request_id: str) -> dict[str, Any]:
        # Opportunistically reap a stale ``in_progress`` entry for this id so
        # an old crash (e.g. daemon restart mid-turn at ~17:33) does not pin
        # the slot forever.  ``begin_request`` already reaps the same id, but
        # we emit an operator-visible verification event here so
        # ``bridge_report`` reflects that reaping happened.
        try:
            existing = (self.offset.data.get("requests") or {}).get(request_id)
            if isinstance(existing, dict):
                # ``_is_stale_in_progress`` is static; call through the store.
                from offset_store import OffsetStore as _OS  # local to avoid cycle at import time
                if _OS._is_stale_in_progress(existing, time.time()):  # type: ignore[attr-defined]
                    self.verification.append("request.stale_reaped", requestKey=request_key(request_id), ageSec=round(time.time() - float(existing.get("lastAttemptAt", existing.get("createdAt", time.time()))), 1))
        except Exception:
            pass
        fingerprint = request_fingerprint(source, text)
        # ``begin_request`` reaps the stale entry for this id if present; the
        # event above records that it happened.
        cached = self.offset.begin_request(request_id, fingerprint)
        if cached is not None:
            self.verification.append("request.replayed", requestKey=request_key(request_id))
            self._fanout_event.set()
            return cached
        self.verification.append("request.accepted", requestKey=request_key(request_id), source=source)
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
        except (AppServerError, OSError, BrokenPipeError, ConnectionResetError) as e:
            log(f"run_turn failed ({type(e).__name__}): {e} — reconnecting once")
            await self._reconnect_ws()
            tid = await self.ensure_active_thread()
            reply, turn_id = await self.ws.run_turn(tid, payload)
        result = {"reply": reply, "threadId": tid, "turnId": turn_id}
        delivery_id = self.offset.finish_request(request_id, fingerprint, result)
        self.offset.maybe_trim()
        if delivery_id:
            result["deliveryId"] = delivery_id
            self.verification.append(
                "outbox.enqueued",
                deliveryId=delivery_id,
                requestKey=request_key(request_id),
                threadId=tid,
                turnId=turn_id,
                replyHash=hashlib.sha256(reply.encode("utf-8")).hexdigest()[:16],
                replyLength=len(reply),
            )
            self._fanout_event.set()
        self.verification.append(
            "turn.completed",
            requestKey=request_key(request_id),
            threadId=tid,
            turnId=turn_id,
            replyLength=len(reply),
        )
        return result

    def _is_turn_active(self) -> bool:
        """True while a Codex turn is certainly still running.

        Only two signals are reliable here: the worker queue is non-empty and
        the worker is holding an id in ``_active_request_ids``.  A
        lastAttemptAt timestamp alone is not a live heartbeat, so it is not
        used to keep entries alive — otherwise a crash without cleanup would
        keep a dead entry for an hour.  Deceased entries are reaped by TTL;
        live ones are protected by the two active signals above and by
        deferring the whole sweep while a turn is active.
        """

        return bool(self._active_request_ids or self.queue.qsize() > 0)

    def _reap_with_active_guard(self) -> list[str]:
        """Reap stale entries, skipping requestIds whose turns are live.

        The long-turn bug is that ``OffsetStore._is_stale_in_progress`` uses
        only a 600 s TTL without a live heartbeat, so the watchdog must not
        call it for an active request.  Protect only the active IDs instead of
        deferring the entire sweep, otherwise unrelated stale entries would
        accumulate during continuous traffic.
        """

        active_ids = set(self._active_request_ids)
        if active_ids:
            log(f"stale watchdog protecting active requests: {len(active_ids)}")
        return self.offset.reap_stale_requests(exclude_request_ids=active_ids)

    async def stale_watchdog(self) -> None:
        """Periodically reap ``in_progress`` requests that never finished."""

        # First sweep shortly after boot to surface the 17:33 stale entry that
        # is still live as of this PR.  Afterwards sweep every five minutes.
        # Guard: if a long turn (>600 s) is still active, defer the sweep so
        # an in-flight turn is not misclassified as stale and reaped.  The
        # TTL-based OffsetStore check alone has no live heartbeat, so the
        # watchdog explicitly checks _is_turn_active() before reaping.
        await asyncio.sleep(10)
        while True:
            try:
                reaped = self._reap_with_active_guard()
                for rid in reaped:
                    self.verification.append("request.stale_reaped", requestKey=request_key(rid), reason="watchdog")
                    log(f"stale reaped {request_key(rid)} (watchdog)")
            except Exception as e:
                log(f"stale watchdog error: {e}")
            await asyncio.sleep(300)

    async def fanout_worker(self) -> None:
        """Drain the durable outbox and retry each target independently."""

        while True:
            self._fanout_event.clear()
            did_work = False
            now = time.time()
            for item in self.offset.pending_replies(now=now):
                delivery_id = str(item.get("deliveryId") or "")
                reply = str(item.get("reply") or "")
                for target in ("telegram", "miku"):
                    target_state = (item.get("targets") or {}).get(target) or {}
                    if target_state.get("delivered"):
                        continue
                    if float(target_state.get("nextAttemptAt", 0)) > now:
                        continue
                    did_work = True
                    try:
                        if target == "telegram":
                            delivered = await asyncio.to_thread(deliver_telegram, reply)
                        else:
                            delivered = await asyncio.to_thread(deliver_miku, reply)
                        error = "delivery returned non-zero status"
                    except Exception as exc:
                        delivered = False
                        error = f"{type(exc).__name__}: {exc}"
                    if delivered:
                        self.offset.mark_target_delivered(delivery_id, target)
                        self.verification.append(
                            "fanout.delivered",
                            deliveryId=delivery_id,
                            target=target,
                            attempts=int(target_state.get("attempts", 0)) + 1,
                        )
                    else:
                        self.offset.mark_target_failed(delivery_id, target, error)
                        updated = self.offset.get_delivery(delivery_id) or {}
                        updated_target = (updated.get("targets") or {}).get(target) or {}
                        self.verification.append(
                            "fanout.failed",
                            deliveryId=delivery_id,
                            target=target,
                            attempts=int(updated_target.get("attempts", 0)),
                            error=error[:200],
                        )
            if did_work:
                continue
            try:
                await asyncio.wait_for(self._fanout_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    def status_snapshot(self) -> dict[str, Any]:
        """Return local health state without waiting on an active Codex turn."""

        return {
            "activeThreadId": self.store.active_thread_id,
            "store": self.store.to_dict(),
            "wsConnected": self._ws_connected,
            "offset": self.offset.summary(),
            "verification": self.verification.summary(),
        }

    async def status_info(self, include_thread: bool = True) -> dict[str, Any]:
        info = self.status_snapshot()
        tid = info["activeThreadId"]
        if include_thread and tid:
            try:
                r = await self.ws.thread_read(tid)
                thread = r.get("thread") or {}
                info["threadStatus"] = thread.get("status")
                info["turnCount"] = len(thread.get("turns") or [])
                info["itemCount"] = sum(len(t.get("items") or []) for t in (thread.get("turns") or []))
            except Exception as e:
                info["threadStatusError"] = str(e)
        return info

    async def worker(self) -> None:
        while True:
            req, fut = await self.queue.get()
            try:
                method = req.get("method")
                params = req.get("params") or {}
                if method == "submit":
                    text = str(params.get("text") or "")
                    source = str(params.get("source") or "unknown")
                    request_id = str(params.get("requestId") or f"anonymous:{uuid.uuid4().hex}")
                    if len(request_id) > 256:
                        fut.set_result({"error": {"code": -32602, "message": "requestId too long"}})
                        continue
                    if not text.strip():
                        fut.set_result({"error": {"code": -32602, "message": "text required"}})
                    else:
                        self._active_request_ids.add(request_id)
                        try:
                            res = await self.handle_submit(text, source, request_id)
                        finally:
                            self._active_request_ids.discard(request_id)
                        fut.set_result({"result": res})
                elif method == "status":
                    fut.set_result({"result": await self.status_info()})
                elif method == "ping":
                    fut.set_result({"result": {"ok": True, "activeThreadId": self.store.active_thread_id}})
                else:
                    fut.set_result({"error": {"code": -32601, "message": f"unknown method {method}"}})
            except RequestConflict as e:
                fut.set_result({"error": {"code": -32600, "message": str(e)}})
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
                method = req.get("method")
                if method == "ping":
                    out = {"result": {"ok": True, "activeThreadId": self.store.active_thread_id}}
                    if rid is not None:
                        out["id"] = rid
                    writer.write((json.dumps(out, ensure_ascii=False) + "\n").encode())
                    await writer.drain()
                    continue
                if method == "status":
                    out = {"result": self.status_snapshot()}
                    if rid is not None:
                        out["id"] = rid
                    writer.write((json.dumps(out, ensure_ascii=False) + "\n").encode())
                    await writer.drain()
                    continue
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
        self.verification.append("daemon.start", uds=str(self.uds))
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
        self._fanout_task = asyncio.create_task(self.fanout_worker())
        self._stale_task = asyncio.create_task(self.stale_watchdog())
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
    ap.add_argument("--verification-log", default=str(DEFAULT_VERIFICATION_LOG))
    ap.add_argument("--limit-items", type=int, default=12000)
    ap.add_argument("--limit-turns", type=int, default=50)
    args = ap.parse_args()
    daemon = Daemon(
        Path(args.uds),
        Path(args.state),
        Path(args.offset),
        Path(args.verification_log),
        limit_items=args.limit_items,
        limit_turns=args.limit_turns,
    )
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
