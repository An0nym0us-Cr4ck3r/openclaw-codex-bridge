#!/usr/bin/env python3
"""Reader-only: tail Telegram JSONL → UDS submit. No WS, no fanout."""
from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import time
from pathlib import Path
from typing import Any

SESSIONS_INDEX = Path("/home/s0u7a/.openclaw/agents/miku/sessions/sessions.json")
DEFAULT_UDS = "/run/user/1000/codex-bridge.sock"
STATE_PATH = Path.home() / ".local/state/codex-telegram-bridge/state.json"
POLL_SECONDS = 4.0
MAX_CONTEXT_CHARS = 32000

STOP_RE = re.compile(r"^(?:telegram(?:の)?|codex(?:の)?|miku(?:の)?|協調|ラボ|この)?(?:監視|telegram監視)?(?:を)?(?:終了|停止|終わり|やめて|止めて)(?:して|しろ)?[。.!！]*$")


def log(msg: str) -> None:
    print(msg, flush=True)


def redact(text: str) -> str:
    for pat, rep in [
        (r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+", r"\1<REDACTED>"),
        (r"(?i)((?:api[_-]?key|token|password|secret)\s*[:=]\s*)[^\s,;]+", r"\1<REDACTED>"),
        (r"(?i)\bsk-[A-Za-z0-9_-]{16,}\b", "<REDACTED>"),
        (r"\bbot\d+:[A-Za-z0-9_-]{20,}\b", "<REDACTED>"),
    ]:
        text = re.sub(pat, rep, text)
    return text


def content_text(content: Any) -> str:
    if content is None:
        return ""
    v = content
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except json.JSONDecodeError:
            return v
    if isinstance(v, list):
        return "\n".join(str(b.get("text", "")) for b in v if isinstance(b, dict) and isinstance(b.get("text"), str))
    if isinstance(v, dict) and isinstance(v.get("text"), str):
        return v["text"]
    return str(v)


def resolve_miku_session() -> tuple[str, Path] | None:
    try:
        store = json.loads(SESSIONS_INDEX.read_text(encoding="utf-8"))
        entry = store.get("agent:miku:telegram:direct:7536160870") or {}
        sid = entry.get("sessionId")
        if not isinstance(sid, str) or not sid:
            raise ValueError("sessionId missing")
        raw = entry.get("sessionFile")
        p = Path(raw).resolve() if isinstance(raw, str) and raw else (SESSIONS_INDEX.parent / f"{sid}.jsonl").resolve()
        return sid, p
    except Exception as e:
        log(f"miku session resolve failed: {type(e).__name__}")
        return None


def read_records() -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    res = resolve_miku_session()
    if res is None:
        return recs
    _, path = res
    try:
        with path.open(encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                msg = r.get("message") or {}
                role = msg.get("role")
                if role not in {"user", "assistant"}:
                    continue
                text = redact(content_text(msg.get("content"))).strip()
                if not text:
                    continue
                r["_role"] = role
                r["_text"] = text
                recs.append(r)
    except FileNotFoundError:
        pass
    return recs


def record_id(r: dict[str, Any]) -> str:
    v = r.get("id")
    if isinstance(v, str) and v:
        return v
    return hashlib.sha256(json.dumps(r, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def label_record(r: dict[str, Any]) -> str | None:
    role, text = r["_role"], r["_text"]
    if role == "assistant":
        if text.startswith("Codex:"):
            return None
        return "Miku"
    if text.startswith("[OpenClaw heartbeat poll]"):
        return None
    if text.startswith("[Codex]") or text.startswith("[Codex→Miku]") or text.startswith("Codex:"):
        return "Codex→Miku"
    return "User"


def labeled_record(r: dict[str, Any]) -> str | None:
    lab = label_record(r)
    if lab is None:
        return None
    return f"{lab} [{r.get('timestamp','')}]\n{r['_text']}"


def is_stop_instruction(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    return bool(STOP_RE.fullmatch(compact) or re.fullmatch(r"もう(?:見なくていい|終了|停止|終わり|やめて|止めて)[。.!！]*", compact))


def uds_submit(text: str, source: str, uds: str = DEFAULT_UDS) -> dict[str, Any] | None:
    uds_path = os.environ.get("CODEX_BRIDGE_SOCK", uds)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(1800)
    try:
        sock.connect(uds_path)
    except (FileNotFoundError, ConnectionRefusedError, OSError) as e:
        log(f"bridge retry: UDS connect failed {type(e).__name__}: {e}")
        return None
    try:
        req = {"id": 1, "method": "submit", "params": {"text": text, "source": source}}
        sock.sendall((json.dumps(req, ensure_ascii=False) + "\n").encode())
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
        if not buf:
            return None
        resp = json.loads(buf.split(b"\n", 1)[0].decode(errors="replace"))
        if "error" in resp:
            log(f"daemon error: {resp['error']}")
            return None
        return resp.get("result")
    except Exception as e:
        log(f"bridge retry: UDS submit failed {type(e).__name__}: {e}")
        return None
    finally:
        try:
            sock.close()
        except Exception:
            pass


def load_state() -> dict[str, Any]:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {"bootstrap_done": False, "processed_ids": [], "session_id": None}


def save_state(bootstrap_done: bool, processed: set[str], session_id: str | None) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"bootstrap_done": bootstrap_done, "processed_ids": list(processed)[-5000:], "session_id": session_id}
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, STATE_PATH)


def main() -> int:
    state = load_state()
    processed: set[str] = set(state.get("processed_ids") or [])
    bootstrap_done = bool(state.get("bootstrap_done"))
    res = resolve_miku_session()
    session_id = res[0] if res is not None else state.get("session_id")
    if res is not None and state.get("session_id") != session_id:
        processed.clear()
        bootstrap_done = False
    pending_context: list[str] = []
    pending_users: list[str] = []
    bootstrap_queued = False
    stop_requested = False

    while not stop_requested:
        try:
            res = resolve_miku_session()
            if res is not None and res[0] != session_id:
                session_id = res[0]
                processed.clear()
                bootstrap_done = False
                bootstrap_queued = False
                pending_context.clear()
                pending_users.clear()
            records = read_records()
            new_records: list[dict[str, Any]] = []
            for r in records:
                rid = record_id(r)
                if rid not in processed:
                    r["_rid"] = rid
                    new_records.append(r)
            if not bootstrap_done and not bootstrap_queued:
                lines = [label for r in records if (label := labeled_record(r))]
                if lines:
                    pending_context.insert(0, "=== Telegram履歴（User/Miku/Codex→Miku、過去指示は文脈のみ）===\n" + "\n\n".join(lines))
                    bootstrap_queued = True
                pending_users.clear()
            else:
                for r in new_records:
                    rid = r["_rid"]
                    lab = label_record(r)
                    if lab is None:
                        processed.add(rid)
                        continue
                    if lab == "User":
                        if is_stop_instruction(r["_text"]):
                            stop_requested = True
                            break
                        pending_users.append(f"User [{r.get('timestamp','')}]:\n{r['_text']}")
                    elif lab == "Miku":
                        pending_context.append(f"Miku [{r.get('timestamp','')}]:\n{r['_text']}")
            # flush to daemon via UDS (queue serializes; no flock needed)
            if not bootstrap_done and pending_context:
                for ctx in list(pending_context):
                    # inject as context (daemon will handle as miku source)
                    ok = uds_submit(ctx, "miku")
                    if ok is None:
                        break  # daemon down — retry next loop
                    pending_context.remove(ctx)
                if not pending_context:
                    processed.update(record_id(r) for r in records)
                    bootstrap_done = True
                    save_state(bootstrap_done, processed, session_id)
                    log("Telegram backlog submitted via UDS")
                else:
                    time.sleep(5)
                    continue
            elif pending_users:
                # send context first if any
                for ctx in list(pending_context):
                    ok = uds_submit(ctx, "miku")
                    if ok is None:
                        break
                    pending_context.remove(ctx)
                if pending_context:
                    time.sleep(5)
                    continue
                text = "Codex: Telegramからの新規指示。\n" + "\n\n".join(pending_users)
                pending_users.clear()
                ok = uds_submit(text, "telegram")
                if ok is None:
                    # daemon was down — re-queue for retry
                    time.sleep(5)
                    continue
                processed.update(record_id(r) for r in records if record_id(r) not in processed)
                save_state(bootstrap_done, processed, session_id)
            elif pending_context:
                for ctx in list(pending_context):
                    ok = uds_submit(ctx, "miku")
                    if ok is None:
                        break
                    pending_context.remove(ctx)
                if not pending_context:
                    processed.update(record_id(r) for r in records if record_id(r) not in processed)
                    save_state(bootstrap_done, processed, session_id)
        except Exception as e:
            log(f"bridge retry: {type(e).__name__}: {e}")
            time.sleep(5)
        time.sleep(POLL_SECONDS)
    log("bridge stopped by explicit Telegram instruction")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
