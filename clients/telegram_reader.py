#!/usr/bin/env python3
"""Reader-only: tail Telegram JSONL → UDS submit. No WS, no fanout."""
from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import tempfile
import time
from pathlib import Path
from typing import Any

SESSIONS_INDEX = Path("/home/s0u7a/.openclaw/agents/miku/sessions/sessions.json")
DEFAULT_UDS = "/run/user/1000/codex-bridge.sock"
STATE_PATH = Path.home() / ".local/state/codex-telegram-bridge/state.json"
POLL_SECONDS = 4.0
MAX_CONTEXT_CHARS = 32000
MAX_PROCESSED_IDS = 5000

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


def build_bootstrap_context(records: list[dict[str, Any]]) -> str:
    """Build a bounded history snapshot, retaining the newest records."""

    lines = [label for r in records if (label := labeled_record(r))]
    header = "=== Telegram履歴（User/Miku/Codex→Miku、過去指示は文脈のみ）===\n"
    if not lines:
        return ""
    full = "\n\n".join(lines)
    if len(header) + len(full) <= MAX_CONTEXT_CHARS:
        return header + full

    marker_template = "…古い履歴を{}件省略…\n\n"
    selected: list[str] = []
    used = 0
    for line in reversed(lines):
        separator = 2 if selected else 0
        if used + separator + len(line) > MAX_CONTEXT_CHARS - len(header) - len(marker_template.format(0)):
            break
        selected.append(line)
        used += separator + len(line)
    selected.reverse()
    omitted = len(lines) - len(selected)
    marker = marker_template.format(omitted)
    budget = max(0, MAX_CONTEXT_CHARS - len(header) - len(marker))
    body = "\n\n".join(selected)
    if len(body) > budget:
        body = body[-budget:] if budget else ""
    return header + marker + body


def advance_processed_cursor(
    records: list[dict[str, Any]],
    processed: set[str],
    cursor: int,
    cursor_id: str | None,
) -> tuple[int, str | None]:
    """Compact the acknowledged prefix while preserving out-of-order IDs."""

    if cursor < 0 or (cursor and (cursor > len(records) or not cursor_id or record_id(records[cursor - 1]) != cursor_id)):
        cursor, cursor_id = 0, None
    while cursor < len(records):
        rid = record_id(records[cursor])
        if rid not in processed:
            break
        processed.discard(rid)
        cursor += 1
    return cursor, record_id(records[cursor - 1]) if cursor else None


def is_stop_instruction(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    return bool(STOP_RE.fullmatch(compact) or re.fullmatch(r"もう(?:見なくていい|終了|停止|終わり|やめて|止めて)[。.!！]*", compact))


def stable_request_id(kind: str, session_id: str | None, record_ids: list[str], text: str) -> str:
    material = json.dumps(
        [kind, session_id or "", record_ids, text],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"{kind}:{hashlib.sha256(material).hexdigest()}"


def uds_submit(
    text: str,
    source: str,
    request_id: str | None = None,
    uds: str = DEFAULT_UDS,
) -> dict[str, Any] | None:
    uds_path = os.environ.get("CODEX_BRIDGE_SOCK", uds)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(1800)
    try:
        sock.connect(uds_path)
    except (FileNotFoundError, ConnectionRefusedError, OSError) as e:
        log(f"bridge retry: UDS connect failed {type(e).__name__}: {e}")
        return None
    try:
        req = {
            "id": 1,
            "method": "submit",
            "params": {
                "text": text,
                "source": source,
                "requestId": request_id or f"reader:{hashlib.sha256(text.encode('utf-8')).hexdigest()}",
            },
        }
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
        value = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"bootstrap_done": False, "processed_ids": [], "session_id": None}
    except json.JSONDecodeError as exc:
        # Resetting a damaged cursor silently would replay the whole session.
        # Keep the evidence intact and let systemd surface the failure for
        # recovery instead of replacing it with an empty state.
        raise RuntimeError(f"invalid reader state JSON: {STATE_PATH}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"reader state must be a JSON object: {STATE_PATH}")
    return value


def save_state(
    bootstrap_done: bool,
    processed: set[str],
    session_id: str | None,
    frozen_batch: list[tuple[str, str]] | None = None,
    frozen_batch_ids: list[str] | None = None,
    frozen_batch_request_id: str | None = None,
    frozen_batch_session_id: str | None = None,
    processed_cursor: int = 0,
    processed_cursor_id: str | None = None,
) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "bootstrap_done": bootstrap_done,
        "processed_ids": sorted(processed)[-MAX_PROCESSED_IDS:],
        "processed_cursor": max(0, int(processed_cursor)),
        "processed_cursor_id": processed_cursor_id,
        "session_id": session_id,
    }
    # Persist in-flight frozen batch so a crash between UDS success and
    # processed_ids save does not cause the old User entries to be split
    # into a new requestId and double-submitted on restart.
    # frozen_batch_session_id pins the batch to the session it was created
    # in; on restore a mismatched session_id discards the stale batch.
    if frozen_batch is not None:
        payload["frozen_batch"] = frozen_batch
    if frozen_batch_ids is not None:
        payload["frozen_batch_ids"] = frozen_batch_ids
    if frozen_batch_request_id is not None:
        payload["frozen_batch_request_id"] = frozen_batch_request_id
    if frozen_batch_session_id is not None:
        payload["frozen_batch_session_id"] = frozen_batch_session_id
    fd, tmp_name = tempfile.mkstemp(prefix=f".{STATE_PATH.name}.", suffix=".tmp", dir=STATE_PATH.parent)
    tmp = Path(tmp_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, STATE_PATH)
        try:
            dir_fd = os.open(STATE_PATH.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    state = load_state()
    processed: set[str] = set(state.get("processed_ids") or [])
    bootstrap_done = bool(state.get("bootstrap_done"))
    try:
        processed_cursor = max(0, int(state.get("processed_cursor") or 0))
    except (TypeError, ValueError):
        processed_cursor = 0
    processed_cursor_id = state.get("processed_cursor_id") if isinstance(state.get("processed_cursor_id"), str) else None
    res = resolve_miku_session()
    session_id = res[0] if res is not None else state.get("session_id")
    if res is not None and state.get("session_id") != session_id:
        processed.clear()
        bootstrap_done = False
        processed_cursor = 0
        processed_cursor_id = None
    pending_context: list[tuple[str, str, set[str]]] = []
    pending_context_ids: set[str] = set()
    pending_users: list[tuple[str, str]] = []
    pending_user_ids: set[str] = set()
    bootstrap_queued = False
    stop_requested = False
    # Restore frozen batch persisted before a crash (P1 fix).  If previous
    # run froze a batch but stopped before processed_ids was updated, the
    # same requestId must be retried instead of splitting old+new Users.
    # Guard: if the batch was recorded in a different session, discard it
    # — otherwise a session switch would mis-deliver the old batch into the
    # new session (Codex review 4616355 P1.1).
    _fb = state.get("frozen_batch")
    _fb_ids = state.get("frozen_batch_ids")
    _fb_rid = state.get("frozen_batch_request_id")
    _fb_sid = state.get("frozen_batch_session_id")
    if isinstance(_fb, list) and isinstance(_fb_ids, list) and isinstance(_fb_rid, str) and _fb and _fb_ids and _fb_rid:
        # Session-bound guard (Codex 0030528 review): legacy state without
        # frozen_batch_session_id must be handled by comparing state.session_id
        # to current session_id — same session → restore with old requestId
        # (idempotent retry, avoids double-turn); mismatch/missing → discard.
        _has_session_guard = "frozen_batch_session_id" in state
        if not _has_session_guard:
            _state_sid = state.get("session_id") if isinstance(state.get("session_id"), str) else state.get("sessionId")
            if isinstance(_state_sid, str) and _state_sid and _state_sid == session_id:
                try:
                    pending_user_batch = [(str(a), str(b)) for a, b in _fb if isinstance(a, str) and isinstance(b, str)]  # type: ignore[union-attr]
                    pending_user_batch_ids = [str(x) for x in _fb_ids if isinstance(x, str)]  # type: ignore[union-attr]
                    pending_user_batch_request_id = _fb_rid  # type: ignore[assignment]
                    if not pending_user_batch or not pending_user_batch_ids:
                        raise ValueError("empty frozen batch")
                    log(f"restored frozen batch (legacy same-session) {len(pending_user_batch)} entries requestId={_fb_rid[:16]}...")  # type: ignore[index]
                except Exception as e:
                    log(f"frozen batch restore failed: {e}")
                    pending_user_batch = None
                    pending_user_batch_ids = None
                    pending_user_batch_request_id = None
            else:
                log("discarding frozen batch: legacy state without frozen_batch_session_id (session mismatch or missing)")
                pending_user_batch = None
                pending_user_batch_ids = None
                pending_user_batch_request_id = None
        elif _fb_sid != session_id:
            log(f"discarding frozen batch: session mismatch {str(_fb_sid)[:16]}.. != {str(session_id)[:16]}..")
            pending_user_batch = None
            pending_user_batch_ids = None
            pending_user_batch_request_id = None
        else:
            try:
                pending_user_batch: list[tuple[str, str]] | None = [(str(a), str(b)) for a, b in _fb if isinstance(a, str) and isinstance(b, str)]
                pending_user_batch_ids: list[str] | None = [str(x) for x in _fb_ids if isinstance(x, str)]
                pending_user_batch_request_id: str | None = _fb_rid
                if not pending_user_batch or not pending_user_batch_ids:
                    raise ValueError("empty frozen batch")
                log(f"restored frozen batch {len(pending_user_batch)} entries requestId={_fb_rid[:16]}...")
            except Exception as e:
                log(f"frozen batch restore failed: {e}")
                pending_user_batch = None
                pending_user_batch_ids = None
                pending_user_batch_request_id = None
    else:
        pending_user_batch = None
        pending_user_batch_ids = None
        pending_user_batch_request_id = None

    while not stop_requested:
        try:
            res = resolve_miku_session()
            if res is not None and res[0] != session_id:
                session_id = res[0]
                processed.clear()
                bootstrap_done = False
                bootstrap_queued = False
                pending_context.clear()
                pending_context_ids.clear()
                pending_users.clear()
                pending_user_ids.clear()
                pending_user_batch = None
                pending_user_batch_ids = None
                pending_user_batch_request_id = None
                processed_cursor = 0
                processed_cursor_id = None
            records = read_records()
            if processed_cursor and (
                processed_cursor > len(records)
                or not processed_cursor_id
                or record_id(records[processed_cursor - 1]) != processed_cursor_id
            ):
                log("reader cursor mismatch; replaying current session with stable request IDs")
                processed_cursor = 0
                processed_cursor_id = None
            new_records: list[dict[str, Any]] = []
            for index, r in enumerate(records):
                rid = record_id(r)
                if index >= processed_cursor and rid not in processed:
                    r["_rid"] = rid
                    new_records.append(r)
            if not bootstrap_done and not bootstrap_queued:
                context = build_bootstrap_context(records)
                if context:
                    ids = {record_id(r) for r in records}
                    request_id = stable_request_id("bootstrap", session_id, sorted(ids), context)
                    pending_context.insert(0, (request_id, context, ids))
                    pending_context_ids.update(ids)
                    bootstrap_queued = True
                pending_users.clear()
                pending_user_ids.clear()
            else:
                for r in new_records:
                    rid = r["_rid"]
                    if rid in pending_context_ids or rid in pending_user_ids or (pending_user_batch_ids is not None and rid in pending_user_batch_ids):
                        continue
                    lab = label_record(r)
                    if lab not in {"User", "Miku"}:
                        processed.add(rid)
                        continue
                    if lab == "User":
                        if is_stop_instruction(r["_text"]):
                            stop_requested = True
                            break
                        pending_users.append((rid, f"User [{r.get('timestamp','')}]:\n{r['_text']}"))
                        pending_user_ids.add(rid)
                    elif lab == "Miku":
                        context = f"Miku [{r.get('timestamp','')}]:\n{r['_text']}"
                        request_id = stable_request_id("context", session_id, [rid], context)
                        pending_context.append((request_id, context, {rid}))
                        pending_context_ids.add(rid)

            def flush_context() -> bool:
                for request_id, context, ids in list(pending_context):
                    if uds_submit(context, "miku", request_id=request_id) is None:
                        return False
                    pending_context.remove((request_id, context, ids))
                    pending_context_ids.difference_update(ids)
                    processed.update(ids)
                return True

            def flush_users() -> bool:
                nonlocal pending_user_batch, pending_user_batch_ids, pending_user_batch_request_id
                if not pending_users and pending_user_batch is None:
                    return True
                if pending_user_batch is None:
                    pending_user_batch = list(pending_users)
                    pending_user_batch_ids = [rid for rid, _ in pending_user_batch]
                    text = "Codex: Telegramからの新規指示。\n" + "\n\n".join(t for _, t in pending_user_batch)
                    pending_user_batch_request_id = stable_request_id("telegram", session_id, pending_user_batch_ids, text)
                    pending_users.clear()
                    pending_user_ids.clear()
                    # Persist frozen batch before UDS so a crash between
                    # UDS success and processed_ids update can be recovered
                    # with the same requestId (P1 fix).
                    checkpoint()
                assert pending_user_batch is not None and pending_user_batch_ids is not None and pending_user_batch_request_id is not None
                batch_text = "Codex: Telegramからの新規指示。\n" + "\n\n".join(t for _, t in pending_user_batch)
                if uds_submit(batch_text, "telegram", request_id=pending_user_batch_request_id) is None:
                    return False
                processed.update(pending_user_batch_ids)
                # Clear persisted frozen batch after success
                pending_user_batch = None
                pending_user_batch_ids = None
                pending_user_batch_request_id = None
                checkpoint()
                # checkpoint() overwrites the state without frozen keys after
                # the successful submit, so a restart will not replay it.
                return True

            def checkpoint() -> None:
                nonlocal processed_cursor, processed_cursor_id
                processed_cursor, processed_cursor_id = advance_processed_cursor(
                    records, processed, processed_cursor, processed_cursor_id
                )
                save_state(
                    bootstrap_done,
                    processed,
                    session_id,
                    pending_user_batch,
                    pending_user_batch_ids,
                    pending_user_batch_request_id,
                    session_id if pending_user_batch is not None else None,
                    processed_cursor,
                    processed_cursor_id,
                )

            # flush to daemon via UDS (queue serializes; no flock needed)
            if not bootstrap_done and pending_context:
                if flush_context() and not pending_context:
                    bootstrap_done = True
                    checkpoint()
                    log("Telegram backlog submitted via UDS")
                else:
                    time.sleep(5)
                    continue
            elif pending_users or pending_user_batch is not None:
                if not flush_context():
                    time.sleep(5)
                    continue
                if not flush_users():
                    time.sleep(5)
                    continue
                # flush_users already persisted cleared frozen state; remaining
                # save covers any context flush that happened above
                checkpoint()
            elif pending_context:
                if flush_context() and not pending_context:
                    checkpoint()
            else:
                checkpoint()
        except Exception as e:
            log(f"bridge retry: {type(e).__name__}: {e}")
            time.sleep(5)
        time.sleep(POLL_SECONDS)
    log("bridge stopped by explicit Telegram instruction")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
