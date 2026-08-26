"""Small durable JSONL event log used by the bridge verification report."""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any


class VerificationLog:
    """Append-only, secret-free operational events.

    Event payloads deliberately contain identifiers, counts, and hashes only;
    request text and Codex replies stay in the local outbox when they need to
    be replayed and are never copied into this log.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def append(self, event: str, **fields: Any) -> dict[str, Any]:
        record: dict[str, Any] = {
            "ts": time.time(),
            "event": event,
        }
        record.update(fields)
        line = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.fchmod(fd, 0o600)
                payload = (line + "\n").encode("utf-8")
                while payload:
                    written = os.write(fd, payload)
                    if written <= 0:
                        raise OSError("verification log write made no progress")
                    payload = payload[written:]
                os.fsync(fd)
            finally:
                os.close(fd)
        return record

    def tail(self, limit: int = 100) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        try:
            lines = self.path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]
        except FileNotFoundError:
            return []
        result: list[dict[str, Any]] = []
        for line in lines:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                result.append(value)
        return result

    def summary(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        targets: dict[str, dict[str, int]] = {}
        total = 0
        first_ts: float | None = None
        last_ts: float | None = None
        try:
            handle = self.path.open(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return {"events": 0, "counts": {}, "fanout": {}}
        with handle:
            for line in handle:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(value, dict) or not isinstance(value.get("event"), str):
                    continue
                total += 1
                event = value["event"]
                counts[event] = counts.get(event, 0) + 1
                ts = value.get("ts")
                if isinstance(ts, (int, float)):
                    first_ts = ts if first_ts is None else min(first_ts, ts)
                    last_ts = ts if last_ts is None else max(last_ts, ts)
                target = value.get("target")
                if isinstance(target, str):
                    target_counts = targets.setdefault(target, {})
                    target_counts[event] = target_counts.get(event, 0) + 1
        return {
            "events": total,
            "counts": counts,
            "fanout": targets,
            "firstTs": first_ts,
            "lastTs": last_ts,
        }
