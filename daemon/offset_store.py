"""Crash-safe request idempotency and fanout outbox persistence.

The bridge has two different durability problems:

* a client may retry a request after losing its UDS response; and
* a daemon may stop between producing a reply and delivering it to either
  downstream channel.

``OffsetStore`` keeps both pieces of state in one atomically replaced JSON
file.  A reply remains in ``pendingReplies`` until *both* fanout targets have
confirmed delivery.  The older ``delivered``/``pending`` fields are retained
as a compatibility mirror for the C' ver.2 format.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any


TARGETS = ("telegram", "miku")
SCHEMA_VERSION = 2
MAX_REQUESTS = 5000
MAX_COMPLETED_DELIVERIES = 5000
MAX_LEGACY_HASHES = 2000


class RequestConflict(ValueError):
    """Raised when one request id is reused for different input."""


def request_fingerprint(source: str, text: str) -> str:
    """Return a stable, non-reversible key for request input."""

    raw = f"{source}\0{text}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def request_key(request_id: str) -> str:
    """Return a short key suitable for verification logs."""

    return hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:16]


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON so a process stop cannot leave a half-written state file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        try:
            dir_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            # Directory fsync is not available on every test filesystem.  The
            # atomic replace is still useful in that environment.
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


class OffsetStore:
    """Persistent request ledger and per-target reply outbox."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = self._load()
        migrated = self._normalize()
        if migrated:
            self.save()

    @staticmethod
    def _default_data() -> dict[str, Any]:
        return {
            "version": SCHEMA_VERSION,
            "delivered": [],
            "pending": [],
            "pendingReplies": [],
            "completedDeliveries": [],
            "requests": {},
        }

    def _load(self) -> dict[str, Any]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return self._default_data()
        return value if isinstance(value, dict) else self._default_data()

    @staticmethod
    def _target_state(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            delivered = bool(value.get("delivered", False))
            try:
                attempts = max(0, int(value.get("attempts", 0)))
            except (TypeError, ValueError):
                attempts = 0
            result = {
                "delivered": delivered,
                "attempts": attempts,
            }
            for key in ("lastError", "deliveredAt", "lastAttemptAt", "nextAttemptAt"):
                if key in value and value[key] is not None:
                    result[key] = value[key]
            return result
        return {"delivered": bool(value), "attempts": 0}

    def _new_delivery(
        self,
        reply: str,
        thread_id: str,
        turn_id: str,
        request_id: str,
        created_at: float | None = None,
    ) -> dict[str, Any]:
        reply_hash = hashlib.sha256(reply.encode("utf-8")).hexdigest()
        seed = "\0".join((request_id, thread_id, turn_id, reply_hash)).encode("utf-8")
        delivery_id = hashlib.sha256(seed).hexdigest()
        return {
            "deliveryId": delivery_id,
            "requestId": request_id,
            "reply": reply,
            "replyHash": reply_hash,
            "threadId": thread_id,
            "turnId": turn_id,
            "createdAt": created_at if created_at is not None else time.time(),
            "targets": {target: {"delivered": False, "attempts": 0} for target in TARGETS},
        }

    def _normalize(self) -> bool:
        """Migrate C' ver.2 state and repair malformed optional fields."""

        changed = False
        if self.data.get("version") != SCHEMA_VERSION:
            self.data["version"] = SCHEMA_VERSION
            changed = True
        if not isinstance(self.data.get("delivered"), list):
            self.data["delivered"] = []
            changed = True
        if not isinstance(self.data.get("completedDeliveries"), list):
            self.data["completedDeliveries"] = []
            changed = True
        if not isinstance(self.data.get("requests"), dict):
            self.data["requests"] = {}
            changed = True

        pending_replies = self.data.get("pendingReplies")
        if not isinstance(pending_replies, list):
            pending_replies = []
            self.data["pendingReplies"] = pending_replies
            changed = True

        # The first C' ver.2 implementation used ``pending`` entries without
        # per-target state.  Convert them once; version 2 writes ``pending``
        # only as a compatibility mirror and must not be converted again.
        legacy_pending = self.data.get("pending")
        if self.data.get("version") == SCHEMA_VERSION and not pending_replies and isinstance(legacy_pending, list):
            for item in legacy_pending:
                if not isinstance(item, dict) or not isinstance(item.get("reply"), str):
                    continue
                pending_replies.append(
                    self._new_delivery(
                        item["reply"],
                        str(item.get("threadId") or ""),
                        str(item.get("turnId") or ""),
                        str(item.get("requestId") or f"legacy:{hashlib.sha256(item['reply'].encode()).hexdigest()}"),
                        float(item.get("ts") or time.time()),
                    )
                )
                changed = True

        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in pending_replies:
            if not isinstance(item, dict) or not isinstance(item.get("reply"), str):
                changed = True
                continue
            reply = item["reply"]
            request_id = str(item.get("requestId") or f"legacy:{hashlib.sha256(reply.encode()).hexdigest()}")
            thread_id = str(item.get("threadId") or "")
            turn_id = str(item.get("turnId") or "")
            fresh = self._new_delivery(reply, thread_id, turn_id, request_id, item.get("createdAt"))
            fresh["deliveryId"] = str(item.get("deliveryId") or fresh["deliveryId"])
            fresh["replyHash"] = str(item.get("replyHash") or fresh["replyHash"])
            targets = item.get("targets") or {}
            fresh["targets"] = {target: self._target_state(targets.get(target)) for target in TARGETS}
            if fresh["deliveryId"] in seen:
                changed = True
                continue
            seen.add(fresh["deliveryId"])
            normalized.append(fresh)
        if normalized != pending_replies:
            self.data["pendingReplies"] = normalized
            changed = True

        # Keep the new ledger bounded.  Completed request entries are retained
        # for retries; in-progress entries are never discarded automatically.
        changed = self._trim_requests() or changed

        completed_ids = [str(value) for value in self.data["completedDeliveries"] if value]
        if len(completed_ids) > MAX_COMPLETED_DELIVERIES:
            self.data["completedDeliveries"] = completed_ids[-MAX_COMPLETED_DELIVERIES:]
            changed = True
        else:
            self.data["completedDeliveries"] = completed_ids

        return changed

    def _trim_requests(self) -> bool:
        requests = self.data.get("requests")
        if not isinstance(requests, dict) or len(requests) <= MAX_REQUESTS:
            return False
        completed = [
            (key, value)
            for key, value in requests.items()
            if isinstance(value, dict) and value.get("status") == "completed"
        ]
        completed.sort(key=lambda pair: float(pair[1].get("updatedAt", 0)))
        drop = max(0, len(requests) - MAX_REQUESTS)
        changed = False
        for key, _ in completed[:drop]:
            requests.pop(key, None)
            changed = True
        return changed

    def maybe_trim(self) -> bool:
        """Trim completed requests if the live ledger exceeds MAX_REQUESTS."""

        if self._trim_requests():
            self.save()
            return True
        return False

    def save(self) -> None:
        # ``pending`` is intentionally only a downgrade-compatible mirror;
        # all new code reads ``pendingReplies`` with target-level state.
        self.data["pending"] = [
            {
                "reply": item["reply"],
                "threadId": item.get("threadId", ""),
                "turnId": item.get("turnId", ""),
                "requestId": item.get("requestId", ""),
                "ts": item.get("createdAt", time.time()),
            }
            for item in self.data.get("pendingReplies", [])
        ]
        _atomic_write_json(self.path, self.data)

    def _request_entry(self, request_id: str, fingerprint: str) -> dict[str, Any] | None:
        entry = self.data.get("requests", {}).get(request_id)
        if not isinstance(entry, dict):
            return None
        if entry.get("fingerprint") != fingerprint:
            raise RequestConflict(f"request id already used for different input: {request_key(request_id)}")
        return entry

    def begin_request(self, request_id: str, fingerprint: str) -> dict[str, Any] | None:
        """Record an attempt, returning a durable completed result if present."""

        requests = self.data.setdefault("requests", {})
        entry = self._request_entry(request_id, fingerprint)
        if entry is not None and entry.get("status") == "completed":
            result = entry.get("result")
            return copy.deepcopy(result) if isinstance(result, dict) else None

        now = time.time()
        if entry is None:
            entry = {
                "fingerprint": fingerprint,
                "status": "in_progress",
                "attempts": 0,
                "createdAt": now,
            }
            requests[request_id] = entry
        entry["status"] = "in_progress"
        entry["attempts"] = int(entry.get("attempts", 0)) + 1
        entry["lastAttemptAt"] = now
        self.save()
        return None

    def finish_request(self, request_id: str, fingerprint: str, result: dict[str, Any]) -> str | None:
        """Atomically persist a completed response and its fanout outbox item."""

        requests = self.data.setdefault("requests", {})
        entry = self._request_entry(request_id, fingerprint)
        if entry is not None and entry.get("status") == "completed":
            existing = entry.get("result")
            return None if not isinstance(existing, dict) else existing.get("deliveryId")

        delivery_id: str | None = None
        reply = str(result.get("reply") or "")
        if reply:
            thread_id = str(result.get("threadId") or "")
            turn_id = str(result.get("turnId") or "")
            existing_delivery = next(
                (
                    item
                    for item in self.data.setdefault("pendingReplies", [])
                    if item.get("requestId") == request_id
                ),
                None,
            )
            if existing_delivery is None:
                delivery = self._new_delivery(reply, thread_id, turn_id, request_id)
                self.data["pendingReplies"].append(delivery)
                delivery_id = delivery["deliveryId"]
            else:
                delivery_id = str(existing_delivery.get("deliveryId"))

        stored_result = copy.deepcopy(result)
        if delivery_id:
            stored_result["deliveryId"] = delivery_id
        now = time.time()
        requests[request_id] = {
            "fingerprint": fingerprint,
            "status": "completed",
            "attempts": int((entry or {}).get("attempts", 1)),
            "createdAt": (entry or {}).get("createdAt", now),
            "updatedAt": now,
            "result": stored_result,
        }
        self.save()
        return delivery_id

    def enqueue_reply(self, reply: str, thread_id: str, turn_id: str, request_id: str) -> str:
        """Add a reply to the durable outbox unless it already exists."""

        existing = next(
            (item for item in self.data.setdefault("pendingReplies", []) if item.get("requestId") == request_id),
            None,
        )
        if existing is not None:
            return str(existing["deliveryId"])
        delivery = self._new_delivery(reply, thread_id, turn_id, request_id)
        if delivery["deliveryId"] not in self.data.setdefault("completedDeliveries", []):
            self.data["pendingReplies"].append(delivery)
        self.save()
        return delivery["deliveryId"]

    def pending_replies(self, now: float | None = None) -> list[dict[str, Any]]:
        current = time.time() if now is None else now
        result: list[dict[str, Any]] = []
        for item in self.data.get("pendingReplies", []):
            targets = item.get("targets") or {}
            if all(bool((targets.get(target) or {}).get("delivered")) for target in TARGETS):
                continue
            available = [
                float((targets.get(target) or {}).get("nextAttemptAt", 0))
                for target in TARGETS
                if not (targets.get(target) or {}).get("delivered")
            ]
            if not available or min(available) <= current:
                result.append(copy.deepcopy(item))
        return result

    def get_delivery(self, delivery_id: str) -> dict[str, Any] | None:
        for item in self.data.get("pendingReplies", []):
            if item.get("deliveryId") == delivery_id:
                return copy.deepcopy(item)
        return None

    def mark_target_delivered(self, delivery_id: str, target: str) -> bool:
        if target not in TARGETS:
            raise ValueError(f"unknown fanout target: {target}")
        for item in self.data.get("pendingReplies", []):
            if item.get("deliveryId") != delivery_id:
                continue
            state = item.setdefault("targets", {}).setdefault(target, {"delivered": False, "attempts": 0})
            if state.get("delivered"):
                return False
            state["attempts"] = max(1, int(state.get("attempts", 0)))
            state["delivered"] = True
            state["deliveredAt"] = time.time()
            state.pop("lastError", None)
            state.pop("nextAttemptAt", None)
            targets = item["targets"]
            if all(bool((targets.get(name) or {}).get("delivered")) for name in TARGETS):
                self.data.setdefault("completedDeliveries", []).append(delivery_id)
                self.data["completedDeliveries"] = self.data["completedDeliveries"][-MAX_COMPLETED_DELIVERIES:]
                self.data["pendingReplies"] = [
                    candidate
                    for candidate in self.data["pendingReplies"]
                    if candidate.get("deliveryId") != delivery_id
                ]
            self.save()
            return True
        return False

    def mark_target_failed(self, delivery_id: str, target: str, error: str, retry_after: float | None = None) -> bool:
        if target not in TARGETS:
            raise ValueError(f"unknown fanout target: {target}")
        for item in self.data.get("pendingReplies", []):
            if item.get("deliveryId") != delivery_id:
                continue
            state = item.setdefault("targets", {}).setdefault(target, {"delivered": False, "attempts": 0})
            if state.get("delivered"):
                return False
            state["attempts"] = int(state.get("attempts", 0)) + 1
            state["lastAttemptAt"] = time.time()
            state["lastError"] = str(error)[:500]
            delay = retry_after if retry_after is not None else min(60.0, 2.0 ** min(state["attempts"], 6))
            state["nextAttemptAt"] = time.time() + max(0.0, delay)
            self.save()
            return True
        return False

    def is_delivery_complete(self, delivery_id: str) -> bool:
        if delivery_id in set(self.data.get("completedDeliveries") or []):
            return True
        item = self.get_delivery(delivery_id)
        return bool(item and all(bool((item.get("targets", {}).get(target) or {}).get("delivered")) for target in TARGETS))

    # Compatibility helpers for callers of the original C' ver.2 class.
    def is_delivered(self, reply_hash: str) -> bool:
        return reply_hash in set(self.data.get("delivered") or [])

    def mark_delivered(self, reply_hash: str) -> None:
        hashes = self.data.setdefault("delivered", [])
        if reply_hash not in hashes:
            hashes.append(reply_hash)
            self.data["delivered"] = hashes[-MAX_LEGACY_HASHES:]
            self.save()

    def summary(self) -> dict[str, Any]:
        pending = self.data.get("pendingReplies") or []
        pending_targets = {
            target: sum(
                1
                for item in pending
                if not bool(((item.get("targets") or {}).get(target) or {}).get("delivered"))
            )
            for target in TARGETS
        }
        requests = self.data.get("requests") or {}
        return {
            "version": self.data.get("version", SCHEMA_VERSION),
            "requests": len(requests),
            "completedRequests": sum(1 for item in requests.values() if isinstance(item, dict) and item.get("status") == "completed"),
            "inProgressRequests": sum(1 for item in requests.values() if isinstance(item, dict) and item.get("status") == "in_progress"),
            "pendingReplies": len(pending),
            "pendingTargets": pending_targets,
            "completedDeliveries": len(self.data.get("completedDeliveries") or []),
        }
