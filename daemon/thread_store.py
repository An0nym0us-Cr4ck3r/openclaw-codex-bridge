"""ThreadStore — activeThreadId + rotation on systemError / item limit."""
from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any


DEFAULT_LIMIT_ITEMS = 12000
DEFAULT_LIMIT_TURNS = 50


class ThreadStore:
    def __init__(self, path: Path, limit_items: int = DEFAULT_LIMIT_ITEMS, limit_turns: int = DEFAULT_LIMIT_TURNS) -> None:
        self.path = path
        self.limit_items = limit_items
        self.limit_turns = limit_turns
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            self._data = {}
            return
        except json.JSONDecodeError as exc:
            # Never silently replace a damaged state file with an empty one:
            # doing so can make the daemon select a different Codex thread
            # and lose the context pointer.  Atomic writes should make this
            # rare; failing closed preserves the evidence for recovery.
            raise RuntimeError(f"invalid thread state JSON: {self.path}") from exc
        if not isinstance(value, dict):
            raise RuntimeError(f"thread state must be a JSON object: {self.path}")
        self._data = value

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent)
        tmp = Path(tmp_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self._data, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
            try:
                dir_fd = os.open(self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                # Some test filesystems do not support directory fsync.  The
                # atomic file replacement still prevents torn JSON content.
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

    @property
    def active_thread_id(self) -> str | None:
        v = self._data.get("activeThreadId")
        return v if isinstance(v, str) and v else None

    def set_active(self, thread_id: str, forked_from: str | None = None) -> None:
        self._data["activeThreadId"] = thread_id
        self._data["updatedAt"] = int(time.time())
        if forked_from:
            self._data["forkedFrom"] = forked_from
        else:
            self._data.pop("forkedFrom", None)
        self._save()

    def record_usage(self, item_count: int, turn_count: int) -> None:
        self._data["itemCount"] = item_count
        self._data["turnCount"] = turn_count
        self._data["updatedAt"] = int(time.time())
        self._save()

    def needs_rotation(self, *, status_type: str | None, item_count: int, turn_count: int) -> bool:
        if status_type == "systemError":
            return True
        if item_count >= self.limit_items:
            return True
        if turn_count >= self.limit_turns:
            return True
        return False

    def to_dict(self) -> dict[str, Any]:
        return dict(self._data)
