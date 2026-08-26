"""ThreadStore — activeThreadId + rotation on systemError / item limit."""
from __future__ import annotations

import json
import os
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
            self._data = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)

    @property
    def active_thread_id(self) -> str | None:
        v = self._data.get("activeThreadId")
        return v if isinstance(v, str) and v else None

    def set_active(self, thread_id: str, forked_from: str | None = None) -> None:
        self._data["activeThreadId"] = thread_id
        self._data["updatedAt"] = int(time.time())
        if forked_from:
            self._data["forkedFrom"] = forked_from
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
