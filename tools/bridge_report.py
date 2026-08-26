#!/usr/bin/env python3
"""Render the bridge's durable outbox and verification log as a small dashboard."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "daemon"))

from offset_store import OffsetStore  # noqa: E402
from verification import VerificationLog  # noqa: E402


DEFAULT_OFFSET = Path.home() / ".local/state/codex-bridge/offset.json"
DEFAULT_LOG = Path.home() / ".local/state/codex-bridge/verification.jsonl"


def build_report(offset_path: Path, log_path: Path, tail: int) -> dict[str, Any]:
    offset = OffsetStore(offset_path)
    verification = VerificationLog(log_path)
    return {
        "offset": offset.summary(),
        "verification": verification.summary(),
        "recent": verification.tail(tail),
    }


def render_table(report: dict[str, Any]) -> str:
    offset = report["offset"]
    verification = report["verification"]
    pending_targets = offset.get("pendingTargets") or {}
    counts = verification.get("counts") or {}
    lines = [
        "Codex bridge verification",
        "=========================",
        f"state schema       : v{offset.get('version', '?')}",
        f"requests           : {offset.get('completedRequests', 0)} completed / {offset.get('inProgressRequests', 0)} in progress",
        f"pending replies    : {offset.get('pendingReplies', 0)}",
        f"pending fanout     : telegram={pending_targets.get('telegram', 0)} miku={pending_targets.get('miku', 0)}",
        f"completed delivery : {offset.get('completedDeliveries', 0)}",
        f"verification events: {verification.get('events', 0)}",
    ]
    if counts:
        lines.append("events             : " + ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))
    recent = report.get("recent") or []
    if recent:
        last = recent[-1]
        lines.append(f"last event         : {last.get('event', '?')} @ {last.get('ts', '?')}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offset", type=Path, default=DEFAULT_OFFSET)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--tail", type=int, default=20)
    parser.add_argument("--format", choices=("table", "json"), default="table")
    args = parser.parse_args()
    report = build_report(args.offset, args.log, max(0, args.tail))
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_table(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
