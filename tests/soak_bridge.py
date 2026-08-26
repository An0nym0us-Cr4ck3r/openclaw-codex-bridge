#!/usr/bin/env python3
"""Offline durability soak test for request deduplication and fanout recovery."""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "daemon"))
from offset_store import OffsetStore, request_fingerprint


def run(count: int, crash_every: int) -> None:
    if count < 1 or crash_every < 1:
        raise ValueError("count and crash-every must be positive")
    from offset_store import MAX_REQUESTS

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        store = OffsetStore(path)
        for index in range(count):
            request_id = f"soak:{index}"
            fingerprint = request_fingerprint("soak", request_id)
            cached = store.begin_request(request_id, fingerprint)
            if cached is None:
                result = {
                    "reply": f"reply-{index}",
                    "threadId": "soak-thread",
                    "turnId": f"turn-{index}",
                }
                delivery_id = store.finish_request(request_id, fingerprint, result)
                assert delivery_id
            else:
                assert cached["reply"] == f"reply-{index}"

            # A client may retry after receiving no UDS response.  The stored
            # result must be replayed without creating a second Codex turn.
            replay = store.begin_request(request_id, fingerprint)
            assert replay is not None and replay["reply"] == f"reply-{index}"

            if index % crash_every == 0:
                store = OffsetStore(path)
            pending = store.pending_replies(now=10**12)
            item = next(item for item in pending if item["requestId"] == request_id)
            delivery_id = item["deliveryId"]
            store.mark_target_delivered(delivery_id, "telegram")
            if index % crash_every == 0:
                store = OffsetStore(path)
            store.mark_target_delivered(delivery_id, "miku")

        assert store.pending_replies(now=10**12) == []
        summary = store.summary()
        # Ledger and delivery history are both bounded (MAX_REQUESTS /
        # MAX_COMPLETED_DELIVERIES are both 5000; see daemon/offset_store.py).
        from offset_store import MAX_COMPLETED_DELIVERIES

        expected_requests = min(count, MAX_REQUESTS)
        expected_deliveries = min(count, MAX_COMPLETED_DELIVERIES)
        assert summary["completedRequests"] == expected_requests, (
            f"expected {expected_requests} completedRequests, got {summary['completedRequests']}"
        )
        assert summary["completedDeliveries"] == expected_deliveries, (
            f"expected {expected_deliveries} completedDeliveries, got {summary['completedDeliveries']}"
        )
        # When trimmed, pending must still be 0 (no loss, just history eviction).
        assert summary["pendingReplies"] == 0
        print(f"PASS soak_bridge count={count} crash_every={crash_every}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--crash-every", type=int, default=37)
    args = parser.parse_args()
    run(args.count, args.crash_every)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
