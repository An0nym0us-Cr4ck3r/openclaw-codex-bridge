#!/usr/bin/env python3
import json
import tempfile
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "daemon"))
from offset_store import OffsetStore, RequestConflict, request_fingerprint


def test_restart_idempotency_and_outbox() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        store = OffsetStore(path)
        fingerprint = request_fingerprint("miku", "hello")
        assert store.begin_request("req-1", fingerprint) is None
        result = {"reply": "world", "threadId": "thread-1", "turnId": "turn-1"}
        delivery_id = store.finish_request("req-1", fingerprint, result)
        assert delivery_id

        restarted = OffsetStore(path)
        cached = restarted.begin_request("req-1", fingerprint)
        assert cached is not None
        assert cached["reply"] == "world"
        assert cached["deliveryId"] == delivery_id
        assert len(restarted.pending_replies()) == 1

        assert restarted.mark_target_delivered(delivery_id, "telegram")
        assert len(restarted.pending_replies()) == 1
        assert restarted.mark_target_delivered(delivery_id, "miku")
        assert restarted.pending_replies() == []
        assert restarted.is_delivery_complete(delivery_id)

        restarted_again = OffsetStore(path)
        assert restarted_again.is_delivery_complete(delivery_id)
        assert restarted_again.begin_request("req-1", fingerprint)["reply"] == "world"


def test_retry_backoff_and_conflict() -> None:
    with tempfile.TemporaryDirectory() as directory:
        store = OffsetStore(Path(directory) / "offset.json")
        fingerprint = request_fingerprint("telegram", "same")
        store.begin_request("req-2", fingerprint)
        delivery_id = store.finish_request("req-2", fingerprint, {"reply": "ok", "threadId": "t", "turnId": "u"})
        assert delivery_id
        assert store.mark_target_failed(delivery_id, "telegram", "temporary", retry_after=60)
        pending_now = store.pending_replies(now=time.time())
        assert len(pending_now) == 1
        assert pending_now[0]["targets"]["telegram"]["delivered"] is False
        assert len(store.pending_replies(now=time.time() + 61)) == 1
        try:
            store.begin_request("req-2", request_fingerprint("telegram", "different"))
        except RequestConflict:
            pass
        else:
            raise AssertionError("request id conflict was not detected")


def test_legacy_pending_migration() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        path.write_text(json.dumps({"delivered": [], "pending": [{"reply": "old", "threadId": "t", "ts": 1}]}), encoding="utf-8")
        store = OffsetStore(path)
        pending = store.pending_replies()
        assert len(pending) == 1
        assert pending[0]["reply"] == "old"
        assert pending[0]["targets"]["telegram"]["delivered"] is False
        assert json.loads(path.read_text(encoding="utf-8"))["version"] == 2


def main() -> None:
    test_restart_idempotency_and_outbox()
    test_retry_backoff_and_conflict()
    test_legacy_pending_migration()
    print("PASS offset_store")


if __name__ == "__main__":
    main()
