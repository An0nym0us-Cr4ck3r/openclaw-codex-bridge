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


def test_target_chunk_progress_is_durable() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        store = OffsetStore(path)
        fingerprint = request_fingerprint("miku", "chunked")
        store.begin_request("req-chunked", fingerprint)
        delivery_id = store.finish_request(
            "req-chunked",
            fingerprint,
            {"reply": "long", "threadId": "t", "turnId": "u"},
        )
        assert delivery_id
        assert store.mark_target_chunk_delivered(delivery_id, "telegram", 0, 3)
        assert not store.mark_target_chunk_delivered(delivery_id, "telegram", 0, 3)
        assert store.get_delivery(delivery_id)["targets"]["telegram"]["chunkIndex"] == 1
        restarted = OffsetStore(path)
        assert restarted.mark_target_chunk_delivered(delivery_id, "telegram", 1, 3)
        assert restarted.mark_target_chunk_delivered(delivery_id, "telegram", 2, 3)
        assert restarted.get_delivery(delivery_id)["targets"]["telegram"]["delivered"] is True


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


def test_current_empty_outbox_does_not_remigrate_compatibility_mirror() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        path.write_text(
            json.dumps(
                {
                    "version": 2,
                    "delivered": [],
                    "pending": [{"reply": "already-delivered", "threadId": "t", "ts": 1}],
                    "pendingReplies": [],
                    "completedDeliveries": [],
                    "requests": {},
                }
            ),
            encoding="utf-8",
        )
        store = OffsetStore(path)
        assert store.pending_replies() == []
        store.save()
        assert json.loads(path.read_text(encoding="utf-8"))["pending"] == []


def test_corrupt_state_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        path.write_text("{not-json", encoding="utf-8")
        try:
            OffsetStore(path)
        except RuntimeError as exc:
            assert "invalid offset state JSON" in str(exc)
        else:
            raise AssertionError("corrupt offset state was silently accepted")
        assert path.read_text(encoding="utf-8") == "{not-json"


def test_trim_tolerates_invalid_timestamps() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        store = OffsetStore(path)
        for index in range(5001):
            store.data["requests"][f"req-{index}"] = {
                "fingerprint": f"fp-{index}",
                "status": "completed",
                "updatedAt": "not-a-timestamp" if index == 0 else index,
                "result": {"reply": ""},
            }
        assert store.maybe_trim()
        assert len(store.data["requests"]) == 5000


def test_stale_in_progress_reap() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        store = OffsetStore(path)
        fingerprint = request_fingerprint("miku", "stuck")
        assert store.begin_request("req-stale", fingerprint) is None
        # Simulate a crash leaving the request in_progress without a reply.
        stale_created = time.time() - 700
        store.data["requests"]["req-stale"]["createdAt"] = stale_created
        store.data["requests"]["req-stale"]["lastAttemptAt"] = stale_created
        store.save()
        # Reload simulates the next daemon boot; begin_request must reap and allow a retry.
        reloaded = OffsetStore(path)
        assert reloaded.begin_request("req-stale", fingerprint) is None
        assert reloaded.data["requests"]["req-stale"]["attempts"] == 1
        assert reloaded.data["requests"]["req-stale"]["createdAt"] > stale_created
        # The watchdog also reaps when left idle.
        reloaded.data["requests"]["req-stale"]["lastAttemptAt"] = time.time() - 700
        reloaded.data["requests"]["req-stale"]["createdAt"] = time.time() - 700
        reloaded.save()
        reaped = OffsetStore(path).reap_stale_requests()
        assert "req-stale" in reaped
        empty = OffsetStore(path)
        assert "req-stale" not in empty.data.get("requests", {})
        # Completed entries are never reaped.
        done_store = OffsetStore(Path(directory) / "done.json")
        fp2 = request_fingerprint("miku", "done")
        done_store.begin_request("req-done", fp2)
        done_store.finish_request("req-done", fp2, {"reply": "ok", "threadId": "t", "turnId": "u"})
        done_store.data["requests"]["req-done"]["updatedAt"] = time.time() - 9999
        done_store.save()
        assert OffsetStore(Path(directory) / "done.json").reap_stale_requests() == []
        assert "req-done" in OffsetStore(Path(directory) / "done.json").data["requests"]


def test_stale_reap_excludes_live_requests() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "offset.json"
        store = OffsetStore(path)
        old = time.time() - 700
        for request_id in ("req-live", "req-crashed"):
            fp = request_fingerprint("miku", request_id)
            store.begin_request(request_id, fp)
            store.data["requests"][request_id]["createdAt"] = old
            store.data["requests"][request_id]["lastAttemptAt"] = old
        store.save()
        reaped = store.reap_stale_requests(exclude_request_ids={"req-live"})
        assert reaped == ["req-crashed"]
        assert "req-live" in store.data["requests"]
        assert "req-crashed" not in store.data["requests"]


def main() -> None:
    test_restart_idempotency_and_outbox()
    test_retry_backoff_and_conflict()
    test_target_chunk_progress_is_durable()
    test_legacy_pending_migration()
    test_current_empty_outbox_does_not_remigrate_compatibility_mirror()
    test_corrupt_state_fails_closed()
    test_trim_tolerates_invalid_timestamps()
    test_stale_in_progress_reap()
    test_stale_reap_excludes_live_requests()
    print("PASS offset_store")


if __name__ == "__main__":
    main()
