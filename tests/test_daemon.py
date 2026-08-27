#!/usr/bin/env python3
import asyncio
import json
import tempfile
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "daemon"))
import daemon as bridge


class FakeWS:
    def __init__(self) -> None:
        self.turns = 0

    async def thread_read(self, thread_id: str):
        return {"thread": {"status": {"type": "idle"}, "turns": []}}

    async def run_turn(self, thread_id: str, text: str):
        self.turns += 1
        return ("stable reply", f"turn-{self.turns}")


class BrokenForkWS:
    async def thread_read(self, thread_id: str):
        if thread_id == "old":
            return {
                "thread": {
                    "status": {"type": "idle"},
                    "turns": [{"items": [{"id": str(i)} for i in range(5)]}],
                }
            }
        if thread_id == "child":
            return {"thread": {"status": {"type": "systemError"}, "turns": []}}
        raise AssertionError(f"unexpected thread read: {thread_id}")

    async def request(self, method: str, params: dict):
        assert method == "thread/list"
        return {"data": []}

    async def thread_fork(self, thread_id: str) -> str:
        assert thread_id == "old"
        return "child"


class StaleThreadListWS:
    async def request(self, method: str, params: dict):
        assert method == "thread/list"
        return {
            "data": [
                {"id": "stale", "historyMode": "legacy", "status": {"type": "idle"}, "recencyAt": 2},
                {"id": "healthy", "historyMode": "legacy", "status": {"type": "idle"}, "recencyAt": 1},
            ]
        }

    async def thread_read(self, thread_id: str):
        if thread_id == "stale":
            return {"thread": {"status": {"type": "systemError"}, "turns": []}}
        if thread_id == "healthy":
            return {"thread": {"status": {"type": "idle"}, "turns": [{"items": [{"id": "one"}]}]}}
        raise AssertionError(f"unexpected thread read: {thread_id}")


def test_detailed_delivery_failure_is_not_truthy_success() -> None:
    assert bridge.normalize_delivery_result((False, "openclaw failed")) == (False, "openclaw failed")
    assert bridge.normalize_delivery_result((True, "")) == (True, "")
    assert bridge.normalize_delivery_result(False) == (False, "delivery returned false")


async def test_thread_picker_verifies_live_status_and_limits() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        instance = bridge.Daemon(
            root / "bridge.sock",
            root / "thread-state.json",
            root / "offset.json",
            root / "verification.jsonl",
            limit_items=5,
            limit_turns=3,
        )
        instance.ws = StaleThreadListWS()
        assert await instance._pick_thread() == "healthy"


async def exercise_partial_fanout() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        instance = bridge.Daemon(
            root / "bridge.sock",
            root / "thread-state.json",
            root / "offset.json",
            root / "verification.jsonl",
        )
        delivery_id = instance.offset.enqueue_reply("x" * 4000, "thread", "turn", "request")
        instance._fanout_task = asyncio.create_task(instance.fanout_worker())
        try:
            with patch.object(bridge, "deliver_telegram", side_effect=[True, False]) as telegram, patch.object(bridge, "deliver_miku", return_value=True):
                instance._fanout_event.set()
                for _ in range(100):
                    item = instance.offset.get_delivery(delivery_id) or {}
                    target = (item.get("targets") or {}).get("telegram") or {}
                    if target.get("chunkIndex") == 1:
                        break
                    await asyncio.sleep(0.01)
                item = instance.offset.get_delivery(delivery_id) or {}
                target = (item.get("targets") or {}).get("telegram") or {}
                assert telegram.call_count == 2
                assert target.get("chunkIndex") == 1
                assert target.get("delivered") is False
        finally:
            instance._fanout_task.cancel()
            try:
                await instance._fanout_task
            except asyncio.CancelledError:
                pass


async def exercise() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        instance = bridge.Daemon(
            root / "bridge.sock",
            root / "thread-state.json",
            root / "offset.json",
            root / "verification.jsonl",
        )
        instance.store.set_active("thread-1")
        instance.ws = FakeWS()
        instance._ws_connected = True
        instance._fanout_task = asyncio.create_task(instance.fanout_worker())
        instance._worker_task = asyncio.create_task(instance.worker())
        try:
            with patch.object(bridge, "deliver_telegram", return_value=True) as telegram, patch.object(bridge, "deliver_miku", return_value=True) as miku:
                first = await instance.handle_submit("hello", "miku", "stable-request")
                replay = await instance.handle_submit("hello", "miku", "stable-request")
                assert first["reply"] == replay["reply"] == "stable reply"
                assert first["deliveryId"] == replay["deliveryId"]
                assert instance.ws.turns == 1
                for _ in range(100):
                    if instance.offset.summary()["pendingReplies"] == 0:
                        break
                    await asyncio.sleep(0.01)
                assert instance.offset.summary()["pendingReplies"] == 0
                assert telegram.call_count == 1
                assert miku.call_count == 1

            server = await asyncio.start_unix_server(instance.handle_client, path=str(root / "control.sock"))
            try:
                reader, writer = await asyncio.open_unix_connection(str(root / "control.sock"))
                writer.write(b'{"id":1,"method":"ping"}\n{"id":2,"method":"status"}\n')
                await writer.drain()
                ping = json.loads((await reader.readline()).decode())
                status = json.loads((await reader.readline()).decode())
                assert ping["result"]["ok"] is True
                assert status["result"]["activeThreadId"] == "thread-1"
                writer.write(b"[]\n{\"id\":3,\"method\":\"submit\",\"params\":{\"text\":7}}\n")
                await writer.drain()
                invalid_request = json.loads((await reader.readline()).decode())
                invalid_params = json.loads((await reader.readline()).decode())
                assert invalid_request["error"]["code"] == -32600
                assert invalid_params["id"] == 3
                assert invalid_params["error"]["code"] == -32602
                writer.close()
                await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()
        finally:
            instance._fanout_task.cancel()
            try:
                await instance._fanout_task
            except asyncio.CancelledError:
                pass
            instance._worker_task.cancel()
            try:
                await instance._worker_task
            except asyncio.CancelledError:
                pass


def main() -> None:
    asyncio.run(exercise())
    asyncio.run(test_thread_picker_verifies_live_status_and_limits())
    asyncio.run(exercise_partial_fanout())
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        instance = bridge.Daemon(
            root / "bridge.sock",
            root / "thread-state.json",
            root / "offset.json",
            root / "verification.jsonl",
            limit_items=5,
        )
        instance.store.set_active("old")
        instance.ws = BrokenForkWS()
        try:
            asyncio.run(instance.ensure_active_thread())
        except bridge.AppServerError as exc:
            assert "systemError" in str(exc)
        else:
            raise AssertionError("systemError fork child was accepted")
        assert instance.store.active_thread_id == "old"
    print("PASS daemon")


if __name__ == "__main__":
    main()
