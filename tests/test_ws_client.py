#!/usr/bin/env python3
import asyncio
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "daemon"))
from ws_client import AppServerError, WSClient


async def test_turn_events_are_scoped_to_the_current_turn() -> None:
    client = WSClient("unused")

    async def fake_request(method: str, params: dict):
        if method == "thread/read":
            return {"thread": {"turns": []}}
        if method == "turn/start":
            client._event_q.put_nowait(
                {"method": "item/agentMessage/delta", "params": {"turnId": "old", "delta": "stale"}}
            )
            client._event_q.put_nowait(
                {"method": "item/agentMessage/delta", "params": {"turnId": "new", "delta": "fresh"}}
            )
            client._event_q.put_nowait(
                {"method": "turn/completed", "params": {"turn": {"id": "new"}}}
            )
            return {"turn": {"id": "new"}}
        raise AssertionError(f"unexpected request: {method} {params}")

    async def fake_send(_obj: dict) -> None:
        return

    client.request = fake_request  # type: ignore[method-assign]
    client._send_obj = fake_send  # type: ignore[method-assign]
    client._event_q.put_nowait(
        {"method": "turn/completed", "params": {"turn": {"id": "previous"}}}
    )
    reply, turn_id = await client.run_turn("thread", "hello")
    assert (reply, turn_id) == ("fresh", "new")


async def test_close_fails_pending_requests_and_discards_events() -> None:
    client = WSClient("unused")
    pending = asyncio.get_running_loop().create_future()
    client._pending[1] = pending
    client._event_q.put_nowait({"method": "old.event"})
    await client.close()
    assert pending.done()
    assert isinstance(pending.exception(), AppServerError)
    assert client._event_q.empty()


async def test_dead_receive_task_is_not_reused() -> None:
    client = WSClient("unused")
    client.writer = object()  # type: ignore[assignment]
    client._bg_task = asyncio.create_task(asyncio.sleep(0))
    await client._bg_task
    try:
        await client.request("status", {})
    except AppServerError as exc:
        assert str(exc) == "not connected"
    else:
        raise AssertionError("completed receive task was reused")


def main() -> None:
    asyncio.run(test_turn_events_are_scoped_to_the_current_turn())
    asyncio.run(test_close_fails_pending_requests_and_discards_events())
    asyncio.run(test_dead_receive_task_is_not_reused())
    print("PASS ws_client")


if __name__ == "__main__":
    main()
