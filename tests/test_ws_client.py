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


async def test_cancelled_request_is_removed_from_pending() -> None:
    client = WSClient("unused")
    client.writer = object()  # type: ignore[assignment]
    client._bg_task = asyncio.create_task(asyncio.sleep(60))

    async def fake_send(_obj: dict) -> None:
        return

    client._send_obj = fake_send  # type: ignore[method-assign]
    try:
        try:
            await asyncio.wait_for(client.request("status", {}), timeout=0.01)
        except asyncio.TimeoutError:
            pass
        else:
            raise AssertionError("request unexpectedly completed")
        assert client._pending == {}
    finally:
        client._bg_task.cancel()
        try:
            await client._bg_task
        except asyncio.CancelledError:
            pass


async def test_bounded_control_request_surfaces_timeout() -> None:
    client = WSClient("unused")

    async def never_returns(_method: str, _params: dict):
        await asyncio.Event().wait()

    client.request = never_returns  # type: ignore[method-assign]
    try:
        await client._request_bounded("thread/read", {}, timeout=0.01)
    except AppServerError as exc:
        assert "thread/read timed out" in str(exc)
    else:
        raise AssertionError("bounded request unexpectedly completed")


async def test_terminal_error_notification_is_not_ignored() -> None:
    client = WSClient("unused")

    async def fake_request(method: str, params: dict):
        if method == "thread/read":
            return {"thread": {"turns": []}}
        if method == "turn/start":
            client._event_q.put_nowait(
                {
                    "method": "error",
                    "params": {
                        "threadId": "thread",
                        "turnId": "new",
                        "willRetry": False,
                        "error": {"message": "provider failed"},
                    },
                }
            )
            return {"turn": {"id": "new"}}
        raise AssertionError(f"unexpected request: {method} {params}")

    client.request = fake_request  # type: ignore[method-assign]
    try:
        await client.run_turn("thread", "hello", idle_timeout=0.1)
    except AppServerError as exc:
        assert "provider failed" in str(exc)
    else:
        raise AssertionError("terminal error notification was ignored")


async def test_idle_timeout_interrupts_the_active_turn() -> None:
    client = WSClient("unused")
    interrupted: list[dict] = []

    async def fake_request(method: str, params: dict):
        if method == "thread/read":
            return {"thread": {"turns": []}}
        if method == "turn/start":
            return {"turn": {"id": "new"}}
        if method == "turn/interrupt":
            interrupted.append(params)
            return {}
        raise AssertionError(f"unexpected request: {method} {params}")

    client.request = fake_request  # type: ignore[method-assign]
    try:
        await client.run_turn("thread", "hello", idle_timeout=0.01)
    except AppServerError as exc:
        assert "idle timeout" in str(exc)
    else:
        raise AssertionError("idle turn did not time out")
    assert interrupted == [{"threadId": "thread", "turnId": "new"}]


async def test_total_timeout_is_not_extended_by_progress() -> None:
    client = WSClient("unused")
    interrupted: list[dict] = []

    async def fake_request(method: str, params: dict):
        if method == "thread/read":
            return {"thread": {"turns": []}}
        if method == "turn/start":
            return {"turn": {"id": "new"}}
        if method == "turn/interrupt":
            interrupted.append(params)
            return {}
        raise AssertionError(f"unexpected request: {method} {params}")

    client.request = fake_request  # type: ignore[method-assign]
    stop = asyncio.Event()

    async def keep_progressing() -> None:
        while not stop.is_set():
            client._event_q.put_nowait(
                {"method": "item/agentMessage/delta", "params": {"turnId": "new", "delta": "."}}
            )
            await asyncio.sleep(0.003)

    producer = asyncio.create_task(keep_progressing())
    try:
        try:
            await client.run_turn("thread", "hello", idle_timeout=0.02, total_timeout=0.04)
        except AppServerError as exc:
            assert "total timeout" in str(exc)
        else:
            raise AssertionError("progress stream bypassed total timeout")
    finally:
        stop.set()
        producer.cancel()
        try:
            await producer
        except asyncio.CancelledError:
            pass
    assert interrupted == [{"threadId": "thread", "turnId": "new"}]


def main() -> None:
    asyncio.run(test_turn_events_are_scoped_to_the_current_turn())
    asyncio.run(test_close_fails_pending_requests_and_discards_events())
    asyncio.run(test_dead_receive_task_is_not_reused())
    asyncio.run(test_cancelled_request_is_removed_from_pending())
    asyncio.run(test_bounded_control_request_surfaces_timeout())
    asyncio.run(test_terminal_error_notification_is_not_ignored())
    asyncio.run(test_idle_timeout_interrupts_the_active_turn())
    asyncio.run(test_total_timeout_is_not_extended_by_progress())
    print("PASS ws_client")


if __name__ == "__main__":
    main()
