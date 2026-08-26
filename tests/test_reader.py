#!/usr/bin/env python3
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "clients"))
import telegram_reader as reader


def test_stable_request_ids() -> None:
    first = reader.stable_request_id("telegram", "session-1", ["record-1"], "hello")
    same = reader.stable_request_id("telegram", "session-1", ["record-1"], "hello")
    changed_record = reader.stable_request_id("telegram", "session-1", ["record-2"], "hello")
    changed_order = reader.stable_request_id("telegram", "session-1", ["record-1", "record-2"], "hello")
    assert first == same
    assert first != changed_record
    assert first != changed_order


def test_state_round_trip_is_atomic() -> None:
    with tempfile.TemporaryDirectory() as directory:
        previous = reader.STATE_PATH
        reader.STATE_PATH = Path(directory) / "state.json"
        try:
            reader.save_state(True, {"b", "a"}, "session-1")
            state = reader.load_state()
            assert state["bootstrap_done"] is True
            assert state["processed_ids"] == ["a", "b"]
            assert not list(Path(directory).glob("*.tmp"))
        finally:
            reader.STATE_PATH = previous


if __name__ == "__main__":
    test_stable_request_ids()
    test_state_round_trip_is_atomic()
    print("PASS reader")
