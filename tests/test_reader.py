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


def test_bootstrap_context_is_bounded() -> None:
    records = [
        {"id": f"record-{index}", "_role": "user", "_text": f"record-{index} " + "x" * 2000, "timestamp": str(index)}
        for index in range(40)
    ]
    context = reader.build_bootstrap_context(records)
    assert len(context) <= reader.MAX_CONTEXT_CHARS
    assert "record-39" in context
    assert "record-0" not in context


def test_processed_cursor_compacts_only_acknowledged_prefix() -> None:
    records = [{"id": f"record-{index}", "_role": "user", "_text": str(index)} for index in range(3)]
    processed = {reader.record_id(record) for record in records}
    cursor, cursor_id = reader.advance_processed_cursor(records, processed, 0, None)
    assert cursor == 3
    assert cursor_id == reader.record_id(records[-1])
    assert processed == set()
    records.append({"id": "record-3", "_role": "user", "_text": "3"})
    processed.add(reader.record_id(records[-1]))
    cursor, cursor_id = reader.advance_processed_cursor(records, processed, cursor, cursor_id)
    assert cursor == 4
    assert cursor_id == reader.record_id(records[-1])


def test_codex_to_miku_is_non_actionable() -> None:
    record = {"_role": "user", "_text": "[Codex] already delivered"}
    assert reader.label_record(record) == "Codex→Miku"


def test_state_round_trip_is_atomic() -> None:
    with tempfile.TemporaryDirectory() as directory:
        previous = reader.STATE_PATH
        reader.STATE_PATH = Path(directory) / "state.json"
        try:
            reader.save_state(True, {"b", "a"}, "session-1")
            state = reader.load_state()
            assert state["bootstrap_done"] is True
            assert state["processed_ids"] == ["a", "b"]
            assert state["processed_cursor"] == 0
            assert not list(Path(directory).glob("*.tmp"))
        finally:
            reader.STATE_PATH = previous


if __name__ == "__main__":
    test_stable_request_ids()
    test_bootstrap_context_is_bounded()
    test_processed_cursor_compacts_only_acknowledged_prefix()
    test_codex_to_miku_is_non_actionable()
    test_state_round_trip_is_atomic()
    print("PASS reader")
