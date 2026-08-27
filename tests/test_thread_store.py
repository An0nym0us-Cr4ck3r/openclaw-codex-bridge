import json, stat, tempfile
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "daemon"))
from thread_store import ThreadStore

def test_rotation():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "state.json"
        s = ThreadStore(p, limit_items=5, limit_turns=3)
        assert s.active_thread_id is None
        s.set_active("thr-1")
        assert s.active_thread_id == "thr-1"
        assert not s.needs_rotation(status_type="idle", item_count=2, turn_count=2)
        assert s.needs_rotation(status_type="systemError", item_count=1, turn_count=1)
        assert s.needs_rotation(status_type="idle", item_count=5, turn_count=1)
        assert s.needs_rotation(status_type="idle", item_count=1, turn_count=3)
        s2 = ThreadStore(p, limit_items=5, limit_turns=3)
        assert s2.active_thread_id == "thr-1"
        print("PASS thread_store")


def test_state_write_is_durable_and_private():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "state.json"
        s = ThreadStore(p)
        s.set_active("thr-1", forked_from="old")
        s.set_active("thr-2")
        assert ThreadStore(p).active_thread_id == "thr-2"
        assert "forkedFrom" not in ThreadStore(p).to_dict()
        assert stat.S_IMODE(p.stat().st_mode) == 0o600
        assert not list(Path(d).glob("*.tmp"))


def test_corrupt_state_fails_closed():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "state.json"
        p.write_text("{not-json", encoding="utf-8")
        try:
            ThreadStore(p)
        except RuntimeError as exc:
            assert "invalid thread state JSON" in str(exc)
        else:
            raise AssertionError("corrupt state was silently accepted")
        assert p.read_text(encoding="utf-8") == "{not-json"

if __name__ == "__main__":
    test_rotation()
    test_state_write_is_durable_and_private()
    test_corrupt_state_fails_closed()
