import json, tempfile
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

if __name__ == "__main__":
    test_rotation()
