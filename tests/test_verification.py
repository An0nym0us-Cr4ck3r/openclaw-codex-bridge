#!/usr/bin/env python3
import json
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "daemon"))
from verification import VerificationLog


def test_append_tail_and_summary() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "verification.jsonl"
        log = VerificationLog(path)
        log.append("request.accepted", requestKey="a")
        log.append("fanout.delivered", deliveryId="d", target="telegram")
        log.append("fanout.failed", deliveryId="d", target="miku", error="temporary")
        assert len(log.tail(2)) == 2
        summary = log.summary()
        assert summary["events"] == 3
        assert summary["counts"]["request.accepted"] == 1
        assert summary["fanout"]["telegram"]["fanout.delivered"] == 1
        assert summary["fanout"]["miku"]["fanout.failed"] == 1
        for line in path.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            assert "reply" not in record
            assert "text" not in record


if __name__ == "__main__":
    test_append_tail_and_summary()
    print("PASS verification")
