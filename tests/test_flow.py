"""Pipeline-shape test without Spark: the pure-python mirror must conserve rows.

Runs everywhere (CI and locally) — it validates the rule logic the Spark job
mirrors, on real generator output.
"""

import json
from pathlib import Path

from generator.produce_events import main as generate
from src.quality import clean_batch


def test_flow_conserves_every_row(tmp_path):
    generate(
        ["--count", "200", "--batches", "4", "--out", str(tmp_path / "in"),
         "--error-rate", "0.05", "--seed", "7"]
    )
    events = [
        json.loads(line)
        for f in sorted(Path(tmp_path / "in").glob("*.json"))
        for line in f.read_text().splitlines()
    ]
    assert len(events) == 200
    clean, quar = clean_batch(events)
    # Nothing silently dropped: every row lands in silver or quarantine.
    assert len(clean) + len(quar) == len(events)
    assert len(quar) > 0  # the 5% error rate must produce rejects
    assert all(q["violations"] for q in quar)  # every reject has a reason
