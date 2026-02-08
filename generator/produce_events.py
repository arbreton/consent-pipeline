#!/usr/bin/env python3
"""Synthetic consent events -> NDJSON files (one file per batch, streaming-friendly).

Usage:
    python generator/produce_events.py --count 200 --batches 4 --out data/input --error-rate 0.05
"""

import argparse
import json
import random
import time
import uuid
from pathlib import Path

PLATFORMS = ["ios", "android", "web"]
CONSENTS = ["granted", "denied"]


def make_event(rng: random.Random, bad: bool) -> dict:
    now = time.time()
    e = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"u-{rng.randint(1, 5000)}",
        "consent": rng.choice(CONSENTS),
        "platform": rng.choice(PLATFORMS),
        "event_ts": now - rng.randint(0, 600),
    }
    if bad:
        e[rng.choice(["user_id", "consent", "platform", "event_ts"])] = rng.choice(
            ["", "maybe", "tv", now + 7200]
        )
    return e


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--batches", type=int, default=4)
    ap.add_argument("--out", default="data/input")
    ap.add_argument("--error-rate", type=float, default=0.05)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(a.seed)
    per, bad_total = a.count // a.batches, 0
    for b in range(a.batches):
        rows = []
        for _ in range(per):
            bad = rng.random() < a.error_rate
            bad_total += bad
            rows.append(make_event(rng, bad))
        (out / f"events-{b:03d}.json").write_text(
            "\n".join(json.dumps(r) for r in rows) + "\n"
        )
    print(f"wrote {a.count} events in {a.batches} files to {out}/ (~{bad_total} bad)")


if __name__ == "__main__":
    main()
