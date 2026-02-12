#!/usr/bin/env python3
"""Synthetic consent events -> NDJSON files or Azure Event Hubs.

File sink (local dev, streaming-friendly — one file per batch):
    python generator/produce_events.py --count 200 --batches 4 --out data/input

Event Hubs sink (partition key = user_id, so one user's events stay ordered):
    python generator/produce_events.py --sink eventhubs \\
        --conn-str 'Endpoint=sb://...' --eventhub consent-events --count 200

The azure-eventhub dependency is imported lazily so the file sink works
without Azure libraries installed.
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


def make_batch(count: int, error_rate: float, seed: int) -> list[dict]:
    rng = random.Random(seed)
    return [make_event(rng, rng.random() < error_rate) for _ in range(count)]


def write_files(events: list[dict], out: str, batches: int) -> None:
    path = Path(out)
    path.mkdir(parents=True, exist_ok=True)
    per = len(events) // batches
    for b in range(batches):
        rows = events[b * per : (b + 1) * per]
        (path / f"events-{b:03d}.json").write_text(
            "\n".join(json.dumps(r) for r in rows) + "\n"
        )


def send_eventhubs(events: list[dict], conn_str: str, eventhub: str) -> int:
    from azure.eventhub import EventData, EventHubProducerClient

    producer = EventHubProducerClient.from_connection_string(conn_str, eventhub)
    by_user: dict[str, list[dict]] = {}
    for e in events:
        by_user.setdefault(e["user_id"], []).append(e)
    sent = 0
    with producer:
        for user_id, rows in by_user.items():
            batch = producer.create_batch(partition_key=user_id)
            for r in rows:
                body = json.dumps(r)
                try:
                    batch.add(EventData(body))
                except ValueError:
                    producer.send_batch(batch)
                    sent += len(batch)
                    batch = producer.create_batch(partition_key=user_id)
                    batch.add(EventData(body))
            producer.send_batch(batch)
            sent += len(batch)
    return sent


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--batches", type=int, default=4)
    ap.add_argument("--out", default="data/input")
    ap.add_argument("--error-rate", type=float, default=0.05)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--sink", choices=["files", "eventhubs"], default="files")
    ap.add_argument("--conn-str", default="")
    ap.add_argument("--eventhub", default="consent-events")
    a = ap.parse_args(argv)

    events = make_batch(a.count, a.error_rate, a.seed)
    if a.sink == "eventhubs":
        if not a.conn_str:
            raise SystemExit("--conn-str is required with --sink eventhubs")
        n = send_eventhubs(events, a.conn_str, a.eventhub)
        print(f"sent {n} events to Event Hubs '{a.eventhub}' (key=user_id)")
    else:
        write_files(events, a.out, a.batches)
        print(f"wrote {a.count} events in {a.batches} files to {a.out}/")


if __name__ == "__main__":
    main()
