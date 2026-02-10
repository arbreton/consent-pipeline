"""Pure-python consent-event validation.

Imported by unit tests here; the same rules are mirrored in Spark SQL
inside databricks/consent_pipeline.py (Spark can't import this file
on a vanilla cluster, so the notebook re-implements them natively).

Rule set (violation codes are the contract — see README data-contract table):
  missing_user_id / missing_event_id (null or empty)
  bad_consent / bad_platform (not in the allowlist)
  bad_ts (not a number — incl. bool and NaN, which slip past comparisons)
  future_ts (> now + 300s clock-skew allowance)
  stale_event (< now - 7 days; arrivals older than the dedupe horizon)

Dedupe key is event_id in both engines.
"""

import math
import time

VALID_CONSENTS = {"granted", "denied"}
VALID_PLATFORMS = {"ios", "android", "web"}
FUTURE_SKEW_S = 300
STALE_AFTER_S = 7 * 86400


def validate_event(e: dict, now_ts: float | None = None) -> list[str]:
    """Return violation strings; empty list means the event is clean."""
    violations: list[str] = []
    if not isinstance(e.get("user_id"), str) or not e["user_id"]:
        violations.append("missing_user_id")
    if not isinstance(e.get("event_id"), str) or not e["event_id"]:
        violations.append("missing_event_id")
    if e.get("consent") not in VALID_CONSENTS:
        violations.append("bad_consent")
    if e.get("platform") not in VALID_PLATFORMS:
        violations.append("bad_platform")
    ts = e.get("event_ts")
    now = now_ts if now_ts is not None else time.time()
    if (
        not isinstance(ts, (int, float))
        or isinstance(ts, bool)
        or (isinstance(ts, float) and math.isnan(ts))
    ):
        violations.append("bad_ts")
    else:
        if ts > now + FUTURE_SKEW_S:
            violations.append("future_ts")
        if ts < now - STALE_AFTER_S:
            violations.append("stale_event")
    return violations


def dedupe_key(e: dict) -> str:
    return str(e.get("event_id"))


def clean_batch(events: list[dict], now_ts: float | None = None):
    """Split events into (clean, quarantine). Duplicates go to quarantine."""
    seen: set[str] = set()
    clean, quarantine = [], []
    for e in events:
        v = validate_event(e, now_ts)
        if v:
            quarantine.append({"event": e, "violations": v})
            continue
        k = dedupe_key(e)
        if k in seen:
            quarantine.append({"event": e, "violations": ["duplicate"]})
            continue
        seen.add(k)
        clean.append(e)
    return clean, quarantine
