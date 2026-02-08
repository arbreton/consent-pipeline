"""Pure-python consent-event validation.

Imported by unit tests here; the same rules are mirrored in Spark SQL
inside databricks/consent_pipeline.py (Spark can't import this file
on a vanilla cluster, so the notebook re-implements them natively).
"""

VALID_CONSENTS = {"granted", "denied"}
VALID_PLATFORMS = {"ios", "android", "web"}


def validate_event(e: dict, now_ts: float | None = None) -> list[str]:
    """Return violation strings; empty list means the event is clean."""
    import time

    violations: list[str] = []
    if not isinstance(e.get("user_id"), str) or not e["user_id"]:
        violations.append("missing_user_id")
    if e.get("consent") not in VALID_CONSENTS:
        violations.append("bad_consent")
    if e.get("platform") not in VALID_PLATFORMS:
        violations.append("bad_platform")
    ts = e.get("event_ts")
    now = now_ts if now_ts is not None else time.time()
    if not isinstance(ts, (int, float)) or isinstance(ts, bool):
        violations.append("bad_ts")
    else:
        if ts > now + 300:
            violations.append("future_ts")
        if ts < now - 365 * 86400:
            violations.append("ancient_ts")
    return violations


def dedupe_key(e: dict) -> str:
    return f"{e.get('user_id')}|{e.get('event_ts')}|{e.get('consent')}"


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
