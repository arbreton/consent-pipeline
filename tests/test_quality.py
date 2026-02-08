"""Unit tests for the consent-event validation rules (no Spark needed)."""

import time

from src.quality import clean_batch, validate_event

NOW = 1_700_000_000.0


def good(**kw):
    e = {
        "user_id": "u-123",
        "consent": "granted",
        "platform": "ios",
        "event_ts": NOW - 60,
        "event_id": "e-1",
    }
    e.update(kw)
    return e


def test_clean_event_passes():
    assert validate_event(good(), NOW) == []


def test_missing_user_id():
    assert "missing_user_id" in validate_event(good(user_id=""), NOW)


def test_bad_consent_value():
    assert "bad_consent" in validate_event(good(consent="maybe"), NOW)


def test_bad_platform():
    assert "bad_platform" in validate_event(good(platform="tv"), NOW)


def test_future_timestamp_rejected():
    assert "future_ts" in validate_event(good(event_ts=NOW + 3600), NOW)


def test_ancient_timestamp_rejected():
    assert "ancient_ts" in validate_event(good(event_ts=NOW - 400 * 86400), NOW)


def test_duplicates_go_to_quarantine():
    clean, quar = clean_batch([good(), good()], NOW)
    assert len(clean) == 1
    assert quar[0]["violations"] == ["duplicate"]


def test_clean_batch_splits():
    clean, quar = clean_batch([good(), good(consent="maybe")], NOW)
    assert len(clean) == 1 and len(quar) == 1
