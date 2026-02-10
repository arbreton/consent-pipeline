"""Unit tests for the consent-event validation rules (no Spark needed)."""

from src.quality import STALE_AFTER_S, clean_batch, validate_event

NOW = 1_700_000_000.0


def good(**kw):
    e = {
        "user_id": "u-123",
        "event_id": "e-1",
        "consent": "granted",
        "platform": "ios",
        "event_ts": NOW - 60,
    }
    e.update(kw)
    return e


def test_clean_event_passes():
    assert validate_event(good(), NOW) == []


def test_missing_user_id():
    assert "missing_user_id" in validate_event(good(user_id=""), NOW)
    assert "missing_user_id" in validate_event(good(user_id=None), NOW)


def test_missing_event_id_collapses_nothing():
    assert "missing_event_id" in validate_event(good(event_id=""), NOW)
    assert "missing_event_id" in validate_event(good(event_id=None), NOW)


def test_bad_consent_value():
    assert "bad_consent" in validate_event(good(consent="maybe"), NOW)
    assert "bad_consent" in validate_event(good(consent=None), NOW)


def test_bad_platform():
    assert "bad_platform" in validate_event(good(platform="tv"), NOW)
    assert "bad_platform" in validate_event(good(platform=None), NOW)


def test_null_ts_is_bad_not_silent():
    assert validate_event(good(event_ts=None), NOW) == ["bad_ts"]


def test_bool_ts_is_bad():
    assert "bad_ts" in validate_event(good(event_ts=True), NOW)


def test_nan_ts_is_bad():
    assert "bad_ts" in validate_event(good(event_ts=float("nan")), NOW)


def test_future_boundary():
    assert validate_event(good(event_ts=NOW + 300), NOW) == []
    assert "future_ts" in validate_event(good(event_ts=NOW + 301), NOW)


def test_stale_boundary():
    assert validate_event(good(event_ts=NOW - STALE_AFTER_S), NOW) == []
    assert "stale_event" in validate_event(
        good(event_ts=NOW - STALE_AFTER_S - 1), NOW
    )


def test_duplicates_go_to_quarantine_by_event_id():
    a, b = good(), good()
    b["user_id"] = "u-other"  # same event_id, different user: still a dupe
    clean, quar = clean_batch([a, b], NOW)
    assert len(clean) == 1
    assert quar[0]["violations"] == ["duplicate"]


def test_clean_batch_splits():
    clean, quar = clean_batch([good(), good(consent="maybe")], NOW)
    assert len(clean) == 1 and len(quar) == 1
