from datetime import datetime, timedelta, timezone

import pytest

from seip.ingestion import kafka_producer as kp
from seip.ingestion.esios_client import EsiosClientError

NOW = datetime(2026, 7, 30, 12, 0, 0, tzinfo=timezone.utc)


def test_is_due_never_run():
    indicator = kp.INDICATORS[0]
    assert kp.is_due(indicator, last_run=None, now=NOW) is True


def test_is_due_before_interval_elapsed():
    indicator = kp.IndicatorConfig(1, "topic", 1, timedelta(minutes=15))
    last_run = NOW - timedelta(minutes=5)
    assert kp.is_due(indicator, last_run, NOW) is False


def test_is_due_after_interval_elapsed():
    indicator = kp.IndicatorConfig(1, "topic", 1, timedelta(minutes=15))
    last_run = NOW - timedelta(minutes=20)
    assert kp.is_due(indicator, last_run, NOW) is True


def test_build_message_uses_datetime_utc_as_key():
    key, payload = kp.build_message(1001, {"datetime_utc": "2026-07-30T10:00:00Z", "value": 100.0})

    assert key == b"2026-07-30T10:00:00Z"
    assert payload == b'{"indicator_id": 1001, "datetime_utc": "2026-07-30T10:00:00Z", "value": 100.0}'


def test_poll_once_fetches_and_sends_only_due_indicators(monkeypatch):
    calls = []

    def fake_fetch(indicator_id, start_date, end_date, geo_id, api_key):
        calls.append(indicator_id)
        return [{"datetime_utc": "2026-07-30T11:00:00Z", "value": 1.0, "geo_id": geo_id}]

    monkeypatch.setattr(kp, "fetch_indicator_values", fake_fetch)

    sent = []
    last_run = {kp.INDICATORS[0].indicator_id: NOW}  # PVPC just ran, not due again in 1h
    kp.poll_once(kp.INDICATORS, last_run, NOW, api_key="key", send=lambda t, k, v: sent.append((t, k, v)))

    fetched_ids = set(calls)
    assert kp.INDICATORS[0].indicator_id not in fetched_ids  # PVPC skipped
    assert fetched_ids == {i.indicator_id for i in kp.INDICATORS[1:]}
    assert len(sent) == len(kp.INDICATORS) - 1
    assert last_run[kp.INDICATORS[1].indicator_id] == NOW


def test_poll_once_survives_esios_failure_and_keeps_due(monkeypatch):
    def failing_fetch(*args, **kwargs):
        raise EsiosClientError("transient")

    monkeypatch.setattr(kp, "fetch_indicator_values", failing_fetch)

    last_run: dict[int, object] = {}
    sent = []
    kp.poll_once(kp.INDICATORS, last_run, NOW, api_key="key", send=lambda t, k, v: sent.append((t, k, v)))

    assert sent == []
    assert last_run == {}  # not updated on failure, so it stays "due" and retries next tick
