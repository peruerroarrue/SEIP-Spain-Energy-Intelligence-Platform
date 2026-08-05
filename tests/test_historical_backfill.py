import json
from datetime import date, datetime, timezone

from seip.ingestion.esios_client import EsiosClientError
from seip.ingestion.historical_backfill import fetch_month, month_windows, to_bronze_row


def test_month_windows_splits_by_calendar_month():
    windows = month_windows(date(2026, 1, 15), date(2026, 4, 1))
    assert windows == [
        (date(2026, 1, 15), date(2026, 2, 1)),
        (date(2026, 2, 1), date(2026, 3, 1)),
        (date(2026, 3, 1), date(2026, 4, 1)),
    ]


def test_month_windows_handles_year_boundary():
    windows = month_windows(date(2025, 12, 10), date(2026, 2, 1))
    assert windows == [
        (date(2025, 12, 10), date(2026, 1, 1)),
        (date(2026, 1, 1), date(2026, 2, 1)),
    ]


def test_month_windows_single_partial_month():
    windows = month_windows(date(2026, 3, 5), date(2026, 3, 20))
    assert windows == [(date(2026, 3, 5), date(2026, 3, 20))]


def test_month_windows_empty_range():
    assert month_windows(date(2026, 3, 1), date(2026, 3, 1)) == []


def test_to_bronze_row_matches_streaming_bronze_shape():
    fetched_at = datetime(2026, 7, 30, 10, 0, 0, tzinfo=timezone.utc)
    value = {"value": 120.5, "datetime_utc": "2026-07-29T22:00:00Z", "geo_id": 8741, "geo_name": "Península"}

    row = to_bronze_row("ree.pvpc", 1001, value, fetched_at)

    assert row == {
        "ingestion_date": "2026-07-30",
        "source": "ree.pvpc",
        "fetched_at": "2026-07-30T10:00:00+00:00",
        "raw_json": json.dumps({"indicator_id": 1001, **value}),
    }
    assert json.loads(row["raw_json"])["indicator_id"] == 1001


def test_fetch_month_wraps_all_returned_values(monkeypatch):
    def fake_fetch(indicator_id, start_date, end_date, geo_id, api_key):
        return [
            {"value": 100.0, "datetime_utc": "2026-01-01T00:00:00Z", "geo_id": geo_id},
            {"value": 101.0, "datetime_utc": "2026-01-01T01:00:00Z", "geo_id": geo_id},
        ]

    monkeypatch.setattr("seip.ingestion.historical_backfill.fetch_indicator_values", fake_fetch)

    rows = fetch_month(1001, "ree.pvpc", 8741, date(2026, 1, 1), date(2026, 2, 1), api_key="key")

    assert len(rows) == 2
    assert all(json.loads(r["raw_json"])["indicator_id"] == 1001 for r in rows)


def test_fetch_month_propagates_client_error(monkeypatch):
    def failing_fetch(*args, **kwargs):
        raise EsiosClientError("boom")

    monkeypatch.setattr("seip.ingestion.historical_backfill.fetch_indicator_values", failing_fetch)

    try:
        fetch_month(1001, "ree.pvpc", 8741, date(2026, 1, 1), date(2026, 2, 1), api_key="key")
        assert False, "expected EsiosClientError"
    except EsiosClientError:
        pass
