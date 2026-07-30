import json
from datetime import date, datetime, timezone

import pytest

from seip.ingestion.batch_job import FetchTask, build_tasks, fetch_all, to_bronze_rows
from seip.ingestion.redata_client import RedataClientError

RUN_DATE = date(2026, 7, 29)


def test_build_tasks_marks_intercambios_as_non_critical():
    tasks = build_tasks()
    by_source = {t.source: t for t in tasks}

    assert by_source["generacion_estructura"].critical is True
    assert by_source["balance_electrico"].critical is True
    assert by_source["intercambios_francia"].critical is False
    assert by_source["intercambios_portugal"].critical is False
    assert by_source["intercambios_marruecos"].critical is False


def test_fetch_all_tags_records_with_source():
    tasks = (
        FetchTask("source_a", lambda s, e: [{"value": 1.0}]),
        FetchTask("source_b", lambda s, e: [{"value": 2.0}, {"value": 3.0}]),
    )
    records = fetch_all(RUN_DATE, tasks)

    assert records == [
        {"source": "source_a", "value": 1.0},
        {"source": "source_b", "value": 2.0},
        {"source": "source_b", "value": 3.0},
    ]


def test_fetch_all_raises_on_critical_failure():
    def failing_fetch(s, e):
        raise RedataClientError("boom")

    tasks = (FetchTask("critical_source", failing_fetch, critical=True),)

    with pytest.raises(RedataClientError):
        fetch_all(RUN_DATE, tasks)


def test_fetch_all_skips_noncritical_failure():
    def failing_fetch(s, e):
        raise RedataClientError("boom")

    tasks = (
        FetchTask("intercambios_francia", failing_fetch, critical=False),
        FetchTask("generacion_estructura", lambda s, e: [{"value": 1.0}], critical=True),
    )
    records = fetch_all(RUN_DATE, tasks)

    assert records == [{"source": "generacion_estructura", "value": 1.0}]


def test_to_bronze_rows_wraps_record_with_metadata_and_raw_json():
    fetched_at = datetime(2026, 7, 30, 8, 0, tzinfo=timezone.utc)
    records = [{"source": "generacion_estructura", "title": "Eólica", "value": 4300.0}]

    rows = to_bronze_rows(records, RUN_DATE, fetched_at=fetched_at)

    assert rows == [
        {
            "ingestion_date": "2026-07-29",
            "source": "generacion_estructura",
            "fetched_at": "2026-07-30T08:00:00+00:00",
            "raw_json": json.dumps(records[0]),
        }
    ]
    assert json.loads(rows[0]["raw_json"]) == records[0]
