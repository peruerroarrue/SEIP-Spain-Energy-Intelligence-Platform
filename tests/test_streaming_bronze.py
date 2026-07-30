import json
from datetime import datetime, timezone

from seip.ingestion.streaming_bronze import to_bronze_row, topics


def test_topics_returns_all_four_kafka_topics():
    assert set(topics()) == {"ree.pvpc", "ree.spot", "ree.eolica", "ree.solar"}


def test_to_bronze_row_wraps_raw_kafka_value_untouched():
    kafka_timestamp = datetime(2026, 7, 31, 9, 5, 0, tzinfo=timezone.utc)
    raw_value = json.dumps({"indicator_id": 1001, "value": 120.0}).encode("utf-8")

    row = to_bronze_row("ree.pvpc", raw_value, kafka_timestamp)

    assert row == {
        "ingestion_date": "2026-07-31",
        "source": "ree.pvpc",
        "fetched_at": "2026-07-31T09:05:00+00:00",
        "raw_json": '{"indicator_id": 1001, "value": 120.0}',
    }
    assert json.loads(row["raw_json"]) == {"indicator_id": 1001, "value": 120.0}


def test_to_bronze_row_attaches_utc_to_naive_timestamp():
    """Spark's Kafka `timestamp` column comes back naive; it must be treated as UTC."""
    naive_timestamp = datetime(2026, 7, 31, 9, 5, 0)

    row = to_bronze_row("ree.pvpc", b"{}", naive_timestamp)

    assert row["fetched_at"] == "2026-07-31T09:05:00+00:00"
