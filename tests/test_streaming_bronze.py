import json
from datetime import datetime, timezone

from seip.ingestion.streaming_bronze import to_bronze_row, topics


def test_topics_returns_all_four_kafka_topics():
    assert set(topics()) == {"ree.pvpc", "ree.spot", "ree.eolica", "ree.solar"}


def test_to_bronze_row_wraps_raw_kafka_value_untouched():
    kafka_timestamp_epoch = datetime(2026, 7, 31, 9, 5, 0, tzinfo=timezone.utc).timestamp()
    raw_value = json.dumps({"indicator_id": 1001, "value": 120.0}).encode("utf-8")

    row = to_bronze_row("ree.pvpc", raw_value, kafka_timestamp_epoch)

    assert row == {
        "ingestion_date": "2026-07-31",
        "source": "ree.pvpc",
        "fetched_at": "2026-07-31T09:05:00+00:00",
        "raw_json": '{"indicator_id": 1001, "value": 120.0}',
    }
    assert json.loads(row["raw_json"]) == {"indicator_id": 1001, "value": 120.0}


def test_to_bronze_row_epoch_is_timezone_agnostic():
    """A Unix epoch always maps to the same UTC instant, regardless of any local
    timezone the epoch value itself might have been derived from — this is
    exactly why to_bronze_row takes an epoch rather than a collected Spark
    datetime (see its docstring: .collect() silently uses the JVM's local
    timezone, not spark.sql.session.timeZone).
    """
    epoch = 1785574200.0  # 2026-07-30T15:50:00Z, arbitrary fixed instant

    row = to_bronze_row("ree.pvpc", b"{}", epoch)

    assert row["fetched_at"] == datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
