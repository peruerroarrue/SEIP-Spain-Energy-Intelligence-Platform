"""Historical backfill of the 4 ESIOS indicators into Bronze.

Fills data/bronze/esios — the same table streaming_bronze.py writes to —
with months of real history, so downstream Silver/hourly-join/features has
enough data to train seip.ml.train against (the spec's Phase 4 acceptance
criterion needs a >=3 month held-out test set).

Paginates by month with a courtesy pause between calls — Regla 3 (already
validated in the project spec): requesting multi-month hourly ranges from
ESIOS in a single call causes ReadTimeout. Reuses kafka_producer.INDICATORS
(indicator_id/topic/geo_id) as the single source of truth for which 4
indicators this project ingests, instead of redefining them here.

Planning/wrapping logic (month_windows, to_bronze_row) has no Spark import
and stays unit-testable; only `run` touches PySpark/Delta Lake.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from seip.ingestion.esios_client import EsiosClientError, fetch_indicator_values
from seip.ingestion.kafka_producer import INDICATORS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

COURTESY_PAUSE_SECONDS = 1.2  # Regla 3: ~1-1.5s pause between paginated calls


def month_windows(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """Split [start_date, end_date) into month-sized (start, end) windows."""
    windows: list[tuple[date, date]] = []
    current = start_date
    while current < end_date:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        chunk_end = min(next_month, end_date)
        windows.append((current, chunk_end))
        current = chunk_end
    return windows


def to_bronze_row(topic: str, indicator_id: int, value: dict, fetched_at: datetime) -> dict:
    """Wrap one ESIOS value into the same Bronze row shape streaming_bronze.py uses."""
    record = {"indicator_id": indicator_id, **value}
    return {
        "ingestion_date": fetched_at.date().isoformat(),
        "source": topic,
        "fetched_at": fetched_at.isoformat(),
        "raw_json": json.dumps(record),
    }


def fetch_month(indicator_id: int, topic: str, geo_id: int, start: date, end: date, api_key: str) -> list[dict]:
    """Fetch one indicator for one month window and wrap it as Bronze rows."""
    values = fetch_indicator_values(indicator_id, start.isoformat(), end.isoformat(), geo_id, api_key)
    fetched_at = datetime.now(timezone.utc)
    return [to_bronze_row(topic, indicator_id, v, fetched_at) for v in values]


def run(spark: "SparkSession", start_date: date, end_date: date, api_key: str, bronze_path: str) -> int:
    """Backfill all 4 ESIOS indicators for [start_date, end_date) into Bronze.

    A failed month for one indicator is logged and skipped rather than
    aborting the whole backfill — a multi-month run losing one month of one
    indicator is far better than losing everything to a single transient
    failure this far into a long-running job.
    """
    total_rows = 0
    for indicator in INDICATORS:
        for window_start, window_end in month_windows(start_date, end_date):
            try:
                rows = fetch_month(
                    indicator.indicator_id, indicator.topic, indicator.geo_id, window_start, window_end, api_key
                )
            except EsiosClientError:
                logger.exception(
                    "Backfill failed for indicator %s, %s to %s — skipping this window",
                    indicator.indicator_id,
                    window_start,
                    window_end,
                )
                time.sleep(COURTESY_PAUSE_SECONDS)
                continue

            if rows:
                spark.createDataFrame(rows).write.format("delta").mode("append").partitionBy(
                    "ingestion_date"
                ).save(bronze_path)
                total_rows += len(rows)

            logger.info(
                "Backfilled %d rows for indicator %s, %s to %s",
                len(rows),
                indicator.indicator_id,
                window_start,
                window_end,
            )
            time.sleep(COURTESY_PAUSE_SECONDS)

    return total_rows


if __name__ == "__main__":
    import os
    import sys
    from datetime import timedelta

    from seip.ingestion.spark_session import build_local_spark_session

    logging.basicConfig(level=logging.INFO)

    months_back = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    end = date.today()
    start = end - timedelta(days=30 * months_back)

    spark_session = build_local_spark_session("seip-historical-backfill")
    written = run(
        spark_session,
        start_date=start,
        end_date=end,
        api_key=os.environ["ESIOS_API_TOKEN"],
        bronze_path="data/bronze/esios",
    )
    print(f"Backfilled {written} rows for {start} to {end}")
