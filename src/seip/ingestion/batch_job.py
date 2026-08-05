"""Daily batch ingestion of REData series into Bronze.

Planning/fetch/normalization logic has no Spark import, so it stays fast to
unit test. Only `run` touches PySpark/Delta Lake.

Bronze keeps each record as a raw JSON string (schema-on-read) plus ingestion
metadata — typing/casting into real columns is a Silver responsibility, not
done here.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable

from seip.ingestion.redata_client import (
    RedataClientError,
    fetch_balance_electrico,
    fetch_evolucion_renovable_no_renovable,
    fetch_generacion_estructura,
    fetch_intercambios,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

INTERCAMBIOS_PAISES = ("francia", "portugal", "marruecos")


@dataclass(frozen=True)
class FetchTask:
    source: str
    fetch: Callable[[str, str], list[dict]]
    critical: bool = True


def build_tasks(pais_intercambios: tuple[str, ...] = INTERCAMBIOS_PAISES) -> tuple[FetchTask, ...]:
    """REData endpoints ingested by this job, and whether a failure should stop the run.

    `intercambios` is validated as unstable (frequent 500/503) — best-effort,
    must not fail the whole daily batch.
    """
    tasks = [
        FetchTask("generacion_estructura", fetch_generacion_estructura, critical=True),
        FetchTask("evolucion_renovable_no_renovable", fetch_evolucion_renovable_no_renovable, critical=True),
        FetchTask("balance_electrico", fetch_balance_electrico, critical=True),
    ]
    for pais in pais_intercambios:
        tasks.append(
            FetchTask(f"intercambios_{pais}", lambda s, e, _p=pais: fetch_intercambios(_p, s, e), critical=False)
        )
    return tuple(tasks)


def fetch_all(run_date: date, tasks: tuple[FetchTask, ...]) -> list[dict]:
    """Fetch every task for `run_date`, tagging each record with its source.

    Non-critical task failures (intercambios) are logged and skipped. Critical
    task failures propagate — they mean a real problem with the day's data,
    not the API's known instability.
    """
    start = run_date.isoformat()
    end = (run_date + timedelta(days=1)).isoformat()

    records: list[dict] = []
    for task in tasks:
        try:
            values = task.fetch(start, end)
        except RedataClientError:
            if task.critical:
                raise
            logger.warning("Non-critical source %s failed for %s, skipping", task.source, run_date)
            continue
        records.extend({"source": task.source, **v} for v in values)
    return records


def to_bronze_rows(records: list[dict], run_date: date, fetched_at: datetime | None = None) -> list[dict]:
    """Wrap each record as a Bronze row: raw payload untouched, plus ingestion metadata."""
    fetched_at = fetched_at or datetime.now(timezone.utc)
    return [
        {
            "ingestion_date": run_date.isoformat(),
            "source": record["source"],
            "fetched_at": fetched_at.isoformat(),
            "raw_json": json.dumps(record),
        }
        for record in records
    ]


def run(spark: "SparkSession", run_date: date, bronze_path: str) -> "DataFrame":
    """Fetch one day of REData series and append it to the Bronze Delta table."""
    tasks = build_tasks()
    records = fetch_all(run_date, tasks)
    rows = to_bronze_rows(records, run_date)

    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").partitionBy("ingestion_date").save(bronze_path)
    return df


def fetch_range(start_date: date, end_date: date, tasks: tuple[FetchTask, ...]) -> list[dict]:
    """Fetch every task for the full [start_date, end_date) range in one call each.

    Unlike the daily fetch_all, this is for historical backfill: REData's
    day-granularity endpoints are validated (Regla 3 — a 5-year daily backfill
    completed without failures) to handle multi-month/year ranges in a single
    call, unlike ESIOS's finer-grained indicators — no month-by-month
    pagination loop is needed here, unlike historical_backfill.py.
    """
    start = start_date.isoformat()
    end = end_date.isoformat()

    records: list[dict] = []
    for task in tasks:
        try:
            values = task.fetch(start, end)
        except RedataClientError:
            if task.critical:
                raise
            logger.warning(
                "Non-critical source %s failed for range %s to %s, skipping", task.source, start_date, end_date
            )
            continue
        records.extend({"source": task.source, **v} for v in values)
    return records


def to_bronze_rows_backfill(records: list[dict], fetched_at: datetime | None = None) -> list[dict]:
    """Wrap backfilled records as Bronze rows.

    `ingestion_date` is the day the backfill ran (today), not each record's
    own data date — same convention as to_bronze_rows and
    historical_backfill.py's ESIOS backfill: Bronze is partitioned by
    ingestion date per the spec, not by the date the data describes.
    """
    fetched_at = fetched_at or datetime.now(timezone.utc)
    ingestion_date = fetched_at.date().isoformat()
    return [
        {
            "ingestion_date": ingestion_date,
            "source": record["source"],
            "fetched_at": fetched_at.isoformat(),
            "raw_json": json.dumps(record),
        }
        for record in records
    ]


def run_backfill(spark: "SparkSession", start_date: date, end_date: date, bronze_path: str) -> "DataFrame":
    """Backfill REData series for [start_date, end_date) into Bronze, one call per task.

    Kept in this module (not a separate file like historical_backfill.py) —
    it's the same tasks/logic as the daily `run`, just over a wider range in
    one shot, not a fundamentally different pagination-heavy job.
    """
    tasks = build_tasks()
    records = fetch_range(start_date, end_date, tasks)
    rows = to_bronze_rows_backfill(records)

    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").partitionBy("ingestion_date").save(bronze_path)
    return df


if __name__ == "__main__":
    import sys

    from seip.ingestion.spark_session import build_local_spark_session

    logging.basicConfig(level=logging.INFO)
    spark_session = build_local_spark_session("seip-batch-job")

    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        months_back = int(sys.argv[2]) if len(sys.argv) > 2 else 6
        end_date = date.today()
        start_date = end_date - timedelta(days=30 * months_back)
        run_backfill(spark_session, start_date, end_date, bronze_path="data/bronze/redata")
        print(f"Backfilled REData for {start_date} to {end_date}")
    else:
        target_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today() - timedelta(days=1)
        run(spark_session, target_date, bronze_path="data/bronze/redata")
