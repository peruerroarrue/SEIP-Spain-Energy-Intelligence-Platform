"""Daily batch ingestion of REData series into Bronze.

Planning/fetch/normalization logic has no Spark import, so it stays fast to
unit test. Only `run` and `_build_local_spark_session` (used by the manual
smoke test) touch PySpark/Delta Lake.

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


def _build_local_spark_session() -> "SparkSession":
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("seip-batch-job")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run(spark: "SparkSession", run_date: date, bronze_path: str) -> "DataFrame":
    """Fetch one day of REData series and append it to the Bronze Delta table."""
    tasks = build_tasks()
    records = fetch_all(run_date, tasks)
    rows = to_bronze_rows(records, run_date)

    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").partitionBy("ingestion_date").save(bronze_path)
    return df


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    target_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today() - timedelta(days=1)
    spark_session = _build_local_spark_session()
    run(spark_session, target_date, bronze_path="data/bronze/redata")
