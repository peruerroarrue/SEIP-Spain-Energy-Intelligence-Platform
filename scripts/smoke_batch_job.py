"""Manual smoke test: run the REData batch job with local PySpark + Delta Lake.

Not part of the pytest suite (needs the `spark` extra installed). Writes to a
local Delta table under data/bronze/redata (gitignored), then reads it back
to confirm the write actually landed correctly.

Run: python scripts/smoke_batch_job.py [YYYY-MM-DD]
"""

from __future__ import annotations

import sys
from datetime import date, timedelta

from seip.ingestion.batch_job import run
from seip.ingestion.spark_session import build_local_spark_session

BRONZE_PATH = "data/bronze/redata"


def main() -> None:
    target_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today() - timedelta(days=1)

    spark = build_local_spark_session("seip-batch-job")
    spark.sparkContext.setLogLevel("ERROR")

    print(f"-- running batch job for {target_date} --")
    run(spark, target_date, BRONZE_PATH)

    print("\n-- reading back from Bronze --")
    df = spark.read.format("delta").load(BRONZE_PATH)
    print(f"total rows in table: {df.count()}")
    df.groupBy("ingestion_date", "source").count().orderBy("source").show(truncate=False)
    print("sample row:")
    df.show(1, truncate=80, vertical=True)


if __name__ == "__main__":
    main()
