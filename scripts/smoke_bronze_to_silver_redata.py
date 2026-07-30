"""Manual smoke test: run the REData Bronze -> Silver batch transform.

Not part of the pytest suite (needs `spark` extra + a populated
data/bronze/redata table — run scripts/smoke_batch_job.py first if it's
empty).

Run: python scripts/smoke_bronze_to_silver_redata.py
"""

from __future__ import annotations

from seip.ingestion.spark_session import build_local_spark_session
from seip.transform.bronze_to_silver import run_redata

BRONZE_PATH = "data/bronze/redata"
SILVER_PATH = "data/silver/redata"


def main() -> None:
    spark = build_local_spark_session("seip-bronze-to-silver-redata-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    print("-- running REData Bronze -> Silver --")
    run_redata(spark, BRONZE_PATH, SILVER_PATH)

    print("\n-- reading back from Silver --")
    df = spark.read.format("delta").load(SILVER_PATH)
    print(f"total rows: {df.count()}")
    df.groupBy("source").count().orderBy("source").show(truncate=False)
    print("sample rows:")
    df.orderBy("datetime_utc").show(5, truncate=40)


if __name__ == "__main__":
    main()
