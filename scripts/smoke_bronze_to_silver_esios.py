"""Manual smoke test: run the ESIOS Bronze -> Silver streaming job once (availableNow).

Not part of the pytest suite (needs `spark` extra + a populated
data/bronze/esios table — run scripts/smoke_streaming_bronze.py first if it's
empty).

Run: python scripts/smoke_bronze_to_silver_esios.py
"""

from __future__ import annotations

from seip.ingestion.spark_session import build_local_spark_session
from seip.transform.bronze_to_silver import run_esios

BRONZE_PATH = "data/bronze/esios"
SILVER_PATH = "data/silver/esios"
CHECKPOINT_PATH = "data/checkpoints/esios_silver"


def main() -> None:
    spark = build_local_spark_session("seip-bronze-to-silver-esios-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    print("-- running Bronze -> Silver (availableNow) --")
    run_esios(spark, BRONZE_PATH, SILVER_PATH, CHECKPOINT_PATH)

    print("\n-- reading back from Silver --")
    df = spark.read.format("delta").load(SILVER_PATH)
    print(f"total rows: {df.count()}")
    df.groupBy("indicator_id").count().orderBy("indicator_id").show()
    print("rows with validation flags:")
    df.filter("size(validation_flags) > 0").show(truncate=False)
    print("sample row:")
    df.orderBy("datetime_utc").show(3, truncate=60, vertical=True)


if __name__ == "__main__":
    main()
