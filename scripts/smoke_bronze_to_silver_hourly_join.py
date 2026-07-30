"""Manual smoke test: run the ESIOS hourly join (PVPC/SPOT/eolica/solar -> one wide table).

Not part of the pytest suite (needs `spark` extra + a populated
data/silver/esios table — run scripts/smoke_bronze_to_silver_esios.py first
if it's empty).

Run: python scripts/smoke_bronze_to_silver_hourly_join.py
"""

from __future__ import annotations

from seip.ingestion.spark_session import build_local_spark_session
from seip.transform.bronze_to_silver import run_esios_hourly_join

SILVER_PATH = "data/silver/esios"
OUTPUT_PATH = "data/silver/esios_hourly"


def main() -> None:
    spark = build_local_spark_session("seip-esios-hourly-join-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    print("-- running ESIOS hourly join --")
    run_esios_hourly_join(spark, SILVER_PATH, OUTPUT_PATH)

    print("\n-- reading back joined hourly table --")
    df = spark.read.format("delta").load(OUTPUT_PATH)
    print(f"total rows: {df.count()}")
    df.orderBy("hour_utc").show(20, truncate=False)


if __name__ == "__main__":
    main()
