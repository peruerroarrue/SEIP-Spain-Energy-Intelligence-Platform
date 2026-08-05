"""Manual smoke test: compute the % renewable penetration Gold KPI.

Not part of the pytest suite (needs `spark` extra + populated
data/silver/esios_hourly and data/silver/redata tables).

Run: python scripts/smoke_renewable_kpi.py
"""

from __future__ import annotations

from seip.ingestion.spark_session import build_local_spark_session
from seip.transform.silver_to_gold import run_renewable_penetration_kpi

SILVER_HOURLY_PATH = "data/silver/esios_hourly"
REDATA_SILVER_PATH = "data/silver/redata"
OUTPUT_PATH = "data/gold/renewable_penetration"


def main() -> None:
    spark = build_local_spark_session("seip-renewable-kpi-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    print("-- computing % renewable penetration KPI --")
    df = run_renewable_penetration_kpi(spark, SILVER_HOURLY_PATH, REDATA_SILVER_PATH, OUTPUT_PATH)

    print(f"\ntotal rows: {df.count()}")
    print(f"rows with a renewable_share assigned: {df.filter('renewable_share is not null').count()}")
    df.orderBy("hour_utc").show(10, truncate=False)


if __name__ == "__main__":
    main()
