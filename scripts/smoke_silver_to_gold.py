"""Manual smoke test: compute the two ESIOS-only Gold KPIs.

Not part of the pytest suite (needs `spark` extra + a populated
data/silver/esios_hourly table — run scripts/smoke_bronze_to_silver_hourly_join.py
first if it's empty).

Run: python scripts/smoke_silver_to_gold.py
"""

from __future__ import annotations

from seip.ingestion.spark_session import build_local_spark_session
from seip.transform.silver_to_gold import run_price_kpis

SILVER_HOURLY_PATH = "data/silver/esios_hourly"
GOLD_PREFIX = "data/gold"


def main() -> None:
    spark = build_local_spark_session("seip-silver-to-gold-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    print("-- computing Gold KPIs --")
    price_by_hour_df, pvpc_vs_spot_df = run_price_kpis(spark, SILVER_HOURLY_PATH, GOLD_PREFIX)

    print("\n-- price_by_hour_of_day --")
    price_by_hour_df.show(24, truncate=False)

    print("\n-- pvpc_vs_spot --")
    pvpc_vs_spot_df.orderBy("hour_utc").show(20, truncate=False)


if __name__ == "__main__":
    main()
