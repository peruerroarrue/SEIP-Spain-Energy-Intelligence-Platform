"""Manual smoke test: build the ML feature table including the renewable_share feature.

Not part of the pytest suite. Run: python scripts/smoke_features_renewable.py
"""

from __future__ import annotations

from seip.ingestion.spark_session import build_local_spark_session
from seip.ml.features import run

SILVER_HOURLY_PATH = "data/silver/esios_hourly"
REDATA_SILVER_PATH = "data/silver/redata"
OUTPUT_PATH = "data/gold/ml_features_with_renewable"


def main() -> None:
    spark = build_local_spark_session("seip-features-renewable-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    df = run(spark, SILVER_HOURLY_PATH, OUTPUT_PATH, redata_silver_path=REDATA_SILVER_PATH)

    print(f"total rows: {df.count()}")
    print(f"rows with renewable_share: {df.filter('renewable_share is not null').count()}")
    df.select("hour_utc", "pvpc_eur_mwh", "renewable_share").filter("renewable_share is not null").show(5, truncate=False)


if __name__ == "__main__":
    main()
