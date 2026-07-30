"""Manual smoke test: build the ML feature table and cross-check Spark vs plain Python.

Not part of the pytest suite (needs `spark` extra + a populated
data/silver/esios_hourly table — run scripts/smoke_bronze_to_silver_hourly_join.py
first if it's empty).

Run: python scripts/smoke_features.py
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

from seip.ingestion.spark_session import build_local_spark_session
from seip.ml.features import compute_calendar_features, run

SILVER_HOURLY_PATH = "data/silver/esios_hourly"
OUTPUT_PATH = "data/gold/ml_features"


def main() -> None:
    spark = build_local_spark_session("seip-features-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    print("-- building feature table --")
    df = run(spark, SILVER_HOURLY_PATH, OUTPUT_PATH)

    print(f"\ntotal rows: {df.count()}")
    df.select(
        "hour_utc", "pvpc_eur_mwh", "pvpc_lag_1h", "pvpc_lag_24h", "pvpc_lag_168h",
        "hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_of_year",
    ).show(20, truncate=False)

    print("-- cross-checking Spark vs plain-Python calendar features --")
    # Collect hour_utc as an epoch (double), not as a native TimestampType --
    # confirmed experimentally that PySpark's .collect() converts TimestampType
    # columns using the JVM's local default timezone, ignoring
    # spark.sql.session.timeZone entirely (this is exactly the bug fixed in
    # streaming_bronze.py's to_bronze_row/_process_batch; see DECISIONS.md).
    # An epoch is timezone-agnostic, so fromtimestamp(epoch, tz=utc) is safe.
    rows = df.select(
        F.col("hour_utc").cast("double").alias("hour_utc_epoch"),
        "hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_of_year",
    ).collect()
    mismatches = 0
    for row in rows:
        hour_utc = datetime.fromtimestamp(row["hour_utc_epoch"], tz=timezone.utc)
        python_features = compute_calendar_features(hour_utc)
        for key in ("hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_of_year"):
            if abs(row[key] - python_features[key]) > 1e-6:
                mismatches += 1
                print(f"MISMATCH at {hour_utc} for {key}: spark={row[key]} python={python_features[key]}")
    print(f"{mismatches} mismatches found" if mismatches else "all rows match exactly")


if __name__ == "__main__":
    main()
