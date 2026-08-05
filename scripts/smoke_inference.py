"""Manual smoke test: run inference and show the next-24h PVPC forecast.

Not part of the pytest suite (needs the `ml` extra + all 24 horizon models
registered with the "reference" alias — run scripts/smoke_train.py first).

Run: python scripts/smoke_inference.py
"""

from __future__ import annotations

from seip.ingestion.spark_session import build_local_spark_session
from seip.ml.inference import run

FEATURES_PATH = "data/gold/ml_features"
OUTPUT_PATH = "data/gold/pvpc_forecast"


def main() -> None:
    spark = build_local_spark_session("seip-inference-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    print("-- running inference for the next 24 hours --")
    forecast_df = run(spark, FEATURES_PATH, OUTPUT_PATH)

    print(f"\ntotal rows: {forecast_df.count()}")
    forecast_df.orderBy("horizon_hours").show(24, truncate=False)


if __name__ == "__main__":
    main()
