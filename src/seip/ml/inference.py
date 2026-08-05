"""Inference job: predict the next 24 hours of PVPC price and write them to Gold.

Loads, for each horizon h=1..24, the MLflow Model Registry version aliased
"reference" (registered by seip.ml.train) and scores the single most recent
feature row for that horizon — a forecast made *now*, using whatever the
latest available data is, not a backfilled historical forecast.

Per the spec's realistic MLOps scope (section 6): a batch job that loads a
reference model version and writes predictions to a Gold table. No
real-time serving endpoint, no automatic retraining, no drift monitoring —
explicitly out of scope.

build_feature_row/build_forecast_row are plain Python and fast to unit
test. `run` is the only piece touching PySpark/MLflow.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from seip.ml.train import (
    FEATURE_COLUMNS,
    HORIZONS,
    LAG_FEATURE_COLUMNS,
    compute_target_calendar_features,
    registered_model_name,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def build_feature_row(latest_row: dict, horizon_hours: int) -> dict:
    """The feature vector one horizon's model needs, built from the latest known data.

    Uses seip.ml.train.compute_target_calendar_features — the same function
    that built each model's training targets — so a live prediction is
    scored on features computed exactly the same way, not a reimplementation
    that could silently drift out of sync.
    """
    feature_row = {col: latest_row[col] for col in LAG_FEATURE_COLUMNS}
    feature_row.update(compute_target_calendar_features(latest_row["hour_utc"], horizon_hours))
    return feature_row


def build_forecast_row(origin_hour: datetime, horizon_hours: int, predicted_value: float) -> dict:
    """One row of the output forecast table."""
    target_hour = origin_hour + timedelta(hours=horizon_hours)
    return {
        "origin_hour_utc": origin_hour.isoformat(),
        "target_hour_utc": target_hour.isoformat(),
        "horizon_hours": horizon_hours,
        "predicted_pvpc_eur_mwh": predicted_value,
    }


def load_reference_model(horizon_hours: int):
    """Load the model version aliased "reference" for one horizon."""
    import mlflow

    model_name = registered_model_name(horizon_hours)
    return mlflow.pyfunc.load_model(f"models:/{model_name}@reference")


def run(
    spark: "SparkSession",
    features_path: str,
    output_path: str,
    horizons: tuple[int, ...] = HORIZONS,
) -> "DataFrame":
    """Predict the next `horizons` hours of PVPC price from the latest known data.

    Every horizon is scored from the *same* origin row (the latest feature
    row available) — this is what makes the 24 outputs a single coherent
    "forecast made now" rather than 24 unrelated point predictions.
    """
    import pandas as pd

    features_df = spark.read.format("delta").load(features_path)
    latest_row = features_df.orderBy(features_df.hour_utc.desc()).limit(1).toPandas().iloc[0].to_dict()
    origin_hour = latest_row["hour_utc"]

    forecast_rows = []
    for horizon_hours in horizons:
        model = load_reference_model(horizon_hours)
        feature_row = build_feature_row(latest_row, horizon_hours)
        x = pd.DataFrame([feature_row])[list(FEATURE_COLUMNS)]
        predicted_value = float(model.predict(x)[0])
        forecast_rows.append(build_forecast_row(origin_hour, horizon_hours, predicted_value))

    forecast_df = spark.createDataFrame(forecast_rows)
    forecast_df.write.format("delta").mode("overwrite").save(output_path)
    return forecast_df


if __name__ == "__main__":
    from seip.ingestion.spark_session import build_local_spark_session

    spark_session = build_local_spark_session("seip-inference")
    run(spark_session, features_path="data/gold/ml_features", output_path="data/gold/pvpc_forecast")
