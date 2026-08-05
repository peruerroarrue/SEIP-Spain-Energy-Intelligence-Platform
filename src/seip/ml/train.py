"""Train independent PVPC forecasting models, one per horizon (h=1..24 hours ahead).

Multiple independent models per horizon, not recursive forecasting — the
spec's explicit choice to avoid the error accumulation recursive forecasting
causes.

For each horizon h, the training set joins each row's already-built features
(seip.ml.features) to the PVPC value h hours later (the target), and to
freshly computed calendar features *of the target hour* — not reused from
the origin row — since what matters for the daily/weekly price cycle is
which hour/day is being forecast, which is known in advance regardless of
horizon.

Baseline: naive persistence (predicted price = price the same hour, 1 day
earlier — i.e. the `pvpc_lag_24h` feature already in the feature table).
The spec's Phase 4 acceptance criterion is beating this baseline on
RMSE/MAE over a >=3 month held-out test set.

Metric/split logic (compute_rmse/compute_mae/train_test_split_by_date) is
plain Python, fast to unit test. `build_horizon_training_set` is the
Spark-native piece; `train_one_horizon`/`run` additionally need pandas,
lightgbm and mlflow (the `ml` extra) and are only exercised by
scripts/smoke_train.py, not the fast pytest suite.
"""

from __future__ import annotations

import math
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

HORIZONS = tuple(range(1, 25))
LAG_FEATURE_COLUMNS = ("pvpc_lag_1h", "pvpc_lag_24h", "pvpc_lag_168h")
TARGET_CALENDAR_COLUMNS = ("target_hour_sin", "target_hour_cos", "target_dow_sin", "target_dow_cos", "target_month")
FEATURE_COLUMNS = LAG_FEATURE_COLUMNS + TARGET_CALENDAR_COLUMNS
TARGET_COLUMN = "target_pvpc_eur_mwh"
BASELINE_COLUMN = "pvpc_lag_24h"  # naive persistence: same hour, 1 day earlier


def registered_model_name(horizon_hours: int) -> str:
    """Model Registry name for one horizon's model.

    Each horizon gets its own registered model, not one shared name — h+1 and
    h+24 are genuinely different problems (different training set, different
    error profile), so "the reference version" has to mean something
    per-horizon for inference.py to load the right model for each hour it
    predicts.
    """
    return f"seip-pvpc-forecast-h{horizon_hours}"


def compute_errors(y_true: list[float], y_pred: list[float]) -> list[float]:
    return [true - pred for true, pred in zip(y_true, y_pred)]


def compute_rmse(errors: list[float]) -> float:
    return math.sqrt(sum(e * e for e in errors) / len(errors))


def compute_mae(errors: list[float]) -> float:
    return sum(abs(e) for e in errors) / len(errors)


def train_test_split_by_date(rows: list[dict], test_start: date, date_key: str = "hour_utc") -> tuple[list[dict], list[dict]]:
    """Chronological split: everything from test_start onward is test, the rest is train.

    Never a random split for time series data — that would leak nearby future
    information into training.
    """
    train = [r for r in rows if r[date_key].date() < test_start]
    test = [r for r in rows if r[date_key].date() >= test_start]
    return train, test


def build_horizon_training_set(features_df: "DataFrame", horizon_hours: int) -> "DataFrame":
    """Join each row to its PVPC value `horizon_hours` ahead, with calendar features of that target hour.

    Rows missing any required lag/target value are dropped — a model can't
    train on an incomplete row, and LightGBM's native NaN handling isn't
    worth relying on here for a first cut.
    """
    from pyspark.sql import functions as F

    two_pi = 2 * math.pi
    target = features_df.select(
        (F.col("hour_utc") - F.expr(f"INTERVAL {horizon_hours} HOURS")).alias("hour_utc"),
        F.col("pvpc_eur_mwh").alias(TARGET_COLUMN),
    )
    joined = features_df.join(target, on="hour_utc", how="inner")

    target_hour_col = F.col("hour_utc") + F.expr(f"INTERVAL {horizon_hours} HOURS")
    hour_angle = two_pi * F.hour(target_hour_col) / F.lit(24)
    python_weekday = (F.dayofweek(target_hour_col) + 5) % 7
    dow_angle = two_pi * python_weekday / F.lit(7)

    return joined.select(
        "hour_utc",
        *LAG_FEATURE_COLUMNS,
        F.sin(hour_angle).alias("target_hour_sin"),
        F.cos(hour_angle).alias("target_hour_cos"),
        F.sin(dow_angle).alias("target_dow_sin"),
        F.cos(dow_angle).alias("target_dow_cos"),
        F.month(target_hour_col).alias("target_month"),
        TARGET_COLUMN,
    ).dropna()


def train_one_horizon(horizon_df: "DataFrame", horizon_hours: int, test_start_date: date, experiment_name: str) -> dict:
    """Train + evaluate one horizon's model, logging params/metrics/model to MLflow."""
    import mlflow
    import mlflow.lightgbm
    from lightgbm import LGBMRegressor

    pdf = horizon_df.toPandas()
    train_pdf = pdf[pdf["hour_utc"].dt.date < test_start_date]
    test_pdf = pdf[pdf["hour_utc"].dt.date >= test_start_date]

    if len(train_pdf) == 0 or len(test_pdf) == 0:
        raise ValueError(
            f"Not enough data to train horizon h+{horizon_hours}: {len(train_pdf)} train / {len(test_pdf)} test rows"
        )

    x_train, y_train = train_pdf[list(FEATURE_COLUMNS)], train_pdf[TARGET_COLUMN]
    x_test, y_test = test_pdf[list(FEATURE_COLUMNS)], test_pdf[TARGET_COLUMN]

    baseline_errors = compute_errors(y_test.tolist(), test_pdf[BASELINE_COLUMN].tolist())
    baseline_rmse, baseline_mae = compute_rmse(baseline_errors), compute_mae(baseline_errors)

    model = LGBMRegressor(n_estimators=200, learning_rate=0.05, random_state=42)

    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=f"h{horizon_hours}"):
        mlflow.log_param("horizon_hours", horizon_hours)
        mlflow.log_param("train_rows", len(train_pdf))
        mlflow.log_param("test_rows", len(test_pdf))
        mlflow.log_param("test_start_date", str(test_start_date))
        mlflow.log_param("features", list(FEATURE_COLUMNS))

        model.fit(x_train, y_train)
        model_errors = compute_errors(y_test.tolist(), list(model.predict(x_test)))
        model_rmse, model_mae = compute_rmse(model_errors), compute_mae(model_errors)

        mlflow.log_metric("baseline_rmse", baseline_rmse)
        mlflow.log_metric("baseline_mae", baseline_mae)
        mlflow.log_metric("rmse", model_rmse)
        mlflow.log_metric("mae", model_mae)
        mlflow.log_metric("beats_baseline_rmse", int(model_rmse < baseline_rmse))

        # Logged via the LightGBM flavor, not mlflow.sklearn: MLflow's sklearn
        # flavor serializes through skops, whose default trusted-types allowlist
        # rejects LightGBM's own Booster/LGBMRegressor classes even though
        # LGBMRegressor implements the sklearn estimator interface. The
        # dedicated lightgbm flavor uses LightGBM's native serialization and
        # doesn't hit that check.
        mlflow.lightgbm.log_model(model, name="model")
        run_id = mlflow.active_run().info.run_id

    return {
        "horizon_hours": horizon_hours,
        "train_rows": len(train_pdf),
        "test_rows": len(test_pdf),
        "baseline_rmse": baseline_rmse,
        "baseline_mae": baseline_mae,
        "rmse": model_rmse,
        "mae": model_mae,
        "beats_baseline": model_rmse < baseline_rmse,
        "run_id": run_id,
    }


def run(
    spark: "SparkSession",
    features_path: str,
    test_start_date: date,
    horizons: tuple[int, ...] = HORIZONS,
    experiment_name: str = "seip-pvpc-forecast",
) -> list[dict]:
    """Train + evaluate every horizon, registering each as its own Model Registry
    entry with a "reference" alias on its version.

    All 24 are registered (not just h+1): inference.py needs the reference
    model for *every* horizon to produce a full next-24h forecast, per the
    spec's actual deliverable ("genera las predicciones de las próximas 24h
    en una tabla Gold") — a single representative horizon isn't enough for
    that, even though it would have satisfied the letter of the "at least
    one reference version" acceptance criterion on its own.
    """
    import mlflow

    features_df = spark.read.format("delta").load(features_path)
    client = mlflow.tracking.MlflowClient()

    results = []
    for horizon_hours in horizons:
        horizon_df = build_horizon_training_set(features_df, horizon_hours)
        result = train_one_horizon(horizon_df, horizon_hours, test_start_date, experiment_name)

        model_name = registered_model_name(horizon_hours)
        model_uri = f"runs:/{result['run_id']}/model"
        registered = mlflow.register_model(model_uri, model_name)
        client.set_registered_model_alias(model_name, "reference", registered.version)
        result["registered_model_name"] = model_name
        result["registered_version"] = registered.version

        results.append(result)

    return results


if __name__ == "__main__":
    import sys
    from datetime import timedelta

    from seip.ingestion.spark_session import build_local_spark_session

    test_months = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    test_start = date.today() - timedelta(days=30 * test_months)

    spark_session = build_local_spark_session("seip-train")
    all_results = run(spark_session, features_path="data/gold/ml_features", test_start_date=test_start)

    for r in all_results:
        status = "BEATS baseline" if r["beats_baseline"] else "does NOT beat baseline"
        print(
            f"h+{r['horizon_hours']:>2}: rmse={r['rmse']:.2f} (baseline {r['baseline_rmse']:.2f}) "
            f"mae={r['mae']:.2f} (baseline {r['baseline_mae']:.2f}) — {status} "
            f"[{r['train_rows']} train / {r['test_rows']} test rows]"
        )
