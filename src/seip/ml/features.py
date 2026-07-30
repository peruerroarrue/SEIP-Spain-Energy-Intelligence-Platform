"""Feature engineering for the PVPC price forecasting model.

Reads the ESIOS hourly Silver table (data/silver/esios_hourly) and builds
the model-ready feature table: PVPC price lags (1h/24h/168h), cyclical
hour-of-day and day-of-week, and month-of-year.

Deliberately NOT included yet (see DECISIONS.md/TODO.md):
  - % renewable penetration — needs REData's daily total generation
    reconciled against this hourly grain, a cross-source decision not made
    yet.
  - Demand forecast-vs-actual — the spec makes this conditional on ingesting
    a demand indicator ("si se ingiere ese indicador"), which hasn't
    happened.

Calendar-feature semantics are defined twice, same pattern as transform/:
plain Python (`compute_calendar_features`) documents the rule and is fast to
test; the Spark-native version (`add_calendar_features`) is what `run` uses,
and is built to produce numerically identical output for the same
timestamp (see its docstring re: day-of-week convention).

Lags are computed via a join on the shifted timestamp, not a positional
`lag()` window function — see add_price_lags for why.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

LAG_HOURS = (1, 24, 168)


def compute_calendar_features(hour_utc: datetime) -> dict:
    """Cyclical hour-of-day / day-of-week encoding, plus plain month-of-year.

    day-of-week follows Python's `weekday()` convention (Monday=0..Sunday=6);
    add_calendar_features's Spark version converts to match this exactly.
    """
    hour_angle = 2 * math.pi * hour_utc.hour / 24
    dow_angle = 2 * math.pi * hour_utc.weekday() / 7
    return {
        "hour_sin": math.sin(hour_angle),
        "hour_cos": math.cos(hour_angle),
        "dow_sin": math.sin(dow_angle),
        "dow_cos": math.cos(dow_angle),
        "month_of_year": hour_utc.month,
    }


def add_calendar_features(df: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of compute_calendar_features.

    Spark's `dayofweek()` is Sun=1..Sat=7; converted here to Python's
    `weekday()` convention (Mon=0..Sun=6) via `(dayofweek + 5) % 7` so the
    two implementations agree on the exact same numeric angle, not just "some
    cyclical encoding".
    """
    from pyspark.sql import functions as F

    two_pi = 2 * math.pi
    hour_angle = two_pi * F.hour("hour_utc") / F.lit(24)
    python_weekday = (F.dayofweek("hour_utc") + 5) % 7
    dow_angle = two_pi * python_weekday / F.lit(7)

    return (
        df.withColumn("hour_sin", F.sin(hour_angle))
        .withColumn("hour_cos", F.cos(hour_angle))
        .withColumn("dow_sin", F.sin(dow_angle))
        .withColumn("dow_cos", F.cos(dow_angle))
        .withColumn("month_of_year", F.month("hour_utc"))
    )


def add_price_lags(df: "DataFrame", lag_hours: tuple[int, ...] = LAG_HOURS) -> "DataFrame":
    """Join each row to its own PVPC value `lag` hours earlier, by timestamp.

    Deliberately not a positional `lag()` window function: if the hourly
    series has a gap, `lag(1)` over an ordered window would silently return
    "the previous existing row" instead of "exactly 1 hour ago" — wrong data
    fed straight into training. Joining on the shifted timestamp instead
    makes a missing hour correctly produce a null lag, not a misaligned one.
    """
    from pyspark.sql import functions as F

    base = df.select("hour_utc", "pvpc_eur_mwh")
    result = df
    for lag in lag_hours:
        shifted = base.select(
            (F.col("hour_utc") + F.expr(f"INTERVAL {lag} HOURS")).alias("hour_utc"),
            F.col("pvpc_eur_mwh").alias(f"pvpc_lag_{lag}h"),
        )
        result = result.join(shifted, on="hour_utc", how="left")
    return result


def build_features(silver_hourly_df: "DataFrame") -> "DataFrame":
    """Full feature table: PVPC lags + calendar features, one row per hour."""
    return add_calendar_features(add_price_lags(silver_hourly_df)).orderBy("hour_utc")


def run(spark: "SparkSession", silver_hourly_path: str, output_path: str) -> "DataFrame":
    """Build the ML feature table and write it to Delta (plain batch, no streaming)."""
    silver_hourly_df = spark.read.format("delta").load(silver_hourly_path)
    features_df = build_features(silver_hourly_df)
    features_df.write.format("delta").mode("overwrite").save(output_path)
    return features_df


if __name__ == "__main__":
    from seip.ingestion.spark_session import build_local_spark_session

    spark_session = build_local_spark_session("seip-ml-features")
    run(spark_session, silver_hourly_path="data/silver/esios_hourly", output_path="data/gold/ml_features")
