"""Bronze -> Silver transformation for the ESIOS streaming data (PVPC, SPOT, wind, solar).

The validation rules and record shape are defined twice on purpose:
  - `parse_bronze_record` / `validate_record` are plain Python — the readable,
    fast-to-test "spec" of what Silver must do to one record.
  - `parse_bronze_stream` / `add_validation_flags` are the Spark-native
    column-expression equivalents, used by `run`. They have to be Spark
    expressions (not a Python row loop) so that `dropDuplicates` +
    `withWatermark` run as genuine stateful streaming operators instead of
    being reset every micro-batch.

Keep both in sync when a rule changes; `scripts/smoke_bronze_to_silver.py`
exercises the Spark-native path against real data as the check that they
still agree.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

# Spec 4.3: "alerta si PVPC < 0 o > 700 €/MWh, si potencia solar nocturna > 10 MW, etc."
# Only these two explicit rules are implemented — the spec's "etc." leaves room
# for more, deliberately left as a TODO rather than guessed at.
PRICE_RANGE = (0.0, 700.0)
PRICE_INDICATOR_IDS = (1001, 600)  # PVPC, SPOT
SOLAR_INDICATOR_ID = 1295
NIGHT_SOLAR_MAX_MW = 10.0


def parse_bronze_record(raw_json: str) -> dict:
    """Parse one Bronze `raw_json` string into a typed record.

    Untyped Bronze -> typed Silver is exactly the boundary the spec assigns to
    this layer; Bronze itself never casts/parses.
    """
    record = json.loads(raw_json)
    return {
        "indicator_id": int(record["indicator_id"]),
        "datetime_utc": datetime.fromisoformat(record["datetime_utc"].replace("Z", "+00:00")),
        "value": float(record["value"]),
        "geo_id": int(record["geo_id"]),
        "geo_name": record.get("geo_name"),
    }


def _is_night_hour_utc(datetime_utc: datetime, local_utc_offset_hours: int = 2) -> bool:
    """Rough local-hour check for the sun-down window, used only for the solar sanity check.

    Uses a fixed CEST-ish offset rather than a full timezone/DST library — good
    enough for a sanity alert, not for anything that needs to be exact.
    """
    local_hour = (datetime_utc.hour + local_utc_offset_hours) % 24
    return local_hour < 6 or local_hour >= 22


def validate_record(record: dict) -> list[str]:
    """Return the range-validation rule names this record violates (empty if none).

    Violations are flagged, not used to drop the record — Silver keeps every
    row so Gold/analysis can decide what to do with flagged data.
    """
    violations = []
    indicator_id = record["indicator_id"]
    value = record["value"]

    if indicator_id in PRICE_INDICATOR_IDS:
        low, high = PRICE_RANGE
        if not (low <= value <= high):
            violations.append(f"value_out_of_range_{low}_{high}")

    if (
        indicator_id == SOLAR_INDICATOR_ID
        and _is_night_hour_utc(record["datetime_utc"])
        and value > NIGHT_SOLAR_MAX_MW
    ):
        violations.append("night_solar_generation_above_threshold")

    return violations


def parse_bronze_stream(bronze_stream: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of parse_bronze_record, applied to a Bronze DataFrame/stream."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("indicator_id", LongType()),
            StructField("value", DoubleType()),
            StructField("datetime_utc", StringType()),
            StructField("geo_id", LongType()),
            StructField("geo_name", StringType()),
        ]
    )
    return bronze_stream.select(
        F.to_timestamp("fetched_at").alias("fetched_at"),
        F.from_json("raw_json", schema).alias("data"),
    ).select(
        "fetched_at",
        F.col("data.indicator_id").alias("indicator_id"),
        F.to_timestamp("data.datetime_utc").alias("datetime_utc"),
        F.col("data.value").alias("value"),
        F.col("data.geo_id").alias("geo_id"),
        F.col("data.geo_name").alias("geo_name"),
    )


def _validation_flags_column() -> "Column":
    """Spark-native mirror of validate_record — keep the two in sync."""
    from pyspark.sql import functions as F

    low, high = PRICE_RANGE
    price_flag = F.when(
        F.col("indicator_id").isin(*PRICE_INDICATOR_IDS) & ~F.col("value").between(low, high),
        F.lit(f"value_out_of_range_{low}_{high}"),
    )

    local_hour = (F.hour("datetime_utc") + 2) % 24
    is_night = (local_hour < 6) | (local_hour >= 22)
    solar_flag = F.when(
        (F.col("indicator_id") == SOLAR_INDICATOR_ID) & is_night & (F.col("value") > NIGHT_SOLAR_MAX_MW),
        F.lit("night_solar_generation_above_threshold"),
    )

    flags_array = F.array(price_flag, solar_flag)
    return F.array_except(flags_array, F.array(F.lit(None).cast("string")))


def add_validation_flags(df: "DataFrame") -> "DataFrame":
    return df.withColumn("validation_flags", _validation_flags_column())


def run(spark: "SparkSession", bronze_path: str, silver_path: str, checkpoint_path: str) -> "StreamingQuery":
    """Stream new Bronze rows into Silver: parse, validate, dedup with a 2h watermark.

    Bronze is read as a stream (not a static batch) specifically so
    `dropDuplicates` can run as a genuine stateful streaming operator bounded
    by `withWatermark` — this is what makes duplicate/late-arriving records
    (expected, by design, from kafka_producer's overlapping fetch windows)
    resolve correctly across runs instead of only within one micro-batch.
    availableNow keeps this cost-consistent with the rest of the pipeline
    (scheduled, not an always-on cluster).
    """
    bronze_stream = spark.readStream.format("delta").load(bronze_path)

    silver_df = add_validation_flags(parse_bronze_stream(bronze_stream))

    deduped = silver_df.withWatermark("fetched_at", "2 hours").dropDuplicates(["indicator_id", "datetime_utc"])

    query = (
        deduped.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start(silver_path)
    )
    query.awaitTermination()
    return query


if __name__ == "__main__":
    from seip.ingestion.spark_session import build_local_spark_session

    spark_session = build_local_spark_session("seip-bronze-to-silver")
    run(
        spark_session,
        bronze_path="data/bronze/esios",
        silver_path="data/silver/esios",
        checkpoint_path="data/checkpoints/esios_silver",
    )
