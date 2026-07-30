"""Bronze -> Silver transformations.

Three independent pipelines live here — kept in the same file per the repo's
planned structure, but named distinctly so they don't get confused:

  - ESIOS streaming (PVPC, SPOT, wind, solar): `run_esios`. Bronze is read as
    a genuine Spark Structured Streaming source with a 2h watermark, because
    that data can arrive with duplicates/late records by design (see
    kafka_producer's overlapping fetch windows).
  - ESIOS hourly join: `run_esios_hourly_join`. Reads run_esios's already
    deduplicated output (not raw Bronze) and averages each series down to
    hourly grain, joining PVPC/SPOT/eolica/solar into one wide table — a
    plain batch step, no streaming/watermarking concern of its own.
  - REData batch (generación, balance, renovable/no-renovable): `run_redata`.
    This source is a scheduled batch job, not streaming, so there is no
    late-arrival concern — a plain batch read + dedup + overwrite is enough
    and much simpler.

Each pipeline's parsing/validation logic is defined twice on purpose:
  - `parse_esios_bronze_record` / `validate_esios_record` and
    `parse_redata_bronze_record` are plain Python — the readable,
    fast-to-test "spec" of what Silver must do to one record.
  - `parse_esios_bronze_stream` / `add_esios_validation_flags` and
    `parse_redata_bronze_batch` are the Spark-native column-expression
    equivalents actually used by `run_esios`/`run_redata`. For the ESIOS path
    this has to be native Spark expressions (not a Python row loop) so that
    `dropDuplicates` + `withWatermark` run as genuine stateful streaming
    operators instead of being reset every micro-batch.

Keep each pair in sync when a rule changes; `scripts/smoke_bronze_to_silver_esios.py`
and `scripts/smoke_bronze_to_silver_redata.py` exercise the Spark-native paths
against real data as the check that they still agree.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

# Spec 4.3: "alerta si PVPC < 0 o > 700 €/MWh, si potencia solar nocturna > 10 MW, etc."
# Only these two explicit rules are implemented — the spec's "etc." leaves room
# for more, deliberately left as a TODO rather than guessed at. No equivalent
# range rules are specified for REData, so none are invented for it either.
PRICE_RANGE = (0.0, 700.0)
PRICE_INDICATOR_IDS = (1001, 600)  # PVPC, SPOT
SOLAR_INDICATOR_ID = 1295
NIGHT_SOLAR_MAX_MW = 10.0


# --- ESIOS streaming -------------------------------------------------------


def parse_esios_bronze_record(raw_json: str) -> dict:
    """Parse one ESIOS Bronze `raw_json` string into a typed record.

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


def validate_esios_record(record: dict) -> list[str]:
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


def parse_esios_bronze_stream(bronze_stream: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of parse_esios_bronze_record, applied to a Bronze DataFrame/stream."""
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


def _esios_validation_flags_column() -> "Column":
    """Spark-native mirror of validate_esios_record — keep the two in sync."""
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


def add_esios_validation_flags(df: "DataFrame") -> "DataFrame":
    return df.withColumn("validation_flags", _esios_validation_flags_column())


def run_esios(spark: "SparkSession", bronze_path: str, silver_path: str, checkpoint_path: str) -> "StreamingQuery":
    """Stream new ESIOS Bronze rows into Silver: parse, validate, dedup with a 2h watermark.

    Bronze is read as a stream (not a static batch) specifically so
    `dropDuplicates` can run as a genuine stateful streaming operator bounded
    by `withWatermark` — this is what makes duplicate/late-arriving records
    (expected, by design, from kafka_producer's overlapping fetch windows)
    resolve correctly across runs instead of only within one micro-batch.
    availableNow keeps this cost-consistent with the rest of the pipeline
    (scheduled, not an always-on cluster).
    """
    bronze_stream = spark.readStream.format("delta").load(bronze_path)

    silver_df = add_esios_validation_flags(parse_esios_bronze_stream(bronze_stream))

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


# --- ESIOS hourly join ------------------------------------------------------

# Friendly column name per indicator for the joined hourly table.
INDICATOR_COLUMN_NAMES = {
    1001: "pvpc_eur_mwh",
    600: "spot_eur_mwh",
    551: "eolica_mw",
    1295: "solar_mw",
}


def aggregate_records_to_hour(records: list[dict]) -> dict[tuple[int, datetime], float]:
    """Reference (non-Spark) implementation: average value per (indicator_id, hour).

    PVPC is already hourly (averaging a single reading is a no-op); SPOT
    (15min) and eolica/solar (5min) get genuinely averaged down to the hour.
    Documents the exact semantics that aggregate_esios_silver_to_hour must
    replicate in Spark.
    """
    sums: dict[tuple[int, datetime], float] = {}
    counts: dict[tuple[int, datetime], int] = {}
    for record in records:
        hour = record["datetime_utc"].replace(minute=0, second=0, microsecond=0)
        key = (record["indicator_id"], hour)
        sums[key] = sums.get(key, 0.0) + record["value"]
        counts[key] = counts.get(key, 0) + 1
    return {key: sums[key] / counts[key] for key in sums}


def pivot_hourly_records(hourly_averages: dict[tuple[int, datetime], float]) -> dict[datetime, dict[str, float | None]]:
    """Reference pivot: one row per hour, one column per indicator's friendly name.

    An hour missing a given indicator's data gets `None` for that column
    rather than being dropped — Silver keeps partial data rather than hiding
    gaps, same philosophy as the validation flags above.
    """
    hours = sorted({hour for _, hour in hourly_averages})
    return {
        hour: {
            col_name: hourly_averages.get((indicator_id, hour))
            for indicator_id, col_name in INDICATOR_COLUMN_NAMES.items()
        }
        for hour in hours
    }


def aggregate_esios_silver_to_hour(silver_df: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of aggregate_records_to_hour."""
    from pyspark.sql import functions as F

    return (
        silver_df.withColumn("hour_utc", F.date_trunc("hour", F.col("datetime_utc")))
        .groupBy("indicator_id", "hour_utc")
        .agg(F.avg("value").alias("value"))
    )


def pivot_hourly_indicators(hourly_df: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of pivot_hourly_records: one wide row per hour."""
    from pyspark.sql import functions as F

    pivoted = hourly_df.groupBy("hour_utc").pivot("indicator_id", list(INDICATOR_COLUMN_NAMES.keys())).agg(
        F.first("value")
    )
    for indicator_id, col_name in INDICATOR_COLUMN_NAMES.items():
        pivoted = pivoted.withColumnRenamed(str(indicator_id), col_name)
    return pivoted.orderBy("hour_utc")


def run_esios_hourly_join(spark: "SparkSession", silver_path: str, output_path: str) -> "DataFrame":
    """Join PVPC/SPOT/eolica/solar into one hourly-grain table.

    Reads the already-deduplicated ESIOS Silver table (not raw Bronze) and
    recomputes the join as a plain batch overwrite — no Structured
    Streaming/watermarking needed here, that concern was already resolved by
    run_esios upstream.
    """
    silver_df = spark.read.format("delta").load(silver_path)
    joined_df = pivot_hourly_indicators(aggregate_esios_silver_to_hour(silver_df))
    joined_df.write.format("delta").mode("overwrite").save(output_path)
    return joined_df


# --- REData batch -----------------------------------------------------------


def parse_redata_bronze_record(raw_json: str) -> dict:
    """Parse one REData Bronze `raw_json` string into a typed record.

    Unlike ESIOS, REData records have no ready-made `datetime_utc` field —
    only an offset-aware `datetime` string (e.g. "...+02:00") that must be
    normalized to UTC here. `percentage` is optional (only some REData series
    carry it), so it's kept nullable rather than defaulted.
    """
    record = json.loads(raw_json)
    percentage = record.get("percentage")
    return {
        "source": record["source"],
        "title": record.get("title"),
        "datetime_utc": datetime.fromisoformat(record["datetime"]).astimezone(timezone.utc),
        "value": float(record["value"]),
        "percentage": float(percentage) if percentage is not None else None,
    }


def parse_redata_bronze_batch(bronze_df: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of parse_redata_bronze_record."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("source", StringType()),
            StructField("title", StringType()),
            StructField("value", DoubleType()),
            StructField("percentage", DoubleType()),
            StructField("datetime", StringType()),
        ]
    )
    return bronze_df.select(F.from_json("raw_json", schema).alias("data")).select(
        F.col("data.source").alias("source"),
        F.col("data.title").alias("title"),
        F.to_timestamp("data.datetime").alias("datetime_utc"),
        F.col("data.value").alias("value"),
        F.col("data.percentage").alias("percentage"),
    )


def run_redata(spark: "SparkSession", bronze_path: str, silver_path: str) -> "DataFrame":
    """Batch (not streaming) Bronze -> Silver for REData: parse, normalize UTC, dedup.

    REData ingestion is a scheduled daily batch (see batch_job.py), not
    streaming, so there is no late-arrival/watermarking concern here — a
    plain batch read + dedup + overwrite is enough. Re-running batch_job for
    an already-ingested day is the only realistic source of duplicates, and
    the (source, title, datetime_utc) dedup below covers that.
    """
    bronze_df = spark.read.format("delta").load(bronze_path)
    silver_df = parse_redata_bronze_batch(bronze_df).dropDuplicates(["source", "title", "datetime_utc"])
    silver_df.write.format("delta").mode("overwrite").save(silver_path)
    return silver_df


if __name__ == "__main__":
    import sys

    from seip.ingestion.spark_session import build_local_spark_session

    target = sys.argv[1] if len(sys.argv) > 1 else "esios"
    spark_session = build_local_spark_session("seip-bronze-to-silver")

    if target == "esios":
        run_esios(
            spark_session,
            bronze_path="data/bronze/esios",
            silver_path="data/silver/esios",
            checkpoint_path="data/checkpoints/esios_silver",
        )
    elif target == "esios_hourly_join":
        run_esios_hourly_join(spark_session, silver_path="data/silver/esios", output_path="data/silver/esios_hourly")
    elif target == "redata":
        run_redata(spark_session, bronze_path="data/bronze/redata", silver_path="data/silver/redata")
    else:
        raise SystemExit(f"unknown target {target!r} (expected 'esios', 'esios_hourly_join' or 'redata')")
