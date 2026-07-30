"""Silver -> Gold: analytical KPIs.

Scoped to the two KPIs that only need the ESIOS hourly Silver table
(data/silver/esios_hourly): average price by hour-of-day, and the PVPC vs
SPOT spread. The third KPI from the spec, % renewable penetration, needs
REData's daily total generation reconciled against ESIOS's hourly grain — a
cross-source design decision left for a follow-up rather than bundled here.

Both KPIs are plain batch reads over the already-materialized hourly Silver
table — no streaming/watermarking concern, that was already resolved
upstream by bronze_to_silver.run_esios.

As in bronze_to_silver.py, the aggregation logic is defined twice: plain
Python (`average_by_hour_of_day` / `compute_spread`) as the fast-to-test
spec, and native Spark expressions (`aggregate_price_by_hour_of_day` /
`add_pvpc_spot_spread`) as what `run_price_kpis` actually runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def average_by_hour_of_day(rows: list[dict]) -> dict[int, dict[str, float | None]]:
    """Reference (non-Spark): average PVPC/SPOT per hour-of-day (0-23) across all days.

    Missing values are skipped, not treated as 0 — a day missing PVPC for a
    given hour shouldn't drag that hour's average down.
    """
    pvpc_sums: dict[int, float] = {}
    pvpc_counts: dict[int, int] = {}
    spot_sums: dict[int, float] = {}
    spot_counts: dict[int, int] = {}

    for row in rows:
        hour_of_day = row["hour_utc"].hour
        pvpc = row.get("pvpc_eur_mwh")
        if pvpc is not None:
            pvpc_sums[hour_of_day] = pvpc_sums.get(hour_of_day, 0.0) + pvpc
            pvpc_counts[hour_of_day] = pvpc_counts.get(hour_of_day, 0) + 1
        spot = row.get("spot_eur_mwh")
        if spot is not None:
            spot_sums[hour_of_day] = spot_sums.get(hour_of_day, 0.0) + spot
            spot_counts[hour_of_day] = spot_counts.get(hour_of_day, 0) + 1

    hours = sorted(set(pvpc_counts) | set(spot_counts))
    return {
        hour_of_day: {
            "avg_pvpc_eur_mwh": (pvpc_sums[hour_of_day] / pvpc_counts[hour_of_day])
            if hour_of_day in pvpc_counts
            else None,
            "avg_spot_eur_mwh": (spot_sums[hour_of_day] / spot_counts[hour_of_day])
            if hour_of_day in spot_counts
            else None,
        }
        for hour_of_day in hours
    }


def compute_spread(pvpc_eur_mwh: float | None, spot_eur_mwh: float | None) -> float | None:
    """PVPC minus SPOT for one hour — None if either side is missing."""
    if pvpc_eur_mwh is None or spot_eur_mwh is None:
        return None
    return pvpc_eur_mwh - spot_eur_mwh


def aggregate_price_by_hour_of_day(hourly_df: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of average_by_hour_of_day."""
    from pyspark.sql import functions as F

    return (
        hourly_df.withColumn("hour_of_day", F.hour("hour_utc"))
        .groupBy("hour_of_day")
        .agg(
            F.avg("pvpc_eur_mwh").alias("avg_pvpc_eur_mwh"),
            F.avg("spot_eur_mwh").alias("avg_spot_eur_mwh"),
        )
        .orderBy("hour_of_day")
    )


def add_pvpc_spot_spread(hourly_df: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of compute_spread, applied per hour.

    Spark's `-` already returns null when either side is null, so no extra
    null-handling is needed here (unlike the plain-Python reference version).
    """
    from pyspark.sql import functions as F

    return hourly_df.select(
        "hour_utc",
        "pvpc_eur_mwh",
        "spot_eur_mwh",
        (F.col("pvpc_eur_mwh") - F.col("spot_eur_mwh")).alias("spread_eur_mwh"),
    )


def run_price_kpis(spark: "SparkSession", silver_hourly_path: str, output_path_prefix: str) -> tuple["DataFrame", "DataFrame"]:
    """Compute both ESIOS-only Gold KPIs and write them to their own Delta tables."""
    hourly_df = spark.read.format("delta").load(silver_hourly_path)

    price_by_hour_df = aggregate_price_by_hour_of_day(hourly_df)
    price_by_hour_df.write.format("delta").mode("overwrite").save(f"{output_path_prefix}/price_by_hour_of_day")

    pvpc_vs_spot_df = add_pvpc_spot_spread(hourly_df)
    pvpc_vs_spot_df.write.format("delta").mode("overwrite").save(f"{output_path_prefix}/pvpc_vs_spot")

    return price_by_hour_df, pvpc_vs_spot_df


if __name__ == "__main__":
    from seip.ingestion.spark_session import build_local_spark_session

    spark_session = build_local_spark_session("seip-silver-to-gold")
    run_price_kpis(spark_session, silver_hourly_path="data/silver/esios_hourly", output_path_prefix="data/gold")
