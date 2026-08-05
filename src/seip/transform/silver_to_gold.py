"""Silver -> Gold: analytical KPIs.

Three KPIs from the spec:
  - Price by hour-of-day and PVPC vs SPOT spread (`run_price_kpis`) only need
    the ESIOS hourly Silver table (data/silver/esios_hourly).
  - % renewable penetration (`run_renewable_penetration_kpi`) reconciles
    REData's daily renewable/non-renewable split (data/silver/redata,
    source=evolucion_renovable_no_renovable) against ESIOS's hourly grain —
    see broadcast_daily_renewable_share for the cross-source join and its
    timezone subtlety.

All three are plain batch reads over already-materialized Silver tables —
no streaming/watermarking concern, that was already resolved upstream by
bronze_to_silver.run_esios/run_redata.

As elsewhere in this project, the aggregation logic is defined twice: plain
Python (`average_by_hour_of_day` / `compute_spread` /
`find_daily_renewable_share_for_hour`) as the fast-to-test spec, and native
Spark expressions as what the `run_*` functions actually run.
"""

from __future__ import annotations

from datetime import datetime, timedelta
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


# --- % penetración renovable (REData día -> ESIOS hora) ---------------------


def find_daily_renewable_share_for_hour(
    hour_utc: datetime, daily_shares: list[tuple[datetime, float]]
) -> float | None:
    """Reference (non-Spark): which daily REData window (if any) contains this hour.

    Mirrors broadcast_daily_renewable_share's join condition:
    day_start_utc <= hour_utc < day_start_utc + 24h. REData's daily
    datetime_utc already marks the correct start-of-day-in-Spain UTC instant
    (e.g. 22:00 UTC the previous day during CEST) — matching by UTC calendar
    date instead would misalign the share to the wrong ~22 of 24 hours.
    """
    for day_start_utc, share in daily_shares:
        if day_start_utc <= hour_utc < day_start_utc + timedelta(hours=24):
            return share
    return None


def broadcast_daily_renewable_share(redata_silver_df: "DataFrame", hourly_df: "DataFrame") -> "DataFrame":
    """Spark-native equivalent of find_daily_renewable_share_for_hour.

    REData's `evolucion_renovable_no_renovable` "Renovable" row already
    carries the day's renewable share as `percentage` (Renovable /
    (Renovable + No renovable), confirmed against real data). This broadcasts
    that single daily value across every ESIOS hour it covers, via a range
    join on the UTC instant rather than a calendar-date match — see
    find_daily_renewable_share_for_hour's docstring for why.

    Note the resulting `renewable_share` is REData's day-level renewable mix
    (hydro + other renewables included, not just solar+eolica), broadcast
    unchanged to every hour of that day — an approximation, not a true
    hourly solar+eolica share, given REData doesn't publish this at hourly
    grain.
    """
    from pyspark.sql import functions as F

    daily_renewable = redata_silver_df.filter(
        (F.col("source") == "evolucion_renovable_no_renovable") & (F.col("title") == "Renovable")
    ).select(
        F.col("datetime_utc").alias("day_start_utc"),
        F.col("percentage").alias("renewable_share"),
    )

    condition = (hourly_df["hour_utc"] >= daily_renewable["day_start_utc"]) & (
        hourly_df["hour_utc"] < daily_renewable["day_start_utc"] + F.expr("INTERVAL 24 HOURS")
    )
    return hourly_df.join(daily_renewable, on=condition, how="left").drop("day_start_utc")


def run_renewable_penetration_kpi(
    spark: "SparkSession", silver_hourly_path: str, redata_silver_path: str, output_path: str
) -> "DataFrame":
    """Compute the % renewable penetration KPI and write it to Gold."""
    hourly_df = spark.read.format("delta").load(silver_hourly_path)
    redata_silver_df = spark.read.format("delta").load(redata_silver_path)

    result_df = broadcast_daily_renewable_share(redata_silver_df, hourly_df).select(
        "hour_utc", "pvpc_eur_mwh", "spot_eur_mwh", "eolica_mw", "solar_mw", "renewable_share"
    )
    result_df.write.format("delta").mode("overwrite").save(output_path)
    return result_df


if __name__ == "__main__":
    from seip.ingestion.spark_session import build_local_spark_session

    spark_session = build_local_spark_session("seip-silver-to-gold")
    run_price_kpis(spark_session, silver_hourly_path="data/silver/esios_hourly", output_path_prefix="data/gold")
    run_renewable_penetration_kpi(
        spark_session,
        silver_hourly_path="data/silver/esios_hourly",
        redata_silver_path="data/silver/redata",
        output_path="data/gold/renewable_penetration",
    )
