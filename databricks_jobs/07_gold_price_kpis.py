"""Job task: price-by-hour-of-day and PVPC-vs-SPOT spread Gold KPIs.

Task 5 of `seip-batch-pipeline`, depends on 06_silver_esios_hourly_join.
"""

from pyspark.sql import SparkSession

from seip.transform.silver_to_gold import run_price_kpis

spark = SparkSession.builder.getOrCreate()

price_by_hour_df, pvpc_vs_spot_df = run_price_kpis(
    spark,
    silver_hourly_path="abfss://silver@seipdatalake.dfs.core.windows.net/esios_hourly",
    output_path_prefix="abfss://gold@seipdatalake.dfs.core.windows.net",
)
print(f"gold/price_by_hour_of_day rows: {price_by_hour_df.count()}")
print(f"gold/pvpc_vs_spot rows: {pvpc_vs_spot_df.count()}")
