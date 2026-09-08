"""Job task: price-by-hour-of-day and PVPC-vs-SPOT spread Gold KPIs.

Task 5 of `seip-batch-pipeline`, depends on 06_silver_esios_hourly_join.
"""

from pyspark.sql import SparkSession

from seip.transform.silver_to_gold import run_price_kpis

spark = SparkSession.builder.getOrCreate()

run_price_kpis(
    spark,
    silver_hourly_path="abfss://silver@seipdatalake.dfs.core.windows.net/esios_hourly",
    output_path_prefix="abfss://gold@seipdatalake.dfs.core.windows.net",
)
