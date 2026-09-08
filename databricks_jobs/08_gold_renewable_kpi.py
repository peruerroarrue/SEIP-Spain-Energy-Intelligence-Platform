"""Job task: % renewable penetration Gold KPI.

Task 6 of `seip-batch-pipeline`, depends on 04_silver_redata and
06_silver_esios_hourly_join (crosses REData's daily split with ESIOS's
hourly grain - see silver_to_gold.broadcast_daily_renewable_share).
"""

from pyspark.sql import SparkSession

from seip.transform.silver_to_gold import run_renewable_penetration_kpi

spark = SparkSession.builder.getOrCreate()

run_renewable_penetration_kpi(
    spark,
    silver_hourly_path="abfss://silver@seipdatalake.dfs.core.windows.net/esios_hourly",
    redata_silver_path="abfss://silver@seipdatalake.dfs.core.windows.net/redata",
    output_path="abfss://gold@seipdatalake.dfs.core.windows.net/renewable_penetration",
)
