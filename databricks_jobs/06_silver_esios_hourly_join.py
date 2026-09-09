"""Job task: join PVPC/SPOT/eolica/solar Silver into one hourly-grain table.

Task 4 of `seip-batch-pipeline`, depends on 05_silver_esios.
"""

from pyspark.sql import SparkSession

from seip.transform.bronze_to_silver import run_esios_hourly_join

spark = SparkSession.builder.getOrCreate()

hourly_df = run_esios_hourly_join(
    spark,
    silver_path="abfss://silver@seipdatalake.dfs.core.windows.net/esios",
    output_path="abfss://silver@seipdatalake.dfs.core.windows.net/esios_hourly",
)
print(f"silver/esios_hourly total rows: {hourly_df.count()}")
