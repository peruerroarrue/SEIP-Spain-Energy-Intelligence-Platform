"""Job task: REData Bronze -> Silver.

Task 2 of `seip-batch-pipeline`, depends on 03_batch_redata.
"""

from pyspark.sql import SparkSession

from seip.transform.bronze_to_silver import run_redata

spark = SparkSession.builder.getOrCreate()

silver_df = run_redata(
    spark,
    bronze_path="abfss://bronze@seipdatalake.dfs.core.windows.net/redata",
    silver_path="abfss://silver@seipdatalake.dfs.core.windows.net/redata",
)
print(f"silver/redata total rows: {silver_df.count()}")
