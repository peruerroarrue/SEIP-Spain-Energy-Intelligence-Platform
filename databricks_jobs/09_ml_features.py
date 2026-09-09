"""Job task: build the ML feature table.

Task 7 of `seip-batch-pipeline`, depends on 04_silver_redata (for
renewable_share) and 06_silver_esios_hourly_join.
"""

from pyspark.sql import SparkSession

from seip.ml.features import run

spark = SparkSession.builder.getOrCreate()

features_df = run(
    spark,
    silver_hourly_path="abfss://silver@seipdatalake.dfs.core.windows.net/esios_hourly",
    output_path="abfss://gold@seipdatalake.dfs.core.windows.net/ml_features",
    redata_silver_path="abfss://silver@seipdatalake.dfs.core.windows.net/redata",
)
print(f"gold/ml_features rows: {features_df.count()}")
