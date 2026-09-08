"""Job task: ESIOS Bronze (streaming) -> Silver.

Task 3 of `seip-batch-pipeline`, independent within this Job - it reads
whatever `seip-streaming-ingest` has landed in Bronze by the time this
daily Job runs, not something produced by an earlier task here.
"""

from pyspark.sql import SparkSession

from seip.transform.bronze_to_silver import run_esios

spark = SparkSession.builder.getOrCreate()

run_esios(
    spark,
    bronze_path="abfss://bronze@seipdatalake.dfs.core.windows.net/esios",
    silver_path="abfss://silver@seipdatalake.dfs.core.windows.net/esios",
    checkpoint_path="abfss://silver@seipdatalake.dfs.core.windows.net/_checkpoints/esios_silver",
)
