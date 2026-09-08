"""Job task: ingest today's REData series into Bronze.

Task 1 of `seip-batch-pipeline` (scheduled daily), independent of the other
tasks in this Job.
"""

from datetime import date

from pyspark.sql import SparkSession

from seip.ingestion.batch_job import run

spark = SparkSession.builder.getOrCreate()

run(spark, run_date=date.today(), bronze_path="abfss://bronze@seipdatalake.dfs.core.windows.net/redata")
