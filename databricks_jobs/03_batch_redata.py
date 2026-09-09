"""Job task: ingest today's REData series into Bronze.

Task 1 of `seip-batch-pipeline` (scheduled daily), independent of the other
tasks in this Job.
"""

import logging
from datetime import date

from pyspark.sql import SparkSession

from seip.ingestion.batch_job import run

# batch_job logs a "non-critical source X failed" line per skipped REData
# series via the standard logging module - needs a configured handler/level
# to actually show up in this task's Output tab.
logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder.getOrCreate()

df = run(spark, run_date=date.today(), bronze_path="abfss://bronze@seipdatalake.dfs.core.windows.net/redata")
print(f"bronze/redata rows written this run: {df.count()}")
