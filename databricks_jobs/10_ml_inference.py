"""Job task: score the next 24h of PVPC price from the latest features.

Task 8 (last) of `seip-batch-pipeline`, depends on 09_ml_features. Loads
each horizon's `reference`-aliased model from the Model Registry - training
a new reference version is a separate, manual/on-demand step (train.py),
not part of this scheduled Job.
"""

from pyspark.sql import SparkSession

from seip.ml.inference import run

spark = SparkSession.builder.getOrCreate()

run(
    spark,
    features_path="abfss://gold@seipdatalake.dfs.core.windows.net/ml_features",
    output_path="abfss://gold@seipdatalake.dfs.core.windows.net/pvpc_forecast",
)
