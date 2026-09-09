"""Job task: score the next 24h of PVPC price from the latest features.

Task 8 (last) of `seip-batch-pipeline`, depends on 09_ml_features. Loads
each horizon's `reference`-aliased model from the Model Registry - training
a new reference version is a separate, manual/on-demand step (train.py),
not part of this scheduled Job.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from seip.ml.inference import run

spark = SparkSession.builder.getOrCreate()

forecast_df = run(
    spark,
    features_path="abfss://gold@seipdatalake.dfs.core.windows.net/ml_features",
    output_path="abfss://gold@seipdatalake.dfs.core.windows.net/pvpc_forecast",
)
summary = forecast_df.agg(
    F.min("target_hour_utc").alias("first_target"),
    F.max("target_hour_utc").alias("last_target"),
    F.min("predicted_pvpc_eur_mwh").alias("min_price"),
    F.max("predicted_pvpc_eur_mwh").alias("max_price"),
).collect()[0]
print(
    f"gold/pvpc_forecast: {forecast_df.count()} rows, "
    f"{summary['first_target']} to {summary['last_target']}, "
    f"predicted {summary['min_price']:.2f}-{summary['max_price']:.2f} EUR/MWh"
)
