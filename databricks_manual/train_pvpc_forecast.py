"""Manual, on-demand retraining of the 24 PVPC horizon models on Databricks.

Deliberately NOT part of any scheduled Job (see databricks_jobs/, and
DECISIONS.md / TODO.md's MLOps notes) - retraining automatically every day
without judgment on whether there's enough new, good-quality data isn't
reasonable MLOps for this project, matches the spec's realistic MLOps
scope (no automatic retraining). This script exists so that even a manual
step is checked-in, reviewed code - run from the Databricks Git folder,
not typed ad hoc into a notebook cell - not so that it runs unattended.

Run it (Databricks Git folder, "Python script", attached to a cluster
with the current wheel installed), then optionally follow with
databricks_jobs/10_ml_inference.py to refresh the forecast against the
newly registered `reference` models.
"""

from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from seip.ml.train import run as train_run

spark = SparkSession.builder.getOrCreate()

features_path = "abfss://gold@seipdatalake.dfs.core.windows.net/ml_features"
features_df = spark.read.format("delta").load(features_path)

# Same reasoning as the first Databricks run (2026-09-07, see DECISIONS.md):
# a fixed local-style test window (e.g. 3 months) would leave zero test rows
# whenever Azure's real data range is shorter than that - always derive the
# split from the latest hour actually present.
max_hour = features_df.agg(F.max("hour_utc").alias("max_hour")).toPandas().iloc[0]["max_hour"]
test_start = (max_hour - timedelta(days=3)).date()
print(f"max_hour: {max_hour}, test_start: {test_start}")

# mlflow.set_experiment() on Databricks requires an absolute workspace path,
# unlike the local file-based tracking server - see DECISIONS.md 2026-09-07.
# current_user() (Spark SQL, Unity Catalog-aware) avoids hardcoding an email.
current_user = spark.sql("SELECT current_user()").first()[0]
experiment_name = f"/Users/{current_user}/seip-pvpc-forecast"

results = train_run(spark, features_path=features_path, test_start_date=test_start, experiment_name=experiment_name)

for r in results:
    status = "BEATS baseline" if r["beats_baseline"] else "does NOT beat baseline"
    print(
        f"h+{r['horizon_hours']:>2}: rmse={r['rmse']:.2f} (baseline {r['baseline_rmse']:.2f}) "
        f"mae={r['mae']:.2f} (baseline {r['baseline_mae']:.2f}) - {status} "
        f"[{r['train_rows']} train / {r['test_rows']} test rows] "
        f"-> {r['registered_model_name']} v{r['registered_version']}"
    )
