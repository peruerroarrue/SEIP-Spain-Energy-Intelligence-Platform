"""Manual, on-demand historical backfill (ESIOS + REData) against Azure, and
rebuild of the downstream Silver/hourly-join/feature-store chain.

Deliberately NOT part of any scheduled Job (see databricks_jobs/) - this is
a "fill a gap" operation, not something that should run daily. Needed
whenever `seip-streaming-ingest` was paused/not running for a stretch (e.g.
between the initial ESIOS backfill and when the scheduled Job actually
started running continuously - see DECISIONS.md 2026-09-09) and downstream
tables end up with fewer usable rows than their date range suggests, once
row-dropping features like `renewable_share` or the 168h lag are involved.

Edit START_DATE below before running - defaults to a wide, safe range
rather than trying to guess the exact gap.
"""

from datetime import date

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from seip.ingestion.batch_job import run_backfill as backfill_redata
from seip.ingestion.historical_backfill import run as backfill_esios
from seip.ml.features import run as features_run
from seip.transform.bronze_to_silver import run_esios, run_esios_hourly_join, run_redata

START_DATE = date(2026, 8, 5)
END_DATE = date.today()

BRONZE = "abfss://bronze@seipdatalake.dfs.core.windows.net"
SILVER = "abfss://silver@seipdatalake.dfs.core.windows.net"
GOLD = "abfss://gold@seipdatalake.dfs.core.windows.net"

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

print(f"-- backfilling ESIOS [{START_DATE}, {END_DATE}) --")
esios_token = dbutils.secrets.get(scope="seip-secrets", key="esios-api-token")
esios_rows = backfill_esios(spark, START_DATE, END_DATE, esios_token, bronze_path=f"{BRONZE}/esios")
print(f"ESIOS: {esios_rows} rows written to Bronze")

print(f"-- backfilling REData [{START_DATE}, {END_DATE}) --")
redata_df = backfill_redata(spark, START_DATE, END_DATE, bronze_path=f"{BRONZE}/redata")
print(f"REData: {redata_df.count()} rows written to Bronze")

print("-- rebuilding Silver + hourly join + feature store --")
run_redata(spark, bronze_path=f"{BRONZE}/redata", silver_path=f"{SILVER}/redata")
run_esios(
    spark,
    bronze_path=f"{BRONZE}/esios",
    silver_path=f"{SILVER}/esios",
    checkpoint_path=f"{SILVER}/_checkpoints/esios_silver",
)
run_esios_hourly_join(spark, silver_path=f"{SILVER}/esios", output_path=f"{SILVER}/esios_hourly")

features_df = features_run(
    spark,
    silver_hourly_path=f"{SILVER}/esios_hourly",
    output_path=f"{GOLD}/ml_features",
    redata_silver_path=f"{SILVER}/redata",
)
with_share = features_df.filter("renewable_share is not null").count()
print(f"features: {features_df.count()} rows total, {with_share} with renewable_share")
