"""Local PySpark + Delta Lake session builder, for dev/smoke-test use only.

Not used against Databricks — there, the Spark session and Delta support are
already provided by the workspace/cluster runtime.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def build_local_spark_session(app_name: str = "seip-local", with_kafka: bool = False) -> "SparkSession":
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Without this, timestamp columns (e.g. Kafka's `timestamp`) are interpreted
        # in the JVM's default timezone when converted to Python, silently producing
        # naive/local timestamps instead of UTC — caught when streaming Bronze's
        # `fetched_at` came out without a UTC offset, unlike the batch path's.
        .config("spark.sql.session.timeZone", "UTC")
    )
    extra_packages = []
    if with_kafka:
        import pyspark

        extra_packages = [f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"]
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
