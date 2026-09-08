"""Job task: publish one ESIOS tick to the 4 Confluent Cloud topics.

Task 1 of the `seip-streaming-ingest` Job (scheduled every 5-15 min). Runs
as a Databricks "Python script" task, not a notebook - this file lives in
the project's git repo (synced to the workspace via a Databricks Git
folder), so the Job always runs whatever is on `main`, not an ad-hoc copy
edited in the Databricks UI.

Plain Python script tasks don't get `spark`/`dbutils` injected as globals
the way notebooks do - both have to be created explicitly. This task
doesn't touch Spark at all (kafka_producer.run_once is pure Python +
confluent_kafka), but still needs a SparkSession to construct DBUtils and
read secrets.
"""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from seip.ingestion.kafka_producer import run_once

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

esios_token = dbutils.secrets.get(scope="seip-secrets", key="esios-api-token")
sasl_username = dbutils.secrets.get(scope="seip-secrets", key="kafka-sasl-username")
sasl_password = dbutils.secrets.get(scope="seip-secrets", key="kafka-sasl-password")

run_once(
    api_key=esios_token,
    bootstrap_servers="pkc-55q18.switzerlandnorth.azure.confluent.cloud:9092",
    security_protocol="SASL_SSL",
    **{
        "sasl.mechanisms": "PLAIN",
        "sasl.username": sasl_username,
        "sasl.password": sasl_password,
    },
)
