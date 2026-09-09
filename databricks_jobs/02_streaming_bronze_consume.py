"""Job task: consume the 4 Confluent Cloud topics into Bronze.

Task 2 of `seip-streaming-ingest`, depends on 01_streaming_producer_tick
(reads whatever that tick just published, plus anything from before).
"""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from seip.ingestion.streaming_bronze import run

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

sasl_username = dbutils.secrets.get(scope="seip-secrets", key="kafka-sasl-username")
sasl_password = dbutils.secrets.get(scope="seip-secrets", key="kafka-sasl-password")
jaas_config = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{sasl_username}" password="{sasl_password}";'
)

bronze_path = "abfss://bronze@seipdatalake.dfs.core.windows.net/esios"

run(
    spark,
    bootstrap_servers="pkc-55q18.switzerlandnorth.azure.confluent.cloud:9092",
    bronze_path=bronze_path,
    checkpoint_path="abfss://bronze@seipdatalake.dfs.core.windows.net/_checkpoints/esios_bronze",
    **{
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
    },
)

# run() already awaited the query's termination - read the table back just
# for a row count, this task's stream write doesn't report one on its own.
print(f"bronze/esios total rows: {spark.read.format('delta').load(bronze_path).count()}")
