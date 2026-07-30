"""Spark Structured Streaming job: lands the 4 ESIOS Kafka topics into Bronze.

This is the streaming counterpart of batch_job.py — same Bronze row shape
(ingestion_date, source, fetched_at, raw_json), but sourced from Kafka
instead of a REData API call. Kept in its own Delta table (data/bronze/esios
by default) since it's a structurally different source than the REData batch
Bronze table, even though both share the same generic schema.

`to_bronze_row` / `topics` have no Spark import and stay unit-testable. Only
`run` (and its `_process_batch` callback) touch PySpark/Delta Lake.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from seip.ingestion.kafka_producer import INDICATORS

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


def topics() -> list[str]:
    """Kafka topics to consume — the same ones kafka_producer.py publishes to."""
    return [indicator.topic for indicator in INDICATORS]


def to_bronze_row(topic: str, value: bytes, kafka_timestamp_epoch: float) -> dict:
    """Wrap one raw Kafka record as a Bronze row, in the same schema-on-read
    style as batch_job.to_bronze_rows: the message payload is kept untouched,
    typing/casting is a Silver responsibility.

    Takes the Kafka record timestamp as a Unix epoch (seconds), not a
    collected Spark datetime. Confirmed experimentally on this machine:
    PySpark's `.collect()` converts TimestampType columns to Python datetimes
    using the JVM's local default timezone, completely ignoring
    `spark.sql.session.timeZone` — a naive datetime collected that way was
    off by the local UTC offset (+2h under CEST). Casting to an epoch in
    Spark *before* collecting sidesteps this: an epoch is timezone-agnostic,
    so `datetime.fromtimestamp(epoch, tz=timezone.utc)` reconstructs the
    correct instant unambiguously. See _process_batch for the Spark-side cast.
    """
    kafka_timestamp = datetime.fromtimestamp(kafka_timestamp_epoch, tz=timezone.utc)
    return {
        "ingestion_date": kafka_timestamp.date().isoformat(),
        "source": topic,
        "fetched_at": kafka_timestamp.isoformat(),
        "raw_json": value.decode("utf-8"),
    }


def _process_batch(batch_df: "DataFrame", batch_id: int, bronze_path: str) -> None:
    from pyspark.sql import functions as F

    rows = [
        to_bronze_row(row.topic, row.value, row.timestamp_epoch)
        for row in batch_df.select(
            "topic", "value", F.col("timestamp").cast("double").alias("timestamp_epoch")
        ).collect()
    ]
    if not rows:
        return
    spark = batch_df.sparkSession
    spark.createDataFrame(rows).write.format("delta").mode("append").partitionBy("ingestion_date").save(bronze_path)


def run(
    spark: "SparkSession",
    bootstrap_servers: str,
    bronze_path: str,
    checkpoint_path: str,
) -> "StreamingQuery":
    """Consume the 4 ESIOS topics once (availableNow) and append new records to Bronze.

    availableNow (rather than a continuous/always-on trigger) processes
    whatever is currently in Kafka and stops — meant to be run on a schedule
    (e.g. every 15-30 min via a Databricks Job), not kept running 24/7, given
    this project's limited Azure credit budget.
    """
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", ",".join(topics()))
        .option("startingOffsets", "earliest")
        .load()
    )

    query = (
        df.writeStream.foreachBatch(lambda batch_df, batch_id: _process_batch(batch_df, batch_id, bronze_path))
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return query


if __name__ == "__main__":
    import os

    from seip.ingestion.spark_session import build_local_spark_session

    spark_session = build_local_spark_session("seip-streaming-bronze", with_kafka=True)
    run(
        spark_session,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        bronze_path="data/bronze/esios",
        checkpoint_path="data/checkpoints/esios_bronze",
    )
