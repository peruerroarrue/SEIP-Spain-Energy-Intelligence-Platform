"""Manual smoke test: run the streaming Bronze job (Kafka -> Delta) once, availableNow.

Not part of the pytest suite (needs `spark` extra + local Kafka running).
Run: docker compose up -d
     python scripts/smoke_kafka_producer.py   (to have fresh messages to consume)
     python scripts/smoke_streaming_bronze.py
"""

from __future__ import annotations

from seip.ingestion.spark_session import build_local_spark_session
from seip.ingestion.streaming_bronze import run

BRONZE_PATH = "data/bronze/esios"
CHECKPOINT_PATH = "data/checkpoints/esios_bronze"


def main() -> None:
    spark = build_local_spark_session("seip-streaming-bronze-smoke", with_kafka=True)
    spark.sparkContext.setLogLevel("ERROR")

    print("-- running streaming Bronze job (availableNow) --")
    run(spark, bootstrap_servers="localhost:9092", bronze_path=BRONZE_PATH, checkpoint_path=CHECKPOINT_PATH)

    print("\n-- reading back from Bronze --")
    df = spark.read.format("delta").load(BRONZE_PATH)
    print(f"total rows in table: {df.count()}")
    df.groupBy("ingestion_date", "source").count().orderBy("source").show(truncate=False)
    print("sample row:")
    df.show(1, truncate=80, vertical=True)


if __name__ == "__main__":
    main()
