"""Manual smoke test: one real producer tick against local Kafka + real ESIOS API.

Not part of the pytest suite. Requires:
  - docker compose up -d  (local Kafka on localhost:9092)
  - ESIOS_API_TOKEN in .env / token.env at the repo root

Run: python scripts/smoke_kafka_producer.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer

from seip.ingestion import kafka_producer as kp


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    _load_dotenv(repo_root / ".env")
    _load_dotenv(repo_root / "token.env")
    token = os.environ["ESIOS_API_TOKEN"]

    producer = Producer({"bootstrap.servers": "localhost:9092"})
    sent_counts: dict[str, int] = {}

    def send(topic: str, key: bytes, value: bytes) -> None:
        producer.produce(topic, key=key, value=value)
        sent_counts[topic] = sent_counts.get(topic, 0) + 1

    now = datetime.now(timezone.utc)
    kp.poll_once(kp.INDICATORS, last_run={}, now=now, api_key=token, send=send)
    producer.flush(10)

    print("-- produced --")
    for topic, count in sent_counts.items():
        print(f"{topic}: {count} messages")

    print("\n-- consuming back one message per topic --")
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "smoke-test",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(list(sent_counts.keys()))
    remaining = set(sent_counts.keys())
    deadline = datetime.now(timezone.utc).timestamp() + 15
    while remaining and datetime.now(timezone.utc).timestamp() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        print(f"{msg.topic()}: key={msg.key()!r} value={msg.value()!r}")
        remaining.discard(msg.topic())
    consumer.close()

    if remaining:
        print(f"\nWARNING: no message consumed back for: {remaining}")


if __name__ == "__main__":
    main()
