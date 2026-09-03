"""Manual smoke test: one real producer tick against Confluent Cloud (SASL_SSL) + real ESIOS API.

Not part of the pytest suite. Requires:
  - ESIOS_API_TOKEN in .env / token.env at the repo root (same as smoke_kafka_producer.py)
  - KAFKA_BOOTSTRAP_SERVERS / KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD in .env / token.env
    (Confluent Cloud service account API Key = username, API Secret = password)

Unlike smoke_kafka_producer.py (local Docker Kafka, PLAINTEXT), this exercises the
actual code path that will run in production: kafka_producer.run_once() with the
SASL_SSL config built the same way the module's own __main__ entrypoint builds it.

Run: python scripts/smoke_kafka_confluent_cloud.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer

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

    esios_token = os.environ["ESIOS_API_TOKEN"]
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    sasl_config = kp._sasl_config_from_env()
    if not sasl_config:
        raise RuntimeError(
            "KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD not set — see this script's docstring."
        )

    print(f"-- producing one tick to {bootstrap_servers} (SASL_SSL) --")
    kp.run_once(
        api_key=esios_token,
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        **sasl_config,
    )
    print("run_once() completed without raising — see logging.INFO output above for per-topic counts")

    print("\n-- consuming back one message per topic --")
    topics = [indicator.topic for indicator in kp.INDICATORS]
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": sasl_config["sasl.mechanisms"],
            "sasl.username": sasl_config["sasl.username"],
            "sasl.password": sasl_config["sasl.password"],
            "group.id": "seip-smoke-test-confluent-cloud",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(topics)
    remaining = set(topics)
    deadline = datetime.now(timezone.utc).timestamp() + 20
    while remaining and datetime.now(timezone.utc).timestamp() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        print(f"{msg.topic()}: key={msg.key()!r} value={msg.value()!r}"[:200])
        remaining.discard(msg.topic())
    consumer.close()

    if remaining:
        print(f"\nWARNING: no message consumed back for: {remaining}")
    else:
        print("\nAll 4 topics confirmed round-trip through Confluent Cloud.")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
