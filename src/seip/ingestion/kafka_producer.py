"""Kafka producer for the 4 ESIOS streaming indicators (PVPC, SPOT, wind, solar).

Business logic (which indicators are due, how to build a message) has no
confluent_kafka import at module level, so it stays unit-testable without the
`streaming` extra installed or a broker running. Only `create_producer` /
`run_forever` touch confluent_kafka, and they import it lazily.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable

from seip.ingestion.esios_client import EsiosClientError, fetch_indicator_values

logger = logging.getLogger(__name__)

TICK_SECONDS = 5 * 60  # finest native granularity among the 4 indicators (wind/solar)

# Overlap added to every fetch window so a missed/delayed tick doesn't lose data.
# Duplicates this creates in Bronze are expected and removed downstream by the
# Silver-layer dedup on (indicator_id, datetime_utc) — the producer does not need
# to be exactly-once.
OVERLAP_MINUTES = 20


@dataclass(frozen=True)
class IndicatorConfig:
    indicator_id: int
    topic: str
    geo_id: int
    interval: timedelta


INDICATORS: tuple[IndicatorConfig, ...] = (
    IndicatorConfig(1001, "ree.pvpc", 8741, timedelta(hours=1)),
    IndicatorConfig(600, "ree.spot", 3, timedelta(minutes=15)),
    IndicatorConfig(551, "ree.eolica", 8741, timedelta(minutes=5)),
    IndicatorConfig(1295, "ree.solar", 8741, timedelta(minutes=5)),
)


def is_due(indicator: IndicatorConfig, last_run: datetime | None, now: datetime) -> bool:
    """An indicator is due if it has never run yet, or its native interval has elapsed."""
    if last_run is None:
        return True
    return now - last_run >= indicator.interval


def fetch_window(now: datetime, overlap_minutes: int = OVERLAP_MINUTES) -> tuple[str, str]:
    """(start, end) ISO datetime strings covering the last tick plus an overlap buffer."""
    start = now - timedelta(seconds=TICK_SECONDS) - timedelta(minutes=overlap_minutes)
    return start.strftime("%Y-%m-%dT%H:%M:%S"), now.strftime("%Y-%m-%dT%H:%M:%S")


def build_message(indicator_id: int, value: dict) -> tuple[bytes, bytes]:
    """Build the (key, value) Kafka message for one raw ESIOS value.

    Key = datetime_utc, so records for the same timestamp land on the same
    partition (not strictly required at this volume, but harmless).
    """
    key = value.get("datetime_utc", "")
    payload = {"indicator_id": indicator_id, **value}
    return key.encode("utf-8"), json.dumps(payload).encode("utf-8")


def poll_once(
    indicators: Iterable[IndicatorConfig],
    last_run: dict[int, datetime],
    now: datetime,
    api_key: str,
    send: Callable[[str, bytes, bytes], None],
) -> None:
    """Run one tick: fetch + publish for every indicator that is due, mutating last_run in place."""
    for indicator in indicators:
        if not is_due(indicator, last_run.get(indicator.indicator_id), now):
            continue

        start, end = fetch_window(now)
        try:
            values = fetch_indicator_values(
                indicator.indicator_id, start, end, indicator.geo_id, api_key
            )
        except EsiosClientError:
            logger.exception(
                "ESIOS fetch failed for indicator %s, will retry next tick", indicator.indicator_id
            )
            continue

        for value in values:
            key, payload = build_message(indicator.indicator_id, value)
            send(indicator.topic, key, payload)

        last_run[indicator.indicator_id] = now
        logger.info(
            "Published %d values for indicator %s to %s", len(values), indicator.indicator_id, indicator.topic
        )


def create_producer(bootstrap_servers: str, security_protocol: str = "PLAINTEXT", **extra_config):
    from confluent_kafka import Producer  # lazy import: keeps this module testable without confluent-kafka

    config = {"bootstrap.servers": bootstrap_servers, "security.protocol": security_protocol, **extra_config}
    return Producer(config)


def run_forever(api_key: str, bootstrap_servers: str, **producer_config) -> None:
    producer = create_producer(bootstrap_servers, **producer_config)

    def send(topic: str, key: bytes, value: bytes) -> None:
        producer.produce(topic, key=key, value=value)
        producer.poll(0)

    last_run: dict[int, datetime] = {}
    while True:
        now = datetime.now(timezone.utc)
        poll_once(INDICATORS, last_run, now, api_key, send)
        producer.flush()
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_forever(
        api_key=os.environ["ESIOS_API_TOKEN"],
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    )
