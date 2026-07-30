"""Client for the ESIOS API (api.esios.ree.es).

Applies the ingestion rules already validated experimentally for this project
(see DECISIONS.md / project spec section 3.2-3.3): never request time_trunc
aggregation, always filter by geo_id, never use the discontinued PVPC
indicator 1013.
"""

from __future__ import annotations

import time

import requests

ESIOS_BASE_URL = "https://api.esios.ree.es"

# Discontinued after the 2021 tariff reform (2.0TD) — indicator 1001 replaces it.
DISCONTINUED_INDICATORS = {1013}

TRANSIENT_STATUS_CODES = {429, 500, 502, 503}


class EsiosClientError(Exception):
    """Raised when an ESIOS request fails after exhausting retries."""


def _headers(api_key: str) -> dict:
    return {
        "x-api-key": api_key,
        "Accept": "application/json; application/vnd.esios-api-v2+json",
    }


def fetch_indicator_values(
    indicator_id: int,
    start_date: str,
    end_date: str,
    geo_id: int,
    api_key: str,
    session: requests.Session | None = None,
    max_retries: int = 3,
    backoff_seconds: float = 5.0,
) -> list[dict]:
    """Fetch raw indicator values at their native granularity, filtered by geo_id.

    Never pass a time_trunc parameter: requesting aggregation via the API produces
    values that match neither the sum nor the mean of the underlying native data
    (verified experimentally for indicators 1001, 600, 551, 1295). Aggregate
    downstream in Spark/pandas instead.
    """
    if indicator_id in DISCONTINUED_INDICATORS:
        raise ValueError(
            f"Indicator {indicator_id} is discontinued since the 2021 tariff reform; use 1001 instead."
        )

    http = session or requests.Session()
    url = f"{ESIOS_BASE_URL}/indicators/{indicator_id}"
    params = {"start_date": start_date, "end_date": end_date}

    response = None
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = http.get(url, headers=_headers(api_key), params=params, timeout=30)
        except requests.RequestException as exc:
            last_error = exc
            response = None
        else:
            if response.status_code not in TRANSIENT_STATUS_CODES:
                break
            last_error = EsiosClientError(f"Transient HTTP {response.status_code} from ESIOS")

        if attempt < max_retries - 1:
            time.sleep(backoff_seconds)

    if response is None or response.status_code in TRANSIENT_STATUS_CODES:
        raise EsiosClientError(f"ESIOS request failed after {max_retries} attempts") from last_error

    response.raise_for_status()
    values = response.json().get("indicator", {}).get("values", [])
    return [v for v in values if v.get("geo_id") == geo_id]


def get_indicator_metadata(
    indicator_id: int,
    api_key: str,
    session: requests.Session | None = None,
) -> dict:
    """Fetch indicator metadata (magnitud, tiempo) without requesting values.

    Check `tiempo` before wiring up ingestion for a new indicator: it reports the
    real native granularity (e.g. "Cinco minutos", "Hora").
    """
    http = session or requests.Session()
    url = f"{ESIOS_BASE_URL}/indicators/{indicator_id}"
    response = http.get(url, headers=_headers(api_key), timeout=30)
    response.raise_for_status()
    return response.json().get("indicator", {})
