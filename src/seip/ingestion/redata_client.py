"""Client for the REData API (apidatos.ree.es).

No authentication required. Applies the ingestion rules already validated
experimentally for this project (see DECISIONS.md / project spec section
3.1-3.3): retry on transient failures, and a dedicated parser branch for the
balance-electrico endpoint's nested JSON shape.
"""

from __future__ import annotations

import time

import requests

REDATA_BASE_URL = "https://apidatos.ree.es"

TRANSIENT_STATUS_CODES = {400, 500, 502, 503}


class RedataClientError(Exception):
    """Raised when a REData request fails after exhausting retries."""


def _parse_included(included: list[dict]) -> list[dict]:
    """Flatten the `included` section of a REData response into value records.

    Most endpoints expose values directly at `attributes.values`. The
    balance-electrico endpoint instead nests them one level deeper, inside
    `attributes.content[].attributes.values` — this checks both shapes.
    """
    records: list[dict] = []
    for item in included:
        attributes = item.get("attributes", {})
        title = attributes.get("title")
        values = attributes.get("values")
        if values:
            records.extend({"title": title, **v} for v in values)
            continue

        for sub_item in attributes.get("content", []):
            sub_attributes = sub_item.get("attributes", {})
            sub_title = sub_attributes.get("title", title)
            records.extend({"title": sub_title, **v} for v in sub_attributes.get("values", []))

    return records


def fetch_series(
    category: str,
    widget: str,
    start_date: str,
    end_date: str,
    time_trunc: str = "day",
    session: requests.Session | None = None,
    max_retries: int = 3,
    backoff_seconds: float = 5.0,
) -> list[dict]:
    """Fetch one REData series (e.g. category='generacion', widget='estructura-generacion').

    REData suffers transient failures (400 "datos no disponibles", 500, 503)
    unrelated to the request parameters — always retried with backoff before
    being treated as a real failure.
    """
    http = session or requests.Session()
    url = f"{REDATA_BASE_URL}/es/datos/{category}/{widget}"
    params = {"start_date": start_date, "end_date": end_date, "time_trunc": time_trunc}

    response = None
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = http.get(url, params=params, timeout=30)
        except requests.RequestException as exc:
            last_error = exc
            response = None
        else:
            if response.status_code not in TRANSIENT_STATUS_CODES:
                break
            last_error = RedataClientError(f"Transient HTTP {response.status_code} from REData")

        if attempt < max_retries - 1:
            time.sleep(backoff_seconds)

    if response is None or response.status_code in TRANSIENT_STATUS_CODES:
        raise RedataClientError(f"REData request failed after {max_retries} attempts") from last_error

    response.raise_for_status()
    return _parse_included(response.json().get("included", []))


def fetch_generacion_estructura(start_date: str, end_date: str, time_trunc: str = "day", **kwargs) -> list[dict]:
    """Generación por 15 fuentes. Granularidad segura: day (rangos de un mes funcionan bien).

    `hour` en rangos largos puede dar un 400 transitorio — lo cubre el retry.
    """
    return fetch_series("generacion", "estructura-generacion", start_date, end_date, time_trunc, **kwargs)


def fetch_evolucion_renovable_no_renovable(
    start_date: str, end_date: str, time_trunc: str = "day", **kwargs
) -> list[dict]:
    """Renovable vs no renovable, granularidad day."""
    return fetch_series("generacion", "evolucion-renovable-no-renovable", start_date, end_date, time_trunc, **kwargs)


def fetch_demanda_evolucion(start_date: str, end_date: str, time_trunc: str = "hour", **kwargs) -> list[dict]:
    """Demanda eléctrica horaria. Rango máximo seguro validado: ~2 días por llamada."""
    return fetch_series("demanda", "evolucion", start_date, end_date, time_trunc, **kwargs)


def fetch_balance_electrico(start_date: str, end_date: str, time_trunc: str = "day", **kwargs) -> list[dict]:
    """Balance eléctrico, 21 indicadores. Usa el parser que revisa `content` anidado."""
    return fetch_series("balance", "balance-electrico", start_date, end_date, time_trunc, **kwargs)


def fetch_intercambios(pais: str, start_date: str, end_date: str, time_trunc: str = "day", **kwargs) -> list[dict]:
    """Intercambios con Francia/Portugal/Marruecos.

    Endpoint de enriquecimiento opcional, no crítico: ha devuelto 500/503 con
    frecuencia en pruebas — tratar los fallos aquí de forma no bloqueante.
    """
    return fetch_series("intercambios", f"{pais}-frontera", start_date, end_date, time_trunc, **kwargs)
