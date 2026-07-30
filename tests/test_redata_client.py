import pytest
import requests_mock

from seip.ingestion.redata_client import (
    RedataClientError,
    fetch_balance_electrico,
    fetch_generacion_estructura,
    fetch_intercambios,
    fetch_series,
)

URL_GENERACION = "https://apidatos.ree.es/es/datos/generacion/estructura-generacion"
URL_BALANCE = "https://apidatos.ree.es/es/datos/balance/balance-electrico"
URL_INTERCAMBIOS_FR = "https://apidatos.ree.es/es/datos/intercambios/francia-frontera"


def test_fetch_series_parses_standard_included_shape():
    payload = {
        "included": [
            {
                "type": "Generación",
                "attributes": {
                    "title": "Eólica",
                    "values": [
                        {"value": 4300.0, "percentage": 0.3, "datetime": "2026-07-30T00:00:00.000+02:00"},
                    ],
                },
            }
        ]
    }
    with requests_mock.Mocker() as m:
        m.get(URL_GENERACION, json=payload)
        result = fetch_generacion_estructura("2026-07-01", "2026-07-31")

    assert result == [
        {"title": "Eólica", "value": 4300.0, "percentage": 0.3, "datetime": "2026-07-30T00:00:00.000+02:00"}
    ]


def test_fetch_series_parses_balance_electrico_nested_content_shape():
    payload = {
        "included": [
            {
                "type": "Balance",
                "attributes": {
                    "title": "Generación renovable",
                    "content": [
                        {
                            "type": "Renovable",
                            "attributes": {
                                "title": "Renovable",
                                "values": [{"value": 100.0, "datetime": "2026-07-30T00:00:00.000+02:00"}],
                            },
                        },
                        {
                            "type": "No renovable",
                            "attributes": {
                                "title": "No renovable",
                                "values": [{"value": 50.0, "datetime": "2026-07-30T00:00:00.000+02:00"}],
                            },
                        },
                    ],
                },
            }
        ]
    }
    with requests_mock.Mocker() as m:
        m.get(URL_BALANCE, json=payload)
        result = fetch_balance_electrico("2026-07-01", "2026-07-31")

    assert result == [
        {"title": "Renovable", "value": 100.0, "datetime": "2026-07-30T00:00:00.000+02:00"},
        {"title": "No renovable", "value": 50.0, "datetime": "2026-07-30T00:00:00.000+02:00"},
    ]


def test_fetch_series_retries_then_succeeds():
    payload = {"included": [{"attributes": {"title": "x", "values": [{"value": 1.0}]}}]}
    with requests_mock.Mocker() as m:
        m.get(URL_GENERACION, [{"status_code": 400}, {"json": payload, "status_code": 200}])
        result = fetch_generacion_estructura("2026-07-01", "2026-07-31", backoff_seconds=0)

    assert result == [{"title": "x", "value": 1.0}]


def test_fetch_series_raises_after_max_retries():
    with requests_mock.Mocker() as m:
        m.get(URL_GENERACION, status_code=503)
        with pytest.raises(RedataClientError):
            fetch_generacion_estructura("2026-07-01", "2026-07-31", max_retries=2, backoff_seconds=0)


def test_fetch_intercambios_builds_country_specific_url():
    with requests_mock.Mocker() as m:
        m.get(URL_INTERCAMBIOS_FR, json={"included": []})
        result = fetch_intercambios("francia", "2026-07-01", "2026-07-31")

    assert result == []
    assert m.last_request.url.startswith(URL_INTERCAMBIOS_FR)


def test_fetch_series_passes_time_trunc_param():
    with requests_mock.Mocker() as m:
        m.get(URL_GENERACION, json={"included": []})
        fetch_series("generacion", "estructura-generacion", "2026-07-01", "2026-07-31", time_trunc="month")

    assert m.last_request.qs["time_trunc"] == ["month"]
