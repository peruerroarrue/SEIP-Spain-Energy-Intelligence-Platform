import pytest
import requests_mock

from seip.ingestion.esios_client import (
    EsiosClientError,
    fetch_indicator_values,
    get_indicator_metadata,
)

API_KEY = "test-key"
URL_1001 = "https://api.esios.ree.es/indicators/1001"


def _payload(values: list[dict]) -> dict:
    return {"indicator": {"id": 1001, "values": values}}


def test_fetch_indicator_values_filters_by_geo_id():
    values = [
        {"value": 120.5, "datetime_utc": "2026-01-01T00:00:00Z", "geo_id": 8741, "geo_name": "Península"},
        {"value": 99.0, "datetime_utc": "2026-01-01T00:00:00Z", "geo_id": 3, "geo_name": "España"},
    ]
    with requests_mock.Mocker() as m:
        m.get(URL_1001, json=_payload(values))
        result = fetch_indicator_values(1001, "2026-01-01", "2026-01-02", geo_id=8741, api_key=API_KEY)

    assert result == [values[0]]


def test_fetch_indicator_values_retries_then_succeeds():
    with requests_mock.Mocker() as m:
        m.get(
            URL_1001,
            [
                {"status_code": 503},
                {"json": _payload([{"value": 1.0, "geo_id": 8741}]), "status_code": 200},
            ],
        )
        result = fetch_indicator_values(
            1001, "2026-01-01", "2026-01-02", geo_id=8741, api_key=API_KEY, backoff_seconds=0
        )

    assert result == [{"value": 1.0, "geo_id": 8741}]


def test_fetch_indicator_values_raises_after_max_retries():
    with requests_mock.Mocker() as m:
        m.get(URL_1001, status_code=503)
        with pytest.raises(EsiosClientError):
            fetch_indicator_values(
                1001, "2026-01-01", "2026-01-02", geo_id=8741, api_key=API_KEY,
                max_retries=2, backoff_seconds=0,
            )


def test_rejects_discontinued_indicator():
    with requests_mock.Mocker() as m:
        with pytest.raises(ValueError, match="discontinued"):
            fetch_indicator_values(1013, "2026-01-01", "2026-01-02", geo_id=8741, api_key=API_KEY)
        assert not m.called


def test_get_indicator_metadata():
    with requests_mock.Mocker() as m:
        m.get(URL_1001, json={"indicator": {"magnitud": "Precio", "tiempo": "Hora"}})
        metadata = get_indicator_metadata(1001, api_key=API_KEY)

    assert metadata == {"magnitud": "Precio", "tiempo": "Hora"}
