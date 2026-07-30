import json
from datetime import datetime, timezone

from seip.transform.bronze_to_silver import parse_redata_bronze_record


def _raw(source, title, value, dt, percentage=None):
    payload = {"source": source, "title": title, "value": value, "datetime": dt}
    if percentage is not None:
        payload["percentage"] = percentage
    return json.dumps(payload)


def test_parse_redata_bronze_record_normalizes_offset_datetime_to_utc():
    record = parse_redata_bronze_record(_raw("generacion_estructura", "Eólica", 4300.0, "2026-07-30T00:00:00+02:00"))

    assert record == {
        "source": "generacion_estructura",
        "title": "Eólica",
        "datetime_utc": datetime(2026, 7, 29, 22, 0, 0, tzinfo=timezone.utc),
        "value": 4300.0,
        "percentage": None,
    }


def test_parse_redata_bronze_record_keeps_optional_percentage():
    record = parse_redata_bronze_record(
        _raw("evolucion_renovable_no_renovable", "Renovable", 100.0, "2026-07-30T00:00:00+02:00", percentage=0.42)
    )

    assert record["percentage"] == 0.42


def test_parse_redata_bronze_record_allows_negative_value():
    # Real data has shown legitimate negative values (e.g. "Carbón": -3 in
    # generacion_estructura) — Silver must not reject or clamp these.
    record = parse_redata_bronze_record(_raw("generacion_estructura", "Carbón", -3.0, "2026-07-30T00:00:00+02:00"))
    assert record["value"] == -3.0
