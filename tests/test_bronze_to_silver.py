import json
from datetime import datetime, timezone

from seip.transform.bronze_to_silver import parse_bronze_record, validate_record


def _raw(indicator_id, value, datetime_utc, geo_id=8741, geo_name="Península"):
    return json.dumps(
        {
            "indicator_id": indicator_id,
            "value": value,
            "datetime_utc": datetime_utc,
            "geo_id": geo_id,
            "geo_name": geo_name,
        }
    )


def test_parse_bronze_record_types_and_normalizes_utc():
    record = parse_bronze_record(_raw(1001, 120.5, "2026-07-30T14:00:00Z"))

    assert record == {
        "indicator_id": 1001,
        "datetime_utc": datetime(2026, 7, 30, 14, 0, 0, tzinfo=timezone.utc),
        "value": 120.5,
        "geo_id": 8741,
        "geo_name": "Península",
    }


def test_validate_record_flags_price_below_zero():
    record = parse_bronze_record(_raw(1001, -5.0, "2026-07-30T14:00:00Z"))
    assert validate_record(record) == ["value_out_of_range_0.0_700.0"]


def test_validate_record_flags_price_above_700():
    record = parse_bronze_record(_raw(600, 850.0, "2026-07-30T14:00:00Z"))
    assert validate_record(record) == ["value_out_of_range_0.0_700.0"]


def test_validate_record_no_flag_for_price_in_range():
    record = parse_bronze_record(_raw(1001, 120.5, "2026-07-30T14:00:00Z"))
    assert validate_record(record) == []


def test_validate_record_flags_night_solar_generation():
    # 2026-07-30T23:00:00Z -> local (UTC+2) 01:00, clearly night
    record = parse_bronze_record(_raw(1295, 15.0, "2026-07-30T23:00:00Z"))
    assert validate_record(record) == ["night_solar_generation_above_threshold"]


def test_validate_record_no_flag_for_daytime_solar():
    # 2026-07-30T11:00:00Z -> local (UTC+2) 13:00, clearly daytime
    record = parse_bronze_record(_raw(1295, 25000.0, "2026-07-30T11:00:00Z"))
    assert validate_record(record) == []


def test_validate_record_no_flag_for_low_night_solar():
    record = parse_bronze_record(_raw(1295, 2.0, "2026-07-30T23:00:00Z"))
    assert validate_record(record) == []


def test_validate_record_no_rules_for_wind_indicator():
    record = parse_bronze_record(_raw(551, 999999.0, "2026-07-30T23:00:00Z"))
    assert validate_record(record) == []
