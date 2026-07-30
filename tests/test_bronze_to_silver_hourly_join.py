from datetime import datetime, timezone

from seip.transform.bronze_to_silver import aggregate_records_to_hour, pivot_hourly_records

HOUR = datetime(2026, 7, 30, 14, 0, 0, tzinfo=timezone.utc)


def _record(indicator_id, value, minute):
    return {"indicator_id": indicator_id, "value": value, "datetime_utc": HOUR.replace(minute=minute)}


def test_aggregate_records_to_hour_averages_within_hour():
    records = [
        _record(600, 80.0, 0),
        _record(600, 90.0, 15),
        _record(600, 100.0, 30),
        _record(600, 110.0, 45),
    ]
    result = aggregate_records_to_hour(records)
    assert result == {(600, HOUR): 95.0}


def test_aggregate_records_to_hour_is_noop_for_single_reading():
    records = [_record(1001, 120.5, 0)]
    result = aggregate_records_to_hour(records)
    assert result == {(1001, HOUR): 120.5}


def test_aggregate_records_to_hour_keeps_separate_hours_and_indicators_apart():
    next_hour = HOUR.replace(hour=15)
    records = [
        _record(1001, 100.0, 0),
        {"indicator_id": 1001, "value": 200.0, "datetime_utc": next_hour},
        _record(551, 4000.0, 0),
    ]
    result = aggregate_records_to_hour(records)
    assert result == {
        (1001, HOUR): 100.0,
        (1001, next_hour): 200.0,
        (551, HOUR): 4000.0,
    }


def test_pivot_hourly_records_builds_wide_row_per_hour():
    hourly_averages = {(1001, HOUR): 120.5, (600, HOUR): 95.0, (551, HOUR): 4000.0, (1295, HOUR): 28000.0}
    result = pivot_hourly_records(hourly_averages)

    assert result == {
        HOUR: {
            "pvpc_eur_mwh": 120.5,
            "spot_eur_mwh": 95.0,
            "eolica_mw": 4000.0,
            "solar_mw": 28000.0,
        }
    }


def test_pivot_hourly_records_uses_none_for_missing_indicator():
    hourly_averages = {(1001, HOUR): 120.5}
    result = pivot_hourly_records(hourly_averages)

    assert result[HOUR] == {
        "pvpc_eur_mwh": 120.5,
        "spot_eur_mwh": None,
        "eolica_mw": None,
        "solar_mw": None,
    }
