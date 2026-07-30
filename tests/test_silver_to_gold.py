from datetime import datetime, timezone

from seip.transform.silver_to_gold import average_by_hour_of_day, compute_spread


def _hour(day, hour, pvpc=None, spot=None):
    row = {"hour_utc": datetime(2026, 7, day, hour, 0, 0, tzinfo=timezone.utc)}
    if pvpc is not None:
        row["pvpc_eur_mwh"] = pvpc
    if spot is not None:
        row["spot_eur_mwh"] = spot
    return row


def test_average_by_hour_of_day_averages_across_days():
    rows = [
        _hour(29, 14, pvpc=100.0, spot=90.0),
        _hour(30, 14, pvpc=120.0, spot=110.0),
    ]
    result = average_by_hour_of_day(rows)
    assert result[14] == {"avg_pvpc_eur_mwh": 110.0, "avg_spot_eur_mwh": 100.0}


def test_average_by_hour_of_day_keeps_hours_separate():
    rows = [_hour(29, 14, pvpc=100.0), _hour(29, 15, pvpc=200.0)]
    result = average_by_hour_of_day(rows)
    assert result[14]["avg_pvpc_eur_mwh"] == 100.0
    assert result[15]["avg_pvpc_eur_mwh"] == 200.0


def test_average_by_hour_of_day_skips_missing_values_instead_of_treating_as_zero():
    rows = [
        _hour(29, 14, pvpc=100.0),
        _hour(30, 14, pvpc=None),  # missing PVPC that day
    ]
    result = average_by_hour_of_day(rows)
    # Average must be 100.0 (the one real reading), not 50.0 (as if missing == 0)
    assert result[14]["avg_pvpc_eur_mwh"] == 100.0


def test_average_by_hour_of_day_none_for_hour_with_no_data_at_all():
    rows = [_hour(29, 14, pvpc=100.0, spot=90.0)]
    result = average_by_hour_of_day(rows)
    assert result[14]["avg_spot_eur_mwh"] == 90.0
    assert 15 not in result


def test_compute_spread_basic():
    assert compute_spread(120.0, 100.0) == 20.0


def test_compute_spread_none_if_pvpc_missing():
    assert compute_spread(None, 100.0) is None


def test_compute_spread_none_if_spot_missing():
    assert compute_spread(120.0, None) is None
