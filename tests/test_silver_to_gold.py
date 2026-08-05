from datetime import datetime, timedelta, timezone

from seip.transform.silver_to_gold import (
    average_by_hour_of_day,
    compute_spread,
    find_daily_renewable_share_for_hour,
)


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


def test_find_daily_renewable_share_for_hour_within_window():
    # Day "2026-07-29" in Spain (CEST, UTC+2) starts at 2026-07-28T22:00:00Z.
    daily_shares = [(datetime(2026, 7, 28, 22, 0, 0, tzinfo=timezone.utc), 0.55)]

    # 2026-07-28T23:00Z is 01:00 on 2026-07-29 in Spain -- within that day's window.
    assert find_daily_renewable_share_for_hour(datetime(2026, 7, 28, 23, 0, 0, tzinfo=timezone.utc), daily_shares) == 0.55
    # 2026-07-29T21:00Z is 23:00 on 2026-07-29 in Spain -- still within the window.
    assert find_daily_renewable_share_for_hour(datetime(2026, 7, 29, 21, 0, 0, tzinfo=timezone.utc), daily_shares) == 0.55


def test_find_daily_renewable_share_for_hour_naive_utc_date_match_would_be_wrong():
    """The whole point of the range join: naively matching date(hour_utc) == date(day_start_utc)
    would only cover 2026-07-28T22:00-23:59Z (2 of 24 hours) -- this confirms the
    function instead covers the full 24h window starting at day_start_utc.
    """
    daily_shares = [(datetime(2026, 7, 28, 22, 0, 0, tzinfo=timezone.utc), 0.55)]

    # Same UTC calendar date (2026-07-28) but before the window starts.
    assert find_daily_renewable_share_for_hour(datetime(2026, 7, 28, 10, 0, 0, tzinfo=timezone.utc), daily_shares) is None
    # Different UTC calendar date (2026-07-29) but still inside the window.
    assert find_daily_renewable_share_for_hour(datetime(2026, 7, 29, 10, 0, 0, tzinfo=timezone.utc), daily_shares) == 0.55


def test_find_daily_renewable_share_for_hour_no_match_returns_none():
    daily_shares = [(datetime(2026, 7, 28, 22, 0, 0, tzinfo=timezone.utc), 0.55)]
    assert find_daily_renewable_share_for_hour(datetime(2026, 7, 30, 10, 0, 0, tzinfo=timezone.utc), daily_shares) is None


def test_find_daily_renewable_share_for_hour_upper_boundary_exclusive():
    day_start = datetime(2026, 7, 28, 22, 0, 0, tzinfo=timezone.utc)
    daily_shares = [(day_start, 0.55)]
    # Exactly 24h after day_start belongs to the *next* day's window, not this one.
    assert find_daily_renewable_share_for_hour(day_start + timedelta(hours=24), daily_shares) is None
