import math
from datetime import datetime, timedelta

from seip.ml.features import compute_calendar_features


def test_hour_zero_is_the_reference_angle():
    features = compute_calendar_features(datetime(2026, 7, 27, 0, 0, 0))
    assert math.isclose(features["hour_sin"], 0.0, abs_tol=1e-9)
    assert math.isclose(features["hour_cos"], 1.0, abs_tol=1e-9)


def test_hour_six_is_a_quarter_cycle():
    features = compute_calendar_features(datetime(2026, 7, 27, 6, 0, 0))
    assert math.isclose(features["hour_sin"], 1.0, abs_tol=1e-9)
    assert math.isclose(features["hour_cos"], 0.0, abs_tol=1e-9)


def test_hour_twelve_is_a_half_cycle():
    features = compute_calendar_features(datetime(2026, 7, 27, 12, 0, 0))
    assert math.isclose(features["hour_sin"], 0.0, abs_tol=1e-9)
    assert math.isclose(features["hour_cos"], -1.0, abs_tol=1e-9)


def test_day_of_week_repeats_every_seven_days():
    a = compute_calendar_features(datetime(2026, 7, 27, 10, 0, 0))
    b = compute_calendar_features(datetime(2026, 7, 27, 10, 0, 0) + timedelta(days=7))
    assert math.isclose(a["dow_sin"], b["dow_sin"], abs_tol=1e-9)
    assert math.isclose(a["dow_cos"], b["dow_cos"], abs_tol=1e-9)


def test_day_of_week_differs_across_the_week():
    monday = compute_calendar_features(datetime(2026, 7, 27, 10, 0, 0))  # a Monday
    tuesday = compute_calendar_features(datetime(2026, 7, 28, 10, 0, 0))
    assert (monday["dow_sin"], monday["dow_cos"]) != (tuesday["dow_sin"], tuesday["dow_cos"])


def test_month_of_year_passthrough():
    features = compute_calendar_features(datetime(2026, 12, 15, 10, 0, 0))
    assert features["month_of_year"] == 12
