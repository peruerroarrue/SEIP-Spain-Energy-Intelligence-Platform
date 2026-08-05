import math
from datetime import date, datetime

from seip.ml.train import (
    compute_errors,
    compute_mae,
    compute_rmse,
    compute_target_calendar_features,
    registered_model_name,
    train_test_split_by_date,
)


def test_registered_model_name_is_per_horizon():
    assert registered_model_name(1) == "seip-pvpc-forecast-h1"
    assert registered_model_name(24) == "seip-pvpc-forecast-h24"
    assert registered_model_name(1) != registered_model_name(2)


def test_compute_target_calendar_features_hour_zero_reference_angle():
    features = compute_target_calendar_features(datetime(2026, 7, 27, 23, 0, 0), horizon_hours=1)
    # origin 23:00 + 1h -> target 00:00
    assert math.isclose(features["target_hour_sin"], 0.0, abs_tol=1e-9)
    assert math.isclose(features["target_hour_cos"], 1.0, abs_tol=1e-9)


def test_compute_target_calendar_features_h24_is_same_hour_next_day():
    origin = datetime(2026, 7, 27, 10, 0, 0)  # a Monday
    h24_features = compute_target_calendar_features(origin, horizon_hours=24)
    # 24h later is the same hour-of-day...
    direct = compute_target_calendar_features(datetime(2026, 7, 28, 10, 0, 0), horizon_hours=0)
    assert math.isclose(h24_features["target_hour_sin"], direct["target_hour_sin"], abs_tol=1e-9)
    assert math.isclose(h24_features["target_hour_cos"], direct["target_hour_cos"], abs_tol=1e-9)
    # ...but a different day of the week than the origin day.
    origin_dow_features = compute_target_calendar_features(origin, horizon_hours=0)
    assert h24_features["target_dow_sin"] != origin_dow_features["target_dow_sin"]


def test_compute_target_calendar_features_month_passthrough():
    features = compute_target_calendar_features(datetime(2026, 12, 31, 23, 0, 0), horizon_hours=1)
    assert features["target_month"] == 1  # rolls into January


def test_compute_errors_is_true_minus_predicted():
    errors = compute_errors([100.0, 200.0], [90.0, 210.0])
    assert errors == [10.0, -10.0]


def test_compute_rmse_basic():
    # errors [3, -4] -> squared [9, 16] -> mean 12.5 -> sqrt ~3.5355
    rmse = compute_rmse([3.0, -4.0])
    assert round(rmse, 4) == 3.5355


def test_compute_rmse_zero_for_perfect_predictions():
    assert compute_rmse([0.0, 0.0, 0.0]) == 0.0


def test_compute_mae_basic():
    assert compute_mae([3.0, -4.0, 5.0]) == 4.0


def test_train_test_split_by_date_splits_chronologically():
    rows = [
        {"hour_utc": datetime(2026, 1, 1, 10, 0)},
        {"hour_utc": datetime(2026, 3, 1, 10, 0)},
        {"hour_utc": datetime(2026, 5, 1, 10, 0)},
    ]
    train, test = train_test_split_by_date(rows, test_start=date(2026, 4, 1))

    assert train == [rows[0], rows[1]]
    assert test == [rows[2]]


def test_train_test_split_by_date_boundary_day_goes_to_test():
    rows = [{"hour_utc": datetime(2026, 4, 1, 0, 0)}]
    train, test = train_test_split_by_date(rows, test_start=date(2026, 4, 1))

    assert train == []
    assert test == rows
