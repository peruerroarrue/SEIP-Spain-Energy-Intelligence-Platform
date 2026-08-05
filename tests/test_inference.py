from datetime import datetime

from seip.ml.inference import build_feature_row, build_forecast_row
from seip.ml.train import compute_target_calendar_features


def test_build_feature_row_uses_lags_from_latest_row_and_target_calendar():
    latest_row = {
        "hour_utc": datetime(2026, 7, 27, 10, 0, 0),
        "pvpc_lag_1h": 100.0,
        "pvpc_lag_24h": 110.0,
        "pvpc_lag_168h": 120.0,
        "pvpc_eur_mwh": 130.0,  # not a feature, must be ignored
    }
    row = build_feature_row(latest_row, horizon_hours=3)

    assert row["pvpc_lag_1h"] == 100.0
    assert row["pvpc_lag_24h"] == 110.0
    assert row["pvpc_lag_168h"] == 120.0
    assert "pvpc_eur_mwh" not in row
    assert row == {
        "pvpc_lag_1h": 100.0,
        "pvpc_lag_24h": 110.0,
        "pvpc_lag_168h": 120.0,
        **compute_target_calendar_features(latest_row["hour_utc"], 3),
    }


def test_build_feature_row_differs_by_horizon():
    latest_row = {
        "hour_utc": datetime(2026, 7, 27, 10, 0, 0),
        "pvpc_lag_1h": 100.0,
        "pvpc_lag_24h": 110.0,
        "pvpc_lag_168h": 120.0,
    }
    row_h1 = build_feature_row(latest_row, horizon_hours=1)
    row_h2 = build_feature_row(latest_row, horizon_hours=2)

    # Same lags (they describe the origin, not the target)...
    for col in ("pvpc_lag_1h", "pvpc_lag_24h", "pvpc_lag_168h"):
        assert row_h1[col] == row_h2[col]
    # ...but different target-hour calendar features.
    assert row_h1["target_hour_sin"] != row_h2["target_hour_sin"]


def test_build_forecast_row_shape():
    origin = datetime(2026, 7, 27, 10, 0, 0)
    row = build_forecast_row(origin, horizon_hours=5, predicted_value=123.45)

    assert row == {
        "origin_hour_utc": "2026-07-27T10:00:00",
        "target_hour_utc": "2026-07-27T15:00:00",
        "horizon_hours": 5,
        "predicted_pvpc_eur_mwh": 123.45,
    }
