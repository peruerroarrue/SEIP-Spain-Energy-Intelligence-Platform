"""Manual smoke test: train all 24 horizon models and report RMSE/MAE vs the naive baseline.

Not part of the pytest suite (needs the `ml` extra + a populated
data/gold/ml_features table with enough history for the lags to be
non-null — run the historical backfill + full pipeline first).

Run: python scripts/smoke_train.py [test_months]
"""

from __future__ import annotations

import sys
from datetime import date, timedelta

from seip.ingestion.spark_session import build_local_spark_session
from seip.ml.train import run

FEATURES_PATH = "data/gold/ml_features"


def main() -> None:
    test_months = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    test_start = date.today() - timedelta(days=30 * test_months)

    spark = build_local_spark_session("seip-train-smoke")
    spark.sparkContext.setLogLevel("ERROR")

    print(f"-- training all horizons, test set starting {test_start} --")
    results = run(spark, FEATURES_PATH, test_start_date=test_start)

    print(f"\n{'horizon':>8} {'train':>7} {'test':>6} {'rmse':>8} {'base_rmse':>10} {'mae':>8} {'base_mae':>9}  beats?")
    beats_count = 0
    for r in sorted(results, key=lambda x: x["horizon_hours"]):
        beats_count += r["beats_baseline"]
        print(
            f"h+{r['horizon_hours']:>5} {r['train_rows']:>7} {r['test_rows']:>6} "
            f"{r['rmse']:>8.2f} {r['baseline_rmse']:>10.2f} {r['mae']:>8.2f} {r['baseline_mae']:>9.2f}  "
            f"{'YES' if r['beats_baseline'] else 'no'}"
        )
    print(f"\n{beats_count}/{len(results)} horizons beat the naive baseline on RMSE")

    print("\n-- Model Registry --")
    for r in sorted(results, key=lambda x: x["horizon_hours"]):
        print(f"{r['registered_model_name']} v{r['registered_version']} @ alias 'reference'")


if __name__ == "__main__":
    main()
