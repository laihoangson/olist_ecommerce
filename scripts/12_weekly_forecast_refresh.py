"""
Re-fits the Model 1 (Prophet) revenue forecast and writes the next 90 days
to `{PREDICTIONS_DATASET}.mart_revenue_forecast`. Run weekly or on-demand
-- NOT part of the 6h live cadence (see ops_dashboard_plan.md). The
dashboard/API overlays this forecast against the ACTUAL daily revenue
already in `{GOLD}.mart_daily_revenue` (is_synthetic=TRUE) -- these are two
separate tables joined at the API/frontend layer, not merged here.

IMPORTANT design note: unlike model/train_and_export.py (which trains on
`is_synthetic = FALSE` only, matching model_v4_improved.ipynb), this
script fits Prophet on the FULL continuous timeline -- historical
(is_synthetic=FALSE, ...-2018-08-31) PLUS live (is_synthetic=TRUE,
2024+) -- because a live "today" dashboard needs a forecast that actually
reaches into the live period. Note this also means there's a large gap in
the training timeline between the historical cutoff and whenever live
replay started; Prophet handles gaps in the `ds` column fine (it just
treats missing dates as no observation, not zero), but yearly-seasonality
estimation quality across such a gap hasn't been specifically validated --
watch the CV metrics if you rely on this heavily.

Usage:
    python scripts/12_weekly_forecast_refresh.py
"""

import sys
from pathlib import Path

import pandas as pd
from prophet import Prophet

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from replay import bq_writer, predictions_writer  # noqa: E402
from replay.bq_helpers import q as bq_q  # noqa: E402

HISTORICAL_STABLE_END = "2018-08-31"  # see model_v4_improved.ipynb changelog


def main():
    config.require_gcp_project()
    gold = f"{config.GCP_PROJECT_ID}.{config.BQ_GOLD_DATASET}"
    client = bq_writer.get_client()

    print("Querying full revenue timeline (historical <= cutoff + all live)...")
    daily_rev = bq_q(client, f"""
        SELECT DATE(order_purchase_timestamp) AS ds, SUM(price) AS y
        FROM `{gold}.fct_order_items` i
        JOIN `{gold}.fct_orders` o USING(order_id)
        WHERE (o.is_synthetic = FALSE AND DATE(order_purchase_timestamp) <= '{HISTORICAL_STABLE_END}')
           OR o.is_synthetic = TRUE
        GROUP BY ds ORDER BY ds
    """)
    daily_rev["ds"] = pd.to_datetime(daily_rev["ds"])

    if len(daily_rev) < 30:
        print(f"[WARN] Only {len(daily_rev)} days of data -- Prophet needs more history for a "
              f"meaningful yearly-seasonality fit. Proceeding anyway, but treat the forecast with caution.")

    black_friday = pd.DataFrame({"holiday": "black_friday",
        "ds": pd.to_datetime(["2016-11-25", "2017-11-24", "2018-11-23", "2024-11-29", "2025-11-28"]),
        "lower_window": -3, "upper_window": 1})
    christmas = pd.DataFrame({"holiday": "christmas",
        "ds": pd.to_datetime(["2016-12-25", "2017-12-25", "2018-12-25", "2024-12-25", "2025-12-25"]),
        "lower_window": -14, "upper_window": 0})
    mothers_day = pd.DataFrame({"holiday": "mothers_day",
        "ds": pd.to_datetime(["2017-05-14", "2018-05-13", "2024-05-12", "2025-05-11"]),
        "lower_window": -10, "upper_window": 0})
    carnival = pd.DataFrame({"holiday": "carnival",
        "ds": pd.to_datetime(["2017-02-28", "2018-02-13", "2024-02-13", "2025-03-04"]),
        "lower_window": -1, "upper_window": 2})
    all_holidays = pd.concat([black_friday, christmas, mothers_day, carnival], ignore_index=True)

    print("Fitting Prophet...")
    m = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=False, holidays=all_holidays)
    m.fit(daily_rev)

    future = m.make_future_dataframe(periods=90)
    forecast = m.predict(future)
    out = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(90).copy()
    out["yhat"] = out["yhat"].clip(lower=0)
    out["yhat_lower"] = out["yhat_lower"].clip(lower=0)

    print(f"Forecast window: {out['ds'].min().date()} .. {out['ds'].max().date()}")
    predictions_writer.write_full_rescore(out, config.PREDICTIONS_DATASET, "mart_revenue_forecast")


if __name__ == "__main__":
    main()
