"""
Quick EDA #1 — time-based seasonality of the Olist orders.

Reads `data/olist_orders_dataset.csv` only (no BigQuery needed). Produces:
  - outputs/eda_daily_order_volume.png
  - outputs/eda_day_of_week.png
  - outputs/eda_hour_of_day.png
  - outputs/eda_monthly_trend.png
  - outputs/seasonality_summary.json

These outputs are meant to inform the Phase 1 replay script's seasonal
index mapping (synthetic date -> equivalent historical date).
"""

import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # headless, no display needed
import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def load_orders() -> pd.DataFrame:
    path = config.DATA_DIR / config.RAW_FILES["olist_orders_dataset"]
    if not path.exists():
        print(f"[ERROR] {path} not found. See README.md for setup steps.")
        sys.exit(1)

    parse_cols = config.TIMESTAMP_COLUMNS["olist_orders_dataset"]
    df = pd.read_csv(path, parse_dates=parse_cols, low_memory=False)
    return df


def plot_daily_volume(df: pd.DataFrame) -> pd.Series:
    daily = df.set_index("order_purchase_timestamp").resample("D").size()
    daily = daily[daily.index.notna()]

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(daily.index, daily.values, linewidth=1)
    ax.set_title("Daily order volume — full history")
    ax.set_xlabel("Date")
    ax.set_ylabel("Orders")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_daily_order_volume.png", dpi=120)
    plt.close(fig)
    return daily


def plot_day_of_week(df: pd.DataFrame) -> dict:
    dow_counts = df["order_purchase_timestamp"].dt.dayofweek.value_counts().sort_index()
    dow_counts.index = [DOW_LABELS[i] for i in dow_counts.index]

    fig, ax = plt.subplots(figsize=(8, 5))
    dow_counts.reindex(DOW_LABELS).plot(kind="bar", ax=ax, color="#4C72B0")
    ax.set_title("Order volume by day of week")
    ax.set_xlabel("Day of week")
    ax.set_ylabel("Orders")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_day_of_week.png", dpi=120)
    plt.close(fig)

    total = dow_counts.sum()
    return {day: round(count / total, 4) for day, count in dow_counts.items()}


def plot_hour_of_day(df: pd.DataFrame) -> dict:
    hour_counts = df["order_purchase_timestamp"].dt.hour.value_counts().sort_index()

    fig, ax = plt.subplots(figsize=(10, 5))
    hour_counts.plot(kind="bar", ax=ax, color="#55A868")
    ax.set_title("Order volume by hour of day")
    ax.set_xlabel("Hour (0-23)")
    ax.set_ylabel("Orders")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_hour_of_day.png", dpi=120)
    plt.close(fig)

    total = hour_counts.sum()
    return {int(h): round(c / total, 4) for h, c in hour_counts.items()}


def plot_monthly_trend(df: pd.DataFrame) -> dict:
    monthly = df.set_index("order_purchase_timestamp").resample("MS").size()

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.bar(monthly.index, monthly.values, width=20, color="#C44E52")
    ax.set_title("Monthly order volume (look for Black Friday / seasonal spikes)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Orders")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_monthly_trend.png", dpi=120)
    plt.close(fig)

    return {str(month.date()): int(count) for month, count in monthly.items()}


def detect_spike_months(monthly_counts: dict, z_threshold: float = 1.5) -> list:
    """Flag months whose volume is an outlier vs the mean (e.g. Black Friday)."""
    import statistics

    values = list(monthly_counts.values())
    if len(values) < 2:
        return []
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values) or 1
    spikes = [
        month
        for month, count in monthly_counts.items()
        if (count - mean) / stdev >= z_threshold
    ]
    return spikes


def main():
    print("Loading olist_orders_dataset.csv ...")
    df = load_orders()
    print(f"  {len(df):,} orders loaded.")

    missing_ts = df["order_purchase_timestamp"].isna().sum()
    if missing_ts:
        print(f"  [WARN] {missing_ts} rows have null order_purchase_timestamp; excluded from time plots.")
    df = df[df["order_purchase_timestamp"].notna()]

    print("Plotting daily order volume ...")
    daily = plot_daily_volume(df)

    print("Plotting day-of-week distribution ...")
    dow_dist = plot_day_of_week(df)

    print("Plotting hour-of-day distribution ...")
    hour_dist = plot_hour_of_day(df)

    print("Plotting monthly trend ...")
    monthly_counts = plot_monthly_trend(df)

    spike_months = detect_spike_months(monthly_counts)

    summary = {
        "date_range": {
            "start": str(df["order_purchase_timestamp"].min().date()),
            "end": str(df["order_purchase_timestamp"].max().date()),
        },
        "total_orders": int(len(df)),
        "avg_orders_per_day": round(float(daily.mean()), 2),
        "day_of_week_distribution": dow_dist,
        "hour_of_day_distribution": hour_dist,
        "monthly_order_counts": monthly_counts,
        "flagged_spike_months": spike_months,
        "notes": (
            "flagged_spike_months are months whose order volume is >= 1.5 std "
            "dev above the mean monthly volume. Expect to see a spike around "
            "Nov 2017 (Black Friday) if the dataset matches the known Olist "
            "pattern. Use day_of_week_distribution and hour_of_day_distribution "
            "as sampling weights for the Phase 1 seasonal index mapping."
        ),
    }

    with open(config.OUTPUT_DIR / "seasonality_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("\nSummary:")
    print(json.dumps(summary, indent=2)[:2000])
    print(f"\nAll charts + summary saved to {config.OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
