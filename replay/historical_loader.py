"""
Loads the real Olist CSVs and prepares the lookup structures the replay
engine needs:

  - orders/items/payments/reviews restricted to the "stable" date window
    (see config.HISTORICAL_STABLE_START/END — excludes the 2016 ramp-up and
    2018 tail-cut months identified in the Phase 0 EDA).
  - a bucket index: (month, day_of_week) -> list of historical calendar
    dates that fall in that bucket. This is what the replay engine samples
    from to find a "template day" for any target date.

This module only reads local CSVs — it never talks to BigQuery. Historical
data is loaded to bronze separately and unmodified.
"""

import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402


class HistoricalReference:
    def __init__(self):
        self.orders = None
        self.items = None
        self.payments = None
        self.reviews = None
        self.customers = None
        self.bucket_index = {}
        self._loaded = False

    def load(self):
        if self._loaded:
            return

        orders = self._read("olist_orders_dataset")
        items = self._read("olist_order_items_dataset")
        payments = self._read("olist_order_payments_dataset")
        reviews = self._read("olist_order_reviews_dataset")
        customers = self._read("olist_customers_dataset")

        orders = orders[orders["order_purchase_timestamp"].notna()].copy()
        orders["purchase_date"] = orders["order_purchase_timestamp"].dt.date

        stable_mask = (orders["purchase_date"] >= config.HISTORICAL_STABLE_START) & (
            orders["purchase_date"] <= config.HISTORICAL_STABLE_END
        )
        orders = orders[stable_mask].copy()

        valid_order_ids = set(orders["order_id"])
        items = items[items["order_id"].isin(valid_order_ids)].copy()
        payments = payments[payments["order_id"].isin(valid_order_ids)].copy()
        reviews = reviews[reviews["order_id"].isin(valid_order_ids)].copy()

        self.orders = orders
        self.items = items
        self.payments = payments
        self.reviews = reviews
        self.customers = customers

        self.bucket_index = self._build_bucket_index(orders)
        self._loaded = True

        n_days = orders["purchase_date"].nunique()
        print(
            f"[historical_loader] Loaded {len(orders):,} stable-window orders "
            f"across {n_days} distinct calendar days "
            f"({config.HISTORICAL_STABLE_START} .. {config.HISTORICAL_STABLE_END})."
        )

    def _read(self, table_name: str) -> pd.DataFrame:
        path = config.DATA_DIR / config.RAW_FILES[table_name]
        if not path.exists():
            raise FileNotFoundError(
                f"{path} not found. Copy the 9 Olist CSVs into data/ "
                f"(same files used in Phase 0)."
            )
        parse_cols = config.TIMESTAMP_COLUMNS.get(table_name)
        return pd.read_csv(path, low_memory=False, parse_dates=parse_cols)

    @staticmethod
    def _build_bucket_index(orders: pd.DataFrame) -> dict:
        index = defaultdict(set)
        unique_days = orders[["purchase_date"]].drop_duplicates()
        for d in unique_days["purchase_date"]:
            key = (d.month, d.weekday())  # weekday(): Mon=0 .. Sun=6
            index[key].add(d)
        return {k: sorted(v) for k, v in index.items()}

    def get_template_dates(self, month: int, weekday: int) -> list:
        """Historical dates matching (month, weekday). Falls back to
        same-weekday-any-month if the exact bucket is empty (shouldn't
        normally happen with a full year of stable data)."""
        exact = self.bucket_index.get((month, weekday))
        if exact:
            return exact
        fallback = [
            d for (m, w), dates in self.bucket_index.items() if w == weekday for d in dates
        ]
        return sorted(fallback)

    def orders_on(self, template_date) -> pd.DataFrame:
        return self.orders[self.orders["purchase_date"] == template_date]
