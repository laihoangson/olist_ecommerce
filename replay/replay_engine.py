"""
Core replay logic.

Design (see chat discussion / plan.md section 3 for the reasoning):

  1. Seasonal index mapping: a target date is mapped to a "bucket" of
     (month, day_of_week). A real historical date sharing that bucket is
     sampled uniformly as the *template day*.
  2. Whole-day resampling: ALL orders/items/payments/reviews from that
     template day are taken together, preserving joint structure (price vs
     freight vs category, order_status vs review_score, etc.) instead of
     sampling each column independently.
  3. Because we resample a real historical day wholesale, seasonal effects
     (e.g. the Nov 2017 Black Friday spike) are reproduced automatically —
     no separate spike-injection logic is needed.
  4. Light perturbation is applied so rows aren't byte-identical to their
     historical source: Gaussian relative noise on price/freight/payment
     value, and a small random time jitter that preserves event ordering
     (purchase < approved < carrier < delivered).
  5. Determinism: the RNG is seeded from the target date/window, so re-running
     the same batch (e.g. a retried GitHub Action) produces identical rows —
     required for the MERGE upsert in bq_writer to be idempotent.
"""

import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import config


def _seed_for(target_date, window_start_hour) -> int:
    base = int(target_date.strftime("%Y%m%d"))
    if window_start_hour is not None:
        base = base * 100 + window_start_hour
    return base


def _new_synthetic_customer(rng, customers_df: pd.DataFrame, source_customer_id: str) -> dict:
    """Create a brand-new synthetic customer, copying the zip/city/state
    profile of a real historical customer so geographic distribution is
    preserved, but with fresh synthetic IDs."""
    profile = customers_df.loc[customers_df["customer_id"] == source_customer_id]
    if profile.empty:
        zip_prefix, city, state = 0, "unknown", "NA"
    else:
        row = profile.iloc[0]
        zip_prefix = row["customer_zip_code_prefix"]
        city = row["customer_city"]
        state = row["customer_state"]

    return {
        "customer_id": f"synth-{uuid.uuid4()}",
        "customer_unique_id": f"synth-{uuid.uuid4()}",
        "customer_zip_code_prefix": zip_prefix,
        "customer_city": city,
        "customer_state": state,
        "is_synthetic": True,
    }


def _perturb_amount(rng, value: float, std: float, floor: float = 0.5) -> float:
    if pd.isna(value):
        return value
    noisy = value * (1 + rng.normal(0, std))
    return round(max(noisy, floor), 2)


def _growth_factor(target_date) -> float:
    if config.GROWTH_RATE_PER_YEAR == 1.0:
        return 1.0
    years_elapsed = (target_date - config.BACKFILL_START_DATE).days / 365.25
    return config.GROWTH_RATE_PER_YEAR ** years_elapsed


def generate_batch(
    historical_ref,
    target_date,
    window_start_hour: int = None,
    window_end_hour: int = None,
    synthetic_customer_pool: list = None,
    batch_id: str = None,
) -> dict:
    """
    Generate one synthetic batch for `target_date`.

    If window_start_hour/window_end_hour are given (e.g. 0 and 6), only
    orders whose historical purchase hour falls in [start, end) are
    replayed — used by the live 6h cron. Leave both None to replay the
    full day — used by the one-time backfill.

    synthetic_customer_pool: list of previously-created synthetic
    (customer_id, customer_unique_id, zip, city, state) tuples, so repeat
    customers can be sampled consistently across batches. Pass the running
    pool in from the caller and use the returned 'new_customers' to extend
    it for the next call.

    Returns a dict of DataFrames: customers_new, orders, items, payments,
    reviews — all tagged is_synthetic=True and ready to upsert into bronze.
    """
    seed = _seed_for(target_date, window_start_hour)
    rng = np.random.default_rng(seed)

    template_dates = historical_ref.get_template_dates(target_date.month, target_date.weekday())
    template_date = template_dates[rng.integers(0, len(template_dates))]

    day_orders = historical_ref.orders_on(template_date)

    if window_start_hour is not None:
        hours = day_orders["order_purchase_timestamp"].dt.hour
        mask = (hours >= window_start_hour) & (hours < window_end_hour)
        day_orders = day_orders[mask]

    factor = _growth_factor(target_date)
    target_n = max(1, round(len(day_orders) * factor)) if len(day_orders) else 0
    if target_n and target_n != len(day_orders):
        day_orders = day_orders.sample(n=target_n, replace=target_n > len(day_orders),
                                        random_state=seed)

    new_customers = []
    pool = list(synthetic_customer_pool or [])

    out_orders, out_items, out_payments, out_reviews = [], [], [], []

    for _, order in day_orders.iterrows():
        new_order_id = f"synth-{uuid.uuid4()}"

        use_repeat = pool and rng.random() < config.SYNTHETIC_REPEAT_CUSTOMER_RATE
        if use_repeat:
            customer = pool[rng.integers(0, len(pool))]
        else:
            customer = _new_synthetic_customer(rng, historical_ref.customers, order["customer_id"])
            new_customers.append(customer)
            pool.append(customer)

        # Preserve the historical deltas between lifecycle timestamps,
        # relocate the base purchase time to target_date + jitter.
        hist_purchase = order["order_purchase_timestamp"]
        time_of_day = hist_purchase.time()
        jitter_minutes = int(rng.integers(-45, 45))
        new_purchase = datetime.combine(target_date, time_of_day) + timedelta(minutes=jitter_minutes)

        def _shift(hist_ts):
            if pd.isna(hist_ts):
                return pd.NaT
            delta = hist_ts - hist_purchase
            return new_purchase + delta

        out_orders.append({
            "order_id": new_order_id,
            "customer_id": customer["customer_id"],
            "order_status": order["order_status"],
            "order_purchase_timestamp": new_purchase,
            "order_approved_at": _shift(order["order_approved_at"]),
            "order_delivered_carrier_date": _shift(order["order_delivered_carrier_date"]),
            "order_delivered_customer_date": _shift(order["order_delivered_customer_date"]),
            "order_estimated_delivery_date": _shift(order["order_estimated_delivery_date"]),
            "is_synthetic": True,
            "source_template_date": str(template_date),
            "batch_id": batch_id,
        })

        items = historical_ref.items[historical_ref.items["order_id"] == order["order_id"]]
        for _, item in items.iterrows():
            out_items.append({
                "order_id": new_order_id,
                "order_item_id": item["order_item_id"],
                "product_id": item["product_id"],
                "seller_id": item["seller_id"],
                "shipping_limit_date": _shift(item["shipping_limit_date"]),
                "price": _perturb_amount(rng, item["price"], config.PERTURBATION_STD),
                "freight_value": _perturb_amount(rng, item["freight_value"], config.PERTURBATION_STD),
                "is_synthetic": True,
                "batch_id": batch_id,
            })

        payments = historical_ref.payments[historical_ref.payments["order_id"] == order["order_id"]]
        for _, pay in payments.iterrows():
            out_payments.append({
                "order_id": new_order_id,
                "payment_sequential": pay["payment_sequential"],
                "payment_type": pay["payment_type"],
                "payment_installments": pay["payment_installments"],
                "payment_value": _perturb_amount(rng, pay["payment_value"], config.PERTURBATION_STD),
                "is_synthetic": True,
                "batch_id": batch_id,
            })

        reviews = historical_ref.reviews[historical_ref.reviews["order_id"] == order["order_id"]]
        for _, rev in reviews.iterrows():
            out_reviews.append({
                "review_id": f"synth-{uuid.uuid4()}",
                "order_id": new_order_id,
                "review_score": rev["review_score"],
                "review_comment_title": None,
                "review_comment_message": None,
                "review_creation_date": _shift(rev["review_creation_date"]),
                "review_answer_timestamp": _shift(rev["review_answer_timestamp"]),
                "is_synthetic": True,
                "batch_id": batch_id,
            })

    return {
        "new_customers": pd.DataFrame(new_customers),
        "orders": pd.DataFrame(out_orders),
        "items": pd.DataFrame(out_items),
        "payments": pd.DataFrame(out_payments),
        "reviews": pd.DataFrame(out_reviews),
        "template_date_used": template_date,
        "updated_customer_pool": pool,
    }
