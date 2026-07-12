"""
Quick EDA #2 — value distributions, category patterns, and the repeat-purchase
claim from the project plan.

Reads locally: orders, order_items, order_payments, order_reviews, products,
customers, product_category_name_translation. No BigQuery needed.

Produces:
  - outputs/eda_price_freight_overall.png
  - outputs/eda_price_by_category_top15.png
  - outputs/eda_order_status.png
  - outputs/eda_payment_type.png
  - outputs/eda_review_score.png
  - outputs/eda_delivery_delay.png
  - outputs/distributions_summary.json
"""

import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402


def load_csv(table_name: str, **kwargs) -> pd.DataFrame:
    path = config.DATA_DIR / config.RAW_FILES[table_name]
    if not path.exists():
        print(f"[ERROR] {path} not found. See README.md for setup steps.")
        sys.exit(1)
    parse_cols = config.TIMESTAMP_COLUMNS.get(table_name)
    if parse_cols:
        kwargs.setdefault("parse_dates", parse_cols)
    return pd.read_csv(path, low_memory=False, **kwargs)


def quantiles(series: pd.Series) -> dict:
    q = series.quantile([0.25, 0.5, 0.75, 0.95]).round(2)
    return {"p25": q[0.25], "p50_median": q[0.5], "p75": q[0.75], "p95": q[0.95],
            "mean": round(series.mean(), 2), "std": round(series.std(), 2)}


def price_freight_section(items: pd.DataFrame, products: pd.DataFrame,
                           translation: pd.DataFrame) -> dict:
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].hist(items["price"].clip(upper=items["price"].quantile(0.99)), bins=60, color="#4C72B0")
    axes[0].set_title("Price distribution (clipped at p99)")
    axes[0].set_xlabel("price (BRL)")

    axes[1].hist(items["freight_value"].clip(upper=items["freight_value"].quantile(0.99)),
                 bins=60, color="#DD8452")
    axes[1].set_title("Freight value distribution (clipped at p99)")
    axes[1].set_xlabel("freight_value (BRL)")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_price_freight_overall.png", dpi=120)
    plt.close(fig)

    # price by category (top 15 by volume), translated to English names
    merged = items.merge(products[["product_id", "product_category_name"]], on="product_id", how="left")
    merged = merged.merge(translation, on="product_category_name", how="left")
    merged["category"] = merged["product_category_name_english"].fillna(
        merged["product_category_name"]
    ).fillna("unknown")

    top_categories = merged["category"].value_counts().head(15).index.tolist()
    subset = merged[merged["category"].isin(top_categories)]

    fig, ax = plt.subplots(figsize=(12, 6))
    order = subset.groupby("category")["price"].median().sort_values(ascending=False).index
    subset.boxplot(column="price", by="category", ax=ax, rot=75,
                    showfliers=False)
    plt.suptitle("")
    ax.set_title("Price by product category (top 15 by order volume, outliers hidden)")
    ax.set_xlabel("")
    ax.set_ylabel("price (BRL)")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_price_by_category_top15.png", dpi=120)
    plt.close(fig)

    category_stats = (
        merged.groupby("category")["price"]
        .agg(["count", "median", "mean"])
        .sort_values("count", ascending=False)
        .head(15)
        .round(2)
    )

    return {
        "price_quantiles_brl": quantiles(items["price"]),
        "freight_quantiles_brl": quantiles(items["freight_value"]),
        "top_15_categories_by_volume": category_stats.reset_index().to_dict(orient="records"),
    }


def order_status_section(orders: pd.DataFrame) -> dict:
    counts = orders["order_status"].value_counts()
    proportions = (counts / counts.sum()).round(4)

    fig, ax = plt.subplots(figsize=(8, 5))
    counts.plot(kind="bar", ax=ax, color="#8172B2")
    ax.set_title("order_status counts")
    ax.set_ylabel("orders")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_order_status.png", dpi=120)
    plt.close(fig)

    return {status: float(p) for status, p in proportions.items()}


def payment_type_section(payments: pd.DataFrame) -> dict:
    counts = payments["payment_type"].value_counts()
    proportions = (counts / counts.sum()).round(4)

    fig, ax = plt.subplots(figsize=(8, 5))
    counts.plot(kind="bar", ax=ax, color="#64B5CD")
    ax.set_title("payment_type counts")
    ax.set_ylabel("payments")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_payment_type.png", dpi=120)
    plt.close(fig)

    return {ptype: float(p) for ptype, p in proportions.items()}


def review_score_section(reviews: pd.DataFrame, orders: pd.DataFrame) -> dict:
    fig, ax = plt.subplots(figsize=(8, 5))
    reviews["review_score"].value_counts().sort_index().plot(kind="bar", ax=ax, color="#CCB974")
    ax.set_title("review_score distribution")
    ax.set_xlabel("score (1-5)")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_review_score.png", dpi=120)
    plt.close(fig)

    overall_dist = (
        reviews["review_score"].value_counts(normalize=True).round(4).sort_index()
    )

    # conditional: review_score given the order was delivered late
    orders_delay = orders.copy()
    orders_delay["is_delayed"] = (
        orders_delay["order_delivered_customer_date"] > orders_delay["order_estimated_delivery_date"]
    )
    joined = reviews.merge(orders_delay[["order_id", "is_delayed"]], on="order_id", how="inner")
    cond = (
        joined.groupby("is_delayed")["review_score"]
        .mean()
        .round(2)
        .rename(index={True: "delayed_orders", False: "on_time_orders"})
    )

    return {
        "overall_distribution": {str(int(k)): float(v) for k, v in overall_dist.items()},
        "avg_score_by_delivery_status": cond.to_dict(),
    }


def delivery_delay_section(orders: pd.DataFrame) -> dict:
    delivered = orders[orders["order_status"] == "delivered"].copy()
    delivered = delivered.dropna(
        subset=["order_delivered_customer_date", "order_estimated_delivery_date", "order_purchase_timestamp"]
    )
    delivered["delivery_days"] = (
        delivered["order_delivered_customer_date"] - delivered["order_purchase_timestamp"]
    ).dt.total_seconds() / 86400
    delivered["is_delayed"] = (
        delivered["order_delivered_customer_date"] > delivered["order_estimated_delivery_date"]
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(delivered["delivery_days"].clip(upper=delivered["delivery_days"].quantile(0.99)),
            bins=60, color="#937860")
    ax.set_title("Delivery days distribution (purchase -> delivered), clipped at p99")
    ax.set_xlabel("days")
    fig.tight_layout()
    fig.savefig(config.OUTPUT_DIR / "eda_delivery_delay.png", dpi=120)
    plt.close(fig)

    delay_rate = float(delivered["is_delayed"].mean())

    return {
        "delivered_orders_analyzed": int(len(delivered)),
        "delivery_days_quantiles": quantiles(delivered["delivery_days"]),
        "pct_delivered_late_vs_estimate": round(delay_rate, 4),
    }


def repeat_purchase_section(orders: pd.DataFrame, customers: pd.DataFrame) -> dict:
    """Re-verify the plan.md claim using customer_unique_id (per-person), not
    customer_id (per-order)."""
    joined = orders.merge(customers[["customer_id", "customer_unique_id"]], on="customer_id", how="left")
    order_counts = joined.groupby("customer_unique_id")["order_id"].nunique()

    total_unique_customers = int(order_counts.shape[0])
    one_time = int((order_counts == 1).sum())
    repeat = int((order_counts > 1).sum())
    pct_one_time = round(one_time / total_unique_customers, 4)

    return {
        "total_unique_customers": total_unique_customers,
        "one_time_customers": one_time,
        "repeat_customers": repeat,
        "pct_one_time_customers": pct_one_time,
        "plan_md_claim_96_88pct": (
            "MATCHES within rounding" if abs(pct_one_time - 0.9688) < 0.01 else
            "DOES NOT MATCH — re-check plan.md assumption before designing model #2/#3"
        ),
    }


def main():
    print("Loading source tables ...")
    orders = load_csv("olist_orders_dataset")
    items = load_csv("olist_order_items_dataset")
    payments = load_csv("olist_order_payments_dataset")
    reviews = load_csv("olist_order_reviews_dataset")
    products = load_csv("olist_products_dataset")
    customers = load_csv("olist_customers_dataset")
    translation = load_csv("product_category_name_translation")
    print("  all tables loaded.")

    summary = {}

    print("Analyzing price/freight and category patterns ...")
    summary["price_freight"] = price_freight_section(items, products, translation)

    print("Analyzing order_status ...")
    summary["order_status_proportions"] = order_status_section(orders)

    print("Analyzing payment_type ...")
    summary["payment_type_proportions"] = payment_type_section(payments)

    print("Analyzing review_score ...")
    summary["review_score"] = review_score_section(reviews, orders)

    print("Analyzing delivery delay ...")
    summary["delivery_delay"] = delivery_delay_section(orders)

    print("Re-verifying repeat-purchase claim (using customer_unique_id) ...")
    summary["repeat_purchase_check"] = repeat_purchase_section(orders, customers)

    with open(config.OUTPUT_DIR / "distributions_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print("\nRepeat-purchase check:")
    print(json.dumps(summary["repeat_purchase_check"], indent=2))
    print(f"\nAll charts + summary saved to {config.OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
