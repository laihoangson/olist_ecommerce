"""
Shared configuration for Phase 1. Everything sensitive/environment-specific
comes from `.env` (see .env.example) — nothing is hardcoded here.
"""

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"

# ---------------------------------------------------------------------------
# GCP / BigQuery
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
BQ_RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "olist_raw")
BQ_BRONZE_DATASET = os.environ.get("BQ_BRONZE_DATASET", "olist_bronze")

# GOOGLE_APPLICATION_CREDENTIALS is read directly by the google-cloud-bigquery
# library from the environment, we don't need to touch it here — just make
# sure it's set before instantiating a client. We verify at runtime instead
# of importing bigquery at module load time (keeps --dry-run usable without
# google-cloud-bigquery installed/configured).

# ---------------------------------------------------------------------------
# Historical reference window (derived from the real Phase 0 EDA output).
# 2016-09..2016-12 (ramp-up) and 2018-09..2018-10 (tail-cut) are excluded
# because their volume is near-zero and not representative of a normal
# seasonal cycle — see seasonality_summary.json monthly_order_counts.
# ---------------------------------------------------------------------------
HISTORICAL_STABLE_START = date.fromisoformat(
    os.environ.get("HISTORICAL_STABLE_START", "2017-01-01")
)
HISTORICAL_STABLE_END = date.fromisoformat(
    os.environ.get("HISTORICAL_STABLE_END", "2018-08-31")
)

# ---------------------------------------------------------------------------
# Backfill (Phase A)
# ---------------------------------------------------------------------------
BACKFILL_START_DATE = date.fromisoformat(os.environ.get("BACKFILL_START_DATE", "2024-01-01"))
_backfill_end_raw = os.environ.get("BACKFILL_END_DATE", "").strip()
BACKFILL_END_DATE = date.fromisoformat(_backfill_end_raw) if _backfill_end_raw else date.today()

# ---------------------------------------------------------------------------
# Replay / perturbation knobs
# ---------------------------------------------------------------------------
SYNTHETIC_REPEAT_CUSTOMER_RATE = float(os.environ.get("SYNTHETIC_REPEAT_CUSTOMER_RATE", "0.0312"))
GROWTH_RATE_PER_YEAR = float(os.environ.get("GROWTH_RATE_PER_YEAR", "1.0"))
PERTURBATION_STD = float(os.environ.get("PERTURBATION_STD", "0.03"))

# Live batch window size in hours (Phase B cron cadence). Matches the "every
# 6h" schedule in the project plan.
LIVE_BATCH_HOURS = int(os.environ.get("LIVE_BATCH_HOURS", "6"))

# ---------------------------------------------------------------------------
# Source CSV filenames (same convention as Phase 0)
# ---------------------------------------------------------------------------
RAW_FILES = {
    "olist_customers_dataset": "olist_customers_dataset.csv",
    "olist_order_items_dataset": "olist_order_items_dataset.csv",
    "olist_order_payments_dataset": "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset": "olist_order_reviews_dataset.csv",
    "olist_orders_dataset": "olist_orders_dataset.csv",
    "olist_products_dataset": "olist_products_dataset.csv",
    "olist_sellers_dataset": "olist_sellers_dataset.csv",
    "olist_geolocation_dataset": "olist_geolocation_dataset.csv",
    # Added for Phase 2: dim_products needs the English category names, and
    # this table was missing from Phase 1's bronze load entirely.
    "product_category_name_translation": "product_category_name_translation.csv",
}

# ---------------------------------------------------------------------------
# Phase 2 (dbt): silver/gold dataset names. Bronze stays BQ_BRONZE_DATASET
# (already defined above); dbt writes staging models into BQ_SILVER_DATASET
# and fact/dim/mart models into BQ_GOLD_DATASET.
# ---------------------------------------------------------------------------
BQ_SILVER_DATASET = os.environ.get("BQ_SILVER_DATASET", "olist_silver")
BQ_GOLD_DATASET = os.environ.get("BQ_GOLD_DATASET", "olist_gold")

TIMESTAMP_COLUMNS = {
    "olist_orders_dataset": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "olist_order_reviews_dataset": [
        "review_creation_date",
        "review_answer_timestamp",
    ],
    "olist_order_items_dataset": [
        "shipping_limit_date",
    ],
}


def require_gcp_project():
    if not GCP_PROJECT_ID:
        raise RuntimeError(
            "GCP_PROJECT_ID is not set. Copy .env.example to .env and fill it in."
        )
