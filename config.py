"""
Shared configuration for Phase 0 scripts.

Local paths are fixed relative to the project root. BigQuery settings are
read from environment variables so credentials never live in code.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Local paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Expected raw CSV filenames -> logical dataset name (also used as the
# BigQuery table name so lineage from source file to table is obvious).
RAW_FILES = {
    "olist_customers_dataset": "olist_customers_dataset.csv",
    "olist_geolocation_dataset": "olist_geolocation_dataset.csv",
    "olist_order_items_dataset": "olist_order_items_dataset.csv",
    "olist_order_payments_dataset": "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset": "olist_order_reviews_dataset.csv",
    "olist_orders_dataset": "olist_orders_dataset.csv",
    "olist_products_dataset": "olist_products_dataset.csv",
    "olist_sellers_dataset": "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}

# Columns that should be parsed as timestamps per file, used both by the
# BigQuery loader (for schema hints) and by the EDA scripts.
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

# ---------------------------------------------------------------------------
# BigQuery settings (only needed for scripts/01_load_raw_to_bigquery.py)
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BQ_RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "olist_raw")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
