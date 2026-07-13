# Phase 0 — Foundation (Olist E-commerce Data Platform)

This is Phase 0 of the project: one-time raw data load into BigQuery (`olist_raw`)
and a quick EDA pass used to design the Phase 1 replay/synthetic-data script.

No transformation happens here. Everything is 1:1 with the source CSVs.

---

## 1. Prerequisites

1. Download the dataset from Kaggle:
   https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Unzip all 9 CSV files into the `data/` folder. Expected filenames:
   ```
   data/olist_customers_dataset.csv
   data/olist_geolocation_dataset.csv
   data/olist_order_items_dataset.csv
   data/olist_order_payments_dataset.csv
   data/olist_order_reviews_dataset.csv
   data/olist_orders_dataset.csv
   data/olist_products_dataset.csv
   data/olist_sellers_dataset.csv
   data/product_category_name_translation.csv
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. (Only needed for the BigQuery load script) Set up a GCP service account with
   BigQuery Data Editor + Job User roles, download its JSON key, then set:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="service-account.json"
   export GCP_PROJECT_ID="your-gcp-project-id"
   export BQ_RAW_DATASET="olist_raw"
   export BQ_LOCATION="US"   # or your region, e.g. "asia-southeast1"
   ```
   The dataset (`olist_raw`) will be created automatically if it doesn't exist.

---

## 2. Scripts

Run in this order:

### `scripts/00_load_raw_to_bigquery.py`
One-time, immutable load of all 9 CSVs into `olist_raw`. Table names mirror
the CSV filenames (e.g. `olist_orders_dataset`). By default the script
**refuses to overwrite** a table that already has rows — this is intentional,
since `olist_raw` is meant to be immutable and only loaded once. Pass `--force`
if you really need to reload (e.g. during initial development).

```bash
python scripts/00_load_raw_to_bigquery.py
python scripts/00_load_raw_to_bigquery.py --force   # only if you need to reload
python scripts/00_load_raw_to_bigquery.py --dry-run # validate CSVs locally, skip BigQuery
```

### `scripts/01_eda_seasonality.py`
Reads `olist_orders_dataset.csv` locally (no BigQuery needed) and produces
time-based EDA: daily order volume over the full history, day-of-week
distribution, hour-of-day distribution, monthly trend, and an explicit
flag of the Black Friday 2017 spike. Saves PNG charts + a JSON summary to
`outputs/`.

```bash
python scripts/01_eda_seasonality.py
```

### `scripts/02_eda_distributions.py`
Reads orders + items + payments + reviews + products + customers locally.
Produces: price/freight distribution overall and by product category,
order_status proportions, payment_type proportions, review_score
distribution, delivery delay distribution, and a re-verification of the
"96.88% one-time customers" claim from the project plan (using
`customer_unique_id`, not `customer_id` — these are different in Olist).
Saves PNG charts + a JSON summary to `outputs/`.

```bash
python scripts/02_eda_distributions.py
```

---

## 3. Why this matters for Phase 1

The EDA outputs (`outputs/seasonality_summary.json`,
`outputs/distributions_summary.json`) are meant to be read before writing the
replay/synthetic-data script. In particular:

- Day-of-week and hour-of-day distributions → used to build the seasonal
  index mapping (synthetic date → equivalent historical date).
- Monthly trend / Black Friday spike → decide whether to preserve or flatten
  that spike in synthetic 2024+ data.
- Price/freight quantiles by category → used to size the perturbation noise
  (e.g. ±3% Gaussian) without pushing values outside realistic bounds.
- order_status and review_score conditional distributions → used so
  synthetic orders keep the same status/review logic instead of being
  randomly assigned.

## 4. Notes / things to verify on the real data

- `customer_id` (per order) is **not** the same as `customer_unique_id`
  (per person). Repeat-purchase analysis must use `customer_unique_id`.
- `order_approved_at`, `order_delivered_carrier_date`, and
  `order_delivered_customer_date` can be null (canceled/unavailable orders).
  The scripts handle nulls explicitly rather than dropping rows silently.
- `order_status` has multiple values beyond `delivered` — don't assume every
  row can produce a delivery-delay label.
