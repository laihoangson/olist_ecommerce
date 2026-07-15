"""
Live silver+gold transform (Python, replaces dbt for the 6h cadence).
FREE-TIER / NO-BILLING SAFE. See live.md for the full design writeup.

WHY THIS EXISTS
----------------
dbt's `table` materialization (CREATE OR REPLACE TABLE AS SELECT) is the
only strategy that works without a billing account (see dbt_project.yml /
phase2.md) — but that means every `dbt run` rebuilds ALL 8 silver + 8 gold
models from scratch, scanning the FULL bronze/silver history every time.
Chaining that into a 6h cron (4x/day) rescans an ever-growing table on every
tick, which is wasteful: each tick only ever adds one small batch.

This script replaces dbt for the 6h cadence for the ROW-LEVEL tables only
(silver_customers/orders/order_items/order_payments/order_reviews, and
fct_orders/fct_order_items/fct_payments/fct_reviews). Each run:
  1. Figures out which live-ingest windows (batch_ids) haven't been
     transformed yet, by re-deriving the same window boundaries
     05_live_ingest.py uses, between its own checkpoint and 05's
     `replay_cursor` state (NOT wall-clock now — see main() for why).
  2. For each window, runs a small set of BigQuery query jobs that mirror
     the corresponding dbt SQL model 1:1 (same SELECT list, same casts, same
     business logic for is_delayed/delivery_days) but with the source
     filtered to just that batch_id, and the result appended (not replaced)
     into the existing silver/gold table via bq_writer.run_transform_query
     (a destination-table query job — not DML, sandbox-safe).
  3. Advances a dedicated `live_transform_cursor` checkpoint in
     pipeline_state once the whole window's steps succeed.

WHAT THIS SCRIPT DOES NOT TOUCH
--------------------------------
- dim_products, dim_sellers, dim_date, dim_geolocation, silver_products,
  silver_sellers, silver_geolocation: products/sellers/geolocation are
  NEVER faked by the replay engine (see phase1.md) — these are built once
  by dbt during backfill and never change afterward.
- dim_customers, mart_customer_rfm, mart_daily_revenue: these are
  cross-history AGGREGATES (recency/frequency/monetary depend on a
  customer's ENTIRE order history, and dim_customers' reference_date is the
  max order_purchase_timestamp across ALL customers, so it can shift for
  every customer whenever ANY new order lands, not just the ones in this
  batch). They cannot be correctly updated by appending — they still need a
  full recompute. Left on dbt, but demoted to a DAILY cadence (see
  .github/workflows/daily_gold_refresh.yml) instead of every 6h, since nothing
  downstream (Power BI, ML training) actually needs RFM/segment freshness
  at 6h granularity. This is what keeps a SINGLE source of truth for that
  aggregation logic (in dbt SQL) instead of duplicating it in Python.

USAGE
-----
    # One-time bootstrap, right after the LAST full `dbt run` you intend to
    # do (e.g. the one chained at the end of backfill.yml). Sets the
    # checkpoint WITHOUT processing anything — dbt already covered
    # everything up to this point.
    python scripts/06_live_transform.py --init-cursor "2026-07-14T09:00:00+00:00"

    # Normal run (called every 6h by .github/workflows/live_ingest.yml,
    # right after 05_live_ingest.py):
    python scripts/06_live_transform.py

    # Validate + see estimated bytes scanned WITHOUT writing anything:
    python scripts/06_live_transform.py --dry-run
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

PIPELINE_NAME = "live_transform_cursor"


# ---------------------------------------------------------------------------
# Window derivation — intentionally duplicated from 05_live_ingest.py's
# build_windows() rather than imported (scripts/ isn't a package and this is
# ~15 lines of pure calendar math, not business logic). If LIVE_BATCH_HOURS
# semantics ever change, update both copies.
# ---------------------------------------------------------------------------
def build_windows(start: datetime, end: datetime, hours: int):
    cursor = start
    while True:
        day = cursor.date()
        block_start_hour = (cursor.hour // hours) * hours
        block_start = datetime.combine(day, datetime.min.time(), tzinfo=cursor.tzinfo) + timedelta(hours=block_start_hour)
        block_end = block_start + timedelta(hours=hours)

        if block_end > end:
            break

        yield day, block_start.hour, block_end.hour if block_end.hour != 0 else 24
        cursor = block_end


def _ds(dataset: str) -> str:
    return f"{config.GCP_PROJECT_ID}.{dataset}"


# ---------------------------------------------------------------------------
# SQL templates. Each mirrors the equivalent dbt model's SELECT list and
# logic EXACTLY (same column list, same casts, same business logic) so that
# rows appended here are indistinguishable from rows a `dbt run` would have
# produced — this matters because dim_customers/mart_customer_rfm (still
# built by dbt, daily) read straight from these same silver tables.
#
# Source is filtered to `batch_id IN UNNEST(@batch_ids)` — bronze tables
# always have batch_id. EVERY one of the 9 silver/gold steps below also
# carries batch_id through to its own output, and — as of this fix —
# dbt/models/silver/silver_customers.sql and
# dbt/models/gold/fct_order_items.sql / fct_payments.sql / fct_reviews.sql
# now select `batch_id` too (they originally didn't, since dbt itself has
# no use for it). This is what lets process_window() check each step's OWN
# destination table for "has this batch_id already landed here?"
# independently, instead of checking one table (silver_orders) as a proxy
# for whether all 9 steps finished.
#
# Getting dbt to select this column too (rather than relying only on
# ALLOW_FIELD_ADDITION to have Python bolt it on after the fact) matters
# because dbt's `table` materialization is CREATE OR REPLACE TABLE AS
# SELECT: a full `dbt run` — e.g. the periodic reconciliation live.md #5
# recommends — recreates these 4 tables from dbt's SELECT list exactly.
# If batch_id weren't in that list, every reconciliation run would silently
# wipe the column, and the next 06 run would see it as missing, treat every
# batch as never-loaded, and duplicate-append. Selecting it from dbt too
# means a full rebuild keeps the column populated (dbt just passes through
# whatever batch_id each source row already carries — backfill-YYYY-MM-DD
# for historical rows, live-YYYY-MM-DD-HH-HH for live ones), so
# reconciliation and incremental Python appends can no longer conflict.
# ---------------------------------------------------------------------------
def silver_steps():
    bronze = _ds(config.BQ_BRONZE_DATASET)
    return [
        dict(
            table="silver_customers",
            sql=f"""
                WITH source AS (
                    SELECT * FROM `{bronze}.bronze_customers`
                    WHERE batch_id IN UNNEST(@batch_ids)
                ),
                deduped AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY customer_id ORDER BY batch_id DESC
                    ) AS rn
                    FROM source
                )
                SELECT
                    customer_id,
                    customer_unique_id,
                    CAST(customer_zip_code_prefix AS STRING) AS customer_zip_code_prefix,
                    TRIM(LOWER(customer_city)) AS customer_city,
                    UPPER(TRIM(customer_state)) AS customer_state,
                    is_synthetic,
                    batch_id
                FROM deduped
                WHERE rn = 1
            """,
        ),
        dict(
            table="silver_orders",
            sql=f"""
                WITH source AS (
                    SELECT * FROM `{bronze}.bronze_orders`
                    WHERE batch_id IN UNNEST(@batch_ids)
                ),
                deduped AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY order_id ORDER BY batch_id DESC
                    ) AS rn
                    FROM source
                )
                SELECT
                    order_id,
                    customer_id,
                    order_status,
                    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
                    CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
                    CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
                    CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
                    CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
                    is_synthetic,
                    batch_id,
                    source_template_date
                FROM deduped
                WHERE rn = 1
            """,
        ),
        dict(
            table="silver_order_items",
            sql=f"""
                WITH source AS (
                    SELECT * FROM `{bronze}.bronze_order_items`
                    WHERE batch_id IN UNNEST(@batch_ids)
                ),
                deduped AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY order_id, order_item_id ORDER BY batch_id DESC
                    ) AS rn
                    FROM source
                )
                SELECT
                    order_id,
                    CAST(order_item_id AS INT64) AS order_item_id,
                    product_id,
                    seller_id,
                    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
                    CAST(price AS NUMERIC) AS price,
                    CAST(freight_value AS NUMERIC) AS freight_value,
                    is_synthetic,
                    batch_id
                FROM deduped
                WHERE rn = 1
            """,
        ),
        dict(
            table="silver_order_payments",
            sql=f"""
                WITH source AS (
                    SELECT * FROM `{bronze}.bronze_order_payments`
                    WHERE batch_id IN UNNEST(@batch_ids)
                ),
                deduped AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY order_id, payment_sequential ORDER BY batch_id DESC
                    ) AS rn
                    FROM source
                )
                SELECT
                    order_id,
                    CAST(payment_sequential AS INT64) AS payment_sequential,
                    payment_type,
                    CAST(payment_installments AS INT64) AS payment_installments,
                    CAST(payment_value AS NUMERIC) AS payment_value,
                    is_synthetic,
                    batch_id
                FROM deduped
                WHERE rn = 1
            """,
        ),
        dict(
            table="silver_order_reviews",
            sql=f"""
                WITH source AS (
                    SELECT * FROM `{bronze}.bronze_order_reviews`
                    WHERE batch_id IN UNNEST(@batch_ids)
                ),
                deduped AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY review_id ORDER BY batch_id DESC
                    ) AS rn
                    FROM source
                )
                SELECT
                    review_id,
                    order_id,
                    CAST(review_score AS INT64) AS review_score,
                    review_comment_title,
                    review_comment_message,
                    CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
                    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp,
                    is_synthetic,
                    batch_id
                FROM deduped
                WHERE rn = 1
            """,
        ),
    ]


def gold_steps():
    silver = _ds(config.BQ_SILVER_DATASET)
    return [
        dict(
            table="fct_orders",
            sql=f"""
                WITH orders AS (
                    SELECT * FROM `{silver}.silver_orders`
                    WHERE batch_id IN UNNEST(@batch_ids)
                ),
                customers AS (
                    -- Small dimension table (one row per synthetic/historical
                    -- customer_id) — read unfiltered, cheap regardless of
                    -- fact-table size.
                    SELECT customer_id, customer_unique_id FROM `{silver}.silver_customers`
                )
                SELECT
                    o.order_id,
                    o.customer_id,
                    c.customer_unique_id,
                    o.order_status,
                    o.order_purchase_timestamp,
                    o.order_approved_at,
                    o.order_delivered_carrier_date,
                    o.order_delivered_customer_date,
                    o.order_estimated_delivery_date,
                    CASE
                        WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
                        THEN TIMESTAMP_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, HOUR) / 24.0
                        ELSE NULL
                    END AS delivery_days,
                    CASE
                        WHEN o.order_status = 'delivered'
                             AND o.order_delivered_customer_date IS NOT NULL
                             AND o.order_estimated_delivery_date IS NOT NULL
                        THEN o.order_delivered_customer_date > o.order_estimated_delivery_date
                        ELSE NULL
                    END AS is_delayed,
                    o.is_synthetic,
                    o.source_template_date,
                    o.batch_id
                FROM orders o
                LEFT JOIN customers c USING (customer_id)
            """,
        ),
        dict(
            table="fct_order_items",
            sql=f"""
                SELECT
                    order_id, order_item_id, product_id, seller_id,
                    shipping_limit_date, price, freight_value, is_synthetic,
                    batch_id
                FROM `{silver}.silver_order_items`
                WHERE batch_id IN UNNEST(@batch_ids)
            """,
        ),
        dict(
            table="fct_payments",
            sql=f"""
                SELECT
                    order_id, payment_sequential, payment_type,
                    payment_installments, payment_value, is_synthetic,
                    batch_id
                FROM `{silver}.silver_order_payments`
                WHERE batch_id IN UNNEST(@batch_ids)
            """,
        ),
        dict(
            table="fct_reviews",
            sql=f"""
                SELECT
                    review_id,
                    order_id,
                    review_score,
                    review_creation_date,
                    review_answer_timestamp,
                    TIMESTAMP_DIFF(review_answer_timestamp, review_creation_date, HOUR) / 24.0
                        AS review_response_days,
                    is_synthetic,
                    batch_id
                FROM `{silver}.silver_order_reviews`
                WHERE batch_id IN UNNEST(@batch_ids)
            """,
        ),
    ]


def process_window(batch_id: str, dry_run: bool) -> int:
    """Runs all silver + gold steps for one batch_id. Returns total bytes
    processed (for logging). Raises on any step failure — the caller must
    not advance the checkpoint if this raises.

    Each step is individually idempotent: before running it, we check
    whether ITS OWN destination table already has this batch_id, and skip
    just that step if so. This is deliberately NOT a single check done once
    for the whole window (that used to check only silver_orders as a proxy
    for all 9 steps) — a crash between two steps used to mean the retry
    would see the proxy table as "done" and skip everything after it
    forever, permanently losing whatever hadn't run yet with no error
    surfaced anywhere. Checking per-step means a retry resumes exactly at
    the step that didn't finish, re-running only what's actually missing,
    and never double-appending a step that already succeeded.
    """
    from google.cloud import bigquery
    from replay import bq_writer

    params = [bigquery.ArrayQueryParameter("batch_ids", "STRING", [batch_id])]
    total_bytes = 0

    for step in silver_steps() + gold_steps():
        dataset = config.BQ_SILVER_DATASET if step["table"].startswith("silver_") else config.BQ_GOLD_DATASET

        if dry_run:
            total_bytes += bq_writer.dry_run_query(step["sql"], query_parameters=params)
            print(f"[dry-run] {step['table']}: SQL validated.")
            continue

        table_id = f"{config.GCP_PROJECT_ID}.{dataset}.{step['table']}"
        if bq_writer.already_loaded_batch_ids(table_id, [batch_id]):
            print(f"[{batch_id}] {step['table']}: already loaded, skipping this step.")
            continue

        total_bytes += bq_writer.run_transform_query(
            step["sql"], dataset, step["table"], query_parameters=params,
        )

    return total_bytes


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                         help="Validate SQL and estimate bytes scanned; write nothing.")
    parser.add_argument("--init-cursor", type=str, default=None,
                         help="ISO timestamp. One-time bootstrap: sets the live_transform_cursor "
                              "checkpoint WITHOUT processing anything. Use the timestamp of the "
                              "last full `dbt run` you intend to do.")
    args = parser.parse_args()

    from replay import bq_writer

    if args.init_cursor:
        ts = datetime.fromisoformat(args.init_cursor)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        bq_writer.set_state(PIPELINE_NAME, ts)
        print(f"[06_live_transform] Checkpoint initialized to {ts.isoformat()}. "
              f"Nothing processed (bootstrap only).")
        return

    checkpoint = bq_writer.get_state(PIPELINE_NAME)
    if checkpoint is None:
        print(
            "[ERROR] No live_transform_cursor checkpoint found. Run this once with "
            "--init-cursor '<timestamp of your last full dbt run>' before starting "
            "the live cron. See live.md."
        )
        sys.exit(1)

    # Upper bound is replay_cursor (05_live_ingest.py's own checkpoint), NOT
    # wall-clock now(). 05 only advances replay_cursor past a window once it
    # has actually written that window's rows to bronze. Using now() instead
    # would let this script "see" a window as calendar-complete a few
    # minutes before 05 has run for it in a given cron tick, try to
    # transform it (silently reading 0 rows from bronze), advance past it
    # anyway, and then skip it forever once 05 finally does write it a run
    # later — a real gap, not just a harmless no-op. Bounding by
    # replay_cursor guarantees this script only ever processes windows 05
    # has definitely already committed to bronze.
    upper_bound = bq_writer.get_state("replay_cursor")
    if upper_bound is None:
        print(
            "[ERROR] No replay_cursor state found. Run scripts/05_live_ingest.py "
            "at least once (which itself requires 04_backfill.py to have run) "
            "before running this script."
        )
        sys.exit(1)

    windows = list(build_windows(checkpoint, upper_bound, config.LIVE_BATCH_HOURS))
    if not windows:
        if checkpoint > upper_bound:
            print(
                f"[skip] live_transform_cursor ({checkpoint.isoformat()}) is AHEAD of "
                f"replay_cursor ({upper_bound.isoformat()}) — this usually means "
                f"--init-cursor was set to a timestamp later than what 05_live_ingest.py "
                f"has actually reached in BigQuery (e.g. a guessed 'now' instead of the "
                f"real last-dbt-run time, or 05 was only ever run with --dry-run so far, "
                f"which never advances the real replay_cursor). Re-run "
                f"--init-cursor with a timestamp <= replay_cursor, or run "
                f"05_live_ingest.py for real first."
            )
        else:
            print(f"[skip] No untransformed complete {config.LIVE_BATCH_HOURS}h window yet "
                  f"(checkpoint={checkpoint.isoformat()}, replay_cursor={upper_bound.isoformat()}).")
        return

    total_bytes = 0
    latest_checkpoint = checkpoint

    for target_date, start_hour, end_hour in windows:
        batch_id = f"live-{target_date.isoformat()}-{start_hour:02d}-{end_hour:02d}"

        window_end_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc) + \
            timedelta(hours=end_hour if end_hour != 24 else 24)

        # No upfront "is this whole window done?" check here anymore — every
        # step inside process_window() now checks its OWN destination table
        # individually and skips itself if already loaded. A window where
        # every step is already done will just print 9 "already loaded"
        # lines and cost 9 cheap metadata lookups instead of 1, which is a
        # negligible price for making crash-recovery actually correct (see
        # process_window()'s docstring).

        bytes_used = process_window(batch_id, args.dry_run)
        total_bytes += bytes_used
        print(f"[{batch_id}] done ({bytes_used / 1e9:.4f} GB).")

        if not args.dry_run:
            latest_checkpoint = window_end_dt
            bq_writer.set_state(PIPELINE_NAME, latest_checkpoint)

    print(f"\nLive-transform run complete. {len(windows)} window(s) processed. "
          f"Total: {total_bytes / 1e9:.4f} GB.")
    if not args.dry_run:
        print(f"New checkpoint: {latest_checkpoint.isoformat()}")
    else:
        print("(--dry-run: no checkpoint was advanced, nothing was written.)")


if __name__ == "__main__":
    main()