# Phase 1 — DE: Replay Script, Backfill, Live Cron

Generates synthetic 2024+ Olist data (`is_synthetic=true`) by replaying real
historical days into `olist_bronze`, plus loads the real historical data
(`is_synthetic=false`) unchanged.

## 1. Setup

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in real values (project id already filled)
```

Put your `service-account.json` in this folder (same level as `config.py`)
and make sure `.env`'s `GOOGLE_APPLICATION_CREDENTIALS` points to it.

Copy the same 9 Olist CSVs from Phase 0 into `data/` (same filenames).

**Never commit `.env` or `service-account.json`.** Add both to `.gitignore`.

## 2. Order of operations

```bash
# 1) One-time: copy real historical data into bronze (is_synthetic=false)
python scripts/03_load_historical_to_bronze.py

# 2) One-time, manual trigger: backfill 2024-01-01 -> today (is_synthetic=true)
python scripts/04_backfill.py

# 3) Recurring, every 6h (via GitHub Actions cron): continue from where
#    backfill stopped
python scripts/05_live_ingest.py
```

Steps 1 and 2 can also be triggered from GitHub Actions
(`.github/workflows/backfill.yml`, manual `workflow_dispatch`). Step 3 runs
automatically every 6h via `.github/workflows/live_ingest.yml`.

### Required GitHub repo secrets/variables (for the workflows)
- `GCP_SA_KEY_JSON` — full contents of your service-account.json (secret)
- `GCP_PROJECT_ID` — `` (secret or repo variable)
- `BQ_LOCATION` — `US` (repo variable, optional — defaults to US)

The workflows write `.env` and `service-account.json` at runtime from these
secrets, so nothing sensitive lives in the repo itself.

## 3. Test everything locally first — `--dry-run`

Every script that touches BigQuery supports `--dry-run`, which skips
BigQuery entirely and writes to local CSVs under `outputs_dry_run/` instead.
**Do this before your first real run** to sanity-check the replay logic
against your actual CSVs:

```bash
python scripts/03_load_historical_to_bronze.py --dry-run
python scripts/04_backfill.py --dry-run --max-days 5
python scripts/05_live_ingest.py --dry-run --dry-run-since "2026-07-13T00:00:00+00:00"
```

Inspect `outputs_dry_run/*.csv` — check that `bronze_orders.order_purchase_timestamp`
falls on the target dates, that `source_template_date` values make sense
(should be inside 2017-01-01..2018-08-31, matching the day-of-week/month of
the target date), and that price/freight look close to but not identical to
the historical values.

## 4. Design decisions (read before changing anything)

**Whole-day resampling, not column-by-column sampling.** For each target
date, we bucket by `(month, day_of_week)`, pick one real historical date from
that bucket, and replay ALL of that day's orders/items/payments/reviews
together. This preserves cross-column and cross-table correlations (price vs
freight vs category, review_score vs delay, etc.) that would be lost by
fitting independent distributions per column.

**The Black Friday spike needs no special-case code.** Because we resample
whole historical days, a target date that lands on Nov-with-matching-weekday
will sometimes get mapped to the real Nov 2017 spike days, which already
carry ~60% more orders than a normal day. No manual multiplier was added.

**Stable window excludes ramp-up/tail months.** `HISTORICAL_STABLE_START` /
`END` (2017-01-01 .. 2018-08-31) intentionally exclude 2016-09..2016-12
(near-zero order counts — Olist was just launching) and 2018-09..2018-10
(near-zero — data export tail-cut). Confirmed from the real
`seasonality_summary.json` you generated in Phase 0. Including those months
would make some `(month, weekday)` buckets nearly empty and skew the
templates used for those calendar positions.

**Products, sellers, and geolocation are never faked.** Synthetic orders
reference the SAME `product_id`/`seller_id` as their historical template row
— we don't invent a new catalog. Only `bronze_customers` gets new synthetic
rows (new orders need a "person" behind them), reusing the zip/city/state
profile of the historical customer they were sampled from. Repeat customers
are sampled at `SYNTHETIC_REPEAT_CUSTOMER_RATE` (default 0.0312, taken
directly from your real repeat-purchase EDA number) so the synthetic dataset
keeps the same repeat-vs-one-time customer ratio going forward.

**Idempotent by construction.** The RNG seed for any given
`(target_date, window)` is deterministic, and every BigQuery write goes
through `bq_writer.upsert_dataframe`, which MERGEs on primary keys. Re-running
a failed backfill day or a retried cron tick produces the exact same rows and
never duplicates.

**Resumable backfill.** Progress is checkpointed after every day into
`pipeline_state` (`replay_cursor`). If the backfill job dies partway through,
just re-run `01_backfill.py` — it resumes from `last_processed_at + 1 day`
instead of starting over.

**Live-ingest requires backfill to have run at least once.** It reads the
same `replay_cursor` state; if none exists it exits with an error telling you
to run the backfill first. This matches the plan.md requirement that live
"tiếp nối vị trí Phase A dừng lại."

**Windows never cross a day boundary.** With `LIVE_BATCH_HOURS=6`, windows
are fixed at `[0-6), [6-12), [12-18), [18-24)` UTC — this keeps the
per-day template-sampling logic in `replay_engine` simple. If the cron
misses a run (GitHub Actions outage, etc.), the next run processes every
missed window in order, not just the latest one.

## 5. What's NOT handled yet (by design, later phases)

- No dbt / silver / gold transforms — that's Phase 2 (AE).
- No validation-vs-historical-distribution report for the *generated*
  synthetic data yet (recommended: reuse Phase 0's EDA scripts against the
  new bronze tables once there's enough synthetic volume to compare).
- `GROWTH_RATE_PER_YEAR` defaults to 1.0 (no growth). If you want synthetic
  volume to trend upward over 2024-2026, set it in `.env` — the thinning/
  oversampling logic already exists in `replay_engine._growth_factor`.
