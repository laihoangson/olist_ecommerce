# Olist E-commerce Data Platform — Phase 1 (DE) + Phase 2 (AE / dbt)

Single project, built incrementally. This zip contains everything from
Phase 1 (replay/backfill/live-ingest into BigQuery) plus Phase 2 (dbt
bronze -> silver -> gold). Nothing from Phase 1 was removed — Phase 2 is
purely additive (a new `dbt/` folder, plus small, backward-compatible
extensions to `config.py`, `.env.example`, `requirements.txt`, and the
GitHub Actions workflows).

Everything in this project is **BigQuery-sandbox-safe (no billing account
required)**: no MERGE, no other DML, no streaming inserts anywhere, in
either the Python pipeline or the dbt models. See `replay/bq_writer.py` and
each dbt model's comments for how that constraint shaped the design.

---

## 1. Setup

```bash
pip install -r requirements.txt      # now also installs dbt-core + dbt-bigquery
cp .env.example .env                 # fill in real values if you haven't already
```

Put `service-account.json` in the project root (same level as `config.py`).
Copy the 9 Olist CSVs into `data/`. Never commit `.env` or
`service-account.json`.

## 2. Order of operations

```bash
# Phase 1 — DE
python scripts/00_load_raw_to_bigquery.py         # one-time: raw CSVs -> olist_raw
python scripts/03_load_historical_to_bronze.py    # one-time: historical -> olist_bronze (is_synthetic=false)
python scripts/04_backfill.py                     # one-time, manual trigger: synthetic backfill -> olist_bronze
python scripts/05_live_ingest.py                  # recurring, every 6h (via cron): continues from backfill

# Phase 2 — AE (dbt)
cd dbt
dbt deps                                          # installs dbt_utils (via git — see packages.yml)
dbt run                                           # bronze -> silver -> gold, full-refresh table rebuild
dbt test                                          # not_null / unique / relationships / accepted_values
```

`00` and `03` are one-time. `04` (backfill) is one-time, manual trigger.
`05` (live-ingest) and the dbt run are both meant to run automatically,
every 6h, **as one chained job** — see `.github/workflows/live_ingest.yml`,
which now runs `05_live_ingest.py` → `dbt run` → `dbt test` in sequence.
That's what makes dbt "chạy tự động khi có live data": every time new
synthetic data lands in bronze, the very same workflow run rebuilds
silver/gold on top of it, so gold is never more than 6h stale.

`.github/workflows/backfill.yml` does the same chaining for the one-time
Phase A run (historical load → backfill → `dbt run` → `dbt test`), so gold
is fully populated the moment backfill finishes — you don't have to
remember to run dbt manually afterward.

### Required GitHub repo secrets/variables (unchanged from Phase 1)
- `GCP_SA_KEY_JSON`, `GCP_PROJECT_ID` (secrets)
- `BQ_LOCATION` (repo variable, optional, defaults to `US`)

## 3. Test everything locally first

**Python scripts** — every script that touches BigQuery supports
`--dry-run` (writes to `outputs_dry_run/*.csv` instead):

```bash
python scripts/03_load_historical_to_bronze.py --dry-run
python scripts/04_backfill.py --dry-run --max-days 5
python scripts/05_live_ingest.py --dry-run --dry-run-since "2024-01-06T00:00:00+00:00"
```

**dbt** — `dbt parse` and `dbt compile` don't need real data, just a working
BigQuery connection (they'll still try to authenticate, but won't run
queries that cost anything):

```bash
cd dbt
dbt deps
dbt parse      # validates the whole project (refs, sources, Jinja) resolves
dbt run --select silver_customers   # try one model first
dbt run                              # then the full DAG
dbt test
```

## 4. Design decisions — Phase 1 (recap)

- **Whole-day resampling** for synthetic data: a target date is bucketed by
  `(month, day_of_week)`, a real historical date sharing that bucket is
  sampled, and its ENTIRE day of orders/items/payments/reviews is replayed
  together (preserves cross-column/cross-table correlations). The Nov 2017
  Black Friday spike reproduces itself automatically this way — no special
  case needed.
- **Stable window** `2017-01-01..2018-08-31` excludes the 2016 ramp-up and
  2018 tail-cut months (confirmed near-zero volume from the real Phase 0
  EDA), so every `(month, weekday)` bucket has enough real days to sample.
- **No MERGE anywhere.** `replay/bq_writer.py` only uses load jobs
  (`WRITE_EMPTY` / `WRITE_APPEND` with `ALLOW_FIELD_ADDITION` for schema
  evolution) and `SELECT` checks, keyed by a `batch_id` tagged onto every
  row. Re-running a batch skips it instead of duplicating it — that's the
  free-tier-safe idempotency model, not a per-row MERGE.
- **Products/sellers/geolocation are never faked** — synthetic orders reuse
  the real catalog's `product_id`/`seller_id`. Only `bronze_customers` gets
  new synthetic rows, at `SYNTHETIC_REPEAT_CUSTOMER_RATE` (0.0312, taken
  directly from the real repeat-purchase EDA number) to preserve the
  repeat-vs-one-time ratio going forward.
- **Resumable backfill**, **live-ingest catch-up** for missed cron runs —
  both checkpoint via the `pipeline_state` / `replay_cursor` row.

## 5. Design decisions — Phase 2 (dbt)

- **`table` materialization everywhere, never `incremental`.** This is the
  single most important thing to not "fix" later: dbt-bigquery's
  incremental strategies (`merge`, `insert_overwrite`) both compile down to
  a `MERGE` statement internally — exactly the DML that fails without
  billing. `table` materialization is `CREATE OR REPLACE TABLE AS SELECT`,
  which is DDL, not DML, so it works in the sandbox. The trade-off is a
  full rebuild of silver/gold on every run — perfectly fine at this data
  volume (well under BigQuery's free query-scan quota even with a year+ of
  synthetic data).
- **Defensive dedup in every silver model** (`row_number() over (partition
  by <key> order by batch_id desc) ... where rn = 1`). Bronze is
  append-only by design (see above), so silver is where we guarantee one
  row per natural key before anything downstream joins on it.
- **`generate_schema_name` is overridden** (`macros/generate_schema_name.sql`)
  so `+schema: olist_silver` / `+schema: olist_gold` set the BigQuery
  dataset directly. Without this override, dbt's default behavior
  concatenates the profile's default dataset with the custom schema (e.g.
  `olist_silver_olist_gold`), which is not what we want here.
- **`source_template_date`** on `bronze_orders`/`silver_orders`/`fct_orders`
  is NULL for historical rows and populated for synthetic rows (it's the
  historical calendar day a synthetic order was resampled from — see Phase
  1 above). `03_load_historical_to_bronze.py` adds this column explicitly
  on the historical load so the schema is consistent from day one, whether
  or not backfill has run yet.
- **Business logic lives in gold, not silver**, per the project plan:
  `is_delayed` and `delivery_days` are computed in `fct_orders.sql`, not in
  `silver_orders.sql`. Silver only cleans/casts/dedupes.
- **`is_synthetic` flows through every layer** (bronze → silver → gold →
  marts) so every downstream consumer — Power BI (`is_synthetic=false`
  only), the live dashboard (`is_synthetic=true`), and each ML model — can
  filter independently without needing to know how the flag originated.
- **`mart_customer_rfm` deliberately excludes Frequency** (per plan.md:
  96.88% of real customers buy exactly once, so Frequency is near-constant
  and would add no signal to KMeans). `dim_customers` still has Frequency
  for BI use — it's just not carried into the ML-feature mart.
- **`product_category_name_translation`** is loaded into bronze as its own
  table (`bronze_product_category_translation`) via `03_load_historical_to_bronze.py`,
  same as every other historical table — not a dbt seed. It's real Kaggle
  data you already have in `data/`, so there was no reason to duplicate it
  as a seed file or (worse) hand-type a possibly-wrong translation table.

## 6. What's NOT handled yet (later phases)

- Power BI / dashboard consumption of the gold layer (Phase 3, DA).
- ML feature specs / leakage-contract doc, model training (Phase 4, DS/ML) —
  `mart_repeat_purchase_training` and `mart_delivery_delay_training` (the
  point-in-time marts for models #3/#4) aren't built yet; only the two
  models plan.md marked as safe with the current gold layer
  (`mart_daily_revenue` for Prophet, `mart_customer_rfm` for KMeans) exist
  so far.
- dbt docs generation / a hosted catalog (`dbt docs generate` works out of
  the box once you can run the project — not wired into CI yet).
