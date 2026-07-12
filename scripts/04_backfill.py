"""
Phase A — Backfill (run ONCE, manual trigger). FREE-TIER / NO-BILLING SAFE.

All BigQuery I/O goes through replay/bq_writer.py (write_table, get_state,
set_state, fetch_synthetic_customer_pool) — see that module's docstring for
why it never issues MERGE/DML/streaming inserts, and how it handles tables
that predate the batch_id column (e.g. tables bootstrapped by
00_load_historical_to_bronze.py).

Usage:
    python scripts/04_backfill.py             # writes to BigQuery
    python scripts/04_backfill.py --dry-run   # local CSVs only, no BigQuery/state
    python scripts/04_backfill.py --max-days 5
"""

import argparse
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from replay.historical_loader import HistoricalReference  # noqa: E402
from replay import replay_engine  # noqa: E402


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def write_batch(batch: dict, batch_id: str, dry_run: bool, dry_run_dir: Path):
    tables = {
        "bronze_customers": batch["new_customers"],
        "bronze_orders": batch["orders"],
        "bronze_order_items": batch["items"],
        "bronze_order_payments": batch["payments"],
        "bronze_order_reviews": batch["reviews"],
    }

    if dry_run:
        dry_run_dir.mkdir(exist_ok=True)
        for name, df in tables.items():
            if df is None or df.empty:
                continue
            out = df.copy()
            out["batch_id"] = batch_id
            out_path = dry_run_dir / f"{name}.csv"
            out.to_csv(out_path, mode="a", header=not out_path.exists(), index=False)
        return

    from replay import bq_writer
    for name, df in tables.items():
        bq_writer.write_table(df, config.BQ_BRONZE_DATASET, name, batch_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                         help="Write to local CSVs instead of BigQuery, skip state tracking.")
    parser.add_argument("--max-days", type=int, default=None,
                         help="Optional cap on number of days to process this run (useful for smoke-testing).")
    parser.add_argument("--start-date", type=str, default=None,
                         help="Override config.BACKFILL_START_DATE (YYYY-MM-DD). If given, takes priority "
                              "over the replay_cursor state resume too, so use it when you deliberately "
                              "want to (re)run a specific window rather than continue from where state left off.")
    parser.add_argument("--end-date", type=str, default=None,
                         help="Override config.BACKFILL_END_DATE (YYYY-MM-DD).")
    args = parser.parse_args()

    window_start = date.fromisoformat(args.start_date) if args.start_date else config.BACKFILL_START_DATE
    window_end = date.fromisoformat(args.end_date) if args.end_date else config.BACKFILL_END_DATE

    print(f"Backfill window: {window_start} .. {window_end}")

    historical_ref = HistoricalReference()
    historical_ref.load()

    resume_from = window_start
    customer_pool = []

    if not args.dry_run:
        from replay import bq_writer
        last_state = bq_writer.get_state("replay_cursor")
        # Only auto-resume from state when the caller didn't explicitly pin
        # --start-date. This lets a chunked GH Actions run (e.g. one
        # workflow_dispatch per month) rely on state for the *next*
        # unspecified run, while an explicit --start-date always wins so you
        # can deliberately re-run or fill a specific window.
        if args.start_date is None and last_state is not None:
            resume_from = last_state.date() + timedelta(days=1)
            print(f"[resume] Found existing replay_cursor state, resuming from {resume_from}.")
        customer_pool = bq_writer.fetch_synthetic_customer_pool()
        print(f"[resume] Loaded {len(customer_pool)} existing synthetic customers into the pool.")

    dry_run_dir = config.PROJECT_ROOT / "outputs_dry_run"
    days_processed = 0

    for target_date in daterange(resume_from, window_end):
        if args.max_days is not None and days_processed >= args.max_days:
            print(f"[stop] Reached --max-days={args.max_days}.")
            break

        batch_id = f"backfill-{target_date.isoformat()}"
        batch = replay_engine.generate_batch(
            historical_ref,
            target_date,
            synthetic_customer_pool=customer_pool,
            batch_id=batch_id,
        )
        customer_pool = batch["updated_customer_pool"]

        write_batch(batch, batch_id, args.dry_run, dry_run_dir)

        n_orders = len(batch["orders"])
        print(
            f"[{target_date}] template={batch['template_date_used']} "
            f"orders={n_orders} new_customers={len(batch['new_customers'])}"
        )

        if not args.dry_run:
            from replay import bq_writer
            checkpoint = datetime.combine(target_date, time.max, tzinfo=timezone.utc)
            bq_writer.set_state("replay_cursor", checkpoint)

        days_processed += 1

    print(f"\nBackfill run complete. {days_processed} day(s) processed.")
    if args.dry_run:
        print(f"Dry-run output written to {dry_run_dir}/")


if __name__ == "__main__":
    main()