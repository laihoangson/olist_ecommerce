"""
Phase B — Live (cron, every LIVE_BATCH_HOURS, default 6h). FREE-TIER / NO-BILLING SAFE.

Continues from wherever Phase A backfill (or a previous live-ingest run)
left off, using the `replay_cursor` pipeline_state row. Each run processes
every complete 6h window between the checkpoint and "now" (normally just
one window, but if the cron missed a run or two it will catch up by
processing each missed window as its own batch, in order).

This script REQUIRES Phase A to have completed at least once (there must be
an existing replay_cursor state) — that's what defines "where Phase A
stopped" per the project plan. Run scripts/04_backfill.py first.

All BigQuery I/O goes through replay/bq_writer.py (write_table, get_state,
set_state, fetch_synthetic_customer_pool) — no MERGE/DML/streaming inserts,
so this works without a billing account attached to the project.

Usage:
    python scripts/05_live_ingest.py             # writes to BigQuery
    python scripts/05_live_ingest.py --dry-run   # local CSVs only, no BigQuery/state
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from replay.historical_loader import HistoricalReference  # noqa: E402
from replay import replay_engine  # noqa: E402


def build_windows(start: datetime, end: datetime, hours: int):
    """Yield (date, window_start_hour, window_end_hour) tuples covering every
    complete `hours`-sized block between start and end. Windows never cross a
    day boundary (0-6, 6-12, 12-18, 18-24 for hours=6), which keeps the
    replay_engine's per-day template sampling simple."""
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
                         help="Write to local CSVs, use a fake in-memory cursor instead of BigQuery state.")
    parser.add_argument("--dry-run-since", type=str, default=None,
                         help="ISO timestamp to use as the fake checkpoint in --dry-run mode.")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)

    historical_ref = HistoricalReference()
    historical_ref.load()

    if args.dry_run:
        if not args.dry_run_since:
            print("[ERROR] --dry-run requires --dry-run-since '<ISO timestamp>' to simulate a checkpoint.")
            sys.exit(1)
        checkpoint = datetime.fromisoformat(args.dry_run_since)
        if checkpoint.tzinfo is None:
            checkpoint = checkpoint.replace(tzinfo=timezone.utc)
        customer_pool = []
    else:
        from replay import bq_writer
        checkpoint = bq_writer.get_state("replay_cursor")
        if checkpoint is None:
            print(
                "[ERROR] No replay_cursor state found. Run scripts/04_backfill.py "
                "at least once before starting the live-ingest cron."
            )
            sys.exit(1)
        customer_pool = bq_writer.fetch_synthetic_customer_pool()

    windows = list(build_windows(checkpoint, now, config.LIVE_BATCH_HOURS))
    if not windows:
        print(f"[skip] No complete {config.LIVE_BATCH_HOURS}h window available yet "
              f"(checkpoint={checkpoint.isoformat()}, now={now.isoformat()}).")
        return

    dry_run_dir = config.PROJECT_ROOT / "outputs_dry_run"
    latest_checkpoint = checkpoint

    for target_date, start_hour, end_hour in windows:
        batch_id = f"live-{target_date.isoformat()}-{start_hour:02d}-{end_hour:02d}"
        batch = replay_engine.generate_batch(
            historical_ref,
            target_date,
            window_start_hour=start_hour,
            window_end_hour=end_hour,
            synthetic_customer_pool=customer_pool,
            batch_id=batch_id,
        )
        customer_pool = batch["updated_customer_pool"]

        write_batch(batch, batch_id, args.dry_run, dry_run_dir)

        n_orders = len(batch["orders"])
        print(
            f"[{batch_id}] template={batch['template_date_used']} "
            f"orders={n_orders} new_customers={len(batch['new_customers'])}"
        )

        window_end_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc) + \
            timedelta(hours=end_hour if end_hour != 24 else 24)
        latest_checkpoint = window_end_dt

        if not args.dry_run:
            from replay import bq_writer
            bq_writer.set_state("replay_cursor", latest_checkpoint)

    print(f"\nLive-ingest run complete. {len(windows)} window(s) processed. "
          f"New checkpoint: {latest_checkpoint.isoformat()}")
    if args.dry_run:
        print(f"Dry-run output written to {dry_run_dir}/")


if __name__ == "__main__":
    main()