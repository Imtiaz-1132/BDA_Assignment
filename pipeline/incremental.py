"""Simple persistent ledger for idempotent EpiScope batch processing."""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


def load_ledger(ledger_path: Path) -> dict:
    if not ledger_path.exists():
        return {"completed_batches": {}}
    return json.loads(ledger_path.read_text(encoding="utf-8"))


def save_ledger(ledger_path: Path, ledger: dict) -> None:
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = ledger_path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(ledger, indent=2), encoding="utf-8"
    )
    temporary_path.replace(ledger_path)


def batch_is_complete(ledger_path: Path, batch_id: str) -> bool:
    ledger = load_ledger(ledger_path)
    return batch_id in ledger["completed_batches"]



def available_silver_batches(silver_dir: Path) -> list[str]:
    """List batch IDs whose Silver encounter datasets are available."""
    if not silver_dir.exists():
        return []

    batch_ids = []
    for batch_path in silver_dir.glob("batch_id=*"):
        encounters_path = batch_path / "encounters"
        if batch_path.is_dir() and encounters_path.is_dir():
            batch_id = batch_path.name.removeprefix("batch_id=")
            if batch_id:
                batch_ids.append(batch_id)

    return sorted(batch_ids)


def read_available_silver_batches(spark, silver_dir: Path):
    """Read all available Silver encounter batches into one DataFrame."""
    from functools import reduce

    from pyspark.sql import DataFrame

    batch_ids = available_silver_batches(silver_dir)
    if not batch_ids:
        raise FileNotFoundError(
            f"No Silver encounter batches found under {silver_dir}"
        )

    frames = [
        spark.read.parquet(str(silver_dir / f"batch_id={batch_id}" / "encounters"))
        for batch_id in batch_ids
    ]

    # unionByName tolerates column ordering differences but intentionally
    # fails if the batch schemas are incompatible.
    combined = reduce(
        lambda left, right: left.unionByName(right),
        frames,
    )
    return combined

def pending_affected_weeks(ledger_path: Path) -> list[str]:
    """Return distinct epidemiological weeks marked for recomputation."""
    ledger = load_ledger(ledger_path)
    weeks = set()
    for batch in ledger["completed_batches"].values():
        weeks.update(batch.get("affected_weeks", []))
    return sorted(weeks)


def mark_batch_complete(
    ledger_path: Path,
    batch_id: str,
    input_rows: int,
    affected_weeks: list[str] | None = None,
) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError("batch_id may contain only letters, digits, _ and -")
    if input_rows < 0:
        raise ValueError("input_rows cannot be negative")

    ledger = load_ledger(ledger_path)
    ledger["completed_batches"][batch_id] = {
        "input_rows": input_rows,
        "affected_weeks": sorted(set(affected_weeks or [])),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    save_ledger(ledger_path, ledger)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=Path("data/lakehouse/processed_batches.json"))
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--input-rows", type=int)
    parser.add_argument("--affected-week", action="append", default=[])
    args = parser.parse_args()

    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.batch_id):
        parser.error("batch_id may contain only letters, digits, _ and -")

    if args.input_rows is None:
        print(json.dumps({
            "batch_id": args.batch_id,
            "already_completed": batch_is_complete(args.ledger, args.batch_id),
        }, indent=2))
    else:
        mark_batch_complete(
            args.ledger,
            args.batch_id,
            args.input_rows,
            args.affected_week,
        )
        print(f"Recorded completed batch: {args.batch_id}")


if __name__ == "__main__":
    main()
