
"""Generate synthetic daily healthcare resource data."""

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

from generator import config


def generate_resources(output_dir: Path, seed: int) -> None:
    """Generate daily stock and bed occupancy for each facility."""

    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    start = date.fromisoformat(config.START_DATE)
    end = date.fromisoformat(config.END_DATE)

    output_path = output_dir / "resources.csv"

    facility_ids = [
        f"F{i:04d}"
        for i in range(1, config.NUMBER_OF_FACILITIES + 1)
    ]

    fields = [
        "date",
        "facility_id",
        "beds_available",
        "beds_occupied",
        "iv_saline_stock",
        "test_kit_stock",
    ]

    total_rows = 0

    with output_path.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        current = start

        while current <= end:
            for facility_id in facility_ids:
                beds_available = rng.randint(20, 200)
                beds_occupied = rng.randint(
                    0, beds_available
                )

                iv_saline_stock = rng.randint(50, 2000)
                test_kit_stock = rng.randint(20, 1000)

                writer.writerow({
                    "date": current.isoformat(),
                    "facility_id": facility_id,
                    "beds_available": beds_available,
                    "beds_occupied": beds_occupied,
                    "iv_saline_stock": iv_saline_stock,
                    "test_kit_stock": test_kit_stock,
                })

                total_rows += 1

            current += timedelta(days=1)

    print(f"Generated {total_rows:,} daily resource records")
    print(f"Resource data: {output_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--seed",
        type=int,
        default=config.RANDOM_SEED,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=config.SYNTHETIC_DIR,
    )

    args = parser.parse_args()
    generate_resources(args.output_dir, args.seed)


if __name__ == "__main__":
    main()