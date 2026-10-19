
"""Generate a synthetic registry of healthcare facilities."""

import argparse
import csv
import random
from pathlib import Path

from generator import config


def generate_facilities(
    output_dir: Path,
    seed: int,
) -> None:
    """Create a deterministic facility registry covering all districts."""
    if config.NUMBER_OF_DISTRICTS <= 0:
        raise ValueError("NUMBER_OF_DISTRICTS must be positive")

    if config.NUMBER_OF_FACILITIES < config.NUMBER_OF_DISTRICTS:
        raise ValueError(
            "NUMBER_OF_FACILITIES must cover all districts"
        )

    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "facilities.csv"

    district_ids = [
        f"D{i:03d}"
        for i in range(1, config.NUMBER_OF_DISTRICTS + 1)
    ]

    facility_types = [
        "Upazila Health Complex",
        "District Hospital",
        "Medical College Hospital",
        "Community Clinic",
        "Private Hospital",
    ]

    fields = [
        "facility_id",
        "facility_name",
        "district_id",
        "facility_type",
        "latitude",
        "longitude",
    ]

    with output_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        for i in range(config.NUMBER_OF_FACILITIES):
            district = district_ids[i % len(district_ids)]

            # Approximate Bangladesh-wide coordinates for synthetic data.
            latitude = round(rng.uniform(20.6, 26.6), 6)
            longitude = round(rng.uniform(88.0, 92.7), 6)

            writer.writerow({
                "facility_id": f"F{i + 1:04d}",
                "facility_name": f"Synthetic Facility {i + 1:04d}",
                "district_id": district,
                "facility_type": rng.choice(facility_types),
                "latitude": latitude,
                "longitude": longitude,
            })

    print(f"Generated {config.NUMBER_OF_FACILITIES} facilities")
    print(f"Facility registry: {output_path}")


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

    generate_facilities(args.output_dir, args.seed)


if __name__ == "__main__":
    main()