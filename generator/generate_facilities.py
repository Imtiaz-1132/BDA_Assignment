"""Generate a synthetic registry of healthcare facilities."""

import argparse
import csv
import random
from pathlib import Path

from generator import config

CENTROIDS_PATH = config.DATA_DIR / "geography" / "district_centroids.csv"


def load_district_centroids(centroids_path: Path) -> dict:
    """Load real district centroids, or return an empty mapping if unavailable."""
    if not centroids_path.is_file():
        print("Centroid file not found; using fallback synthetic coordinates.")
        return {}

    with centroids_path.open("r", newline="", encoding="utf-8-sig") as file:
        rows = list(csv.DictReader(file))

    required = {"district_id", "district_name", "latitude", "longitude"}
    if not rows or not required.issubset(rows[0].keys()):
        raise ValueError(
            f"Centroid CSV must contain these columns: {sorted(required)}"
        )

    centroids = {}
    for row in rows:
        district_id = row["district_id"]
        if district_id in centroids:
            raise ValueError(f"Duplicate district ID in centroid CSV: {district_id}")

        centroids[district_id] = {
            "district_name": row["district_name"],
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
        }

    expected_ids = {
        f"D{i:03d}" for i in range(1, config.NUMBER_OF_DISTRICTS + 1)
    }
    if set(centroids) != expected_ids:
        raise ValueError(
            "Centroid district IDs do not match the configured district IDs."
        )

    return centroids


def generate_facilities(
    output_dir: Path,
    seed: int,
) -> None:
    """Create a deterministic facility registry covering all districts."""
    if config.NUMBER_OF_DISTRICTS <= 0:
        raise ValueError("NUMBER_OF_DISTRICTS must be positive")

    if config.NUMBER_OF_FACILITIES < config.NUMBER_OF_DISTRICTS:
        raise ValueError("NUMBER_OF_FACILITIES must cover all districts")

    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "facilities.csv"

    district_ids = [
        f"D{i:03d}"
        for i in range(1, config.NUMBER_OF_DISTRICTS + 1)
    ]

    centroids = load_district_centroids(CENTROIDS_PATH)

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

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()

        for i in range(config.NUMBER_OF_FACILITIES):
            district = district_ids[i % len(district_ids)]

            if centroids:
                latitude = centroids[district]["latitude"]
                longitude = centroids[district]["longitude"]
            else:
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
    if centroids:
        print("Facility coordinates: real district centroids")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=config.RANDOM_SEED)
    parser.add_argument("--output-dir", type=Path, default=config.SYNTHETIC_DIR)
    args = parser.parse_args()

    generate_facilities(args.output_dir, args.seed)


if __name__ == "__main__":
    main()
