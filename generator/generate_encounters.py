
"""Generate synthetic EpiScope encounter records.

Records are generated incrementally to limit memory usage.
Outbreak ground truth is written to a separate file.
"""

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

from generator import config


def choose_disease(rng: random.Random, district: str, onset: date) -> str:
    """Choose a disease using seasonal and geographic patterns."""
    month = onset.month

    weights = {
        "dengue": 1.0 + (3.0 if month in (7, 8, 9, 10) else 0.2),
        "diarrhoea": 1.0 + (2.0 if month in (4, 5, 6, 7, 8) else 0.2),
        "influenza": 1.0 + (1.5 if month in (6, 7, 8) else 0.2),
        "malaria": 1.0 + (
            2.0 if district in ("D011", "D012", "D013") else 0.0
        ),
    }

    diseases = list(weights)
    return rng.choices(
        diseases,
        weights=[weights[disease] for disease in diseases],
        k=1,
    )[0]


def generate_encounters(
    output_dir: Path,
    scale_factor: float,
    seed: int,
) -> None:
    """Generate synthetic encounters and separate outbreak ground truth."""

    if scale_factor <= 0:
        raise ValueError("scale_factor must be greater than zero")

    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    start = date.fromisoformat(config.START_DATE)
    end = date.fromisoformat(config.END_DATE)

    total_records = int(25_000_000 * scale_factor)
    if total_records < 1:
        raise ValueError("Scale factor produces fewer than one record")

    encounter_path = output_dir / "encounters.csv"
    truth_path = output_dir / "ground_truth_outbreaks.csv"

    district_ids = [
        f"D{i:03d}"
        for i in range(1, config.NUMBER_OF_DISTRICTS + 1)
    ]
    facility_ids = [
        f"F{i:04d}"
        for i in range(1, config.NUMBER_OF_FACILITIES + 1)
    ]

    disease_codes = config.DISEASE_CODES
    disease_names = list(disease_codes)

    # Generate at least 30 reproducible outbreak truth records.
    truth_rng = random.Random(seed + 1000)
    outbreak_rows = []

    for outbreak_id in range(1, max(config.MINIMUM_OUTBREAKS, 30) + 1):
        disease = disease_names[(outbreak_id - 1) % len(disease_names)]
        district = district_ids[(outbreak_id * 7) % len(district_ids)]

        duration = truth_rng.randint(7, 35)
        latest_start = end - timedelta(days=duration)
        span = (latest_start - start).days

        outbreak_start = start + timedelta(
            days=truth_rng.randint(0, max(0, span))
        )
        outbreak_end = outbreak_start + timedelta(days=duration)

        outbreak_rows.append({
            "outbreak_id": f"OB{outbreak_id:03d}",
            "disease": disease,
            "district_id": district,
            "start_date": outbreak_start.isoformat(),
            "end_date": outbreak_end.isoformat(),
            "severity": truth_rng.choice(["low", "medium", "high"]),
        })

    with truth_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(outbreak_rows[0].keys()),
        )
        writer.writeheader()
        writer.writerows(outbreak_rows)

    encounter_fields = [
        "encounter_id",
        "patient_id",
        "facility_id",
        "district_id",
        "onset_date",
        "report_date",
        "age",
        "sex",
        "icd10_code",
        "lab_confirmed",
        "admission",
        "outcome",
    ]

    with encounter_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=encounter_fields)
        writer.writeheader()

        previous_row = None

        for i in range(total_records):

            # Copy every 50th record exactly to simulate duplicate records.
            if i > 0 and i % 50 == 0:
                writer.writerow(previous_row)
                continue

            # Inject some cases during known outbreak periods.
            # Downstream detection and evaluation must not read ground truth.
            if outbreak_rows and rng.random() < 0.25:
                outbreak = rng.choice(outbreak_rows)

                outbreak_start = date.fromisoformat(
                    outbreak["start_date"]
                )
                outbreak_end = date.fromisoformat(
                    outbreak["end_date"]
                )

                onset = outbreak_start + timedelta(
                    days=rng.randint(
                        0, (outbreak_end - outbreak_start).days
                    )
                )
                district = outbreak["district_id"]
                disease = outbreak["disease"]

            else:
                onset = start + timedelta(
                    days=rng.randint(0, (end - start).days)
                )
                district = rng.choice(district_ids)
                disease = choose_disease(rng, district, onset)

            icd10_code = rng.choice(disease_codes[disease])

            # Reporting delay is right-skewed and limited to 0-21 days.
            delay = min(21, int(rng.expovariate(0.35)))

            # Approximate reduced weekend reporting.
            initial_report_date = onset + timedelta(days=delay)
            if initial_report_date.weekday() >= 5 and rng.random() < 0.25:
                delay = min(21, delay + rng.randint(1, 3))

            # Keep report dates within the configured simulation period.
            # At the end of the simulation, cap the delay to the available
            # number of days so the actual date difference remains accurate.
            maximum_delay = min(21, (end - onset).days)
            delay = min(delay, maximum_delay)
            report_date = onset + timedelta(days=delay)

            # Approximately 0.5% orphan facility references.
            if rng.random() < 0.005:
                facility_id = f"ORPHAN_{i}"
            else:
                facility_id = rng.choice(facility_ids)

            # Approximately 1% invalid ages for quality-rule testing.
            if rng.random() < 0.01:
                age = rng.choice([-5, 150, 212])
            else:
                age = rng.randint(0, 100)

            admission = rng.random() < (
                0.25 if disease == "dengue" else 0.10
            )
            lab_confirmed = rng.random() < 0.25

            if admission and rng.random() < 0.03:
                outcome = "Deceased"
            elif admission:
                outcome = "Admitted"
            else:
                outcome = "Recovered"

            row = {
                "encounter_id": f"ENC{i:09d}",
                "patient_id": (
                    f"PAT{rng.randint(1, max(1, total_records // 2)):09d}"
                ),
                "facility_id": facility_id,
                "district_id": district,
                "onset_date": onset.isoformat(),
                "report_date": report_date.isoformat(),
                "age": age,
                "sex": rng.choice(["Male", "Female", "Other", "Unknown"]),
                "icd10_code": icd10_code,
                "lab_confirmed": lab_confirmed,
                "admission": admission,
                "outcome": outcome,
            }

            writer.writerow(row)
            previous_row = row.copy()

    print(f"Generated {total_records:,} encounter records")
    print(f"Encounter data: {encounter_path}")
    print(f"Outbreak ground truth: {truth_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=config.SCALE_FACTOR,
    )
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

    generate_encounters(
        output_dir=args.output_dir,
        scale_factor=args.scale_factor,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()