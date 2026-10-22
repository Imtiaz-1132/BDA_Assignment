"""Generate synthetic EpiScope encounter records using observed weather."""

import argparse
import bisect
import csv
import random
from itertools import accumulate
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from generator import config
from generator.daily_incidence import build_daily_incidence_weights
from generator.outbreak_catalog import build_outbreaks
from generator.outbreak_model import outbreak_intensity, negative_binomial_cases

DEFAULT_WEATHER_PATH = config.DATA_DIR / "weather" / "observed_weather.csv"


def load_climate_data(weather_path: Path) -> dict:
    """Load daily weather and calculate district-specific climate features."""
    if not weather_path.is_file():
        raise FileNotFoundError(
            f"Weather file not found: {weather_path}. "
            "Run generator.fetch_weather first."
        )

    weather = pd.read_csv(weather_path, parse_dates=["date"])
    required = {
        "district_id", "date", "rainfall_mm", "temperature_c"
    }
    if not required.issubset(weather.columns):
        raise ValueError(
            f"Weather CSV must contain: {sorted(required)}"
        )

    weather = weather[
        ["district_id", "date", "rainfall_mm", "temperature_c"]
    ].copy()

    if weather[["district_id", "date"]].isna().any().any():
        raise ValueError("Weather data has missing district IDs or dates.")

    if weather.duplicated(["district_id", "date"]).any():
        raise ValueError("Weather contains duplicate district/date rows.")

    for column in ["rainfall_mm", "temperature_c"]:
        weather[column] = pd.to_numeric(weather[column], errors="coerce")

    if weather[["rainfall_mm", "temperature_c"]].isna().any().any():
        raise ValueError("Weather contains missing or invalid climate values.")

    if (weather["rainfall_mm"] < 0).any():
        raise ValueError("Rainfall values cannot be negative.")

    weather = weather.sort_values(
        ["district_id", "date"]
    ).reset_index(drop=True)

    grouped = weather.groupby("district_id", sort=False)

    # Average weather from 28–56 days before the onset date.
    weather["rain_lag_28_56"] = grouped["rainfall_mm"].transform(
        lambda series: series.shift(28).rolling(
            window=29, min_periods=29
        ).mean()
    )
    weather["temp_lag_28_56"] = grouped["temperature_c"].transform(
        lambda series: series.shift(28).rolling(
            window=29, min_periods=29
        ).mean()
    )

    # Recent rainfall: seven-day total ending on the onset date.
    weather["rain_recent_7"] = weather.groupby(
        "district_id", sort=False
    )["rainfall_mm"].transform(
        lambda series: series.rolling(
            window=7, min_periods=7
        ).sum()
    )

    # District-specific extreme-rain threshold.
    # Compare seven-day totals with seven-day totals.
    thresholds = weather.groupby("district_id")[
        "rain_recent_7"
    ].quantile(0.95).to_dict()

    result = {}
    for row in weather.itertuples(index=False):
        key = (str(row.district_id), row.date.date())
        result[key] = {
            "rain_lag_28_56": row.rain_lag_28_56,
            "temp_lag_28_56": row.temp_lag_28_56,
            "rain_recent_7": row.rain_recent_7,
            "rainfall_today": row.rainfall_mm,
            "temperature_today": row.temperature_c,
            "extreme_rain_threshold": thresholds[str(row.district_id)],
        }

    print(f"Loaded observed weather for {weather['district_id'].nunique()} districts.")
    print(f"Weather dates: {weather['date'].min().date()} to {weather['date'].max().date()}")
    return result


def get_climate_signals(onset: date) -> tuple[float, float]:
    """Fallback monthly proxies for isolated unit tests without weather."""
    rainfall = {
        1: 0.15, 2: 0.15, 3: 0.25, 4: 0.40,
        5: 0.65, 6: 0.85, 7: 1.00, 8: 1.00,
        9: 0.90, 10: 0.60, 11: 0.30, 12: 0.20,
    }
    temperature = {
        1: 0.35, 2: 0.45, 3: 0.65, 4: 0.85,
        5: 1.00, 6: 0.95, 7: 0.90, 8: 0.88,
        9: 0.82, 10: 0.70, 11: 0.50, 12: 0.35,
    }
    return rainfall[onset.month], temperature[onset.month]


def choose_disease(
    rng: random.Random,
    district: str,
    onset: date,
    climate_data: dict | None = None,
) -> str:
    """Choose a disease using seasonal and, when available, observed weather."""
    month = onset.month

    if climate_data is not None:
        climate = climate_data.get((district, onset))
    else:
        climate = None

    if climate is not None:
        lag_rain = climate["rain_lag_28_56"]
        lag_temp = climate["temp_lag_28_56"]
        recent_rain = climate["rain_recent_7"]
        threshold = climate["extreme_rain_threshold"]

        # Use neutral values during the initial period before a full lag exists.
        if pd.isna(lag_rain):
            lag_rain = 5.0
        if pd.isna(lag_temp):
            lag_temp = 27.0
        if pd.isna(recent_rain):
            recent_rain = 35.0

        # Smoothly scale observed climate values to avoid extreme probabilities.
        dengue_rain_effect = min(max(float(lag_rain), 0.0) / 10.0, 3.0)
        dengue_temp_effect = min(
            max(float(lag_temp) - 25.0, 0.0) / 5.0, 1.5
        )
        extreme_rain = (
            float(recent_rain) >= float(threshold)
            if not pd.isna(threshold)
            else False
        )
        diarrhoea_rain_effect = min(
            max(float(recent_rain), 0.0) / 20.0, 3.0
        )
    else:
        rainfall, temperature = get_climate_signals(onset)
        dengue_rain_effect = 2.5 * rainfall
        dengue_temp_effect = 1.5 * temperature
        diarrhoea_rain_effect = 1.8 * rainfall
        extreme_rain = False

    weights = {
        "dengue": 1.0 + dengue_rain_effect + dengue_temp_effect,
        "diarrhoea": (
            1.0 + diarrhoea_rain_effect + (2.0 if extreme_rain else 0.0)
        ),
        "influenza": 2.5 if month in (11, 12, 1, 2) else 1.0,
        "malaria": (
            1.0
            + (2.0 if district in ("D011", "D012", "D013") else 0.0)
            + 0.5 * min(dengue_rain_effect / 2.5, 1.0)
        ),
    }

    return rng.choices(
        list(weights),
        weights=list(weights.values()),
        k=1,
    )[0]


def generate_encounters(
    output_dir: Path,
    scale_factor: float,
    seed: int,
    weather_path: Path | None = DEFAULT_WEATHER_PATH,
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

    climate_data = None
    if weather_path is not None:
        climate_data = load_climate_data(weather_path)

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
    alternative_facilities = [
        facility for facility in facility_ids if facility != "F0001"
    ]

    disease_codes = config.DISEASE_CODES
    disease_names = list(disease_codes)

    # Build deterministic outbreak ground truth separately from encounters.
    catalog = build_outbreaks(
        start,
        end,
        district_ids,
        minimum=max(config.MINIMUM_OUTBREAKS, 30),
        seed=seed + 1000,
    )

    outbreak_rows = []
    for item in catalog:
        outbreak_rows.append({
            "outbreak_id": item["outbreak_id"],
            "disease": item["disease"],
            "district_id": item["district_id"],
            "start_date": item["start_date"].isoformat(),
            "end_date": item["end_date"].isoformat(),
            "severity": item["severity"],
            "outbreak_pattern": item["outbreak_pattern"],
        })

    with truth_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(outbreak_rows[0]))
        writer.writeheader()
        writer.writerows(outbreak_rows)

    # Index outbreaks by district. "ALL" outbreaks apply to every district.
    for item in outbreak_rows:
        item["_start"] = date.fromisoformat(item["start_date"])
        item["_end"] = date.fromisoformat(item["end_date"])

    # Build daily weights once; use cumulative weights for efficient sampling.
    incidence_rng = random.Random(seed + 2000)
    daily_weights = build_daily_incidence_weights(
        start, end, district_ids, outbreak_rows, incidence_rng, climate_data
    )
    cumulative_weights = list(accumulate(item["weight"] for item in daily_weights))
    total_weight = cumulative_weights[-1]

    fields = [
        "encounter_id", "patient_id", "facility_id", "district_id",
        "onset_date", "report_date", "age", "sex", "icd10_code",
        "lab_confirmed", "admission", "outcome",
    ]

    # Facility F0001 is silent from 1–21 June 2023, then backfills on 22 June.
    blackout_facility = "F0001"
    blackout_start = date(2023, 6, 1)
    blackout_end = date(2023, 6, 21)
    backfill_date = blackout_end + timedelta(days=1)

    with encounter_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        previous_row = None

        for i in range(total_records):
            # Exact duplicates, approximately 2% of generated rows.
            if i > 0 and i % 50 == 0:
                writer.writerow(previous_row)
                continue

            # Sample district/date using overdispersed daily incidence weights.
            draw = rng.random() * total_weight
            selected_index = bisect.bisect_left(cumulative_weights, draw)
            selected = daily_weights[min(selected_index, len(daily_weights) - 1)]
            onset = selected["onset_date"]
            district = selected["district_id"]
            disease = choose_disease(rng, district, onset, climate_data)

            # Apply the intensity of any outbreak active in this district/date.
            active = [
                item for item in outbreak_rows
                if item["_start"] <= onset <= item["_end"]
                and item["district_id"] in (district, "ALL")
            ]

            if active:
                intensity_weights = []
                for item in active:
                    intensity = outbreak_intensity(
                        onset,
                        item["_start"],
                        item["_end"],
                        item["outbreak_pattern"],
                    )
                    # A gamma-Poisson / negative-binomial draw adds
                    # overdispersion to the outbreak signal.
                    sampled_cases = negative_binomial_cases(
                        rng, mean_cases=max(intensity, 0.01), dispersion=2.0
                    )
                    intensity_weights.append(
                        max(intensity, 0.01) * (1.0 + min(sampled_cases, 10))
                    )

                if rng.random() < min(0.85, 0.20 * max(intensity_weights)):
                    outbreak = rng.choices(active, weights=intensity_weights, k=1)[0]
                    disease = outbreak["disease"]

            # The synthetic nationwide respiratory outbreak uses existing
            # influenza ICD-10 codes as a proxy; R99 is not used as a
            # respiratory diagnosis code.
            codes_for_disease = (
                config.DISEASE_CODES["influenza"]
                if disease == "novel_respiratory"
                else disease_codes[disease]
            )
            icd10_code = rng.choice(codes_for_disease)

            # Assign facility before determining the reporting date.
            if rng.random() < 0.005:
                facility_id = f"ORPHAN_{i}"
            elif onset.year == 2023 and rng.random() < 0.08:
                facility_id = blackout_facility
            else:
                facility_id = rng.choice(facility_ids)

            # Right-skewed reporting delay, capped at 21 days.
            delay = min(21, int(rng.expovariate(0.35)))
            initial_report_date = onset + timedelta(days=delay)

            # Fewer reports on weekends: shift some weekend reports forward.
            if initial_report_date.weekday() >= 5 and rng.random() < 0.25:
                delay = min(21, delay + rng.randint(1, 3))

            maximum_delay = min(21, (end - onset).days)
            delay = min(delay, maximum_delay)
            report_date = onset + timedelta(days=delay)

            # Cases with onset during the outage are batch-reported on 22 June.
            if (
                facility_id == blackout_facility
                and blackout_start <= onset <= blackout_end
            ):
                report_date = backfill_date

            # No F0001 reports are allowed during the blackout.
            elif (
                facility_id == blackout_facility
                and blackout_start <= report_date <= blackout_end
            ):
                if 0 <= (backfill_date - onset).days <= 21:
                    report_date = backfill_date
                else:
                    facility_id = rng.choice(alternative_facilities)

            # Invalid ages are intentionally injected for quality testing.
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
    parser.add_argument("--seed", type=int, default=config.RANDOM_SEED)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=config.SYNTHETIC_DIR,
    )
    parser.add_argument(
        "--weather-file",
        type=Path,
        default=DEFAULT_WEATHER_PATH,
        help="Daily observed weather CSV.",
    )
    args = parser.parse_args()

    generate_encounters(
        args.output_dir,
        args.scale_factor,
        args.seed,
        args.weather_file,
    )


if __name__ == "__main__":
    main()
