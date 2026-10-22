"""Validate and standardize observed weather data supplied as a CSV."""

import argparse
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {
    "district_id",
    "date",
    "rainfall_mm",
    "temperature_c",
}


def validate_weather(input_path: Path, output_path: Path) -> int:
    if not input_path.is_file():
        raise FileNotFoundError(f"Weather CSV not found: {input_path}")

    weather = pd.read_csv(input_path)

    missing = REQUIRED_COLUMNS - set(weather.columns)
    if missing:
        raise ValueError(f"Weather CSV is missing columns: {sorted(missing)}")

    weather = weather[list(sorted(REQUIRED_COLUMNS))].copy()
    weather["district_id"] = weather["district_id"].astype("string").str.strip()
    weather["date"] = pd.to_datetime(weather["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    weather["rainfall_mm"] = pd.to_numeric(weather["rainfall_mm"], errors="coerce")
    weather["temperature_c"] = pd.to_numeric(weather["temperature_c"], errors="coerce")

    invalid = (
        weather["district_id"].isna()
        | weather["district_id"].eq("")
        | weather["date"].isna()
        | weather["rainfall_mm"].isna()
        | weather["temperature_c"].isna()
        | weather["rainfall_mm"].lt(0)
        | weather["temperature_c"].lt(-10)
        | weather["temperature_c"].gt(60)
    )

    rejected = int(invalid.sum())
    clean = weather.loc[~invalid].drop_duplicates(
        subset=["district_id", "date"], keep="last"
    )

    if clean.empty:
        raise ValueError("No valid weather records remain after validation.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    clean.sort_values(["district_id", "date"]).to_csv(output_path, index=False)

    print(f"Valid weather records: {len(clean):,}")
    print(f"Rejected records: {rejected:,}")
    print(f"Output: {output_path}")
    return len(clean)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/weather/observed_weather.csv"),
    )
    args = parser.parse_args()
    validate_weather(args.input, args.output)


if __name__ == "__main__":
    main()
