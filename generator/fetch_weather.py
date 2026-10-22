
import argparse
import json
import time
from pathlib import Path

import pandas as pd
import requests

from generator import config

API_URL = "https://archive-api.open-meteo.com/v1/archive"
CENTROIDS_PATH = config.DATA_DIR / "geography" / "district_centroids.csv"
REQUEST_DELAY_SECONDS = 5
MAX_RETRIES = 6


def fetch_with_retries(session, params, district_id):
    """Fetch weather and retry politely when the API rate-limits requests."""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(API_URL, params=params, timeout=90)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_seconds = int(retry_after)
                else:
                    wait_seconds = min(30 * (attempt + 1), 180)

                print(
                    f"Rate limited for {district_id}; "
                    f"waiting {wait_seconds} seconds "
                    f"(attempt {attempt + 1}/{MAX_RETRIES})."
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            payload = response.json()

            if "daily" not in payload:
                raise ValueError(
                    f"No daily weather returned for {district_id}: {payload}"
                )

            return payload

        except (requests.Timeout, requests.ConnectionError) as exc:
            wait_seconds = min(10 * (attempt + 1), 60)
            print(
                f"Connection issue for {district_id}: {exc}. "
                f"Retrying in {wait_seconds} seconds."
            )
            time.sleep(wait_seconds)

    raise RuntimeError(
        f"Weather download failed for {district_id} after {MAX_RETRIES} attempts."
    )


def fetch_weather(
    centroids_path: Path,
    output_dir: Path,
    start_date: str = config.START_DATE,
    end_date: str = config.END_DATE,
    max_districts: int | None = None,
) -> Path:
    if not centroids_path.is_file():
        raise FileNotFoundError(
            f"District centroids CSV not found: {centroids_path}"
        )

    districts = pd.read_csv(centroids_path)
    required = {"district_id", "district_name", "latitude", "longitude"}

    if not required.issubset(districts.columns):
        raise ValueError(
            f"Centroids CSV must contain: {sorted(required)}"
        )

    if districts["district_id"].duplicated().any():
        raise ValueError("Duplicate district IDs found in centroid file")

    if districts[["latitude", "longitude"]].isna().any().any():
        raise ValueError("Missing district coordinates found")

    if not districts["district_id"].astype(str).str.fullmatch(
        r"D\d{3}"
    ).all():
        raise ValueError("District IDs must use the D001-D064 format")

    districts = districts.sort_values("district_id").reset_index(drop=True)

    if max_districts is not None:
        if max_districts < 1:
            raise ValueError("--max-districts must be at least 1")
        districts = districts.head(max_districts)

    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = output_dir / "raw_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    records = []
    session = requests.Session()

    for index, row in enumerate(districts.itertuples(index=False), start=1):
        district_id = str(row.district_id)
        cache_file = cache_dir / f"{district_id}_{start_date}_{end_date}.json"

        if cache_file.exists():
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
            print(
                f"[{index}/{len(districts)}] "
                f"Cached {district_id}: {row.district_name}"
            )
        else:
            params = {
                "latitude": float(row.latitude),
                "longitude": float(row.longitude),
                "start_date": start_date,
                "end_date": end_date,
                "daily": (
                    "precipitation_sum,temperature_2m_mean,"
                    "relative_humidity_2m_mean"
                ),
                "timezone": "Asia/Dhaka",
            }

            payload = fetch_with_retries(session, params, district_id)

            # Cache only a successful API response.
            cache_file.write_text(
                json.dumps(payload), encoding="utf-8"
            )
            print(
                f"[{index}/{len(districts)}] "
                f"Downloaded {district_id}: {row.district_name}"
            )
            time.sleep(REQUEST_DELAY_SECONDS)

        daily = payload.get("daily", {})
        dates = daily.get("time", [])
        rain = daily.get("precipitation_sum", [])
        temp = daily.get("temperature_2m_mean", [])
        humidity = daily.get("relative_humidity_2m_mean", [])

        if (
            not dates
            or len(dates) != len(rain)
            or len(dates) != len(temp)
        ):
            raise ValueError(
                f"Incomplete daily weather response for {district_id}"
            )

        if humidity and len(dates) != len(humidity):
            raise ValueError(
                f"Humidity array length mismatch for {district_id}"
            )

        for i, day in enumerate(dates):
            records.append({
                "district_id": district_id,
                "district_name": row.district_name,
                "date": day,
                "rainfall_mm": rain[i],
                "temperature_c": temp[i],
                "humidity_pct": humidity[i] if humidity else None,
                "latitude": float(row.latitude),
                "longitude": float(row.longitude),
                "source": "Open-Meteo Historical Weather API",
            })

    output_path = output_dir / "observed_weather.csv"
    pd.DataFrame(records).to_csv(output_path, index=False)

    print(f"Districts downloaded/read: {len(districts)}")
    print(f"Weather rows: {len(records):,}")
    print(f"Weather CSV: {output_path}")
    print(f"Raw API cache: {cache_dir}")

    return output_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--centroids", type=Path, default=CENTROIDS_PATH)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=config.DATA_DIR / "weather",
    )
    parser.add_argument("--start-date", default=config.START_DATE)
    parser.add_argument("--end-date", default=config.END_DATE)
    parser.add_argument("--max-districts", type=int, default=None)
    args = parser.parse_args()

    fetch_weather(
        args.centroids,
        args.output_dir,
        args.start_date,
        args.end_date,
        args.max_districts,
    )


if __name__ == "__main__":
    main()
