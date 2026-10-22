"""Tests for observed-weather features used by the encounter generator."""

from datetime import date, timedelta
import random

import pandas as pd
import pytest

from generator.generate_encounters import load_climate_data, choose_disease


def make_weather_csv(path):
    """Create predictable weather records for two districts."""
    rows = []
    start = date(2021, 1, 1)

    for district in ("D001", "D002"):
        for day_number in range(100):
            day = start + timedelta(days=day_number)
            rainfall = 100.0 if day_number == 70 else 1.0
            temperature = 30.0 if district == "D001" else 20.0

            rows.append({
                "district_id": district,
                "date": day.isoformat(),
                "rainfall_mm": rainfall,
                "temperature_c": temperature,
            })

    pd.DataFrame(rows).to_csv(path, index=False)


def test_weather_features_are_district_specific(tmp_path):
    path = tmp_path / "weather.csv"
    make_weather_csv(path)

    climate = load_climate_data(path)
    day = date(2021, 3, 15)

    assert ("D001", day) in climate
    assert ("D002", day) in climate
    assert climate[("D001", day)]["temperature_today"] == 30.0
    assert climate[("D002", day)]["temperature_today"] == 20.0


def test_duplicate_district_date_is_rejected(tmp_path):
    path = tmp_path / "weather.csv"
    make_weather_csv(path)

    weather = pd.read_csv(path)
    weather = pd.concat([weather, weather.iloc[[0]]], ignore_index=True)
    weather.to_csv(path, index=False)

    with pytest.raises(ValueError, match="duplicate"):
        load_climate_data(path)


def test_missing_climate_values_are_rejected(tmp_path):
    path = tmp_path / "weather.csv"
    make_weather_csv(path)

    weather = pd.read_csv(path)
    weather.loc[0, "rainfall_mm"] = None
    weather.to_csv(path, index=False)

    with pytest.raises(ValueError, match="missing or invalid"):
        load_climate_data(path)


def test_weather_based_disease_choice_is_reproducible():
    climate = {
        ("D001", date(2021, 6, 1)): {
            "rain_lag_28_56": 20.0,
            "temp_lag_28_56": 30.0,
            "rain_recent_7": 200.0,
            "rainfall_today": 10.0,
            "temperature_today": 30.0,
            "extreme_rain_threshold": 100.0,
        }
    }

    first = choose_disease(
        random.Random(42), "D001", date(2021, 6, 1), climate
    )
    second = choose_disease(
        random.Random(42), "D001", date(2021, 6, 1), climate
    )

    assert first == second
    assert first in {"dengue", "diarrhoea", "influenza", "malaria"}
