import pandas as pd
import pytest

from generator.weather_ingestion import validate_weather


def test_weather_validation_removes_invalid_and_duplicate_rows(tmp_path):
    source = tmp_path / "weather.csv"
    output = tmp_path / "clean_weather.csv"

    pd.DataFrame([
        {"district_id": "D001", "date": "2023-01-01", "rainfall_mm": 4, "temperature_c": 25},
        {"district_id": "D001", "date": "2023-01-01", "rainfall_mm": 5, "temperature_c": 26},
        {"district_id": "D002", "date": "2023-01-02", "rainfall_mm": -1, "temperature_c": 25},
    ]).to_csv(source, index=False)

    count = validate_weather(source, output)

    assert count == 1
    clean = pd.read_csv(output)
    assert len(clean) == 1
    assert clean.iloc[0]["district_id"] == "D001"
    assert clean.iloc[0]["rainfall_mm"] == 5


def test_weather_validation_rejects_missing_columns(tmp_path):
    source = tmp_path / "weather.csv"
    source.write_text("district_id,date\nD001,2023-01-01\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing columns"):
        validate_weather(source, tmp_path / "clean.csv")


def test_weather_validation_rejects_empty_valid_dataset(tmp_path):
    source = tmp_path / "weather.csv"
    pd.DataFrame([
        {"district_id": "D001", "date": "2023-01-01", "rainfall_mm": -2, "temperature_c": 25},
    ]).to_csv(source, index=False)

    with pytest.raises(ValueError, match="No valid weather records"):
        validate_weather(source, tmp_path / "clean.csv")
