
"""Automated tests for EpiScope synthetic data generators."""

import csv
from pathlib import Path

from generator.generate_encounters import generate_encounters
from generator.generate_facilities import generate_facilities
from generator.generate_resources import generate_resources


def read_csv(path: Path):
    """Read a CSV file and return its rows."""
    with path.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def test_encounter_generator(tmp_path):
    """Check encounter schema, dates, and injected quality issues."""
    generate_encounters(tmp_path, scale_factor=0.001, seed=42)

    path = tmp_path / "encounters.csv"
    rows = read_csv(path)

    assert len(rows) == 25_000

    required_fields = {
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
    }

    assert set(rows[0].keys()) == required_fields

    # Intentional duplicate records must be exact copies.
    assert rows[49] == rows[50]

    # Date order and reporting delay must be valid.
    for row in rows:
        assert row["onset_date"] <= row["report_date"]

        from datetime import date

        onset = date.fromisoformat(row["onset_date"])
        report = date.fromisoformat(row["report_date"])
        assert 0 <= (report - onset).days <= 21

    assert (tmp_path / "ground_truth_outbreaks.csv").exists()


def test_facility_generator(tmp_path):
    """Check facility count, ID uniqueness, and district coverage."""
    generate_facilities(tmp_path, seed=42)

    rows = read_csv(tmp_path / "facilities.csv")

    assert len(rows) == 600

    facility_ids = [row["facility_id"] for row in rows]
    assert len(facility_ids) == len(set(facility_ids))

    districts = {row["district_id"] for row in rows}
    assert len(districts) == 64

    for row in rows:
        assert row["facility_id"]
        assert row["facility_name"]
        assert row["facility_type"]
        assert row["latitude"]
        assert row["longitude"]


def test_resource_generator(tmp_path):
    """Check resource values and daily facility coverage."""
    generate_resources(tmp_path, seed=42)

    rows = read_csv(tmp_path / "resources.csv")

    # 2021-2025 contains 1,826 days, with 600 facilities per day.
    assert len(rows) == 1_095_600

    for row in rows:
        available = int(row["beds_available"])
        occupied = int(row["beds_occupied"])
        saline = int(row["iv_saline_stock"])
        kits = int(row["test_kit_stock"])

        assert available >= 0
        assert 0 <= occupied <= available
        assert saline >= 0
        assert kits >= 0
