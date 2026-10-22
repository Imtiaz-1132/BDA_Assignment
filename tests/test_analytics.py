import pandas as pd
import pytest

from analytics.analytics import (
    detect_outbreaks,
    forecast_next_week,
    estimate_resource_demand,
    optimize_allocation,
)


def sample_data():
    return pd.DataFrame({
        "epi_week_start": pd.date_range("2026-01-05", periods=5, freq="W-MON"),
        "district_id": ["D1"] * 5,
        "disease": ["dengue"] * 5,
        "case_count": [10, 12, 11, 10, 40],
    })


def test_outbreak_detection_flags_spike():
    result = detect_outbreaks(sample_data())
    assert bool(result.iloc[-1]["outbreak_flag"]) is True


def test_forecast_returns_recent_average():
    result = forecast_next_week(sample_data())
    assert result.iloc[0]["forecast_cases"] == 18


def test_resource_demand_estimation():
    forecasts = pd.DataFrame({
        "district_id": ["D1", "D2"],
        "disease": ["dengue", "influenza"],
        "forecast_cases": [10, 20],
    })
    result = estimate_resource_demand(forecasts, resources_per_case=0.2)
    assert result["estimated_demand"].tolist() == [2, 4]


def test_allocation_respects_resource_limit():
    demand = pd.DataFrame({
        "district_id": ["D1", "D2"],
        "estimated_demand": [5, 3],
    })
    result = optimize_allocation(demand, available_resources=6)
    assert result["allocated_resources"].sum() == 6
    assert (result["unmet_demand"] >= 0).all()


def test_negative_resource_supply_rejected():
    demand = pd.DataFrame({
        "district_id": ["D1"],
        "estimated_demand": [2],
    })
    with pytest.raises(ValueError):
        optimize_allocation(demand, available_resources=-1)
