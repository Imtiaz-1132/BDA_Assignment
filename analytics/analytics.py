"""EpiScope analytics: outbreak detection, forecasting, and resource planning."""

import pandas as pd


def detect_outbreaks(data, window=4, threshold=2.0):
    """Flag district-disease weeks that exceed recent baseline."""
    required = {"epi_week_start", "district_id", "disease", "case_count"}
    if not required.issubset(data.columns):
        raise ValueError(f"Missing columns: {sorted(required - set(data.columns))}")

    df = data.copy()
    df["epi_week_start"] = pd.to_datetime(df["epi_week_start"])
    df["case_count"] = pd.to_numeric(df["case_count"], errors="raise")
    df = df.sort_values(["district_id", "disease", "epi_week_start"])

    grouped = df.groupby(["district_id", "disease"], group_keys=False)
    df["baseline"] = grouped["case_count"].transform(
        lambda s: s.shift(1).rolling(window, min_periods=2).mean()
    )
    df["outbreak_flag"] = (
        df["baseline"].notna()
        & (df["case_count"] >= threshold * df["baseline"])
        & (df["case_count"] > 0)
    )
    return df


def forecast_next_week(data):
    """Forecast next-week cases from the latest four valid observations."""
    required = {"epi_week_start", "district_id", "disease", "case_count"}
    if not required.issubset(data.columns):
        raise ValueError(f"Missing columns: {sorted(required - set(data.columns))}")

    df = data.copy()
    df["epi_week_start"] = pd.to_datetime(df["epi_week_start"])
    df["case_count"] = pd.to_numeric(df["case_count"], errors="raise")
    df = df.sort_values("epi_week_start")

    rows = []
    for (district, disease), group in df.groupby(["district_id", "disease"]):
        recent = group.dropna(subset=["case_count"]).tail(4)
        forecast = (
            max(0, round(recent["case_count"].mean()))
            if not recent.empty
            else 0
        )
        rows.append({
            "district_id": district,
            "disease": disease,
            "forecast_cases": forecast,
            "based_on_weeks": len(recent),
        })
    return pd.DataFrame(rows)

def estimate_resource_demand(forecasts, resources_per_case=0.2):
    """Estimate resource units from forecast case counts."""
    if resources_per_case < 0:
        raise ValueError("resources_per_case must be non-negative")
    required = {"district_id", "disease", "forecast_cases"}
    if not required.issubset(forecasts.columns):
        raise ValueError(f"Missing columns: {sorted(required - set(forecasts.columns))}")

    result = forecasts.copy()
    result["estimated_demand"] = (
        pd.to_numeric(result["forecast_cases"], errors="raise")
        .clip(lower=0)
        .mul(resources_per_case)
        .apply(lambda x: int(-(-x // 1)))
    )
    return result


def optimize_allocation(demand, available_resources):
    """Allocate limited resources to districts with highest estimated demand."""
    if available_resources < 0:
        raise ValueError("available_resources must be non-negative")
    required = {"district_id", "estimated_demand"}
    if not required.issubset(demand.columns):
        raise ValueError(f"Missing columns: {sorted(required - set(demand.columns))}")

    result = (
        demand.groupby("district_id", as_index=False)["estimated_demand"]
        .sum()
        .sort_values(["estimated_demand", "district_id"], ascending=[False, True])
    )
    remaining = int(available_resources)
    allocations = []
    for row in result.itertuples(index=False):
        demand_units = max(0, int(row.estimated_demand))
        allocated = min(demand_units, remaining)
        allocations.append(allocated)
        remaining -= allocated

    result["allocated_resources"] = allocations
    result["unmet_demand"] = result["estimated_demand"] - result["allocated_resources"]
    return result



def evaluate_forecast_accuracy(actual, predicted):
    """Evaluate forecasts using MAE and RMSE."""
    import math

    try:
        actual_values = [float(value) for value in actual]
        predicted_values = [float(value) for value in predicted]
    except (TypeError, ValueError) as exc:
        raise ValueError("Actual and predicted values must be numeric sequences") from exc

    if not actual_values or not predicted_values:
        raise ValueError("Actual and predicted values must not be empty")
    if len(actual_values) != len(predicted_values):
        raise ValueError("Actual and predicted sequences must have equal lengths")
    if not all(math.isfinite(value) for value in actual_values + predicted_values):
        raise ValueError("Values must be finite numbers")
    if any(value < 0 for value in actual_values + predicted_values):
        raise ValueError("Case counts must be non-negative")

    errors = [
        actual_value - predicted_value
        for actual_value, predicted_value in zip(actual_values, predicted_values)
    ]
    mae = sum(abs(error) for error in errors) / len(errors)
    rmse = math.sqrt(sum(error ** 2 for error in errors) / len(errors))

    return {"mae": mae, "rmse": rmse}
