from datetime import date, timedelta

from generator.outbreak_model import outbreak_intensity, negative_binomial_cases


def build_daily_incidence_weights(
    start_date: date,
    end_date: date,
    district_ids: list[str],
    outbreaks: list[dict],
    rng,
    climate_data: dict | None = None,
) -> list[dict]:
    """Build reproducible, overdispersed weights for district-day sampling."""
    weights = []
    day_count = (end_date - start_date).days + 1

    for district in district_ids:
        for offset in range(day_count):
            day = start_date + timedelta(days=offset)

            # Background incidence, modulated by the existing climate-based
            # disease-selection logic elsewhere in the generator.
            mean_cases = 1.0
            active = [
                outbreak for outbreak in outbreaks
                if outbreak["_start"] <= day <= outbreak["_end"]
                and outbreak["district_id"] in (district, "ALL")
            ]

            for outbreak in active:
                intensity = outbreak_intensity(
                    day,
                    outbreak["_start"],
                    outbreak["_end"],
                    outbreak["outbreak_pattern"],
                )
                mean_cases += 3.0 * intensity

            # Gamma-Poisson draw: overdispersed daily incidence weight.
            sampled = negative_binomial_cases(
                rng, mean_cases=mean_cases, dispersion=2.0
            )
            weights.append({
                "district_id": district,
                "onset_date": day,
                "weight": max(sampled, 0.05),
            })

    if not weights or sum(item["weight"] for item in weights) <= 0:
        raise ValueError("Daily incidence weights are empty or invalid.")

    return weights
