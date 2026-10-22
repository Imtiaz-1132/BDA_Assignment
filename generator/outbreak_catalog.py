"""Build reproducible outbreak definitions for EpiScope."""

import random
from datetime import date, timedelta

PATTERNS = ("sharp", "slow_growth", "sustained")


def build_outbreaks(start_date, end_date, district_ids, minimum=30, seed=1042):
    """Return outbreak definitions, including a nationwide respiratory surge."""
    if minimum < 30:
        minimum = 30
    if not district_ids:
        raise ValueError("district_ids cannot be empty")
    if end_date <= start_date:
        raise ValueError("end_date must be after start_date")

    rng = random.Random(seed)
    outbreaks = []
    span = (end_date - start_date).days

    for index in range(1, minimum + 1):
        disease = ("dengue", "diarrhoea", "influenza", "malaria")[
            (index - 1) % 4
        ]
        pattern = PATTERNS[(index - 1) % len(PATTERNS)]
        duration = rng.randint(14, 42)
        latest_offset = max(0, span - duration)
        offset = rng.randint(0, latest_offset)
        onset = start_date + timedelta(days=offset)

        outbreaks.append({
            "outbreak_id": f"OB{index:03d}",
            "disease": disease,
            "district_id": district_ids[(index * 7) % len(district_ids)],
            "start_date": onset,
            "end_date": min(onset + timedelta(days=duration), end_date),
            "severity": rng.choice(("low", "medium", "high")),
            "outbreak_pattern": pattern,
        })

    # Explicit nationwide novel-respiratory surge.
    surge_start = date(2024, 10, 1)
    surge_end = date(2025, 3, 31)
    surge_start = max(start_date, surge_start)
    surge_end = min(end_date, surge_end)

    if surge_start <= surge_end:
        outbreaks.append({
            "outbreak_id": f"OB{minimum + 1:03d}",
            "disease": "novel_respiratory",
            "district_id": "ALL",
            "start_date": surge_start,
            "end_date": surge_end,
            "severity": "high",
            "outbreak_pattern": "nationwide_surge",
        })

    return outbreaks
