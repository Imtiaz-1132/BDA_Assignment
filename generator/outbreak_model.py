"""Outbreak patterns and overdispersed daily case counts for EpiScope."""

import math
import random
from datetime import date


def outbreak_intensity(day, start_date, end_date, pattern):
    """Return relative outbreak intensity for a day in an outbreak."""
    if day < start_date or day > end_date:
        return 0.0

    duration = max((end_date - start_date).days, 1)
    progress = (day - start_date).days / duration

    if pattern == "sharp":
        return 0.25 + 2.0 * math.exp(-((progress - 0.5) / 0.18) ** 2)

    if pattern == "slow_growth":
        return 0.25 + 2.0 * progress

    if pattern == "sustained":
        return 1.5

    if pattern == "nationwide_surge":
        if progress < 0.25:
            return 0.5 + 4.0 * progress
        if progress < 0.75:
            return 1.5
        return max(0.5, 1.5 - 4.0 * (progress - 0.75))

    raise ValueError(f"Unknown outbreak pattern: {pattern}")


def negative_binomial_cases(rng, mean_cases, dispersion=2.0):
    """Draw an overdispersed count using a Gamma-Poisson mixture."""
    if mean_cases < 0:
        raise ValueError("mean_cases must be non-negative")
    if dispersion <= 0:
        raise ValueError("dispersion must be positive")
    if mean_cases == 0:
        return 0

    latent_rate = rng.gammavariate(
        dispersion, mean_cases / dispersion
    )
    return _poisson_sample(rng, latent_rate)


def _poisson_sample(rng, rate):
    """Sample a Poisson count."""
    if rate <= 0:
        return 0

    if rate > 30:
        return max(0, round(rng.gauss(rate, math.sqrt(rate))))

    threshold = math.exp(-rate)
    product = 1.0
    count = 0

    while product > threshold:
        count += 1
        product *= rng.random()

    return count - 1
