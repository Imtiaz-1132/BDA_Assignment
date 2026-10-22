from datetime import date, timedelta
import random

import pytest

from generator.outbreak_model import (
    outbreak_intensity,
    negative_binomial_cases,
)


def test_intensity_is_zero_outside_period():
    start = date(2023, 1, 1)
    end = start + timedelta(days=20)

    assert outbreak_intensity(start - timedelta(days=1), start, end, "sharp") == 0.0
    assert outbreak_intensity(end + timedelta(days=1), start, end, "sharp") == 0.0


def test_slow_growth_increases_over_time():
    start = date(2023, 1, 1)
    end = start + timedelta(days=20)

    assert outbreak_intensity(end, start, end, "slow_growth") > outbreak_intensity(
        start, start, end, "slow_growth"
    )


def test_nationwide_surge_pattern():
    start = date(2023, 1, 1)
    end = start + timedelta(days=100)

    values = [
        outbreak_intensity(start + timedelta(days=i), start, end, "nationwide_surge")
        for i in range(101)
    ]

    assert max(values) > values[0]
    assert values[-1] < max(values)


def test_counts_are_reproducible_and_nonnegative():
    first_rng = random.Random(42)
    second_rng = random.Random(42)

    first = [negative_binomial_cases(first_rng, 10, 2) for _ in range(100)]
    second = [negative_binomial_cases(second_rng, 10, 2) for _ in range(100)]

    assert first == second
    assert all(count >= 0 for count in first)


def test_invalid_parameters_are_rejected():
    with pytest.raises(ValueError):
        negative_binomial_cases(random.Random(1), -1)

    with pytest.raises(ValueError):
        negative_binomial_cases(random.Random(1), 5, dispersion=0)
