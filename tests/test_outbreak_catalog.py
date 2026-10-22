from datetime import date
from generator.outbreak_catalog import build_outbreaks


def test_catalog_has_at_least_30_outbreaks():
    rows = build_outbreaks(
        date(2021, 1, 1), date(2025, 12, 31),
        [f"D{i:03d}" for i in range(1, 65)]
    )
    assert len(rows) >= 31
    assert any(r["outbreak_pattern"] == "slow_growth" for r in rows)
    assert any(r["outbreak_pattern"] == "nationwide_surge" for r in rows)


def test_catalog_is_reproducible():
    args = (
        date(2021, 1, 1), date(2025, 12, 31),
        [f"D{i:03d}" for i in range(1, 65)]
    )
    assert build_outbreaks(*args) == build_outbreaks(*args)
