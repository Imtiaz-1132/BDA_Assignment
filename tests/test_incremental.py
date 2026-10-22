from pipeline.incremental import (
    batch_is_complete,
    mark_batch_complete,
)


def test_incremental_batch_ledger_is_idempotent(tmp_path):
    ledger = tmp_path / "processed_batches.json"

    assert not batch_is_complete(ledger, "day_001")

    mark_batch_complete(
        ledger, "day_001", 12, ["2023-01-02", "2023-01-09"]
    )
    assert batch_is_complete(ledger, "day_001")

    # Re-recording the same batch must not create a second batch entry.
    mark_batch_complete(ledger, "day_001", 12, ["2023-01-02"])
    assert batch_is_complete(ledger, "day_001")

    import json
    data = json.loads(ledger.read_text(encoding="utf-8"))
    assert list(data["completed_batches"]) == ["day_001"]
    assert data["completed_batches"]["day_001"]["affected_weeks"] == [
        "2023-01-02"
    ]


def test_incremental_ledger_rejects_invalid_batch_id(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="batch_id"):
        mark_batch_complete(tmp_path / "ledger.json", "../bad", 1)


def test_incremental_reports_affected_weeks_for_recomputation(tmp_path):
    from pipeline.incremental import pending_affected_weeks

    ledger = tmp_path / "processed_batches.json"
    mark_batch_complete(ledger, "batch_a", 10, ["2023-01-02", "2023-01-09"])
    mark_batch_complete(ledger, "batch_b", 5, ["2023-01-09", "2023-01-16"])

    assert pending_affected_weeks(ledger) == [
        "2023-01-02", "2023-01-09", "2023-01-16"
    ]
