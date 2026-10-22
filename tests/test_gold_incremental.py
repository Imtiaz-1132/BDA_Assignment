from pyspark.sql import SparkSession

from pipeline.gold import process_gold


def _silver_row(encounter_id, onset_date, batch_id, patient_id):
    return {
        "encounter_id": encounter_id,
        "patient_id_hash": patient_id,
        "facility_id": "F001",
        "district_id": "D001",
        "onset_date": onset_date,
        "report_date": onset_date,
        "icd10_code": "A90",
        "lab_confirmed": "true",
        "admitted": "false",
        "outcome": "recovered",
        "_batch_id": batch_id,
    }


def test_incremental_gold_recomputes_only_affected_week(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("EpiScope-Incremental-Gold-Test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        silver_dir = tmp_path / "silver"
        gold_dir = tmp_path / "gold"
        facilities_path = tmp_path / "facilities.csv"

        facilities_path.write_text(
            "facility_id,district_id\nF001,D001\n",
            encoding="utf-8",
        )

        # Batch A contains cases in two different weeks.
        batch_a = [
            _silver_row("E001", "2023-01-03", "batch_a", "P001"),
            _silver_row("E002", "2023-01-10", "batch_a", "P002"),
        ]

        batch_a_path = silver_dir / "batch_id=batch_a" / "encounters"
        batch_a_path.mkdir(parents=True)
        spark.createDataFrame(batch_a).write.mode("overwrite").parquet(
            str(batch_a_path)
        )

        # Build Gold for both weeks.
        process_gold(
            spark,
            silver_dir,
            gold_dir,
            "initial_build",
            facilities_path,
            all_batches=True,
            affected_weeks=["2023-01-02", "2023-01-09"],
            shared_output=True,
        )

        # Batch B adds a late case to the first week.
        batch_b = [
            _silver_row("E003", "2023-01-04", "batch_b", "P003"),
        ]

        batch_b_path = silver_dir / "batch_id=batch_b" / "encounters"
        batch_b_path.mkdir(parents=True)
        spark.createDataFrame(batch_b).write.mode("overwrite").parquet(
            str(batch_b_path)
        )

        # Recompute only the first week using both batches.
        process_gold(
            spark,
            silver_dir,
            gold_dir,
            "late_batch",
            facilities_path,
            all_batches=True,
            affected_weeks=["2023-01-02"],
            shared_output=True,
        )

        result = spark.read.parquet(
            str(gold_dir / "weekly_district_disease")
        )

        week_one = result.filter(
            result.epi_week_start == "2023-01-02"
        ).collect()

        week_two = result.filter(
            result.epi_week_start == "2023-01-09"
        ).collect()

        assert len(week_one) == 1
        assert week_one[0]["case_count"] == 2
        assert set(week_one[0]["source_batch_ids"]) == {
            "batch_a", "batch_b"
        }

        # The unaffected week must remain unchanged.
        assert len(week_two) == 1
        assert week_two[0]["case_count"] == 1
        assert week_two[0]["source_batch_ids"] == ["batch_a"]

    finally:
        spark.stop()
