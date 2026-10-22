import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_gold_aggregates_diseases_and_flags_missing_facility_weeks(tmp_path):
    from pipeline.gold import process_gold

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("EpiScope-Gold-Integration-Test")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        batch = "gold_test"
        silver_dir = tmp_path / "silver"
        output_dir = tmp_path / "gold"
        facilities_path = tmp_path / "facilities.csv"
        input_path = silver_dir / f"batch_id={batch}" / "encounters"

        rows = [
            ("E1", "HASH1", "F1", "D1", "2023-01-02", "2023-01-04", "A90", True, True, "recovered", "gold_test"),
            ("E2", "HASH2", "F2", "D1", "2023-01-03", "2023-01-05", "A91", False, False, "outpatient", "gold_test"),
            ("E3", "HASH3", "F3", "D2", "2023-01-10", "2023-01-12", "J10", True, True, "deceased", "gold_test"),
        ]
        columns = [
            "encounter_id", "patient_id_hash", "facility_id", "district_id",
            "onset_date", "report_date", "icd10_code", "lab_confirmed",
            "admitted", "outcome", "_batch_id",
        ]

        spark.createDataFrame(rows, columns).write.mode("overwrite").parquet(
            str(input_path)
        )

        facilities = spark.createDataFrame(
            [("F1", "D1"), ("F2", "D1"), ("F3", "D2"), ("F4", "D2")],
            ["facility_id", "district_id"],
        )
        facilities.write.mode("overwrite").option("header", True).csv(
            str(tmp_path / "facilities_dir")
        )
        # Spark writes CSV as a directory; pass its actual directory path.
        facilities_path = tmp_path / "facilities_dir"

        process_gold(spark, silver_dir, output_dir, batch, facilities_path)

        target = output_dir / f"batch_id={batch}"
        weekly = spark.read.parquet(str(target / "weekly_district_disease"))
        facility = spark.read.parquet(str(target / "weekly_facility_reporting"))

        assert "source_batch_ids" in weekly.columns
        assert weekly.first()["source_batch_ids"] == ["gold_test"]

        dengue = weekly.filter(F.col("disease") == "dengue")
        assert dengue.count() == 1
        dengue_row = dengue.first()
        assert dengue_row["district_id"] == "D1"
        assert dengue_row["case_count"] == 2
        assert dengue_row["distinct_patients"] == 2
        assert dengue_row["admission_count"] == 1

        assert weekly.filter(F.col("disease") == "influenza").count() == 1
        assert weekly.filter(F.col("disease") == "unknown").count() == 0

        missing_report = facility.filter(F.col("facility_id") == "F4")
        assert missing_report.count() > 0
        assert missing_report.filter(F.col("potential_reporting_gap")).count() > 0
        assert missing_report.filter(F.col("reported_case_count") == 0).count() > 0

        report = json.loads(
            (target / "quality_report.json").read_text(encoding="utf-8")
        )
        assert report["input_rows"] == 3
        assert report["ground_truth_used"] is False
        assert report["unknown_icd10_code_counts"] == {}
    finally:
        spark.stop()
