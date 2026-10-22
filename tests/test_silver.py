import json
import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("EpiScope-Silver-Tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_silver_module_can_be_imported():
    from pipeline.silver import process_silver
    assert callable(process_silver)


def test_silver_requires_patient_id_salt(monkeypatch, tmp_path):
    from pipeline.silver import process_silver

    monkeypatch.delenv("PATIENT_ID_SALT", raising=False)
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("EpiScope-Silver-Salt-Test")
        .getOrCreate()
    )
    try:
        with pytest.raises(ValueError, match="PATIENT_ID_SALT"):
            process_silver(
                spark, tmp_path / "bronze", tmp_path / "silver", "test"
            )
    finally:
        spark.stop()


def test_silver_validates_quarantines_and_hashes(spark, tmp_path, monkeypatch):
    from pipeline.silver import process_silver

    monkeypatch.setenv("PATIENT_ID_SALT", "test-only-salt-that-is-long-enough-123")

    bronze = tmp_path / "bronze"
    output = tmp_path / "silver"
    batch = "test"
    encounter_path = bronze / "encounters" / f"batch_id={batch}"
    facility_path = bronze / "facilities" / f"batch_id={batch}"

    rows = [
        ("E001", "PATIENT_RAW_001", "F001", "D001", "2023-01-01", "2023-01-03", 30, "F", "A90"),
        ("E002", "PATIENT_RAW_002", "F001", "D001", "2023-01-01", "2023-01-03", 150, "M", "A90"),
        ("E003", "PATIENT_RAW_003", "F999", "D001", "2023-01-01", "2023-01-03", 40, "F", "A90"),
        ("E004", "PATIENT_RAW_004", "F001", "D001", "2023-01-01", "2023-01-03", 25, "M", "A90"),
        ("E004", "PATIENT_RAW_005", "F001", "D001", "2023-01-01", "2023-01-03", 26, "F", "A90"),
    ]
    columns = [
        "encounter_id", "patient_id", "facility_id", "district_id",
        "onset_date", "report_date", "age_years", "sex", "icd10_code",
    ]
    encounters = spark.createDataFrame(rows, columns)
    encounters = (
        encounters
        .withColumn("_source_file", __import__("pyspark").sql.functions.lit("test.csv"))
        .withColumn("_batch_id", __import__("pyspark").sql.functions.lit(batch))
        .withColumn("_ingested_at", __import__("pyspark").sql.functions.lit("2023-01-04"))
    )
    facilities = spark.createDataFrame(
        [("F001",)], ["facility_id"]
    ).withColumn("_source_file", __import__("pyspark").sql.functions.lit("facilities.csv")) \
     .withColumn("_batch_id", __import__("pyspark").sql.functions.lit(batch)) \
     .withColumn("_ingested_at", __import__("pyspark").sql.functions.lit("2023-01-04"))

    encounters.write.mode("overwrite").parquet(str(encounter_path))
    facilities.write.mode("overwrite").parquet(str(facility_path))

    process_silver(spark, bronze, output, batch)

    target = output / f"batch_id={batch}"
    silver = spark.read.parquet(str(target / "encounters"))
    quarantine = spark.read.parquet(str(target / "quarantine"))

    assert silver.count() == 2
    assert quarantine.count() == 3
    assert "patient_id" not in silver.columns
    assert "patient_id_hash" in silver.columns

    hashes = [r["patient_id_hash"] for r in silver.select("patient_id_hash").collect()]
    assert all(h and len(h) == 64 for h in hashes)

    raw_ids = {"PATIENT_RAW_001", "PATIENT_RAW_004"}
    assert not any(raw in str(row.asDict()) for row in silver.collect() for raw in raw_ids)

    reasons = [
        reason
        for row in quarantine.select("quarantine_reasons").collect()
        for reason in row["quarantine_reasons"].split(",")
    ]
    assert "invalid_age" in reasons
    assert "unknown_facility" in reasons
    assert "duplicate_encounter_id" in reasons

    report = json.loads((target / "quality_report.json").read_text(encoding="utf-8"))
    assert report["input_rows"] == 5
    assert report["silver_rows"] == 2
    assert report["quarantined_rows"] == 3
    assert report["quarantine_reason_counts_are_non_exclusive"] is True


def test_silver_quarantines_unknown_district_and_icd10(spark, tmp_path, monkeypatch):
    from pipeline.silver import process_silver

    monkeypatch.setenv("PATIENT_ID_SALT", "test-only-salt-that-is-long-enough-123")

    bronze = tmp_path / "bronze"
    output = tmp_path / "silver"
    batch = "registry_test"
    encounter_path = bronze / "encounters" / f"batch_id={batch}"
    facility_path = bronze / "facilities" / f"batch_id={batch}"

    from pyspark.sql import functions as F

    rows = [
        ("E101", "P101", "F001", "D001", "2023-01-01", "2023-01-02", 30, "F", "A90"),
        ("E102", "P102", "F001", "D999", "2023-01-01", "2023-01-02", 30, "F", "A90"),
        ("E103", "P103", "F001", "D001", "2023-01-01", "2023-01-02", 30, "F", "ZZZ"),
    ]
    columns = [
        "encounter_id", "patient_id", "facility_id", "district_id",
        "onset_date", "report_date", "age_years", "sex", "icd10_code",
    ]

    encounters = spark.createDataFrame(rows, columns)
    encounters = (
        encounters
        .withColumn("_source_file", F.lit("test.csv"))
        .withColumn("_batch_id", F.lit(batch))
        .withColumn("_ingested_at", F.lit("2023-01-03"))
    )
    facilities = (
        spark.createDataFrame([("F001",)], ["facility_id"])
        .withColumn("_source_file", F.lit("facilities.csv"))
        .withColumn("_batch_id", F.lit(batch))
        .withColumn("_ingested_at", F.lit("2023-01-03"))
    )

    encounters.write.mode("overwrite").parquet(str(encounter_path))
    facilities.write.mode("overwrite").parquet(str(facility_path))

    process_silver(spark, bronze, output, batch)

    quarantine = spark.read.parquet(
        str(output / f"batch_id={batch}" / "quarantine")
    )
    reasons = {
        row["encounter_id"]: row["quarantine_reasons"]
        for row in quarantine.select("encounter_id", "quarantine_reasons").collect()
    }

    assert "unknown_district" in reasons["E102"]
    assert "invalid_icd10_code" in reasons["E103"]
    assert "E101" not in reasons
    assert "patient_id" not in quarantine.columns
    assert "age_years" not in quarantine.columns
