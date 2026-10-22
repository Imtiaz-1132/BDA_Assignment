import pytest
from pyspark.sql import SparkSession

from pipeline.bronze import ingest_bronze


def test_bronze_batches_are_isolated_and_rerunnable(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("EpiScope-Bronze-Tests")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        source_dir = tmp_path / "sources"
        output_dir = tmp_path / "bronze"
        source_dir.mkdir()

        (source_dir / "encounters.csv").write_text(
            "encounter_id,patient_id\nE1,P1\n",
            encoding="utf-8",
        )
        (source_dir / "facilities.csv").write_text(
            "facility_id,district_id\nF1,D1\n",
            encoding="utf-8",
        )
        (source_dir / "resources.csv").write_text(
            "facility_id,snapshot_date\nF1,2023-01-01\n",
            encoding="utf-8",
        )

        ingest_bronze(spark, source_dir, output_dir, "batch_a")

        first_path = output_dir / "encounters" / "batch_id=batch_a"
        first = spark.read.parquet(str(first_path))
        assert first.count() == 1

        # A separate batch must not overwrite the first batch.
        (source_dir / "encounters.csv").write_text(
            "encounter_id,patient_id\nE2,P2\n",
            encoding="utf-8",
        )
        ingest_bronze(spark, source_dir, output_dir, "batch_b")

        second_path = output_dir / "encounters" / "batch_id=batch_b"
        second = spark.read.parquet(str(second_path))
        assert second.count() == 1
        assert first.count() == 1
        assert first.first()["encounter_id"] == "E1"
        assert second.first()["encounter_id"] == "E2"

        # Re-running batch_b replaces that batch with its current source.
        (source_dir / "encounters.csv").write_text(
            "encounter_id,patient_id\nE3,P3\nE4,P4\n",
            encoding="utf-8",
        )
        ingest_bronze(spark, source_dir, output_dir, "batch_b")

        second_rerun = spark.read.parquet(str(second_path))
        assert second_rerun.count() == 2
        assert first.count() == 1

        with pytest.raises(ValueError, match="batch_id"):
            ingest_bronze(spark, source_dir, output_dir, "../unsafe")
    finally:
        spark.stop()
