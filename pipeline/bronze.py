"""Bronze ingestion for EpiScope source CSV files."""

import argparse
import hashlib
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit


SOURCES = ("encounters", "facilities", "resources")


def ingest_bronze(
    spark: SparkSession,
    input_dir: Path,
    output_dir: Path,
    batch_id: str,
) -> None:
    """Ingest source CSV files into separate, rerunnable Parquet batches."""

    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError("batch_id may contain only letters, digits, _ and -")

    for source in SOURCES:
        csv_path = input_dir / f"{source}.csv"

        if not csv_path.is_file():
            raise FileNotFoundError(f"Source file not found: {csv_path}")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("encoding", "UTF-8")
            .csv(str(csv_path))
        )

        # Compute the checksum from the original source bytes before ingestion.
        checksum = hashlib.sha256(csv_path.read_bytes()).hexdigest()
        target = output_dir / source / f"batch_id={batch_id}"

        # Same batch ID is intentionally rerunnable: replace only this
        # source's batch. Other batch IDs remain isolated.

        df = (
            df.withColumn("_source_file", input_file_name())
            .withColumn("_source_load_time", current_timestamp())
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_batch_id", lit(batch_id))
            .withColumn("source_checksum_sha256", lit(checksum))
        )

        (
            df.write
            .mode("overwrite")
            .parquet(str(target))
        )

        print(
            f"Bronze {source}: {df.count():,} rows "
            f"-> {target}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest EpiScope CSV sources into the Bronze layer."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/synthetic"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/lakehouse/bronze"),
    )
    parser.add_argument("--batch-id", default="initial")

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("EpiScope-Bronze-Ingestion")
        .getOrCreate()
    )

    try:
        ingest_bronze(
            spark,
            args.input_dir,
            args.output_dir,
            args.batch_id,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
