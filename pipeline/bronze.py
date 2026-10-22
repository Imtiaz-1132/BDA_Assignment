
"""Bronze ingestion for EpiScope source files.

Preserves source CSV records in Parquet with provenance metadata.
Required synthetic sources remain mandatory. Geography, weather,
population, and GeoJSON sources are ingested when available.
"""

import argparse
import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit


# Required synthetic source files.
SOURCES = ("encounters", "facilities", "resources")

# Optional source CSVs, relative to extra_source_dir.
EXTRA_CSV_SOURCES = {
    "district_centroids": Path("geography/district_centroids.csv"),
    "observed_weather": Path("weather/observed_weather.csv"),
    "district_population": Path("population/district_population_2022.csv"),
}


def _write_csv_bronze(
    spark: SparkSession,
    csv_path: Path,
    target: Path,
    batch_id: str,
) -> None:
    """Ingest a CSV into Parquet with source provenance metadata."""
    csv_path = Path(csv_path)
    checksum = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    source_load_time = datetime.now(timezone.utc).isoformat()

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .csv(str(csv_path))
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_load_time", lit(source_load_time))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("source_checksum_sha256", lit(checksum))
    )

    df.write.mode("overwrite").parquet(str(target))
    row_count = df.count()

    print(
        f"Bronze {csv_path.name}: {row_count:,} rows -> {target}"
    )


def _write_geojson_bronze(
    source_path: Path,
    target: Path,
    batch_id: str,
) -> None:
    """Preserve original GeoJSON bytes and record provenance."""
    source_path = Path(source_path)
    checksum = hashlib.sha256(source_path.read_bytes()).hexdigest()
    target.mkdir(parents=True, exist_ok=True)

    # Preserve the source without modifying its contents.
    shutil.copyfile(source_path, target / source_path.name)

    metadata = {
        "source_file": str(source_path),
        "source_load_time_utc": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "source_checksum_sha256": checksum,
    }

    (target / "_metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    print(f"Bronze {source_path.name}: preserved original -> {target}")


def ingest_bronze(
    spark: SparkSession,
    input_dir: Path,
    output_dir: Path,
    batch_id: str,
    extra_source_dir: Path | None = None,
) -> None:
    """Ingest required CSVs and available extra sources into Bronze."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch_id):
        raise ValueError(
            "batch_id may contain only letters, digits, underscores, and hyphens"
        )

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    extra_source_dir = (
        Path(extra_source_dir)
        if extra_source_dir is not None
        else input_dir
    )

    # The three existing synthetic sources are mandatory.
    for source in SOURCES:
        csv_path = input_dir / f"{source}.csv"

        if not csv_path.is_file():
            raise FileNotFoundError(
                f"Required source file not found: {csv_path}"
            )

        target = output_dir / source / f"batch_id={batch_id}"
        _write_csv_bronze(spark, csv_path, target, batch_id)

    # Optional geography, weather, and population CSV sources.
    for source, relative_path in EXTRA_CSV_SOURCES.items():
        csv_path = extra_source_dir / relative_path

        if not csv_path.is_file():
            print(f"Optional source not found; skipping: {csv_path}")
            continue

        target = output_dir / source / f"batch_id={batch_id}"
        _write_csv_bronze(spark, csv_path, target, batch_id)

    # Preserve original Bangladesh ADM2 boundaries when available.
    geojson_path = (
        extra_source_dir / "geography" / "bangladesh_adm2.geojson"
    )

    if geojson_path.is_file():
        target = output_dir / "geography_geojson" / f"batch_id={batch_id}"
        _write_geojson_bronze(geojson_path, target, batch_id)
    else:
        print(f"Optional GeoJSON not found; skipping: {geojson_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument(
        "--extra-source-dir",
        type=Path,
        default=None,
        help=(
            "Root directory containing geography/, weather/, "
            "and population/ source folders."
        ),
    )
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
            extra_source_dir=args.extra_source_dir,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
