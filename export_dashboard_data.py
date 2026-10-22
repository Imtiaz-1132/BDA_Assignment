import json
from pathlib import Path
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EpiScopeDashboardExport")
    .master("local[2]")
    .getOrCreate()
)

source = "data/pipeline_test/lakehouse/gold/weekly_district_disease"
output = Path("dashboard/data/weekly_district_disease.json")
output.parent.mkdir(parents=True, exist_ok=True)

df = spark.read.parquet(source)

columns = [
    "epi_week_start",
    "district_id",
    "disease",
    "case_count",
    "distinct_patients",
    "admission_count",
    "death_count",
    "lab_confirmed_count",
    "mean_reporting_delay_days",
]

rows = df.select(*columns).orderBy(
    "epi_week_start", "district_id", "disease"
).toJSON().collect()

with output.open("w", encoding="utf-8") as f:
    f.write("[\n")
    f.write(",\n".join(rows))
    f.write("\n]\n")

print(f"Exported {len(rows):,} records to {output}")
print(f"File size: {output.stat().st_size:,} bytes")

spark.stop()
