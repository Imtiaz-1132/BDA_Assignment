# EpiScope — Bangladesh Disease Surveillance

EpiScope is a Big Data Analytics course project focused on disease surveillance and resource planning in Bangladesh.

## Project Status

The data engineering pipeline and synthetic data generation components are implemented and tested. The project includes Bronze ingestion, Silver validation and privacy processing, Gold epidemiological aggregation, weather ingestion, outbreak simulation, and incremental Gold recomputation.

**Latest test status:** 27 tests passed.

## Technology Stack

- Python 3.12
- Apache Spark / PySpark
- Pandas
- pytest
- Open-Meteo Historical Archive API
- geoBoundaries district geography
- Parquet
- D3.js v7 (planned dashboard)

## Project Structure

- `generator/` — synthetic encounters, facilities, outbreak models, weather ingestion, and geography.
- `pipeline/bronze.py` — raw-source ingestion with batch metadata and checksums.
- `pipeline/silver.py` — validation, quarantine, patient-ID hashing, and age-band processing.
- `pipeline/gold.py` — weekly district-disease and facility-reporting aggregations.
- `pipeline/incremental.py` — batch ledger and multi-batch Silver reading.
- `orchestration/` — Bronze-to-Silver-to-Gold pipeline orchestration.
- `analytics/` — disease detection, forecasting, and resource-planning work.
- `dashboard/` — dashboard work.
- `serving/` — serving components.
- `tests/` — automated unit and integration tests.
- `docs/` — project documentation.
- `data/` — locally generated or downloaded data; excluded from Git.

## Requirements

- Python 3.12
- Java compatible with the installed PySpark version
- Dependencies listed in `requirements.txt`

## Setup (Windows PowerShell)

Create and activate a virtual environment:

```powershell
uv venv --python 3.12 .venv
.\.venv\Scripts\Activate.ps1
uv pip install -r requirements.txt
```

Configure Spark to use the virtual environment's Python interpreter:

```powershell
$pythonPath = (Resolve-Path .\.venv\Scripts\python.exe).Path
$env:PYSPARK_PYTHON = $pythonPath
$env:PYSPARK_DRIVER_PYTHON = $pythonPath
```

Set a private salt for patient-ID hashing. Generate a fresh value for your own environment:

```powershell
$env:PATIENT_ID_SALT = (python -c "import secrets; print(secrets.token_hex(32))")
```

Keep this value secret. Do not commit it or share it in reports, screenshots, or source control.

## Run the Tests

```powershell
python -m pytest tests -q
```

Check Python syntax:

```powershell
python -m py_compile orchestration\run_pipeline.py pipeline\gold.py pipeline\incremental.py
```

## Run the Pipeline

The orchestration CLI accepts an input directory containing the required source CSV files, a lakehouse output directory, a batch ID, and a facility registry.

Example command (use an appropriate prepared input directory):

```powershell
python -m orchestration.run_pipeline `
  --input-dir data\integration_test\input `
  --lakehouse-dir data\integration_test\lakehouse `
  --batch-id demo_batch `
  --facilities-path data\integration_test\input\facilities.csv `
  --ledger data\integration_test\processed_batches.json
```

The input directory must contain the source files expected by the Bronze ingestion code. Use isolated test data for initial validation.

## Data Engineering Features

### Bronze
- Reads source CSV files without applying analytical transformations.
- Adds batch metadata, source file information, load timestamps, and SHA-256 checksums.

### Silver
- Validates encounter data and facility/district references.
- Quarantines invalid records with validation reasons.
- Hashes patient identifiers using a secret salt.
- Reduces ages to predefined age bands.

### Gold
- Aggregates cases by epidemiological week, district, and disease.
- Produces facility-week reporting summaries and potential reporting-gap indicators.
- Records source batch IDs for lineage.
- Supports recomputation of affected weeks from available Silver batches.

### Synthetic Data and Weather
- Generates synthetic encounters, facilities, and outbreak scenarios.
- Retrieves historical daily weather for district centroids.
- Stores downloaded weather responses in a local cache.

## Data and Privacy

- Generated data and downloaded datasets are excluded from Git by `.gitignore`.
- Never commit `.env` files, patient-ID salts, credentials, or other secrets.
- Patient identifiers must not be exposed beyond the intended Bronze boundary.
- The simulated novel-respiratory scenario uses a synthetic placeholder/proxy; it should not be interpreted as a validated real-world diagnosis.

## Limitations and Remaining Work

- Validate all source requirements and attribution before final submission.
- Confirm that every required source is represented in Bronze.
- Validate incremental processing against late-arriving records and repeated batch runs.
- Complete and test the analytics, dashboard, and resource-planning components assigned to the team.
- Run the required scale-factor experiments and document performance results.
- Complete the technical report, policy brief, and demonstration video.

## Course Project

EpiScope is an academic simulation. Synthetic cases and simulated outbreak patterns are not real surveillance data and must not be used for clinical or public-health decisions.
