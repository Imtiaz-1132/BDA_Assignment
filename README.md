# EpiScope — Bangladesh Disease Surveillance

**A Big Data Analytics Engineering Project for Synthetic Disease Surveillance, Outbreak Analysis, and Healthcare Resource Planning in Bangladesh**

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Testing](https://img.shields.io/badge/Tests-38%20passing-brightgreen)](#testing)
[![Project](https://img.shields.io/badge/Project-Academic%20Research-blue)](#disclaimer)

EpiScope is an academic Big Data Analytics project that demonstrates how a data engineering pipeline can process synthetic disease surveillance data, validate data quality, protect patient identifiers, aggregate epidemiological indicators, and support healthcare resource planning.

The project is designed around a scalable **Bronze–Silver–Gold data processing architecture**, with synthetic data generation, weather data integration, batch processing, and automated tests.

> **Important:** EpiScope is an academic simulation. Its synthetic cases and simulated outbreak patterns are not real surveillance data and must not be used for clinical decisions or actual public-health interventions.

## Table of Contents

- [Project Objectives](#project-objectives)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Installation and Setup](#installation-and-setup)
- [Running the Tests](#running-the-tests)
- [Running the Data Pipeline](#running-the-data-pipeline)
- [Data Processing Layers](#data-processing-layers)
- [Privacy and Data Governance](#privacy-and-data-governance)
- [Testing and Evidence](#testing-and-evidence)
- [Limitations and Future Work](#limitations-and-future-work)
- [Contributing](#contributing)
- [Disclaimer](#disclaimer)

## Project Objectives

EpiScope aims to demonstrate how big data engineering techniques can support disease surveillance and resource planning through:

1. Generating reproducible synthetic disease encounters, facilities, and outbreak scenarios.
2. Ingesting source files with batch metadata, file information, and checksums.
3. Validating records and isolating invalid data for further inspection.
4. Protecting patient identifiers through salted hashing and reducing age precision through age bands.
5. Producing district-, disease-, epidemiological-week-, and facility-level summaries.
6. Integrating historical weather information into synthetic disease modelling.
7. Supporting batch tracking and recomputation of affected analytical periods.
8. Testing pipeline behaviour using automated unit and integration tests.

## Key Features

- **Synthetic data generation:** Creates simulated encounters, healthcare facilities, resources, and outbreak ground truth.
- **Data quality validation:** Checks encounter records and relevant facility and district references.
- **Data quarantine:** Separates invalid records and records validation reasons.
- **Data privacy processing:** Hashes patient identifiers using a secret salt and converts ages into predefined bands.
- **Batch-aware ingestion:** Records source metadata and SHA-256 checksums.
- **Epidemiological aggregation:** Summarizes cases by disease, district, and epidemiological week.
- **Facility reporting analysis:** Produces facility-week summaries and potential reporting-gap indicators.
- **Incremental processing:** Maintains batch-processing information and supports recomputation of affected periods.
- **Weather integration:** Retrieves historical weather data for district locations and caches downloaded responses.
- **Automated testing:** Uses pytest to validate important generator and pipeline behaviours.

## System Architecture

EpiScope organizes data processing into three logical layers.

```text
       Synthetic Data and External Sources
       ┌─────────────────────────────────┐
       │ Encounters, Facilities, Weather │
       │ Resources and Outbreak Scenarios│
       └────────────────┬────────────────┘
                        │
                        ▼
       ┌─────────────────────────────────┐
       │         BRONZE LAYER            │
       │ Raw ingestion, batch metadata,  │
       │ source tracking and checksums   │
       └────────────────┬────────────────┘
                        │
                        ▼
       ┌─────────────────────────────────┐
       │         SILVER LAYER            │
       │ Validation, quarantine,          │
       │ identifier hashing, age bands    │
       └────────────────┬────────────────┘
                        │
                        ▼
       ┌─────────────────────────────────┐
       │          GOLD LAYER             │
       │ Epidemiological and facility    │
       │ reporting aggregations          │
       └────────────────┬────────────────┘
                        │
                        ▼
       ┌─────────────────────────────────┐
       │ Analytics, Visualization and    │
       │ Resource-Planning Components    │
       └─────────────────────────────────┘
```

The analytics and visualization components represent downstream project areas; their completeness and integration should be assessed separately from the tested data engineering pipeline.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data generation, orchestration, and processing logic |
| Apache Spark / PySpark | Distributed data processing |
| Pandas | Tabular data processing and weather feature preparation |
| pytest | Automated testing |
| Parquet | Columnar data storage for analytical processing |
| Open-Meteo Historical Archive API | Historical weather data source |
| geoBoundaries | Geographic boundary and district reference data |
| D3.js v7 | Planned or ongoing interactive visualization work |

## Repository Structure

```text
BDA_Assignment/
├── analytics/              # Analytics and forecasting components
├── dashboard/              # Dashboard and visualization work
├── docs/
│   └── testing/
│       ├── member4_test_report.md
│       └── pytest-final.txt
├── generator/               # Synthetic data and source-data generators
├── orchestration/            # End-to-end pipeline orchestration
├── pipeline/
│   ├── bronze.py             # Raw ingestion and batch metadata
│   ├── silver.py             # Validation and privacy processing
│   ├── gold.py               # Analytical aggregations
│   └── incremental.py        # Batch tracking and incremental processing
├── serving/                  # Data-serving components
├── tests/                    # Automated unit and integration tests
├── .env.example              # Example environment configuration
├── .gitignore
├── AI_USAGE.md
├── Makefile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

Some directories may contain additional components or may still be under development. Consult the source code and project documentation for their current implementation status.

## Prerequisites

Before setting up the project, install:

- Git
- Python 3.12, as specified in the project setup
- A compatible Java runtime for the installed PySpark version
- The Python packages listed in `requirements.txt`

The following instructions use Windows PowerShell.

## Installation and Setup

### 1. Clone the repository

```powershell
git clone https://github.com/Imtiaz-1132/BDA_Assignment.git
cd BDA_Assignment
```

### 2. Create a virtual environment

If `uv` is installed:

```powershell
uv venv --python 3.12 .venv
.\.venv\Scripts\Activate.ps1
```

Alternatively, use the Python launcher or an installed Python 3.12 executable:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 4. Configure PySpark

Set PySpark to use the Python interpreter from the virtual environment:

```powershell
$pythonPath = (Resolve-Path .\.venv\Scripts\python.exe).Path

$env:PYSPARK_PYTHON = $pythonPath
$env:PYSPARK_DRIVER_PYTHON = $pythonPath
```

These environment variables apply to the current PowerShell session. Set them again when opening a new session.

### 5. Configure the patient-ID hashing salt

Generate a private salt for local development:

```powershell
$env:PATIENT_ID_SALT = (
    python -c "import secrets; print(secrets.token_hex(32))"
)
```

Keep the salt private. Never commit it, publish it in screenshots, or share it in reports. Use a consistent secret configuration for workflows that need stable pseudonymous identifiers.

Review `.env.example` and the project configuration for any other required environment variables.

## Running the Tests

Run the complete automated test suite:

```powershell
python -m pytest tests -q
```

Run a specific test module:

```powershell
python -m pytest tests/test_generator.py -q
```

```powershell
python -m pytest tests/test_silver.py -q
```

Run the Bronze pipeline tests:

```powershell
python -m pytest tests/test_bronze.py -q
```

Check the syntax of selected Python modules:

```powershell
python -m py_compile `
    orchestration\run_pipeline.py `
    pipeline\gold.py `
    pipeline\incremental.py
```

### Latest Recorded Test Result

The Member 4 testing work recorded a full test run with:

| Metric | Result |
|---|---:|
| Tests passed | 38 |
| Tests failed | 0 |
| Runtime | 58.79 seconds |
| Exit code | 0 |

The associated output and report are available in `docs/testing/pytest-final.txt` and `docs/testing/member4_test_report.md`.

These results describe a specific local test run. Test outcomes may vary with code changes, dependency versions, environment configuration, and available input data. Rerun the suite to verify the current checkout.

## Running the Data Pipeline

The orchestration CLI accepts an input directory, a lakehouse output directory, a batch identifier, a facility registry, and a batch ledger.

Use prepared input data containing the source CSV files expected by the Bronze ingestion code.

Example:

```powershell
python -m orchestration.run_pipeline `
    --input-dir data\integration_test\input `
    --lakehouse-dir data\integration_test\lakehouse `
    --batch-id demo_batch `
    --facilities-path data\integration_test\input\facilities.csv `
    --ledger data\integration_test\processed_batches.json
```

Before executing the command, verify that the input files and facility registry exist. The example paths are illustrative and require appropriate prepared input data.

For synthetic data generation, weather ingestion, and other command-line options, inspect the relevant modules under `generator/` and the orchestration code.

## Data Processing Layers

### Bronze — Raw Ingestion

The Bronze layer provides the initial ingestion boundary.

Responsibilities include:

- Reading source CSV files.
- Recording batch and source-file metadata.
- Capturing load information and SHA-256 checksums.
- Preserving traceability to ingested source data.

### Silver — Validation and Privacy Processing

The Silver layer prepares data for downstream analytics.

Responsibilities include:

- Validating encounter records.
- Checking facility and district references.
- Quarantining invalid records with reasons.
- Hashing patient identifiers with a secret salt.
- Converting age values into predefined age bands.

### Gold — Analytical Aggregation

The Gold layer produces aggregated outputs for epidemiological analysis.

Responsibilities include:

- Aggregating cases by epidemiological week, district, and disease.
- Summarizing facility-week reporting.
- Identifying potential reporting gaps.
- Preserving source-batch lineage.
- Supporting recomputation of affected weeks from available Silver batches.

### Synthetic Data and Weather

The generator components support:

- Synthetic encounter generation.
- Healthcare facility and resource generation.
- Simulated outbreak ground truth.
- Historical weather ingestion for district locations.
- Local caching of downloaded weather responses.

Synthetic patterns and weather-derived features are intended for project experimentation, not validated epidemiological prediction.

## Privacy and Data Governance

Privacy and responsible handling of data are important parts of the project.

- Use synthetic or appropriately authorized data for development and testing.
- Keep patient-ID hashing salts and other secrets outside version control.
- Do not expose patient identifiers beyond the intended processing boundary.
- Use age bands where reduced age precision is appropriate.
- Quarantine invalid records and retain useful validation reasons.
- Review source licensing, attribution, and data provenance before final submission.
- Apply appropriate access controls and disclosure protections to any future reporting interface.

Hashing identifiers does not automatically make a dataset anonymous. Privacy risk depends on the complete data, available auxiliary information, and downstream release practices.

## Testing and Evidence

Member 4's testing and integration work includes automated tests and saved test evidence.

- **Test report:** [`docs/testing/member4_test_report.md`](docs/testing/member4_test_report.md)
- **Captured pytest output:** [`docs/testing/pytest-final.txt`](docs/testing/pytest-final.txt)

The test suite provides evidence for the behaviours covered by the tests. It does not, by itself, establish that every assignment requirement has been fulfilled.

Performance benchmarks, large-scale experiments, dashboard usability, and end-to-end acceptance should be documented separately when completed.

## Limitations and Future Work

Potential areas for further development and verification include:

- Completing and validating all analytics, dashboard, and resource-planning components.
- Running the required scale-factor experiments and documenting performance.
- Recording Spark optimization evidence and before/after measurements.
- Testing incremental processing with late-arriving data and repeated batches.
- Verifying data-source coverage, provenance, and licensing.
- Evaluating small-cell suppression and disclosure risks before releasing aggregate data.
- Completing user acceptance testing, the technical report, policy brief, and demonstration.
- Validating the pipeline using representative end-to-end input datasets.

These are project follow-up areas, not claims that all items are already implemented.

## Contributing

This repository is maintained as a collaborative academic project.

1. Create a branch for your task.
2. Make changes that align with the project architecture.
3. Add or update tests for changed behaviour.
4. Run the relevant tests and the complete test suite.
5. Document any configuration requirements, assumptions, and limitations.
6. Submit a pull request for team review.

Example:

```powershell
git checkout -b feature/your-feature
git add .
git commit -m "feat: describe your change"
git push -u origin feature/your-feature
```

Avoid committing generated datasets, local environment files, credentials, or private salts.

## Disclaimer

EpiScope is developed for educational and engineering experimentation as part of a Big Data Analytics project. It is not a validated clinical system, an operational public-health surveillance platform, or a substitute for professional epidemiological analysis.

All synthetic disease cases, simulated outbreaks, and derived signals should be interpreted within the limits of the project's assumptions and validation.

---

**Repository:** [Imtiaz-1132/BDA_Assignment](https://github.com/Imtiaz-1132/BDA_Assignment)

**Project:** EpiScope — Bangladesh Disease Surveillance

**Focus:** Big Data Analytics, data engineering, privacy-aware processing, and epidemiological aggregation
