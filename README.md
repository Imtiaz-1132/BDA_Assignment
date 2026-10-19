@'
# EpiScope

Big Data Analytics course project for disease surveillance and resource planning in Bangladesh.

## Project Status

Initial setup and synthetic data generator development.

## Technology Stack

- Python
- Apache Spark (PySpark)
- Pandas
- pytest
- D3.js v7 (planned dashboard)

## Project Structure

- `generator/` - synthetic data generators and configuration
- `pipeline/` - data processing layers
- `analytics/` - detection, forecasting, and resource allocation
- `dashboard/` - visualization
- `tests/` - automated tests
- `docs/` - project documentation

## Setup

Create and activate a Python virtual environment, then install dependencies:

```powershell
uv venv --python 3.12 .venv
.\.venv\Scripts\Activate.ps1
uv pip install -r requirements.txt
