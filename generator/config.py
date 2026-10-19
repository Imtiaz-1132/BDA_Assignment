
"""Configuration for the EpiScope synthetic data generator."""

from pathlib import Path

# Project directories
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SYNTHETIC_DIR = DATA_DIR / "synthetic"

# Reproducibility
RANDOM_SEED = 42

# Generator scale
# SF=1 must eventually generate at least 25 million encounters.
SCALE_FACTOR = 0.001

# Assignment period
START_DATE = "2021-01-01"
END_DATE = "2025-12-31"

# Bangladesh has 64 districts in the assignment.
NUMBER_OF_DISTRICTS = 64

# Synthetic facility registry requirement
NUMBER_OF_FACILITIES = 600

# Disease groups and ICD-10 codes
DISEASE_CODES = {
    "dengue": ["A90", "A91"],
    "diarrhoea": ["A00", "A09"],
    "influenza": ["J10", "J11"],
    "malaria": ["B50", "B51", "B52", "B53", "B54"],
}

# Required injected outbreaks
MINIMUM_OUTBREAKS = 30
