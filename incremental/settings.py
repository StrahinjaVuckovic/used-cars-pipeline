"""Local folders and fixed settings of the incremental file load demo (everything stays in incremental_data/)."""

from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_FILE = PROJECT_ROOT / "data" / "FourthSourceSystem"  # NDJSON, raw columns as in Bronze

DEMO_DIR = PROJECT_ROOT / "incremental_data"
LANDING_DIR = DEMO_DIR / "landing"
BRONZE_DIR = DEMO_DIR / "bronze"
SILVER_DIR = DEMO_DIR / "silver"
GOLD_DIR = DEMO_DIR / "gold"
ORIGINAL_FILE = LANDING_DIR / "IncrementalSource" / "original" / "listings_original.jsonl"

DAYS = 5                       # the file is split into fifths: one day each
START_DATE = date(2026, 11, 1)  # day 1; every next fifth is one day later
SEED = 42
CHANGE_RATES = {
    "price_drop": 0.03,         # share of the already published listings whose price drops on a day
    "attribute_change": 0.005,  # share whose condition / title status changes on a day
}
