"""Local folders and fixed settings of the CDC demo (everything it writes stays in cdc_data/, gitignored)."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_FILE = PROJECT_ROOT / "data" / "FourthSourceSystem"  # NDJSON, raw columns as in Bronze
LOOKUP_FILE = PROJECT_ROOT / "data" / "model_to_brand_lookup.csv"

DEMO_DIR = PROJECT_ROOT / "cdc_data"
BRONZE_DIR = DEMO_DIR / "bronze"
SILVER_DIR = DEMO_DIR / "silver"
GOLD_DIR = DEMO_DIR / "gold"
RESERVE_FILE = DEMO_DIR / "reserve.jsonl"          # listings kept aside, inserted later as "new" listings
STATE_FILE = DEMO_DIR / "simulator_state.json"     # how far the reserve is used + what every day changed

CHANGES_FOLDER = "ListingChanges"  # bronze/ListingChanges/ingestion_date=YYYY-MM-DD/<file>.json

SAMPLE_SIZE = 20_000   # listings taken from the source file
INITIAL_SHARE = 0.8    # 80 % are in the database at the start, 20 % arrive later as new listings
SEED = 42
