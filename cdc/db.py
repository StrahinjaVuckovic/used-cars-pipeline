"""
The source system of the CDC demo: a PostgreSQL database (Docker, cdc/docker-compose.yml) that plays the
operational database of a listing platform. The table `listings` holds the raw listing columns exactly as the
platform stores them (the Bronze shape, not cleaned); cleaning stays the job of Silver.

Credentials come from the project `.env` (gitignored): CDC_PG_USER and CDC_PG_PASSWORD.
"""

import os
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

HOST = "127.0.0.1"
PORT = 55432  # mapped in docker-compose.yml (localhost only)
DATABASE = "listings_source"
TABLE = "listings"
SLOT_NAME = "listings_cdc"
OUTPUT_PLUGIN = "wal2json"

# Raw columns of a vehicle source with their PostgreSQL types. The names must equal RAW_LISTING_COLUMNS in
# notebooks/utils/vehicle_listings_cleaning.py (a test compares them), so Silver cleans CDC events with the
# same function as the files.
SOURCE_COLUMNS = {
    "id": "text PRIMARY KEY",
    "url": "text",
    "region": "text",
    "region_url": "text",
    "price": "double precision",
    "year": "double precision",
    "model": "text",
    "condition": "text",
    "cylinders": "text",
    "fuel": "text",
    "odometer": "double precision",
    "title_status": "text",
    "transmission": "text",
    "VIN": "text",
    "drive": "text",
    "size": "text",
    "type": "text",
    "paint_color": "text",
    "image_url": "text",
    "description": "text",
    "state": "text",
    "lat": "double precision",
    "long": "double precision",
    "posting_date": "text",
    "source_system_id": "bigint",
    "source_system_name": "text",
    "currency": "text",
    "SourceSystemId": "text",
}
COLUMN_NAMES = list(SOURCE_COLUMNS)


def quoted(name: str) -> str:
    """Column name in double quotes, so mixed-case names ("VIN", "SourceSystemId") keep their case."""
    return '"' + name.replace('"', '""') + '"'


def load_env(path: Path = ENV_FILE) -> dict:
    """KEY=VALUE pairs of the .env file (comments and empty lines skipped); environment variables win."""
    values = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    return {**values, **{k: v for k, v in os.environ.items() if k.startswith("CDC_PG_")}}


def connect(autocommit: bool = True, **extra):
    """A connection to the source database. Every statement commits on its own unless autocommit=False."""
    env = load_env()
    missing = [k for k in ("CDC_PG_USER", "CDC_PG_PASSWORD") if k not in env]
    if missing:
        raise RuntimeError(f"Set {missing} in {ENV_FILE} (see cdc/docker-compose.yml)")
    conn = psycopg2.connect(
        host=HOST, port=PORT, dbname=DATABASE, user=env["CDC_PG_USER"], password=env["CDC_PG_PASSWORD"],
        connect_timeout=5, **extra,
    )
    if "connection_factory" not in extra:  # a replication connection is always in autocommit mode
        conn.autocommit = autocommit
    return conn


def is_available() -> bool:
    """True when the demo database answers (used by the integration test to skip without Docker)."""
    try:
        connect().close()
        return True
    except Exception:  # noqa: BLE001 – any connection problem means "not available"
        return False


def drop_slot_if_exists(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
            (SLOT_NAME,),
        )


def recreate_table(conn) -> None:
    """
    Drop the slot and the table and create an empty `listings` table.

    REPLICA IDENTITY FULL makes PostgreSQL write the whole old row to the WAL for UPDATE and DELETE, so every
    change event carries a complete "before" image (by default only the primary key is written).
    """
    drop_slot_if_exists(conn)
    columns = ",\n    ".join(f"{quoted(name)} {sql_type}" for name, sql_type in SOURCE_COLUMNS.items())
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(f"CREATE TABLE {TABLE} (\n    {columns}\n)")
        cur.execute(f"ALTER TABLE {TABLE} REPLICA IDENTITY FULL")


def insert_rows(conn, rows: list) -> None:
    """Insert raw rows (dicts with the source columns) in one transaction."""
    from psycopg2.extras import execute_values

    column_list = ", ".join(quoted(c) for c in COLUMN_NAMES)
    values = [tuple(row.get(c) for c in COLUMN_NAMES) for row in rows]
    with conn.cursor() as cur:
        execute_values(cur, f"INSERT INTO {TABLE} ({column_list}) VALUES %s", values, page_size=1000)


def fetch_rows(conn, columns: list = None) -> list:
    """All rows of the source table as dicts (default: every column)."""
    columns = columns or COLUMN_NAMES
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(quoted(c) for c in columns)} FROM {TABLE}")
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {TABLE}")
        return cur.fetchone()[0]
