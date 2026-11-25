"""
Fill the source database with its starting state ("split the data", as the mentor suggested).

A deterministic sample of the Fourth source is split in two:
- 80 % go into PostgreSQL: the platform's database on day 0;
- 20 % are kept in cdc_data/reserve.jsonl: real listings that the simulator inserts later as new listings.

The rows are inserted raw (no cleaning), because the database plays the source system; Silver cleans them.

Usage:  .venv/bin/python -m cdc.load_initial
"""

import json
import math
import random

from cdc import db, settings


def raw_row(record: dict) -> dict:
    """The source columns of one JSON record; NaN / Infinity numbers become None (Silver treats them as null)."""
    row = {}
    for column in db.COLUMN_NAMES:
        value = record.get(column)
        if isinstance(value, float) and not math.isfinite(value):
            value = None
        row[column] = value
    return row


def read_sample(path=settings.SOURCE_FILE, size: int = settings.SAMPLE_SIZE, seed: int = settings.SEED) -> list:
    """A deterministic random sample of `size` listings (the same rows on every run with the same seed)."""
    with open(path, "rb") as source:
        line_count = sum(1 for _ in source)
    chosen = set(random.Random(seed).sample(range(line_count), min(size, line_count)))
    rows = []
    with open(path, encoding="utf-8") as source:
        for index, line in enumerate(source):
            if index in chosen:
                rows.append(raw_row(json.loads(line)))
    return rows


def split_rows(rows: list, initial_share: float = settings.INITIAL_SHARE, seed: int = settings.SEED) -> tuple:
    """(initial rows, reserve rows): a shuffled split, deterministic for a seed."""
    shuffled = list(rows)
    random.Random(seed).shuffle(shuffled)
    cut = round(len(shuffled) * initial_share)
    return shuffled[:cut], shuffled[cut:]


def main() -> dict:
    settings.DEMO_DIR.mkdir(parents=True, exist_ok=True)
    initial, reserve = split_rows(read_sample())

    conn = db.connect()
    try:
        db.recreate_table(conn)
        db.insert_rows(conn, initial)
    finally:
        conn.close()

    with open(settings.RESERVE_FILE, "w", encoding="utf-8") as target:
        for row in reserve:
            target.write(json.dumps(row) + "\n")
    settings.STATE_FILE.write_text(json.dumps({"reserve_offset": 0, "days": []}, indent=2), encoding="utf-8")

    summary = {"initial_rows": len(initial), "reserve_rows": len(reserve)}
    print(f"Source database loaded: {summary['initial_rows']} listings; {summary['reserve_rows']} kept as new listings")
    return summary


if __name__ == "__main__":
    main()
