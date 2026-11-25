"""
The CDC reader: reads the changes of the source database from its write-ahead log (WAL) and lands them in Bronze.

How it works (documentation/cdc.md):
- PostgreSQL writes every change to the WAL before it changes the table. Logical decoding with the wal2json
  plugin turns the WAL records into JSON rows (INSERT / UPDATE / DELETE with the old and the new row).
- A replication slot is our bookmark in the WAL: it remembers up to which position (LSN) we confirmed, and
  PostgreSQL keeps every WAL record after it until we confirm it.
- Initial snapshot (once): the slot is created with an exported snapshot, and the table is read inside that
  snapshot. The snapshot and the slot start at the same point, so no change is lost or counted twice
  (Debezium does the same). Snapshot rows are written as events with op "R" (read).
- Every run afterwards: peek all new changes, write them to one NDJSON file in
  bronze/ListingChanges/ingestion_date=YYYY-MM-DD/, and only then advance the slot. A crash between the
  write and the confirmation means the same changes arrive again (at-least-once); Silver skips them by LSN.

Event (one JSON line):
{"op": "U", "lsn": "0/1A2B3C", "lsn_num": 27339580, "commit_ts": "2026-11-24T10:15:03.123456Z",
 "table": "listings", "before": {...raw columns...}, "after": {...raw columns...}}

Usage:  .venv/bin/python -m cdc.cdc_reader [--snapshot] [--crash-before-ack]
"""

import argparse
import json
import os
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

from cdc import db, settings

WAL2JSON_OPTIONS = [
    "format-version", "2",          # one JSON row per change
    "include-timestamp", "1",       # commit time of the transaction
    "include-lsn", "1",             # WAL position of every change
    "include-transaction", "true",  # BEGIN / COMMIT rows: the slot is confirmed up to the end of a transaction
    "add-tables", f"public.{db.TABLE}",
]


def lsn_to_number(lsn: str) -> int:
    """'0/1A2B3C' -> 1715004: an LSN is two hex numbers (high/low 32 bits); as one number it orders the changes."""
    high, low = lsn.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def utc_iso(timestamp) -> str:
    """A PostgreSQL timestamp ('2026-11-24 15:03:05.847018+00' or a datetime) as ISO 8601 UTC with 'Z'."""
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    return timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def columns_to_dict(columns: list) -> dict:
    return {column["name"]: column["value"] for column in columns or []}


def change_event(message: dict):
    """A wal2json (format 2) message as a change event, or None for BEGIN / COMMIT messages."""
    action = message["action"]
    if action not in ("I", "U", "D"):
        return None
    # REPLICA IDENTITY FULL: "identity" holds the whole old row of an UPDATE / DELETE.
    before = columns_to_dict(message.get("identity")) if action in ("U", "D") else None
    after = columns_to_dict(message.get("columns")) if action in ("I", "U") else None
    if action == "U":
        # PostgreSQL stores long values (e.g. a long description) apart from the row (TOAST). When an UPDATE does
        # not change such a value, the WAL does not repeat it and wal2json leaves the column out of the new row.
        # It is unchanged by definition, so it is taken from the old row (Debezium has the same gap and writes a
        # placeholder instead). Without this, Silver would see the description disappear.
        after = {**{k: v for k, v in before.items() if k not in after}, **after}
    return {
        "op": action,
        "lsn": message["lsn"],
        "lsn_num": lsn_to_number(message["lsn"]),
        "commit_ts": utc_iso(message["timestamp"]),
        "table": message["table"],
        "before": before,
        "after": after,
    }


def snapshot_event(row: dict, lsn: str, snapshot_ts: str) -> dict:
    return {"op": "R", "lsn": lsn, "lsn_num": lsn_to_number(lsn), "commit_ts": snapshot_ts,
            "table": db.TABLE, "before": None, "after": row}


def changes_folder(bronze_dir: Path, ingestion_date: str) -> Path:
    return Path(bronze_dir) / settings.CHANGES_FOLDER / f"ingestion_date={ingestion_date}"


def write_events(bronze_dir: Path, ingestion_date: str, events: list, prefix: str) -> Path:
    """
    Write the events as one NDJSON file. The file appears only when it is complete (written under a temporary
    name and renamed), so Silver never reads half a file. The name holds the LSN range and the write time,
    so a re-delivery after a crash lands next to the first file instead of overwriting it.
    """
    folder = changes_folder(bronze_dir, ingestion_date)
    folder.mkdir(parents=True, exist_ok=True)
    first, last = events[0]["lsn"].replace("/", "-"), events[-1]["lsn"].replace("/", "-")
    written_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    target = folder / f"{prefix}_{first}_{last}_{written_at}.json"
    temporary = folder / f".{target.name}.tmp"
    with open(temporary, "w", encoding="utf-8") as output:
        for event in events:
            output.write(json.dumps(event) + "\n")
    os.replace(temporary, target)
    return target


def create_slot_and_snapshot(bronze_dir: Path = settings.BRONZE_DIR, ingestion_date: str = None) -> dict:
    """Create the replication slot and write the initial snapshot of the table as "R" events."""
    from psycopg2.extras import LogicalReplicationConnection

    ingestion_date = ingestion_date or date.today().isoformat()
    replication = db.connect(connection_factory=LogicalReplicationConnection)
    reader = db.connect(autocommit=False)
    try:
        db.drop_slot_if_exists(reader)
        reader.commit()
        with replication.cursor() as cur:
            cur.execute(f"CREATE_REPLICATION_SLOT {db.SLOT_NAME} LOGICAL {db.OUTPUT_PLUGIN} EXPORT_SNAPSHOT")
            _, consistent_point, snapshot_name, _ = cur.fetchone()

        # Read the table exactly as it was when the slot was created (the snapshot stays valid while the
        # replication connection is open and idle).
        reader.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with reader.cursor() as cur:
            cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_name,))
            cur.execute("SELECT now()")
            snapshot_ts = utc_iso(cur.fetchone()[0])
        rows = db.fetch_rows(reader)
        reader.commit()
    finally:
        reader.close()
        replication.close()

    events = [snapshot_event(row, consistent_point, snapshot_ts) for row in rows]
    path = write_events(bronze_dir, ingestion_date, events, "snapshot")
    print(f"Slot {db.SLOT_NAME} created at {consistent_point}; snapshot of {len(events)} listings -> {path}")
    return {"events": len(events), "lsn": consistent_point, "file": str(path)}


def read_changes(bronze_dir: Path = settings.BRONZE_DIR, ingestion_date: str = None,
                 crash_before_ack: bool = False) -> dict:
    """
    Land all changes since the last confirmation in Bronze, then confirm them (advance the slot).
    `crash_before_ack` stops after the write, like a reader that dies before confirming (for the demo test).
    """
    ingestion_date = ingestion_date or date.today().isoformat()
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            placeholders = ", ".join(["%s"] * len(WAL2JSON_OPTIONS))
            cur.execute(
                f"SELECT lsn::text, data FROM pg_logical_slot_peek_changes(%s, NULL, NULL, {placeholders})",
                [db.SLOT_NAME, *WAL2JSON_OPTIONS],
            )
            rows = cur.fetchall()
        events = [e for e in (change_event(json.loads(data)) for _, data in rows) if e is not None]
        path = write_events(bronze_dir, ingestion_date, events, "changes") if events else None
        if rows and not crash_before_ack:
            # Confirm up to the end of the last transaction (its COMMIT row), so it is not delivered again.
            with conn.cursor() as cur:
                cur.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (db.SLOT_NAME, rows[-1][0]))
    finally:
        conn.close()

    by_op = dict(Counter(e["op"] for e in events))
    state = "NOT confirmed (simulated crash)" if crash_before_ack else "confirmed"
    print(f"Read {len(events)} changes {by_op} -> {path or 'nothing new'}; slot {state}")
    return {"events": len(events), "by_op": by_op, "file": str(path) if path else None,
            "acknowledged": not crash_before_ack}


def slot_lag_bytes() -> int:
    """WAL bytes the slot still holds for us (in production this is monitored, so the disk never fills up)."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots "
                "WHERE slot_name = %s", (db.SLOT_NAME,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--snapshot", action="store_true", help="create the slot and write the initial snapshot")
    parser.add_argument("--crash-before-ack", action="store_true", help="write the changes but do not confirm them")
    arguments = parser.parse_args()
    if arguments.snapshot:
        create_slot_and_snapshot()
    else:
        read_changes(crash_before_ack=arguments.crash_before_ack)
