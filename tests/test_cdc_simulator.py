"""
Tests for the source side of the CDC demo (cdc/): parsing the wal2json messages, the change simulator and the
data split. Pure Python – no database and no Spark.
"""

import json
from collections import Counter

import pytest

from cdc import cdc_reader, db, load_initial, simulate_changes


# ---------- WAL messages -> change events ----------


def test_lsn_to_number_orders_like_the_wal():
    assert cdc_reader.lsn_to_number("0/1A2B3C") == 0x1A2B3C
    assert cdc_reader.lsn_to_number("1/0") == 1 << 32
    lsns = ["0/FFFFFFFF", "1/0", "0/10", "1/1"]
    assert sorted(lsns, key=cdc_reader.lsn_to_number) == ["0/10", "0/FFFFFFFF", "1/0", "1/1"]


def test_utc_iso_converts_postgres_timestamps():
    assert cdc_reader.utc_iso("2026-11-24 16:03:05.847018+01") == "2026-11-24T15:03:05.847018Z"


def message(action, columns=None, identity=None, lsn="0/10"):
    to_list = lambda row: [{"name": k, "type": "text", "value": v} for k, v in row.items()]  # noqa: E731
    result = {"action": action, "timestamp": "2026-11-24 15:00:00+00", "lsn": lsn, "schema": "public",
              "table": "listings"}
    if columns is not None:
        result["columns"] = to_list(columns)
    if identity is not None:
        result["identity"] = to_list(identity)
    return result


def test_change_event_insert_update_delete():
    insert = cdc_reader.change_event(message("I", columns={"id": "a", "price": 10}))
    assert (insert["op"], insert["before"], insert["after"]) == ("I", None, {"id": "a", "price": 10})

    update = cdc_reader.change_event(message("U", columns={"id": "a", "price": 9}, identity={"id": "a", "price": 10}))
    assert (update["before"], update["after"]) == ({"id": "a", "price": 10}, {"id": "a", "price": 9})

    delete = cdc_reader.change_event(message("D", identity={"id": "a", "price": 9}, lsn="0/20"))
    assert (delete["op"], delete["after"], delete["before"]["price"], delete["lsn_num"]) == ("D", None, 9, 0x20)
    assert delete["commit_ts"] == "2026-11-24T15:00:00.000000Z"


def test_begin_and_commit_messages_are_not_events():
    assert cdc_reader.change_event({"action": "B", "lsn": "0/1"}) is None
    assert cdc_reader.change_event({"action": "C", "lsn": "0/2"}) is None


def test_unchanged_toast_column_is_taken_from_the_old_row():
    """wal2json leaves an unchanged long value (TOAST) out of the new row of an UPDATE."""
    event = cdc_reader.change_event(message(
        "U", columns={"id": "a", "price": 9}, identity={"id": "a", "price": 10, "description": "long text"},
    ))
    assert event["after"] == {"id": "a", "price": 9, "description": "long text"}


def test_written_event_files_are_complete_ndjson(tmp_path):
    events = [cdc_reader.snapshot_event({"id": "a"}, "0/5", "2026-11-24T15:00:00.000000Z"),
              cdc_reader.snapshot_event({"id": "b"}, "0/5", "2026-11-24T15:00:00.000000Z")]
    path = cdc_reader.write_events(tmp_path, "2026-11-24", events, "snapshot")
    assert path.parent.name == "ingestion_date=2026-11-24"
    assert [json.loads(line)["after"]["id"] for line in path.read_text().splitlines()] == ["a", "b"]
    assert not list(path.parent.glob(".*"))  # no temporary file left


# ---------- simulator ----------


def current_rows(count):
    return {f"{i:012x}": {"id": f"{i:012x}", "price": 10000.0, "odometer": 50000.0, "condition": "excellent",
                          "title_status": "clean"} for i in range(count)}


def reserve_rows(count):
    return [{"id": f"r{i:011x}", "price": 5000.0} for i in range(count)]


def test_plan_changes_is_deterministic_and_follows_the_rates():
    current, reserve = current_rows(2000), reserve_rows(100)
    plan = simulate_changes.plan_changes(current, reserve, day=1, seed=7)
    assert plan == simulate_changes.plan_changes(current, reserve, day=1, seed=7)
    assert plan != simulate_changes.plan_changes(current, reserve, day=2, seed=7)

    summary = simulate_changes.summarize(plan)
    assert summary["inserts"] == 40           # 2 %
    assert summary["deletes"] == 20           # 1 %
    assert summary["updated_vehicles"] == 60 + 10  # 3 % price drops + 0.5 % attribute changes
    assert summary["attribute_updates"] == 10
    assert 60 < summary["price_updates"] < 60 * 2  # some prices drop twice


def test_plan_changes_keeps_vehicles_apart_and_in_order():
    current = current_rows(2000)
    plan = simulate_changes.plan_changes(current, reserve_rows(100), day=3, seed=1)
    deleted = {o["id"] for o in plan if o["op"] == "delete"}
    updated = {o["id"] for o in plan if o["op"] == "update"}
    assert not deleted & updated

    by_vehicle = {}
    for operation in plan:
        if operation["op"] == "update" and "price" in operation["set"]:
            by_vehicle.setdefault(operation["id"], []).append(operation["set"]["price"])
    for prices in by_vehicle.values():
        assert prices == sorted(prices, reverse=True)  # the second drop comes after the first
        assert all(7000 <= p < 10000 for p in prices)
    assert any(len(p) == 2 for p in by_vehicle.values())


def test_inserts_come_from_the_reserve_in_order():
    reserve = reserve_rows(100)
    plan = simulate_changes.plan_changes(current_rows(1000), reserve, day=1)
    inserted = [o["row"]["id"] for o in plan if o["op"] == "insert"]
    assert sorted(inserted) == [r["id"] for r in reserve[:20]]


@pytest.mark.parametrize(
    "row, expected",
    [
        ({"condition": "excellent"}, {"condition": "good"}),
        ({"condition": "Like New"}, {"condition": "excellent"}),
        ({"condition": "salvage", "title_status": "clean"}, {"title_status": "rebuilt"}),
        ({"condition": None, "title_status": "rebuilt"}, {"title_status": "clean"}),
    ],
)
def test_next_attribute(row, expected):
    assert simulate_changes.next_attribute(row) == expected


# ---------- initial load ----------


def test_split_rows_is_deterministic_and_complete():
    rows = [{"id": str(i)} for i in range(100)]
    initial, reserve = load_initial.split_rows(rows, 0.8, seed=3)
    assert (len(initial), len(reserve)) == (80, 20)
    assert Counter(r["id"] for r in initial + reserve) == Counter(r["id"] for r in rows)
    assert (initial, reserve) == load_initial.split_rows(rows, 0.8, seed=3)


def test_raw_row_keeps_the_source_columns_and_drops_nan():
    row = load_initial.raw_row({"id": "a", "price": float("nan"), "lat": float("inf"), "extra": 1, "year": 2015.0})
    assert list(row) == db.COLUMN_NAMES
    assert (row["price"], row["lat"], row["year"], row["model"]) == (None, None, 2015.0, None)
    assert "extra" not in row
