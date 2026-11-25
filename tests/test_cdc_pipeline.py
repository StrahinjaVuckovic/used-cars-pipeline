"""
Tests for the lakehouse side of the CDC demo: notebooks/utils/cdc_helpers.py, 10_silver_listing_changes and
11_gold_cdc_vehicle_history. The input are small, hand-written change files in the format of cdc/cdc_reader.py,
so the SCD2 rules can be checked case by case without PostgreSQL. The end-to-end run against the Docker
database is opt-in (CDC_INTEGRATION=1 pytest -m cdc_integration), because it resets the demo database.
"""

import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from cdc import cdc_reader, db, local_pipeline
from notebook_utils import PROJECT_ROOT
from test_gold import dbml_tables

DAY = "2026-11-24"
START = datetime(2026, 11, 24, 8, 0, tzinfo=timezone.utc)
CDC_DBML_PATH = PROJECT_ROOT / "documentation" / "cdc_gold_schema.dbml"
IDS = {"a": "aaaaaaaaaaa1", "b": "bbbbbbbbbbb2", "c": "ccccccccccc3", "d": "ddddddddddd4"}


def raw(vehicle, **changes):
    row = {c: None for c in db.COLUMN_NAMES}
    row.update({
        "id": IDS[vehicle], "url": f"https://x.craigslist.org/cto/d/city-2015-ford-f-150/{vehicle}.html",
        "region": "x", "price": 15000.0, "year": 2015.0, "model": "f-150", "condition": "excellent",
        "fuel": "gas", "odometer": 80000.0, "title_status": "clean", "transmission": "automatic",
        "state": "ca", "posting_date": "2021-04-01", "source_system_id": 2, "source_system_name": "CarGurus",
        "currency": "USD", "description": "a" * 50,
    })
    row.update(changes)
    return row


def lsn(number):
    return f"{number >> 32:X}/{number & 0xFFFFFFFF:X}"


def event(op, number, minutes, before=None, after=None):
    return {"op": op, "lsn": lsn(number), "lsn_num": number, "table": "listings", "before": before, "after": after,
            "commit_ts": cdc_reader.utc_iso(START + timedelta(minutes=minutes))}


SNAPSHOT = [event("R", 100, 0, after=raw(v)) for v in ("a", "b", "c")]
DAY_ONE = [
    event("U", 101, 10, before=raw("a"), after=raw("a", price=14000.0)),
    event("U", 102, 20, before=raw("a", price=14000.0), after=raw("a", price=13000.0, odometer=80500.0)),
    event("U", 103, 30, before=raw("b"), after=raw("b", condition="good")),
    event("D", 104, 40, before=raw("c")),
    event("I", 105, 50, after=raw("d", price=9000.0)),
    # The same values again (e.g. a user saved without changing anything): no new version.
    event("U", 106, 60, before=raw("b", condition="good"), after=raw("b", condition="good")),
]


@pytest.fixture(autouse=True)
def release_cached_data(spark):
    """The notebooks cache DataFrames; a test that stops a notebook early leaves them cached. Free the memory."""
    yield
    spark.catalog.clearCache()


def make_lake(spark, root) -> dict:
    """Local bronze/silver/gold folders with a tiny brand lookup."""
    paths = {name: root / name for name in ("bronze", "silver", "gold")}
    spark.createDataFrame([("f-150", "ford")], "model_normalized STRING, manufacturer STRING") \
        .write.format("delta").save(str(paths["silver"] / "brand_lookup"))
    return paths


@pytest.fixture()
def lake(spark, tmp_path):
    return make_lake(spark, tmp_path)


def land(lake, events, prefix="changes"):
    return cdc_reader.write_events(lake["bronze"], DAY, events, prefix)


def run_silver(spark, lake):
    return local_pipeline.run(spark, "10_silver_listing_changes.py", **lake)


def run_all(spark, lake):
    silver_ns = run_silver(spark, lake)
    gold_ns = local_pipeline.run(spark, "11_gold_cdc_vehicle_history.py", **lake)
    return silver_ns, gold_ns


def history(spark, lake):
    return spark.read.format("delta").load(str(lake["silver"] / "vehicle_listings_cdc"))


def gold(spark, lake, table):
    return spark.read.format("delta").load(str(lake["gold"] / "cdc" / table))


def versions_of(spark, lake, vehicle):
    return history(spark, lake).filter(F.col("vehicle_id") == IDS[vehicle]).orderBy("valid_from").collect()


def ts(minutes):
    """The event time as PySpark returns timestamps: a naive datetime in the local time zone of this machine."""
    return (START + timedelta(minutes=minutes)).astimezone().replace(tzinfo=None)


@pytest.fixture(scope="module")
def after_day_one(spark, tmp_path_factory):
    """Snapshot + day one through Silver and Gold, built once for the module. Tests only read it."""
    lake = make_lake(spark, tmp_path_factory.mktemp("cdc_day_one"))
    land(lake, SNAPSHOT, "snapshot")
    run_all(spark, lake)
    land(lake, DAY_ONE)
    run_all(spark, lake)
    return lake


@pytest.fixture()
def day_one_copy(after_day_one, tmp_path):
    """A private copy of the day-one lake for tests that apply more changes."""
    return {name: Path(shutil.copytree(path, tmp_path / name)) for name, path in after_day_one.items()}


# ---------- Silver: SCD2 history ----------


def test_initial_snapshot_gives_one_current_version_per_listing(spark, lake):
    land(lake, SNAPSHOT, "snapshot")
    run_all(spark, lake)
    rows = history(spark, lake).collect()
    assert sorted(r.vehicle_id for r in rows) == sorted(IDS[v] for v in "abc")
    assert all(r.is_current and r.valid_to is None and r.change_op == "R" for r in rows)
    assert {r.source_system for r in rows} == {"listings_db"}
    assert {r.manufacturer for r in rows} == {"ford"}


def test_two_changes_of_one_listing_give_two_versions_in_wal_order(spark, after_day_one):
    a = versions_of(spark, after_day_one, "a")
    assert [r.price for r in a] == [Decimal("15000.00"), Decimal("14000.00"), Decimal("13000.00")]
    assert [r.is_current for r in a] == [False, False, True]
    assert [(r.valid_from, r.valid_to) for r in a] == [(ts(0), ts(10)), (ts(10), ts(20)), (ts(20), None)]
    assert a[1].valid_to_lsn == 102


def test_change_without_new_values_makes_no_version(spark, after_day_one):
    b = versions_of(spark, after_day_one, "b")
    assert [r.condition for r in b] == ["excellent", "good"]
    assert b[-1].is_current and b[-1].lsn_num == 103


def test_delete_closes_the_version_without_a_new_one(spark, after_day_one):
    c = versions_of(spark, after_day_one, "c")
    assert len(c) == 1 and not c[0].is_current
    assert (c[0].valid_to, c[0].valid_to_lsn) == (ts(40), 104)


def test_current_state_is_the_source_state(spark, after_day_one):
    current = {r.vehicle_id: r.price for r in history(spark, after_day_one).filter("is_current").collect()}
    assert current == {IDS["a"]: Decimal("13000.00"), IDS["b"]: Decimal("15000.00"), IDS["d"]: Decimal("9000.00")}


def test_redelivered_changes_change_nothing(spark, day_one_copy):
    """At-least-once: after a crash the reader writes the same changes into a second file."""
    before = sorted(history(spark, day_one_copy).collect())
    land(day_one_copy, DAY_ONE)
    namespace = run_silver(spark, day_one_copy)
    assert namespace["notebook_exit_value"] == "nothing to apply"
    assert sorted(history(spark, day_one_copy).collect()) == before


def test_partly_applied_file_applies_only_the_new_changes(spark, day_one_copy):
    new_change = event("U", 107, 70, before=raw("d", price=9000.0), after=raw("d", price=8500.0))
    land(day_one_copy, DAY_ONE[-2:] + [new_change])
    run_silver(spark, day_one_copy)
    d = versions_of(spark, day_one_copy, "d")
    assert [r.price for r in d] == [Decimal("9000.00"), Decimal("8500.00")]
    assert history(spark, day_one_copy).count() == 3 + 2 + 1 + 2  # versions of a, b, c, d


def test_change_at_the_snapshot_position_follows_the_snapshot(spark, lake):
    """The first change after the slot was created can have the same LSN as the snapshot."""
    land(lake, SNAPSHOT + [event("U", 100, 5, before=raw("a"), after=raw("a", price=1000.0))], "snapshot")
    run_silver(spark, lake)
    assert [r.change_op for r in versions_of(spark, lake, "a")] == ["R", "U"]


def test_first_run_needs_the_snapshot(spark, lake):
    land(lake, DAY_ONE)
    with pytest.raises(AssertionError, match="initial snapshot"):
        run_silver(spark, lake)


def test_check_cdc_versions_finds_overlapping_versions(spark, after_day_one):
    namespace = local_pipeline.namespace(spark, **after_day_one)
    df = history(spark, after_day_one)
    broken = df.withColumn("valid_to", F.when(F.col("lsn_num") == 101, F.lit(ts(25))).otherwise(F.col("valid_to")))
    with pytest.raises(AssertionError, match="overlap"):
        namespace["check_cdc_versions"](broken, "x")
    two_current = df.withColumn("is_current", F.lit(True)).withColumn("valid_to", F.lit(None).cast("timestamp"))
    with pytest.raises(AssertionError, match="more than one current version"):
        namespace["check_cdc_versions"](two_current, "x")


def test_select_change_partitions(spark, lake):
    select = local_pipeline.namespace(spark, **lake)["select_change_partitions"]
    available = ["2026-11-20", "2026-11-21", "2026-11-22"]
    assert select(available, "latest") == available
    assert select(available, "latest", "2026-11-21") == ["2026-11-21", "2026-11-22"]
    assert select(available, "2026-11-21", "2026-11-20") == ["2026-11-20", "2026-11-21"]
    with pytest.raises(AssertionError):
        select(available, "2026-11-23")


# ---------- Gold: SCD2 dimension and change facts ----------


def test_price_changes_do_not_make_dimension_versions(spark, after_day_one):
    dim = gold(spark, after_day_one, "dim_vehicle")
    a = dim.filter(F.col("vehicle_id") == IDS["a"]).collect()
    assert len(a) == 1 and a[0].is_current and a[0].valid_from == ts(0)


def test_attribute_change_makes_a_dimension_version(spark, after_day_one):
    dim = gold(spark, after_day_one, "dim_vehicle")
    b = dim.filter(F.col("vehicle_id") == IDS["b"]).orderBy("version_number").collect()
    assert [(r.condition, r.version_number, r.is_current) for r in b] == [("Excellent", 1, False), ("Good", 2, True)]
    assert b[0].valid_to == b[1].valid_from == ts(30)
    assert b[0].vehicle_key == b[1].vehicle_key != b[0].vehicle_version_key
    assert b[0].manufacturer == "Ford"


def test_sold_vehicle_has_no_current_version(spark, after_day_one):
    c = gold(spark, after_day_one, "dim_vehicle").filter(F.col("vehicle_id") == IDS["c"]).collect()
    assert [(r.is_current, r.is_deleted, r.valid_to) for r in c] == [(False, True, ts(40))]


def test_dimension_has_the_unknown_member(spark, after_day_one):
    unknown = gold(spark, after_day_one, "dim_vehicle").filter("vehicle_version_key = -1").collect()
    assert len(unknown) == 1 and unknown[0].vehicle_key == -1 and unknown[0].manufacturer == "Unknown"


def test_events_with_prices_and_vehicle_versions(spark, after_day_one):
    fact = gold(spark, after_day_one, "fact_listing_event")
    dim = gold(spark, after_day_one, "dim_vehicle")
    rows = (
        fact.join(dim.select("vehicle_version_key", "vehicle_id", "version_number"), "vehicle_version_key")
        .orderBy("event_ts", "vehicle_id").collect()
    )
    events = [(r.vehicle_id[0], r.event_type, r.price, r.price_change, r.version_number) for r in rows]
    assert events == [
        ("a", "Snapshot", Decimal("15000.00"), None, 1),
        ("b", "Snapshot", Decimal("15000.00"), None, 1),
        ("c", "Snapshot", Decimal("15000.00"), None, 1),
        ("a", "Price Change", Decimal("14000.00"), Decimal("-1000.00"), 1),
        ("a", "Price Change", Decimal("13000.00"), Decimal("-1000.00"), 1),
        ("b", "Updated", Decimal("15000.00"), Decimal("0.00"), 2),   # the version valid from that moment
        ("c", "Sold", Decimal("15000.00"), None, 1),                 # the version it closed
        ("d", "Listed", Decimal("9000.00"), None, 1),
    ]
    assert {r.event_date_key for r in rows} == {20261124}


def test_two_attribute_changes_at_the_same_commit_time_get_their_own_versions(spark, lake):
    """One transaction changed the vehicle twice: same commit time, different WAL positions -> different keys."""
    land(lake, SNAPSHOT, "snapshot")
    run_silver(spark, lake)
    land(lake, [
        event("U", 101, 10, before=raw("a"), after=raw("a", condition="good")),
        event("U", 102, 10, before=raw("a", condition="good"), after=raw("a", condition="good", title_status="rebuilt")),
    ])
    run_all(spark, lake)

    dim = gold(spark, lake, "dim_vehicle").filter(F.col("vehicle_id") == IDS["a"]).orderBy("version_number").collect()
    assert [(r.version_number, r.condition, r.title_status, r.is_current) for r in dim] == [
        (1, "Excellent", "Clean", False), (2, "Good", "Clean", False), (3, "Good", "Rebuilt", True),
    ]
    assert dim[1].valid_from == dim[2].valid_from == ts(10)  # same time, still three different keys
    assert len({r.vehicle_version_key for r in dim}) == 3

    events = (
        gold(spark, lake, "fact_listing_event").filter(F.col("vehicle_key") == dim[0].vehicle_key)
        .orderBy("lsn_num").collect()
    )
    assert [(e.event_type, e.vehicle_version_key) for e in events] == [
        ("Snapshot", dim[0].vehicle_version_key),
        ("Updated", dim[1].vehicle_version_key),
        ("Updated", dim[2].vehicle_version_key),
    ]


def test_cdc_gold_diagram_matches_the_code(spark, lake):
    columns = local_pipeline.namespace(spark, **lake)["CDC_GOLD_COLUMNS"]
    assert dbml_tables(CDC_DBML_PATH) == columns


def test_source_table_columns_are_the_raw_listing_columns(spark, lake):
    assert db.COLUMN_NAMES == local_pipeline.namespace(spark, **lake)["RAW_LISTING_COLUMNS"]


# ---------- end to end against PostgreSQL (opt-in) ----------


@pytest.mark.cdc_integration
@pytest.mark.skipif(os.environ.get("CDC_INTEGRATION") != "1", reason="set CDC_INTEGRATION=1 (resets the demo database)")
def test_demo_end_to_end_with_postgres(spark):
    if not db.is_available():
        pytest.skip("the demo PostgreSQL is not running (docker compose in cdc/)")
    from cdc import run_demo

    report = run_demo.main(days=2, crash_test=True, spark=spark)
    assert report[-1]["day"] == 2 and report[-1]["changes"] > 0
