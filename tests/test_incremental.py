"""
Tests for the incremental file load (documentation/incremental_load.md): building the original file
(incremental/prepare_original.py) and the notebooks 12 (original -> source), 13 (source -> Bronze), 14 (Silver SCD2)
and 15 (Gold current state), on a small hand-made original file. The run on the full Fourth file is the demo
`python -m incremental.run_demo`.
"""

import datetime
import shutil
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from cdc import db, local_pipeline
from incremental import prepare_original
from incremental.run_demo import STEPS

DAYS = ["2026-11-01", "2026-11-02", "2026-11-03"]


def raw(index: int, **changes) -> dict:
    row = {c: None for c in db.COLUMN_NAMES}
    row.update({
        "id": f"{index:012x}", "url": f"https://x.craigslist.org/cto/d/city-2015-ford-f-150/{index}.html",
        "region": "x", "price": 10000.0 + index, "year": 2015.0, "model": "f-150", "condition": "excellent",
        "fuel": "gas", "odometer": 80000.0, "title_status": "clean", "transmission": "automatic", "state": "ca",
        "posting_date": "2021-04-01", "source_system_id": 2, "source_system_name": "CarGurus", "currency": "USD",
    })
    row.update(changes)
    return row


ROWS = [raw(i) for i in range(1, 13)]
RATES = {"price_drop": 0.5, "attribute_change": 0.0}  # half of the published listings drop their price every day


def build(rows=ROWS, days=len(DAYS)):
    return prepare_original.build_original(rows, days=days, seed=7, start_date=datetime.date(2026, 11, 1),
                                           rates=RATES)


# ---------- the original file ----------


def test_original_is_split_into_days_with_new_and_changed_listings():
    records = build()
    summary = prepare_original.summarize(records)
    assert [day for day, _, _ in summary] == DAYS
    assert [new for _, new, _ in summary] == [4, 4, 4]          # thirds of 12 listings
    assert [changed for _, _, changed in summary] == [0, 2, 4]  # half of the 4 / 8 already published
    assert [r["change_seq"] for r in records] == list(range(1, len(records) + 1))
    assert build() == records                                   # deterministic


def test_changes_only_touch_listings_published_on_an_earlier_day():
    first_day = {}
    per_day = {}
    for record in build():
        first_day.setdefault(record["id"], record["updated_at"])
        per_day.setdefault((record["id"], record["updated_at"]), 0)
        per_day[(record["id"], record["updated_at"])] += 1
        if record["updated_at"] != first_day[record["id"]]:
            assert record["updated_at"] > first_day[record["id"]]
            assert record["price"] < 10000.0 + int(record["id"], 16)  # a price drop
    assert max(per_day.values()) == 1  # at most one record per listing and day


def test_split_into_days_keeps_every_row():
    parts = prepare_original.split_into_days(list(range(11)), 5)
    assert [len(p) for p in parts] == [2, 2, 3, 2, 2] and sum(parts, []) == list(range(11))


# ---------- the notebooks, day by day ----------


@pytest.fixture(autouse=True)
def release_cached_data(spark):
    yield
    spark.catalog.clearCache()


def make_lake(spark, root: Path, records) -> dict:
    paths = {name: root / name for name in ("landing", "bronze", "silver", "gold")}
    prepare_original.write_original(
        records, paths["landing"] / "IncrementalSource" / "original" / "listings_original.jsonl"
    )
    spark.createDataFrame([("f-150", "ford")], "model_normalized STRING, manufacturer STRING") \
        .write.format("delta").save(str(paths["silver"] / "brand_lookup"))
    return paths


def run_step(spark, lake, step):
    return local_pipeline.run(spark, step, **lake).get("notebook_exit_value")


def history(spark, lake):
    return spark.read.format("delta").load(str(lake["silver"] / "vehicle_listings_history"))


@pytest.fixture(scope="module")
def loaded(spark, tmp_path_factory):
    """All three days loaded one by one; returns the lake and what each day left behind."""
    lake = make_lake(spark, tmp_path_factory.mktemp("incremental"), build())
    after = []
    for _ in DAYS:
        exits = [run_step(spark, lake, step) for step in STEPS]
        namespace = local_pipeline.namespace(spark, **lake)
        after.append({
            "exits": exits,
            "watermarks": (namespace["read_watermark"]("original_to_source"), namespace["read_watermark"]("source_to_bronze")),
            "source_days": sorted(r.updated_at for r in namespace["read_records"](namespace["source_path"]())
                                  .select("updated_at").distinct().collect()),
            "bronze_days": namespace["list_ingestion_dates"]("IncrementalListings"),
            "versions": history(spark, lake).count(),
            "ops": {r.change_op: r["count"] for r in history(spark, lake).groupBy("change_op").count().collect()},
        })
    return lake, after


def test_every_run_moves_one_day_through_all_layers(loaded):
    _, after = loaded
    for number, state in enumerate(after, start=1):
        assert state["exits"] == [None] * 4  # every step did its work
        assert state["watermarks"] == (DAYS[number - 1], DAYS[number - 1])
        assert state["source_days"] == DAYS[:number] == state["bronze_days"]


def test_silver_versions_grow_with_new_and_changed_listings(loaded):
    _, after = loaded
    assert [state["versions"] for state in after] == [4, 4 + 4 + 2, 4 + 4 + 2 + 4 + 4]
    assert after[-1]["ops"] == {"I": 12, "U": 6}


def test_current_state_is_the_latest_record_of_every_listing(spark, loaded):
    lake, _ = loaded
    latest = {}
    for record in build():
        latest[record["id"]] = record["price"]
    current = {r.vehicle_id: float(r.price) for r in history(spark, lake).filter("is_current").collect()}
    assert current == latest


def test_changed_listing_has_a_closed_and_a_current_version(spark, loaded):
    lake, _ = loaded
    versions = history(spark, lake)
    changed_id = versions.filter("change_op = 'U'").orderBy("change_seq").first().vehicle_id
    rows = versions.filter(F.col("vehicle_id") == changed_id).orderBy("valid_from").collect()
    assert rows[0].change_op == "I" and not rows[0].is_current and rows[0].valid_to == rows[1].valid_from
    assert rows[-1].is_current and rows[-1].valid_to is None
    assert rows[1].price < rows[0].price


def test_gold_has_one_current_row_per_vehicle(spark, loaded):
    lake, _ = loaded
    dim = spark.read.format("delta").load(str(lake["gold"] / "incremental" / "dim_vehicle"))
    assert dim.filter("vehicle_key <> -1").count() == 12
    assert dim.filter("vehicle_key <> -1").select("vehicle_id").distinct().count() == 12
    assert dim.filter("vehicle_key = -1").count() == 1


def test_rerun_without_a_new_day_changes_nothing(spark, loaded, tmp_path):
    lake = {name: Path(shutil.copytree(path, tmp_path / name)) for name, path in loaded[0].items()}
    versions = history(spark, lake).count()
    exits = [run_step(spark, lake, step) for step in STEPS[:3]]
    assert exits == ["nothing to extract", "nothing to ingest", "nothing to apply"]
    assert history(spark, lake).count() == versions


def test_bronze_load_repeated_after_a_crash_changes_nothing_in_silver(spark, loaded, tmp_path):
    """Data written, watermark not moved: the next run loads the same day again; Silver skips it by change_seq."""
    lake = {name: Path(shutil.copytree(path, tmp_path / name)) for name, path in loaded[0].items()}
    before = sorted(history(spark, lake).collect())
    local_pipeline.namespace(spark, **lake)["write_watermark"]("source_to_bronze", DAYS[-2])
    assert run_step(spark, lake, STEPS[1]) is None              # the last day is loaded into Bronze again
    assert run_step(spark, lake, STEPS[2]) == "nothing to apply"
    assert sorted(history(spark, lake).collect()) == before


def test_bronze_load_needs_the_source(spark, tmp_path):
    lake = make_lake(spark, tmp_path, build())
    with pytest.raises(AssertionError, match="run 12_incremental_extract_source first"):
        run_step(spark, lake, STEPS[1])
