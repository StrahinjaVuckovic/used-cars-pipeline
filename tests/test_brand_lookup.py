"""Tests for notebooks/02_silver_brand_lookup.py (cleaned brand lookup → silver/brand_lookup)."""

import datetime

import pytest

from conftest import INGESTION_DATE
from notebook_utils import load_config_and_utils, run_notebook, run_notebook_cells_until

NOTEBOOK = "02_silver_brand_lookup.py"
CLEANING_UTILS = "utils/vehicle_listings_cleaning.py"

EXPECTED_DTYPES = {
    "model_normalized": "string",
    "manufacturer": "string",
    "ingestion_date": "date",
    "source_file_path": "string",
    "silver_processed_at": "timestamp",
}

LOOKUP_ROWS = [
    ("tacoma", "toyota"),
    ("Tacoma", "Toyota"),              # Title Case copy of the row above (91 such rows in the real lookup)
    ("silverado  1500", "chevrolet"),  # extra space in the lookup
    ("astra", "saturn"),
    ("astra", "Opel"),                 # conflict: one model, two manufacturers
    ("range rover", "land rover"),
    ("NaN", "ford"),                   # "no data" model
]


def lookup_namespace(spark, bronze, silver, dev_mode="false", ingestion_date="latest") -> dict:
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze),
        silver_path=str(silver),
        widget_values={"ingestion_date": ingestion_date, "dev_mode": dev_mode},
    )
    return run_notebook(CLEANING_UTILS, namespace)


def run_brand_lookup(spark, bronze, silver, dev_mode="false", ingestion_date="latest") -> dict:
    return run_notebook(NOTEBOOK, lookup_namespace(spark, bronze, silver, dev_mode, ingestion_date))


@pytest.fixture(scope="module")
def nb(spark, tmp_path_factory):
    """Namespace with the notebook's functions (cells before reading Bronze)."""
    namespace = lookup_namespace(spark, tmp_path_factory.mktemp("unused-bronze"), tmp_path_factory.mktemp("unused-silver"))
    return run_notebook_cells_until(NOTEBOOK, namespace, "## Read Bronze and clean")


def test_clean_brand_lookup_normalizes_deduplicates_and_drops_conflicts(spark, nb):
    raw = spark.createDataFrame(LOOKUP_ROWS, "model STRING, manufacturer STRING")
    lookup = {r.model_normalized: r.manufacturer for r in nb["clean_brand_lookup"](raw).collect()}
    assert lookup == {"tacoma": "toyota", "silverado 1500": "chevrolet", "range rover": "land rover"}


def test_conflicting_models_lists_every_manufacturer(spark, nb):
    raw = spark.createDataFrame(LOOKUP_ROWS, "model STRING, manufacturer STRING")
    conflicts = nb["conflicting_models"](nb["normalize_brand_lookup"](raw)).collect()
    assert [(r.model_normalized, r.manufacturers) for r in conflicts] == [("astra", ["opel", "saturn"])]


def test_full_run_on_the_real_lookup(spark, bronze_sample, silver_dir, catalog_untouched):
    namespace = run_brand_lookup(spark, bronze_sample, silver_dir)  # the notebook's own asserts run here

    table = spark.read.format("delta").load(str(silver_dir / "brand_lookup"))
    # Profiling of the real lookup: 23,848 rows → 23,757 normalized models, minus the conflicting "astra".
    assert table.count() == 23756
    assert table.select("manufacturer").distinct().count() == 44
    assert table.groupBy("model_normalized").count().filter("count > 1").count() == 0
    assert table.columns == namespace["BRAND_LOOKUP_COLUMNS"]
    assert dict(table.dtypes) == EXPECTED_DTYPES
    assert {r.ingestion_date for r in table.select("ingestion_date").distinct().collect()} == {
        datetime.date.fromisoformat(INGESTION_DATE)
    }
    assert table.filter("source_file_path NOT LIKE '%model_to_brand_lookup.csv'").count() == 0
    assert table.filter("model_normalized = 'astra'").count() == 0
    assert spark.sql(f"DESCRIBE DETAIL delta.`{silver_dir / 'brand_lookup'}`").first().numFiles == 1

    assert namespace["raw_count"] == 23848
    assert [r.model_normalized for r in namespace["conflicts_df"].collect()] == ["astra"]
    assert len(namespace["display"].results) == 3  # every report cell ran without errors


def test_dev_mode_writes_the_full_lookup_to_the_dev_table(spark, bronze_sample, silver_dir, catalog_untouched):
    run_brand_lookup(spark, bronze_sample, silver_dir, dev_mode="true")
    assert spark.read.format("delta").load(str(silver_dir / "brand_lookup_dev")).count() == 23756
    assert not (silver_dir / "brand_lookup").exists()


def test_unknown_ingestion_date_fails(spark, bronze_sample, silver_dir):
    with pytest.raises(ValueError, match="Ingestion date 1999-01-01 not found for Lookup"):
        run_brand_lookup(spark, bronze_sample, silver_dir, ingestion_date="1999-01-01")
    assert not (silver_dir / "brand_lookup").exists()
