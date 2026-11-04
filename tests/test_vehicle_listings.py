"""Tests for notebooks/02_silver_vehicle_listings.py (clean_vehicle_listings and the full notebook run)."""

import csv
import datetime
import shutil
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from conftest import INGESTION_DATE, SAMPLE_ROWS
from notebook_utils import load_config_and_utils, run_notebook, run_notebook_cells_until

NOTEBOOK = "02_silver_vehicle_listings.py"

EXPECTED_DTYPES = {
    "vehicle_id": "string", "source_system": "string", "source_system_uuid": "string",
    "platform_id": "int", "platform_name": "string", "listing_url": "string", "region": "string",
    "region_url": "string", "state": "string", "latitude": "double", "longitude": "double",
    "price": "decimal(18,2)", "currency": "string", "manufacture_year": "int", "model": "string",
    "model_normalized": "string", "condition": "string", "cylinders": "int", "fuel": "string",
    "odometer": "int", "title_status": "string", "transmission": "string", "vin": "string",
    "drive": "string", "size": "string", "body_type": "string", "paint_color": "string",
    "image_url": "string", "description": "string", "posting_date": "date", "posting_date_raw": "string",
    "ingestion_date": "date", "source_file_path": "string", "silver_processed_at": "timestamp",
    "dq_issues": "array<string>", "is_valid": "boolean",
}


@pytest.fixture(scope="module")
def nb(spark, tmp_path_factory):
    """Namespace with config, utils and the 02 notebook cells up to (not including) the pipeline run."""
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(tmp_path_factory.mktemp("unused-bronze")),
        silver_path=str(tmp_path_factory.mktemp("unused-silver")),
        widget_values={"ingestion_date": "latest", "dev_mode": "false"},
    )
    return run_notebook_cells_until(NOTEBOOK, namespace, "## Read and clean each source")


def raw_row(**overrides) -> dict:
    """A valid raw listing as it arrives from the CSV source (every value is a string)."""
    row = {
        "id": "v1", "url": "https://prescott.craigslist.org/x.html", "region": "Prescott",
        "region_url": "https://prescott.craigslist.org", "price": "15000.0", "year": "2015.0",
        "model": "  F-150   XL ", "condition": "Excellent", "cylinders": "6 cylinders", "fuel": "Gas",
        "odometer": "85000.0", "title_status": "clean", "transmission": "automatic",
        "VIN": "1FTFW1ET5DFC10312", "drive": "4wd", "size": "full-size", "type": "truck",
        "paint_color": "white", "image_url": "https://images.craigslist.org/x.jpg", "description": " nice car ",
        "state": "ca", "lat": "34.05", "long": "-118.24", "posting_date": "2021-04-12",
        "source_system_id": "2", "source_system_name": "CarGurus", "currency": "gbp",
        "SourceSystemId": "8c678a99-76b5-4102-ac36-61fcab2a014e", "source_file_path": "file:/bronze/First.csv",
    }
    row.update(overrides)
    return row


def clean(spark, nb, rows, source_system="First"):
    columns = list(raw_row().keys())
    schema = ", ".join(f"`{c}` STRING" for c in columns)
    raw_df = spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], schema)
    return nb["clean_vehicle_listings"](raw_df, source_system, INGESTION_DATE)


def clean_one(spark, nb, **overrides):
    return clean(spark, nb, [raw_row(**overrides)]).first()


# ---------- valid row and standardization ----------


def test_valid_row_is_standardized(spark, nb):
    row = clean_one(spark, nb)
    assert row.dq_issues == [] and row.is_valid is True
    assert row.vehicle_id == "v1"
    assert row.source_system == "First"
    assert row.platform_id == 2 and row.platform_name == "CarGurus"
    assert row.price == Decimal("15000.00")
    assert row.currency == "GBP"
    assert row.manufacture_year == 2015
    assert row.odometer == 85000
    assert row.model == "F-150 XL" and row.model_normalized == "f-150 xl"
    assert row.condition == "excellent" and row.fuel == "gas" and row.region == "prescott"
    assert row.cylinders == 6
    assert row.state == "CA" and row.vin == "1FTFW1ET5DFC10312"
    assert row.body_type == "truck" and row.description == "nice car"
    assert row.posting_date == datetime.date(2021, 4, 12) and row.posting_date_raw == "2021-04-12"
    assert row.ingestion_date == datetime.date(2026, 11, 3)


def test_final_schema_matches_documentation(spark, nb):
    df = clean(spark, nb, [raw_row()])
    assert df.columns == nb["VEHICLE_LISTINGS_COLUMNS"]
    assert dict(df.dtypes) == EXPECTED_DTYPES


@pytest.mark.parametrize(
    "platform, raw_date",
    [
        ("CarGurus", "2021-04-12"),
        ("Craigslist", "04/12/2021"),
        ("AutoScout24", "12.04.2021"),
        ("CarsGuide", "April 12, 2021"),
    ],
)
def test_all_posting_date_formats(spark, nb, platform, raw_date):
    row = clean_one(spark, nb, posting_date=raw_date, source_system_name=platform)
    assert row.posting_date == datetime.date(2021, 4, 12)
    assert "unparsed_posting_date" not in row.dq_issues


def test_unknown_posting_date_format_is_flagged(spark, nb):
    row = clean_one(spark, nb, posting_date="2021/04/12")
    assert row.posting_date is None and row.posting_date_raw == "2021/04/12"
    assert "unparsed_posting_date" in row.dq_issues


@pytest.mark.parametrize("raw_date", ["02/30/2021", "2021-13-01", "31.02.2021", "Februar 30, 2021"])
def test_impossible_calendar_date_is_flagged_not_failing(spark, nb, raw_date):
    row = clean_one(spark, nb, posting_date=raw_date)
    assert row.posting_date is None and row.posting_date_raw == raw_date
    assert "unparsed_posting_date" in row.dq_issues


@pytest.mark.parametrize(
    "raw, expected",
    [("6 cylinders", 6), ("12 cylinders", 12), ("4 Cylinders", 4), ("other", None), (None, None), ("", None)],
)
def test_cylinders(spark, nb, raw, expected):
    row = clean_one(spark, nb, cylinders=raw)
    assert row.cylinders == expected
    assert row.dq_issues == []


def test_null_tokens_become_null_without_dq_issue(spark, nb):
    row = clean_one(spark, nb, condition="null", fuel="NaN", price="", VIN="  ", drive="None")
    assert (row.condition, row.fuel, row.price, row.vin, row.drive) == (None, None, None, None, None)
    assert row.dq_issues == []  # missing data is not invalid data


# ---------- validation and dq_issues ----------


@pytest.mark.parametrize(
    "overrides, column, issue",
    [
        ({"price": "0"}, "price", "invalid_price"),
        ({"price": "-10"}, "price", "invalid_price"),
        ({"price": "3024942282.0"}, "price", "invalid_price"),
        ({"currency": "XYZ"}, "currency", "invalid_currency"),
        ({"year": "1899.0"}, "manufacture_year", "invalid_manufacture_year"),
        ({"year": "2100.0"}, "manufacture_year", "invalid_manufacture_year"),
        ({"year": "2023.0", "posting_date": "2021-04-12"}, "manufacture_year", "invalid_manufacture_year"),
        ({"odometer": "10000000.0"}, "odometer", "invalid_odometer"),
        ({"odometer": "-1"}, "odometer", "invalid_odometer"),
        ({"lat": "199.91"}, "latitude", "invalid_latitude"),
        ({"long": "-399.88"}, "longitude", "invalid_longitude"),
        ({"VIN": "123"}, "vin", "invalid_vin"),
        ({"VIN": "1FTFW1ET5DFC1031O"}, "vin", "invalid_vin"),  # letter O is not allowed in a VIN
        ({"state": "California"}, "state", "invalid_state"),
    ],
)
def test_invalid_value_is_nulled_and_flagged(spark, nb, overrides, column, issue):
    row = clean_one(spark, nb, **overrides)
    assert row[column] is None
    assert row.dq_issues == [issue]
    assert row.is_valid is False


def test_manufacture_year_one_year_after_posting_is_valid(spark, nb):
    row = clean_one(spark, nb, year="2022.0", posting_date="2021-04-12")
    assert row.manufacture_year == 2022 and row.dq_issues == []


def test_zero_odometer_is_flagged_but_kept(spark, nb):
    row = clean_one(spark, nb, odometer="0.0")
    assert row.odometer == 0
    assert row.dq_issues == ["zero_odometer"]


def test_multiple_issues_are_collected(spark, nb):
    row = clean_one(spark, nb, price="0", VIN="bad", state="XXX")
    assert sorted(row.dq_issues) == ["invalid_price", "invalid_state", "invalid_vin"]


def test_rows_are_never_dropped(spark, nb):
    rows = [raw_row(id=f"v{i}", price="0", year="2100.0", lat="999") for i in range(5)]
    assert clean(spark, nb, rows).count() == 5


# ---------- deduplication ----------


def test_duplicates_keep_latest_posting_date(spark, nb):
    rows = [
        raw_row(id="dup", posting_date="2021-01-01", price="1000.0"),
        raw_row(id="dup", posting_date="2021-06-01", price="2000.0"),
        raw_row(id="other"),
    ]
    result = {r.vehicle_id: r for r in clean(spark, nb, rows).collect()}
    assert set(result) == {"dup", "other"}
    assert result["dup"].price == Decimal("2000.00")


def test_missing_bronze_column_fails(spark, nb):
    raw_df = spark.createDataFrame([("v1",)], "id STRING")
    with pytest.raises(AssertionError, match="missing Bronze columns"):
        nb["clean_vehicle_listings"](raw_df, "First", INGESTION_DATE)


# ---------- real samples from every source ----------


@pytest.mark.parametrize("source_system", ["First", "Second", "Third", "Fourth"])
def test_real_sample_per_source(spark, nb, bronze_sample, source_system):
    source = nb["VEHICLE_SOURCES"][source_system]
    path = str(bronze_sample / source["folder"] / f"ingestion_date={INGESTION_DATE}")
    df = nb["clean_vehicle_listings"](nb["read_bronze_files"](path, source["format"]), source_system, INGESTION_DATE)

    assert df.count() == SAMPLE_ROWS
    assert dict(df.dtypes) == EXPECTED_DTYPES
    assert df.filter(F.col("posting_date_raw").isNotNull() & F.col("posting_date").isNull()).count() == 0
    assert df.select("vehicle_id").distinct().count() == SAMPLE_ROWS
    assert df.filter(F.col("cylinders").isNotNull()).count() > 0
    currencies = {r.currency for r in df.select("currency").distinct().collect()}
    assert currencies - {None} <= {"USD", "GBP", "EUR", "AUD"}


# ---------- full notebook run ----------


def test_full_notebook_run(spark, bronze_sample, silver_dir, catalog_untouched):
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze_sample),
        silver_path=str(silver_dir),
        widget_values={"ingestion_date": "latest", "dev_mode": "false"},
    )
    run_notebook(NOTEBOOK, namespace)  # the notebook's own asserts run here

    table = spark.read.format("delta").load(str(silver_dir / "vehicle_listings"))
    assert table.count() == 4 * SAMPLE_ROWS
    assert {r.source_system: r["count"] for r in table.groupBy("source_system").count().collect()} == {
        "First": SAMPLE_ROWS, "Second": SAMPLE_ROWS, "Third": SAMPLE_ROWS, "Fourth": SAMPLE_ROWS,
    }
    assert (silver_dir / "vehicle_listings" / "_delta_log").is_dir()

    currencies = {
        r.source_system: r.currencies
        for r in table.groupBy("source_system").agg(F.collect_set("currency").alias("currencies")).collect()
    }
    assert currencies["Second"] == ["EUR"] and currencies["Third"] == ["AUD"] and currencies["Fourth"] == ["GBP"]
    assert len(namespace["display"].results) >= 5  # every report cell ran without errors


def test_full_notebook_run_continues_with_unparsed_dates(spark, bronze_sample, tmp_path, catalog_untouched):
    """An impossible date in Bronze must not stop the notebook: the row is written and flagged (option b)."""
    bronze = tmp_path / "bronze"
    shutil.copytree(bronze_sample, bronze)
    first_csv = bronze / "FirstSourceSystem" / f"ingestion_date={INGESTION_DATE}" / "FirstSourceSystem.csv"
    lines = first_csv.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    # Rewrite one row through the csv module so quoted fields stay intact.
    rows = list(csv.reader(lines))
    rows[1][header.index("posting_date")] = "02/30/2021"
    bad_vehicle_id = rows[1][header.index("id")]
    with first_csv.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)

    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze),
        silver_path=str(tmp_path / "silver"),
        widget_values={"ingestion_date": "latest", "dev_mode": "false"},
    )
    run_notebook(NOTEBOOK, namespace)

    table = spark.read.format("delta").load(str(tmp_path / "silver" / "vehicle_listings"))
    assert table.count() == 4 * SAMPLE_ROWS
    row = table.filter(F.col("vehicle_id") == bad_vehicle_id).first()
    assert row.posting_date is None and row.posting_date_raw == "02/30/2021"
    assert "unparsed_posting_date" in row.dq_issues
    assert namespace["unparsed_dates"] == 1


def test_dev_mode_writes_to_dev_table(spark, bronze_sample, silver_dir, catalog_untouched):
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze_sample),
        silver_path=str(silver_dir),
        widget_values={"ingestion_date": "latest", "dev_mode": "true"},
    )
    run_notebook(NOTEBOOK, namespace)
    assert (silver_dir / "vehicle_listings_dev" / "_delta_log").is_dir()
    assert not (silver_dir / "vehicle_listings").exists()
