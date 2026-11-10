"""
Tests for the vehicle listings pipeline: the shared cleaning code (notebooks/utils/vehicle_listings_cleaning.py),
the four per-source stage notebooks (02_silver_stage_<source>_source.py) and the union + brand mapping notebook
(03_silver_vehicle_listings.py). The brand lookup notebook itself is tested in test_brand_lookup.py.
"""

import csv
import datetime
import shutil
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from conftest import INGESTION_DATE, SAMPLE_ROWS
from notebook_utils import load_config_and_utils, run_notebook, run_notebook_cells_until

CLEANING_UTILS = "utils/vehicle_listings_cleaning.py"
LISTINGS_NOTEBOOK = "03_silver_vehicle_listings.py"
BRAND_LOOKUP_NOTEBOOK = "02_silver_brand_lookup.py"
SOURCES = ["First", "Second", "Third", "Fourth"]


def stage_notebook(source_system: str) -> str:
    return f"02_silver_stage_{source_system.lower()}_source.py"


def first_csv_path(bronze: Path) -> Path:
    return bronze / "FirstSourceSystem" / f"ingestion_date={INGESTION_DATE}" / "FirstSourceSystem.csv"


def copy_bronze_and_edit_first_row(bronze_sample: Path, tmp_path: Path, edit) -> Path:
    """
    Copy the Bronze sample and change the first data row of FirstSourceSystem.csv with `edit(row, header)`.
    The csv module (newline="") keeps quoted values, including bare \\r characters, intact; rows are written
    back with Windows line endings like the real file.
    """
    bronze = tmp_path / "bronze"
    shutil.copytree(bronze_sample, bronze)
    path = first_csv_path(bronze)
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    edit(rows[1], rows[0])
    with path.open("w", encoding="utf-8", newline="") as f:
        csv.writer(f, lineterminator="\r\n").writerows(rows)
    return bronze


def first_row_value(bronze: Path, column: str) -> str:
    with first_csv_path(bronze).open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        return next(reader)[header.index(column)]


def vehicle_namespace(spark, bronze: Path, silver: Path, dev_mode: str = "false", ingestion_date: str = "latest") -> dict:
    """Fresh notebook namespace with config, silver_transformations and the shared vehicle cleaning code."""
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze),
        silver_path=str(silver),
        widget_values={"ingestion_date": ingestion_date, "dev_mode": dev_mode},
    )
    return run_notebook(CLEANING_UTILS, namespace)


def run_stage(spark, bronze: Path, silver: Path, source_system: str, dev_mode: str = "false") -> dict:
    return run_notebook(stage_notebook(source_system), vehicle_namespace(spark, bronze, silver, dev_mode))


def run_brand_lookup(spark, bronze: Path, silver: Path, dev_mode: str = "false") -> dict:
    return run_notebook(BRAND_LOOKUP_NOTEBOOK, vehicle_namespace(spark, bronze, silver, dev_mode))


def run_listings(spark, bronze: Path, silver: Path, dev_mode: str = "false", ingestion_date: str = "latest") -> dict:
    return run_notebook(LISTINGS_NOTEBOOK, vehicle_namespace(spark, bronze, silver, dev_mode, ingestion_date))


def run_listings_pipeline(spark, bronze: Path, silver: Path, dev_mode: str = "false") -> dict:
    """The four stage notebooks and the brand lookup, then the union notebook (like the Databricks Job).
    Returns the 03 namespace."""
    for source_system in SOURCES:
        run_stage(spark, bronze, silver, source_system, dev_mode)
    run_brand_lookup(spark, bronze, silver, dev_mode)
    return run_listings(spark, bronze, silver, dev_mode)

EXPECTED_DTYPES = {
    "vehicle_id": "string", "source_system": "string", "source_system_uuid": "string",
    "platform_id": "int", "platform_name": "string", "listing_url": "string", "region": "string",
    "region_url": "string", "state": "string", "latitude": "double", "longitude": "double",
    "price": "decimal(18,2)", "currency": "string", "manufacture_year": "int", "model": "string",
    "model_normalized": "string", "manufacturer": "string", "manufacturer_source": "string", "condition": "string", "cylinders": "int", "fuel": "string",
    "odometer": "int", "title_status": "string", "transmission": "string", "vin": "string",
    "drive": "string", "size": "string", "body_type": "string", "paint_color": "string",
    "image_url": "string", "description": "string", "posting_date": "date", "posting_date_raw": "string",
    "ingestion_date": "date", "source_file_path": "string", "silver_processed_at": "timestamp",
    "dq_issues": "array<string>", "is_valid": "boolean",
}
BRAND_COLUMNS = ["manufacturer", "manufacturer_source"]
# clean_vehicle_listings runs per source; the brand columns are added after the union.
CLEANED_DTYPES = {c: t for c, t in EXPECTED_DTYPES.items() if c not in BRAND_COLUMNS}


@pytest.fixture(scope="module")
def nb(spark, tmp_path_factory):
    """Namespace with config, the shared cleaning code and the 03 cells up to (not including) the pipeline run."""
    namespace = vehicle_namespace(
        spark, tmp_path_factory.mktemp("unused-bronze"), tmp_path_factory.mktemp("unused-silver")
    )
    run_notebook_cells_until(LISTINGS_NOTEBOOK, namespace, "## Read the four stage tables")
    # clean_brand_lookup lives in the brand lookup notebook; the brand mapping tests need a cleaned lookup.
    lookup_namespace = vehicle_namespace(
        spark, tmp_path_factory.mktemp("unused-bronze"), tmp_path_factory.mktemp("unused-silver")
    )
    run_notebook_cells_until(BRAND_LOOKUP_NOTEBOOK, lookup_namespace, "## Read Bronze and clean")
    namespace["clean_brand_lookup"] = lookup_namespace["clean_brand_lookup"]
    return namespace


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


def clean(spark, nb, rows, source_system="First", ingestion_date=INGESTION_DATE):
    columns = list(raw_row().keys())
    schema = ", ".join(f"`{c}` STRING" for c in columns)
    raw_df = spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], schema)
    return nb["clean_vehicle_listings"](raw_df, source_system, ingestion_date)


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
    assert df.columns == nb["STAGE_COLUMNS"]
    assert nb["STAGE_COLUMNS"] == [c for c in nb["VEHICLE_LISTINGS_COLUMNS"] if c not in BRAND_COLUMNS]
    assert dict(df.dtypes) == CLEANED_DTYPES
    assert nb["VEHICLE_LISTINGS_COLUMNS"] == list(EXPECTED_DTYPES)  # full table incl. brand columns


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


@pytest.mark.parametrize("year, valid", [("2011.0", True), ("2012.0", False)])
def test_without_posting_date_the_year_limit_follows_the_ingestion_date(spark, nb, year, valid):
    """Idempotency: the limit is ingestion year + 1, not the year of the run, so re-runs give the same result."""
    row = clean(spark, nb, [raw_row(year=year, posting_date=None)], ingestion_date="2010-06-01").first()
    assert ("invalid_manufacture_year" not in row.dq_issues) is valid


def test_unexpected_currencies(spark, nb):
    df = spark.createDataFrame([("EUR",), ("GBP",), (None,)], "currency STRING")
    assert nb["unexpected_currencies"](df, ["EUR"]) == ["GBP"]
    assert nb["unexpected_currencies"](df, ["EUR", "GBP"]) == []


def test_stage_table_path(nb):
    assert nb["stage_table_path"]("First", False).endswith("/vehicle_listings_stage/first")
    assert nb["stage_table_path"]("Fourth", True).endswith("/vehicle_listings_stage_dev/fourth")


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


# ---------- brand mapping ----------

LOOKUP_ROWS = [
    ("tacoma", "toyota"),
    ("Tacoma", "Toyota"),            # Title Case copy of the row above (91 such rows in the real lookup)
    ("silverado  1500", "chevrolet"),  # extra space in the lookup
    ("astra", "saturn"),
    ("astra", "Opel"),               # conflict: one model, two manufacturers
    ("range rover", "land rover"),
    ("NaN", "ford"),                 # "no data" model
]


def brand_lookup(spark, nb):
    raw = spark.createDataFrame(LOOKUP_ROWS, "model STRING, manufacturer STRING")
    return nb["clean_brand_lookup"](raw)


def listings(spark, rows):
    return spark.createDataFrame(rows, "vehicle_id STRING, model_normalized STRING, listing_url STRING")


def test_add_manufacturer_lookup_then_url_then_null(spark, nb):
    df = listings(spark, [
        ("v1", "tacoma", "https://x.craigslist.org/cto/d/city-2015-toyota-tacoma/1.html"),
        ("v2", "lac xt5 crossover", "https://x.craigslist.org/ctd/d/new-castle-2018-cadillac-xt5/2.html"),
        ("v3", "astra", "https://x.craigslist.org/cto/d/city-2008-saturn-astra/3.html"),
        ("v4", "freightliner cascadia", "https://x.craigslist.org/cto/d/florence-refrigerated-box-truck/4.html"),
        ("v5", None, None),
        ("v6", "defender", "https://x.craigslist.org/cto/d/city-1995-land-rover-defender/6.html"),
    ])
    extra = spark.createDataFrame([("xt5", "cadillac"), ("ion", "saturn")], "m STRING, f STRING")
    lookup = brand_lookup(spark, nb).union(extra)
    result = {r.vehicle_id: (r.manufacturer, r.manufacturer_source)
              for r in nb["add_manufacturer"](df, lookup).collect()}
    assert result == {
        "v1": ("toyota", "lookup"),
        "v2": ("cadillac", "listing_url"),   # model text lost its first characters; the URL still has the brand
        "v3": ("saturn", "listing_url"),     # conflicting model is not mapped by the lookup
        "v4": (None, None),                  # brand not in the lookup and not in the URL
        "v5": (None, None),
        "v6": ("land rover", "listing_url"),  # "land-rover" in the URL, not "rover"
    }


def test_add_manufacturer_never_multiplies_rows(spark, nb):
    df = listings(spark, [("v1", "tacoma", None), ("v2", "silverado 1500", None)])
    result = nb["add_manufacturer"](df, brand_lookup(spark, nb))
    assert result.count() == 2


def test_url_match_needs_whole_word(spark, nb):
    df = listings(spark, [("v1", "unknown", "https://x.craigslist.org/cto/d/city-rammed-truck/1.html")])
    row = nb["add_manufacturer"](df, spark.createDataFrame([("1500", "ram")], "model_normalized STRING, manufacturer STRING")).first()
    assert (row.manufacturer, row.manufacturer_source) == (None, None)


# ---------- real samples from every source ----------


@pytest.mark.parametrize("source_system", ["First", "Second", "Third", "Fourth"])
def test_real_sample_per_source(spark, nb, bronze_sample, source_system):
    source = nb["VEHICLE_SOURCES"][source_system]
    path = str(bronze_sample / source["folder"] / f"ingestion_date={INGESTION_DATE}")
    raw_df = nb["read_bronze_files"](path, source["format"], source.get("options"))
    df = nb["clean_vehicle_listings"](raw_df, source_system, INGESTION_DATE)

    assert df.count() == SAMPLE_ROWS
    assert dict(df.dtypes) == CLEANED_DTYPES
    assert df.filter(F.col("posting_date_raw").isNotNull() & F.col("posting_date").isNull()).count() == 0
    assert df.select("vehicle_id").distinct().count() == SAMPLE_ROWS
    assert df.filter(F.col("cylinders").isNotNull()).count() > 0
    currencies = {r.currency for r in df.select("currency").distinct().collect()}
    assert currencies - {None} <= {"USD", "GBP", "EUR", "AUD"}


# ---------- full pipeline run (4 stage notebooks + 03) ----------


def test_full_pipeline_run(spark, bronze_sample, silver_dir, catalog_untouched):
    namespace = run_listings_pipeline(spark, bronze_sample, silver_dir)  # the notebooks' own asserts run here

    for source_system in SOURCES:
        stage_path = silver_dir / "vehicle_listings_stage" / source_system.lower()
        stage = spark.read.format("delta").load(str(stage_path))
        assert stage.count() == SAMPLE_ROWS
        assert {r.source_system for r in stage.select("source_system").distinct().collect()} == {source_system}
        assert spark.sql(f"DESCRIBE DETAIL delta.`{stage_path}`").first().numFiles == 1

    table = spark.read.format("delta").load(str(silver_dir / "vehicle_listings"))
    assert table.count() == 4 * SAMPLE_ROWS
    assert {r.source_system: r["count"] for r in table.groupBy("source_system").count().collect()} == {
        "First": SAMPLE_ROWS, "Second": SAMPLE_ROWS, "Third": SAMPLE_ROWS, "Fourth": SAMPLE_ROWS,
    }
    assert table.columns == namespace["VEHICLE_LISTINGS_COLUMNS"]
    assert dict(table.dtypes) == EXPECTED_DTYPES
    assert (silver_dir / "vehicle_listings" / "_delta_log").is_dir()
    assert (silver_dir / "brand_lookup" / "_delta_log").is_dir()
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{silver_dir / 'vehicle_listings'}`").first()
    assert detail.numFiles == namespace["OUTPUT_FILES"]  # no small-files problem
    assert namespace["stage_ingestion_date"] == INGESTION_DATE

    currencies = {
        r.source_system: r.currencies
        for r in table.groupBy("source_system").agg(F.collect_set("currency").alias("currencies")).collect()
    }
    assert currencies["Second"] == ["EUR"] and currencies["Third"] == ["AUD"] and currencies["Fourth"] == ["GBP"]

    sources = {r.manufacturer_source: r["count"] for r in table.groupBy("manufacturer_source").count().collect()}
    assert set(sources) <= {"lookup", "listing_url", None}
    assert sources["lookup"] / table.count() > 0.9  # profiling: ~96% of listings match the lookup
    assert table.filter("manufacturer_source IS NOT NULL AND manufacturer IS NULL").count() == 0
    assert len(namespace["display"].results) >= 6  # every report cell ran without errors


@pytest.mark.parametrize("source_system", SOURCES)
def test_stage_notebook_has_no_unexpected_currency(spark, bronze_sample, tmp_path, source_system, capsys):
    namespace = run_stage(spark, bronze_sample, tmp_path / "silver", source_system)
    assert namespace["other_currencies"] == []
    assert "WARNING" not in capsys.readouterr().out


def test_listings_notebook_needs_every_stage_table(spark, bronze_sample, tmp_path, catalog_untouched):
    silver = tmp_path / "silver"
    for source_system in ["First", "Second", "Third"]:
        run_stage(spark, bronze_sample, silver, source_system)
    with pytest.raises(AssertionError, match="No stage table for Fourth .* run 02_silver_stage_fourth_source first"):
        run_listings(spark, bronze_sample, silver)
    assert not (silver / "vehicle_listings").exists()


def test_listings_notebook_rejects_mixed_ingestion_dates(spark, bronze_sample, tmp_path, catalog_untouched):
    silver = tmp_path / "silver"
    for source_system in SOURCES:
        run_stage(spark, bronze_sample, silver, source_system)
    third = str(silver / "vehicle_listings_stage" / "third")
    older = spark.read.format("delta").load(third).withColumn("ingestion_date", F.to_date(F.lit("2026-11-02")))
    older.write.format("delta").mode("overwrite").save(third)

    with pytest.raises(AssertionError, match="different ingestion dates"):
        run_listings(spark, bronze_sample, silver)
    assert not (silver / "vehicle_listings").exists()


def test_listings_notebook_checks_the_requested_ingestion_date(spark, bronze_sample, tmp_path, catalog_untouched):
    silver = tmp_path / "silver"
    for source_system in SOURCES:
        run_stage(spark, bronze_sample, silver, source_system)
    with pytest.raises(AssertionError, match=f"built from ingestion_date={INGESTION_DATE}, not 2026-11-05"):
        run_listings(spark, bronze_sample, silver, ingestion_date="2026-11-05")


def run_all_stages(spark, bronze, silver):
    for source_system in SOURCES:
        run_stage(spark, bronze, silver, source_system)


def test_listings_notebook_needs_the_brand_lookup(spark, bronze_sample, tmp_path, catalog_untouched):
    silver = tmp_path / "silver"
    run_all_stages(spark, bronze_sample, silver)
    with pytest.raises(AssertionError, match="No brand lookup at .* run 02_silver_brand_lookup first"):
        run_listings(spark, bronze_sample, silver)
    assert not (silver / "vehicle_listings").exists()


def test_listings_notebook_rejects_a_brand_lookup_from_another_ingestion(
    spark, bronze_sample, tmp_path, catalog_untouched
):
    silver = tmp_path / "silver"
    run_all_stages(spark, bronze_sample, silver)
    run_brand_lookup(spark, bronze_sample, silver)
    lookup = str(silver / "brand_lookup")
    older = spark.read.format("delta").load(lookup).withColumn("ingestion_date", F.to_date(F.lit("2026-11-02")))
    older.write.format("delta").mode("overwrite").save(lookup)

    with pytest.raises(AssertionError, match=f"Brand lookup was built from ingestion_date=2026-11-02, the stage tables from {INGESTION_DATE}"):
        run_listings(spark, bronze_sample, silver)
    assert not (silver / "vehicle_listings").exists()


def test_full_pipeline_continues_with_unparsed_dates(spark, bronze_sample, tmp_path, catalog_untouched):
    """An impossible date in Bronze must not stop the pipeline: the row is written and flagged (option b)."""
    bronze = copy_bronze_and_edit_first_row(
        bronze_sample, tmp_path, lambda row, header: row.__setitem__(header.index("posting_date"), "02/30/2021")
    )
    bad_vehicle_id = first_row_value(bronze, "id")

    namespace = run_listings_pipeline(spark, bronze, tmp_path / "silver")

    table = spark.read.format("delta").load(str(tmp_path / "silver" / "vehicle_listings"))
    assert table.count() == 4 * SAMPLE_ROWS
    row = table.filter(F.col("vehicle_id") == bad_vehicle_id).first()
    assert row.posting_date is None and row.posting_date_raw == "02/30/2021"
    assert "unparsed_posting_date" in row.dq_issues
    assert namespace["unparsed_dates"] == 1


def test_dev_mode_writes_to_dev_tables(spark, bronze_sample, silver_dir, catalog_untouched):
    run_listings_pipeline(spark, bronze_sample, silver_dir, dev_mode="true")
    assert (silver_dir / "vehicle_listings_stage_dev" / "first" / "_delta_log").is_dir()
    assert (silver_dir / "vehicle_listings_dev" / "_delta_log").is_dir()
    assert (silver_dir / "brand_lookup_dev" / "_delta_log").is_dir()
    assert not (silver_dir / "vehicle_listings_stage").exists()
    assert not (silver_dir / "brand_lookup").exists()
    assert not (silver_dir / "vehicle_listings").exists()


def test_bare_carriage_return_in_description_does_not_split_rows(spark, bronze_sample, tmp_path, catalog_untouched):
    """Regression test for the cluster run where FirstSourceSystem got 1,186 extra rows."""
    description = "Line one\rLine two\rLine three"
    bronze = copy_bronze_and_edit_first_row(
        bronze_sample, tmp_path, lambda row, header: row.__setitem__(header.index("description"), description)
    )
    vehicle_id = first_row_value(bronze, "id")

    run_stage(spark, bronze, tmp_path / "silver", "First")

    stage = spark.read.format("delta").load(str(tmp_path / "silver" / "vehicle_listings_stage" / "first"))
    assert stage.count() == SAMPLE_ROWS
    assert stage.filter(F.col("vehicle_id") == vehicle_id).first().description == description


def test_malformed_vehicle_id_stops_the_stage_notebook(spark, bronze_sample, tmp_path, catalog_untouched):
    """A vehicle_id that is not 12 hex characters means a wrong parse: the notebook must stop before writing."""
    bronze = copy_bronze_and_edit_first_row(
        bronze_sample, tmp_path, lambda row, header: row.__setitem__(header.index("id"), "broken text")
    )
    with pytest.raises(AssertionError, match="not 12 hex characters"):
        run_stage(spark, bronze, tmp_path / "silver", "First")
    assert not (tmp_path / "silver" / "vehicle_listings_stage" / "first").exists()


def test_bronze_sample_matches_real_first_source_format():
    """The sample copy keeps Windows line endings, like the real file."""
    import itertools
    from notebook_utils import DATA_DIR
    if not DATA_DIR.exists():
        pytest.skip("data/ folder with the source files is not available")
    with (DATA_DIR / "FirstSourceSystem.csv").open("rb") as f:
        first_line = next(itertools.islice(f, 1))
    assert first_line.endswith(b"\r\n")
