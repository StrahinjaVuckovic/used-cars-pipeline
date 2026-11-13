"""
Tests for the Gold layer: notebooks/utils/gold_helpers.py and the 07_gold_* / 08_gold_* notebooks.

The full-pipeline fixture builds Silver from the Bronze sample (like the Silver Jobs) and then runs every Gold
notebook in the order of the Gold Job. Local Spark has no Unity Catalog, so views are session temp views.
"""

import datetime
import re
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from conftest import SAMPLE_ROWS
from notebook_utils import PROJECT_ROOT, load_config_and_utils, run_notebook, run_notebook_cells_until
from test_exchange_rates import run_rates
from test_service_events import run_service_events_notebook
from test_vehicle_listings import run_listings_pipeline

DBML_PATH = PROJECT_ROOT / "documentation" / "gold_star_schema.dbml"
GOLD_NOTEBOOKS = [  # order of the Gold Job: dimensions, facts, views
    "07_gold_dim_date.py", "07_gold_dim_vehicle.py", "07_gold_dim_small.py",
    "08_gold_fact_listing.py", "08_gold_fact_service_event.py", "08_gold_fact_exchange_rate.py",
    "08_gold_views.py",
]


def gold_namespace(spark, bronze, silver, gold, dev_mode="false") -> dict:
    namespace = load_config_and_utils(
        spark, bronze_path=str(bronze), silver_path=str(silver),
        widget_values={"ingestion_date": "latest", "dev_mode": dev_mode},
    )
    namespace["GOLD_PATH"] = str(gold)
    namespace["UC_CATALOG"] = None
    run_notebook("utils/gold_helpers.py", namespace)
    return namespace


def run_gold(spark, notebook, bronze, silver, gold, dev_mode="false") -> dict:
    return run_notebook(notebook, gold_namespace(spark, bronze, silver, gold, dev_mode))


def read_gold(spark, gold, table):
    return spark.read.format("delta").load(str(gold / table))


@pytest.fixture(scope="module")
def helpers(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("gold_helpers")
    return gold_namespace(spark, tmp, tmp, tmp)


@pytest.fixture(scope="module")
def full_gold(spark, bronze_sample, tmp_path_factory):
    """Silver from the Bronze sample, then the whole Gold Job. Returns (gold path, namespaces per notebook)."""
    root = tmp_path_factory.mktemp("gold_pipeline")
    silver, gold = root / "silver", root / "gold"
    run_listings_pipeline(spark, bronze_sample, silver)
    run_service_events_notebook(spark, bronze_sample, silver)
    run_rates(spark, bronze_sample, silver)
    namespaces = {nb: run_gold(spark, nb, bronze_sample, silver, gold) for nb in GOLD_NOTEBOOKS}
    return gold, silver, namespaces


# ---------- helpers ----------


def test_surrogate_key_is_deterministic_and_minus_one_for_null(spark, helpers):
    df = spark.createDataFrame([("a1", "x"), ("a1", "x"), (None, None), ("a1", None)], "id STRING, other STRING")
    keys = [r.k for r in df.select(helpers["surrogate_key"]("id", "other").alias("k")).collect()]
    assert keys[0] == keys[1] and keys[0] != -1
    assert keys[2] == -1
    assert keys[3] not in (-1, keys[0])


def test_date_key(spark, helpers):
    df = spark.createDataFrame([(datetime.date(2026, 11, 7),), (None,)], "d DATE")
    assert [r.k for r in df.select(helpers["date_key"]("d").alias("k")).collect()] == [20261107, -1]


def test_manufacturer_display_names(spark, helpers):
    df = spark.createDataFrame([("toyota",), ("bmw",), ("mercedes-benz",), ("land rover",), (None,)], "m STRING")
    names = [r.n for r in df.select(helpers["manufacturer_display_name"]("m").alias("n")).collect()]
    assert names == ["Toyota", "BMW", "Mercedes-Benz", "Land Rover", "Unknown"]


def test_unknown_member_row(spark, helpers):
    dim = spark.createDataFrame([(5, "x", 1)], "k BIGINT, label STRING, n INT")
    unknown = helpers["with_unknown_member"](dim, "k").filter("k = -1").first()
    assert (unknown.k, unknown.label, unknown.n) == (-1, "Unknown", None)


def test_foreign_key_check_names_the_orphans(spark, helpers):
    fact = spark.createDataFrame([(1,), (2,)], "k BIGINT")
    dim = spark.createDataFrame([(1,)], "k BIGINT")
    with pytest.raises(AssertionError, match=r"missing in the dimension \(first 5: \[2\]\)"):
        helpers["assert_foreign_key"](fact, "k", dim, "k", "fact_x")


# ---------- dbml = code ----------


def dbml_tables() -> dict:
    """{table: [columns]} from the Table blocks of the .dbml file (Note lines, comments and indexes are skipped)."""
    tables, current, in_indexes = {}, None, False
    for line in DBML_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        header = re.match(r"Table (\w+)", line)
        if header:
            current = tables.setdefault(header.group(1), [])
        elif line.startswith("indexes"):
            in_indexes = True
        elif line.startswith("}"):
            if in_indexes:
                in_indexes = False
            else:
                current = None
        elif current is not None and not in_indexes and line and not line.startswith(("Note", "//")):
            current.append(line.split()[0])
    return tables


def test_dbml_diagram_matches_the_code(helpers):
    documented = dbml_tables()
    for table, columns in helpers["GOLD_COLUMNS"].items():
        assert documented.get(table) == columns, f"{table}: .dbml {documented.get(table)} != code {columns}"
    # The only documented table without code yet is the planned reliability fact.
    assert set(documented) - set(helpers["GOLD_COLUMNS"]) == {"fact_vehicle_reliability"}


# ---------- dim_date ----------


def test_dim_date_calendar(full_gold, spark):
    gold, _, _ = full_gold
    dim_date = read_gold(spark, gold, "dim_date")
    days = (datetime.date(2030, 12, 31) - datetime.date(1950, 1, 1)).days + 1
    assert dim_date.count() == days + 1
    saturday = dim_date.filter("date_key = 20261107").first()
    assert (saturday.day_name, saturday.day_of_week, saturday.is_weekend, saturday.quarter) == ("Saturday", 6, True, 4)
    monday = dim_date.filter("date_key = 20261109").first()
    assert (monday.day_of_week, monday.is_weekend, monday.month_name) == (1, False, "November")
    assert dim_date.filter("date_key = -1").first().day_name == "Unknown"


# ---------- full Gold on the sample ----------


def test_gold_tables_have_the_documented_columns(full_gold, spark, helpers):
    gold, _, _ = full_gold
    for table, columns in helpers["GOLD_COLUMNS"].items():
        assert read_gold(spark, gold, table).columns == columns, table


def test_fact_and_dimension_row_counts(full_gold, spark):
    gold, silver, _ = full_gold
    listings = spark.read.format("delta").load(str(silver / "vehicle_listings")).count()
    events = spark.read.format("delta").load(str(silver / "service_events")).count()
    assert listings == 4 * SAMPLE_ROWS
    assert read_gold(spark, gold, "fact_listing").count() == listings
    assert read_gold(spark, gold, "dim_vehicle").count() == listings + 1
    assert read_gold(spark, gold, "fact_service_event").count() == events
    assert read_gold(spark, gold, "dim_currency").count() == 5
    # 2 rate days x (3 quoted currencies + EUR).
    assert read_gold(spark, gold, "fact_exchange_rate").count() == 8


def test_every_dimension_has_an_unknown_member(full_gold, spark):
    gold, _, _ = full_gold
    for table, key in [("dim_vehicle", "vehicle_key"), ("dim_location", "location_key"), ("dim_platform", "platform_key"),
                       ("dim_currency", "currency_key"), ("dim_service_type", "service_type_key")]:
        assert read_gold(spark, gold, table).filter(F.col(key) == -1).count() == 1, table


def test_brand_questions_on_service_events_need_no_fact_join(full_gold, spark):
    """fact_service_event -> dim_vehicle gives the manufacturer directly (conformed dimension)."""
    gold, _, _ = full_gold
    by_brand = (
        read_gold(spark, gold, "fact_service_event")
        .join(read_gold(spark, gold, "dim_vehicle"), "vehicle_key")
        .groupBy("manufacturer").count()
    )
    assert by_brand.agg(F.sum("count")).first()[0] == read_gold(spark, gold, "fact_service_event").count()


def test_business_ready_values_in_dim_vehicle(full_gold, spark):
    gold, _, _ = full_gold
    dim_vehicle = read_gold(spark, gold, "dim_vehicle").filter("vehicle_key != -1")
    assert dim_vehicle.filter(F.col("manufacturer").isNull() | F.col("fuel").isNull()).count() == 0
    assert dim_vehicle.filter(F.col("manufacturer") == F.lower("manufacturer")).count() == 0  # display names
    assert dim_vehicle.filter("manufacturer = 'Unknown'").count() == dim_vehicle.filter("manufacturer_source = 'unknown'").count()


def test_current_price_view(full_gold, spark):
    _, _, namespaces = full_gold
    view = spark.table("vw_listing_current_price")
    assert view.count() == 4 * SAMPLE_ROWS
    assert {r[0] for r in view.select("fx_rate_date_key").distinct().collect()} == {20261102}
    eur = view.join(spark.table("dim_currency"), "currency_key").filter("currency_code = 'EUR' AND price IS NOT NULL").first()
    assert eur.price_eur_current == eur.price
    assert eur.price_gbp_current == Decimal(str(round(float(eur.price) * 0.8343, 2))).quantize(Decimal("0.01"))
    assert "fact_listing" in [t.name for t in spark.catalog.listTables()]


# ---------- fact_listing: prices at the posting date ----------


@pytest.fixture(scope="module")
def fact_listing_functions(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("fact_listing_functions")
    namespace = gold_namespace(spark, tmp, tmp, tmp)
    return run_notebook_cells_until("08_gold_fact_listing.py", namespace, "listings_df = read_required_table")


def test_prices_use_the_rate_of_the_last_working_day_before_posting(spark, fact_listing_functions):
    listings = spark.createDataFrame(
        [
            ("sat_eur", datetime.date(2026, 10, 31), Decimal("1000.00"), "EUR"),  # Saturday -> Friday's rate
            ("mon_aud", datetime.date(2026, 11, 2), Decimal("20000.00"), "AUD"),
            ("too_early", datetime.date(2026, 10, 1), Decimal("500.00"), "EUR"),  # before the first rate
            ("no_currency", datetime.date(2026, 11, 2), Decimal("500.00"), None),
            ("no_date", None, Decimal("500.00"), "EUR"),
        ],
        "vehicle_id STRING, posting_date DATE, price DECIMAL(18,2), currency STRING",
    ).select(
        "*", F.lit("CA").alias("state"), F.lit("x").alias("region"), F.lit(1).alias("platform_id"),
        F.lit(100).alias("odometer"), F.lit(1.0).alias("latitude"), F.lit(1.0).alias("longitude"),
        F.lit("First").alias("source_system"), F.lit(True).alias("is_valid"), F.array().cast("array<string>").alias("dq_issues"),
    )
    rates = spark.createDataFrame(
        [(datetime.date(2026, 10, 30), q, r) for q, r in [("AUD", 1.6301), ("GBP", 0.8361), ("USD", 1.1162)]]
        + [(datetime.date(2026, 11, 2), q, r) for q, r in [("AUD", 1.6236), ("GBP", 0.8343), ("USD", 1.1193)]],
        "rate_date DATE, quote_currency STRING, rate DOUBLE",
    )
    fact = fact_listing_functions["build_fact_listing"](listings, rates)
    key = fact_listing_functions["surrogate_key"]
    rows = {r.vid: r for r in fact.join(listings.select("vehicle_id", key("vehicle_id").alias("vehicle_key")), "vehicle_key")
            .withColumnRenamed("vehicle_id", "vid").collect()}

    sat = rows["sat_eur"]
    assert (str(sat.fx_rate_date), sat.price_eur_at_posting, sat.price_gbp_at_posting) == ("2026-10-30", Decimal("1000.00"), Decimal("836.10"))
    assert rows["mon_aud"].price_gbp_at_posting == Decimal("10277.16")  # 20,000 / 1.6236 * 0.8343
    assert rows["mon_aud"].price_aud_at_posting == Decimal("20000.00")
    assert (rows["too_early"].fx_rate_date, rows["too_early"].price_gbp_at_posting) == (None, None)
    assert rows["no_currency"].price_gbp_at_posting is None and rows["no_currency"].currency_key == -1
    assert rows["no_date"].posting_date_key == -1 and rows["no_date"].price_gbp_at_posting is None
