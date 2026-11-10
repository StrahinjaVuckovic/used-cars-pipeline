"""
Tests for notebooks/06_silver_vehicle_listings_priced.py (vehicle_listings + exchange_rates → priced view).

Local Spark has no Unity Catalog, so the notebook runs with UC_CATALOG = None and creates a session temp view
with the same query as the Unity Catalog view on Databricks.
"""

import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from conftest import SAMPLE_ROWS
from notebook_utils import load_config_and_utils, run_notebook
from test_vehicle_listings import run_listings_pipeline

NOTEBOOK = "06_silver_vehicle_listings_priced.py"
RATES_NOTEBOOK = "05_silver_exchange_rates.py"
VIEW = "vw_vehicle_listings_priced"

LATEST = "2026-11-02"
# Older rates that must not be used, and the latest rates (1 EUR = rate × currency).
RATES = [
    ("2026-10-30", "AUD", 1.6301), ("2026-10-30", "GBP", 0.8361), ("2026-10-30", "USD", 1.1162),
    (LATEST, "AUD", 1.6236), (LATEST, "GBP", 0.8343), (LATEST, "USD", 1.1193),
]


def namespace_without_catalog(spark, bronze, silver, dev_mode="false", ingestion_date="latest") -> dict:
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze),
        silver_path=str(silver),
        widget_values={"ingestion_date": ingestion_date, "dev_mode": dev_mode},
    )
    namespace["UC_CATALOG"] = None
    return namespace


def run_priced(spark, silver, dev_mode="false", ingestion_date="latest") -> dict:
    namespace = namespace_without_catalog(spark, silver.parent / "unused-bronze", silver, dev_mode, ingestion_date)
    return run_notebook(NOTEBOOK, namespace)


def write_inputs(spark, silver, listings, rates=RATES, ingestion_date="2026-11-03", suffix=""):
    """Minimal vehicle_listings (the view keeps all its columns) and exchange_rates tables."""
    spark.createDataFrame(
        [(vid, Decimal(price) if price is not None else None, cur, datetime.date.fromisoformat(ingestion_date))
         for vid, price, cur in listings],
        "vehicle_id STRING, price DECIMAL(18,2), currency STRING, ingestion_date DATE",
    ).write.format("delta").mode("overwrite").save(str(silver / f"vehicle_listings{suffix}"))
    spark.createDataFrame(
        [(datetime.date.fromisoformat(d), "EUR", q, r) for d, q, r in rates],
        "rate_date DATE, base_currency STRING, quote_currency STRING, rate DOUBLE",
    ).write.format("delta").mode("overwrite").save(str(silver / f"exchange_rates{suffix}"))


def priced(spark, view=VIEW) -> dict:
    return {r.vehicle_id: r for r in spark.table(view).collect()}


LISTINGS = [
    ("eur", "1000.00", "EUR"),
    ("aud", "20000.00", "AUD"),
    ("gbp", "5000.00", "GBP"),
    ("usd", "1500.00", "USD"),
    ("no_price", None, "USD"),
    ("no_currency", "1000.00", None),
]


def test_prices_are_converted_at_the_latest_rates(spark, tmp_path, catalog_untouched):
    silver = tmp_path / "silver"
    write_inputs(spark, silver, LISTINGS)
    run_priced(spark, silver)  # the notebook's own asserts run here

    rows = priced(spark)
    # EUR → target: price × rate(target).
    assert (rows["eur"].price_gbp, rows["eur"].price_usd, rows["eur"].price_aud) == (
        Decimal("834.30"), Decimal("1119.30"), Decimal("1623.60"))
    # Cross rate through EUR: 20,000 AUD / 1.6236 × 0.8343 = 10,277.16 GBP.
    assert rows["aud"].price_gbp == Decimal("10277.16")
    assert rows["aud"].price_aud == Decimal("20000.00")
    assert rows["gbp"].price_gbp == Decimal("5000.00")
    assert rows["gbp"].price_usd == Decimal("6708.02")   # 5,000 / 0.8343 × 1.1193
    assert rows["usd"].price_aud == Decimal("2175.82")   # 1,500 / 1.1193 × 1.6236
    assert {str(r.fx_rate_date) for r in rows.values()} == {LATEST}


def test_missing_price_or_currency_gives_null_prices(spark, tmp_path, catalog_untouched):
    silver = tmp_path / "silver"
    write_inputs(spark, silver, LISTINGS)
    run_priced(spark, silver)

    rows = priced(spark)
    for vehicle_id in ["no_price", "no_currency"]:
        assert (rows[vehicle_id].price_gbp, rows[vehicle_id].price_usd, rows[vehicle_id].price_aud) == (None, None, None)


def test_view_keeps_every_listing_and_column(spark, tmp_path, catalog_untouched):
    silver = tmp_path / "silver"
    write_inputs(spark, silver, LISTINGS)
    namespace = run_priced(spark, silver)

    view = spark.table(VIEW)
    assert view.count() == len(LISTINGS)
    assert view.columns == ["vehicle_id", "price", "currency", "ingestion_date", "fx_rate_date",
                            "price_gbp", "price_usd", "price_aud"]
    assert dict(view.dtypes)["price_gbp"] == "decimal(18,2)"
    assert namespace["rate_age_days"] == 1
    assert len(namespace["display"].results) == 2


def test_new_rates_show_up_without_recreating_the_view(spark, tmp_path, catalog_untouched):
    """A view stores no data: loading newer rates changes the prices it returns."""
    silver = tmp_path / "silver"
    write_inputs(spark, silver, LISTINGS)
    run_priced(spark, silver)

    newer = RATES + [("2026-11-03", "AUD", 1.6), ("2026-11-03", "GBP", 0.9), ("2026-11-03", "USD", 1.2)]
    write_inputs(spark, silver, LISTINGS, rates=newer)

    rows = priced(spark)
    assert str(rows["eur"].fx_rate_date) == "2026-11-03"
    assert rows["eur"].price_gbp == Decimal("900.00")


def test_stale_rates_stop_the_run(spark, tmp_path):
    silver = tmp_path / "silver"
    # The latest rate is 6 days older than the listings (the API copy has failed for a week).
    write_inputs(spark, silver, LISTINGS, ingestion_date="2026-11-08")
    with pytest.raises(AssertionError, match="6 days before the listings' ingestion date 2026-11-08"):
        run_priced(spark, silver)


def test_missing_rates_table_stops_the_run(spark, tmp_path):
    silver = tmp_path / "silver"
    write_inputs(spark, silver, LISTINGS)
    import shutil
    shutil.rmtree(silver / "exchange_rates")
    with pytest.raises(AssertionError, match="run 05_silver_exchange_rates first"):
        run_priced(spark, silver)


def test_requested_ingestion_date_must_match_the_listings(spark, tmp_path):
    silver = tmp_path / "silver"
    write_inputs(spark, silver, LISTINGS)
    with pytest.raises(AssertionError, match="built from ingestion_date=2026-11-03, not 2026-11-05"):
        run_priced(spark, silver, ingestion_date="2026-11-05")


def test_dev_mode_uses_the_dev_tables_and_view(spark, tmp_path, catalog_untouched):
    silver = tmp_path / "silver"
    write_inputs(spark, silver, LISTINGS, suffix="_dev")
    namespace = run_priced(spark, silver, dev_mode="true")
    assert namespace["VIEW_NAME"] == "vw_vehicle_listings_priced_dev"
    assert spark.table("vw_vehicle_listings_priced_dev").count() == len(LISTINGS)


def test_view_name_uses_unity_catalog_on_databricks(spark, tmp_path):
    namespace = load_config_and_utils(spark, bronze_path=str(tmp_path), silver_path=str(tmp_path))
    assert (namespace["UC_CATALOG"], namespace["UC_SILVER_SCHEMA"]) == ("dbw_used_cars_strahinja", "silver")


def test_full_silver_pipeline_on_the_sample(spark, bronze_sample, silver_dir, catalog_untouched):
    """stage × 4 → brand_lookup → 03 → 05 → 06, like the Databricks Jobs."""
    run_listings_pipeline(spark, bronze_sample, silver_dir)
    run_notebook(RATES_NOTEBOOK, namespace_without_catalog(spark, bronze_sample, silver_dir))
    run_notebook(NOTEBOOK, namespace_without_catalog(spark, bronze_sample, silver_dir))

    view = spark.table(VIEW)
    assert view.count() == 4 * SAMPLE_ROWS
    assert {str(r.fx_rate_date) for r in view.select("fx_rate_date").distinct().collect()} == {LATEST}
    converted = view.filter(F.col("price").isNotNull() & F.col("currency").isNotNull())
    assert converted.count() > 0
    assert converted.filter(F.col("price_gbp").isNull() | F.col("price_usd").isNull() | F.col("price_aud").isNull()).count() == 0
