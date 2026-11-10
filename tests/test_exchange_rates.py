"""Tests for notebooks/05_silver_exchange_rates.py (Frankfurter responses in Bronze → silver/exchange_rates)."""

import datetime

import pytest

from conftest import INGESTION_DATE, SAMPLE_RATES, write_rates_partition
from notebook_utils import load_config_and_utils, run_notebook

NOTEBOOK = "05_silver_exchange_rates.py"

EXPECTED_DTYPES = {
    "rate_date": "date",
    "base_currency": "string",
    "quote_currency": "string",
    "rate": "double",
    "ingestion_date": "date",
    "source_file_path": "string",
    "silver_processed_at": "timestamp",
}


def run_rates(spark, bronze, silver, dev_mode="false", ingestion_date="latest") -> dict:
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze),
        silver_path=str(silver),
        widget_values={"ingestion_date": ingestion_date, "dev_mode": dev_mode},
    )
    return run_notebook(NOTEBOOK, namespace)


def read_rates(spark, silver, table="exchange_rates"):
    return spark.read.format("delta").load(str(silver / table))


def day_rates(day: str, aud=1.6, gbp=0.8, usd=1.1):
    return [(day, "AUD", aud), (day, "GBP", gbp), (day, "USD", usd)]


def test_full_run_on_the_sample(spark, bronze_sample, silver_dir, catalog_untouched):
    namespace = run_rates(spark, bronze_sample, silver_dir)  # the notebook's own asserts run here

    table = read_rates(spark, silver_dir)
    assert table.columns == namespace["EXCHANGE_RATES_COLUMNS"]
    assert dict(table.dtypes) == EXPECTED_DTYPES
    rows = {(str(r.rate_date), r.quote_currency): r.rate for r in table.collect()}
    assert rows == {(d, q): r for d, q, r in SAMPLE_RATES}
    assert {r.base_currency for r in table.collect()} == {"EUR"}
    assert {r.ingestion_date for r in table.collect()} == {datetime.date.fromisoformat(INGESTION_DATE)}
    assert table.filter("source_file_path NOT LIKE '%rates_latest.json'").count() == 0
    assert spark.sql(f"DESCRIBE DETAIL delta.`{silver_dir / 'exchange_rates'}`").first().numFiles == 1
    assert len(namespace["display"].results) == 3  # every report cell ran without errors


def test_all_partitions_are_read_and_the_latest_ingestion_wins(spark, tmp_path, catalog_untouched):
    bronze = tmp_path / "bronze"
    # History file and a daily file: Friday 2026-10-30 comes twice (the later ingestion has another value).
    write_rates_partition(bronze, "2026-11-01", "rates_2026-10-29_2026-10-30.json",
                          day_rates("2026-10-29") + day_rates("2026-10-30", gbp=0.81))
    write_rates_partition(bronze, "2026-11-03", "rates_latest.json",
                          day_rates("2026-10-30", gbp=0.82) + day_rates("2026-11-02"))

    namespace = run_rates(spark, bronze, tmp_path / "silver")

    table = read_rates(spark, tmp_path / "silver")
    assert table.count() == 9  # 3 days × 3 currencies
    friday_gbp = table.filter("rate_date = '2026-10-30' AND quote_currency = 'GBP'").first()
    assert (friday_gbp.rate, str(friday_gbp.ingestion_date)) == (0.82, "2026-11-03")
    assert namespace["raw_count"] - namespace["rates_count"] == 3


def test_ingestion_date_reads_only_partitions_up_to_that_date(spark, tmp_path, catalog_untouched):
    bronze = tmp_path / "bronze"
    write_rates_partition(bronze, "2026-11-01", "rates_latest.json", day_rates("2026-10-30"))
    write_rates_partition(bronze, "2026-11-03", "rates_latest.json", day_rates("2026-11-02"))

    run_rates(spark, bronze, tmp_path / "silver", ingestion_date="2026-11-01")

    assert {str(r.rate_date) for r in read_rates(spark, tmp_path / "silver").collect()} == {"2026-10-30"}


def test_unknown_ingestion_date_fails(spark, bronze_sample, silver_dir):
    with pytest.raises(ValueError, match="Ingestion date 1999-01-01 not found for ExchangeRates"):
        run_rates(spark, bronze_sample, silver_dir, ingestion_date="1999-01-01")


def test_no_partitions_fails(spark, tmp_path):
    (tmp_path / "bronze" / "ExchangeRates").mkdir(parents=True)
    with pytest.raises(ValueError, match="No 'ingestion_date=\\*' partitions"):
        run_rates(spark, tmp_path / "bronze", tmp_path / "silver")


def test_dev_mode_writes_to_the_dev_table(spark, bronze_sample, silver_dir, catalog_untouched):
    run_rates(spark, bronze_sample, silver_dir, dev_mode="true")
    assert read_rates(spark, silver_dir, "exchange_rates_dev").count() == len(SAMPLE_RATES)
    assert not (silver_dir / "exchange_rates").exists()


@pytest.mark.parametrize(
    "rows, base, problem",
    [
        (day_rates("2026-11-02")[:2], "EUR", "days_without_all_currencies"),        # USD missing
        (day_rates("2026-11-02", gbp=0.0), "EUR", "rate_not_positive"),
        (day_rates("2026-11-02", gbp=-0.8), "EUR", "rate_not_positive"),
        (day_rates("2026-11-02"), "USD", "wrong_base_currency"),
        (day_rates("2026-11-04"), "EUR", "rate_date_after_ingestion"),             # after 2026-11-03
        (day_rates("02.11.2026"), "EUR", "unparsed_rate_date"),
    ],
)
def test_invalid_rates_stop_the_run(spark, tmp_path, rows, base, problem):
    bronze = tmp_path / "bronze"
    write_rates_partition(bronze, INGESTION_DATE, "rates_latest.json", rows, base=base)
    with pytest.raises(AssertionError, match=f"invalid exchange rates .*'{problem}': [1-9]"):
        run_rates(spark, bronze, tmp_path / "silver")
    assert not (tmp_path / "silver" / "exchange_rates").exists()


def test_error_response_stops_the_run(spark, tmp_path):
    bronze = tmp_path / "bronze"
    write_rates_partition(bronze, INGESTION_DATE, "rates_latest.json", '{"status": 422, "message": "invalid currency: XYZ"}')
    with pytest.raises(AssertionError, match="missing fields .* \\(not a rates response\\?\\)"):
        run_rates(spark, bronze, tmp_path / "silver")
