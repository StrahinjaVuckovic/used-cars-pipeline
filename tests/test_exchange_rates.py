"""Tests for notebooks/05_silver_exchange_rates.py (Frankfurter responses in Bronze → silver/exchange_rates).

The first run of a test is a full load (no table yet); later runs in the same test are incremental (MERGE).
"""

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


def run_rates(spark, bronze, silver, dev_mode="false", ingestion_date="latest", load_mode="incremental") -> dict:
    namespace = load_config_and_utils(
        spark,
        bronze_path=str(bronze),
        silver_path=str(silver),
        widget_values={"ingestion_date": ingestion_date, "dev_mode": dev_mode, "load_mode": load_mode},
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


# ---------- incremental load ----------


def table_rows(spark, silver):
    """{(rate_date, quote_currency): row} of the Silver table."""
    return {(str(r.rate_date), r.quote_currency): r for r in read_rates(spark, silver).collect()}


def table_version(spark, silver) -> int:
    return spark.sql(f"DESCRIBE HISTORY delta.`{silver / 'exchange_rates'}` LIMIT 1").first().version


def test_first_run_without_a_table_is_a_full_load(spark, tmp_path, catalog_untouched):
    bronze = tmp_path / "bronze"
    write_rates_partition(bronze, "2026-11-01", "rates_latest.json", day_rates("2026-10-30"))

    namespace = run_rates(spark, bronze, tmp_path / "silver")

    assert (namespace["watermark"], namespace["EFFECTIVE_LOAD_MODE"]) == (None, "full")
    assert namespace["load_metrics"]["operation"] == "WRITE"


def test_incremental_run_merges_only_the_partitions_from_the_watermark_on(spark, tmp_path, catalog_untouched):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    write_rates_partition(bronze, "2026-11-01", "rates_2026-10-29_2026-10-30.json",
                          day_rates("2026-10-29") + day_rates("2026-10-30", gbp=0.81))
    run_rates(spark, bronze, silver)
    before = table_rows(spark, silver)

    # Next daily response: Friday again with another value and the new Monday.
    write_rates_partition(bronze, "2026-11-03", "rates_latest.json",
                          day_rates("2026-10-30", gbp=0.82) + day_rates("2026-11-02"))
    namespace = run_rates(spark, bronze, silver)

    assert (namespace["watermark"], namespace["EFFECTIVE_LOAD_MODE"]) == ("2026-11-01", "incremental")
    assert namespace["selected_dates"] == ["2026-11-01", "2026-11-03"]  # the watermark partition is read again
    metrics = namespace["load_metrics"]
    assert (metrics["operation"], metrics["numTargetRowsInserted"], metrics["numTargetRowsUpdated"]) == ("MERGE", 3, 3)

    after = table_rows(spark, silver)
    assert len(after) == 9
    assert (after[("2026-10-30", "GBP")].rate, str(after[("2026-10-30", "GBP")].ingestion_date)) == (0.82, "2026-11-03")
    # Days that were not touched keep their row, including the processing timestamp.
    assert after[("2026-10-29", "USD")] == before[("2026-10-29", "USD")]


def test_rerun_without_new_partitions_changes_nothing(spark, tmp_path, catalog_untouched):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    write_rates_partition(bronze, "2026-11-03", "rates_latest.json", day_rates("2026-11-02"))
    run_rates(spark, bronze, silver)
    before = table_rows(spark, silver)

    namespace = run_rates(spark, bronze, silver)

    metrics = namespace["load_metrics"]
    assert (metrics["numTargetRowsInserted"], metrics["numTargetRowsUpdated"]) == (0, 0)
    assert table_rows(spark, silver) == before


def test_same_day_recopy_of_the_partition_is_merged(spark, tmp_path, catalog_untouched):
    """A second master run on the same day overwrites rates_latest.json in the watermark partition."""
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    write_rates_partition(bronze, "2026-11-03", "rates_latest.json", day_rates("2026-11-02"))
    run_rates(spark, bronze, silver)

    write_rates_partition(bronze, "2026-11-03", "rates_latest.json",
                          day_rates("2026-11-02", gbp=0.9) + day_rates("2026-11-03"))
    namespace = run_rates(spark, bronze, silver)

    metrics = namespace["load_metrics"]
    assert (metrics["numTargetRowsInserted"], metrics["numTargetRowsUpdated"]) == (3, 1)
    assert table_rows(spark, silver)[("2026-11-02", "GBP")].rate == 0.9


def test_invalid_new_partition_leaves_the_table_unchanged(spark, tmp_path, catalog_untouched):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    write_rates_partition(bronze, "2026-11-01", "rates_latest.json", day_rates("2026-10-30"))
    run_rates(spark, bronze, silver)
    before, version = table_rows(spark, silver), table_version(spark, silver)

    write_rates_partition(bronze, "2026-11-03", "rates_latest.json", day_rates("2026-11-02", gbp=0.0))
    with pytest.raises(AssertionError, match="'rate_not_positive': 1"):
        run_rates(spark, bronze, silver)

    assert table_rows(spark, silver) == before
    assert table_version(spark, silver) == version  # nothing was written


def test_full_load_overwrites_the_table(spark, tmp_path, catalog_untouched):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    write_rates_partition(bronze, "2026-11-01", "rates_latest.json", day_rates("2026-10-30"))
    write_rates_partition(bronze, "2026-11-03", "rates_latest.json", day_rates("2026-11-02"))
    run_rates(spark, bronze, silver)

    # Backfill: the table as it was on 2026-11-01.
    namespace = run_rates(spark, bronze, silver, ingestion_date="2026-11-01", load_mode="full")

    assert (namespace["watermark"], namespace["load_metrics"]["operation"]) == (None, "WRITE")
    assert set(table_rows(spark, silver)) == {("2026-10-30", c) for c in ["AUD", "GBP", "USD"]}


def test_incremental_run_for_a_date_before_the_watermark_loads_nothing(spark, tmp_path, catalog_untouched):
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    write_rates_partition(bronze, "2026-11-01", "rates_latest.json", day_rates("2026-10-30"))
    write_rates_partition(bronze, "2026-11-03", "rates_latest.json", day_rates("2026-11-02"))
    run_rates(spark, bronze, silver)
    version = table_version(spark, silver)

    namespace = run_rates(spark, bronze, silver, ingestion_date="2026-11-01")

    assert namespace["notebook_exit_value"] == "nothing to load"
    assert table_version(spark, silver) == version
    assert len(table_rows(spark, silver)) == 6
