"""Unit tests for notebooks/utils/silver_transformations.py."""

import datetime

import pytest
from pyspark.sql import functions as F

from conftest import INGESTION_DATE
from notebook_utils import new_namespace, run_notebook


@pytest.fixture(scope="module")
def utils(spark):
    namespace = new_namespace(spark)
    run_notebook("utils/silver_transformations.py", namespace)
    return namespace


# ---------- standardization ----------


def test_standardize_nulls_turns_null_tokens_into_null_and_trims(spark, utils):
    df = spark.createDataFrame(
        [("  a  ", 1.0), ("", float("nan")), ("   ", 2.0), ("NULL", 3.0), ("None", 4.0), ("NaN", 5.0), ("n/a", 6.0), ("keep me", None)],
        "text STRING, number DOUBLE",
    )
    rows = utils["standardize_nulls"](df).collect()
    assert [r.text for r in rows] == ["a", None, None, None, None, None, None, "keep me"]
    assert rows[1].number is None  # NaN -> null
    assert rows[0].number == 1.0


def test_try_cast_columns_returns_null_for_bad_values(spark, utils):
    df = spark.createDataFrame([("10026.0",), ("abc",), (None,)], "value STRING")
    rows = utils["try_cast_columns"](df, {"value": "double"}).collect()
    assert [r.value for r in rows] == [10026.0, None, None]


def test_rename_columns_keeps_unmapped_columns_and_order(spark, utils):
    df = spark.createDataFrame([(1, 2, 3)], "id INT, `long` INT, other INT")
    renamed = utils["rename_columns"](df, {"id": "vehicle_id", "long": "longitude"})
    assert renamed.columns == ["vehicle_id", "longitude", "other"]


def test_normalize_case(spark, utils):
    df = spark.createDataFrame([("Excellent", "ca")], "condition STRING, state STRING")
    df = utils["normalize_case"](df, ["condition"], "lower")
    df = utils["normalize_case"](df, ["state"], "upper")
    row = df.first()
    assert (row.condition, row.state) == ("excellent", "CA")


# ---------- dates ----------

POSTING_DATE_FORMATS = [
    (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
    (r"^\d{2}/\d{2}/\d{4}$", "MM/dd/yyyy"),
    (r"^\d{2}\.\d{2}\.\d{4}$", "dd.MM.yyyy"),
    (r"^[A-Za-z]+ \d{1,2}, \d{4}$", "MMMM d, yyyy"),
]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2021-04-12", datetime.date(2021, 4, 12)),   # CarGurus
        ("04/12/2021", datetime.date(2021, 4, 12)),   # Craigslist (month first)
        ("12.04.2021", datetime.date(2021, 4, 12)),   # AutoScout24 (day first)
        ("April 12, 2021", datetime.date(2021, 4, 12)),  # CarsGuide
        ("May 5, 2021", datetime.date(2021, 5, 5)),   # CarsGuide, single-digit day
        ("2021/04/12", None),                          # unknown format -> null
        (None, None),
    ],
)
def test_parse_multi_format_date(spark, utils, raw, expected):
    df = spark.createDataFrame([(raw,)], "raw STRING")
    parsed = df.select(utils["parse_multi_format_date"](F.col("raw"), POSTING_DATE_FORMATS).alias("d")).first().d
    assert parsed == expected


def test_parse_multi_format_date_does_not_fail_on_impossible_calendar_date(spark, utils):
    """A value that matches a pattern but is not a real date must become null, not fail the job (ANSI mode)."""
    df = spark.createDataFrame([("02/30/2021",), ("2021-13-01",)], "raw STRING")
    rows = df.select(utils["parse_multi_format_date"](F.col("raw"), POSTING_DATE_FORMATS).alias("d")).collect()
    assert [r.d for r in rows] == [None, None]


# ---------- data quality ----------


def test_flag_and_nullify_and_collect_dq_issues(spark, utils):
    df = spark.createDataFrame([(1, 100.0), (2, -5.0), (3, None), (4, 0.0)], "id INT, price DOUBLE")
    df = utils["flag_and_nullify"](df, "price", F.col("price") < 0, "invalid_price")
    df = utils["add_issue_flag"](df, F.col("price") == 0, "zero_price")
    df = utils["collect_dq_issues"](df)

    rows = {r.id: r for r in df.collect()}
    assert rows[1].price == 100.0 and rows[1].dq_issues == [] and rows[1].is_valid
    assert rows[2].price is None and rows[2].dq_issues == ["invalid_price"] and not rows[2].is_valid
    assert rows[3].price is None and rows[3].dq_issues == [] and rows[3].is_valid  # missing is not invalid
    assert rows[4].price == 0.0 and rows[4].dq_issues == ["zero_price"]
    assert not [c for c in df.columns if c.startswith("_dq_")]


def test_collect_dq_issues_without_flags(spark, utils):
    df = utils["collect_dq_issues"](spark.createDataFrame([(1,)], "id INT"))
    row = df.first()
    assert row.dq_issues == [] and row.is_valid is True


def test_add_lineage_columns(spark, utils):
    df = utils["add_lineage_columns"](spark.createDataFrame([(1,)], "id INT"), INGESTION_DATE)
    row = df.first()
    assert row.ingestion_date == datetime.date(2026, 11, 3)
    assert row.silver_processed_at is not None


def test_deduplicate_keeps_first_row_by_order(spark, utils):
    df = spark.createDataFrame(
        [("a", datetime.date(2021, 1, 1)), ("a", datetime.date(2021, 6, 1)), ("a", None), ("b", None)],
        "vehicle_id STRING, posting_date DATE",
    )
    result = utils["deduplicate"](df, ["vehicle_id"], [F.col("posting_date").desc_nulls_last()])
    rows = {r.vehicle_id: r.posting_date for r in result.collect()}
    assert rows == {"a": datetime.date(2021, 6, 1), "b": None}


def test_assert_unique(spark, utils):
    utils["assert_unique"](spark.createDataFrame([(1,), (2,)], "id INT"), ["id"], "t")
    with pytest.raises(AssertionError, match="1 duplicate keys"):
        utils["assert_unique"](spark.createDataFrame([(1,), (1,)], "id INT"), ["id"], "t")


def test_assert_columns(spark, utils):
    df = spark.createDataFrame([(1, 2)], "a INT, b INT")
    utils["assert_columns"](df, ["a", "b"], "t")
    with pytest.raises(AssertionError, match="Missing: \\['c'\\]"):
        utils["assert_columns"](df, ["a", "b", "c"], "t")


def test_null_percentages_and_issue_counts(spark, utils):
    df = spark.createDataFrame(
        [(1, None, ["x"]), (2, "a", ["x", "y"]), (3, None, [])], "id INT, text STRING, dq_issues ARRAY<STRING>"
    )
    nulls = {r.column: r.null_pct for r in utils["null_percentages"](df).collect()}
    assert nulls == {"id": 0.0, "text": 66.67, "dq_issues": 0.0}
    counts = {r.dq_issue: r["count"] for r in utils["dq_issue_counts"](df).collect()}
    assert counts == {"x": 2, "y": 1}


# ---------- reading and writing ----------


@pytest.mark.parametrize(
    "folder, file_format, options",
    [
        ("FirstSourceSystem", "csv", {"multiLine": "true"}),
        ("SecondSourceSystem", "parquet", None),
        ("ThirdSourceSystem", "avro", None),
        ("FourthSource", "json", None),
        ("ServiceBook", "csv", None),
    ],
)
def test_read_bronze_files_all_formats(utils, bronze_sample, folder, file_format, options):
    path = str(bronze_sample / folder / f"ingestion_date={INGESTION_DATE}")
    df = utils["read_bronze_files"](path, file_format, options)
    assert df.count() == 1000
    assert "source_file_path" in df.columns
    assert df.filter(F.col("source_file_path").isNull()).count() == 0
    if file_format == "csv":
        # No schema inference: every source column arrives as a string.
        assert {t for c, t in df.dtypes if c != "source_file_path"} == {"string"}


CSV_WITH_BARE_CARRIAGE_RETURN = (
    b"id,description,price\r\n"
    b'aaaaaaaaaaaa,"line one\rline two",100.0\r\n'
    b"bbbbbbbbbbbb,plain,200.0\r\n"
)


def test_csv_with_bare_carriage_return_breaks_without_multiline(utils, tmp_path):
    """Documents the bug found on the cluster: without multiLine a bare \\r inside quotes splits the row."""
    (tmp_path / "data.csv").write_bytes(CSV_WITH_BARE_CARRIAGE_RETURN)
    df = utils["read_bronze_files"](str(tmp_path), "csv")
    assert df.count() != 2


def test_csv_with_bare_carriage_return_is_read_correctly_with_multiline(utils, tmp_path):
    (tmp_path / "data.csv").write_bytes(CSV_WITH_BARE_CARRIAGE_RETURN)
    df = utils["read_bronze_files"](str(tmp_path), "csv", {"multiLine": "true"})
    rows = {r.id: r for r in df.collect()}
    assert set(rows) == {"aaaaaaaaaaaa", "bbbbbbbbbbbb"}
    assert rows["aaaaaaaaaaaa"].description == "line one\rline two"
    assert rows["bbbbbbbbbbbb"].price == "200.0"  # last column has no trailing \r


def test_read_bronze_files_rejects_unknown_format(utils):
    with pytest.raises(ValueError, match="Unsupported"):
        utils["read_bronze_files"]("/tmp/whatever", "xml")


def test_write_and_read_delta_table_by_path(spark, utils, tmp_path, catalog_untouched):
    path = str(tmp_path / "t")
    assert utils["delta_table_exists"](path) is False
    utils["write_delta_table"](spark.range(5), path)
    utils["write_delta_table"](spark.range(3), path)  # second run overwrites
    assert utils["delta_table_exists"](path) is True
    assert utils["read_delta_table"](path).count() == 3
    assert (tmp_path / "t" / "_delta_log").is_dir()


def parquet_files_in_current_version(spark, path: str) -> int:
    """Number of data files in the latest Delta version (old versions' files are ignored)."""
    return spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first().numFiles


def test_write_delta_table_with_num_files_controls_file_count(spark, utils, tmp_path):
    path = str(tmp_path / "t")
    many_partitions = spark.range(1000).repartition(50)
    utils["write_delta_table"](many_partitions, path, num_files=3)
    assert parquet_files_in_current_version(spark, path) == 3
    assert utils["read_delta_table"](path).count() == 1000


def test_write_delta_table_without_num_files_keeps_partitioning(spark, utils, tmp_path):
    path = str(tmp_path / "t")
    utils["write_delta_table"](spark.range(1000).repartition(5), path)
    assert parquet_files_in_current_version(spark, path) == 5


def test_delta_table_exists_is_false_for_plain_parquet_folder(spark, utils, tmp_path):
    path = str(tmp_path / "plain")
    spark.range(2).write.parquet(path)
    assert utils["delta_table_exists"](path) is False


def test_write_delta_table_allows_schema_change(spark, utils, tmp_path):
    path = str(tmp_path / "t")
    utils["write_delta_table"](spark.range(2), path)
    utils["write_delta_table"](spark.range(2).withColumn("extra", F.lit("x")), path)
    assert utils["read_delta_table"](path).columns == ["id", "extra"]
