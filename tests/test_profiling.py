"""Tests for notebooks/utils/profiling_helpers.py and the profiling notebooks in notebooks/profiling/."""

import pytest
from pyspark.sql import functions as F

from notebook_utils import load_config_and_utils, new_namespace, run_notebook


@pytest.fixture(scope="module")
def helpers(spark):
    namespace = new_namespace(spark)
    run_notebook("utils/silver_transformations.py", namespace)
    run_notebook("utils/profiling_helpers.py", namespace)
    return namespace


# ---------- helpers ----------


def test_column_profile(spark, helpers):
    df = spark.createDataFrame(
        [("a", 1.0), ("NaN", 5.0), (None, None), ("b", 3.0)], "text STRING, number DOUBLE"
    )
    rows = {r.column: r for r in helpers["column_profile"](df, helpers["NULL_TOKENS"]).collect()}
    assert rows["text"].null_pct == 25.0
    assert rows["text"].null_token_pct == 25.0  # "NaN" is text that means "no data"
    assert rows["number"].null_token_pct is None  # only string columns are checked for tokens
    assert (rows["number"].min, rows["number"].max) == ("1.0", "5.0")
    assert rows["number"].data_type == "double"
    assert rows["text"].approx_distinct == 3


def test_column_profile_of_empty_dataframe(spark, helpers):
    assert helpers["column_profile"](spark.createDataFrame([], "a INT")).count() == 0


def test_top_values(spark, helpers):
    df = spark.createDataFrame([("x",), ("x",), ("y",), (None,)], "v STRING")
    rows = [(r.value, r["count"], r.pct) for r in helpers["top_values"](df, "v", n=2).collect()]
    assert rows == [("x", 2, 50.0), (None, 1, 25.0)]


def test_numeric_summary_casts_strings_and_counts_edge_values(spark, helpers):
    df = spark.createDataFrame([("-1",), ("0",), ("10",), ("abc",), (None,)], "price STRING")
    row = helpers["numeric_summary"](df, ["price"]).first()
    assert (row.min, row.max) == (-1.0, 10.0)
    assert (row.negative, row.zero, row.null_or_invalid) == (1, 1, 2)


def test_numeric_summary_ignores_nan(spark, helpers):
    df = spark.createDataFrame([(1.0,), (float("nan"),)], "x DOUBLE")
    row = helpers["numeric_summary"](df, ["x"]).first()
    assert row.max == 1.0 and row.null_or_invalid == 1


def test_regex_pattern_counts_with_group_by(spark, helpers):
    df = spark.createDataFrame(
        [("A", "2021-01-02"), ("A", "2021-01-03"), ("B", "01/02/2021"), ("B", "junk"), ("B", None)],
        "platform STRING, d STRING",
    )
    patterns = {"iso": r"^\d{4}-\d{2}-\d{2}$", "us": r"^\d{2}/\d{2}/\d{4}$"}
    rows = {(r.platform, r.pattern): r["count"] for r in helpers["regex_pattern_counts"](df, "d", patterns, ["platform"]).collect()}
    assert rows == {("A", "iso"): 2, ("B", "us"): 1, ("B", "no_match"): 1, ("B", "null"): 1}


def test_duplicate_key_count(spark, helpers):
    df = spark.createDataFrame([(1,), (1,), (1,), (2,), (2,), (3,)], "id INT")
    assert helpers["duplicate_key_count"](df, ["id"]) == {"duplicate_keys": 2, "extra_rows": 3}
    assert helpers["duplicate_key_count"](df.distinct(), ["id"]) == {"duplicate_keys": 0, "extra_rows": 0}


def test_constant_columns(spark, helpers):
    df = spark.createDataFrame(
        [(1, "UNKNOWN", None, "a"), (2, "UNKNOWN", None, "b")], "id INT, root_cause STRING, empty STRING, two STRING"
    )
    assert helpers["constant_columns"](df) == ["root_cause", "empty"]


def test_text_anomaly_counts(spark, helpers):
    df = spark.createDataFrame([("a\rb", " x"), ("a\nb", "y"), ("ok", "z ")], "description STRING, model STRING")
    rows = {r.column: (r.carriage_return, r.line_feed, r.untrimmed) for r in helpers["text_anomaly_counts"](df).collect()}
    assert rows == {"description": (1, 1, 0), "model": (0, 0, 2)}


# ---------- notebooks (smoke tests on the local Bronze sample) ----------


def run_profiling_notebook(spark, notebook, bronze_sample, tmp_path, widget_values=None):
    namespace = load_config_and_utils(
        spark, bronze_path=str(bronze_sample), silver_path=str(tmp_path / "silver"), widget_values=widget_values
    )
    run_notebook("utils/profiling_helpers.py", namespace)
    return run_notebook(f"profiling/{notebook}", namespace)


@pytest.mark.parametrize("source", ["all", "Fourth"])
def test_vehicle_sources_profile_runs(spark, bronze_sample, tmp_path, catalog_untouched, source):
    namespace = run_profiling_notebook(
        spark, "01_profile_vehicle_sources.py", bronze_sample, tmp_path,
        {"source": source, "ingestion_date": "latest"},
    )
    assert list(namespace["raw_sources"]) == (["First", "Second", "Third", "Fourth"] if source == "all" else ["Fourth"])
    assert namespace["display"].results  # results were computed
    assert not (tmp_path / "silver").exists()  # profiling never writes


def test_vehicle_sources_profile_shows_multiline_comparison(spark, bronze_sample, tmp_path):
    namespace = run_profiling_notebook(
        spark, "01_profile_vehicle_sources.py", bronze_sample, tmp_path,
        {"source": "First", "ingestion_date": "latest"},
    )
    comparison = {row.read: row for row in namespace["display"].results[0]}
    assert comparison["with multiLine"].row_count == 1000
    assert comparison["with multiLine"].malformed_ids == 0


def test_service_book_profile_runs(spark, bronze_sample, tmp_path, catalog_untouched):
    namespace = run_profiling_notebook(
        spark, "02_profile_service_book.py", bronze_sample, tmp_path, {"ingestion_date": "latest"}
    )
    assert namespace["total_rows"] == 1000
    assert {"root_cause", "warranty_flag", "is_tyre"} <= set(namespace["constant"])
    assert namespace["different"].count() == 0  # desc_all can be derived
    assert not (tmp_path / "silver").exists()


def test_brand_lookup_profile_runs(spark, bronze_sample, tmp_path, catalog_untouched):
    namespace = run_profiling_notebook(
        spark, "03_profile_brand_lookup.py", bronze_sample, tmp_path, {"ingestion_date": "latest"}
    )
    assert namespace["lookup_raw"].count() == 23848
    # Real data finding: 91 models appear twice (lower case and Title Case); after normalization only
    # "astra" maps to two manufacturers (saturn / opel).
    assert namespace["duplicate_key_count"](namespace["lookup"], ["model_normalized"])["extra_rows"] == 91
    assert [r.model_normalized for r in namespace["conflicts"].collect()] == ["astra"]
    assert len(namespace["manufacturers"]) == 45
    assert namespace["model_count"] > 0
    assert 0 < namespace["matched_models"] <= namespace["model_count"]
    assert not (tmp_path / "silver").exists()


def test_normalize_model_matches_silver_normalization(spark, bronze_sample, tmp_path):
    namespace = run_profiling_notebook(
        spark, "03_profile_brand_lookup.py", bronze_sample, tmp_path, {"ingestion_date": "latest"}
    )
    df = spark.createDataFrame([("  Silverado   1500 Crew ",)], "model STRING")
    assert df.select(namespace["normalize_model"](F.col("model"))).first()[0] == "silverado 1500 crew"
