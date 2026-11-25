"""
Tests for the Silver data quality checks: the rule engine (notebooks/utils/dq_checks.py) and the gate notebook
(notebooks/06_silver_dq_checks.py) on Silver tables built from the Bronze sample.

On the sample the listings and the service book are two unrelated samples, so the error rule
`vehicles_in_listings_share` fails by nature: the notebook must store every result and then fail with a message
that names the rule (this is the gate that keeps Gold from running).
"""

import json

import pytest
from pyspark.sql import functions as F

from conftest import copy_silver
from notebook_utils import run_notebook
from test_vehicle_listings import vehicle_namespace
from test_vehicle_listings_priced import run_priced

NOTEBOOK = "06_silver_dq_checks.py"


@pytest.fixture(scope="module")
def engine(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("dq_engine")
    namespace = vehicle_namespace(spark, tmp, tmp)
    return run_notebook("utils/dq_checks.py", namespace)


def rule(engine, name, value, op, threshold, severity="error", unit=""):
    return engine["dq_rule"]("t", name, severity, f"{name} description", lambda: value, op, threshold, unit)


# ---------- engine ----------


@pytest.mark.parametrize(
    "value, op, threshold, status",
    [(0, "==", 0, "PASS"), (1, "==", 0, "FAIL"), (4.9, "<=", 5.0, "PASS"), (5.1, "<=", 5.0, "FAIL"),
     (99.5, ">=", 99.0, "PASS"), (98.0, ">=", 99.0, "FAIL"), (None, "<=", 5.0, "FAIL")],
)
def test_rule_status(engine, value, op, threshold, status):
    result = engine["evaluate_rules"]([rule(engine, "r", value, op, threshold)])[0]
    assert result["status"] == status


def test_messages_say_what_failed_and_why(engine):
    results = engine["evaluate_rules"]([
        rule(engine, "invalid_year_share", 6.1, "<=", 5.0, severity="warning", unit="%"),
        rule(engine, "duplicates", 3, "==", 0),
        rule(engine, "fine", 0, "==", 0),
    ])
    assert results[0]["message"] == "t.invalid_year_share = 6.10%, expected <= 5.00%: invalid_year_share description"
    assert results[2]["message"] == "t.fine = 0 (== 0)"
    message = engine["failed_errors_message"](results)
    assert message.startswith("1 data quality error rule(s) failed:")
    assert "t.duplicates = 3, expected == 0" in message and "invalid_year_share" not in message  # warnings excluded


def test_a_broken_metric_fails_its_rule_without_stopping_the_others(engine):
    def broken():
        raise ValueError("table missing")

    results = engine["evaluate_rules"]([
        engine["dq_rule"]("t", "broken", "error", "d", broken, "==", 0),
        rule(engine, "fine", 0, "==", 0),
    ])
    assert [r["status"] for r in results] == ["FAIL", "PASS"]
    assert "could not be computed (ValueError: table missing)" in results[0]["message"]


def test_threshold_overrides(engine):
    results = engine["evaluate_rules"]([rule(engine, "share", 3.0, "<=", 5.0)], {"share": 2.0})
    assert (results[0]["status"], results[0]["threshold"]) == ("FAIL", 2.0)


# ---------- the gate notebook on the sample ----------


@pytest.fixture(scope="module")
def silver(spark, sample_silver, tmp_path_factory):
    """Silver tables from the Bronze sample (a copy: the notebook appends dq_results) and the priced temp view."""
    silver = copy_silver(sample_silver, tmp_path_factory.mktemp("dq"))
    run_priced(spark, silver)
    return silver


def run_dq(spark, bronze, silver, overrides=None) -> dict:
    namespace = vehicle_namespace(spark, bronze, silver)
    namespace["dbutils"].widgets.values["threshold_overrides"] = json.dumps(overrides or {})
    namespace["UC_CATALOG"] = None
    run_notebook("utils/dq_checks.py", namespace)
    return run_notebook(NOTEBOOK, namespace)


def results_of(spark, silver):
    return spark.read.format("delta").load(str(silver / "dq_results"))


def test_failed_error_rule_stores_results_and_stops_the_run(spark, bronze_sample, silver):
    with pytest.raises(AssertionError, match=r"error rule\(s\) failed:\n- service_events.vehicles_in_listings_share = "):
        run_dq(spark, bronze_sample, silver)

    stored = results_of(spark, silver)
    assert stored.select("run_id").distinct().count() == 1
    failed = {(r.table_name, r.rule_name) for r in stored.filter("status = 'FAIL' AND severity = 'error'").collect()}
    assert failed == {("service_events", "vehicles_in_listings_share")}
    # The structural error rules pass on the sample.
    passed = {r.rule_name for r in stored.filter("status = 'PASS'").collect()}
    assert {"row_count_matches_stages", "vehicle_id_duplicates", "service_event_id_duplicates",
            "model_normalized_duplicates", "latest_rate_age_days", "missing_converted_prices"} <= passed


def test_passing_run_appends_history_and_compares_row_counts(spark, bronze_sample, silver):
    namespace = run_dq(spark, bronze_sample, silver, overrides={"vehicles_in_listings_share": 0})
    assert not namespace["failed_errors"]
    stored = results_of(spark, silver)
    assert stored.select("run_id").distinct().count() >= 2  # appended, not overwritten
    latest = stored.filter(F.col("run_id") == namespace["RUN_ID"])
    change = latest.filter("table_name = 'vehicle_listings' AND rule_name = 'row_count_change_pct'").first()
    assert (change.metric_value, change.status) == (0.0, "PASS")  # same data as the previous run
    assert latest.count() == len(namespace["RULES"])
    assert set(stored.columns) == set(namespace["DQ_RESULT_COLUMNS"])
    assert spark.table("dq_results").count() == stored.count()  # the view


def test_largest_rate_gap(engine, spark, bronze_sample, silver):
    namespace = run_dq(spark, bronze_sample, silver, overrides={"vehicles_in_listings_share": 0})
    assert namespace["rate_gap_days"] == 3  # Friday 2026-10-30 -> Monday 2026-11-02
