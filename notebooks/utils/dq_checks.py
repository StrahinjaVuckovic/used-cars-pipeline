# Databricks notebook source
# MAGIC %md
# MAGIC # DQ checks – a small rule engine for table-level data quality
# MAGIC
# MAGIC Run from other notebooks with `%run ./utils/dq_checks` (after `00_config` and `utils/silver_transformations`).
# MAGIC Used by `06_silver_dq_checks`; the rules, thresholds and their reasons are in `documentation/data_quality.md`.
# MAGIC
# MAGIC A **rule** checks one number about a whole table (a share of invalid values, a count of duplicate keys, the age
# MAGIC of the latest exchange rate…) against a threshold:
# MAGIC
# MAGIC - `metric` is a function without arguments that returns the number (or None when it cannot be computed);
# MAGIC   the numbers come from table profiles computed once, so a big table is read once for all its rules;
# MAGIC - `severity` `error` means Gold would be wrong (the run fails after the results are stored), `warning` means the
# MAGIC   source behaves differently than usual (logged and stored, the run goes on).
# MAGIC
# MAGIC A ready-made framework (Great Expectations, Soda, Delta Live Tables expectations) was not used: it needs extra
# MAGIC libraries or another way to run the pipeline, and for a dozen tables a list of rules in code does the same job.

# COMMAND ----------

import operator as _operator

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DQ_RESULT_SCHEMA = (
    "run_id STRING, run_timestamp TIMESTAMP, ingestion_date DATE, table_name STRING, rule_name STRING, "
    "severity STRING, description STRING, metric_value DOUBLE, operator STRING, threshold DOUBLE, "
    "status STRING, message STRING"
)
DQ_RESULT_COLUMNS = [c.split()[0] for c in DQ_RESULT_SCHEMA.split(", ")]
DQ_OPERATORS = {"<=": _operator.le, ">=": _operator.ge, "==": _operator.eq}
DQ_SEVERITIES = ("error", "warning")

# COMMAND ----------


def dq_rule(table: str, rule: str, severity: str, description: str, metric, op: str, threshold: float,
            unit: str = "") -> dict:
    """One table-level rule. `metric` is called when the rules are evaluated; `unit` is only for the message."""
    assert severity in DQ_SEVERITIES, f"Unknown severity {severity}"
    assert op in DQ_OPERATORS, f"Unknown operator {op}"
    return {"table": table, "rule": rule, "severity": severity, "description": description,
            "metric": metric, "operator": op, "threshold": float(threshold), "unit": unit}


def _format(value, unit: str) -> str:
    if value is None:
        return "n/a"
    if unit == "%":
        return f"{value:.2f}%"
    return f"{value:,.0f}{unit}" if float(value).is_integer() else f"{value:,.2f}{unit}"


def evaluate_rules(rules: list, overrides: dict = None) -> list:
    """
    Evaluate every rule and return one result dict per rule (status PASS / FAIL with a readable message).
    A metric that cannot be computed (None or an exception) fails its rule instead of stopping the evaluation, so
    every rule is always reported. `overrides` replaces thresholds by rule name (tests, negative checks).
    """
    overrides = overrides or {}
    results = []
    for rule in rules:
        threshold = float(overrides.get(rule["rule"], rule["threshold"]))
        try:
            value = rule["metric"]()
            value = None if value is None else float(value)
            error = None
        except Exception as exc:  # noqa: BLE001 – a broken metric must be reported, not hide the other rules
            value, error = None, f"{type(exc).__name__}: {exc}"
        passed = value is not None and DQ_OPERATORS[rule["operator"]](value, threshold)
        name = f"{rule['table']}.{rule['rule']}"
        expected = f"{rule['operator']} {_format(threshold, rule['unit'])}"
        if passed:
            message = f"{name} = {_format(value, rule['unit'])} ({expected})"
        elif error or value is None:
            message = f"{name} could not be computed ({error or 'no value'}); expected {expected}"
        else:
            message = f"{name} = {_format(value, rule['unit'])}, expected {expected}: {rule['description']}"
        results.append({
            "table_name": rule["table"], "rule_name": rule["rule"], "severity": rule["severity"],
            "description": rule["description"], "metric_value": value, "operator": rule["operator"],
            "threshold": threshold, "status": "PASS" if passed else "FAIL", "message": message,
        })
    return results


def failed_rules(results: list, severity: str) -> list:
    return [r for r in results if r["status"] == "FAIL" and r["severity"] == severity]


def failed_errors_message(results: list) -> str:
    """The reason of a failed run: every failed error rule on its own line (also the text for alerts)."""
    failed = failed_rules(results, "error")
    return f"{len(failed)} data quality error rule(s) failed:\n" + "\n".join(f"- {r['message']}" for r in failed)


def results_dataframe(results: list, run_id: str, run_timestamp, ingestion_date) -> DataFrame:
    """The results as rows of the dq_results table."""
    rows = [
        (run_id, run_timestamp, ingestion_date, r["table_name"], r["rule_name"], r["severity"], r["description"],
         r["metric_value"], r["operator"], r["threshold"], r["status"], r["message"])
        for r in results
    ]
    return spark.createDataFrame(rows, DQ_RESULT_SCHEMA)


def issue_shares(df: DataFrame, row_count: int) -> dict:
    """{dq issue code: share of rows in %} from the dq_issues column (one pass over the table)."""
    counts = {r.dq_issue: r["count"] for r in dq_issue_counts(df).collect()}
    return {issue: 100.0 * count / max(row_count, 1) for issue, count in counts.items()}


def previous_metric(results_path: str, table: str, rule: str):
    """The metric of a rule in the latest earlier run (None when there is no earlier run)."""
    if not delta_table_exists(results_path):
        return None
    latest = (
        read_delta_table(results_path)
        .filter((F.col("table_name") == table) & (F.col("rule_name") == rule) & F.col("metric_value").isNotNull())
        .orderBy(F.desc("run_timestamp"))
        .select("metric_value")
        .first()
    )
    return None if latest is None else latest.metric_value
