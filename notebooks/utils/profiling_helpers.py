# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling helpers
# MAGIC
# MAGIC Run from the profiling notebooks with `%run ../utils/profiling_helpers`.
# MAGIC Every function returns a small DataFrame (or a Python value) that can be shown with `display()`.
# MAGIC The helpers only read data: profiling never changes or writes anything.

# COMMAND ----------

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------


def _as_double(column_name: str) -> Column:
    """String or numeric column as double: unparsable strings and NaN become null (works with ANSI mode on)."""
    value = F.expr(f"try_cast(`{column_name}` AS double)")
    return F.when(F.isnan(value), F.lit(None)).otherwise(value)


def column_profile(df: DataFrame, null_tokens: list = None) -> DataFrame:
    """
    One row per column: data type, null %, approximate number of distinct values, min and max (as text).

    With `null_tokens` (e.g. NULL_TOKENS from silver_transformations), string columns also get the % of
    values that are not real nulls but text meaning "no data" ("", "NaN", "None", ...).

    How to read it:
    - min/max of a string column are alphabetical: "999997.0" > "9999958.0" as text. CSV numbers are strings,
      so use numeric_summary for their real range.
    - approx_distinct uses a fast estimate (a few % error on large columns); use countDistinct for an exact count.
    """
    total = df.count()
    if total == 0:
        return spark.createDataFrame(
            [], "column STRING, data_type STRING, null_pct DOUBLE, null_token_pct DOUBLE, "
            "approx_distinct LONG, min STRING, max STRING"
        )

    aggregations = []
    for field in df.schema.fields:
        column = F.col(f"`{field.name}`")
        aggregations += [
            F.sum(column.isNull().cast("int")).alias(f"{field.name}__nulls"),
            F.approx_count_distinct(column).alias(f"{field.name}__distinct"),
            F.min(column).cast("string").alias(f"{field.name}__min"),
            F.max(column).cast("string").alias(f"{field.name}__max"),
        ]
        if null_tokens and isinstance(field.dataType, StringType):
            is_token = F.lower(F.trim(column)).isin(null_tokens)
            aggregations.append(F.sum(is_token.cast("int")).alias(f"{field.name}__tokens"))
    result = df.agg(*aggregations).first().asDict()

    rows = []
    for field in df.schema.fields:
        tokens = result.get(f"{field.name}__tokens")
        rows.append((
            field.name,
            field.dataType.simpleString(),
            round(100.0 * (result[f"{field.name}__nulls"] or 0) / total, 2),
            None if tokens is None else round(100.0 * tokens / total, 2),
            result[f"{field.name}__distinct"],
            result[f"{field.name}__min"],
            result[f"{field.name}__max"],
        ))
    return spark.createDataFrame(
        rows,
        "column STRING, data_type STRING, null_pct DOUBLE, null_token_pct DOUBLE, "
        "approx_distinct LONG, min STRING, max STRING",
    )


def top_values(df: DataFrame, column_name: str, n: int = 10) -> DataFrame:
    """The `n` most frequent values of a column (null included) with count and % of all rows."""
    total = df.count()
    return (
        df.groupBy(F.col(f"`{column_name}`").cast("string").alias("value"))
        .count()
        .withColumn("pct", F.round(100.0 * F.col("count") / F.lit(max(total, 1)), 2))
        .orderBy(F.desc("count"), F.asc_nulls_first("value"))
        .limit(n)
    )


def numeric_summary(df: DataFrame, columns: list) -> DataFrame:
    """
    Distribution of numeric columns (string columns are converted with try_cast):
    min, 1st percentile, median, 99th percentile, max, plus the number of negative, zero and null values.
    The percentiles show where the normal values end, which is how validation limits are chosen.
    """
    rows = []
    for column_name in columns:
        value = _as_double(column_name)
        stats = df.agg(
            F.min(value).alias("min"),
            F.percentile_approx(value, [0.01, 0.5, 0.99], 10000).alias("percentiles"),
            F.max(value).alias("max"),
            F.sum((value < 0).cast("int")).alias("negative"),
            F.sum((value == 0).cast("int")).alias("zero"),
            F.sum(value.isNull().cast("int")).alias("null_or_invalid"),
        ).first()
        p01, p50, p99 = stats.percentiles if stats.percentiles else (None, None, None)
        rows.append((
            column_name, stats["min"], p01, p50, p99, stats["max"],
            stats.negative or 0, stats.zero or 0, stats.null_or_invalid or 0,
        ))
    return spark.createDataFrame(
        rows,
        "column STRING, min DOUBLE, p01 DOUBLE, median DOUBLE, p99 DOUBLE, max DOUBLE, "
        "negative LONG, zero LONG, null_or_invalid LONG",
    )


def regex_pattern_counts(df: DataFrame, column_name: str, patterns: dict, group_by: list = None) -> DataFrame:
    """
    Count how many values match each named regex ({label: regex}); the first matching label wins.
    Values that match none of them are labelled 'no_match', nulls 'null'. Optional `group_by` columns
    show, for example, which date format each platform uses.
    """
    column = F.col(f"`{column_name}`").cast("string")
    labels = list(patterns.items())
    label = F.when(column.isNull(), F.lit("null"))
    for name, regex in labels:
        label = label.when(column.rlike(regex), F.lit(name))
    label = label.otherwise(F.lit("no_match"))

    group_columns = [F.col(f"`{c}`") for c in (group_by or [])]
    return (
        df.groupBy(*group_columns, label.alias("pattern"))
        .count()
        .orderBy(*group_columns, F.desc("count"))
    )


def duplicate_key_count(df: DataFrame, key_columns: list) -> dict:
    """
    How unique a key is: `duplicate_keys` = keys that appear more than once,
    `extra_rows` = rows that deduplication on this key would remove.
    """
    counts = df.groupBy(*key_columns).count().filter("count > 1")
    result = counts.agg(F.count("*").alias("keys"), F.sum(F.col("count") - 1).alias("extra")).first()
    return {"duplicate_keys": result["keys"] or 0, "extra_rows": result["extra"] or 0}


def constant_columns(df: DataFrame) -> list:
    """Columns with at most one distinct non-null value: they carry no information."""
    candidates = df.agg(
        *[F.approx_count_distinct(F.col(f"`{c}`")).alias(c) for c in df.columns]
    ).first().asDict()
    # approx_count_distinct is exact for such small numbers in practice; confirm the candidates exactly.
    suspects = [c for c, n in candidates.items() if n <= 2]
    if not suspects:
        return []
    exact = df.agg(*[F.countDistinct(F.col(f"`{c}`")).alias(c) for c in suspects]).first().asDict()
    return [c for c in suspects if exact[c] <= 1]


def text_anomaly_counts(df: DataFrame, columns: list = None) -> DataFrame:
    """
    Per string column: rows with a carriage return (\\r), a line feed (\\n), or leading/trailing spaces.
    Line breaks inside CSV values are what breaks a CSV read without multiLine.
    """
    if columns is None:
        columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    if not columns:
        return spark.createDataFrame([], "column STRING, carriage_return LONG, line_feed LONG, untrimmed LONG")

    aggregations = []
    for column_name in columns:
        column = F.col(f"`{column_name}`")
        aggregations += [
            F.sum(column.contains("\r").cast("int")).alias(f"{column_name}__cr"),
            F.sum(column.contains("\n").cast("int")).alias(f"{column_name}__lf"),
            F.sum((column != F.trim(column)).cast("int")).alias(f"{column_name}__untrimmed"),
        ]
    result = df.agg(*aggregations).first().asDict()
    rows = [
        (c, result[f"{c}__cr"] or 0, result[f"{c}__lf"] or 0, result[f"{c}__untrimmed"] or 0)
        for c in columns
    ]
    return spark.createDataFrame(rows, "column STRING, carriage_return LONG, line_feed LONG, untrimmed LONG")

