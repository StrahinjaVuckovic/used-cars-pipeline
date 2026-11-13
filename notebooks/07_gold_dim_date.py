# Databricks notebook source
# MAGIC %md
# MAGIC # 07 – Gold: dim_date
# MAGIC
# MAGIC Calendar dimension: one row per day from `DIM_DATE_START` (1950-01-01, the earliest service date Silver
# MAGIC accepts) to `DIM_DATE_END` (2030-12-31, fixed ahead for planning and BI filters), plus the Unknown member
# MAGIC (`date_key = -1`). It is generated, not read from Silver, so it has the same rows every day.
# MAGIC
# MAGIC **Role-playing:** the same table is the posting date of `fact_listing`, the service date of
# MAGIC `fact_service_event` and the rate date of `fact_exchange_rate`.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` writes `dim_date_dev`), `ingestion_date` (not used; every Gold Job task gets the
# MAGIC same Job parameters).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
TABLE_NAME = "dim_date"

log = get_logger("07_gold_dim_date")
log.info(f"dev_mode={DEV_MODE}, range {DIM_DATE_START} -> {DIM_DATE_END}")

# COMMAND ----------


def build_dim_date(start: str, end: str) -> DataFrame:
    """One row per calendar day between start and end (inclusive), plus the Unknown member."""
    days = spark.sql(
        f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date"
    )
    # ISO day of week: Monday = 1 ... Sunday = 7 (Spark's dayofweek starts with Sunday = 1).
    iso_day_of_week = ((F.dayofweek("date") + 5) % 7) + 1
    calendar = days.select(
        date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").alias("week_of_year"),
        iso_day_of_week.alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        (iso_day_of_week >= 6).alias("is_weekend"),
    )
    return with_unknown_member(calendar, "date_key")


# COMMAND ----------

dim_date = build_dim_date(DIM_DATE_START, DIM_DATE_END).cache()

expected_days = spark.sql(f"SELECT datediff(DATE'{DIM_DATE_END}', DATE'{DIM_DATE_START}') + 1").first()[0]
row_count = dim_date.count()
assert row_count == expected_days + 1, f"{TABLE_NAME}: {row_count} rows, expected {expected_days} days + Unknown"
assert_unique(dim_date, ["date_key"], TABLE_NAME)
assert_unique(dim_date, ["date"], TABLE_NAME)

path = write_gold_table(dim_date, TABLE_NAME, DEV_MODE, num_files=1)
dim_date.unpersist()
log.info(f"Written {row_count} rows ({expected_days} days + Unknown) to {path}")

# COMMAND ----------

display(read_delta_table(path).filter(F.col("date").between("2026-11-06", "2026-11-09") | (F.col("date_key") == -1)))
