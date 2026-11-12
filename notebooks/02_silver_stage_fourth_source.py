# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Silver stage: FourthSourceSystem
# MAGIC
# MAGIC Cleans the **Fourth** vehicle source from one Bronze ingestion partition and overwrites its stage table
# MAGIC `abfss://silver@sausedcarspipelinesv.dfs.core.windows.net/vehicle_listings_stage/fourth` (path-based Delta).
# MAGIC `03_silver_vehicle_listings` combines the four stage tables into `silver.vehicle_listings`.
# MAGIC
# MAGIC Every source has its own notebook (its own pipeline task), so a change in one source never touches the others.
# MAGIC Rules that are the same for every source are in `utils/vehicle_listings_cleaning.py`.
# MAGIC Data model and rules: `documentation/silver_data_model.md`.
# MAGIC
# MAGIC **Source specifics**
# MAGIC - Newline-delimited JSON in a file without an extension (`FourthSource/ingestion_date=.../FourthSourceSystem`).
# MAGIC   ADF copies it as DelimitedText, but the content is one JSON object per line, so it is read with
# MAGIC   `spark.read.json`. The Bronze folder is called `FourthSource` (set in `VEHICLE_SOURCES`).
# MAGIC - Every listing is priced in GBP.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) or a `YYYY-MM-DD` Bronze partition date.
# MAGIC - `dev_mode`: `true` reads only a sample and writes to `vehicle_listings_stage_dev/fourth`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/vehicle_listings_cleaning

# COMMAND ----------

SOURCE_SYSTEM = "Fourth"
# Every listing is priced in GBP.
# Any other currency is only reported (the value is still validated by the shared rules).
EXPECTED_CURRENCIES = ["GBP"]

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

TABLE_NAME = f"vehicle_listings_stage/{SOURCE_SYSTEM.lower()}"
TARGET_PATH = stage_table_path(SOURCE_SYSTEM, DEV_MODE)

log = get_logger("02_silver_stage_fourth_source")
log.info(f"source={SOURCE_SYSTEM}, ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Read Bronze and clean

# COMMAND ----------

raw_df, partition_path, partition_date = read_vehicle_source(SOURCE_SYSTEM, INGESTION_DATE, DEV_MODE)
stage_df = clean_vehicle_listings(raw_df, SOURCE_SYSTEM, partition_date).cache()

raw_count = raw_df.count()
stage_count = stage_df.count()
log.info(
    f"{SOURCE_SYSTEM}: {partition_path} -> {raw_count} raw rows, {stage_count} after dedup "
    f"({raw_count - stage_count} duplicates removed)"
)

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

unparsed_dates = check_vehicle_listings(stage_df, STAGE_COLUMNS, TABLE_NAME)

other_currencies = unexpected_currencies(stage_df, EXPECTED_CURRENCIES)
if other_currencies:
    log.warning(f"{SOURCE_SYSTEM} has currencies that are not expected for this source: {other_currencies}")

log.info(f"{TABLE_NAME}: {stage_count} rows, checks passed ({unparsed_dates} unparsed posting_date values)")

# COMMAND ----------

# MAGIC %md ## Write the stage table (Delta, full overwrite)

# COMMAND ----------

write_delta_table(stage_df, TARGET_PATH, num_files=STAGE_OUTPUT_FILES)
stage_df.unpersist()

silver_df = read_delta_table(TARGET_PATH)
log.info(f"Written {silver_df.count()} rows to {TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Data quality report

# COMMAND ----------

display(
    silver_df.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("is_valid").cast("int")).alias("valid_rows"),
        F.round(100 * F.avg(F.col("is_valid").cast("int")), 2).alias("valid_pct"),
    )
)

# COMMAND ----------

display(dq_issue_counts(silver_df))

# COMMAND ----------

display(silver_df.groupBy("platform_name", "currency").count().orderBy("platform_name", "currency"))
