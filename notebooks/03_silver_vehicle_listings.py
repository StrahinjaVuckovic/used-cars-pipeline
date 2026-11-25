# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Silver: vehicle_listings
# MAGIC
# MAGIC Combines the four cleaned vehicle sources (stage tables written by `02_silver_stage_<source>_source`)
# MAGIC into one dataset, maps every listing to its manufacturer with the cleaned brand lookup (`brand_lookup`,
# MAGIC written by `02_silver_brand_lookup`) and overwrites the Delta table stored at
# MAGIC `abfss://silver@sausedcarspipelinesv.dfs.core.windows.net/vehicle_listings` (path-based, see 00_config).
# MAGIC
# MAGIC In the pipeline this task runs after the four source tasks and the brand lookup task. It never reads the
# MAGIC Bronze files itself.
# MAGIC
# MAGIC Data model and rules: `documentation/silver_data_model.md`.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) accepts the input tables as they are; a `YYYY-MM-DD` date also
# MAGIC   checks that every stage table and the brand lookup were built from that Bronze partition.
# MAGIC - `dev_mode`: `true` reads the `vehicle_listings_stage_dev` / `brand_lookup_dev` tables and writes to
# MAGIC   `vehicle_listings_dev`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/vehicle_listings_cleaning

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

# The full table is ~800 MB, so 4 files of ~200 MB each is a good size (no small-files problem).
OUTPUT_FILES = 4

TABLE_NAME = "vehicle_listings_dev" if DEV_MODE else "vehicle_listings"
TARGET_PATH = f"{SILVER_PATH}/{TABLE_NAME}"

log = get_logger("03_silver_vehicle_listings")
log.info(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Read the four stage tables

# COMMAND ----------

stage_tables = {}
stage_rows = []

for source_system in VEHICLE_SOURCES:
    path = stage_table_path(source_system, DEV_MODE)
    notebook = f"02_silver_stage_{source_system.lower()}_source"
    assert delta_table_exists(path), f"No stage table for {source_system} at {path}: run {notebook} first"

    stage_df = read_delta_table(path)
    dates = sorted(str(row.ingestion_date) for row in stage_df.select("ingestion_date").distinct().collect())
    stage_tables[source_system] = stage_df
    stage_rows.append((source_system, ", ".join(dates), stage_df.count(), path))

display(spark.createDataFrame(stage_rows, "source_system STRING, ingestion_date STRING, rows LONG, path STRING"))

# All four sources must come from the same Bronze ingestion, otherwise the table would mix two days.
ingestion_dates = {row[0]: row[1] for row in stage_rows}
assert len(set(ingestion_dates.values())) == 1 and "," not in stage_rows[0][1], (
    f"Stage tables come from different ingestion dates: {ingestion_dates}. "
    "Re-run the source notebooks for the same ingestion_date."
)
stage_ingestion_date = stage_rows[0][1]
if INGESTION_DATE != "latest":
    assert stage_ingestion_date == INGESTION_DATE, (
        f"Stage tables were built from ingestion_date={stage_ingestion_date}, not {INGESTION_DATE}"
    )
log.info(f"All stage tables come from ingestion_date={stage_ingestion_date}")

# COMMAND ----------

# MAGIC %md ## Read the brand lookup

# COMMAND ----------

lookup_path = brand_lookup_path(DEV_MODE)
assert delta_table_exists(lookup_path), f"No brand lookup at {lookup_path}: run 02_silver_brand_lookup first"

lookup_table_df = read_delta_table(lookup_path)
lookup_dates = sorted(str(row.ingestion_date) for row in lookup_table_df.select("ingestion_date").distinct().collect())
# The lookup must come from the same Bronze ingestion as the listings (same ADF copy run).
assert lookup_dates == [stage_ingestion_date], (
    f"Brand lookup was built from ingestion_date={', '.join(lookup_dates)}, the stage tables from "
    f"{stage_ingestion_date}. Re-run 02_silver_brand_lookup for the same ingestion_date."
)
brand_lookup_df = lookup_table_df.select("model_normalized", "manufacturer").cache()
# Checked again here: a duplicate model would multiply listings in the join.
assert_unique(brand_lookup_df, ["model_normalized"], "brand lookup")
log.info(
    f"Brand lookup: {brand_lookup_df.count()} models, "
    f"{brand_lookup_df.select('manufacturer').distinct().count()} manufacturers ({lookup_path})"
)

# COMMAND ----------

# MAGIC %md ## Combine the four sources into one dataset

# COMMAND ----------

vehicle_listings_df = reduce(DataFrame.unionByName, stage_tables.values())

# COMMAND ----------

# MAGIC %md ## Brand mapping

# COMMAND ----------

vehicle_listings_df = add_manufacturer(vehicle_listings_df, brand_lookup_df).select(*VEHICLE_LISTINGS_COLUMNS)

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

# The row count also proves that the brand join did not multiply listings (the lookup key is unique).
expected_rows = sum(row[2] for row in stage_rows)
actual_rows = vehicle_listings_df.count()
assert actual_rows == expected_rows, f"Row count {actual_rows} != sum of stage tables {expected_rows}"

# vehicle_id must also be unique across sources; each source notebook only sees its own rows.
unparsed_dates = check_vehicle_listings(vehicle_listings_df, VEHICLE_LISTINGS_COLUMNS, "vehicle_listings")

log.info(f"vehicle_listings: {actual_rows} rows, checks passed ({unparsed_dates} unparsed posting_date values)")

# COMMAND ----------

# MAGIC %md ## Write to Silver (Delta, full overwrite)

# COMMAND ----------

write_delta_table(vehicle_listings_df, TARGET_PATH, num_files=OUTPUT_FILES)
brand_lookup_df.unpersist()

silver_df = read_delta_table(TARGET_PATH)
log.info(f"Written {silver_df.count()} rows to {TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Data quality report

# COMMAND ----------

display(
    silver_df.groupBy("source_system")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("is_valid").cast("int")).alias("valid_rows"),
        F.round(100 * F.avg(F.col("is_valid").cast("int")), 2).alias("valid_pct"),
    )
    .orderBy("source_system")
)

# COMMAND ----------

display(dq_issue_counts(silver_df))

# COMMAND ----------

display(null_percentages(silver_df))

# COMMAND ----------

display(silver_df.groupBy("source_system", "currency").count().orderBy("source_system", "currency"))

# COMMAND ----------

# Brand mapping coverage: where the manufacturer came from, per source.
display(
    silver_df.groupBy("source_system", "manufacturer_source")
    .count()
    .withColumn("pct", F.round(100 * F.col("count") / F.sum("count").over(Window.partitionBy("source_system")), 2))
    .orderBy("source_system", "manufacturer_source")
)

# COMMAND ----------

# Spot check: raw vs parsed posting_date per platform.
display(
    silver_df.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("platform_name").orderBy("vehicle_id"))
    )
    .filter("_rn <= 5")
    .select("platform_name", "posting_date_raw", "posting_date")
)
