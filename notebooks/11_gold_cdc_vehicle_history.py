# Databricks notebook source
# MAGIC %md
# MAGIC # 11 – Gold: dim_vehicle, current state (CDC demo)
# MAGIC
# MAGIC Builds the Gold dimension of the CDC demo (documentation/cdc.md) from the Silver history
# MAGIC `silver/vehicle_listings_cdc`: **only the current state** (mentor's decision), one row per vehicle that exists
# MAGIC in the source now, with the same business-ready attributes as the main `07_gold_dim_vehicle`. The history
# MAGIC (`valid_from`, `valid_to`, `is_current`) stays in Silver; questions over time are asked there.
# MAGIC
# MAGIC Full rebuild on every run, stored under `gold/cdc/` apart from the main star schema.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads `vehicle_listings_cdc_dev` and writes `dim_vehicle_dev`).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/vehicle_listings_cleaning

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

# MAGIC %run ./utils/scd2_helpers

# COMMAND ----------

# MAGIC %run ./utils/cdc_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

log = get_logger("11_gold_cdc_vehicle_history")
log.info(f"dev_mode={DEV_MODE}")

# COMMAND ----------

versions_df = read_required_table(cdc_silver_path(DEV_MODE), "10_silver_listing_changes")
dim_df = build_current_dim_vehicle(versions_df).cache()
vehicles = check_current_dim_vehicle(dim_df, versions_df, "dim_vehicle")

path = cdc_gold_path("dim_vehicle", DEV_MODE)
write_delta_table(dim_df, path, num_files=1)
log.info(f"Written {vehicles} current vehicles (+ Unknown) to {path}")

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

# Current vehicles per manufacturer (top 10).
display(dim_df.groupBy("manufacturer").count().orderBy(F.desc("count")).limit(10))

# COMMAND ----------

_ = dim_df.unpersist()
