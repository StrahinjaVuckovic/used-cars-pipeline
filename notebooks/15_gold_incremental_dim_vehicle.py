# Databricks notebook source
# MAGIC %md
# MAGIC # 15 – Gold: dim_vehicle, current state (incremental load)
# MAGIC
# MAGIC Builds `gold/incremental/dim_vehicle` from the Silver history `silver/vehicle_listings_history`
# MAGIC (documentation/incremental_load.md): **only the current state**, one row per vehicle, with the same
# MAGIC business-ready attributes as the main `07_gold_dim_vehicle`. The history stays in Silver. Full rebuild on every
# MAGIC run (`build_current_dim_vehicle` in `utils/scd2_helpers`, shared with the CDC demo).
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads `vehicle_listings_history_dev` and writes `dim_vehicle_dev`).

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

# MAGIC %run ./utils/incremental_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

log = get_logger("15_gold_incremental_dim_vehicle")
log.info(f"dev_mode={DEV_MODE}")

# COMMAND ----------

versions_df = read_required_table(history_silver_path(DEV_MODE), "14_silver_listings_history")
dim_df = build_current_dim_vehicle(versions_df).cache()
vehicles = check_current_dim_vehicle(dim_df, versions_df, "dim_vehicle")

path = incremental_gold_path("dim_vehicle", DEV_MODE)
write_delta_table(dim_df, path, num_files=1)
log.info(f"Written {vehicles} current vehicles (+ Unknown) to {path}")

# COMMAND ----------

# Current vehicles per manufacturer (top 10).
display(dim_df.groupBy("manufacturer").count().orderBy(F.desc("count")).limit(10))

# COMMAND ----------

_ = dim_df.unpersist()
