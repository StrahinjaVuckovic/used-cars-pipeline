# Databricks notebook source
# MAGIC %md
# MAGIC # 13 – Incremental load: source file → Bronze
# MAGIC
# MAGIC Loads only the new records of the growing source `landing/IncrementalSource/source/` into Bronze
# MAGIC (documentation/incremental_load.md): the records with `updated_at` after the watermark `source_to_bronze`, one
# MAGIC Bronze partition per day (`bronze/IncrementalListings/ingestion_date=<day>/`), then the watermark moves.
# MAGIC
# MAGIC A day's partition is overwritten, so loading the same day again after a crash (data written, watermark not
# MAGIC moved) gives the same Bronze content.

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

log = get_logger("13_incremental_ingest_bronze")

# COMMAND ----------

assert path_exists(source_path()), f"No source at {source_path()}: run 12_incremental_extract_source first"
source_df = read_records(source_path())
watermark = read_watermark(SOURCE_TO_BRONZE)
new_df = source_df.filter(F.col("updated_at") > F.lit(watermark)) if watermark else source_df
new_days = [r.updated_at for r in new_df.select("updated_at").distinct().orderBy("updated_at").collect()]
log.info(f"Watermark {SOURCE_TO_BRONZE} = {watermark}; new days in the source: {new_days}")

if not new_days:
    log.info("No record after the watermark: nothing to ingest")
    dbutils.notebook.exit("nothing to ingest")

for day in new_days:
    day_df = new_df.filter(F.col("updated_at") == day)
    day_df.coalesce(1).write.mode("overwrite").json(bronze_day_path(day))
    log.info(f"Bronze {bronze_day_path(day)}: {day_df.count()} records")

write_watermark(SOURCE_TO_BRONZE, new_days[-1])
log.info(f"Watermark {SOURCE_TO_BRONZE} = {new_days[-1]}")
