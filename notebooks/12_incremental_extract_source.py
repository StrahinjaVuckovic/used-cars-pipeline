# Databricks notebook source
# MAGIC %md
# MAGIC # 12 – Incremental load: original file → source file
# MAGIC
# MAGIC Simulates a source system that produces new data every day (documentation/incremental_load.md). The original
# MAGIC file `landing/IncrementalSource/original/` holds all records, each with the day it appeared (`updated_at`):
# MAGIC five days, new listings plus changed versions of earlier ones. Every run copies the **next day** after the
# MAGIC watermark `original_to_source` into the growing source `landing/IncrementalSource/source/`, which is the input
# MAGIC of Bronze, and then moves the watermark.
# MAGIC
# MAGIC **Widgets:** `days` (how many new days to copy in this run, default 1).

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

dbutils.widgets.text("days", "1")
DAYS = int(dbutils.widgets.get("days"))
assert DAYS >= 1, "days must be at least 1"

log = get_logger("12_incremental_extract_source")

# COMMAND ----------

original_df = read_records(original_path())
watermark = read_watermark(ORIGINAL_TO_SOURCE)
remaining_days = [
    r.updated_at for r in original_df.select("updated_at").distinct().orderBy("updated_at").collect()
    if watermark is None or r.updated_at > watermark
]
log.info(f"Watermark {ORIGINAL_TO_SOURCE} = {watermark}; days left in the original: {remaining_days}")

if not remaining_days:
    log.info("The original file has no day after the watermark: nothing to extract")
    dbutils.notebook.exit("nothing to extract")

days_to_copy = remaining_days[:DAYS]
batch_df = original_df.filter(F.col("updated_at").isin(days_to_copy))
records = batch_df.count()

# Data first, watermark second: a crash in between copies the same day again (the next layers absorb repeats).
batch_df.coalesce(1).write.mode("append").json(source_path())
write_watermark(ORIGINAL_TO_SOURCE, days_to_copy[-1])
log.info(f"Copied {records} records of {days_to_copy} into the source; watermark {ORIGINAL_TO_SOURCE} = {days_to_copy[-1]}")
