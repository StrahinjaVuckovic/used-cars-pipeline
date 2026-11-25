# Databricks notebook source
# MAGIC %md
# MAGIC # 10 – Silver: vehicle_listings_cdc (CDC demo)
# MAGIC
# MAGIC **Demo of log-based change data capture** (documentation/cdc.md), next to the daily file pipeline and
# MAGIC independent of it. The source is a PostgreSQL database that plays a listing platform; `cdc/cdc_reader.py`
# MAGIC reads its changes from the write-ahead log and lands them in `bronze/ListingChanges/ingestion_date=…/`.
# MAGIC
# MAGIC This notebook applies the new change events to the path-based Delta table `silver/vehicle_listings_cdc`:
# MAGIC one table with **every version** of every listing (SCD Type 2: `valid_from`, `valid_to`, `is_current`);
# MAGIC the current state is `WHERE is_current`.
# MAGIC
# MAGIC 1. Read the Bronze partitions from the newest one already applied up to `ingestion_date`.
# MAGIC 2. Drop the events up to the watermark (highest applied WAL position) and repeated events: the reader
# MAGIC    delivers at least once, so a re-run or a re-delivered file changes nothing.
# MAGIC 3. Clean every row image with the same rules as the files (`clean_vehicle_listings`, `add_manufacturer`).
# MAGIC 4. Build the versions in WAL order and MERGE them: new versions are inserted, the versions they close
# MAGIC    get `valid_to` (a DELETE only closes). The first run loads the initial snapshot.
# MAGIC
# MAGIC **Widgets:** `ingestion_date` (`latest` or a Bronze date), `dev_mode` (`true` writes `vehicle_listings_cdc_dev`).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/vehicle_listings_cleaning

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

# MAGIC %run ./utils/cdc_helpers

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

TARGET_PATH = cdc_silver_path(DEV_MODE)
TABLE_NAME = TARGET_PATH.rsplit("/", 1)[-1]

log = get_logger("10_silver_listing_changes")
log.info(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Read the new change events

# COMMAND ----------

table_exists = delta_table_exists(TARGET_PATH)
table_df = read_delta_table(TARGET_PATH) if table_exists else None
watermark = change_watermark(table_df) if table_exists else None
watermark_date = (
    str(table_df.agg(F.max("ingestion_date")).first()[0]) if table_exists else None
)

partitions = select_change_partitions(list_ingestion_dates(CHANGES_FOLDER), INGESTION_DATE, watermark_date)
paths = [f"{BRONZE_PATH}/{CHANGES_FOLDER}/{INGESTION_PARTITION_PREFIX}{d}" for d in partitions]
log.info(f"Table exists: {table_exists}, watermark LSN {watermark}; reading partitions {partitions}")

events_df = read_change_events(paths).cache()
new_events_df = new_change_events(events_df, table_exists, watermark).cache()

read_count = events_df.count()
new_count = new_events_df.count()
by_op = {r.op: r["count"] for r in new_events_df.groupBy("op").count().collect()}
log.info(f"{read_count} events read, {new_count} new {by_op}, {read_count - new_count} skipped (snapshot or already applied)")

if new_count == 0:
    _ = events_df.unpersist()
    _ = new_events_df.unpersist()
    log.info("Nothing to apply")
    dbutils.notebook.exit("nothing to apply")

if not table_exists:
    assert by_op.get(SNAPSHOT_OP, 0) > 0, "First run without the initial snapshot: run `cdc_reader --snapshot` first"

# COMMAND ----------

# MAGIC %md ## Clean, build the versions and write

# COMMAND ----------

lookup_path = brand_lookup_path(DEV_MODE)
assert delta_table_exists(lookup_path), f"No brand lookup at {lookup_path}: run 02_silver_brand_lookup first"
lookup_df = read_delta_table(lookup_path).select("model_normalized", "manufacturer").cache()

batch_date = str(new_events_df.agg(F.max("ingestion_date")).first()[0])
changes_df = clean_change_events(new_events_df, lookup_df, batch_date).cache()
open_versions_df = table_df.filter("is_current") if table_exists else None

versions_df = build_versions(changes_df, open_versions_df).cache()
versions_count = versions_df.count()

if versions_count == 0:
    # Only changes that repeat the current values (e.g. re-delivered events of a no-op update): nothing to write.
    log.info(f"{new_count} changes without a new business value: nothing to apply")
    for cached in (events_df, new_events_df, lookup_df, changes_df, versions_df):
        _ = cached.unpersist()
    dbutils.notebook.exit("nothing to apply")

if table_exists:
    metrics = merge_delta_table(versions_df, TARGET_PATH, ["version_key"])
    inserted, closed = metrics.get("numTargetRowsInserted", 0), metrics.get("numTargetRowsUpdated", 0)
else:
    write_delta_table(versions_df, TARGET_PATH, num_files=1)
    inserted, closed = versions_count, 0

skipped_changes = new_count - by_op.get(DELETE_OP, 0) - inserted
log.info(
    f"{inserted} new versions, {closed} versions closed by a change or a sale, "
    f"{skipped_changes} changes without a new business value (no new version)"
)

# COMMAND ----------

# MAGIC %md ## Checks on the whole table

# COMMAND ----------

history_df = read_delta_table(TARGET_PATH)
counts = check_cdc_versions(history_df, TABLE_NAME)
log.info(
    f"{TABLE_NAME}: {counts['versions']} versions of {counts['vehicles']} listings, "
    f"{counts['current_versions']} current (the source state); new watermark LSN {change_watermark(history_df)}"
)

for cached in (events_df, new_events_df, lookup_df, changes_df, versions_df):
    _ = cached.unpersist()

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

# Versions per kind of change, current and closed.
display(history_df.groupBy("change_op", "is_current").count().orderBy("change_op", "is_current"))

# COMMAND ----------

# Listings with the most versions (price drops, attribute changes).
display(
    history_df.groupBy("vehicle_id").count().filter("count > 1")
    .orderBy(F.desc("count"), "vehicle_id").limit(10)
)
