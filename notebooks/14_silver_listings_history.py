# Databricks notebook source
# MAGIC %md
# MAGIC # 14 – Silver: vehicle_listings_history (incremental load, SCD Type 2)
# MAGIC
# MAGIC Applies the new Bronze days of the incremental file load (documentation/incremental_load.md) to the
# MAGIC path-based Delta table `silver/vehicle_listings_history`: **every version** of every listing with `valid_from`,
# MAGIC `valid_to` and `is_current` (the same SCD2 model as the CDC demo, `utils/scd2_helpers`). The current state is
# MAGIC `WHERE is_current`.
# MAGIC
# MAGIC 1. Read the Bronze days from the newest one already in the table up to `ingestion_date`.
# MAGIC 2. Skip the records up to the watermark (highest `change_seq` already applied) and repeated records, so a
# MAGIC    re-run or a day loaded twice changes nothing.
# MAGIC 3. Clean with the same rules as the files (`clean_vehicle_listings`, `add_manufacturer`).
# MAGIC 4. `change_op`: `I` for the first record of a vehicle, `U` for a later one.
# MAGIC 5. Build the versions and MERGE them; check the SCD2 rules on the whole table.
# MAGIC
# MAGIC **Widgets:** `ingestion_date` (`latest` or a Bronze day), `dev_mode` (`true` writes `vehicle_listings_history_dev`).

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

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

TARGET_PATH = history_silver_path(DEV_MODE)
TABLE_NAME = TARGET_PATH.rsplit("/", 1)[-1]

log = get_logger("14_silver_listings_history")
log.info(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------


def first_record_flags(changes_df: DataFrame, known_vehicles_df: DataFrame = None) -> DataFrame:
    """`change_op` I for the first record of a vehicle (not in the table, first in the batch), U otherwise."""
    order = Window.partitionBy("vehicle_id").orderBy("change_seq")
    first_in_batch = F.row_number().over(order) == 1
    if known_vehicles_df is not None:
        changes_df = changes_df.join(
            known_vehicles_df.select("vehicle_id").distinct().withColumn("_known", F.lit(True)), "vehicle_id", "left"
        )
    else:
        changes_df = changes_df.withColumn("_known", F.lit(False))
    is_new = first_in_batch & ~F.coalesce(F.col("_known"), F.lit(False))
    return changes_df.withColumn("change_op", F.when(is_new, INSERT_OP).otherwise(UPDATE_OP)).drop("_known")


def clean_records(records_df: DataFrame, lookup_df: DataFrame, ingestion_date: str) -> DataFrame:
    """Clean the raw records like the files and return the change columns for build_versions (without change_op)."""
    raw_df = records_df.select(
        *RAW_LISTING_COLUMNS,
        "source_file_path",
        F.struct(
            "change_seq",
            F.to_timestamp("updated_at").alias("event_ts"),
            "ingestion_date",
        ).alias("_inc"),
    )
    cleaned = clean_vehicle_listings(raw_df, INCREMENTAL_SOURCE_SYSTEM, ingestion_date, carry_columns=["_inc"])
    return (
        add_manufacturer(cleaned, lookup_df)
        .withColumn("ingestion_date", F.col("_inc.ingestion_date"))  # the record's own Bronze day
        .select(
            *VEHICLE_LISTINGS_COLUMNS,
            content_hash(SCD2_BUSINESS_COLUMNS).alias("row_hash"),
            F.col("_inc.change_seq").alias("change_seq"),
            F.col("_inc.event_ts").alias("event_ts"),
        )
    )


# COMMAND ----------

# MAGIC %md ## Read the new records

# COMMAND ----------

table_exists = delta_table_exists(TARGET_PATH)
table_df = read_table_snapshot(TARGET_PATH) if table_exists else None  # pinned: we write to it below
watermark = applied_watermark(table_df) if table_exists else None
watermark_date = str(table_df.agg(F.max("ingestion_date")).first()[0]) if table_exists else None

partitions = select_change_partitions(
    list_ingestion_dates(INCREMENTAL_BRONZE_FOLDER), INGESTION_DATE, watermark_date
)
paths = [bronze_day_path(d) for d in partitions]
log.info(f"Table exists: {table_exists}, watermark change_seq {watermark}; reading Bronze days {partitions}")

records_df = (
    read_records(paths)
    .select("*", F.col("_metadata.file_path").alias("source_file_path"))
    .withColumn("ingestion_date", F.to_date(F.regexp_extract("source_file_path", r"ingestion_date=(\d{4}-\d{2}-\d{2})", 1)))
)
new_df = (records_df.filter(F.col("change_seq") > F.lit(watermark)) if watermark is not None else records_df) \
    .dropDuplicates(["change_seq"]).cache()
read_count, new_count = records_df.count(), new_df.count()
log.info(f"{read_count} records read, {new_count} new, {read_count - new_count} already applied")

if new_count == 0:
    _ = new_df.unpersist()
    log.info("Nothing to apply")
    dbutils.notebook.exit("nothing to apply")

# COMMAND ----------

# MAGIC %md ## Clean, build the versions and write

# COMMAND ----------

lookup_path = brand_lookup_path(DEV_MODE)
assert delta_table_exists(lookup_path), f"No brand lookup at {lookup_path}: run 02_silver_brand_lookup first"
lookup_df = read_delta_table(lookup_path).select("model_normalized", "manufacturer")

batch_date = str(new_df.agg(F.max("ingestion_date")).first()[0])
changes_df = first_record_flags(clean_records(new_df, lookup_df, batch_date), table_df).cache()
versions_df = build_versions(changes_df, table_df.filter("is_current") if table_exists else None).cache()
versions_count = versions_df.count()

if versions_count == 0:
    log.info(f"{new_count} records without a new business value: nothing to apply")
    for cached in (new_df, changes_df, versions_df):
        _ = cached.unpersist()
    dbutils.notebook.exit("nothing to apply")

by_op = {r.change_op: r["count"] for r in changes_df.groupBy("change_op").count().collect()}
inserted, closed = apply_versions(versions_df, TARGET_PATH, table_exists)
log.info(
    f"{new_count} new records {by_op}: {inserted} new versions, {closed} versions closed, "
    f"{new_count - inserted} records without a new business value"
)

# COMMAND ----------

# MAGIC %md ## Checks and report

# COMMAND ----------

history_df = read_delta_table(TARGET_PATH)
counts = check_scd2_versions(history_df, TABLE_NAME)
log.info(
    f"{TABLE_NAME}: {counts['versions']} versions of {counts['vehicles']} listings, "
    f"{counts['current_versions']} current; new watermark change_seq {applied_watermark(history_df)}"
)

for cached in (new_df, changes_df, versions_df):
    _ = cached.unpersist()

# COMMAND ----------

# Versions per Bronze day and kind of record.
display(history_df.groupBy("ingestion_date", "change_op").count().orderBy("ingestion_date", "change_op"))
