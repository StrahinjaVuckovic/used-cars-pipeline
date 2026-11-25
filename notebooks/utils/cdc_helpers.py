# Databricks notebook source
# MAGIC %md
# MAGIC # CDC demo helpers – log-based change data capture
# MAGIC
# MAGIC Used by `10_silver_listing_changes` and `11_gold_cdc_vehicle_history` (documentation/cdc.md). Run after
# MAGIC `00_config`, `utils/silver_transformations`, `utils/vehicle_listings_cleaning`, `utils/gold_helpers` and
# MAGIC `utils/scd2_helpers` (the SCD2 versions and the current-only Gold dimension are shared with the incremental
# MAGIC file load).
# MAGIC
# MAGIC The input are change events that `cdc/cdc_reader.py` read from the WAL of the source PostgreSQL database
# MAGIC (`bronze/ListingChanges/ingestion_date=…/*.json`): `op` R (initial snapshot) / I / U / D, the WAL position
# MAGIC `lsn_num` (orders the changes; it becomes `change_seq` in Silver), the commit time `commit_ts` and the raw
# MAGIC row `before` / `after` the change.

# COMMAND ----------

CDC_SOURCE_SYSTEM = "listings_db"      # `source_system` of the demo rows (the platform's database)
CHANGES_FOLDER = "ListingChanges"      # bronze/ListingChanges/ingestion_date=YYYY-MM-DD/

# Types of the raw columns in the change events (the PostgreSQL column types).
CDC_RAW_TYPES = RAW_LISTING_JSON_TYPES
_ROW_IMAGE = ", ".join(f"`{c}`: {CDC_RAW_TYPES.get(c, 'STRING')}" for c in RAW_LISTING_COLUMNS)
CHANGE_EVENT_SCHEMA = (
    f"op STRING, lsn STRING, lsn_num BIGINT, commit_ts STRING, `table` STRING, "
    f"before STRUCT<{_ROW_IMAGE}>, after STRUCT<{_ROW_IMAGE}>"
)


def cdc_silver_path(dev_mode: bool) -> str:
    return f"{SILVER_PATH}/vehicle_listings_cdc{'_dev' if dev_mode else ''}"


def cdc_gold_path(table_name: str, dev_mode: bool) -> str:
    return f"{GOLD_PATH}/cdc/{table_name}{'_dev' if dev_mode else ''}"


def read_change_events(paths: list) -> DataFrame:
    """The change events of the given Bronze folders, with the vehicle id, the event time and lineage."""
    return (
        spark.read.schema(CHANGE_EVENT_SCHEMA).json(paths)
        .select("*", F.col("_metadata.file_path").alias("source_file_path"))
        .withColumn("vehicle_id", F.coalesce(F.col("after.id"), F.col("before.id")))
        .withColumn("event_ts", F.to_timestamp("commit_ts"))
        .withColumn(
            "ingestion_date",
            F.to_date(F.regexp_extract("source_file_path", r"ingestion_date=(\d{4}-\d{2}-\d{2})", 1)),
        )
    )


def new_change_events(events_df: DataFrame, table_exists: bool, watermark) -> DataFrame:
    """
    Events that still have to be applied. The reader delivers at least once, so the same event can come
    again (in a second file after a crash): events up to the watermark and repeated events are dropped.
    The snapshot is applied only once, when the table does not exist yet.
    """
    if table_exists:
        events_df = events_df.filter(F.col("op") != SNAPSHOT_OP)
        if watermark is not None:
            events_df = events_df.filter(F.col("lsn_num") > F.lit(watermark))
    return events_df.dropDuplicates(["vehicle_id", "lsn_num", "op"])


def clean_change_events(events_df: DataFrame, lookup_df: DataFrame, ingestion_date: str) -> DataFrame:
    """
    Clean the row image of every event with the same rules as the files (`clean_vehicle_listings`) and map
    the manufacturer (`add_manufacturer`). A DELETE has no new row, so its old row ("before") is used.
    Returns `SCD2_CHANGE_COLUMNS` (the WAL position becomes `change_seq`).
    """
    image = F.when(F.col("op") == DELETE_OP, F.col("before")).otherwise(F.col("after"))
    raw_df = events_df.select(
        image.alias("_image"),
        "source_file_path",
        F.struct("op", "lsn_num", "event_ts", "ingestion_date").alias("_cdc"),
    ).select("_image.*", "source_file_path", "_cdc")
    cleaned = clean_vehicle_listings(raw_df, CDC_SOURCE_SYSTEM, ingestion_date, carry_columns=["_cdc"])
    return (
        add_manufacturer(cleaned, lookup_df)
        .withColumn("ingestion_date", F.col("_cdc.ingestion_date"))  # the event's own Bronze partition
        .select(
            *VEHICLE_LISTINGS_COLUMNS,
            content_hash(SCD2_BUSINESS_COLUMNS).alias("row_hash"),
            F.col("_cdc.op").alias("change_op"),
            F.col("_cdc.lsn_num").alias("change_seq"),
            F.col("_cdc.event_ts").alias("event_ts"),
        )
    )
