# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental file load helpers – watermarks, paths, source record schema
# MAGIC
# MAGIC Used by `12_incremental_extract_source`, `13_incremental_ingest_bronze`, `14_silver_listings_history` and
# MAGIC `15_gold_incremental_dim_vehicle` (documentation/incremental_load.md). Run after `00_config`,
# MAGIC `utils/silver_transformations`, `utils/vehicle_listings_cleaning`, `utils/gold_helpers` and `utils/scd2_helpers`.
# MAGIC
# MAGIC **Watermark** = how far a load has come, stored in a small Delta control table
# MAGIC `bronze/_control/watermarks` (`pipeline`, `watermark_value`, `updated_at_run`). A run reads only what is after
# MAGIC its watermark and moves the watermark **after** the data is written: a crash in between means the same data is
# MAGIC loaded again (at-least-once), which the next layers absorb (Bronze overwrites the day's partition, Silver skips
# MAGIC changes it already has by `change_seq`).

# COMMAND ----------

INCREMENTAL_SOURCE_FOLDER = "IncrementalSource"      # landing/IncrementalSource/{original,source}/
INCREMENTAL_BRONZE_FOLDER = "IncrementalListings"     # bronze/IncrementalListings/ingestion_date=YYYY-MM-DD/
INCREMENTAL_SOURCE_SYSTEM = "fourth_incremental"      # `source_system` of the Silver rows

# Watermark names.
ORIGINAL_TO_SOURCE = "original_to_source"   # last day copied from the original file into the source
SOURCE_TO_BRONZE = "source_to_bronze"       # last day loaded from the source into Bronze

# A record of the original / source file: the raw listing columns plus the change date and its sequence number.
SOURCE_RECORD_SCHEMA = ", ".join(
    [f"`{c}` {RAW_LISTING_JSON_TYPES.get(c, 'STRING')}" for c in RAW_LISTING_COLUMNS]
    + ["`updated_at` STRING", "`change_seq` BIGINT"]
)


def original_path() -> str:
    return f"{LANDING_PATH}/{INCREMENTAL_SOURCE_FOLDER}/original"


def source_path() -> str:
    return f"{LANDING_PATH}/{INCREMENTAL_SOURCE_FOLDER}/source"


def bronze_day_path(day: str) -> str:
    return f"{BRONZE_PATH}/{INCREMENTAL_BRONZE_FOLDER}/{INGESTION_PARTITION_PREFIX}{day}"


def history_silver_path(dev_mode: bool) -> str:
    return f"{SILVER_PATH}/vehicle_listings_history{'_dev' if dev_mode else ''}"


def incremental_gold_path(table_name: str, dev_mode: bool) -> str:
    return f"{GOLD_PATH}/incremental/{table_name}{'_dev' if dev_mode else ''}"


def watermarks_path() -> str:
    return f"{BRONZE_PATH}/_control/watermarks"


def read_records(path) -> DataFrame:
    """Records of the original or source file (NDJSON) with the explicit schema."""
    return spark.read.schema(SOURCE_RECORD_SCHEMA).json(path)


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:  # noqa: BLE001 – a missing folder raises a different error on Databricks and locally
        return False


# COMMAND ----------

# MAGIC %md ## Watermark control table

# COMMAND ----------


def read_watermark(pipeline: str):
    """The watermark of a load (a YYYY-MM-DD string), or None before its first run."""
    path = watermarks_path()
    if not delta_table_exists(path):
        return None
    row = read_delta_table(path).filter(F.col("pipeline") == pipeline).first()
    return row.watermark_value if row else None


def write_watermark(pipeline: str, value: str) -> None:
    """Insert or move the watermark of a load (MERGE on `pipeline`)."""
    df = spark.createDataFrame([(pipeline, value)], "pipeline STRING, watermark_value STRING").withColumn(
        "updated_at_run", F.current_timestamp()
    )
    path = watermarks_path()
    if delta_table_exists(path):
        merge_delta_table(df, path, ["pipeline"])
    else:
        write_delta_table(df, path, num_files=1)
