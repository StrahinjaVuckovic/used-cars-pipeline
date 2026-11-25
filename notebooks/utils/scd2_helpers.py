# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 helpers – shared by the CDC demo and the incremental file load
# MAGIC
# MAGIC Run after `00_config`, `utils/silver_transformations`, `utils/vehicle_listings_cleaning` and
# MAGIC `utils/gold_helpers`. Used by `10_silver_listing_changes` / `11_gold_cdc_vehicle_history` (changes read from the
# MAGIC log of a database, documentation/cdc.md) and `14_silver_listings_history` / `15_gold_incremental_dim_vehicle`
# MAGIC (changes read from a growing source file with a watermark, documentation/incremental_load.md).
# MAGIC
# MAGIC **Silver:** one table with every version of every listing – the Silver listing columns plus:
# MAGIC - `version_key`: one version, `xxhash64(vehicle_id, change_seq, change_op)`;
# MAGIC - `row_hash`: hash of the cleaned business columns; a new version only when it changes;
# MAGIC - `change_op`: R (snapshot) / I (new) / U (changed) of the change that opened the version;
# MAGIC - `change_seq`: order of the change in the source (WAL position for CDC, record number in the original file
# MAGIC   for the incremental load); `valid_to_seq`: the change that closed the version;
# MAGIC - `valid_from` / `valid_to` / `is_current`: the validity period; the current state is `WHERE is_current`.
# MAGIC
# MAGIC **Gold:** only the current state (mentor's decision): `dim_vehicle` = the current Silver versions, one row per
# MAGIC vehicle, with the same business-ready attributes as the main `dim_vehicle`. The history stays in Silver.

# COMMAND ----------

from pyspark.sql import Window

SNAPSHOT_OP = "R"
INSERT_OP = "I"
UPDATE_OP = "U"
DELETE_OP = "D"

SCD2_LINEAGE_COLUMNS = ["ingestion_date", "source_file_path", "silver_processed_at"]
# Columns whose change makes a new Silver version: the cleaned listing without lineage and DQ summary.
SCD2_BUSINESS_COLUMNS = [
    c for c in VEHICLE_LISTINGS_COLUMNS if c not in SCD2_LINEAGE_COLUMNS + ["dq_issues", "is_valid"]
]
SCD2_TRACKING_COLUMNS = [
    "row_hash", "change_op", "change_seq", "valid_from", "valid_to", "valid_to_seq", "is_current",
]
SCD2_SILVER_COLUMNS = ["version_key", *VEHICLE_LISTINGS_COLUMNS, *SCD2_TRACKING_COLUMNS]
# Columns a cleaned change must have before build_versions.
SCD2_CHANGE_COLUMNS = [*VEHICLE_LISTINGS_COLUMNS, "row_hash", "change_op", "change_seq", "event_ts"]


def content_hash(columns: list) -> Column:
    """SHA-256 of the columns as JSON (a null in one column never looks like a value in another)."""
    return F.sha2(F.to_json(F.struct(*columns)), 256)


# COMMAND ----------

# MAGIC %md ## Silver: versions

# COMMAND ----------


def select_change_partitions(available: list, ingestion_date: str, watermark_date: str = None) -> list:
    """
    Bronze partitions to read: up to the requested date, and from the newest date already in the table
    (that partition is read again: new files may have been added to it after the last run).
    """
    if ingestion_date != "latest":
        assert ingestion_date in available, f"No partition {ingestion_date}. Available: {available}"
    return [
        d for d in available
        if (ingestion_date == "latest" or d <= ingestion_date) and (watermark_date is None or d >= watermark_date)
    ]


def read_table_snapshot(path: str) -> DataFrame:
    """
    The Delta table pinned to its current version. A run reads the table, then writes to it; a plain read is lazy
    and could be evaluated again after the write and see the new rows (e.g. every vehicle would look known).
    """
    version = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").first().version
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def applied_watermark(table_df: DataFrame):
    """
    The highest change already applied (None for an empty table): the newest change that opened a version or
    closed one (a DELETE). Snapshot rows do not count: in the CDC demo the first change after the snapshot can
    have the same WAL position as the snapshot.
    """
    return table_df.select(
        F.greatest(
            F.max(F.when(F.col("change_op") != SNAPSHOT_OP, F.col("change_seq"))), F.max("valid_to_seq")
        ).alias("watermark")
    ).first().watermark


def build_versions(changes_df: DataFrame, open_versions_df: DataFrame = None) -> DataFrame:
    """
    The Silver rows to write for one batch of cleaned changes (`SCD2_CHANGE_COLUMNS`): new versions, and the open
    versions of the table that these changes close (with `valid_to` set). Unchanged rows are not returned.

    Per vehicle, ordered by `change_seq` (snapshot rows first at the same position):
    - an R / I / U opens a version when it is the first known state, follows a DELETE, or changed a business
      column (`row_hash`); a change that only repeats the current values is skipped;
    - a D closes the open version;
    - `valid_to` of a version = the time of the next change that opened or closed a version.
    """
    keep = [*VEHICLE_LISTINGS_COLUMNS, "row_hash", "change_op", "change_seq"]
    events = changes_df.select(
        *keep, "event_ts", F.lit(None).cast("bigint").alias("version_key"), F.lit(1).alias("_origin")
    )
    if open_versions_df is not None:  # None on the first run (no table yet)
        existing = (
            open_versions_df.join(changes_df.select("vehicle_id").distinct(), "vehicle_id", "left_semi")
            .select(*keep, F.col("valid_from").alias("event_ts"), "version_key", F.lit(0).alias("_origin"))
        )
        events = existing.unionByName(events)
    order = Window.partitionBy("vehicle_id").orderBy(
        "change_seq", F.when(F.col("change_op") == SNAPSHOT_OP, 0).otherwise(1), "_origin"
    )
    previous_op = F.lag("change_op").over(order)
    opens_or_closes = (
        F.when(F.col("_origin") == 0, F.lit(True))  # a version that is already in the table
        .when(F.col("change_op") == DELETE_OP, previous_op.isNotNull() & (previous_op != DELETE_OP))
        .otherwise(
            previous_op.isNull() | (previous_op == DELETE_OP) | (F.col("row_hash") != F.lag("row_hash").over(order))
        )
    )
    boundaries = events.withColumn("_boundary", opens_or_closes).filter("_boundary")
    return (
        boundaries
        .withColumn("valid_to", F.lead("event_ts").over(order))
        .withColumn("valid_to_seq", F.lead("change_seq").over(order))
        .filter(F.col("change_op") != DELETE_OP)
        .filter((F.col("_origin") == 1) | F.col("valid_to").isNotNull())  # table rows only when closed now
        .withColumn("valid_from", F.col("event_ts"))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("version_key", F.coalesce("version_key", F.xxhash64("vehicle_id", "change_seq", "change_op")))
        .select(*SCD2_SILVER_COLUMNS)
    )


def apply_versions(versions_df: DataFrame, path: str, table_exists: bool) -> tuple:
    """Write the versions (first run) or MERGE them on version_key; returns (inserted, closed)."""
    if table_exists:
        metrics = merge_delta_table(versions_df, path, ["version_key"])
        return metrics.get("numTargetRowsInserted", 0), metrics.get("numTargetRowsUpdated", 0)
    write_delta_table(versions_df, path, num_files=1)
    return versions_df.count(), 0


def check_scd2_versions(df: DataFrame, table_name: str) -> dict:
    """SCD2 rules of the Silver history; fails on the first broken rule. Returns a few counts for the log."""
    assert_columns(df, SCD2_SILVER_COLUMNS, table_name)
    assert_unique(df, ["version_key"], table_name)
    several_current = df.filter("is_current").groupBy("vehicle_id").count().filter("count > 1").count()
    assert several_current == 0, f"{table_name}: {several_current} vehicles with more than one current version"
    inconsistent = df.filter(
        (F.col("is_current") != F.col("valid_to").isNull()) | (F.col("valid_to") < F.col("valid_from"))
    ).count()
    assert inconsistent == 0, f"{table_name}: {inconsistent} versions with an inconsistent validity period"
    order = Window.partitionBy("vehicle_id").orderBy("valid_from", "change_seq")
    overlapping = (
        df.withColumn("_previous_current", F.lag("is_current").over(order))
        .withColumn("_previous_to", F.lag("valid_to").over(order))
        .filter(F.col("_previous_current") | (F.col("_previous_to") > F.col("valid_from")))
        .count()
    )
    assert overlapping == 0, f"{table_name}: {overlapping} versions overlap the previous version"
    counts = df.agg(
        F.count("*").alias("versions"),
        F.countDistinct("vehicle_id").alias("vehicles"),
        F.sum(F.col("is_current").cast("int")).alias("current_versions"),
    ).first()
    return counts.asDict()


# COMMAND ----------

# MAGIC %md ## Gold: current state only

# COMMAND ----------


def build_current_dim_vehicle(versions_df: DataFrame) -> DataFrame:
    """
    dim_vehicle with only the current state: one row per vehicle that currently exists (`WHERE is_current`), with
    the business-ready attributes of the main dim_vehicle, plus the Unknown member (-1). SCD Type 1 in Gold:
    rebuilt from Silver on every run, the history stays in Silver.
    """
    dim = versions_df.filter("is_current").select(
        surrogate_key("vehicle_id").alias("vehicle_key"),
        "vehicle_id",
        *vehicle_attributes(),
    ).select(*CURRENT_DIM_VEHICLE_COLUMNS)
    return with_unknown_member(
        dim, "vehicle_key", {"vehicle_id": None, "vin": None, "listing_url": None, "manufacturer_source": "unknown"}
    )


def check_current_dim_vehicle(dim_df: DataFrame, versions_df: DataFrame, table_name: str) -> int:
    """Documented columns, one row per current vehicle (+ Unknown), unique keys. Returns the number of vehicles."""
    assert_columns(dim_df, CURRENT_DIM_VEHICLE_COLUMNS, table_name)
    assert_unique(dim_df, ["vehicle_key"], table_name)
    current = versions_df.filter("is_current").count()
    rows = dim_df.filter(F.col("vehicle_key") != UNKNOWN_KEY).count()
    assert rows == current, f"{table_name}: {rows} vehicles, Silver has {current} current versions"
    return rows
