# Databricks notebook source
# MAGIC %md
# MAGIC # CDC demo helpers – log-based change data capture with SCD Type 2 history
# MAGIC
# MAGIC Shared by `10_silver_listing_changes` and `11_gold_cdc_vehicle_history` (documentation/cdc.md). Run after
# MAGIC `00_config`, `utils/silver_transformations`, `utils/vehicle_listings_cleaning` and `utils/gold_helpers`.
# MAGIC
# MAGIC The input are change events that `cdc/cdc_reader.py` read from the WAL of the source PostgreSQL database
# MAGIC (`bronze/ListingChanges/ingestion_date=…/*.json`): `op` R (initial snapshot) / I / U / D, the WAL position
# MAGIC `lsn_num` (orders the changes), the commit time `commit_ts` and the raw row `before` / `after` the change.
# MAGIC
# MAGIC **Silver** (`vehicle_listings_cdc`): one table with every version of every listing – the Silver listing
# MAGIC columns (cleaned by the same `clean_vehicle_listings` as the files) plus `valid_from`, `valid_to` and
# MAGIC `is_current`. The current state is `WHERE is_current`. A change opens a new version only when a cleaned
# MAGIC business column changed (`row_hash`); a DELETE closes the version without opening a new one.
# MAGIC
# MAGIC **Gold** (`gold/cdc/`): `dim_vehicle` with SCD2 columns (a new version only when a dimension attribute
# MAGIC changes, not for a new price) and `fact_listing_event` (one row per change, linked to the vehicle version
# MAGIC that was valid at that moment).

# COMMAND ----------

from pyspark.sql import Window

CDC_SOURCE_SYSTEM = "listings_db"      # `source_system` of the demo rows (the platform's database)
CHANGES_FOLDER = "ListingChanges"      # bronze/ListingChanges/ingestion_date=YYYY-MM-DD/
SNAPSHOT_OP = "R"
DELETE_OP = "D"

# Types of the raw columns in the change events (the PostgreSQL column types); the rest are strings.
CDC_RAW_TYPES = {
    "price": "DOUBLE", "year": "DOUBLE", "odometer": "DOUBLE", "lat": "DOUBLE", "long": "DOUBLE",
    "source_system_id": "BIGINT",
}
_ROW_IMAGE = ", ".join(f"`{c}`: {CDC_RAW_TYPES.get(c, 'STRING')}" for c in RAW_LISTING_COLUMNS)
CHANGE_EVENT_SCHEMA = (
    f"op STRING, lsn STRING, lsn_num BIGINT, commit_ts STRING, `table` STRING, "
    f"before STRUCT<{_ROW_IMAGE}>, after STRUCT<{_ROW_IMAGE}>"
)

CDC_LINEAGE_COLUMNS = ["ingestion_date", "source_file_path", "silver_processed_at"]
# Columns whose change makes a new Silver version: the cleaned listing without lineage and DQ summary.
CDC_BUSINESS_COLUMNS = [
    c for c in VEHICLE_LISTINGS_COLUMNS if c not in CDC_LINEAGE_COLUMNS + ["dq_issues", "is_valid"]
]
CDC_TRACKING_COLUMNS = ["row_hash", "change_op", "lsn_num", "valid_from", "valid_to", "valid_to_lsn", "is_current"]
CDC_SILVER_COLUMNS = ["version_key", *VEHICLE_LISTINGS_COLUMNS, *CDC_TRACKING_COLUMNS]

# Gold dimension columns that are not attributes of the vehicle (keys and SCD2 columns).
_DIM_TECHNICAL_COLUMNS = [
    "vehicle_version_key", "vehicle_key", "vehicle_id", "valid_from", "valid_to", "is_current", "is_deleted",
    "version_number",
]
CDC_DIM_ATTRIBUTES = [c for c in CDC_GOLD_COLUMNS["dim_vehicle"] if c not in _DIM_TECHNICAL_COLUMNS]


def cdc_silver_path(dev_mode: bool) -> str:
    return f"{SILVER_PATH}/vehicle_listings_cdc{'_dev' if dev_mode else ''}"


def cdc_gold_path(table_name: str, dev_mode: bool) -> str:
    return f"{GOLD_PATH}/cdc/{table_name}{'_dev' if dev_mode else ''}"


def content_hash(columns: list) -> Column:
    """SHA-256 of the columns as JSON (a null in one column never looks like a value in another)."""
    return F.sha2(F.to_json(F.struct(*columns)), 256)


# COMMAND ----------

# MAGIC %md ## Silver: read, filter and clean the change events

# COMMAND ----------


def select_change_partitions(available: list, ingestion_date: str, watermark_date: str = None) -> list:
    """
    Bronze partitions to read: up to the requested date, and from the newest date already in the table
    (that partition is read again: the reader may have added files to it after the last run).
    """
    if ingestion_date != "latest":
        assert ingestion_date in available, f"No ListingChanges partition {ingestion_date}. Available: {available}"
    return [
        d for d in available
        if (ingestion_date == "latest" or d <= ingestion_date) and (watermark_date is None or d >= watermark_date)
    ]


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


def change_watermark(table_df: DataFrame):
    """
    The highest WAL position already applied (None for an empty table): the newest change that opened a
    version, or that closed one (a DELETE). Snapshot rows do not count: the first change after the snapshot
    can have the same LSN as the snapshot.
    """
    return table_df.select(
        F.greatest(
            F.max(F.when(F.col("change_op") != SNAPSHOT_OP, F.col("lsn_num"))), F.max("valid_to_lsn")
        ).alias("watermark")
    ).first().watermark


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
            content_hash(CDC_BUSINESS_COLUMNS).alias("row_hash"),
            F.col("_cdc.op").alias("change_op"),
            F.col("_cdc.lsn_num").alias("lsn_num"),
            F.col("_cdc.event_ts").alias("event_ts"),
        )
    )


# COMMAND ----------

# MAGIC %md ## Silver: SCD Type 2 versions

# COMMAND ----------


def build_versions(changes_df: DataFrame, open_versions_df: DataFrame = None) -> DataFrame:
    """
    The Silver rows to write for one batch of cleaned changes: new versions, and the open versions of the
    table that these changes close (with `valid_to` set). Unchanged rows of the table are not returned.

    Per vehicle, ordered by WAL position (snapshot rows first at the same position):
    - an R / I / U opens a version when it is the first known state, follows a DELETE, or changed a business
      column (`row_hash`); a change that only repeats the current values is skipped;
    - a D closes the open version;
    - `valid_to` of a version = the time of the next change that opened or closed a version.
    """
    keep = [*VEHICLE_LISTINGS_COLUMNS, "row_hash", "change_op", "lsn_num"]
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
        "lsn_num", F.when(F.col("change_op") == SNAPSHOT_OP, 0).otherwise(1), "_origin"
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
        .withColumn("valid_to_lsn", F.lead("lsn_num").over(order))
        .filter(F.col("change_op") != DELETE_OP)
        .filter((F.col("_origin") == 1) | F.col("valid_to").isNotNull())  # table rows only when closed now
        .withColumn("valid_from", F.col("event_ts"))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("version_key", F.coalesce("version_key", F.xxhash64("vehicle_id", "lsn_num", "change_op")))
        .select(*CDC_SILVER_COLUMNS)
    )


def check_cdc_versions(df: DataFrame, table_name: str) -> dict:
    """SCD2 rules of the Silver history; fails on the first broken rule. Returns a few counts for the log."""
    assert_columns(df, CDC_SILVER_COLUMNS, table_name)
    assert_unique(df, ["version_key"], table_name)
    several_current = df.filter("is_current").groupBy("vehicle_id").count().filter("count > 1").count()
    assert several_current == 0, f"{table_name}: {several_current} vehicles with more than one current version"
    inconsistent = df.filter(
        (F.col("is_current") != F.col("valid_to").isNull()) | (F.col("valid_to") < F.col("valid_from"))
    ).count()
    assert inconsistent == 0, f"{table_name}: {inconsistent} versions with an inconsistent validity period"
    order = Window.partitionBy("vehicle_id").orderBy("valid_from", "lsn_num")
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

# MAGIC %md ## Gold: SCD2 dimension and change facts

# COMMAND ----------


def silver_version_order() -> list:
    """Order of the Silver versions of one vehicle: time, then WAL position, a snapshot row before a change at the
    same position. The WAL position also orders changes with the same commit time (one transaction)."""
    return ["valid_from", "lsn_num", F.when(F.col("change_op") == SNAPSHOT_OP, 0).otherwise(1)]


def assign_dimension_versions(versions_df: DataFrame) -> DataFrame:
    """
    Every Silver version with the Gold dimension version it belongs to. Silver versions that follow each other
    with the same dimension attributes form one Gold version (a new price is a fact, not a new version of the
    vehicle). The Gold version key comes from the change that started it: xxhash64(vehicle_id, lsn_num,
    change_op). Every change has its own WAL position, so two versions never share a key, not even when one
    transaction changed the vehicle twice at the same commit time (a key from valid_from would collide there).
    """
    order = Window.partitionBy("vehicle_id").orderBy(*silver_version_order())
    attribute_hash = F.col("_attribute_hash")
    starts_version = F.coalesce(
        F.lag(attribute_hash).over(order).isNull()
        | (F.lag(attribute_hash).over(order) != attribute_hash)
        | (F.lag("valid_to").over(order) != F.col("valid_from")),  # gap: sold, then listed again
        F.lit(True),
    )
    first_change = F.min_by(F.struct("lsn_num", "change_op"), "_order").over(
        Window.partitionBy("vehicle_id", "_group")
    )
    return (
        versions_df.select("version_key", "vehicle_id", "valid_from", "valid_to", "lsn_num", "change_op",
                           *vehicle_attributes())
        .withColumn("_attribute_hash", content_hash(CDC_DIM_ATTRIBUTES))
        .withColumn("_order", F.struct(*[c if isinstance(c, Column) else F.col(c) for c in silver_version_order()]))
        .withColumn("_group", F.sum(starts_version.cast("int")).over(order))
        .withColumn(
            "vehicle_version_key",
            surrogate_key(F.col("vehicle_id"), first_change["lsn_num"], first_change["change_op"]),
        )
    )


def build_cdc_dim_vehicle(versions_df: DataFrame) -> DataFrame:
    """dim_vehicle with SCD Type 2 columns from the Silver history, plus the Unknown member (both keys -1)."""
    merged = (
        assign_dimension_versions(versions_df)
        .groupBy("vehicle_id", "vehicle_version_key")
        .agg(
            F.min("_order").alias("_first"),
            F.min("valid_from").alias("valid_from"),
            F.max_by("valid_to", "_order").alias("valid_to"),
            *[F.first(c).alias(c) for c in CDC_DIM_ATTRIBUTES],
        )
    )
    version_order = Window.partitionBy("vehicle_id").orderBy("_first")
    is_last = F.lead("_first").over(version_order).isNull()
    dim = (
        merged
        .withColumn("version_number", F.row_number().over(version_order))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("is_deleted", is_last & F.col("valid_to").isNotNull())  # last version closed = sold
        .withColumn("vehicle_key", surrogate_key("vehicle_id"))
        .select(*CDC_GOLD_COLUMNS["dim_vehicle"])
    )
    return with_unknown_member(
        dim, "vehicle_version_key",
        {"vehicle_key": UNKNOWN_KEY, "vehicle_id": None, "vin": None, "listing_url": None,
         "manufacturer_source": "unknown", "is_current": True, "is_deleted": False, "version_number": 1},
    )


def listing_events(versions_df: DataFrame) -> DataFrame:
    """
    One row per change from the Silver history: every version is a Snapshot / Listed / Price Change /
    Updated event at its `valid_from`; a version closed by a DELETE (no version starts at its `valid_to_lsn`)
    adds a Sold event at its `valid_to`. `version_key` is the Silver version the event opened or closed.
    """
    order = Window.partitionBy("vehicle_id").orderBy(*silver_version_order())
    versions = (
        versions_df.select("version_key", "vehicle_id", "valid_from", "valid_to", "valid_to_lsn", "lsn_num",
                           "change_op", "price", "currency", "odometer")
        .withColumn("_previous_price", F.lag("price").over(order))
        .withColumn("_next_lsn", F.lead("lsn_num").over(order))
    )
    first_state = F.col("change_op").isin(SNAPSHOT_OP, "I")
    opened = versions.select(
        "version_key",
        "vehicle_id",
        F.col("valid_from").alias("event_ts"),
        "lsn_num",
        F.when(F.col("change_op") == SNAPSHOT_OP, "Snapshot")
        .when(F.col("change_op") == "I", "Listed")
        .when(~F.col("price").eqNullSafe(F.col("_previous_price")), "Price Change")
        .otherwise("Updated").alias("event_type"),
        "price",
        F.when(~first_state, F.col("_previous_price")).alias("previous_price"),
        "currency",
        "odometer",
    )
    sold = versions.filter(
        F.col("valid_to").isNotNull() & (F.col("_next_lsn").isNull() | (F.col("_next_lsn") != F.col("valid_to_lsn")))
    ).select(
        "version_key",
        "vehicle_id",
        F.col("valid_to").alias("event_ts"),
        F.col("valid_to_lsn").alias("lsn_num"),
        F.lit("Sold").alias("event_type"),
        "price",
        F.lit(None).cast(versions.schema["price"].dataType).alias("previous_price"),
        "currency",
        "odometer",
    )
    return opened.unionByName(sold).withColumn("price_change", F.col("price") - F.col("previous_price"))


def build_fact_listing_event(versions_df: DataFrame) -> DataFrame:
    """
    fact_listing_event: every change event with the vehicle version that was valid at that moment. The link
    goes through the Silver version the event opened (or, for Sold, closed), so it is exact even when two
    changes share a commit time; it is the same version a point-in-time join (valid_from <= event time <
    valid_to) would find.
    """
    version_of = assign_dimension_versions(versions_df).select("version_key", "vehicle_version_key")
    return listing_events(versions_df).join(version_of, "version_key", "left").select(
        surrogate_key("vehicle_id", "lsn_num", "event_type").alias("event_key"),
        surrogate_key("vehicle_id").alias("vehicle_key"),
        F.coalesce("vehicle_version_key", F.lit(UNKNOWN_KEY).cast("bigint")).alias("vehicle_version_key"),
        date_key(F.to_date("event_ts")).alias("event_date_key"),
        "event_ts",
        "event_type",
        "lsn_num",
        "price",
        "previous_price",
        "price_change",
        "currency",
        "odometer",
    )
