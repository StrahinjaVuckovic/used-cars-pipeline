# Databricks notebook source
# MAGIC %md
# MAGIC # 04 – Silver: service_events
# MAGIC
# MAGIC Reads ServiceBook from one Bronze ingestion partition, cleans and standardizes it and overwrites
# MAGIC the Delta table stored at `abfss://silver@sausedcarspipelinesv.dfs.core.windows.net/service_events`
# MAGIC (path-based, see 00_config). This table is the basis for the Reliability Score in Gold.
# MAGIC
# MAGIC Data model and rules: `documentation/silver_data_model.md`.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) or a `YYYY-MM-DD` Bronze partition date.
# MAGIC - `dev_mode`: `true` reads only a sample and writes to `service_events_dev`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
DEV_SAMPLE_ROWS = 50000

TABLE_NAME = "service_events_dev" if DEV_MODE else "service_events"
TARGET_PATH = f"{SILVER_PATH}/{TABLE_NAME}"

log = get_logger("04_silver_service_events")
log.info(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

RAW_SERVICE_COLUMNS = [
    "car_id", "service_date", "odometer_km", "event_type", "maintenance_kind", "severity", "root_cause",
    "warranty_flag", "is_maintenance", "is_repair", "is_inspection", "is_tyre", "is_oil_change",
    "interval_km_target", "prev_odometer_km", "km_since_prev", "odometer_monotonic_flag", "oil_gap_km",
    "oil_gap_penalty_piece", "cost_eur", "downtime_days", "service_type_raw", "notes_raw", "desc_all",
]

# Columns with a single value in the whole source (no information). Dropped, but re-checked on every run.
CONSTANT_COLUMNS = ["root_cause", "warranty_flag", "is_tyre"]
# Derivable column: lower(service_type_raw + notes_raw).
DERIVED_COLUMNS = ["desc_all"]

COLUMN_RENAMES = {
    "car_id": "vehicle_id",
    "service_date": "service_date_raw",
    "odometer_km": "odometer_km_raw",
    "service_type_raw": "service_type_source",
    "odometer_monotonic_flag": "is_odometer_monotonic",
    "oil_gap_penalty_piece": "oil_gap_penalty",
    "notes_raw": "notes",
}

FLAG_COLUMNS = ["is_maintenance", "is_repair", "is_inspection", "is_oil_change", "is_odometer_monotonic"]
UPPER_CASE_COLUMNS = ["event_type", "maintenance_kind", "severity"]

MIN_SERVICE_DATE = "1950-01-01"
MAX_ODOMETER_KM = 2_000_000

SERVICE_EVENTS_COLUMNS = [
    "service_event_id", "vehicle_id", "service_date", "odometer_km", "event_type", "service_type",
    "maintenance_kind", "severity", "is_maintenance", "is_repair", "is_inspection", "is_oil_change",
    "interval_km_target", "prev_odometer_km", "km_since_prev", "is_odometer_monotonic", "oil_gap_km",
    "oil_gap_penalty", "cost_eur", "downtime_days", "notes", "is_skipped_service",
    "ingestion_date", "source_file_path", "silver_processed_at", "dq_issues", "is_valid",
]

# COMMAND ----------

# MAGIC %md ## Read Bronze

# COMMAND ----------

partition_path, partition_date = get_ingestion_path(SERVICE_BOOK_SOURCE["folder"], INGESTION_DATE)
raw_df = read_bronze_files(partition_path, SERVICE_BOOK_SOURCE["format"])
if DEV_MODE:
    raw_df = raw_df.limit(DEV_SAMPLE_ROWS)

missing = sorted(set(RAW_SERVICE_COLUMNS) - set(raw_df.columns))
assert not missing, f"ServiceBook: missing Bronze columns {missing}"

raw_df = standardize_nulls(raw_df.select(*RAW_SERVICE_COLUMNS, "source_file_path")).cache()
raw_count = raw_df.count()
log.info(f"ServiceBook: {partition_path} -> {raw_count} raw rows")

# COMMAND ----------

# MAGIC %md ## Re-check dropped constant columns

# COMMAND ----------

distinct_counts = raw_df.select(
    *[F.countDistinct(c).alias(c) for c in CONSTANT_COLUMNS]
).first().asDict()
log.info(f"Distinct values in dropped columns: {distinct_counts}")
for column_name, distinct_count in distinct_counts.items():
    if distinct_count > 1:
        log.warning(f"{column_name} now has {distinct_count} distinct values – reconsider dropping it.")

# COMMAND ----------

# MAGIC %md ## Clean and standardize

# COMMAND ----------

# Surrogate key from ALL raw business columns, so it stays stable even when validation nulls a value.
# Profiling showed 117 pairs of REPAIR events with the same car, date, odometer and event type that differ in
# cost_eur, downtime_days and prev_odometer_km: they are two real repairs on the same day, not duplicates.
# Hashing every column keeps both; only rows that are identical in every column share a key.
df = raw_df.withColumn(
    "service_event_id",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in RAW_SERVICE_COLUMNS]), 256),
)

df = df.drop(*CONSTANT_COLUMNS, *DERIVED_COLUMNS)
df = rename_columns(df, COLUMN_RENAMES)

# Types: numbers are stored as "10026.0" in the CSV, so cast to double first, then to the target type.
df = try_cast_columns(
    df,
    {
        "odometer_km_raw": "double",
        "interval_km_target": "double",
        "prev_odometer_km": "double",
        "km_since_prev": "double",
        "oil_gap_km": "double",
        "oil_gap_penalty": "double",
        "cost_eur": "decimal(10,2)",
        "downtime_days": "double",
    },
)
df = (
    df.withColumn("service_date", F.expr("try_cast(service_date_raw AS date)"))
    .withColumn("odometer_km", F.col("odometer_km_raw"))
    .withColumn(
        "service_type",
        F.upper(F.regexp_replace("service_type_source", r"\s+", "_")),  # "Oil Change" -> "OIL_CHANGE"
    )
    .withColumn("is_skipped_service", F.coalesce(F.lower("notes") == "skipped", F.lit(False)))
)
for flag_column in FLAG_COLUMNS:
    df = df.withColumn(flag_column, F.expr(f"try_cast({flag_column} AS int)") == 1)

df = normalize_case(df, UPPER_CASE_COLUMNS, "upper")

# Validation: invalid values -> null + DQ issue (rows are kept).
# "In the future" means after the Bronze ingestion date, never after the run date: the same Bronze input must
# give the same Silver output on any day (idempotent re-runs and backfills). A service cannot happen after
# the file that records it was delivered.
df = add_issue_flag(df, F.col("service_date_raw").isNotNull() & F.col("service_date").isNull(), "unparsed_service_date")
df = flag_and_nullify(
    df,
    "service_date",
    (F.col("service_date") < F.lit(MIN_SERVICE_DATE).cast("date")) | (F.col("service_date") > F.to_date(F.lit(partition_date))),
    "invalid_service_date",
)
df = flag_and_nullify(
    df, "odometer_km", (F.col("odometer_km") < 0) | (F.col("odometer_km") > MAX_ODOMETER_KM), "invalid_odometer_km"
)
df = add_issue_flag(df, ~F.col("is_odometer_monotonic"), "non_monotonic_odometer")

df = try_cast_columns(
    df,
    {
        "odometer_km": "int",
        "interval_km_target": "int",
        "prev_odometer_km": "int",
        "km_since_prev": "int",
        "oil_gap_km": "int",
        "downtime_days": "int",
    },
)

df = collect_dq_issues(df)
df = add_lineage_columns(df, partition_date)

# Only rows that are identical in every source column share a key; keep one of them.
df = df.dropDuplicates(["service_event_id"])

service_events_df = df.select(*SERVICE_EVENTS_COLUMNS)

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

silver_count = service_events_df.count()
log.info(f"service_events: {raw_count} raw rows -> {silver_count} rows ({raw_count - silver_count} duplicates removed)")

assert_unique(service_events_df, ["service_event_id"], "service_events")
assert_columns(service_events_df, SERVICE_EVENTS_COLUMNS, "service_events")

# Unparsed dates do not stop the pipeline: the rows are kept and flagged (Silver rule).
# `df` is used (not service_events_df) because it still has the original service_date_raw column.
unparsed_df = df.filter(F.array_contains("dq_issues", "unparsed_service_date"))
unparsed_dates = unparsed_df.count()
if unparsed_dates > 0:
    log.warning(
        f"{unparsed_dates} service_date values could not be parsed (flagged 'unparsed_service_date')."
    )
    display(unparsed_df.groupBy("service_date_raw").count().orderBy(F.desc("count")).limit(20))

# COMMAND ----------

# MAGIC %md ## Write to Silver (Delta, full overwrite)

# COMMAND ----------

write_delta_table(service_events_df, TARGET_PATH)
raw_df.unpersist()

silver_df = read_delta_table(TARGET_PATH)
log.info(f"Written {silver_df.count()} rows to {TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Data quality report

# COMMAND ----------

display(
    silver_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("vehicle_id").alias("vehicles"),
        F.round(100 * F.avg(F.col("is_valid").cast("int")), 2).alias("valid_pct"),
    )
)

# COMMAND ----------

display(dq_issue_counts(silver_df))

# COMMAND ----------

display(null_percentages(silver_df))

# COMMAND ----------

display(silver_df.groupBy("event_type", "service_type", "severity").count().orderBy(F.desc("count")))

# COMMAND ----------

# Coverage: how many service-book vehicles exist in silver.vehicle_listings (relationship is not enforced).
listings_path = f"{SILVER_PATH}/{'vehicle_listings_dev' if DEV_MODE else 'vehicle_listings'}"
if delta_table_exists(listings_path):
    service_vehicles = silver_df.select("vehicle_id").distinct()
    listing_vehicles = read_delta_table(listings_path).select("vehicle_id").distinct()
    matched = service_vehicles.join(listing_vehicles, "vehicle_id", "inner").count()
    total = service_vehicles.count()
    log.info(f"{matched} of {total} service-book vehicles ({100 * matched / max(total, 1):.2f}%) exist in {listings_path}")
else:
    log.info(f"No Delta table at {listings_path} yet – run 03_silver_vehicle_listings first to see coverage.")
