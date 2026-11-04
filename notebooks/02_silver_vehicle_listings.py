# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Silver: vehicle_listings
# MAGIC
# MAGIC Reads the four vehicle listing sources from one Bronze ingestion partition, cleans and standardizes
# MAGIC each of them, combines them into one dataset and overwrites the Delta table stored at
# MAGIC `abfss://silver@sausedcarspipelinesv.dfs.core.windows.net/vehicle_listings` (path-based, see 00_config).
# MAGIC
# MAGIC Data model and rules: `documentation/silver_data_model.md`.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) or a `YYYY-MM-DD` Bronze partition date.
# MAGIC - `dev_mode`: `true` reads only a sample per source and writes to `vehicle_listings_dev`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
DEV_SAMPLE_ROWS = 5000

TABLE_NAME = "vehicle_listings_dev" if DEV_MODE else "vehicle_listings"
TARGET_PATH = f"{SILVER_PATH}/{TABLE_NAME}"

print(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

# The 28 columns every vehicle source must have in Bronze.
RAW_LISTING_COLUMNS = [
    "id", "url", "region", "region_url", "price", "year", "model", "condition", "cylinders", "fuel",
    "odometer", "title_status", "transmission", "VIN", "drive", "size", "type", "paint_color",
    "image_url", "description", "state", "lat", "long", "posting_date", "source_system_id",
    "source_system_name", "currency", "SourceSystemId",
]

# Common raw types, so CSV (all strings) and typed formats (Parquet/Avro/JSON) look the same before cleaning.
RAW_LISTING_TYPES = {
    "price": "double",
    "year": "double",
    "odometer": "double",
    "lat": "double",
    "long": "double",
    "source_system_id": "int",
}

COLUMN_RENAMES = {
    "id": "vehicle_id",
    "url": "listing_url",
    "year": "manufacture_year",
    "VIN": "vin",
    "type": "body_type",
    "lat": "latitude",
    "long": "longitude",
    "posting_date": "posting_date_raw",
    "source_system_id": "platform_id",
    "source_system_name": "platform_name",
    "SourceSystemId": "source_system_uuid",
}

LOWER_CASE_COLUMNS = [
    "region", "condition", "fuel", "title_status", "transmission", "drive", "size", "body_type",
    "paint_color",
]
UPPER_CASE_COLUMNS = ["state", "currency", "vin"]

# One posting_date format per listing platform.
POSTING_DATE_FORMATS = [
    (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),          # CarGurus
    (r"^\d{2}/\d{2}/\d{4}$", "MM/dd/yyyy"),          # Craigslist
    (r"^\d{2}\.\d{2}\.\d{4}$", "dd.MM.yyyy"),        # AutoScout24
    (r"^[A-Za-z]+ \d{1,2}, \d{4}$", "MMMM d, yyyy"),  # CarsGuide, e.g. "April 12, 2021"
]

VALID_CURRENCIES = ["USD", "GBP", "EUR", "AUD"]
MAX_PRICE = 1_000_000
MIN_MANUFACTURE_YEAR = 1900
MAX_ODOMETER = 1_000_000
VIN_PATTERN = r"^[A-HJ-NPR-Z0-9]{17}$"
STATE_PATTERN = r"^[A-Z]{2}$"

# Final column order of silver.vehicle_listings (must match the documentation).
VEHICLE_LISTINGS_COLUMNS = [
    "vehicle_id", "source_system", "source_system_uuid", "platform_id", "platform_name",
    "listing_url", "region", "region_url", "state", "latitude", "longitude",
    "price", "currency", "manufacture_year", "model", "model_normalized",
    "condition", "cylinders", "fuel", "odometer", "title_status", "transmission", "vin",
    "drive", "size", "body_type", "paint_color", "image_url", "description",
    "posting_date", "posting_date_raw",
    "ingestion_date", "source_file_path", "silver_processed_at", "dq_issues", "is_valid",
]

# COMMAND ----------

# MAGIC %md ## Cleaning function (applied to each source)

# COMMAND ----------


def clean_vehicle_listings(raw_df: DataFrame, source_system: str, ingestion_date: str) -> DataFrame:
    """Turn one raw Bronze vehicle source into the standardized Silver shape (before the union)."""
    missing = sorted(set(RAW_LISTING_COLUMNS) - set(raw_df.columns))
    assert not missing, f"{source_system}: missing Bronze columns {missing}"

    df = raw_df.select(*RAW_LISTING_COLUMNS, "source_file_path")

    # 1. One representation of "missing" + common raw types.
    df = try_cast_columns(df, RAW_LISTING_TYPES)
    df = standardize_nulls(df)

    # 2. snake_case / descriptive column names.
    df = rename_columns(df, COLUMN_RENAMES)
    df = df.withColumn("source_system", F.lit(source_system))

    # 3. Consistent case for categories and codes.
    df = normalize_case(df, LOWER_CASE_COLUMNS, "lower")
    df = normalize_case(df, UPPER_CASE_COLUMNS, "upper")

    # 4. Derived / reshaped columns.
    df = (
        df.withColumn("posting_date", parse_multi_format_date(F.col("posting_date_raw"), POSTING_DATE_FORMATS))
        .withColumn("model", F.regexp_replace("model", r"\s+", " "))
        .withColumn("model_normalized", F.lower("model"))
        # "6 cylinders" -> "6"; anything else ("other") -> "" which try_cast turns into null in step 6.
        .withColumn("cylinders", F.regexp_extract(F.lower("cylinders"), r"^(\d+) cylinders$", 1))
    )

    # 5. Validation: invalid values -> null + DQ issue (rows are kept).
    reference_year = F.coalesce(F.year("posting_date"), F.year(F.current_date()))

    df = add_issue_flag(df, F.col("posting_date_raw").isNotNull() & F.col("posting_date").isNull(), "unparsed_posting_date")
    df = flag_and_nullify(df, "price", (F.col("price") <= 0) | (F.col("price") > MAX_PRICE), "invalid_price")
    df = flag_and_nullify(df, "currency", ~F.col("currency").isin(VALID_CURRENCIES), "invalid_currency")
    df = flag_and_nullify(
        df,
        "manufacture_year",
        (F.col("manufacture_year") < MIN_MANUFACTURE_YEAR) | (F.col("manufacture_year") > reference_year + 1),
        "invalid_manufacture_year",
    )
    df = flag_and_nullify(df, "odometer", (F.col("odometer") < 0) | (F.col("odometer") > MAX_ODOMETER), "invalid_odometer")
    df = add_issue_flag(df, F.col("odometer") == 0, "zero_odometer")
    df = flag_and_nullify(df, "latitude", ~F.col("latitude").between(-90, 90), "invalid_latitude")
    df = flag_and_nullify(df, "longitude", ~F.col("longitude").between(-180, 180), "invalid_longitude")
    df = flag_and_nullify(df, "vin", ~F.col("vin").rlike(VIN_PATTERN), "invalid_vin")
    df = flag_and_nullify(df, "state", ~F.col("state").rlike(STATE_PATTERN), "invalid_state")

    # 6. Final types (after validation, so out-of-range doubles never overflow the target types).
    df = try_cast_columns(
        df,
        {
            "price": "decimal(18,2)",
            "manufacture_year": "int",
            "odometer": "int",
            "platform_id": "int",
            "cylinders": "int",
        },
    )

    # 7. DQ summary, lineage, deduplication.
    df = collect_dq_issues(df)
    df = add_lineage_columns(df, ingestion_date)
    df = deduplicate(df, ["vehicle_id"], [F.col("posting_date").desc_nulls_last()])

    return df.select(*VEHICLE_LISTINGS_COLUMNS)


# COMMAND ----------

# MAGIC %md ## Read and clean each source

# COMMAND ----------

cleaned_sources = {}
source_row_counts = []

for source_system, source in VEHICLE_SOURCES.items():
    partition_path, partition_date = get_ingestion_path(source["folder"], INGESTION_DATE)
    raw_df = read_bronze_files(partition_path, source["format"])
    if DEV_MODE:
        raw_df = raw_df.limit(DEV_SAMPLE_ROWS)

    cleaned_df = clean_vehicle_listings(raw_df, source_system, partition_date).cache()
    raw_count = raw_df.count()
    cleaned_count = cleaned_df.count()

    cleaned_sources[source_system] = cleaned_df
    source_row_counts.append((source_system, partition_date, raw_count, cleaned_count, raw_count - cleaned_count))
    print(f"{source_system}: {partition_path} -> {raw_count} raw rows, {cleaned_count} after dedup")

display(
    spark.createDataFrame(
        source_row_counts,
        "source_system STRING, ingestion_date STRING, raw_rows LONG, silver_rows LONG, duplicates_removed LONG",
    )
)

# COMMAND ----------

# MAGIC %md ## Combine the four sources into one dataset

# COMMAND ----------

vehicle_listings_df = reduce(DataFrame.unionByName, cleaned_sources.values())

expected_rows = sum(row[3] for row in source_row_counts)
actual_rows = vehicle_listings_df.count()
assert actual_rows == expected_rows, f"Union row count {actual_rows} != sum of sources {expected_rows}"

# vehicle_id must be unique across all sources (profiling showed no overlap between files).
# If this fails, the key must become (source_system, vehicle_id) – see documentation.
assert_unique(vehicle_listings_df, ["vehicle_id"], "vehicle_listings")
assert_columns(vehicle_listings_df, VEHICLE_LISTINGS_COLUMNS, "vehicle_listings")

# Unparsed dates do not stop the pipeline: the rows are kept and flagged (Silver rule).
# A warning with examples makes a new date format or bad source values visible.
unparsed_df = vehicle_listings_df.filter(F.array_contains("dq_issues", "unparsed_posting_date"))
unparsed_dates = unparsed_df.count()
if unparsed_dates > 0:
    print(
        f"WARNING: {unparsed_dates} posting_date values could not be parsed (flagged 'unparsed_posting_date'). "
        "Check whether a new date format appeared in POSTING_DATE_FORMATS."
    )
    display(
        unparsed_df.groupBy("platform_name", "posting_date_raw").count().orderBy(F.desc("count")).limit(20)
    )

print(f"vehicle_listings: {actual_rows} rows, checks passed ({unparsed_dates} unparsed posting_date values)")

# COMMAND ----------

# MAGIC %md ## Write to Silver (Delta, full overwrite)

# COMMAND ----------

write_delta_table(vehicle_listings_df, TARGET_PATH)

for cleaned_df in cleaned_sources.values():
    cleaned_df.unpersist()

silver_df = read_delta_table(TARGET_PATH)
print(f"Written {silver_df.count()} rows to {TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Data quality report

# COMMAND ----------

display(
    silver_df.groupBy("source_system")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("is_valid").cast("int")).alias("valid_rows"),
        F.round(100 * F.avg(F.col("is_valid").cast("int")), 2).alias("valid_pct"),
    )
    .orderBy("source_system")
)

# COMMAND ----------

display(dq_issue_counts(silver_df))

# COMMAND ----------

display(null_percentages(silver_df))

# COMMAND ----------

display(silver_df.groupBy("source_system", "currency").count().orderBy("source_system", "currency"))

# COMMAND ----------

# Spot check: raw vs parsed posting_date per platform.
display(
    silver_df.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("platform_name").orderBy("vehicle_id"))
    )
    .filter("_rn <= 5")
    .select("platform_name", "posting_date_raw", "posting_date")
)
