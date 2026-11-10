# Databricks notebook source
# MAGIC %md
# MAGIC # Vehicle listings – shared cleaning code
# MAGIC
# MAGIC Run from the vehicle Silver notebooks (`02_silver_stage_<source>_source`, `02_silver_brand_lookup`, `03`)
# MAGIC after `00_config` and
# MAGIC `utils/silver_transformations` (`%run` paths are relative to the calling notebook):
# MAGIC
# MAGIC ```
# MAGIC %run ./00_config
# MAGIC %run ./utils/silver_transformations
# MAGIC %run ./utils/vehicle_listings_cleaning
# MAGIC ```
# MAGIC
# MAGIC Every vehicle source is loaded by its own notebook (its own pipeline task), because in real life sources
# MAGIC change independently. The rules that are the same for all of them live here, so they are written once:
# MAGIC the 28 raw columns, the cleaning and validation of one listing, the checks before writing and the
# MAGIC per-source stage table. Source-specific settings (reader options, expected currencies) stay in the
# MAGIC source notebook and in `VEHICLE_SOURCES` (00_config).
# MAGIC
# MAGIC Listing platforms are mixed inside every file, so the four `posting_date` formats apply to every source.

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
# Every real listing id is 12 lower-case hex characters. Anything else means the row was parsed wrongly.
VEHICLE_ID_PATTERN = r"^[0-9a-f]{12}$"
STATE_PATTERN = r"^[A-Z]{2}$"

# Column order of one cleaned source (stage table). The brand columns are added after the union
# (03_silver_vehicle_listings), because the brand mapping needs the lookup, not the source.
STAGE_COLUMNS = [
    "vehicle_id", "source_system", "source_system_uuid", "platform_id", "platform_name",
    "listing_url", "region", "region_url", "state", "latitude", "longitude",
    "price", "currency", "manufacture_year", "model", "model_normalized",
    "condition", "cylinders", "fuel", "odometer", "title_status", "transmission", "vin",
    "drive", "size", "body_type", "paint_color", "image_url", "description",
    "posting_date", "posting_date_raw",
    "ingestion_date", "source_file_path", "silver_processed_at", "dq_issues", "is_valid",
]

# Rows read per source with dev_mode=true.
DEV_SAMPLE_ROWS = 5000
# One cleaned source is ~200 MB, so one file per stage table is a good size (see silver_data_model.md).
STAGE_OUTPUT_FILES = 1

# COMMAND ----------

# MAGIC %md ## Cleaning one source

# COMMAND ----------


def normalize_model(column: Column) -> Column:
    """Normalization shared by listings and the brand lookup: trim, single spaces, lower case."""
    return F.lower(F.regexp_replace(F.trim(column), r"\s+", " "))


def clean_vehicle_listings(raw_df: DataFrame, source_system: str, ingestion_date: str) -> DataFrame:
    """Turn one raw Bronze vehicle source into the standardized Silver shape (STAGE_COLUMNS)."""
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
        .withColumn("model_normalized", normalize_model(F.col("model")))
        # "6 cylinders" -> "6"; anything else ("other") -> "" which try_cast turns into null in step 6.
        .withColumn("cylinders", F.regexp_extract(F.lower("cylinders"), r"^(\d+) cylinders$", 1))
    )

    # 5. Validation: invalid values -> null + DQ issue (rows are kept).
    # Without a posting date the reference is the ingestion date, never the run date: the same Bronze input
    # must give the same Silver output on any day (idempotent re-runs and backfills).
    reference_year = F.coalesce(F.year("posting_date"), F.year(F.to_date(F.lit(ingestion_date))))

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

    return df.select(*STAGE_COLUMNS)


# COMMAND ----------

# MAGIC %md ## Reading, checks and stage tables

# COMMAND ----------


def read_vehicle_source(source_system: str, ingestion_date: str, dev_mode: bool) -> tuple:
    """(raw DataFrame, partition path, partition date) of one vehicle source; a sample in dev mode."""
    source = VEHICLE_SOURCES[source_system]
    partition_path, partition_date = get_ingestion_path(source["folder"], ingestion_date)
    raw_df = read_bronze_files(partition_path, source["format"], source.get("options"))
    if dev_mode:
        raw_df = raw_df.limit(DEV_SAMPLE_ROWS)
    return raw_df, partition_path, partition_date


def stage_table_path(source_system: str, dev_mode: bool) -> str:
    """Silver path of the cleaned table of one source, e.g. .../vehicle_listings_stage/first."""
    folder = "vehicle_listings_stage_dev" if dev_mode else "vehicle_listings_stage"
    return f"{SILVER_PATH}/{folder}/{source_system.lower()}"


def brand_lookup_path(dev_mode: bool) -> str:
    """Silver path of the cleaned brand lookup (written by 02_silver_brand_lookup, read by 03)."""
    return f"{SILVER_PATH}/{'brand_lookup_dev' if dev_mode else 'brand_lookup'}"


def check_vehicle_listings(df: DataFrame, expected_columns: list, table_name: str) -> int:
    """
    Checks before writing a listings table. Stops the run on a structural problem; returns the number of
    unparsed posting dates (kept and flagged, only a warning).
    """
    # vehicle_id must be unique (profiling showed no overlap between files).
    # If this fails, the key must become (source_system, vehicle_id) – see documentation.
    assert_unique(df, ["vehicle_id"], table_name)
    assert_columns(df, expected_columns, table_name)

    # A vehicle_id that is not 12 hex characters means a source file was read wrongly (for example a CSV row
    # split in two). This is a reading error, not a data-quality issue, so it stops the run.
    malformed_ids_df = df.filter(~F.coalesce(F.col("vehicle_id").rlike(VEHICLE_ID_PATTERN), F.lit(False)))
    malformed_ids = malformed_ids_df.count()
    if malformed_ids > 0:
        display(malformed_ids_df.groupBy("source_system").count())
        display(malformed_ids_df.select("source_system", "vehicle_id", "listing_url").limit(20))
    assert malformed_ids == 0, (
        f"{table_name}: {malformed_ids} rows have a vehicle_id that is not 12 hex characters – "
        "a source file was parsed wrongly"
    )

    # Unparsed dates do not stop the pipeline: the rows are kept and flagged (Silver rule).
    # A warning with examples makes a new date format or bad source values visible.
    unparsed_df = df.filter(F.array_contains("dq_issues", "unparsed_posting_date"))
    unparsed_dates = unparsed_df.count()
    if unparsed_dates > 0:
        print(
            f"WARNING: {table_name}: {unparsed_dates} posting_date values could not be parsed "
            "(flagged 'unparsed_posting_date'). Check whether a new date format appeared in POSTING_DATE_FORMATS."
        )
        display(unparsed_df.groupBy("platform_name", "posting_date_raw").count().orderBy(F.desc("count")).limit(20))
    return unparsed_dates


def unexpected_currencies(df: DataFrame, expected_currencies: list) -> list:
    """Currencies in the data that this source is not expected to use (only a warning: the values stay)."""
    found = {row.currency for row in df.select("currency").distinct().collect() if row.currency is not None}
    return sorted(found - set(expected_currencies))
