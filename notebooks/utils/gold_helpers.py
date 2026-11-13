# Databricks notebook source
# MAGIC %md
# MAGIC # Gold helpers – shared by the Gold notebooks
# MAGIC
# MAGIC Run from other notebooks with `%run ./utils/gold_helpers` (after `00_config` and `utils/silver_transformations`).
# MAGIC
# MAGIC - **Column lists** of every Gold table (`GOLD_COLUMNS`). They must match `documentation/gold_star_schema.dbml`
# MAGIC   and `documentation/gold_data_model.md` (a test compares them with the `.dbml` file).
# MAGIC - **Surrogate keys:** `xxhash64` of the natural key, so every daily full rebuild gives the same keys without a
# MAGIC   key mapping table. `-1` is the "Unknown" member of every dimension; a fact row whose natural key is missing
# MAGIC   points to it instead of holding a null.
# MAGIC - **Natural key columns** of the dimensions that facts reference: the dimension and the fact compute the key
# MAGIC   with the same function on the same Silver columns, so they always match.

# COMMAND ----------

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

UNKNOWN_KEY = -1
UNKNOWN_LABEL = "Unknown"

# dim_date covers every valid date of the data: service dates start at 1950-01-01 (the lower bound Silver accepts,
# MIN_SERVICE_DATE in 04_silver_service_events); the end is fixed far enough ahead for planning and BI filters.
DIM_DATE_START = "1950-01-01"
DIM_DATE_END = "2030-12-31"

# Natural keys shared by a dimension and the facts that reference it.
LOCATION_KEY_COLUMNS = ["state", "region"]
SERVICE_TYPE_KEY_COLUMNS = [
    "service_type", "event_type", "maintenance_kind", "severity",
    "is_maintenance", "is_repair", "is_inspection", "is_oil_change",
]

# Currencies of the listings and their display names (dim_currency is built from this list).
CURRENCY_NAMES = {"EUR": "Euro", "USD": "US Dollar", "GBP": "British Pound", "AUD": "Australian Dollar"}
TARGET_CURRENCIES = ["EUR", "GBP", "USD", "AUD"]

# Manufacturer names that initcap would write wrongly (the lookup holds them lower case).
MANUFACTURER_DISPLAY_NAMES = {
    "alfa-romeo": "Alfa Romeo",
    "aston-martin": "Aston Martin",
    "bmw": "BMW",
    "gmc": "GMC",
    "harley-davidson": "Harley-Davidson",
    "mercedes-benz": "Mercedes-Benz",
}

# Final column order of every Gold table (must match documentation/gold_star_schema.dbml).
GOLD_COLUMNS = {
    "dim_date": [
        "date_key", "date", "year", "quarter", "month", "month_name",
        "week_of_year", "day_of_week", "day_name", "is_weekend",
    ],
    "dim_vehicle": [
        "vehicle_key", "vehicle_id", "vin", "manufacture_year", "manufacturer", "model", "manufacturer_source",
        "condition", "cylinders", "fuel", "transmission", "drive", "size", "body_type", "paint_color",
        "title_status", "listing_url", "has_service_history",
    ],
    "dim_location": ["location_key", "state", "region"],
    "dim_platform": ["platform_key", "platform_id", "platform_name"],
    "dim_currency": ["currency_key", "currency_code", "currency_name"],
    "dim_service_type": ["service_type_key", *SERVICE_TYPE_KEY_COLUMNS],
    "fact_listing": [
        "vehicle_key", "posting_date_key", "location_key", "platform_key", "currency_key",
        "price", "price_eur_at_posting", "price_gbp_at_posting", "price_usd_at_posting", "price_aud_at_posting",
        "fx_rate_date", "odometer", "latitude", "longitude", "source_system", "is_valid", "dq_issues",
    ],
    "fact_service_event": [
        "service_event_id", "vehicle_key", "service_date_key", "service_type_key",
        "cost_eur", "downtime_days", "odometer_km", "km_since_prev", "oil_gap_km", "oil_gap_penalty",
        "is_skipped_service", "is_valid",
    ],
    "fact_exchange_rate": ["rate_date_key", "currency_key", "rate_per_eur"],
}

# COMMAND ----------


def gold_path(table_name: str, dev_mode: bool) -> str:
    """Path of a Gold Delta table (`_dev` suffix in dev mode)."""
    return f"{GOLD_PATH}/{table_name}{'_dev' if dev_mode else ''}"


def silver_path(table_name: str, dev_mode: bool) -> str:
    """Path of a Silver Delta table read by Gold (`_dev` suffix in dev mode)."""
    return f"{SILVER_PATH}/{table_name}{'_dev' if dev_mode else ''}"


def read_required_table(path: str, producer: str) -> DataFrame:
    """Read a Delta table that an earlier task must have written; stop with a clear message otherwise."""
    assert delta_table_exists(path), f"No table at {path}: run {producer} first"
    return read_delta_table(path)


def surrogate_key(*columns) -> Column:
    """
    Deterministic surrogate key: xxhash64 over the natural key columns, -1 (Unknown) when all of them are null.
    The same natural key gives the same key in every run, so dimensions and facts can be rebuilt independently.
    """
    columns = [F.col(c) if isinstance(c, str) else c for c in columns]
    all_null = reduce(lambda a, b: a & b, [c.isNull() for c in columns])
    return F.when(all_null, F.lit(UNKNOWN_KEY).cast("bigint")).otherwise(F.xxhash64(*columns))


def date_key(column) -> Column:
    """Date key yyyyMMdd as int; -1 (Unknown) for a null date."""
    column = F.col(column) if isinstance(column, str) else column
    return F.when(column.isNull(), F.lit(UNKNOWN_KEY)).otherwise(F.date_format(column, "yyyyMMdd").cast("int"))


def display_or_unknown(column) -> Column:
    """A descriptive value in Title Case, or "Unknown" when it is missing."""
    column = F.col(column) if isinstance(column, str) else column
    return F.coalesce(F.initcap(column), F.lit(UNKNOWN_LABEL))


def manufacturer_display_name(column) -> Column:
    """Manufacturer as shown to analysts ("Toyota", "BMW", "Mercedes-Benz"); "Unknown" when not mapped."""
    column = F.col(column) if isinstance(column, str) else column
    special = F.create_map(*[F.lit(v) for pair in MANUFACTURER_DISPLAY_NAMES.items() for v in pair])
    return F.coalesce(special[column], F.initcap(column), F.lit(UNKNOWN_LABEL))


def with_unknown_member(df: DataFrame, key_column: str, values: dict = None) -> DataFrame:
    """
    Add the Unknown member (key -1) to a dimension. String columns get "Unknown" unless `values` says otherwise;
    other columns are null.
    """
    values = values or {}
    unknown_columns = []
    for field in df.schema.fields:
        if field.name == key_column:
            value = F.lit(UNKNOWN_KEY)
        elif field.name in values:
            value = F.lit(values[field.name])
        elif field.dataType.simpleString() == "string":
            value = F.lit(UNKNOWN_LABEL)
        else:
            value = F.lit(None)
        unknown_columns.append(value.cast(field.dataType).alias(field.name))
    return df.unionByName(df.sparkSession.range(1).select(*unknown_columns))


def assert_foreign_key(fact_df: DataFrame, fact_column: str, dim_df: DataFrame, dim_column: str, name: str) -> None:
    """Fail if a fact row points to a key that the dimension does not have."""
    orphans = (
        fact_df.select(F.col(fact_column).alias("_key")).distinct()
        .join(dim_df.select(F.col(dim_column).alias("_key")), "_key", "left_anti")
    )
    orphan_keys = [r._key for r in orphans.limit(5).collect()]
    assert not orphan_keys, f"{name}: {fact_column} values missing in the dimension (first 5: {orphan_keys})"


def write_gold_table(df: DataFrame, table_name: str, dev_mode: bool, num_files: int = None) -> str:
    """Check the documented columns and overwrite the Gold table; returns its path."""
    assert_columns(df, GOLD_COLUMNS[table_name], table_name)
    path = gold_path(table_name, dev_mode)
    write_delta_table(df, path, num_files=num_files)
    return path
