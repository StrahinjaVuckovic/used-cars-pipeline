# Databricks notebook source
# MAGIC %md
# MAGIC # 07 – Gold: small dimensions (dim_location, dim_platform, dim_currency, dim_service_type)
# MAGIC
# MAGIC Four small dimensions in one task (each has a few to a few hundred rows, so separate tasks would only add
# MAGIC start-up time). Each gets the Unknown member (key `-1`).
# MAGIC
# MAGIC | Table | Grain | Source |
# MAGIC |---|---|---|
# MAGIC | `dim_location` | state + region | distinct values of `silver.vehicle_listings` |
# MAGIC | `dim_platform` | listing platform | distinct `platform_id` / `platform_name` of `silver.vehicle_listings` |
# MAGIC | `dim_currency` | currency | fixed list `CURRENCY_NAMES` (the currencies Silver accepts) |
# MAGIC | `dim_service_type` | each distinct classification of a service event (junk dimension) | `silver.service_events` |
# MAGIC
# MAGIC Keys are `xxhash64` of the natural key columns (`utils/gold_helpers`), computed on the Silver values before
# MAGIC they are made readable, so the fact notebooks get the same keys from the same columns.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads the `_dev` Silver tables and writes `*_dev`), `ingestion_date` (not used).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

log = get_logger("07_gold_dim_small")
log.info(f"dev_mode={DEV_MODE}")

# COMMAND ----------


def build_dim_location(listings_df: DataFrame) -> DataFrame:
    locations = (
        listings_df.select(*LOCATION_KEY_COLUMNS).distinct()
        .filter(F.col("state").isNotNull() | F.col("region").isNotNull())
        .select(
            surrogate_key(*LOCATION_KEY_COLUMNS).alias("location_key"),
            F.coalesce("state", F.lit(UNKNOWN_LABEL)).alias("state"),
            F.coalesce("region", F.lit(UNKNOWN_LABEL)).alias("region"),
        )
    )
    return with_unknown_member(locations, "location_key")


def build_dim_platform(listings_df: DataFrame) -> DataFrame:
    platforms = (
        listings_df.filter(F.col("platform_id").isNotNull())
        .groupBy("platform_id").agg(F.max("platform_name").alias("platform_name"))
        .select(
            surrogate_key("platform_id").alias("platform_key"),
            "platform_id",
            F.coalesce("platform_name", F.lit(UNKNOWN_LABEL)).alias("platform_name"),
        )
    )
    return with_unknown_member(platforms, "platform_key")


def build_dim_currency() -> DataFrame:
    currencies = spark.createDataFrame(
        sorted(CURRENCY_NAMES.items()), "currency_code STRING, currency_name STRING"
    ).select(surrogate_key("currency_code").alias("currency_key"), "currency_code", "currency_name")
    return with_unknown_member(currencies, "currency_key")


def build_dim_service_type(service_events_df: DataFrame) -> DataFrame:
    flags = ["is_maintenance", "is_repair", "is_inspection", "is_oil_change"]
    texts = [c for c in SERVICE_TYPE_KEY_COLUMNS if c not in flags]
    service_types = (
        service_events_df.select(*SERVICE_TYPE_KEY_COLUMNS).distinct()
        .select(
            surrogate_key(*SERVICE_TYPE_KEY_COLUMNS).alias("service_type_key"),
            *[F.coalesce(c, F.lit(UNKNOWN_LABEL)).alias(c) for c in texts],
            *flags,
        )
    )
    return with_unknown_member(service_types, "service_type_key")


# COMMAND ----------

listings_df = read_required_table(silver_path("vehicle_listings", DEV_MODE), "03_silver_vehicle_listings")
service_events_df = read_required_table(silver_path("service_events", DEV_MODE), "04_silver_service_events")

dimensions = {
    "dim_location": (build_dim_location(listings_df), "location_key"),
    "dim_platform": (build_dim_platform(listings_df), "platform_key"),
    "dim_currency": (build_dim_currency(), "currency_key"),
    "dim_service_type": (build_dim_service_type(service_events_df), "service_type_key"),
}

row_counts = {}
for table_name, (dim_df, key_column) in dimensions.items():
    dim_df = dim_df.select(*GOLD_COLUMNS[table_name]).cache()
    assert_unique(dim_df, [key_column], table_name)
    row_counts[table_name] = dim_df.count()
    path = write_gold_table(dim_df, table_name, DEV_MODE, num_files=1)
    dim_df.unpersist()
    log.info(f"Written {row_counts[table_name]} rows (with Unknown) to {path}")

assert row_counts["dim_currency"] == len(CURRENCY_NAMES) + 1, f"dim_currency: {row_counts['dim_currency']} rows"

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

display(read_delta_table(gold_path("dim_platform", DEV_MODE)).orderBy("platform_id"))

# COMMAND ----------

display(read_delta_table(gold_path("dim_service_type", DEV_MODE)).orderBy("event_type", "service_type"))
