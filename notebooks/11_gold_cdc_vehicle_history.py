# Databricks notebook source
# MAGIC %md
# MAGIC # 11 – Gold: dim_vehicle (SCD2) and fact_listing_event (CDC demo)
# MAGIC
# MAGIC Builds the Gold tables of the CDC demo (documentation/cdc.md) from the Silver history
# MAGIC `silver/vehicle_listings_cdc`, as a **full rebuild** like the rest of Gold: the history lives in Silver,
# MAGIC so rebuilding Gold never loses it. The tables are stored under `gold/cdc/`, apart from the main star schema.
# MAGIC
# MAGIC - **`dim_vehicle`**: the same vehicle dimension as `07_gold_dim_vehicle` (same business-ready attributes)
# MAGIC   plus SCD Type 2 columns: `vehicle_key` (durable, the same for all versions and the same as in the main
# MAGIC   Gold), `vehicle_version_key` (one version), `valid_from`, `valid_to`, `is_current`, `is_deleted` (sold),
# MAGIC   `version_number`. A new version starts only when a dimension attribute changes (e.g. the condition);
# MAGIC   a new price or more kilometres are facts, not a new version of the vehicle.
# MAGIC - **`fact_listing_event`**: one row per change (Snapshot / Listed / Price Change / Updated / Sold) with
# MAGIC   the price before and after, linked to the vehicle version that was valid at that moment.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads `vehicle_listings_cdc_dev` and writes the `_dev` tables).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/vehicle_listings_cleaning

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

# MAGIC %run ./utils/cdc_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

log = get_logger("11_gold_cdc_vehicle_history")
log.info(f"dev_mode={DEV_MODE}")

# COMMAND ----------

# MAGIC %md ## Build and check

# COMMAND ----------

versions_df = read_required_table(cdc_silver_path(DEV_MODE), "10_silver_listing_changes").cache()

dim_df = build_cdc_dim_vehicle(versions_df).cache()
assert_columns(dim_df, CDC_GOLD_COLUMNS["dim_vehicle"], "dim_vehicle")
assert_unique(dim_df, ["vehicle_version_key"], "dim_vehicle")
current_vehicles = versions_df.filter("is_current").select("vehicle_id").distinct().count()
current_versions = dim_df.filter("is_current AND vehicle_version_key <> -1").count()
assert current_versions == current_vehicles, (
    f"dim_vehicle: {current_versions} current versions, Silver has {current_vehicles} current listings"
)

fact_df = build_fact_listing_event(versions_df, dim_df).cache()
assert_columns(fact_df, CDC_GOLD_COLUMNS["fact_listing_event"], "fact_listing_event")
assert_unique(fact_df, ["event_key"], "fact_listing_event")  # also proves the point-in-time join is 1:1
assert_foreign_key(fact_df, "vehicle_version_key", dim_df, "vehicle_version_key", "fact_listing_event")
without_version = fact_df.filter(F.col("vehicle_version_key") == UNKNOWN_KEY).count()
assert without_version == 0, f"fact_listing_event: {without_version} events without a valid vehicle version"

# COMMAND ----------

# MAGIC %md ## Write (full rebuild)

# COMMAND ----------

for table_name, df in (("dim_vehicle", dim_df), ("fact_listing_event", fact_df)):
    path = cdc_gold_path(table_name, DEV_MODE)
    write_delta_table(df, path, num_files=1)
    log.info(f"Written {df.count()} rows to {path}")

versions_per_vehicle = dim_df.filter("vehicle_version_key <> -1").groupBy("vehicle_id").count()
log.info(
    f"dim_vehicle: {versions_per_vehicle.count()} vehicles, "
    f"{versions_per_vehicle.filter('count > 1').count()} with more than one version, "
    f"{dim_df.filter('is_deleted').count()} sold"
)
log.info(f"fact_listing_event: {dict(fact_df.groupBy('event_type').count().collect())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Questions that need the history
# MAGIC
# MAGIC None of them can be answered from a full snapshot, which only knows the current state.

# COMMAND ----------

# Price drops per manufacturer: how many and how big on average (current attributes of the vehicle).
display(
    fact_df.filter("event_type = 'Price Change'")
    .join(dim_df.filter("is_current OR is_deleted").select("vehicle_key", "manufacturer"), "vehicle_key")
    .groupBy("manufacturer")
    .agg(F.count("*").alias("price_drops"), F.round(F.avg("price_change"), 2).alias("avg_price_change"))
    .orderBy(F.desc("price_drops"))
    .limit(10)
)

# COMMAND ----------

# Time on the market of the sold vehicles: from the first event (snapshot or listing) to the sale.
# In the local simulation every "day" takes seconds, so the durations are short; the logic is the same.
first_seen = fact_df.groupBy("vehicle_key").agg(F.min("event_ts").alias("first_seen"))
display(
    fact_df.filter("event_type = 'Sold'").join(first_seen, "vehicle_key")
    .select(
        "vehicle_key",
        "first_seen",
        F.col("event_ts").alias("sold_at"),
        F.round((F.unix_timestamp("event_ts") - F.unix_timestamp("first_seen")) / 60, 1).alias("minutes_on_market"),
    )
    .orderBy(F.desc("minutes_on_market"))
    .limit(10)
)

# COMMAND ----------

# Point in time: the price of every listing as it was at a chosen moment (here: before the latest change).
as_of = fact_df.filter("event_type <> 'Snapshot'").agg(F.max("event_ts")).first()[0] or F.current_timestamp()
display(
    fact_df.filter(F.col("event_ts") < F.lit(as_of))
    .withColumn("_rank", F.row_number().over(Window.partitionBy("vehicle_key").orderBy(F.desc("event_ts"))))
    .filter("_rank = 1 AND event_type <> 'Sold'")  # the latest event before that moment; sold ones are gone
    .groupBy("currency")
    .agg(F.count("*").alias("listings"), F.round(F.avg("price"), 2).alias("avg_price"))
)

# COMMAND ----------

# Vehicles whose attributes changed (more than one version in the dimension).
display(
    dim_df.filter("vehicle_version_key <> -1")
    .groupBy("vehicle_key").agg(F.count("*").alias("versions"), F.collect_list("condition").alias("conditions"))
    .filter("versions > 1")
    .orderBy(F.desc("versions"))
    .limit(10)
)

# COMMAND ----------

for cached in (versions_df, dim_df, fact_df):
    _ = cached.unpersist()
