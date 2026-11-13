# Databricks notebook source
# MAGIC %md
# MAGIC # 07 – Gold: dim_vehicle
# MAGIC
# MAGIC The **conformed vehicle dimension**: one row per vehicle (`vehicle_id` of `silver.vehicle_listings`, where one
# MAGIC listing is one vehicle), plus the Unknown member (`vehicle_key = -1`). `fact_listing`, `fact_service_event` and
# MAGIC `fact_vehicle_reliability` all reference it, so brand questions on service data need no join between facts.
# MAGIC
# MAGIC Model and manufacturer are attributes of the vehicle (star schema, denormalized), not a separate dimension.
# MAGIC
# MAGIC **Business-ready values:** manufacturer as a display name ("Toyota", "BMW", "Mercedes-Benz"), descriptive
# MAGIC attributes in Title Case ("Like New"), and "Unknown" instead of null for text. Numbers stay null when unknown.
# MAGIC Technical and heavy Silver columns (description, image and region URLs, raw posting date, lineage) are not
# MAGIC carried into Gold. SCD Type 1: the table is rebuilt from the latest Silver snapshot every day.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads the `_dev` Silver tables and writes `dim_vehicle_dev`), `ingestion_date`
# MAGIC (`latest`, or a date that `vehicle_listings` must have been built from).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
TABLE_NAME = "dim_vehicle"

# Descriptive attributes shown in Title Case with "Unknown" for missing values.
DESCRIPTIVE_COLUMNS = [
    "condition", "fuel", "transmission", "drive", "size", "body_type", "paint_color", "title_status",
]

log = get_logger("07_gold_dim_vehicle")
log.info(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}")

# COMMAND ----------


def assert_listings_ingestion_date(listings_df: DataFrame, ingestion_date: str) -> str:
    """vehicle_listings holds one ingestion; with a requested date it must be that one. Returns the date."""
    dates = sorted(str(r.ingestion_date) for r in listings_df.select("ingestion_date").distinct().collect())
    assert len(dates) == 1, f"vehicle_listings has several ingestion dates: {dates}"
    if ingestion_date != "latest":
        assert dates[0] == ingestion_date, f"vehicle_listings was built from {dates[0]}, not {ingestion_date}"
    return dates[0]


def build_dim_vehicle(listings_df: DataFrame, service_events_df: DataFrame) -> DataFrame:
    """One business-ready row per vehicle, plus the Unknown member."""
    serviced = service_events_df.select("vehicle_id").distinct().withColumn("has_service_history", F.lit(True))
    vehicles = (
        listings_df.join(serviced, "vehicle_id", "left")
        .select(
            surrogate_key("vehicle_id").alias("vehicle_key"),
            "vehicle_id",
            "vin",
            "manufacture_year",
            manufacturer_display_name("manufacturer").alias("manufacturer"),
            F.coalesce("model", F.lit(UNKNOWN_LABEL)).alias("model"),
            F.coalesce("manufacturer_source", F.lit("unknown")).alias("manufacturer_source"),
            *[display_or_unknown(c).alias(c) for c in DESCRIPTIVE_COLUMNS],
            "cylinders",
            "listing_url",
            F.coalesce("has_service_history", F.lit(False)).alias("has_service_history"),
        )
    )
    # Documented column order (the select above groups the expressions by kind).
    return with_unknown_member(
        vehicles.select(*GOLD_COLUMNS[TABLE_NAME]), "vehicle_key",
        {"vehicle_id": None, "vin": None, "listing_url": None, "manufacturer_source": "unknown",
         "has_service_history": False},
    )


# COMMAND ----------

listings_df = read_required_table(silver_path("vehicle_listings", DEV_MODE), "03_silver_vehicle_listings")
service_events_df = read_required_table(silver_path("service_events", DEV_MODE), "04_silver_service_events")
listings_ingestion_date = assert_listings_ingestion_date(listings_df, INGESTION_DATE)

dim_vehicle = build_dim_vehicle(listings_df, service_events_df).cache()

listings_count = listings_df.count()
row_count = dim_vehicle.count()
assert row_count == listings_count + 1, f"{TABLE_NAME}: {row_count} rows, vehicle_listings has {listings_count} (+1 Unknown)"
assert_unique(dim_vehicle, ["vehicle_key"], TABLE_NAME)  # also proves there is no hash collision between vehicles

path = write_gold_table(dim_vehicle, TABLE_NAME, DEV_MODE)
log.info(f"Written {row_count} rows ({listings_count} vehicles from ingestion_date={listings_ingestion_date} + Unknown) to {path}")

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

# Vehicles per manufacturer (top 15) and how the manufacturer was found.
display(dim_vehicle.groupBy("manufacturer").count().orderBy(F.desc("count")).limit(15))

# COMMAND ----------

display(dim_vehicle.groupBy("manufacturer_source", "has_service_history").count().orderBy("manufacturer_source"))

# COMMAND ----------

_ = dim_vehicle.unpersist()
