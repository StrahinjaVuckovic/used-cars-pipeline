# Databricks notebook source
# MAGIC %md
# MAGIC # 08 – Gold: fact_service_event
# MAGIC
# MAGIC **Grain:** one service event (`silver.service_events`, degenerate key `service_event_id`).
# MAGIC Foreign keys: `dim_vehicle`, `dim_date` (service date), `dim_service_type` (junk dimension of the event
# MAGIC classification). Measures: cost, downtime, odometer and the oil-change gap; the basis of the Reliability Score.
# MAGIC
# MAGIC **Vehicles without a listing:** on the full data every service-book vehicle has a listing (100% coverage,
# MAGIC verified in Silver). In dev mode the two samples do not match, and in the future a service book could hold an
# MAGIC unknown car: such events point to the Unknown vehicle (`-1`) and are counted in the log, so no event is lost.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads the `_dev` tables and writes `fact_service_event_dev`), `ingestion_date`
# MAGIC (not used).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
TABLE_NAME = "fact_service_event"

log = get_logger("08_gold_fact_service_event")
log.info(f"dev_mode={DEV_MODE}")

# COMMAND ----------


def build_fact_service_event(service_events_df: DataFrame, dim_vehicle_df: DataFrame) -> DataFrame:
    # ~430k keys (a few MB): broadcast, so the 8.3M events are not shuffled.
    known_vehicles = F.broadcast(dim_vehicle_df.select(F.col("vehicle_key").alias("_known_vehicle_key")))
    events = service_events_df.withColumn("_vehicle_key", surrogate_key("vehicle_id"))
    return (
        events.join(known_vehicles, F.col("_vehicle_key") == F.col("_known_vehicle_key"), "left")
        .select(
            "service_event_id",
            F.coalesce("_known_vehicle_key", F.lit(UNKNOWN_KEY).cast("bigint")).alias("vehicle_key"),
            date_key("service_date").alias("service_date_key"),
            surrogate_key(*SERVICE_TYPE_KEY_COLUMNS).alias("service_type_key"),
            "cost_eur",
            "downtime_days",
            "odometer_km",
            "km_since_prev",
            "oil_gap_km",
            "oil_gap_penalty",
            "is_skipped_service",
            "is_valid",
        )
    )


# COMMAND ----------

service_events_df = read_required_table(silver_path("service_events", DEV_MODE), "04_silver_service_events")
dim_vehicle_df = read_required_table(gold_path("dim_vehicle", DEV_MODE), "07_gold_dim_vehicle")

fact_service_event = build_fact_service_event(service_events_df, dim_vehicle_df).cache()

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

silver_totals = service_events_df.agg(F.count("*").alias("rows"), F.sum("cost_eur").alias("cost")).first()
gold_totals = fact_service_event.agg(F.count("*").alias("rows"), F.sum("cost_eur").alias("cost")).first()
assert gold_totals.rows == silver_totals.rows, f"{TABLE_NAME}: {gold_totals.rows} rows, service_events has {silver_totals.rows}"
assert gold_totals.cost == silver_totals.cost, f"{TABLE_NAME}: cost {gold_totals.cost} != Silver {silver_totals.cost}"
assert_unique(fact_service_event, ["service_event_id"], TABLE_NAME)

for fk, dim, dim_key in [
    ("vehicle_key", "dim_vehicle", "vehicle_key"),
    ("service_date_key", "dim_date", "date_key"),
    ("service_type_key", "dim_service_type", "service_type_key"),
]:
    dim_df = read_required_table(gold_path(dim, DEV_MODE), f"the Gold task that writes {dim}")
    assert_foreign_key(fact_service_event, fk, dim_df, dim_key, TABLE_NAME)

unknown_vehicles = fact_service_event.filter(F.col("vehicle_key") == UNKNOWN_KEY).count()
unknown_dates = fact_service_event.filter(F.col("service_date_key") == UNKNOWN_KEY).count()
if unknown_vehicles:
    log.warning(f"{unknown_vehicles} service events belong to vehicles without a listing (vehicle_key -1)")
log.info(
    f"{TABLE_NAME}: {gold_totals.rows} rows, cost total {gold_totals.cost} equal to Silver, foreign keys checked; "
    f"Unknown vehicle {unknown_vehicles}, Unknown service date {unknown_dates}"
)

path = write_gold_table(fact_service_event, TABLE_NAME, DEV_MODE)
log.info(f"Written {gold_totals.rows} rows to {path}")

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

# Events, cost and downtime per event type.
display(
    fact_service_event.join(read_delta_table(gold_path("dim_service_type", DEV_MODE)), "service_type_key")
    .groupBy("event_type")
    .agg(
        F.count("*").alias("events"),
        F.round(F.sum("cost_eur"), 2).alias("total_cost_eur"),
        F.round(F.avg("downtime_days"), 2).alias("avg_downtime_days"),
    )
    .orderBy(F.desc("events"))
)

# COMMAND ----------

_ = fact_service_event.unpersist()
