# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling 04 – inputs of the Reliability Score (Silver service_events)
# MAGIC
# MAGIC Explores the cleaned service book to decide which columns the Reliability Score uses and how
# MAGIC (`notebooks/09_gold_fact_vehicle_reliability.py`, `documentation/reliability_score.md`). Nothing is written.
# MAGIC Not part of the daily pipeline; run it on the full data (a few minutes).
# MAGIC
# MAGIC ## Findings (full data, 8,312,490 events, 419,641 vehicles; first profiled 2026-11-13 on the local copy)
# MAGIC
# MAGIC | Question | Finding | Decision |
# MAGIC |---|---|---|
# MAGIC | Which kinds of events exist? | 5 service types: OIL_CHANGE 7,667,397, BIG_SERVICE 498,285, REPAIR 99,848, ENGINE_WORK 26,651, BODY_WORK 20,309 | one role per type |
# MAGIC | Where are cost and downtime? | only on REPAIR rows (median 1,626 EUR, 6.6 days); 90% of downtimes have a fraction | repairs are weighted by downtime; Silver keeps downtime as double |
# MAGIC | How is a failure recorded? | as two REPAIR rows, diagnosis + fix ("transmission failure…" / "transmission replaced…", same count per type); vehicles have exactly 2 or 4 rows | incidents = ceil(repair rows / 2) |
# MAGIC | How many vehicles had a failure? | 33,285 (7.9%): 16,646 with 1 incident, 16,639 with 2 | the main penalty, 25 per incident + 0.5 per downtime day |
# MAGIC | What is ENGINE_WORK? | every row "Major engine inspection", is_inspection = 1, is_maintenance = 1, is_repair = 0, LOW, no cost; vehicles with it fail as often as the rest (7.79% vs 7.93%); engine failures are separate REPAIR rows | neutral (weight 0) |
# MAGIC | What is BODY_WORK? | every row "Minor exterior repair", not maintenance, MEDIUM, no cost; 4.8% of vehicles, at most once | small penalty, 5 |
# MAGIC | Skipped services? | 148,896 BIG_SERVICE rows noted "Skipped"; 27.5% of vehicles have at least one | 5 per skipped service, at most 15 |
# MAGIC | Late oil changes? | `oil_gap_penalty` > 0 on 9.4% of events, 40% of vehicles; per vehicle p75 4, p95 18, p99 46, max 84,048 (impossible odometer jumps) | 0.5 per point, at most 10; rows with a non-monotonic odometer ignored |
# MAGIC | Are the components related? | correlation between repairs, body work, skipped services and oil penalty ≈ 0 | each component adds its own information |
# MAGIC | How long is a vehicle's history? | median 0.97 years and 17 events | no normalization per year or km |

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %run ../utils/silver_transformations

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "false", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
EVENTS_PATH = f"{SILVER_PATH}/service_events{'_dev' if DEV_MODE else ''}"
events = read_delta_table(EVENTS_PATH)

# COMMAND ----------

# MAGIC %md ## Event types: classification, cost and downtime

# COMMAND ----------

display(
    events.groupBy("service_type", "event_type", "severity", "is_maintenance", "is_repair", "is_inspection")
    .agg(
        F.count("*").alias("events"),
        F.round(F.avg(F.col("cost_eur").isNotNull().cast("int")), 3).alias("share_with_cost"),
        F.percentile_approx("cost_eur", 0.5).alias("median_cost_eur"),
        F.percentile_approx("downtime_days", 0.5).alias("median_downtime_days"),
    )
    .orderBy(F.desc("events"))
)

# COMMAND ----------

# Notes per service type (shows that ENGINE_WORK is an inspection and that a failure is diagnosis + fix).
display(events.groupBy("service_type", "notes").count().orderBy("service_type", F.desc("count")))

# COMMAND ----------

# MAGIC %md ## Per vehicle

# COMMAND ----------

per_vehicle = events.groupBy("vehicle_id").agg(
    F.count("*").alias("events"),
    F.sum(F.col("is_repair").cast("int")).alias("repair_rows"),
    F.sum((F.col("service_type") == "ENGINE_WORK").cast("int")).alias("engine_work"),
    F.sum((F.col("service_type") == "BODY_WORK").cast("int")).alias("body_work"),
    F.sum(F.col("is_skipped_service").cast("int")).alias("skipped"),
    F.sum(F.when(F.col("is_odometer_monotonic"), F.col("oil_gap_penalty"))).alias("oil_gap_penalty"),
    F.datediff(F.max("service_date"), F.min("service_date")).alias("history_days"),
).cache()

display(per_vehicle.groupBy("repair_rows").count().orderBy("repair_rows"))

# COMMAND ----------

quantiles = [0.5, 0.75, 0.95, 0.99, 1.0]
display(
    spark.createDataFrame(
        [(c, *per_vehicle.approxQuantile(c, quantiles, 0.001)) for c in ["events", "skipped", "oil_gap_penalty", "history_days"]],
        "metric STRING, p50 DOUBLE, p75 DOUBLE, p95 DOUBLE, p99 DOUBLE, max DOUBLE",
    )
)

# COMMAND ----------

# Do engine inspections go with failures? Compare the failure rate of vehicles with and without ENGINE_WORK.
display(
    per_vehicle.groupBy((F.col("engine_work") > 0).alias("has_engine_work"))
    .agg(F.count("*").alias("vehicles"), F.round(F.avg((F.col("repair_rows") > 0).cast("int")), 4).alias("failure_rate"))
)

# COMMAND ----------

# Correlation between the components (≈ 0 means each one adds its own information).
display(
    spark.createDataFrame(
        [(c, per_vehicle.stat.corr("repair_rows", c)) for c in ["body_work", "engine_work", "skipped", "events"]],
        "component STRING, corr_with_repairs DOUBLE",
    )
)

# COMMAND ----------

_ = per_vehicle.unpersist()
