# Databricks notebook source
# MAGIC %md
# MAGIC # 09 – Gold: fact_vehicle_reliability (Reliability Score 0–100)
# MAGIC
# MAGIC **Grain:** one vehicle of `dim_vehicle` (Unknown member excluded). One row per vehicle with the service-book
# MAGIC summary, the points each component takes away and the **Reliability Score**. The algorithm and the profiling
# MAGIC behind every weight are in `documentation/reliability_score.md`; the weights are `RELIABILITY_WEIGHTS` in
# MAGIC `utils/gold_helpers`.
# MAGIC
# MAGIC ```
# MAGIC score = clamp(0, 100, 100
# MAGIC     - (35 x engine failures + 30 x transmission failures       repairs: the main penalty, by kind of failure
# MAGIC        + 20 x head gasket failures + 25 x other failures      (recognized from the repair notes)
# MAGIC        + 0.5 x repair downtime days)
# MAGIC     - (5 x BODY_WORK + 0 x ENGINE_WORK)                        minor issues (an engine inspection is neutral)
# MAGIC     - min(15, 5 x skipped big services)                         maintenance discipline
# MAGIC     - min(10, 0.5 x oil_gap_penalty)                            late oil changes (valid odometer only)
# MAGIC     - min(10, 0.2 x age years + 0.05 x km / 1000)               wear: age at posting and service-book mileage,
# MAGIC       x (1 + 0.5 x (skipped + oil penalties) / 25))             raised up to x1.5 when maintenance was neglected
# MAGIC ```
# MAGIC
# MAGIC - Only regular maintenance → 100; frequent repairs → lowest (brief).
# MAGIC - A repair incident is recorded as two rows (diagnosis + fix) of the same kind, so incidents per kind =
# MAGIC   ceil(repair rows of that kind / 2). The kind comes from the notes (`failure_type` in `utils/gold_helpers`).
# MAGIC - Vehicles without service history get **no score** (null, category "Unknown"), not 100: there is no evidence.
# MAGIC - `score_confidence` grows with the number of service events (High ≥ 15, Medium ≥ 5, Low).
# MAGIC
# MAGIC Input: `silver.service_events` (it has the skipped-service note and the odometer flag that `fact_service_event`
# MAGIC does not carry), `silver.vehicle_listings` (manufacture year and posting date for the age) and `gold.dim_vehicle`
# MAGIC for the keys. The age is taken at the posting date, not today, so the same data always gives the same score.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads the `_dev` tables and writes `fact_vehicle_reliability_dev`),
# MAGIC `ingestion_date` (not used).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
TABLE_NAME = "fact_vehicle_reliability"

log = get_logger("09_gold_fact_vehicle_reliability")
# Column of the per-kind incident count, and the penalty per incident, for every kind of failure.
FAILURE_INCIDENT_COLUMNS = {
    "engine_failure": "engine_failure_incidents",
    "transmission": "transmission_incidents",
    "head_gasket": "head_gasket_incidents",
    OTHER_FAILURE[0]: "other_failure_incidents",
}
FAILURE_PENALTIES = {code: penalty for code, _, penalty, _ in FAILURE_TYPES}
FAILURE_PENALTIES[OTHER_FAILURE[0]] = OTHER_FAILURE[1]

log.info(f"dev_mode={DEV_MODE}, weights={RELIABILITY_WEIGHTS}, failure penalties={FAILURE_PENALTIES}")

# COMMAND ----------


def vehicle_service_summary(service_events_df: DataFrame) -> DataFrame:
    """The service-book facts per vehicle that the score is built from."""
    is_type = lambda t: F.col("service_type") == t  # noqa: E731
    is_repair = F.coalesce(F.col("is_repair"), F.lit(False))
    skipped = F.coalesce(F.col("is_skipped_service"), F.lit(False))
    performed_maintenance = (is_type("OIL_CHANGE") | is_type("BIG_SERVICE") | is_type("ENGINE_WORK")) & ~skipped
    trusted_odometer = F.coalesce(F.col("is_odometer_monotonic"), F.lit(False))
    count_if = lambda condition: F.sum(F.when(condition, 1).otherwise(0)).cast("int")  # noqa: E731
    kind = failure_type("notes", "is_repair")
    summary = service_events_df.groupBy("vehicle_id").agg(
        F.count("*").cast("int").alias("service_events"),
        count_if(performed_maintenance).alias("maintenance_events"),
        count_if(is_repair).alias("repair_events"),
        F.coalesce(F.sum(F.when(is_repair, F.col("cost_eur"))), F.lit(0)).cast("decimal(12,2)").alias("repair_cost_eur"),
        F.coalesce(F.sum(F.when(is_repair, F.col("downtime_days"))), F.lit(0.0)).cast("double").alias("repair_downtime_days"),
        count_if(is_type("BODY_WORK")).alias("minor_repairs"),
        count_if(is_type("ENGINE_WORK")).alias("engine_inspections"),
        count_if(skipped).alias("skipped_services"),
        F.coalesce(F.sum(F.when(trusted_odometer, F.col("oil_gap_penalty"))), F.lit(0.0)).cast("double").alias("oil_gap_penalty_total"),
        # Highest service-book odometer (km) of the rows with a valid, increasing odometer.
        F.max(F.when(trusted_odometer, F.col("odometer_km"))).alias("max_odometer_km"),
        *[count_if(kind == code).alias(f"{code}_rows") for code in FAILURE_INCIDENT_COLUMNS],
    )
    # Incidents per kind (a diagnosis row and a fix row of the same kind are one incident) and in total.
    for code, column in FAILURE_INCIDENT_COLUMNS.items():
        summary = summary.withColumn(column, F.ceil(F.col(f"{code}_rows") / REPAIR_ROWS_PER_INCIDENT).cast("int"))
    return summary.withColumn(
        "repair_incidents", sum(F.col(c) for c in FAILURE_INCIDENT_COLUMNS.values()).cast("int")
    )


def category_of(score: Column) -> Column:
    expr = F.when(score.isNull(), F.lit(UNKNOWN_LABEL))
    for lower_bound, label in RELIABILITY_CATEGORIES:
        expr = expr.when(score >= lower_bound, F.lit(label))
    return expr


def confidence_of(events: Column) -> Column:
    expr = F.when(events.isNull() | (events == 0), F.lit(UNKNOWN_LABEL))
    for min_events, label in SCORE_CONFIDENCE_EVENTS:
        expr = expr.when(events >= min_events, F.lit(label))
    return expr


def vehicle_ages(listings_df: DataFrame) -> DataFrame:
    """Age in years at the posting date per vehicle (posting year - manufacture year, at least 0; null if unknown)."""
    age = F.year("posting_date") - F.col("manufacture_year")
    return listings_df.select(
        surrogate_key("vehicle_id").alias("vehicle_key"),
        F.when(age.isNotNull(), F.greatest(age, F.lit(0))).cast("int").alias("vehicle_age_years"),
    )


def score_vehicles(summary_df: DataFrame, dim_vehicle_df: DataFrame, listings_df: DataFrame = None) -> DataFrame:
    """One row per vehicle of dim_vehicle: components, penalties, score, category, confidence."""
    w = RELIABILITY_WEIGHTS
    vehicles = dim_vehicle_df.filter(F.col("vehicle_key") != UNKNOWN_KEY).select("vehicle_key")
    summary = summary_df.withColumn("vehicle_key", surrogate_key("vehicle_id"))
    joined = vehicles.join(summary, "vehicle_key", "left")
    if listings_df is not None:
        joined = joined.join(vehicle_ages(listings_df), "vehicle_key", "left")
    else:
        joined = joined.withColumn("vehicle_age_years", F.lit(None).cast("int"))
    has_history = F.coalesce(F.col("service_events"), F.lit(0)) > 0

    def penalty(value: Column) -> Column:
        return F.when(has_history, value.cast("double"))

    failure_points = sum(FAILURE_PENALTIES[code] * F.col(column) for code, column in FAILURE_INCIDENT_COLUMNS.items())
    repair_penalty = penalty(failure_points + w["repair_downtime_day"] * F.col("repair_downtime_days"))
    minor_penalty = penalty(w["minor_repair"] * F.col("minor_repairs") + w["engine_inspection"] * F.col("engine_inspections"))
    skipped_penalty = penalty(F.least(F.lit(w["skipped_service_max"]), w["skipped_service"] * F.col("skipped_services")))
    oil_penalty = penalty(F.least(F.lit(w["oil_gap_max"]), w["oil_gap_point"] * F.col("oil_gap_penalty_total")))
    # Wear: age and mileage (unknown = 0), capped together, raised when maintenance was neglected.
    age_points = penalty(w["age_point_per_year"] * F.coalesce(F.col("vehicle_age_years"), F.lit(0)))
    mileage_points = penalty(w["mileage_point_per_1000_km"] * F.coalesce(F.col("max_odometer_km"), F.lit(0)) / 1000)
    neglect_share = (skipped_penalty + oil_penalty) / (w["skipped_service_max"] + w["oil_gap_max"])
    wear_multiplier = penalty(1 + (w["wear_multiplier_max"] - 1) * neglect_share)
    wear_penalty = F.least(F.lit(w["wear_max"]), age_points + mileage_points) * wear_multiplier

    scored = joined.select(
        "vehicle_key",
        *[F.coalesce(F.col(c), F.lit(0)).cast("int").alias(c) for c in
          ["service_events", "maintenance_events", "repair_incidents", *FAILURE_INCIDENT_COLUMNS.values()]],
        F.coalesce("repair_cost_eur", F.lit(0)).cast("decimal(12,2)").alias("repair_cost_eur"),
        F.coalesce("repair_downtime_days", F.lit(0.0)).alias("repair_downtime_days"),
        *[F.coalesce(F.col(c), F.lit(0)).cast("int").alias(c) for c in ["minor_repairs", "skipped_services"]],
        F.coalesce("oil_gap_penalty_total", F.lit(0.0)).alias("oil_gap_penalty_total"),
        "vehicle_age_years",
        F.col("max_odometer_km").cast("int").alias("max_odometer_km"),
        repair_penalty.alias("repair_penalty"),
        minor_penalty.alias("minor_repair_penalty"),
        skipped_penalty.alias("skipped_service_penalty"),
        oil_penalty.alias("oil_change_penalty"),
        F.round(age_points, 2).alias("age_points"),
        F.round(mileage_points, 2).alias("mileage_points"),
        F.round(wear_multiplier, 3).alias("wear_multiplier"),
        F.round(wear_penalty, 2).alias("wear_penalty"),
    )
    total_penalty = (
        F.col("repair_penalty") + F.col("minor_repair_penalty") + F.col("skipped_service_penalty")
        + F.col("oil_change_penalty") + F.col("wear_penalty")
    )
    # least/greatest skip nulls (least(100, null) = 100), so a vehicle without history must be null explicitly.
    score = F.when(
        total_penalty.isNotNull(),
        F.round(F.greatest(F.lit(0.0), F.least(F.lit(100.0), F.lit(100.0) - total_penalty)), 1),
    )
    return (
        scored.withColumn("reliability_score", score)
        .withColumn("reliability_category", category_of(F.col("reliability_score")))
        .withColumn("score_confidence", confidence_of(F.col("service_events")))
        .select(*GOLD_COLUMNS[TABLE_NAME])
    )


# COMMAND ----------

service_events_df = read_required_table(silver_path("service_events", DEV_MODE), "04_silver_service_events")
dim_vehicle_df = read_required_table(gold_path("dim_vehicle", DEV_MODE), "07_gold_dim_vehicle")
listings_df = read_required_table(silver_path("vehicle_listings", DEV_MODE), "03_silver_vehicle_listings").select(
    "vehicle_id", "manufacture_year", "posting_date"
)

summary_df = vehicle_service_summary(service_events_df).cache()
reliability = score_vehicles(summary_df, dim_vehicle_df, listings_df).cache()

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

vehicle_count = dim_vehicle_df.filter(F.col("vehicle_key") != UNKNOWN_KEY).count()
row_count = reliability.count()
assert row_count == vehicle_count, f"{TABLE_NAME}: {row_count} rows, dim_vehicle has {vehicle_count} vehicles"
assert_unique(reliability, ["vehicle_key"], TABLE_NAME)
assert_foreign_key(reliability, "vehicle_key", dim_vehicle_df, "vehicle_key", TABLE_NAME)

out_of_range = reliability.filter((F.col("reliability_score") < 0) | (F.col("reliability_score") > 100)).count()
assert out_of_range == 0, f"{TABLE_NAME}: {out_of_range} scores outside 0-100"
null_mismatch = reliability.filter(F.col("reliability_score").isNull() != (F.col("service_events") == 0)).count()
assert null_mismatch == 0, f"{TABLE_NAME}: {null_mismatch} rows where a missing score does not mean 'no service history'"

# The per-vehicle summary must keep every repair euro of Silver.
silver_cost = service_events_df.filter(F.coalesce(F.col("is_repair"), F.lit(False))).agg(F.sum("cost_eur")).first()[0] or 0
summary_cost = summary_df.agg(F.sum("repair_cost_eur")).first()[0] or 0
assert summary_cost == silver_cost, f"{TABLE_NAME}: repair cost {summary_cost} != Silver {silver_cost}"

odd_repairs = summary_df.filter(
    reduce(lambda a, b: a | b, [F.col(f"{code}_rows") % REPAIR_ROWS_PER_INCIDENT != 0 for code in FAILURE_INCIDENT_COLUMNS])
).count()
if odd_repairs:
    log.warning(f"{odd_repairs} vehicles have an odd number of repair rows of one kind (a diagnosis without its fix)")
other_failures = summary_df.agg(F.sum(f"{OTHER_FAILURE[0]}_rows")).first()[0] or 0
if other_failures:
    log.warning(f"{other_failures} repair rows match no known kind of failure (penalty {OTHER_FAILURE[1]}): check the notes")
repair_rows = summary_df.agg(F.sum("repair_events")).first()[0] or 0
kind_rows = summary_df.agg(sum(F.sum(f"{code}_rows") for code in FAILURE_INCIDENT_COLUMNS)).first()[0] or 0
assert kind_rows == repair_rows, f"{TABLE_NAME}: {kind_rows} repair rows by kind, {repair_rows} repair rows in total"
unmatched = summary_df.join(reliability.select("vehicle_key"),
                            surrogate_key("vehicle_id") == F.col("vehicle_key"), "left_anti").count()
if unmatched:
    log.warning(f"{unmatched} service-book vehicles have no listing and get no row (expected 0 on the full data)")

log.info(f"{TABLE_NAME}: {row_count} vehicles, checks passed; repair cost total {summary_cost} equal to Silver")

path = write_gold_table(reliability, TABLE_NAME, DEV_MODE)
log.info(f"Written {row_count} rows to {path}")

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

# Vehicles per category and confidence.
display(
    reliability.groupBy("reliability_category", "score_confidence").count()
    .orderBy("reliability_category", "score_confidence")
)

# COMMAND ----------

# The score separates vehicles by the number of repair incidents (vehicles without history excluded).
display(
    reliability.filter(F.col("reliability_score").isNotNull())
    .groupBy("repair_incidents")
    .agg(
        F.count("*").alias("vehicles"),
        F.min("reliability_score").alias("min_score"),
        F.round(F.avg("reliability_score"), 1).alias("avg_score"),
        F.max("reliability_score").alias("max_score"),
    )
    .orderBy("repair_incidents")
)

# COMMAND ----------

# Vehicles with one failure: the kind of failure decides how much the score drops.
display(
    reliability.filter(F.col("repair_incidents") == 1)
    .withColumn("failure", F.coalesce(*[F.when(F.col(c) == 1, F.lit(code)) for code, c in FAILURE_INCIDENT_COLUMNS.items()]))
    .groupBy("failure")
    .agg(
        F.count("*").alias("vehicles"),
        F.min("reliability_score").alias("min_score"),
        F.round(F.avg("reliability_score"), 1).alias("avg_score"),
        F.max("reliability_score").alias("max_score"),
    )
    .orderBy("avg_score")
)

# COMMAND ----------

# Wear (version 3): average wear penalty by age band and mileage band, and the share of vehicles at the cap.
display(
    reliability.filter(F.col("reliability_score").isNotNull())
    .withColumn("age_band", F.when(F.col("vehicle_age_years").isNull(), "unknown")
                .when(F.col("vehicle_age_years") <= 5, "0-5").when(F.col("vehicle_age_years") <= 10, "6-10")
                .when(F.col("vehicle_age_years") <= 20, "11-20").otherwise("21+"))
    .withColumn("km_band", F.when(F.col("max_odometer_km").isNull(), "unknown")
                .when(F.col("max_odometer_km") < 100000, "<100k").when(F.col("max_odometer_km") < 200000, "100-200k")
                .otherwise("200k+"))
    .groupBy("age_band", "km_band")
    .agg(
        F.count("*").alias("vehicles"),
        F.round(F.avg("wear_penalty"), 2).alias("avg_wear_penalty"),
        F.round(F.avg((F.col("age_points") + F.col("mileage_points") >= RELIABILITY_WEIGHTS["wear_max"]).cast("int")), 3).alias("share_at_cap"),
        F.round(F.avg("reliability_score"), 1).alias("avg_score"),
    )
    .orderBy("age_band", "km_band")
)

# COMMAND ----------

# Average score per manufacturer (15 manufacturers with the most scored vehicles).
display(
    reliability.filter(F.col("reliability_score").isNotNull())
    .join(read_delta_table(gold_path("dim_vehicle", DEV_MODE)), "vehicle_key")
    .groupBy("manufacturer")
    .agg(F.count("*").alias("vehicles"), F.round(F.avg("reliability_score"), 1).alias("avg_score"))
    .orderBy(F.desc("vehicles"))
    .limit(15)
)

# COMMAND ----------

# The five lowest scores with their components.
display(reliability.filter(F.col("reliability_score").isNotNull()).orderBy("reliability_score").limit(5))

# COMMAND ----------

_ = summary_df.unpersist()
_ = reliability.unpersist()
