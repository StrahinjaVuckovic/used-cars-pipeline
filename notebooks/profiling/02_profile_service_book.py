# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling 02 – ServiceBook (Bronze)
# MAGIC
# MAGIC Looks at the raw service history before cleaning, to decide what `notebooks/04_silver_service_events.py`
# MAGIC has to fix and which columns the Reliability Score (Gold) can rely on. Nothing is written.
# MAGIC
# MAGIC Not part of the daily pipeline. Run it on the full data (~8.3M rows, a few minutes).
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) or a `YYYY-MM-DD` Bronze partition date.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %run ../utils/silver_transformations

# COMMAND ----------

# MAGIC %run ../utils/profiling_helpers

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()

ID_PATTERN = r"^[0-9a-f]{12}$"
DATE_PATTERNS = {"yyyy-MM-dd": r"^\d{4}-\d{2}-\d{2}$"}

# Candidate natural key of one service event.
EVENT_KEY = ["car_id", "service_date", "odometer_km", "event_type"]

NUMERIC_COLUMNS = [
    "odometer_km", "prev_odometer_km", "km_since_prev", "interval_km_target", "oil_gap_km",
    "oil_gap_penalty_piece", "cost_eur", "downtime_days",
]

# COMMAND ----------

# MAGIC %md ## Read the raw file

# COMMAND ----------

path, partition_date = get_ingestion_path(SERVICE_BOOK_SOURCE["folder"], INGESTION_DATE)
raw_df = read_bronze_files(path, SERVICE_BOOK_SOURCE["format"], SERVICE_BOOK_SOURCE.get("options")).cache()
df = raw_df.drop("source_file_path")

total_rows = df.count()
vehicles = df.select("car_id").distinct().count()
print(f"{path}: {total_rows:,} rows, {vehicles:,} vehicles, {len(df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column profile, line breaks and constant columns
# MAGIC
# MAGIC All CSV columns are strings, so min/max in the column profile are alphabetical (`odometer_km` max shows
# MAGIC `999997.0`); the real numeric ranges are in the numeric summary below. The distinct count is an estimate.

# COMMAND ----------

display(column_profile(df, NULL_TOKENS))

# COMMAND ----------

display(text_anomaly_counts(df))

# COMMAND ----------

constant = constant_columns(df)
print(f"Columns with a single value (no information): {constant}")
for column_name in constant:
    display(top_values(df, column_name, n=3))

# COMMAND ----------

# MAGIC %md ## Vehicles and events per vehicle

# COMMAND ----------

malformed_ids = df.filter(~F.coalesce(F.col("car_id").rlike(ID_PATTERN), F.lit(False))).count()
print(f"car_id values not matching {ID_PATTERN}: {malformed_ids}")

events_per_vehicle = df.groupBy("car_id").agg(F.count("*").alias("events"))
display(numeric_summary(events_per_vehicle, ["events"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event categories
# MAGIC
# MAGIC How `event_type`, `severity`, `service_type_raw` and `maintenance_kind` relate to each other,
# MAGIC and what the free-text `notes_raw` contains. This is the basis for the Reliability Score.

# COMMAND ----------

display(df.groupBy("event_type", "severity").count().orderBy(F.desc("count")))

# COMMAND ----------

display(df.groupBy("service_type_raw", "event_type", "severity").count().orderBy(F.desc("count")))

# COMMAND ----------

display(top_values(df, "maintenance_kind", n=20))

# COMMAND ----------

display(top_values(df, "notes_raw", n=20))

# COMMAND ----------

flag_columns = ["is_maintenance", "is_repair", "is_inspection", "is_oil_change"]
display(df.groupBy("event_type", *flag_columns).count().orderBy("event_type", F.desc("count")))

# COMMAND ----------

# MAGIC %md ## Dates

# COMMAND ----------

display(regex_pattern_counts(df, "service_date", DATE_PATTERNS))

service_date = F.expr("try_cast(service_date AS date)")
display(df.agg(
    F.min(service_date).alias("min_date"),
    F.max(service_date).alias("max_date"),
    F.sum((service_date < F.lit("1950-01-01").cast("date")).cast("int")).alias("before_1950"),
    F.sum((service_date > F.current_date()).cast("int")).alias("in_the_future"),
    F.sum((F.col("service_date").isNotNull() & service_date.isNull()).cast("int")).alias("unparsable"),
))

# COMMAND ----------

display(
    df.groupBy(F.year(service_date).alias("year"))
    .count()
    .orderBy("year")
)

# COMMAND ----------

# MAGIC %md ## Numeric columns and odometer consistency

# COMMAND ----------

display(numeric_summary(df, NUMERIC_COLUMNS))

# COMMAND ----------

# The source already marks odometer readings that go down; check that km_since_prev agrees with the flag.
km_since_prev = F.expr("try_cast(km_since_prev AS double)")
display(
    df.groupBy(
        F.col("odometer_monotonic_flag"),
        F.when(km_since_prev < 0, "negative")
        .when(km_since_prev.isNull(), "null")
        .otherwise("zero_or_positive")
        .alias("km_since_prev"),
    )
    .count()
    .orderBy("odometer_monotonic_flag", "km_since_prev")
)

# COMMAND ----------

# Cost and downtime are filled only for some events.
display(
    df.groupBy("event_type")
    .agg(
        F.count("*").alias("events"),
        F.round(100.0 * F.avg(F.col("cost_eur").isNotNull().cast("int")), 2).alias("cost_filled_pct"),
        F.round(100.0 * F.avg(F.col("downtime_days").isNotNull().cast("int")), 2).alias("downtime_filled_pct"),
    )
    .orderBy(F.desc("events"))
)

# COMMAND ----------

# MAGIC %md ## Duplicates and derived columns

# COMMAND ----------

key_duplicates = duplicate_key_count(df, EVENT_KEY)
exact_duplicates = duplicate_key_count(df, df.columns)
print(f"Duplicates on {EVENT_KEY}: {key_duplicates}")
print(f"Exact duplicate rows (all columns): {exact_duplicates}")

duplicate_keys = df.groupBy(*EVENT_KEY).count().filter("count > 1")
display(df.join(duplicate_keys.select(*EVENT_KEY), EVENT_KEY).groupBy("event_type", "service_type_raw").count())

# COMMAND ----------

# Are the rows that share EVENT_KEY really duplicates? Count, per column, how many key groups have
# different values in it. Different cost/downtime/odometer chain = different events, which must be kept.
duplicate_rows = df.join(duplicate_keys.select(*EVENT_KEY), EVENT_KEY)
other_columns = [c for c in df.columns if c not in EVENT_KEY]
differs = duplicate_rows.groupBy(*EVENT_KEY).agg(
    *[(F.countDistinct(F.coalesce(F.col(c), F.lit("<null>"))) > 1).cast("int").alias(c) for c in other_columns]
)
differing_groups = differs.agg(*[F.sum(c).alias(c) for c in other_columns]).first().asDict()
display(spark.createDataFrame(
    [(c, differing_groups[c]) for c in other_columns if differing_groups[c]],
    "column STRING, key_groups_with_different_values LONG",
))

# COMMAND ----------

# desc_all looks like lower(service_type_raw + " " + notes_raw). If that holds for every row, it can be
# derived at any time and does not have to be stored in Silver.
expected_desc = F.lower(F.concat_ws(" ", F.col("service_type_raw"), F.col("notes_raw")))
different = df.filter(~F.coalesce(F.col("desc_all") == expected_desc, F.lit(False)))
print(f"Rows where desc_all differs from lower(service_type_raw + ' ' + notes_raw): {different.count()}")
display(different.select("service_type_raw", "notes_raw", "desc_all").limit(10))

# COMMAND ----------

# MAGIC %md ## Do the service-book vehicles exist in the vehicle listings?

# COMMAND ----------

listing_ids = None
for source in VEHICLE_SOURCES.values():
    source_path, _ = get_ingestion_path(source["folder"], INGESTION_DATE)
    ids = read_bronze_files(source_path, source["format"], source.get("options")).select(
        F.col("id").cast("string").alias("car_id")
    )
    listing_ids = ids if listing_ids is None else listing_ids.unionByName(ids)
listing_ids = listing_ids.distinct()

service_vehicles = df.select("car_id").distinct()
matched = service_vehicles.join(listing_ids, "car_id", "left_semi").count()
listings_without_service = listing_ids.join(service_vehicles, "car_id", "left_anti").count()
print(f"Service-book vehicles found in the listings: {matched:,} of {vehicles:,} ({100.0 * matched / max(vehicles, 1):.2f}%)")
print(f"Listings without any service history: {listings_without_service:,}")

# COMMAND ----------

raw_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Findings → decisions in Silver and Gold
# MAGIC
# MAGIC Results on the full data of the `2026-11-03` ingestion (see `documentation/silver_data_model.md`):
# MAGIC
# MAGIC | Finding | Decision |
# MAGIC |---|---|
# MAGIC | 8,312,490 rows, 419,641 vehicles, `car_id` = 12 hex characters, every vehicle exists in the listings | `vehicle_id` (renamed `car_id`) links to `vehicle_listings`; 8,239 listings have no service history → their Reliability Score is unknown (null), not 100 |
# MAGIC | `root_cause`, `warranty_flag`, `is_tyre` have a single value | Dropped in Silver (re-checked on every run) |
# MAGIC | `desc_all` = lower(`service_type_raw` + " " + `notes_raw`) in every row | Dropped, can be derived |
# MAGIC | `maintenance_kind` (OIL/OIL_FILTER) and `interval_km_target` (15000) are filled only for Oil Change rows (7.76% null = all other rows) | Kept, but they only repeat `service_type = OIL_CHANGE` |
# MAGIC | `service_date` is ISO, but ranges from 1900 to 2101: 38,518 before 1950, 190,481 in the future | `try_cast` to date; out of range → null + `invalid_service_date` (228,999) |
# MAGIC | `km_since_prev < 0` exactly where `odometer_monotonic_flag = 0` (1,346,300); null = first event of each vehicle (419,641) | Flag `non_monotonic_odometer`, values kept |
# MAGIC | 117 key groups on (`car_id`, `service_date`, `odometer_km`, `event_type`), all REPAIR, but 0 identical rows: they differ in `cost_eur`, `downtime_days` and the odometer chain | Two real repairs on the same day → both kept; `service_event_id` hashes all source columns |
# MAGIC | `cost_eur` and `downtime_days` filled in exactly 99,848 rows = the REPAIR events | Kept as nullable columns; no defaults in Silver |
# MAGIC | Oil Change = MAINTENANCE/LOW, Big Service = INSPECTION/LOW, repair = REPAIR/HIGH, Engine Work = INSPECTION/LOW, Body Work = UNKNOWN/MEDIUM | Reliability Score must use `service_type` (Big Service is regular maintenance, Engine Work is a failure) |
# MAGIC | `odometer_km = 0` in 378,932 rows | Kept; to be considered in Gold (e.g. first service of a new car) |
