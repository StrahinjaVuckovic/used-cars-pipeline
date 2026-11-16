# Databricks notebook source
# MAGIC %md
# MAGIC # 06 – Silver: data quality checks (gate before Gold)
# MAGIC
# MAGIC Runs after all Silver Jobs and checks the Silver tables **as a whole** against thresholds:
# MAGIC
# MAGIC - **error** rules: Gold would be wrong (duplicate keys, missing rows, service-book vehicles without a listing,
# MAGIC   stale exchange rates, prices without conversion). If one fails, the notebook fails **after** storing the
# MAGIC   results, so the ADF master does not start the Gold Job and Gold keeps its last correct version.
# MAGIC - **warning** rules: the source behaves differently than usual (e.g. twice as many invalid manufacture years
# MAGIC   as today). Logged and stored; the run goes on.
# MAGIC
# MAGIC Every result is **appended** to the Delta table `silver/dq_results` (one row per rule and run, with the measured
# MAGIC value, the threshold and a readable message) and exposed as the Unity Catalog view
# MAGIC `dbw_used_cars_strahinja.silver.dq_results`. The rules and the reasons for every threshold are in
# MAGIC `documentation/data_quality.md`; the engine is `utils/dq_checks`.
# MAGIC
# MAGIC The Silver notebooks keep their own `assert`s: those stop a bad write before it happens; this notebook is the
# MAGIC one place that judges the whole Silver layer, keeps the history and gates Gold.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `dev_mode`: `true` checks the `_dev` tables and writes `dq_results_dev`.
# MAGIC - `ingestion_date`: `latest` (default) or the Bronze date the Silver tables must come from.
# MAGIC - `threshold_overrides`: JSON `{"rule_name": threshold}` to change thresholds for one run (tests, a negative
# MAGIC   check); default `{}`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/vehicle_listings_cleaning

# COMMAND ----------

# MAGIC %run ./utils/dq_checks

# COMMAND ----------

import datetime
import json
import uuid

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
dbutils.widgets.text("threshold_overrides", "{}")
INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
THRESHOLD_OVERRIDES = json.loads(dbutils.widgets.get("threshold_overrides") or "{}")

SUFFIX = "_dev" if DEV_MODE else ""
RESULTS_PATH = f"{SILVER_PATH}/dq_results{SUFFIX}"
RESULTS_VIEW = f"dq_results{SUFFIX}"
PRICED_VIEW = f"vw_vehicle_listings_priced{SUFFIX}"
RUN_ID = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

# Days without an ECB rate that still count as normal (a long weekend); more means a gap in the history.
MAX_RATE_GAP_DAYS = 5
MAX_RATE_AGE_DAYS = 5

log = get_logger("06_silver_dq_checks")
log.info(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, run_id={RUN_ID}, overrides={THRESHOLD_OVERRIDES}")

# COMMAND ----------


def uc_name(name: str) -> str:
    return f"{UC_CATALOG}.{UC_SILVER_SCHEMA}.{name}" if UC_CATALOG else name


def required(path: str) -> DataFrame:
    assert delta_table_exists(path), f"No table at {path}: the Silver Jobs must run before the DQ checks"
    return read_delta_table(path)


def share(part, whole) -> float:
    return 100.0 * part / whole if whole else None


def change_pct(table: str, current: int):
    """Change of the row count against the latest earlier run, in % (0 for the first run)."""
    previous = previous_metric(RESULTS_PATH, table, "row_count")
    return 0.0 if not previous else 100.0 * abs(current - previous) / previous


def largest_rate_gap_days(rates_df: DataFrame, window_days: int = 30) -> int:
    """Largest number of calendar days between two consecutive rate dates in the last `window_days` days."""
    dates = sorted(r.rate_date for r in rates_df.select("rate_date").distinct().collect())
    if len(dates) < 2:
        return 0
    recent = [d for d in dates if d >= dates[-1] - datetime.timedelta(days=window_days)]
    return max((b - a).days for a, b in zip(recent, recent[1:])) if len(recent) > 1 else 0


# COMMAND ----------

# MAGIC %md ## Profiles (one pass per table)

# COMMAND ----------

listings = required(f"{SILVER_PATH}/vehicle_listings{SUFFIX}")
service_events = required(f"{SILVER_PATH}/service_events{SUFFIX}")
brand_lookup = required(brand_lookup_path(DEV_MODE))
rates = required(f"{SILVER_PATH}/exchange_rates{SUFFIX}")

expected_date = INGESTION_DATE if INGESTION_DATE != "latest" else list_ingestion_dates(VEHICLE_SOURCES["First"]["folder"])[-1]

lp = listings.agg(
    F.count("*").alias("rows"),
    F.countDistinct("vehicle_id").alias("distinct_ids"),
    F.sum(F.col("vehicle_id").isNull().cast("int")).alias("null_ids"),
    F.sum(F.col("manufacturer").isNull().cast("int")).alias("unknown_manufacturer"),
    F.sum(F.col("is_valid").cast("int")).alias("valid_rows"),
    F.collect_set(F.col("ingestion_date").cast("string")).alias("ingestion_dates"),
).first()
listing_issues = issue_shares(listings, lp.rows)
stage_rows = sum(required(stage_table_path(source, DEV_MODE)).count() for source in VEHICLE_SOURCES)

sp = service_events.agg(
    F.count("*").alias("rows"),
    F.countDistinct("service_event_id").alias("distinct_ids"),
    F.sum(F.col("is_repair").cast("int")).alias("repair_rows"),
    F.sum((F.col("is_repair") & F.col("cost_eur").isNotNull() & F.col("downtime_days").isNotNull()).cast("int")).alias("repair_rows_complete"),
).first()
service_issues = issue_shares(service_events, sp.rows)
service_vehicles = service_events.select("vehicle_id").distinct()
covered_vehicles = service_vehicles.join(listings.select("vehicle_id").distinct(), "vehicle_id").count()
service_vehicle_count = service_vehicles.count()

bp = brand_lookup.agg(F.count("*").alias("rows"), F.countDistinct("model_normalized").alias("distinct_models")).first()

latest_rate_date = rates.agg(F.max("rate_date")).first()[0]
latest_currencies = rates.filter(F.col("rate_date") == F.lit(latest_rate_date)).select("quote_currency").distinct().count()
listings_date = datetime.date.fromisoformat(lp.ingestion_dates[0]) if len(lp.ingestion_dates) == 1 else None
rate_gap_days = largest_rate_gap_days(rates)
rate_rows = rates.count()

priced_exists = spark.catalog.tableExists(uc_name(PRICED_VIEW))
missing_conversions = (
    spark.table(uc_name(PRICED_VIEW)).filter(
        F.col("price").isNotNull() & F.col("currency").isNotNull()
        & (F.col("price_gbp").isNull() | F.col("price_usd").isNull() | F.col("price_aud").isNull())
    ).count()
    if priced_exists else None
)

log.info(
    f"Profiles: vehicle_listings {lp.rows}, service_events {sp.rows}, brand_lookup {bp.rows}, exchange_rates {rate_rows} rows"
)

# COMMAND ----------

# MAGIC %md ## Rules

# COMMAND ----------

VL, SE, BL, FX, PV = "vehicle_listings", "service_events", "brand_lookup", "exchange_rates", "vw_vehicle_listings_priced"

RULES = [
    # ---------- errors: Gold would be wrong ----------
    dq_rule(VL, "row_count_matches_stages", "error", "the union lost or added rows compared with the four stage tables",
            lambda: abs(lp.rows - stage_rows), "==", 0),
    dq_rule(VL, "vehicle_id_duplicates", "error", "vehicle_id must identify one listing",
            lambda: lp.rows - lp.distinct_ids, "==", 0),
    dq_rule(VL, "vehicle_id_nulls", "error", "every listing needs a vehicle_id", lambda: lp.null_ids, "==", 0),
    dq_rule(VL, "ingestion_dates_not_expected", "error", f"the listings must come from one Bronze ingestion ({expected_date})",
            lambda: 0 if lp.ingestion_dates == [expected_date] else len(lp.ingestion_dates) or 1, "==", 0),
    dq_rule(SE, "service_event_id_duplicates", "error", "service_event_id must identify one event",
            lambda: sp.rows - sp.distinct_ids, "==", 0),
    # In dev mode the two samples (listings and service book) do not match by design, so it only warns there.
    dq_rule(SE, "vehicles_in_listings_share", "warning" if DEV_MODE else "error",
            "service-book vehicles must exist in the listings (dim_vehicle)",
            lambda: share(covered_vehicles, service_vehicle_count), ">=", 99.0, "%"),
    dq_rule(BL, "model_normalized_duplicates", "error", "the brand lookup key must be unique",
            lambda: bp.rows - bp.distinct_models, "==", 0),
    dq_rule(FX, "latest_rate_age_days", "error", "the latest exchange rate must be at most 5 days older than the listings",
            lambda: (listings_date - latest_rate_date).days if listings_date and latest_rate_date else None, "<=", MAX_RATE_AGE_DAYS),
    dq_rule(FX, "latest_day_currencies", "error", "the latest rate day must have USD, GBP and AUD",
            lambda: latest_currencies, "==", len(FX_QUOTE_CURRENCIES)),
    dq_rule(PV, "missing_converted_prices", "error", "every listing with price and currency must have its converted prices",
            lambda: missing_conversions, "==", 0),
    # ---------- warnings: the source behaves differently than usual (threshold ~2x the verified values) ----------
    *[dq_rule(VL, f"{issue}_share", "warning", f"share of listings flagged {issue}", (lambda i=issue: listing_issues.get(i, 0.0)), "<=", limit, "%")
      for issue, limit in [("invalid_manufacture_year", 5.0), ("invalid_latitude", 5.0), ("invalid_longitude", 5.0),
                           ("invalid_vin", 2.0), ("zero_odometer", 2.0), ("invalid_odometer", 1.0),
                           ("invalid_price", 1.0), ("invalid_currency", 1.0), ("unparsed_posting_date", 0.0)]],
    dq_rule(VL, "unknown_manufacturer_share", "warning", "share of listings whose brand could not be mapped",
            lambda: share(lp.unknown_manufacturer, lp.rows), "<=", 5.0, "%"),
    dq_rule(VL, "valid_rows_share", "warning", "share of listings without any DQ issue",
            lambda: share(lp.valid_rows, lp.rows), ">=", 90.0, "%"),
    *[dq_rule(SE, f"{issue}_share", "warning", f"share of service events flagged {issue}", (lambda i=issue: service_issues.get(i, 0.0)), "<=", limit, "%")
      for issue, limit in [("invalid_service_date", 5.0), ("invalid_odometer_km", 7.0), ("non_monotonic_odometer", 25.0),
                           ("unparsed_service_date", 0.0)]],
    dq_rule(SE, "repairs_with_cost_and_downtime_share", "warning", "repair events must carry cost and downtime (Reliability Score)",
            lambda: share(sp.repair_rows_complete, sp.repair_rows) if sp.repair_rows else 100.0, ">=", 99.0, "%"),
    dq_rule(FX, "largest_rate_gap_days", "warning", "days between consecutive ECB rates in the last 30 days (a gap in the history)",
            lambda: rate_gap_days, "<=", MAX_RATE_GAP_DAYS),
    # ---------- row counts and their change against the previous run ----------
    *[dq_rule(table, "row_count", "warning", "the table must not be empty", (lambda n=count: n), ">=", 1)
      for table, count in [(VL, lp.rows), (SE, sp.rows), (BL, bp.rows), (FX, rate_rows)]],
    *[dq_rule(table, "row_count_change_pct", "warning", "change of the row count against the previous run",
              (lambda t=table, n=count: change_pct(t, n)), "<=", 20.0, "%")
      for table, count in [(VL, lp.rows), (SE, sp.rows), (BL, bp.rows), (FX, rate_rows)]],
]

results = evaluate_rules(RULES, THRESHOLD_OVERRIDES)

# COMMAND ----------

# MAGIC %md ## Store the results (always, also when a rule failed)

# COMMAND ----------

results_df = results_dataframe(results, RUN_ID, RUN_TIMESTAMP, datetime.date.fromisoformat(expected_date))
append_delta_table(results_df, RESULTS_PATH)
if UC_CATALOG:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SILVER_SCHEMA}")
    spark.sql(f"CREATE OR REPLACE VIEW {uc_name(RESULTS_VIEW)} AS SELECT * FROM delta.`{RESULTS_PATH}`")
else:
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {RESULTS_VIEW} AS SELECT * FROM delta.`{RESULTS_PATH}`")

failed_warnings = failed_rules(results, "warning")
failed_errors = failed_rules(results, "error")
for result in failed_warnings:
    log.warning(result["message"])
log.info(
    f"{len(results)} rules checked: {len(results) - len(failed_warnings) - len(failed_errors)} passed, "
    f"{len(failed_warnings)} warnings, {len(failed_errors)} errors; results appended to {RESULTS_PATH} (run_id {RUN_ID})"
)

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

# Failed rules first, then the rest.
display(
    results_df.select("severity", "status", "table_name", "rule_name", "metric_value", "operator", "threshold", "message")
    .orderBy(F.col("status").desc(), "severity", "table_name", "rule_name")
)

# COMMAND ----------

# MAGIC %md ## Gate: fail when an error rule failed (Gold does not start)

# COMMAND ----------

assert not failed_errors, failed_errors_message(results)
log.info("All error rules passed: Silver is ready for Gold")
