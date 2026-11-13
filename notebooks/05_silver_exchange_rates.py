# Databricks notebook source
# MAGIC %md
# MAGIC # 05 – Silver: exchange_rates
# MAGIC
# MAGIC Builds the daily exchange rate history from the Frankfurter API responses that ADF copies into Bronze
# MAGIC (`bronze/ExchangeRates/ingestion_date=YYYY-MM-DD/*.json`) and loads it into the Delta table
# MAGIC `abfss://silver@sausedcarspipelinesv.dfs.core.windows.net/exchange_rates` (path-based).
# MAGIC `06_silver_vehicle_listings_priced` uses the latest rates to convert listing prices.
# MAGIC
# MAGIC **Source:** `GET https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,AUD&providers=ecb`
# MAGIC (official European Central Bank rates, public API without a key). The response is a JSON array with one
# MAGIC object per day and currency: `{"date": "2026-11-09", "base": "EUR", "quote": "GBP", "rate": 0.8343}`.
# MAGIC The ECB publishes rates on working days only, so weekends and holidays have no row.
# MAGIC
# MAGIC **History, not a snapshot.** A vehicle file is a full snapshot (the latest one is the truth), but every
# MAGIC exchange rate response adds days to a history. The daily response (`rates_latest.json`) holds the **last 7
# MAGIC days** (sliding window); a one-time history response (`rates_<from>_<to>.json`) holds every day since the first
# MAGIC listing. The same day therefore comes from several responses: the row of the latest ingestion is kept.
# MAGIC The window makes gaps heal by themselves: a day missed while ADF did not run (up to ~6 days), or lost when a
# MAGIC second run on the same day overwrote `rates_latest.json`, is delivered again by the next daily call.
# MAGIC
# MAGIC **Incremental load (default).** The watermark is the latest `ingestion_date` already in the table.
# MAGIC Only the Bronze partitions from the watermark on are read and merged into the table on
# MAGIC `(rate_date, quote_currency)`:
# MAGIC - a new day is inserted;
# MAGIC - a known day is updated only by a newer ingestion (or by the same ingestion with another rate, when ADF
# MAGIC   copied the partition again on the same day). Because of the 7-day window every daily run refreshes the
# MAGIC   ~12 rows of the previous working days it delivers again (same rate, newer `ingestion_date`), so a full load
# MAGIC   and the incremental loads give the same table, lineage included;
# MAGIC - all other rows stay as they are.
# MAGIC
# MAGIC The watermark partition itself is read again (`>=`, not `>`): a second master run on the same day
# MAGIC overwrites `rates_latest.json` in the same folder, and its new content must not be skipped.
# MAGIC The first run (no table yet) is a full load.
# MAGIC
# MAGIC **Full load** (`load_mode=full`): reads every partition up to `ingestion_date` and overwrites the table.
# MAGIC Used for a backfill (the table looks as it did on that day) or to rebuild the table after a fix.
# MAGIC
# MAGIC **Checks stop the run before anything is written** (rates are reference data like the brand lookup: one
# MAGIC wrong rate would make every converted price wrong): base EUR, rate > 0, every day has exactly USD, GBP and
# MAGIC AUD, no day after its ingestion date, unique key.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) or a `YYYY-MM-DD` date: only partitions up to that date are read.
# MAGIC - `load_mode`: `incremental` (default) or `full`.
# MAGIC - `dev_mode`: `true` writes to `exchange_rates_dev`. The data is small, so nothing is sampled.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("load_mode", "incremental", ["incremental", "full"])
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
LOAD_MODE = dbutils.widgets.get("load_mode")
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

TABLE_NAME = "exchange_rates_dev" if DEV_MODE else "exchange_rates"
TARGET_PATH = f"{SILVER_PATH}/{TABLE_NAME}"

RAW_RATE_COLUMNS = ["date", "base", "quote", "rate"]
KEY_COLUMNS = ["rate_date", "quote_currency"]
# Final column order of silver.exchange_rates (must match the documentation).
EXCHANGE_RATES_COLUMNS = [
    "rate_date", "base_currency", "quote_currency", "rate",
    "ingestion_date", "source_file_path", "silver_processed_at",
]
# MERGE: a known day is updated by a newer ingestion, or by a re-copy of the same partition with another rate.
# Re-running the same partition unchanged updates nothing.
MERGE_UPDATE_CONDITION = (
    "s.ingestion_date > t.ingestion_date OR (s.ingestion_date = t.ingestion_date AND s.rate <> t.rate)"
)

log = get_logger("05_silver_exchange_rates")
log.info(f"ingestion_date={INGESTION_DATE}, load_mode={LOAD_MODE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------


def select_ingestion_dates(available_dates: list, ingestion_date: str, watermark: str = None) -> list:
    """
    The partitions to read: all of them for "latest", otherwise those up to the requested date (which must
    exist). With a watermark (incremental load) only the partitions from the watermark on.
    """
    if ingestion_date == "latest":
        dates = available_dates
    elif ingestion_date not in available_dates:
        raise ValueError(
            f"Ingestion date {ingestion_date} not found for {EXCHANGE_RATES_SOURCE['folder']}. "
            f"Available: {available_dates}"
        )
    else:
        dates = [d for d in available_dates if d <= ingestion_date]
    return [d for d in dates if d >= watermark] if watermark else dates


def current_watermark(path: str):
    """The latest ingestion_date in the table as YYYY-MM-DD, or None when the table does not exist yet."""
    if not delta_table_exists(path):
        return None
    latest = read_delta_table(path).agg(F.max("ingestion_date")).first()[0]
    return str(latest) if latest else None


def read_rate_partition(ingestion_date: str) -> DataFrame:
    """The raw API responses of one Bronze partition, with the partition date as `ingestion_date`."""
    path = f"{BRONZE_PATH}/{EXCHANGE_RATES_SOURCE['folder']}/{INGESTION_PARTITION_PREFIX}{ingestion_date}"
    raw_df = read_bronze_files(path, EXCHANGE_RATES_SOURCE["format"], EXCHANGE_RATES_SOURCE.get("options"))
    # An error response ({"status": 422, "message": ...}) or a broken file has none of the rate columns.
    missing = sorted(set(RAW_RATE_COLUMNS) - set(raw_df.columns))
    assert not missing, f"Exchange rates in {path}: missing fields {missing} (not a rates response?)"
    return raw_df.select(*RAW_RATE_COLUMNS, "source_file_path").withColumn(
        "ingestion_date", F.to_date(F.lit(ingestion_date))
    )


def standardize_rates(raw_df: DataFrame) -> DataFrame:
    """Typed rate rows; one row per day and currency (the latest ingestion wins)."""
    typed_df = raw_df.select(
        F.try_to_date(F.col("date").cast("string"), "yyyy-MM-dd").alias("rate_date"),
        F.upper(F.trim(F.col("base"))).alias("base_currency"),
        F.upper(F.trim(F.col("quote"))).alias("quote_currency"),
        F.col("rate").cast("double").alias("rate"),
        "ingestion_date",
        "source_file_path",
    )
    return deduplicate(
        typed_df, KEY_COLUMNS, [F.desc("ingestion_date"), F.desc("source_file_path")]
    ).withColumn("silver_processed_at", F.current_timestamp())


def rate_problems(rates_df: DataFrame) -> dict:
    """Number of rows (or days) that break each rule; every value must be 0."""
    expected = sorted(FX_QUOTE_CURRENCIES)
    days = rates_df.groupBy("rate_date").agg(F.sort_array(F.collect_set("quote_currency")).alias("currencies"))
    return {
        "unparsed_rate_date": rates_df.filter(F.col("rate_date").isNull()).count(),
        "wrong_base_currency": rates_df.filter(
            F.col("base_currency").isNull() | (F.col("base_currency") != FX_BASE_CURRENCY)
        ).count(),
        "rate_not_positive": rates_df.filter(F.col("rate").isNull() | (F.col("rate") <= 0)).count(),
        "rate_date_after_ingestion": rates_df.filter(F.col("rate_date") > F.col("ingestion_date")).count(),
        "days_without_all_currencies": days.filter(
            F.col("currencies") != F.array(*[F.lit(c) for c in expected])
        ).count(),
    }


# COMMAND ----------

# MAGIC %md ## Choose the partitions (watermark)

# COMMAND ----------

watermark = current_watermark(TARGET_PATH) if LOAD_MODE == "incremental" else None
if LOAD_MODE == "incremental" and watermark is None:
    log.info(f"No table at {TARGET_PATH} yet: the first run is a full load")
EFFECTIVE_LOAD_MODE = "incremental" if watermark else "full"

available_dates = list_ingestion_dates(EXCHANGE_RATES_SOURCE["folder"])
selected_dates = select_ingestion_dates(available_dates, INGESTION_DATE, watermark)
log.info(
    f"{EFFECTIVE_LOAD_MODE} load, watermark={watermark}: "
    f"reading {len(selected_dates)} of {len(available_dates)} partitions {selected_dates}"
)

if not selected_dates:
    # Incremental load with an ingestion_date before the watermark: these partitions are already loaded.
    log.warning(
        f"No partitions from the watermark {watermark} up to ingestion_date={INGESTION_DATE}: nothing loaded. "
        "Use load_mode=full for a backfill."
    )
    dbutils.notebook.exit("nothing to load")

# COMMAND ----------

# MAGIC %md ## Read Bronze and standardize

# COMMAND ----------

raw_df = reduce(DataFrame.unionByName, [read_rate_partition(d) for d in selected_dates]).cache()
batch_df = standardize_rates(raw_df).select(*EXCHANGE_RATES_COLUMNS).cache()

raw_count = raw_df.count()
rates_count = batch_df.count()
log.info(f"{TABLE_NAME}: {raw_count} raw rows -> {rates_count} rows ({raw_count - rates_count} repeated days removed)")

# COMMAND ----------

# MAGIC %md ## Checks (before anything is written)

# COMMAND ----------

assert_columns(batch_df, EXCHANGE_RATES_COLUMNS, TABLE_NAME)
assert rates_count > 0, f"{TABLE_NAME}: no rates in {selected_dates}"
assert_unique(batch_df, KEY_COLUMNS, TABLE_NAME)

problems = rate_problems(batch_df)
assert not any(problems.values()), f"{TABLE_NAME}: invalid exchange rates {problems}"
log.info(f"{TABLE_NAME}: {rates_count} rows, checks passed")

# COMMAND ----------

# MAGIC %md ## Write to Silver (Delta: MERGE, or overwrite for a full load)

# COMMAND ----------

if EFFECTIVE_LOAD_MODE == "incremental":
    load_metrics = merge_delta_table(batch_df, TARGET_PATH, KEY_COLUMNS, MERGE_UPDATE_CONDITION)
    log.info(
        f"Merged into {TARGET_PATH}: {load_metrics.get('numTargetRowsInserted', 0)} rows inserted, "
        f"{load_metrics.get('numTargetRowsUpdated', 0)} rows refreshed by a newer ingestion "
        "(days delivered again by the 7-day window, or a re-copy with another rate)"
    )
else:
    # A few thousand rows: one file.
    write_delta_table(batch_df, TARGET_PATH, num_files=1)
    load_metrics = last_operation_metrics(TARGET_PATH)
    log.info(f"Overwrote {TARGET_PATH} with {rates_count} rows")


# COMMAND ----------

# The whole table after the load: still one row per day and currency, in the documented columns.
# (unpersist is not the last line of a cell: Databricks would print the DataFrame it returns.)
batch_df.unpersist()
raw_df.unpersist()
silver_df = read_delta_table(TARGET_PATH)
assert_columns(silver_df, EXCHANGE_RATES_COLUMNS, TABLE_NAME)
assert_unique(silver_df, KEY_COLUMNS, TABLE_NAME)
table_count = silver_df.count()
log.info(f"{TABLE_NAME} now has {table_count} rows")

# COMMAND ----------

# MAGIC %md ## Data quality report

# COMMAND ----------

# Coverage: the rates must cover every day from the first posting date (2020-01-01) up to the latest ECB day.
display(
    silver_df.groupBy("quote_currency")
    .agg(
        F.count("*").alias("days"),
        F.min("rate_date").alias("first_rate_date"),
        F.max("rate_date").alias("last_rate_date"),
        F.min("rate").alias("min_rate"),
        F.max("rate").alias("max_rate"),
    )
    .orderBy("quote_currency")
)

# COMMAND ----------

# Rows per Bronze ingestion that survived the deduplication (the latest one is the watermark of the next run).
display(silver_df.groupBy("ingestion_date").count().orderBy("ingestion_date"))

# COMMAND ----------

# The latest rates (used by vw_vehicle_listings_priced).
display(
    silver_df.filter(F.col("rate_date") == silver_df.agg(F.max("rate_date")).first()[0])
    .select("rate_date", "base_currency", "quote_currency", "rate")
    .orderBy("quote_currency")
)
