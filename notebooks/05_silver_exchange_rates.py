# Databricks notebook source
# MAGIC %md
# MAGIC # 05 – Silver: exchange_rates
# MAGIC
# MAGIC Builds the daily exchange rate history from the Frankfurter API responses that ADF copies into Bronze
# MAGIC (`bronze/ExchangeRates/ingestion_date=YYYY-MM-DD/*.json`) and overwrites the Delta table
# MAGIC `abfss://silver@sausedcarspipelinesv.dfs.core.windows.net/exchange_rates` (path-based).
# MAGIC `06_silver_vehicle_listings_priced` uses the latest rates to convert listing prices.
# MAGIC
# MAGIC **Source:** `GET https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,AUD&providers=ecb`
# MAGIC (official European Central Bank rates, public API without a key). The response is a JSON array with one
# MAGIC object per day and currency: `{"date": "2026-11-09", "base": "EUR", "quote": "GBP", "rate": 0.8343}`.
# MAGIC The ECB publishes rates on working days only, so weekends and holidays have no row.
# MAGIC
# MAGIC **Unlike the other sources, all Bronze partitions are read.** A vehicle file is a full snapshot (the
# MAGIC latest one is the truth), but every exchange rate response adds days to a history. The daily response holds
# MAGIC the latest day; a one-time history response (`rates_<from>_<to>.json`) holds every day since the first listing.
# MAGIC The same day can come from several responses (a Saturday run returns Friday again): the row of the latest
# MAGIC ingestion is kept.
# MAGIC
# MAGIC **Checks stop the run** (rates are reference data like the brand lookup: one wrong rate would make every
# MAGIC converted price wrong): base EUR, rate > 0, every day has exactly USD, GBP and AUD, no day after its
# MAGIC ingestion date, unique key.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) reads every partition; a `YYYY-MM-DD` date reads only the partitions
# MAGIC   up to that date (backfill: the table looks as it did on that day).
# MAGIC - `dev_mode`: `true` writes to `exchange_rates_dev`. All partitions are always read (a few thousand rows).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

TABLE_NAME = "exchange_rates_dev" if DEV_MODE else "exchange_rates"
TARGET_PATH = f"{SILVER_PATH}/{TABLE_NAME}"

RAW_RATE_COLUMNS = ["date", "base", "quote", "rate"]
# Final column order of silver.exchange_rates (must match the documentation).
EXCHANGE_RATES_COLUMNS = [
    "rate_date", "base_currency", "quote_currency", "rate",
    "ingestion_date", "source_file_path", "silver_processed_at",
]

print(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------


def select_ingestion_dates(available_dates: list, ingestion_date: str) -> list:
    """All partitions for "latest", otherwise the partitions up to the requested date (which must exist)."""
    if ingestion_date == "latest":
        return available_dates
    if ingestion_date not in available_dates:
        raise ValueError(
            f"Ingestion date {ingestion_date} not found for {EXCHANGE_RATES_SOURCE['folder']}. "
            f"Available: {available_dates}"
        )
    return [d for d in available_dates if d <= ingestion_date]


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
        typed_df, ["rate_date", "quote_currency"], [F.desc("ingestion_date"), F.desc("source_file_path")]
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

# MAGIC %md ## Read Bronze and standardize

# COMMAND ----------

available_dates = list_ingestion_dates(EXCHANGE_RATES_SOURCE["folder"])
selected_dates = select_ingestion_dates(available_dates, INGESTION_DATE)
print(f"Reading {len(selected_dates)} of {len(available_dates)} partitions: {selected_dates}")

raw_df = reduce(DataFrame.unionByName, [read_rate_partition(d) for d in selected_dates]).cache()
exchange_rates_df = standardize_rates(raw_df).select(*EXCHANGE_RATES_COLUMNS).cache()

raw_count = raw_df.count()
rates_count = exchange_rates_df.count()
print(f"{TABLE_NAME}: {raw_count} raw rows -> {rates_count} rows ({raw_count - rates_count} repeated days removed)")

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

assert_columns(exchange_rates_df, EXCHANGE_RATES_COLUMNS, TABLE_NAME)
assert rates_count > 0, f"{TABLE_NAME}: no rates in {selected_dates}"
assert_unique(exchange_rates_df, ["rate_date", "quote_currency"], TABLE_NAME)

problems = rate_problems(exchange_rates_df)
assert not any(problems.values()), f"{TABLE_NAME}: invalid exchange rates {problems}"
print(f"{TABLE_NAME}: {rates_count} rows, checks passed")

# COMMAND ----------

# MAGIC %md ## Write to Silver (Delta, full overwrite)

# COMMAND ----------

# A few thousand rows: one file.
write_delta_table(exchange_rates_df, TARGET_PATH, num_files=1)
exchange_rates_df.unpersist()
raw_df.unpersist()

silver_df = read_delta_table(TARGET_PATH)
print(f"Written {silver_df.count()} rows to {TARGET_PATH}")

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

# Rows per Bronze ingestion that survived the deduplication.
display(silver_df.groupBy("ingestion_date").count().orderBy("ingestion_date"))

# COMMAND ----------

# The latest rates (used by vw_vehicle_listings_priced).
display(
    silver_df.filter(F.col("rate_date") == silver_df.agg(F.max("rate_date")).first()[0])
    .select("rate_date", "base_currency", "quote_currency", "rate")
    .orderBy("quote_currency")
)
