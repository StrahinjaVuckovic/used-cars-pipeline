# Databricks notebook source
# MAGIC %md
# MAGIC # 08 – Gold: fact_exchange_rate
# MAGIC
# MAGIC **Grain:** one ECB working day and currency (`silver.exchange_rates`). Measure `rate_per_eur`
# MAGIC (1 EUR = rate × currency). EUR itself is added with rate 1 for every day, so an analyst can convert any price
# MAGIC with one join: `price / rate(listing currency) * rate(target currency)` on the same `rate_date_key`.
# MAGIC Foreign keys: `dim_date` (rate date), `dim_currency`.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads `exchange_rates_dev` and writes `fact_exchange_rate_dev`), `ingestion_date`
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
TABLE_NAME = "fact_exchange_rate"

log = get_logger("08_gold_fact_exchange_rate")
log.info(f"dev_mode={DEV_MODE}")

# COMMAND ----------


def build_fact_exchange_rate(rates_df: DataFrame) -> DataFrame:
    quoted = rates_df.select("rate_date", F.col("quote_currency").alias("currency"), F.col("rate").alias("rate_per_eur"))
    base = rates_df.select("rate_date").distinct().select(
        "rate_date", F.lit(FX_BASE_CURRENCY).alias("currency"), F.lit(1.0).alias("rate_per_eur")
    )
    return quoted.unionByName(base).select(
        date_key("rate_date").alias("rate_date_key"),
        surrogate_key("currency").alias("currency_key"),
        "rate_per_eur",
    )


# COMMAND ----------

rates_df = read_required_table(silver_path("exchange_rates", DEV_MODE), "05_silver_exchange_rates")
fact_exchange_rate = build_fact_exchange_rate(rates_df).cache()

rate_count = rates_df.count()
day_count = rates_df.select("rate_date").distinct().count()
row_count = fact_exchange_rate.count()
assert row_count == rate_count + day_count, f"{TABLE_NAME}: {row_count} rows, expected {rate_count} + {day_count} EUR rows"
assert_unique(fact_exchange_rate, ["rate_date_key", "currency_key"], TABLE_NAME)
assert fact_exchange_rate.filter(F.col("rate_date_key") == UNKNOWN_KEY).count() == 0, f"{TABLE_NAME}: rate without a date"

for fk, dim, dim_key in [("rate_date_key", "dim_date", "date_key"), ("currency_key", "dim_currency", "currency_key")]:
    dim_df = read_required_table(gold_path(dim, DEV_MODE), f"the Gold task that writes {dim}")
    assert_foreign_key(fact_exchange_rate, fk, dim_df, dim_key, TABLE_NAME)

path = write_gold_table(fact_exchange_rate, TABLE_NAME, DEV_MODE, num_files=1)
_ = fact_exchange_rate.unpersist()
log.info(f"Written {row_count} rows ({day_count} days x {len(CURRENCY_NAMES)} currencies) to {path}")
