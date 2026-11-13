# Databricks notebook source
# MAGIC %md
# MAGIC # 08 – Gold: fact_listing
# MAGIC
# MAGIC **Grain:** one vehicle listing (`silver.vehicle_listings`; one listing = one vehicle, so `vehicle_key` is the key).
# MAGIC Foreign keys: `dim_vehicle`, `dim_date` (posting date), `dim_location`, `dim_platform`, `dim_currency`; a
# MAGIC missing natural key points to the Unknown member `-1`.
# MAGIC
# MAGIC **Prices at the posting date.** Besides the original `price`, the listing price is converted to EUR, GBP, USD and
# MAGIC AUD at the ECB rates of the **last working day on or before the posting date** (`fx_rate_date`; a Saturday
# MAGIC listing uses Friday's rates). This value never changes, so it is stored. Prices at today's rates change every
# MAGIC day and stay in a view (`vw_listing_current_price`, notebook `08_gold_views`).
# MAGIC Conversion through EUR: `price / rate(currency) * rate(target)`, EUR = 1.
# MAGIC
# MAGIC Rows with DQ issues stay (invalid values are already null in Silver); `is_valid` and `dq_issues` let analysts
# MAGIC filter them.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` reads the `_dev` tables and writes `fact_listing_dev`), `ingestion_date` (not used).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
TABLE_NAME = "fact_listing"

log = get_logger("08_gold_fact_listing")
log.info(f"dev_mode={DEV_MODE}")

# COMMAND ----------


def rates_at_posting_dates(listings_df: DataFrame, rates_df: DataFrame) -> DataFrame:
    """
    For every posting date: the last rate date on or before it (fx_rate_date) and that day's rates per target
    currency (rate_eur = 1). An as-of join done on the distinct dates only (a few thousand rows), not per listing.
    """
    rate_dates = rates_df.select("rate_date").distinct()
    posting_dates = listings_df.select("posting_date").where(F.col("posting_date").isNotNull()).distinct()
    as_of = (
        posting_dates.join(rate_dates, F.col("rate_date") <= F.col("posting_date"))
        .groupBy("posting_date").agg(F.max("rate_date").alias("fx_rate_date"))
    )
    daily_rates = (
        rates_df.groupBy("rate_date")
        .pivot("quote_currency", [c for c in TARGET_CURRENCIES if c != FX_BASE_CURRENCY])
        .agg(F.first("rate"))
        .withColumn(f"{FX_BASE_CURRENCY}", F.lit(1.0))
        .select(F.col("rate_date").alias("fx_rate_date"), *[F.col(c).alias(f"rate_{c.lower()}") for c in TARGET_CURRENCIES])
    )
    return as_of.join(daily_rates, "fx_rate_date")


def build_fact_listing(listings_df: DataFrame, rates_df: DataFrame) -> DataFrame:
    rates = F.broadcast(rates_at_posting_dates(listings_df, rates_df))
    with_rates = listings_df.join(rates, "posting_date", "left")
    # Rate of the listing's own currency (null for an unknown currency -> null converted prices).
    listing_rate = F.coalesce(*[
        F.when(F.col("currency") == c, F.col(f"rate_{c.lower()}")) for c in TARGET_CURRENCIES
    ])
    converted = [
        F.round(F.col("price") / listing_rate * F.col(f"rate_{c.lower()}"), 2).cast("decimal(18,2)")
        .alias(f"price_{c.lower()}_at_posting")
        for c in TARGET_CURRENCIES
    ]
    return with_rates.select(
        surrogate_key("vehicle_id").alias("vehicle_key"),
        date_key("posting_date").alias("posting_date_key"),
        surrogate_key(*LOCATION_KEY_COLUMNS).alias("location_key"),
        surrogate_key("platform_id").alias("platform_key"),
        surrogate_key("currency").alias("currency_key"),
        "price",
        *converted,
        "fx_rate_date",
        "odometer",
        "latitude",
        "longitude",
        "source_system",
        "is_valid",
        "dq_issues",
    ).select(*GOLD_COLUMNS[TABLE_NAME])


# COMMAND ----------

listings_df = read_required_table(silver_path("vehicle_listings", DEV_MODE), "03_silver_vehicle_listings")
rates_df = read_required_table(silver_path("exchange_rates", DEV_MODE), "05_silver_exchange_rates")

fact_listing = build_fact_listing(listings_df, rates_df).cache()

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

listings_count = listings_df.count()
row_count = fact_listing.count()
assert row_count == listings_count, f"{TABLE_NAME}: {row_count} rows, vehicle_listings has {listings_count}"
assert_unique(fact_listing, ["vehicle_key"], TABLE_NAME)

for fk, dim, dim_key in [
    ("vehicle_key", "dim_vehicle", "vehicle_key"),
    ("posting_date_key", "dim_date", "date_key"),
    ("location_key", "dim_location", "location_key"),
    ("platform_key", "dim_platform", "platform_key"),
    ("currency_key", "dim_currency", "currency_key"),
]:
    dim_df = read_required_table(gold_path(dim, DEV_MODE), f"the Gold task that writes {dim}")
    assert_foreign_key(fact_listing, fk, dim_df, dim_key, TABLE_NAME)

# A price with a currency and a posting date after the first rate must be converted.
first_rate_date = rates_df.agg(F.min("rate_date")).first()[0]
missing_conversions = fact_listing.filter(
    F.col("price").isNotNull() & (F.col("currency_key") != UNKNOWN_KEY) & F.col("fx_rate_date").isNull()
    & (F.col("posting_date_key") >= int(first_rate_date.strftime("%Y%m%d")))
).count()
assert missing_conversions == 0, f"{TABLE_NAME}: {missing_conversions} prices without a rate at the posting date"

unknown_counts = {
    fk: fact_listing.filter(F.col(fk) == UNKNOWN_KEY).count()
    for fk in ["posting_date_key", "location_key", "platform_key", "currency_key"]
}
log.info(f"{TABLE_NAME}: {row_count} rows, foreign keys checked; rows pointing to Unknown: {unknown_counts}")

path = write_gold_table(fact_listing, TABLE_NAME, DEV_MODE)
log.info(f"Written {row_count} rows to {path}")

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

# Average listing price per original currency, in every currency at the posting-date rates.
display(
    fact_listing.join(read_delta_table(gold_path("dim_currency", DEV_MODE)), "currency_key")
    .groupBy("currency_code")
    .agg(
        F.count("*").alias("listings"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        *[F.round(F.avg(f"price_{c.lower()}_at_posting"), 2).alias(f"avg_{c.lower()}_at_posting") for c in TARGET_CURRENCIES],
    )
    .orderBy("currency_code")
)

# COMMAND ----------

_ = fact_listing.unpersist()
