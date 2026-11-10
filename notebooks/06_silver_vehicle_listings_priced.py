# Databricks notebook source
# MAGIC %md
# MAGIC # 06 – Silver view: vw_vehicle_listings_priced
# MAGIC
# MAGIC Creates (or replaces) the view `dbw_used_cars_strahinja.silver.vw_vehicle_listings_priced`: every row of
# MAGIC `vehicle_listings` with its price converted to **GBP, USD and AUD** at the **latest** exchange rates of
# MAGIC `exchange_rates` (written by `05_silver_exchange_rates`).
# MAGIC
# MAGIC A view stores no data: it runs its query every time it is read. When `05` loads new rates, the view shows
# MAGIC the new prices without rewriting any table. This is the "Silver view refreshed daily with updated rates" of
# MAGIC the brief. The prices in the original currency stay in `vehicle_listings`.
# MAGIC
# MAGIC **Conversion** (all rates are "1 EUR = rate × currency", EUR itself is 1):
# MAGIC `price_<target> = round(price / rate(currency) * rate(target), 2)`.
# MAGIC Example: 20,000 AUD with AUD 1.6236 and GBP 0.8343 → 20,000 / 1.6236 × 0.8343 = 10,277.16 GBP.
# MAGIC A listing without price or currency gets null prices.
# MAGIC
# MAGIC The view reads the tables by path, so it can only be queried from a session that has access to the storage
# MAGIC account (the project cluster after `00_config`). Registering the tables needs a Unity Catalog External
# MAGIC Location (see `documentation/silver_data_model.md`).
# MAGIC
# MAGIC **Checks** (on the view): same rows as `vehicle_listings`, no missing converted price where price and
# MAGIC currency exist, one EUR listing recomputed, and the rates are at most `MAX_FX_RATE_AGE_DAYS` older than the
# MAGIC listings' `ingestion_date` (otherwise the exchange rate API has not delivered for too long).
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default); a `YYYY-MM-DD` date also checks that `vehicle_listings` was built
# MAGIC   from that Bronze partition.
# MAGIC - `dev_mode`: `true` reads `vehicle_listings_dev` / `exchange_rates_dev` and creates
# MAGIC   `vw_vehicle_listings_priced_dev`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

import datetime

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

suffix = "_dev" if DEV_MODE else ""
LISTINGS_PATH = f"{SILVER_PATH}/vehicle_listings{suffix}"
RATES_PATH = f"{SILVER_PATH}/exchange_rates{suffix}"
VIEW_BASE_NAME = f"vw_vehicle_listings_priced{suffix}"
# Unity Catalog view on Databricks; a session temp view when there is no catalog (local tests).
VIEW_NAME = f"{UC_CATALOG}.{UC_SILVER_SCHEMA}.{VIEW_BASE_NAME}" if UC_CATALOG else VIEW_BASE_NAME

TARGET_CURRENCIES = ["GBP", "USD", "AUD"]
PRICE_COLUMNS = [f"price_{c.lower()}" for c in TARGET_CURRENCIES]
# ECB publishes no rates on weekends and holidays (Easter: Friday to Monday), so up to 5 days is normal.
MAX_FX_RATE_AGE_DAYS = 5

print(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, view={VIEW_NAME}")

# COMMAND ----------


def priced_listings_sql(listings_path: str, rates_path: str) -> str:
    """The view query: every listing with its price at the latest rates in GBP, USD and AUD."""
    rate_columns = ",\n        ".join(
        f"max(CASE WHEN currency = '{c}' THEN rate END) AS rate_{c.lower()}" for c in TARGET_CURRENCIES
    )
    price_columns = ",\n    ".join(
        f"CAST(round(l.price / r.rate * p.rate_{c.lower()}, 2) AS DECIMAL(18,2)) AS price_{c.lower()}"
        for c in TARGET_CURRENCIES
    )
    return f"""
WITH latest_rates AS (
    SELECT rate_date, quote_currency AS currency, rate
    FROM delta.`{rates_path}`
    WHERE rate_date = (SELECT max(rate_date) FROM delta.`{rates_path}`)
    UNION ALL
    -- The base currency: 1 EUR = 1 EUR.
    SELECT max(rate_date), '{FX_BASE_CURRENCY}', CAST(1.0 AS DOUBLE) FROM delta.`{rates_path}`
),
target_rates AS (
    SELECT
        max(rate_date) AS fx_rate_date,
        {rate_columns}
    FROM latest_rates
)
SELECT
    l.*,
    p.fx_rate_date,
    {price_columns}
FROM delta.`{listings_path}` AS l
CROSS JOIN target_rates AS p
LEFT JOIN latest_rates AS r ON r.currency = l.currency
"""


def create_priced_view(view_name: str, listings_path: str, rates_path: str) -> None:
    """Create or replace the view (a Unity Catalog view, or a temp view when UC_CATALOG is None)."""
    query = priced_listings_sql(listings_path, rates_path)
    if UC_CATALOG:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SILVER_SCHEMA}")
        spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS {query}")
    else:
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {view_name} AS {query}")


# COMMAND ----------

# MAGIC %md ## Inputs

# COMMAND ----------

assert delta_table_exists(LISTINGS_PATH), f"No table at {LISTINGS_PATH}: run 03_silver_vehicle_listings first"
assert delta_table_exists(RATES_PATH), f"No table at {RATES_PATH}: run 05_silver_exchange_rates first"

listings_df = read_delta_table(LISTINGS_PATH)
listing_dates = sorted(str(r.ingestion_date) for r in listings_df.select("ingestion_date").distinct().collect())
assert len(listing_dates) == 1, f"vehicle_listings has several ingestion dates: {listing_dates}"
listings_ingestion_date = listing_dates[0]
if INGESTION_DATE != "latest":
    assert listings_ingestion_date == INGESTION_DATE, (
        f"vehicle_listings was built from ingestion_date={listings_ingestion_date}, not {INGESTION_DATE}"
    )
listings_count = listings_df.count()
print(f"vehicle_listings: {listings_count} rows from ingestion_date={listings_ingestion_date}")

# COMMAND ----------

# MAGIC %md ## Create the view

# COMMAND ----------

create_priced_view(VIEW_NAME, LISTINGS_PATH, RATES_PATH)
priced_df = spark.table(VIEW_NAME).cache()
print(f"Created view {VIEW_NAME}")

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

priced_count = priced_df.count()
assert priced_count == listings_count, f"{VIEW_NAME}: {priced_count} rows, vehicle_listings has {listings_count}"
assert priced_df.columns == listings_df.columns + ["fx_rate_date", *PRICE_COLUMNS], f"{VIEW_NAME}: unexpected columns"

# The view reads only the latest rates, so there is one rate date.
fx_rate_date = priced_df.select("fx_rate_date").distinct().collect()
assert len(fx_rate_date) == 1 and fx_rate_date[0][0] is not None, f"{VIEW_NAME}: no single fx_rate_date {fx_rate_date}"
fx_rate_date = fx_rate_date[0][0]
rate_age_days = (datetime.date.fromisoformat(listings_ingestion_date) - fx_rate_date).days
assert rate_age_days <= MAX_FX_RATE_AGE_DAYS, (
    f"The latest exchange rates are from {fx_rate_date}, {rate_age_days} days before the listings' ingestion "
    f"date {listings_ingestion_date} (max {MAX_FX_RATE_AGE_DAYS}). Check the ADF copy from the exchange rate API."
)

missing_prices = priced_df.filter(
    F.col("price").isNotNull() & F.col("currency").isNotNull()
    & F.greatest(*[F.col(c).isNull() for c in PRICE_COLUMNS])
).count()
assert missing_prices == 0, f"{VIEW_NAME}: {missing_prices} listings with price and currency but no converted price"

# One EUR listing recomputed by hand: price_gbp = price × rate(GBP).
eur_row = priced_df.filter((F.col("currency") == "EUR") & F.col("price").isNotNull()).select("price", "price_gbp").first()
if eur_row is not None:
    gbp_rate = (
        read_delta_table(RATES_PATH)
        .filter((F.col("rate_date") == F.lit(fx_rate_date)) & (F.col("quote_currency") == "GBP"))
        .first().rate
    )
    expected = round(float(eur_row.price) * gbp_rate, 2)
    assert abs(float(eur_row.price_gbp) - expected) <= 0.01, f"{VIEW_NAME}: EUR check {eur_row} vs {expected}"

print(f"{VIEW_NAME}: {priced_count} rows at the rates of {fx_rate_date} ({rate_age_days} days old), checks passed")

# COMMAND ----------

# MAGIC %md ## Report

# COMMAND ----------

# Average price per original currency, converted to every target currency.
display(
    priced_df.groupBy("currency")
    .agg(
        F.count("*").alias("listings"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        *[F.round(F.avg(c), 2).alias(f"avg_{c}") for c in PRICE_COLUMNS],
    )
    .orderBy("currency")
)

# COMMAND ----------

display(priced_df.select("vehicle_id", "price", "currency", "fx_rate_date", *PRICE_COLUMNS).limit(10))

# COMMAND ----------

priced_df.unpersist()
