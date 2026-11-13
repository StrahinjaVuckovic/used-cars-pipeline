# Databricks notebook source
# MAGIC %md
# MAGIC # 08 – Gold: views for analysts
# MAGIC
# MAGIC Runs after all Gold tables are written.
# MAGIC
# MAGIC 1. **One Unity Catalog view per Gold table:** `dbw_used_cars_strahinja.gold.<table>` = `SELECT *` from the
# MAGIC    path-based Delta table, so analysts query `gold.fact_listing` by name instead of an `abfss://` path
# MAGIC    (the containers have no External Location, so the tables themselves cannot be registered; same pattern
# MAGIC    as `silver.vw_vehicle_listings_priced`).
# MAGIC 2. **`vw_listing_current_price`:** every listing's price at the **latest** ECB rates in EUR, GBP, USD and AUD.
# MAGIC    `fact_listing` stores prices at the posting date (they never change); current prices change every day, so
# MAGIC    they are a view over `fact_listing` and `fact_exchange_rate` instead of stored columns.
# MAGIC
# MAGIC Like every path-based view, they can be queried only from a session with storage access (the project cluster
# MAGIC after `00_config`). Without Unity Catalog (local tests, `UC_CATALOG = None`) they are session temp views.
# MAGIC
# MAGIC **Widgets:** `dev_mode` (`true` registers the `_dev` tables as `<table>_dev` views), `ingestion_date` (not used).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/gold_helpers

# COMMAND ----------

dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"
SUFFIX = "_dev" if DEV_MODE else ""
CURRENT_PRICE_VIEW = f"vw_listing_current_price{SUFFIX}"

log = get_logger("08_gold_views")
log.info(f"dev_mode={DEV_MODE}, catalog={UC_CATALOG}.{UC_GOLD_SCHEMA}")

# COMMAND ----------


def view_name(name: str) -> str:
    return f"{UC_CATALOG}.{UC_GOLD_SCHEMA}.{name}" if UC_CATALOG else name


def create_view(name: str, query: str) -> None:
    if UC_CATALOG:
        spark.sql(f"CREATE OR REPLACE VIEW {view_name(name)} AS {query}")
    else:
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {view_name(name)} AS {query}")


def current_price_sql(listing_path: str, rate_path: str, currency_path: str) -> str:
    """Every listing with its price at the latest rates (cross rate through EUR)."""
    rate_columns = ",\n        ".join(
        f"max(CASE WHEN c.currency_code = '{c}' THEN r.rate_per_eur END) AS rate_{c.lower()}" for c in TARGET_CURRENCIES
    )
    price_columns = ",\n    ".join(
        f"CAST(round(f.price / lr.rate_per_eur * t.rate_{c.lower()}, 2) AS DECIMAL(18,2)) AS price_{c.lower()}_current"
        for c in TARGET_CURRENCIES
    )
    return f"""
WITH latest_rates AS (
    SELECT rate_date_key, currency_key, rate_per_eur
    FROM delta.`{rate_path}`
    WHERE rate_date_key = (SELECT max(rate_date_key) FROM delta.`{rate_path}`)
),
target_rates AS (
    SELECT
        max(r.rate_date_key) AS fx_rate_date_key,
        {rate_columns}
    FROM latest_rates AS r
    JOIN delta.`{currency_path}` AS c ON c.currency_key = r.currency_key
)
SELECT
    f.vehicle_key,
    f.currency_key,
    f.price,
    t.fx_rate_date_key,
    {price_columns}
FROM delta.`{listing_path}` AS f
CROSS JOIN target_rates AS t
LEFT JOIN latest_rates AS lr ON lr.currency_key = f.currency_key
"""


# COMMAND ----------

if UC_CATALOG:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_GOLD_SCHEMA}")

for table_name in GOLD_COLUMNS:
    path = gold_path(table_name, DEV_MODE)
    read_required_table(path, f"the Gold task that writes {table_name}")
    create_view(f"{table_name}{SUFFIX}", f"SELECT * FROM delta.`{path}`")
log.info(f"Registered {len(GOLD_COLUMNS)} table views: {[view_name(t + SUFFIX) for t in GOLD_COLUMNS]}")

create_view(
    CURRENT_PRICE_VIEW,
    current_price_sql(
        gold_path("fact_listing", DEV_MODE), gold_path("fact_exchange_rate", DEV_MODE), gold_path("dim_currency", DEV_MODE)
    ),
)

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

current = spark.table(view_name(CURRENT_PRICE_VIEW)).cache()
listing_count = read_delta_table(gold_path("fact_listing", DEV_MODE)).count()
current_count = current.count()
assert current_count == listing_count, f"{CURRENT_PRICE_VIEW}: {current_count} rows, fact_listing has {listing_count}"
fx_dates = [r[0] for r in current.select("fx_rate_date_key").distinct().collect()]
assert len(fx_dates) == 1 and fx_dates[0] is not None, f"{CURRENT_PRICE_VIEW}: no single rate date {fx_dates}"

missing = current.filter(
    F.col("price").isNotNull() & (F.col("currency_key") != UNKNOWN_KEY) & F.col("price_gbp_current").isNull()
).count()
assert missing == 0, f"{CURRENT_PRICE_VIEW}: {missing} listings with price and currency but no current price"
log.info(f"{view_name(CURRENT_PRICE_VIEW)}: {current_count} rows at the rates of {fx_dates[0]}, checks passed")

# COMMAND ----------

display(current.limit(10))

# COMMAND ----------

_ = current.unpersist()
