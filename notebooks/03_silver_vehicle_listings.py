# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Silver: vehicle_listings
# MAGIC
# MAGIC Combines the four cleaned vehicle sources (stage tables written by `02_silver_stage_<source>_source`)
# MAGIC into one dataset, maps every listing to its manufacturer and overwrites the Delta table stored at
# MAGIC `abfss://silver@sausedcarspipelinesv.dfs.core.windows.net/vehicle_listings` (path-based, see 00_config).
# MAGIC
# MAGIC In the pipeline this task runs after the four source tasks. It never reads the source files itself.
# MAGIC
# MAGIC Data model and rules: `documentation/silver_data_model.md`.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) accepts the stage tables as they are; a `YYYY-MM-DD` date also
# MAGIC   checks that every stage table was built from that Bronze partition.
# MAGIC - `dev_mode`: `true` reads the `vehicle_listings_stage_dev` tables and writes to `vehicle_listings_dev`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/vehicle_listings_cleaning

# COMMAND ----------

import re

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

# The full table is ~800 MB, so 4 files of ~200 MB each is a good size (no small-files problem).
OUTPUT_FILES = 4

TABLE_NAME = "vehicle_listings_dev" if DEV_MODE else "vehicle_listings"
TARGET_PATH = f"{SILVER_PATH}/{TABLE_NAME}"

# Final column order of silver.vehicle_listings (must match the documentation):
# the stage columns with the brand columns right after model_normalized.
BRAND_COLUMNS = ["manufacturer", "manufacturer_source"]
_brand_position = STAGE_COLUMNS.index("model_normalized") + 1
VEHICLE_LISTINGS_COLUMNS = STAGE_COLUMNS[:_brand_position] + BRAND_COLUMNS + STAGE_COLUMNS[_brand_position:]

print(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brand mapping functions (model → manufacturer)
# MAGIC
# MAGIC Rules come from `notebooks/profiling/03_profile_brand_lookup.py`:
# MAGIC - The lookup is normalized like `model_normalized` (trim, single spaces, lower case). This removes the
# MAGIC   91 Title Case copies of existing models.
# MAGIC - A model that maps to more than one manufacturer (`astra` → saturn / opel) is removed from the lookup:
# MAGIC   no arbitrary pick.
# MAGIC - Listings whose model is not in the lookup get the manufacturer from the listing URL when the URL slug
# MAGIC   contains a known manufacturer (Craigslist URLs look like `.../d/city-2018-cadillac-xt5-crossover/...`).
# MAGIC - `manufacturer_source` says where the value came from: `lookup`, `listing_url` or null (unknown).
# MAGIC   An unknown manufacturer is missing data, not an invalid value, so it is not a DQ issue.

# COMMAND ----------


def clean_brand_lookup(raw_lookup_df: DataFrame) -> DataFrame:
    """One row per normalized model with exactly one manufacturer (conflicting models are removed)."""
    lookup_df = (
        standardize_nulls(raw_lookup_df.select("model", "manufacturer"))
        .select(
            normalize_model(F.col("model")).alias("model_normalized"),
            F.lower(F.col("manufacturer")).alias("manufacturer"),
        )
        .dropna()
        .distinct()
    )
    unambiguous = (
        lookup_df.groupBy("model_normalized").agg(F.count("*").alias("manufacturers")).filter("manufacturers = 1")
    )
    return lookup_df.join(unambiguous.select("model_normalized"), "model_normalized", "left_semi")


def manufacturer_from_url(url: Column, manufacturers: list) -> Column:
    """
    The first known manufacturer that appears as a whole word in the URL slug, or null.

    Spaces in a name are written as "-" in URLs ("land rover" -> "land-rover"). Longer names are tried first,
    so "land-rover" wins over "rover"; the leftmost match wins, which in a "year-brand-model" slug is the brand.
    """
    url_tokens = {m.replace(" ", "-"): m for m in manufacturers}
    if not url_tokens:
        return F.lit(None).cast("string")
    alternatives = "|".join(re.escape(token) for token in sorted(url_tokens, key=len, reverse=True))
    token = F.regexp_extract(F.lower(url), rf"[/\-]({alternatives})[\-/.]", 1)
    token_to_manufacturer = F.create_map(*[F.lit(x) for pair in url_tokens.items() for x in pair])
    return F.when(token != "", token_to_manufacturer[token])


def add_manufacturer(listings_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
    """Add `manufacturer` and `manufacturer_source`: lookup first, then the listing URL, otherwise null."""
    manufacturers = [row.manufacturer for row in lookup_df.select("manufacturer").distinct().collect()]
    from_url = manufacturer_from_url(F.col("listing_url"), manufacturers)
    return (
        listings_df.join(F.broadcast(lookup_df.withColumnRenamed("manufacturer", "_lookup_manufacturer")),
                         "model_normalized", "left")
        .withColumn("manufacturer", F.coalesce(F.col("_lookup_manufacturer"), from_url))
        .withColumn(
            "manufacturer_source",
            F.when(F.col("_lookup_manufacturer").isNotNull(), F.lit("lookup"))
            .when(F.col("manufacturer").isNotNull(), F.lit("listing_url")),
        )
        .drop("_lookup_manufacturer")
    )


# COMMAND ----------

# MAGIC %md ## Read the four stage tables

# COMMAND ----------

stage_tables = {}
stage_rows = []

for source_system in VEHICLE_SOURCES:
    path = stage_table_path(source_system, DEV_MODE)
    notebook = f"02_silver_stage_{source_system.lower()}_source"
    assert delta_table_exists(path), f"No stage table for {source_system} at {path}: run {notebook} first"

    stage_df = read_delta_table(path)
    dates = sorted(str(row.ingestion_date) for row in stage_df.select("ingestion_date").distinct().collect())
    stage_tables[source_system] = stage_df
    stage_rows.append((source_system, ", ".join(dates), stage_df.count(), path))

display(spark.createDataFrame(stage_rows, "source_system STRING, ingestion_date STRING, rows LONG, path STRING"))

# All four sources must come from the same Bronze ingestion, otherwise the table would mix two days.
ingestion_dates = {row[0]: row[1] for row in stage_rows}
assert len(set(ingestion_dates.values())) == 1 and "," not in stage_rows[0][1], (
    f"Stage tables come from different ingestion dates: {ingestion_dates}. "
    "Re-run the source notebooks for the same ingestion_date."
)
stage_ingestion_date = stage_rows[0][1]
if INGESTION_DATE != "latest":
    assert stage_ingestion_date == INGESTION_DATE, (
        f"Stage tables were built from ingestion_date={stage_ingestion_date}, not {INGESTION_DATE}"
    )
print(f"All stage tables come from ingestion_date={stage_ingestion_date}")

# COMMAND ----------

# MAGIC %md ## Combine the four sources into one dataset

# COMMAND ----------

vehicle_listings_df = reduce(DataFrame.unionByName, stage_tables.values())

# COMMAND ----------

# MAGIC %md ## Brand mapping

# COMMAND ----------

lookup_path = f"{BRONZE_PATH}/{LOOKUP_SOURCE['folder']}"
brand_lookup_df = clean_brand_lookup(read_bronze_files(lookup_path, LOOKUP_SOURCE["format"])).cache()
assert_unique(brand_lookup_df, ["model_normalized"], "brand lookup")
print(
    f"Brand lookup: {brand_lookup_df.count()} models, "
    f"{brand_lookup_df.select('manufacturer').distinct().count()} manufacturers ({lookup_path})"
)

vehicle_listings_df = add_manufacturer(vehicle_listings_df, brand_lookup_df).select(*VEHICLE_LISTINGS_COLUMNS)

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

# The row count also proves that the brand join did not multiply listings (the lookup key is unique).
expected_rows = sum(row[2] for row in stage_rows)
actual_rows = vehicle_listings_df.count()
assert actual_rows == expected_rows, f"Row count {actual_rows} != sum of stage tables {expected_rows}"

# vehicle_id must also be unique across sources; each source notebook only sees its own rows.
unparsed_dates = check_vehicle_listings(vehicle_listings_df, VEHICLE_LISTINGS_COLUMNS, "vehicle_listings")

print(f"vehicle_listings: {actual_rows} rows, checks passed ({unparsed_dates} unparsed posting_date values)")

# COMMAND ----------

# MAGIC %md ## Write to Silver (Delta, full overwrite)

# COMMAND ----------

write_delta_table(vehicle_listings_df, TARGET_PATH, num_files=OUTPUT_FILES)
brand_lookup_df.unpersist()

silver_df = read_delta_table(TARGET_PATH)
print(f"Written {silver_df.count()} rows to {TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Data quality report

# COMMAND ----------

display(
    silver_df.groupBy("source_system")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("is_valid").cast("int")).alias("valid_rows"),
        F.round(100 * F.avg(F.col("is_valid").cast("int")), 2).alias("valid_pct"),
    )
    .orderBy("source_system")
)

# COMMAND ----------

display(dq_issue_counts(silver_df))

# COMMAND ----------

display(null_percentages(silver_df))

# COMMAND ----------

display(silver_df.groupBy("source_system", "currency").count().orderBy("source_system", "currency"))

# COMMAND ----------

# Brand mapping coverage: where the manufacturer came from, per source.
display(
    silver_df.groupBy("source_system", "manufacturer_source")
    .count()
    .withColumn("pct", F.round(100 * F.col("count") / F.sum("count").over(Window.partitionBy("source_system")), 2))
    .orderBy("source_system", "manufacturer_source")
)

# COMMAND ----------

# Spot check: raw vs parsed posting_date per platform.
display(
    silver_df.withColumn(
        "_rn", F.row_number().over(Window.partitionBy("platform_name").orderBy("vehicle_id"))
    )
    .filter("_rn <= 5")
    .select("platform_name", "posting_date_raw", "posting_date")
)
