# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling 03 – Brand lookup (`model_to_brand_lookup.csv`)
# MAGIC
# MAGIC The vehicle listings have a `model` but no brand. The reference table maps `model → manufacturer`.
# MAGIC This notebook checks whether the lookup can be used as a clean join key and how many listings it covers.
# MAGIC Its results are the input for Silver task 3 (brand/model mapping). Nothing is written.
# MAGIC
# MAGIC Not part of the daily pipeline.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) or a `YYYY-MM-DD` Bronze partition, used for the lookup
# MAGIC   (`bronze/Lookup/ingestion_date=.../`) and for the vehicle sources.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %run ../utils/silver_transformations

# COMMAND ----------

# MAGIC %run ../utils/profiling_helpers

# COMMAND ----------

import re

dbutils.widgets.text("ingestion_date", "latest")
INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()

# One partition only: reading the Lookup/ folder itself would read every ingestion day at once.
LOOKUP_PATH, _ = get_ingestion_path(LOOKUP_SOURCE["folder"], INGESTION_DATE)


def normalize_model(column: Column) -> Column:
    """Same normalization as model_normalized in utils/vehicle_listings_cleaning: trim, single spaces, lower case."""
    return F.lower(F.regexp_replace(F.trim(column), r"\s+", " "))


# COMMAND ----------

# MAGIC %md ## Read the lookup

# COMMAND ----------

lookup_raw = read_bronze_files(LOOKUP_PATH, LOOKUP_SOURCE["format"]).drop("source_file_path").cache()
print(f"{LOOKUP_PATH}: {lookup_raw.count():,} rows, columns {lookup_raw.columns}")
display(column_profile(lookup_raw, NULL_TOKENS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Is `model` a clean, unique key?
# MAGIC
# MAGIC A lookup join only works if every model appears once and maps to exactly one manufacturer.
# MAGIC The check is done on the raw value and again after normalization, because normalization can turn
# MAGIC two different raw values (`"Civic "` and `"civic"`) into the same key.

# COMMAND ----------

text_checks = lookup_raw.agg(
    F.sum((F.col("model") != F.trim("model")).cast("int")).alias("model_untrimmed"),
    F.sum(F.col("model").rlike(r"\s{2,}").cast("int")).alias("model_multiple_spaces"),
    F.sum((F.col("model") != F.lower("model")).cast("int")).alias("model_not_lower_case"),
    F.sum((F.col("manufacturer") != F.lower(F.trim("manufacturer"))).cast("int")).alias("manufacturer_not_clean"),
)
display(text_checks)

# COMMAND ----------

lookup = lookup_raw.select(
    normalize_model(F.col("model")).alias("model_normalized"),
    F.lower(F.trim("manufacturer")).alias("manufacturer"),
    F.col("model").alias("model_raw"),
)

print(f"Duplicates on raw model: {duplicate_key_count(lookup_raw, ['model'])}")
print(f"Duplicates on normalized model: {duplicate_key_count(lookup, ['model_normalized'])}")

conflicts = (
    lookup.groupBy("model_normalized")
    .agg(F.collect_set("manufacturer").alias("manufacturers"))
    .filter(F.size("manufacturers") > 1)
)
print(f"Normalized models that map to more than one manufacturer: {conflicts.count()}")
display(conflicts.limit(20))

# COMMAND ----------

manufacturers = [row.manufacturer for row in lookup.select("manufacturer").distinct().collect() if row.manufacturer]
print(f"{len(manufacturers)} manufacturers")
display(top_values(lookup, "manufacturer", n=30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## How many listings does the lookup cover?
# MAGIC
# MAGIC The listing models are normalized exactly like in Silver and joined to the lookup.
# MAGIC Coverage is measured per row (listings) and per distinct model.

# COMMAND ----------

listings = None
for source_name, source in VEHICLE_SOURCES.items():
    path, _ = get_ingestion_path(source["folder"], INGESTION_DATE)
    part = read_bronze_files(path, source["format"], source.get("options")).select(
        F.lit(source_name).alias("source_system"),
        F.col("model").cast("string").alias("model"),
        F.col("url").cast("string").alias("listing_url"),
    )
    listings = part if listings is None else listings.unionByName(part)

# Values like "", "NaN" or "None" are "no model", the same as in Silver.
listings = standardize_nulls(listings).withColumn("model_normalized", normalize_model(F.col("model")))

# Conflicting models (several manufacturers) are left out, so the measured coverage never relies on a guess.
lookup_keys = (
    lookup.select("model_normalized", "manufacturer")
    .dropDuplicates()
    .join(conflicts.select("model_normalized"), "model_normalized", "left_anti")
)
matched_listings = listings.join(lookup_keys, "model_normalized", "left").cache()

# COMMAND ----------

display(
    matched_listings.groupBy("source_system")
    .agg(
        F.count("*").alias("listings"),
        F.sum(F.col("model").isNull().cast("int")).alias("no_model"),
        F.sum(F.col("manufacturer").isNotNull().cast("int")).alias("matched"),
        F.round(100.0 * F.avg(F.col("manufacturer").isNotNull().cast("int")), 2).alias("matched_pct"),
    )
    .orderBy("source_system")
)

# COMMAND ----------

distinct_models = matched_listings.filter(F.col("model").isNotNull()).select("model_normalized", "manufacturer").distinct()
model_count = distinct_models.count()
matched_models = distinct_models.filter(F.col("manufacturer").isNotNull()).count()
print(f"Distinct listing models: {model_count:,}, found in the lookup: {matched_models:,} "
      f"({100.0 * matched_models / max(model_count, 1):.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are the unmatched models?
# MAGIC
# MAGIC Typical reasons: the model starts with the year (`"2010 freightliner cascadia"`), a brand that is not
# MAGIC in the lookup at all, or a typo. Two possible fallbacks are measured:
# MAGIC - the first word of the model is a known manufacturer,
# MAGIC - the listing URL contains a known manufacturer (Craigslist URLs usually contain `year-brand-model`).

# COMMAND ----------

unmatched = matched_listings.filter(F.col("model").isNotNull() & F.col("manufacturer").isNull())
display(top_values(unmatched, "model_normalized", n=30))

# COMMAND ----------

# Longest names first, so "mercedes-benz" wins over a shorter name that is part of it.
brand_regex = "|".join(re.escape(m) for m in sorted(manufacturers, key=len, reverse=True)) or "(?!)"
first_word = F.element_at(F.split("model_normalized", " "), 1)
url_brand = F.regexp_extract(F.lower("listing_url"), rf"[/\-]({brand_regex})[\-/.]", 1)

fallbacks = unmatched.select(
    first_word.isin(manufacturers).alias("first_word_is_brand"),
    first_word.rlike(r"^(19|20)\d{2}$").alias("starts_with_year"),
    (url_brand != "").alias("brand_in_url"),
)
display(fallbacks.groupBy("first_word_is_brand", "starts_with_year", "brand_in_url").count().orderBy(F.desc("count")))

# COMMAND ----------

display(
    unmatched.select("model_normalized", url_brand.alias("brand_from_url"), "listing_url")
    .filter(F.col("brand_from_url") != "")
    .limit(20)
)

# COMMAND ----------

lookup_raw.unpersist()
matched_listings.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Findings → decisions in Silver task 3
# MAGIC
# MAGIC Results on the full data of the `2026-11-03` ingestion. Implemented in `02_silver_brand_lookup`
# MAGIC (`clean_brand_lookup`) and `03_silver_vehicle_listings` (`add_manufacturer`):
# MAGIC
# MAGIC | Finding | Decision |
# MAGIC |---|---|
# MAGIC | 23,848 rows, no empty values, no duplicate raw model; 95 rows in Title Case, 91 of them copies of a lower-case row | Normalize model and manufacturer (trim, single spaces, lower case) and deduplicate → 45 manufacturers (64 before normalization) |
# MAGIC | One conflict after normalization: `astra` → saturn and opel | Removed from the lookup (no arbitrary pick); such listings can still get a brand from the URL |
# MAGIC | Exact join covers 96.26–96.52% of listings per source; 23,756 of 28,569 distinct listing models are found (the lookup was built from these listings) | Lookup join is the primary source: `manufacturer_source = 'lookup'` |
# MAGIC | Unmatched models: brands missing from the lookup (scion, freightliner, hummer, international, isuzu, genesis, maserati, ...), models starting with a year, and model texts with a cut-off start (`lac xt5` = cadillac, `des-benz cla` = mercedes-benz) | Fallback: known manufacturer in the Craigslist URL slug (3,083 listings) → `manufacturer_source = 'listing_url'`; otherwise null |
