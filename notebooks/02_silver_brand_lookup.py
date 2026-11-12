# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Silver: brand_lookup
# MAGIC
# MAGIC Cleans the brand reference `model_to_brand_lookup.csv` from one Bronze ingestion partition and overwrites
# MAGIC the Delta table `abfss://silver@sausedcarspipelinesv.dfs.core.windows.net/brand_lookup` (path-based).
# MAGIC `03_silver_vehicle_listings` joins the listings with this table to add `manufacturer`.
# MAGIC
# MAGIC The lookup has its own notebook (its own pipeline task, in parallel with the four vehicle sources), so
# MAGIC every task does one job: this one cleans the reference, `03` combines the listings and maps the brands.
# MAGIC Rules come from `notebooks/profiling/03_profile_brand_lookup.py`; data model: `documentation/silver_data_model.md`.
# MAGIC
# MAGIC **Cleaning rules**
# MAGIC - `model` is normalized like `model_normalized` in the listings (trim, single spaces, lower case) and
# MAGIC   `manufacturer` is lower-cased. This removes the 91 Title Case copies of existing models.
# MAGIC - Rows without a model or manufacturer ("NaN" and other null tokens) are removed.
# MAGIC - A model that maps to more than one manufacturer (`astra` → saturn / opel) is removed: no arbitrary pick.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `ingestion_date`: `latest` (default) or a `YYYY-MM-DD` Bronze partition date.
# MAGIC - `dev_mode`: `true` writes to `brand_lookup_dev`. The lookup is always read in full (~24k rows):
# MAGIC   a sample would leave most listings without a brand.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./utils/silver_transformations

# COMMAND ----------

# MAGIC %run ./utils/vehicle_listings_cleaning

# COMMAND ----------

dbutils.widgets.text("ingestion_date", "latest")
dbutils.widgets.dropdown("dev_mode", "true", ["true", "false"])

INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
DEV_MODE = dbutils.widgets.get("dev_mode") == "true"

TARGET_PATH = brand_lookup_path(DEV_MODE)
TABLE_NAME = "brand_lookup"

# Final column order of silver.brand_lookup (must match the documentation).
BRAND_LOOKUP_COLUMNS = [
    "model_normalized", "manufacturer", "ingestion_date", "source_file_path", "silver_processed_at",
]

log = get_logger("02_silver_brand_lookup")
log.info(f"ingestion_date={INGESTION_DATE}, dev_mode={DEV_MODE}, target={TARGET_PATH}")

# COMMAND ----------


def normalize_brand_lookup(raw_lookup_df: DataFrame) -> DataFrame:
    """Normalized (model_normalized, manufacturer) pairs without nulls and without duplicate rows."""
    return (
        standardize_nulls(raw_lookup_df.select("model", "manufacturer"))
        .select(
            normalize_model(F.col("model")).alias("model_normalized"),
            F.lower(F.col("manufacturer")).alias("manufacturer"),
        )
        .dropna()
        .distinct()
    )


def conflicting_models(lookup_df: DataFrame) -> DataFrame:
    """Normalized models that map to more than one manufacturer."""
    return (
        lookup_df.groupBy("model_normalized")
        .agg(F.sort_array(F.collect_set("manufacturer")).alias("manufacturers"))
        .filter(F.size("manufacturers") > 1)
    )


def clean_brand_lookup(raw_lookup_df: DataFrame) -> DataFrame:
    """One row per normalized model with exactly one manufacturer (conflicting models are removed)."""
    lookup_df = normalize_brand_lookup(raw_lookup_df)
    return lookup_df.join(conflicting_models(lookup_df).select("model_normalized"), "model_normalized", "left_anti")


# COMMAND ----------

# MAGIC %md ## Read Bronze and clean

# COMMAND ----------

partition_path, partition_date = get_ingestion_path(LOOKUP_SOURCE["folder"], INGESTION_DATE)
raw_df = read_bronze_files(partition_path, LOOKUP_SOURCE["format"]).cache()

# The lookup is one small file; its path is kept as lineage for every row.
source_files = sorted(row.source_file_path for row in raw_df.select("source_file_path").distinct().collect())
brand_lookup_df = (
    add_lineage_columns(clean_brand_lookup(raw_df), partition_date)
    .withColumn("source_file_path", F.lit(", ".join(source_files)))
    .select(*BRAND_LOOKUP_COLUMNS)
    .cache()
)

raw_count = raw_df.count()
normalized_count = normalize_brand_lookup(raw_df).count()
conflicts_df = conflicting_models(normalize_brand_lookup(raw_df))
lookup_count = brand_lookup_df.count()
manufacturer_count = brand_lookup_df.select("manufacturer").distinct().count()
log.info(f"{TABLE_NAME}: {partition_path} -> {raw_count} raw rows, {lookup_count} models, {manufacturer_count} manufacturers")

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

assert_columns(brand_lookup_df, BRAND_LOOKUP_COLUMNS, TABLE_NAME)
assert lookup_count > 0, f"{TABLE_NAME}: no models left after cleaning {partition_path}"
# 03 joins the listings on model_normalized: a duplicate key would multiply listings.
assert_unique(brand_lookup_df, ["model_normalized"], TABLE_NAME)
log.info(f"{TABLE_NAME}: {lookup_count} rows, checks passed")

# COMMAND ----------

# MAGIC %md ## Write to Silver (Delta, full overwrite)

# COMMAND ----------

# ~24k short rows (< 1 MB): one file.
write_delta_table(brand_lookup_df, TARGET_PATH, num_files=1)
brand_lookup_df.unpersist()
raw_df.unpersist()

silver_df = read_delta_table(TARGET_PATH)
log.info(f"Written {silver_df.count()} rows to {TARGET_PATH}")

# COMMAND ----------

# MAGIC %md ## Data quality report

# COMMAND ----------

# What the cleaning removed, step by step.
display(spark.createDataFrame(
    [
        ("raw rows", raw_count),
        ("removed: no model / manufacturer or duplicate after normalization", raw_count - normalized_count),
        ("removed: conflicting models (one model, several manufacturers)", normalized_count - lookup_count),
        ("models in brand_lookup", lookup_count),
        ("manufacturers in brand_lookup", manufacturer_count),
    ],
    "step STRING, rows LONG",
))

# COMMAND ----------

# Conflicting models (removed from the lookup; their listings get the brand from the URL or stay unknown).
display(conflicts_df)

# COMMAND ----------

display(silver_df.groupBy("manufacturer").count().orderBy(F.desc("count")))
