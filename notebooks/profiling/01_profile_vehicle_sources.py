# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling 01 – Vehicle listing sources (Bronze)
# MAGIC
# MAGIC Looks at the raw vehicle listing files **before** any cleaning, to decide what the Silver layer has to
# MAGIC fix. Every rule in `notebooks/02_silver_vehicle_listings.py` and `documentation/silver_data_model.md`
# MAGIC comes from a finding in this notebook. Nothing is written: the notebook only reads Bronze.
# MAGIC
# MAGIC Not part of the daily pipeline. Run it on the full data (not a sample), because rare problems
# MAGIC (for example line breaks inside one description) only show up in the full files.
# MAGIC
# MAGIC **Widgets**
# MAGIC - `source`: `all` (profile every source and compare them) or one of `First`, `Second`, `Third`, `Fourth`.
# MAGIC - `ingestion_date`: `latest` (default) or a `YYYY-MM-DD` Bronze partition date.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %run ../utils/silver_transformations

# COMMAND ----------

# MAGIC %run ../utils/profiling_helpers

# COMMAND ----------

dbutils.widgets.dropdown("source", "all", ["all", *VEHICLE_SOURCES])
dbutils.widgets.text("ingestion_date", "latest")

SELECTED_SOURCE = dbutils.widgets.get("source")
INGESTION_DATE = dbutils.widgets.get("ingestion_date").strip()
SOURCES = list(VEHICLE_SOURCES) if SELECTED_SOURCE == "all" else [SELECTED_SOURCE]

print(f"sources={SOURCES}, ingestion_date={INGESTION_DATE}")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

# Listing ids (and ServiceBook car_ids) look like 12 lower-case hexadecimal characters.
ID_PATTERN = r"^[0-9a-f]{12}$"

# Candidate formats of posting_date. The result of this profile is the source of POSTING_DATE_FORMATS
# in 02_silver_vehicle_listings.py (one format per listing platform).
DATE_PATTERNS = {
    "yyyy-MM-dd": r"^\d{4}-\d{2}-\d{2}$",
    "MM/dd/yyyy": r"^\d{2}/\d{2}/\d{4}$",
    "dd.MM.yyyy": r"^\d{2}\.\d{2}\.\d{4}$",
    "MMMM d, yyyy": r"^[A-Za-z]+ \d{1,2}, \d{4}$",
}

CATEGORY_COLUMNS = [
    "source_system_name", "currency", "condition", "fuel", "transmission", "cylinders", "drive",
    "size", "type", "title_status", "paint_color", "state", "SourceSystemId",
]
NUMERIC_COLUMNS = ["price", "year", "odometer", "lat", "long"]

# COMMAND ----------

# MAGIC %md ## Read the raw files

# COMMAND ----------

raw_sources = {}
for source_name in SOURCES:
    source = VEHICLE_SOURCES[source_name]
    path, partition_date = get_ingestion_path(source["folder"], INGESTION_DATE)
    raw_sources[source_name] = read_bronze_files(path, source["format"], source.get("options")).cache()
    print(f"{source_name}: {path} ({source['format']}, options={source.get('options')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why FirstSourceSystem.csv is read with multiLine
# MAGIC
# MAGIC Some `description` values in the First CSV contain a bare carriage return (`\r`) inside quotes.
# MAGIC A normal CSV read treats it as the end of a line and splits the row, which creates broken rows whose
# MAGIC `id` is a piece of text. Reading with `multiLine` keeps quoted values together.
# MAGIC Result on the full file: 111,332 rows and 4,362 malformed ids without multiLine vs 106,970 and 0 with it
# MAGIC (every extra row is a broken fragment).

# COMMAND ----------

if "First" in SOURCES:
    first = VEHICLE_SOURCES["First"]
    first_path, _ = get_ingestion_path(first["folder"], INGESTION_DATE)
    comparison = []
    for label, options in [("without multiLine", None), ("with multiLine", first.get("options"))]:
        df = read_bronze_files(first_path, first["format"], options)
        malformed_ids = df.filter(~F.coalesce(F.col("id").rlike(ID_PATTERN), F.lit(False))).count()
        comparison.append((label, df.count(), malformed_ids))
    display(spark.createDataFrame(comparison, "read STRING, row_count LONG, malformed_ids LONG"))
else:
    print("First source not selected – skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile of each source
# MAGIC
# MAGIC For every selected source:
# MAGIC 1. row count and schema,
# MAGIC 2. column profile: null %, "no data" text tokens, distinct values, min/max
# MAGIC    (min/max of string columns are alphabetical and the distinct count is an estimate),
# MAGIC 3. line breaks and untrimmed text,
# MAGIC 4. key: unique and well-formed `id`,
# MAGIC 5. most frequent values of the categorical columns,
# MAGIC 6. distribution of the numeric columns (this is where the validation limits come from),
# MAGIC 7. `posting_date` formats per listing platform.

# COMMAND ----------

for source_name, df in raw_sources.items():
    print(f"===== {source_name}: {df.count():,} rows, {len(df.columns) - 1} source columns =====")
    df.printSchema()
    display(column_profile(df.drop("source_file_path"), NULL_TOKENS))
    display(text_anomaly_counts(df.drop("source_file_path")))

# COMMAND ----------

for source_name, df in raw_sources.items():
    duplicates = duplicate_key_count(df, ["id"])
    malformed_ids = df.filter(~F.coalesce(F.col("id").rlike(ID_PATTERN), F.lit(False))).count()
    print(
        f"{source_name}: duplicate ids={duplicates['duplicate_keys']}, "
        f"extra rows={duplicates['extra_rows']}, ids not matching {ID_PATTERN}={malformed_ids}"
    )

# COMMAND ----------

for source_name, df in raw_sources.items():
    for column_name in CATEGORY_COLUMNS:
        print(f"{source_name} – top values of {column_name}")
        display(top_values(df, column_name, n=15))

# COMMAND ----------

for source_name, df in raw_sources.items():
    print(f"{source_name} – numeric columns")
    display(numeric_summary(df, NUMERIC_COLUMNS))

# COMMAND ----------

for source_name, df in raw_sources.items():
    print(f"{source_name} – posting_date format per platform")
    display(regex_pattern_counts(df, "posting_date", DATE_PATTERNS, group_by=["source_system_name"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison of the sources
# MAGIC
# MAGIC Only when `source = all`: do the four sources have the same columns, do their ids overlap,
# MAGIC and which currencies and platforms appear in each file.

# COMMAND ----------

if len(raw_sources) > 1:
    column_sets = {name: set(df.columns) - {"source_file_path"} for name, df in raw_sources.items()}
    all_columns = sorted(set.union(*column_sets.values()))
    display(spark.createDataFrame(
        [(c, *[c in column_sets[name] for name in raw_sources]) for c in all_columns],
        ", ".join(["column STRING", *[f"{name} BOOLEAN" for name in raw_sources]]),
    ))

    # Only a few columns are needed for the comparison; casting to string makes the types line up
    # (the CSV source has only strings, the other formats have typed columns).
    compared = None
    for name, df in raw_sources.items():
        part = df.select(
            F.lit(name).alias("source"),
            F.col("id").cast("string").alias("id"),
            F.col("currency").cast("string").alias("currency"),
            F.col("source_system_name").cast("string").alias("platform"),
            F.col("SourceSystemId").cast("string").alias("source_system_uuid"),
        )
        compared = part if compared is None else compared.unionByName(part)

    ids_in_several_sources = (
        compared.groupBy("id").agg(F.countDistinct("source").alias("sources")).filter("sources > 1").count()
    )
    print(f"ids that appear in more than one source: {ids_in_several_sources}")

    display(compared.groupBy("source", "currency").count().orderBy("source", F.desc("count")))
    display(compared.groupBy("source", "platform").count().orderBy("source", F.desc("count")))
    display(compared.groupBy("source").agg(F.countDistinct("source_system_uuid").alias("distinct_uuids")))
else:
    print("Only one source selected – comparison skipped.")

# COMMAND ----------

for df in raw_sources.values():
    df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Findings → decisions in Silver
# MAGIC
# MAGIC Results on the full data of the `2026-11-03` ingestion (see `documentation/silver_data_model.md`):
# MAGIC
# MAGIC | Finding | Decision in `02_silver_vehicle_listings` |
# MAGIC |---|---|
# MAGIC | Same 28 columns in all four sources; CSV has only strings, the other formats are typed | Common raw types with `try_cast`, then `unionByName` into one table |
# MAGIC | 106,970 rows per source, `id` unique, no id in two sources, every id = 12 hex characters | `vehicle_id` is the primary key; structural check stops the run on a malformed id |
# MAGIC | First CSV: bare `\r` inside descriptions (111,332 rows / 4,362 malformed ids without multiLine) | First is read with `multiLine=true` |
# MAGIC | `posting_date`: one format per platform (Craigslist `MM/dd/yyyy`, CarGurus `yyyy-MM-dd`, AutoScout24 `dd.MM.yyyy`, CarsGuide `MMMM d, yyyy`) | Regex-dispatched parsing, original kept as `posting_date_raw` |
# MAGIC | `source_system_name` = listing platform, mixed inside every file; `SourceSystemId` = one UUID per file | `platform_id`/`platform_name` and `source_system`/`source_system_uuid` kept as separate columns |
# MAGIC | Currency: First is fixed per platform (CarGurus GBP, Craigslist USD, CarsGuide AUD, AutoScout24 EUR), Second EUR, Third AUD, Fourth GBP | Price stays in its original currency; conversion happens in the Silver view |
# MAGIC | Extremes: price up to billions, year up to 2100, odometer up to 10M, lat/long out of range | Out-of-range values → null + `dq_issues`; limits above the 99th percentile |
# MAGIC | `cylinders` is text (`"6 cylinders"`, `"other"`), categories in mixed case, "no data" text tokens | Number extracted from text; lower-case categories; `standardize_nulls` |
