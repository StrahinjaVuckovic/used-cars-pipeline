# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Validate landing and Bronze
# MAGIC
# MAGIC Checks the first two storage areas before any Silver processing:
# MAGIC - **landing** (`landing` container): where source files arrive, one folder per source;
# MAGIC - **Bronze** (`bronze` container): ADF copies every landing file into `<Source>/ingestion_date=YYYY-MM-DD/`
# MAGIC   (the static lookup goes to `Lookup/` without a date partition).
# MAGIC
# MAGIC For every source it shows the landing files and the latest Bronze partition, and checks that the latest
# MAGIC Bronze copy has the same total size as the landing files (a complete copy). Nothing is written.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Every source folder, in the same order as the ADF copy activities. Landing and Bronze use the same names.
SOURCE_FOLDERS = [
    *[source["folder"] for source in VEHICLE_SOURCES.values()],
    SERVICE_BOOK_SOURCE["folder"],
    LOOKUP_SOURCE["folder"],
]
# Sources without a date partition in Bronze (static reference data).
UNPARTITIONED_FOLDERS = [LOOKUP_SOURCE["folder"]]

# COMMAND ----------


def list_files(path: str) -> list:
    """Files (not folders) directly inside `path`; an empty list if the path does not exist."""
    try:
        return [item for item in dbutils.fs.ls(path) if not item.isDir()]
    except Exception:  # dbutils raises a Java FileNotFoundException wrapper for a missing path
        return []


def size_mb(files: list) -> float:
    return round(sum(f.size for f in files) / (1024 * 1024), 2)


def latest_bronze_files(folder: str) -> tuple:
    """(partition label, files) of the latest Bronze copy of a source."""
    if folder in UNPARTITIONED_FOLDERS:
        return "(no partition)", list_files(f"{BRONZE_PATH}/{folder}")
    try:
        partition_path, partition_date = get_ingestion_path(folder, "latest")
    except Exception:  # no Bronze folder or no ingestion_date partition yet
        return None, []
    return f"ingestion_date={partition_date}", list_files(partition_path)


def storage_overview() -> list:
    """One row per source: landing files and the latest Bronze copy, with a size comparison."""
    rows = []
    for folder in SOURCE_FOLDERS:
        landing_files = list_files(f"{LANDING_PATH}/{folder}")
        partition, bronze_files = latest_bronze_files(folder)
        rows.append((
            folder,
            ", ".join(f.name for f in landing_files),
            size_mb(landing_files),
            partition,
            ", ".join(f.name for f in bronze_files),
            size_mb(bronze_files),
            bool(landing_files) and bool(bronze_files) and size_mb(landing_files) == size_mb(bronze_files),
        ))
    return rows


# COMMAND ----------

# MAGIC %md ## Overview

# COMMAND ----------

overview = storage_overview()
display(spark.createDataFrame(
    overview,
    "source STRING, landing_files STRING, landing_mb DOUBLE, bronze_partition STRING, "
    "bronze_files STRING, bronze_mb DOUBLE, sizes_match BOOLEAN",
))

# COMMAND ----------

# MAGIC %md ## Bronze partitions per source (ingestion history)

# COMMAND ----------

for folder in SOURCE_FOLDERS:
    if folder in UNPARTITIONED_FOLDERS:
        continue
    try:
        partitions = sorted(i.name.rstrip("/") for i in dbutils.fs.ls(f"{BRONZE_PATH}/{folder}") if i.isDir())
    except Exception:
        partitions = []
    print(f"{folder}: {partitions}")

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

missing_in_landing = [row[0] for row in overview if not row[1]]
missing_in_bronze = [row[0] for row in overview if not row[4]]
size_mismatch = [row[0] for row in overview if row[1] and row[4] and not row[6]]

assert not missing_in_landing, f"No files in landing for: {missing_in_landing}"
assert not missing_in_bronze, f"No Bronze copy for: {missing_in_bronze} (run the ADF pipeline pl_ingest_bronze_sources)"
assert not size_mismatch, f"Latest Bronze copy differs in size from landing for: {size_mismatch}"
print(f"All {len(SOURCE_FOLDERS)} sources are in landing and have a complete latest Bronze copy.")
