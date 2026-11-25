"""
Run the Databricks notebooks of the CDC demo locally: a local SparkSession (Spark 4.1 + Delta, like Databricks
Runtime 18) and the test harness in tests/notebook_utils.py, which replaces `spark`, `dbutils` and `display` and
points the storage paths to cdc_data/ instead of ADLS. The notebooks run unchanged.
"""

import os
import shutil
import sys
from datetime import date

from cdc import settings

sys.path.insert(0, str(settings.PROJECT_ROOT / "tests"))
from notebook_utils import load_config_and_utils, run_notebook  # noqa: E402

UTILS = [
    "utils/vehicle_listings_cleaning.py",
    "utils/gold_helpers.py",
    "utils/scd2_helpers.py",
    "utils/cdc_helpers.py",
    "utils/incremental_helpers.py",
]


def spark_session():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[4]")
        .appName("cdc-demo")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")  # as on Databricks Runtime 18
        .config("spark.driver.memory", "3g")
        .config("spark.databricks.delta.snapshotPartitions", "4")
        # Local only: pyspark 4.1.4 cannot read the checkpoint files delta-spark 4.1.0 writes after 10 commits
        # (NoSuchMethodError in ParquetToSparkSchemaConverter); Databricks is not affected.
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", "1000")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def namespace(spark, widgets: dict = None, bronze=settings.BRONZE_DIR, silver=settings.SILVER_DIR,
              gold=settings.GOLD_DIR, landing=None) -> dict:
    """A notebook namespace with 00_config and all utils loaded and the paths pointing to local folders."""
    ns = load_config_and_utils(spark, str(bronze), str(silver), {"dev_mode": "false", **(widgets or {})})
    ns["GOLD_PATH"] = str(gold)
    if landing is not None:
        ns["LANDING_PATH"] = str(landing)
    for utils in UTILS:
        run_notebook(utils, ns)
    return ns


def run(spark, notebook: str, widgets: dict = None, **paths) -> dict:
    """Run one notebook in a fresh namespace (like a Databricks Job task) and return the namespace."""
    return run_notebook(notebook, namespace(spark, widgets, **paths))


def build_brand_lookup(spark, bronze=settings.BRONZE_DIR, silver=settings.SILVER_DIR, lookup_file=settings.LOOKUP_FILE):
    """Silver brand_lookup for the demo: the lookup file in a local Bronze partition, cleaned by 02_silver_brand_lookup."""
    partition = bronze / "Lookup" / f"ingestion_date={date.today().isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(lookup_file, partition / lookup_file.name)
    run(spark, "02_silver_brand_lookup.py", bronze=bronze, silver=silver)


def run_pipeline(spark, **paths) -> dict:
    """Silver (apply the new changes) and Gold (rebuild), as one daily run would."""
    silver_ns = run(spark, "10_silver_listing_changes.py", **paths)
    run(spark, "11_gold_cdc_vehicle_history.py", **paths)
    return {"silver_exit": silver_ns.get("notebook_exit_value")}
