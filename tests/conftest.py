"""
Shared pytest fixtures: a local SparkSession (Spark 4.1 + Delta + Avro, matching Databricks Runtime 18)
and a small local copy of the Bronze layer built from the files in data/.
"""

import itertools
import json
import os
import shutil
import sys
from pathlib import Path

import pytest

from notebook_utils import DATA_DIR

SAMPLE_ROWS = 1000
INGESTION_DATE = "2026-11-03"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    import pyspark
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("silver-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # Databricks Runtime 18 runs with ANSI mode on; keep it explicit so tests behave the same.
        .config("spark.sql.ansi.enabled", "true")
    )
    avro_package = f"org.apache.spark:spark-avro_2.13:{pyspark.__version__}"
    session = configure_spark_with_delta_pip(builder, extra_packages=[avro_package]).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _require_data_dir():
    if not DATA_DIR.exists():
        pytest.skip("data/ folder with the source files is not available")


def _copy_first_lines(source: Path, target: Path, line_count: int) -> None:
    """
    Copy the first `line_count` records byte for byte. Binary mode splits only on "\\n", so a bare "\\r"
    inside a quoted value (present in FirstSourceSystem.csv) stays inside its record, as in the real file.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as src, target.open("wb") as dst:
        dst.writelines(itertools.islice(src, line_count))


def _partition(bronze: Path, folder: str) -> Path:
    return bronze / folder / f"ingestion_date={INGESTION_DATE}"


# (date, quote, rate) with base EUR – real ECB rates of 2026-10-30 and 2026-11-02.
SAMPLE_RATES = [
    ("2026-10-30", "AUD", 1.6301), ("2026-10-30", "GBP", 0.8361), ("2026-10-30", "USD", 1.1162),
    ("2026-11-02", "AUD", 1.6236), ("2026-11-02", "GBP", 0.8343), ("2026-11-02", "USD", 1.1193),
]


def rates_json(rows, base: str = "EUR") -> str:
    """A response in the Frankfurter v2 format: a JSON array of {date, base, quote, rate} objects."""
    return json.dumps([{"date": d, "base": base, "quote": q, "rate": r} for d, q, r in rows])


def write_rates_partition(bronze: Path, ingestion_date: str, file_name: str, rows, base: str = "EUR") -> Path:
    """Write one exchange rate response into bronze/ExchangeRates/ingestion_date=<date>/<file_name>."""
    target = bronze / "ExchangeRates" / f"ingestion_date={ingestion_date}" / file_name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(rates_json(rows, base) if not isinstance(rows, str) else rows, encoding="utf-8")
    return target


@pytest.fixture(scope="session")
def bronze_sample(spark, tmp_path_factory) -> Path:
    """
    Local Bronze layout with SAMPLE_ROWS rows per source, mirroring the real ADLS structure:
    <Source>/ingestion_date=YYYY-MM-DD/<file>.
    """
    _require_data_dir()
    bronze = tmp_path_factory.mktemp("bronze")

    # Text formats: copy the header + first rows (the files have no embedded line breaks).
    _copy_first_lines(
        DATA_DIR / "FirstSourceSystem.csv",
        _partition(bronze, "FirstSourceSystem") / "FirstSourceSystem.csv",
        SAMPLE_ROWS + 1,
    )
    _copy_first_lines(
        DATA_DIR / "FourthSourceSystem",
        _partition(bronze, "FourthSource") / "FourthSourceSystem",
        SAMPLE_ROWS,
    )
    _copy_first_lines(
        DATA_DIR / "ServiceBook.csv",
        _partition(bronze, "ServiceBook") / "ServiceBook.csv",
        SAMPLE_ROWS + 1,
    )

    # The lookup is small: copy the whole file (all 23,848 rows) into its partition like every other source.
    lookup = _partition(bronze, "Lookup")
    lookup.mkdir(parents=True)
    shutil.copyfile(DATA_DIR / "model_to_brand_lookup.csv", lookup / "model_to_brand_lookup.csv")

    # Exchange rates: a small Frankfurter v2 response (Friday and Monday, no weekend rows like the ECB).
    write_rates_partition(bronze, INGESTION_DATE, "rates_latest.json", SAMPLE_RATES)

    # Binary formats: let Spark write a small sample in the same format.
    for folder, file_name, file_format in [
        ("SecondSourceSystem", "SecondSourceSystem.parquet", "parquet"),
        ("ThirdSourceSystem", "ThirdSourceSystem.avro", "avro"),
    ]:
        target = _partition(bronze, folder)
        (
            spark.read.format(file_format)
            .load(str(DATA_DIR / file_name))
            .limit(SAMPLE_ROWS)
            .coalesce(1)
            .write.format(file_format)
            .save(str(target))
        )

    return bronze


@pytest.fixture()
def silver_dir(tmp_path) -> Path:
    path = tmp_path / "silver"
    path.mkdir()
    return path


def catalog_database_names(spark) -> set:
    return {database.name for database in spark.catalog.listDatabases()}


@pytest.fixture()
def catalog_untouched(spark):
    """
    Fail if the test created catalog objects. On Databricks the workspace uses Unity Catalog without an
    External Location for the storage containers, so the notebooks must write path-based Delta tables only.
    """
    before = catalog_database_names(spark)
    yield
    assert catalog_database_names(spark) == before, "Notebook created a database in the catalog"


def write_service_book_partition(bronze_root: Path, csv_text: str) -> Path:
    """Create a Bronze ServiceBook partition containing the given CSV text."""
    target = _partition(bronze_root, "ServiceBook") / "ServiceBook.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(csv_text, encoding="utf-8")
    return target


@pytest.fixture()
def empty_bronze(tmp_path) -> Path:
    path = tmp_path / "bronze"
    path.mkdir()
    yield path
    shutil.rmtree(path, ignore_errors=True)
