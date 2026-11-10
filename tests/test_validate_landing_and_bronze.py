"""Tests for notebooks/01_validate_landing_and_bronze.py."""

import pytest

from conftest import INGESTION_DATE
from notebook_utils import load_config_and_utils, run_notebook

NOTEBOOK = "01_validate_landing_and_bronze.py"

FILES = {
    "FirstSourceSystem": "FirstSourceSystem.csv",
    "SecondSourceSystem": "SecondSourceSystem.parquet",
    "ThirdSourceSystem": "ThirdSourceSystem.avro",
    "FourthSource": "FourthSourceSystem",
    "ServiceBook": "ServiceBook.csv",
    "Lookup": "model_to_brand_lookup.csv",
}


def build_storage(root, bronze_overrides=None, skip_landing=()):
    """Local landing + Bronze layout; every file has the same content in both areas unless overridden."""
    landing, bronze = root / "landing", root / "bronze"
    for folder, file_name in FILES.items():
        content = f"data of {folder}".encode()
        if folder not in skip_landing:
            (landing / folder).mkdir(parents=True)
            (landing / folder / file_name).write_bytes(content)
        target = bronze / folder / f"ingestion_date={INGESTION_DATE}"
        target.mkdir(parents=True)
        (target / file_name).write_bytes((bronze_overrides or {}).get(folder, content))
    return landing, bronze


VEHICLE_PIPELINE_FOLDERS = {
    "FirstSourceSystem", "SecondSourceSystem", "ThirdSourceSystem", "FourthSource", "Lookup",
}


def run_validation(spark, tmp_path, landing, bronze, sources=None):
    widget_values = {"sources": sources} if sources else None
    namespace = load_config_and_utils(
        spark, bronze_path=str(bronze), silver_path=str(tmp_path / "silver"), widget_values=widget_values
    )
    namespace["LANDING_PATH"] = str(landing)
    return run_notebook(NOTEBOOK, namespace)


def test_complete_copy_passes(spark, tmp_path):
    landing, bronze = build_storage(tmp_path)
    namespace = run_validation(spark, tmp_path, landing, bronze)
    rows = {row[0]: row for row in namespace["overview"]}
    assert set(rows) == set(FILES)
    assert all(row[6] for row in rows.values())  # sizes_match
    assert rows["Lookup"][3] == f"ingestion_date={INGESTION_DATE}"
    assert rows["ServiceBook"][3] == f"ingestion_date={INGESTION_DATE}"


def test_lookup_without_date_partition_fails(spark, tmp_path):
    """The old Bronze layout (file directly in Lookup/, no ingestion_date folder) is no longer accepted."""
    landing, bronze = build_storage(tmp_path)
    partition = bronze / "Lookup" / f"ingestion_date={INGESTION_DATE}"
    (partition / FILES["Lookup"]).rename(bronze / "Lookup" / FILES["Lookup"])
    partition.rmdir()
    with pytest.raises(AssertionError, match="No Bronze copy for: \\['Lookup'\\]"):
        run_validation(spark, tmp_path, landing, bronze, sources="vehicle_listings")


def test_missing_landing_file_fails(spark, tmp_path):
    landing, bronze = build_storage(tmp_path, skip_landing=("ServiceBook",))
    with pytest.raises(AssertionError, match="No files in landing for: \\['ServiceBook'\\]"):
        run_validation(spark, tmp_path, landing, bronze)


def test_incomplete_bronze_copy_fails(spark, tmp_path):
    landing, bronze = build_storage(tmp_path, bronze_overrides={"FourthSource": b"x" * 2_000_000})
    with pytest.raises(AssertionError, match="differs in size from landing for: \\['FourthSource'\\]"):
        run_validation(spark, tmp_path, landing, bronze)


def test_vehicle_listings_checks_only_vehicle_sources_and_lookup(spark, tmp_path):
    # ServiceBook belongs to another pipeline, so a missing ServiceBook file must not stop this check.
    landing, bronze = build_storage(tmp_path, skip_landing=("ServiceBook",))
    namespace = run_validation(spark, tmp_path, landing, bronze, sources="vehicle_listings")
    assert {row[0] for row in namespace["overview"]} == VEHICLE_PIPELINE_FOLDERS


def test_vehicle_listings_fails_without_the_lookup(spark, tmp_path):
    landing, bronze = build_storage(tmp_path, skip_landing=("Lookup",))
    with pytest.raises(AssertionError, match="No files in landing for: \\['Lookup'\\]"):
        run_validation(spark, tmp_path, landing, bronze, sources="vehicle_listings")


def test_service_book_checks_only_the_service_book(spark, tmp_path):
    landing, bronze = build_storage(tmp_path, skip_landing=("FirstSourceSystem", "Lookup"))
    namespace = run_validation(spark, tmp_path, landing, bronze, sources="service_book")
    assert [row[0] for row in namespace["overview"]] == ["ServiceBook"]


def test_unknown_sources_value_fails(spark, tmp_path):
    landing, bronze = build_storage(tmp_path)
    with pytest.raises(AssertionError, match="Unknown sources 'vehicles'"):
        run_validation(spark, tmp_path, landing, bronze, sources="vehicles")


def test_landing_path_is_its_own_container(spark):
    namespace = load_config_and_utils(spark, bronze_path="unused", silver_path="unused")
    assert namespace["LANDING_PATH"] == "abfss://landing@sausedcarspipelinesv.dfs.core.windows.net"
