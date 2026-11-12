"""Tests for notebooks/00_config.py: secrets handling and Bronze partition resolution."""

import pytest

from notebook_utils import EXPECTED_SECRET_SCOPE, FAKE_SECRETS, new_namespace, read_notebook, run_notebook


@pytest.fixture()
def config(spark):
    namespace = new_namespace(spark)
    run_notebook("00_config.py", namespace)
    return namespace


def test_credentials_are_read_only_from_key_vault_scope(config):
    requests = config["dbutils"].secrets.requests
    assert {scope for scope, _ in requests} == {EXPECTED_SECRET_SCOPE}
    assert {key for _, key in requests} == {"client-id", "client-secret", "tenant-id"}


def test_no_secret_values_are_hardcoded():
    source = read_notebook("00_config.py")
    for fake_value in FAKE_SECRETS.values():
        assert fake_value not in source
    # The only hardcoded identifiers are non-secret names.
    assert 'SECRET_SCOPE = "key-vault-secrets"' in source
    assert "dbutils.secrets.get(" in source


def test_oauth_settings_use_secret_values(config, spark):
    suffix = f"{config['STORAGE_ACCOUNT']}.dfs.core.windows.net"
    assert spark.conf.get(f"fs.azure.account.auth.type.{suffix}") == "OAuth"
    assert spark.conf.get(f"fs.azure.account.oauth2.client.id.{suffix}") == FAKE_SECRETS["client-id"]
    assert FAKE_SECRETS["tenant-id"] in spark.conf.get(f"fs.azure.account.oauth2.client.endpoint.{suffix}")


def test_fourth_source_uses_real_bronze_folder_name(config):
    assert config["VEHICLE_SOURCES"]["Fourth"] == {"folder": "FourthSource", "format": "json"}


def test_first_source_is_read_with_multiline(config):
    """FirstSourceSystem.csv has bare \\r characters inside quoted descriptions."""
    assert config["VEHICLE_SOURCES"]["First"]["options"] == {"multiLine": "true"}


def test_service_book_is_read_without_multiline(config):
    """ServiceBook has \\r only in line endings; multiLine would make the 1.2 GB read single-threaded."""
    assert "options" not in config["SERVICE_BOOK_SOURCE"]


@pytest.fixture()
def bronze_with_partitions(tmp_path):
    for partition in ["ingestion_date=2026-11-01", "ingestion_date=2026-11-03", "ingestion_date=2026-11-02"]:
        (tmp_path / "FirstSourceSystem" / partition).mkdir(parents=True)
    (tmp_path / "FirstSourceSystem" / "not_a_partition").mkdir()
    (tmp_path / "EmptySource").mkdir()
    return tmp_path


def test_get_ingestion_path_latest(config, bronze_with_partitions):
    config["BRONZE_PATH"] = str(bronze_with_partitions)
    path, date = config["get_ingestion_path"]("FirstSourceSystem")
    assert date == "2026-11-03"
    assert path == f"{bronze_with_partitions}/FirstSourceSystem/ingestion_date=2026-11-03"


def test_get_ingestion_path_specific_date(config, bronze_with_partitions):
    config["BRONZE_PATH"] = str(bronze_with_partitions)
    _, date = config["get_ingestion_path"]("FirstSourceSystem", "2026-11-01")
    assert date == "2026-11-01"


def test_get_ingestion_path_unknown_date_fails(config, bronze_with_partitions):
    config["BRONZE_PATH"] = str(bronze_with_partitions)
    with pytest.raises(ValueError, match="not found"):
        config["get_ingestion_path"]("FirstSourceSystem", "2020-01-01")


def test_get_ingestion_path_without_partitions_fails(config, bronze_with_partitions):
    config["BRONZE_PATH"] = str(bronze_with_partitions)
    with pytest.raises(ValueError, match="No 'ingestion_date="):
        config["get_ingestion_path"]("EmptySource")


def test_logger_writes_timestamped_lines_to_the_current_stdout(config, capsys):
    log = config["get_logger"]("test_notebook")
    log.info("hello")
    log.warning("careful")
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].endswith("INFO    [test_notebook] hello")
    assert lines[1].endswith("WARNING [test_notebook] careful")
    assert lines[0][:4].isdigit() and lines[0][4] == "-"  # starts with the date


def test_logger_is_not_duplicated_when_config_runs_again(spark, capsys):
    """Every notebook runs %run ./00_config, so get_logger runs several times for the same name."""
    for _ in range(2):
        namespace = new_namespace(spark)
        run_notebook("00_config.py", namespace)
        log = namespace["get_logger"]("repeated_notebook")
    log.info("once")
    assert capsys.readouterr().out.count("once") == 1
    assert len(log.handlers) == 1 and log.propagate is False


def test_pipeline_notebooks_log_instead_of_print():
    """Pipeline notebooks use get_logger (timestamped lines in the Job output); profiling notebooks may print."""
    from notebook_utils import NOTEBOOKS_DIR

    pipeline_notebooks = sorted(NOTEBOOKS_DIR.glob("0[1-9]_*.py")) + [NOTEBOOKS_DIR / "utils/vehicle_listings_cleaning.py"]
    assert len(pipeline_notebooks) >= 10
    for notebook in pipeline_notebooks:
        assert "print(" not in notebook.read_text(encoding="utf-8"), f"{notebook.name} still uses print"
