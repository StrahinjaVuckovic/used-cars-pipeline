# Databricks notebook source
# MAGIC %md
# MAGIC # 00 – Shared configuration
# MAGIC
# MAGIC Run from other notebooks with `%run ./00_config`.
# MAGIC
# MAGIC - Configures OAuth (service principal) access to ADLS Gen2.
# MAGIC - All credentials come from the Key Vault-backed secret scope `key-vault-secrets`.
# MAGIC   Only non-secret names (storage account, containers, scope name) are defined here.
# MAGIC - Defines storage paths (landing, bronze, silver, gold), source definitions and a helper that resolves
# MAGIC   Bronze ingestion partitions.

# COMMAND ----------

SECRET_SCOPE = "key-vault-secrets"
STORAGE_ACCOUNT = "sausedcarspipelinesv"

# Landing: where source files arrive (one folder per source, same folder names as in Bronze).
# ADF copies them from here into Bronze partitions <Source>/ingestion_date=YYYY-MM-DD/.
LANDING_PATH = f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Tables are path-based Delta tables under these containers (no catalog registration yet:
# the workspace uses Unity Catalog and has no External Location for these containers).
# Views over those paths can be registered in Unity Catalog (verified 2026-11-10); they live in this
# catalog and schema. None means "no Unity Catalog" (local tests): views are then session-scoped temp views.
UC_CATALOG = "dbw_used_cars_strahinja"
UC_SILVER_SCHEMA = "silver"
UC_GOLD_SCHEMA = "gold"

# COMMAND ----------


def configure_adls_oauth(storage_account: str, secret_scope: str) -> None:
    """Configure the Spark session to access ADLS Gen2 with the service principal stored in Key Vault."""
    client_id = dbutils.secrets.get(scope=secret_scope, key="client-id")
    client_secret = dbutils.secrets.get(scope=secret_scope, key="client-secret")
    tenant_id = dbutils.secrets.get(scope=secret_scope, key="tenant-id")

    suffix = f"{storage_account}.dfs.core.windows.net"
    spark.conf.set(f"fs.azure.account.auth.type.{suffix}", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{suffix}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{suffix}", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{suffix}", client_secret)
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{suffix}",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )


configure_adls_oauth(STORAGE_ACCOUNT, SECRET_SCOPE)

# COMMAND ----------

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)-7s [%(name)s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class _CurrentStdoutHandler(logging.StreamHandler):
    """
    Writes to whatever sys.stdout is at the moment of logging.

    A plain StreamHandler keeps the stdout object it was created with. Databricks redirects stdout for every
    cell (and pytest for every test), so such a handler would write to an old stream and the lines would not
    show up in the notebook or Job task output.
    """

    @property
    def stream(self):
        return sys.stdout

    @stream.setter
    def stream(self, value):
        pass


def get_logger(name: str) -> logging.Logger:
    """
    Logger for a notebook: timestamped `INFO` / `WARNING` lines in the notebook and Job task output.

    `%run ./00_config` runs this cell again in every notebook, so the handler is added only once per logger.
    propagate=False keeps the lines out of the Databricks driver loggers (no duplicates).
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = _CurrentStdoutHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


# COMMAND ----------

# Vehicle listing sources: logical name -> Bronze folder, file format and optional reader options.
# Note: the fourth source lives in the Bronze folder "FourthSource" (file has no extension, content is NDJSON).
# FirstSourceSystem.csv has bare carriage returns ("\r") inside quoted `description` values. Without
# multiLine, Spark treats every "\r" as a line end and splits those rows into broken extra rows.
VEHICLE_SOURCES = {
    "First": {"folder": "FirstSourceSystem", "format": "csv", "options": {"multiLine": "true"}},
    "Second": {"folder": "SecondSourceSystem", "format": "parquet"},
    "Third": {"folder": "ThirdSourceSystem", "format": "avro"},
    "Fourth": {"folder": "FourthSource", "format": "json"},
}

SERVICE_BOOK_SOURCE = {"folder": "ServiceBook", "format": "csv"}
LOOKUP_SOURCE = {"folder": "Lookup", "format": "csv"}

# Exchange rates come from the Frankfurter API (ECB rates), copied by ADF into Bronze as the raw JSON response:
# a JSON array of {"date", "base", "quote", "rate"} objects, so multiLine reads one row per array element.
EXCHANGE_RATES_SOURCE = {"folder": "ExchangeRates", "format": "json", "options": {"multiLine": "true"}}
FX_BASE_CURRENCY = "EUR"
FX_QUOTE_CURRENCIES = ["USD", "GBP", "AUD"]

INGESTION_PARTITION_PREFIX = "ingestion_date="

# COMMAND ----------


def list_ingestion_dates(source_folder: str) -> list:
    """Sorted dates of all `ingestion_date=YYYY-MM-DD` partitions of a Bronze source (fails if there are none)."""
    source_path = f"{BRONZE_PATH}/{source_folder}"
    available_dates = sorted(
        item.name.rstrip("/")[len(INGESTION_PARTITION_PREFIX):]
        for item in dbutils.fs.ls(source_path)
        if item.isDir() and item.name.startswith(INGESTION_PARTITION_PREFIX)
    )
    if not available_dates:
        raise ValueError(f"No '{INGESTION_PARTITION_PREFIX}*' partitions found in {source_path}")
    return available_dates


def get_ingestion_path(source_folder: str, ingestion_date: str = "latest") -> tuple:
    """
    Resolve the Bronze partition folder of a source.

    Returns a tuple (partition_path, ingestion_date). With ingestion_date="latest" the most recent
    `ingestion_date=YYYY-MM-DD` folder is used; otherwise the requested date must exist.
    """
    source_path = f"{BRONZE_PATH}/{source_folder}"
    available_dates = list_ingestion_dates(source_folder)

    if ingestion_date == "latest":
        selected_date = available_dates[-1]
    elif ingestion_date in available_dates:
        selected_date = ingestion_date
    else:
        raise ValueError(
            f"Ingestion date {ingestion_date} not found for {source_folder}. Available: {available_dates}"
        )

    return f"{source_path}/{INGESTION_PARTITION_PREFIX}{selected_date}", selected_date
