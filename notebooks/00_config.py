# Databricks notebook source
# MAGIC %md
# MAGIC # 00 – Shared configuration
# MAGIC
# MAGIC Run from other notebooks with `%run ./00_config`.
# MAGIC
# MAGIC - Configures OAuth (service principal) access to ADLS Gen2.
# MAGIC - All credentials come from the Key Vault-backed secret scope `key-vault-secrets`.
# MAGIC   Only non-secret names (storage account, containers, scope name) are defined here.
# MAGIC - Defines storage paths, source definitions and a helper that resolves Bronze ingestion partitions.

# COMMAND ----------

SECRET_SCOPE = "key-vault-secrets"
STORAGE_ACCOUNT = "sausedcarspipelinesv"

BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Tables are path-based Delta tables under these containers (no catalog registration yet:
# the workspace uses Unity Catalog and has no External Location for these containers).

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

INGESTION_PARTITION_PREFIX = "ingestion_date="

# COMMAND ----------


def get_ingestion_path(source_folder: str, ingestion_date: str = "latest") -> tuple:
    """
    Resolve the Bronze partition folder of a source.

    Returns a tuple (partition_path, ingestion_date). With ingestion_date="latest" the most recent
    `ingestion_date=YYYY-MM-DD` folder is used; otherwise the requested date must exist.
    """
    source_path = f"{BRONZE_PATH}/{source_folder}"
    available_dates = sorted(
        item.name.rstrip("/")[len(INGESTION_PARTITION_PREFIX):]
        for item in dbutils.fs.ls(source_path)
        if item.isDir() and item.name.startswith(INGESTION_PARTITION_PREFIX)
    )
    if not available_dates:
        raise ValueError(f"No '{INGESTION_PARTITION_PREFIX}*' partitions found in {source_path}")

    if ingestion_date == "latest":
        selected_date = available_dates[-1]
    elif ingestion_date in available_dates:
        selected_date = ingestion_date
    else:
        raise ValueError(
            f"Ingestion date {ingestion_date} not found for {source_folder}. Available: {available_dates}"
        )

    return f"{source_path}/{INGESTION_PARTITION_PREFIX}{selected_date}", selected_date
