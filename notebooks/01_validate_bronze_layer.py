client_id = dbutils.secrets.get(scope="key-vault-secrets", key="client-id")
client_secret = dbutils.secrets.get(scope="key-vault-secrets", key="client-secret")
tenant_id = dbutils.secrets.get(scope="key-vault-secrets", key="tenant-id")

storage_account_name = "sausedcarspipelinesv"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

base_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net"

print("--- Bronze Layer Directory Overview ---")
directories = dbutils.fs.ls(base_path)
for item in directories:
    if item.isDir():
        print(f"Directory: {item.name}")

sources = [
    "FirstSourceSystem",
    "SecondSourceSystem",
    "ThirdSourceSystem",
    "FourthSource",
    "ServiceBook",
    "Lookup"
]

print("\n--- Individual Folder Content Check ---")
for src in sources:
    source_path = f"{base_path}/{src}/"
    try:
        sub_items = dbutils.fs.ls(source_path)
        for sub in sub_items:
            print(f"[{src}] -> {sub.name}")
            if sub.isDir():
                files = dbutils.fs.ls(sub.path)
                for f in files:
                    print(f"      └── File: {f.name} (Size: {round(f.size / (1024*1024), 2)} MB)")
    except Exception as e:
        print(f"Error reading {src}: {e}")