# Databricks notebook source
# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

storage_account_name = "formula1dlsgen"  
client_id = dbutils.secrets.get(scope="formula1-scope", key="clientId")
client_secret = dbutils.secrets.get(scope="formula1-scope", key="clientSecret")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="tenantId")                

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type.formula1dlsgen.dfs.core.windows.net": "OAuth",
  "fs.azure.account.oauth.provider.type.formula1dlsgen.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id.formula1dlsgen.dfs.core.windows.net": client_id,
  "fs.azure.account.oauth2.client.secret.formula1dlsgen.dfs.core.windows.net": client_secret,
  "fs.azure.account.oauth2.client.endpoint.formula1dlsgen.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

def mount_adls(container_name):
    mount_point = f"/mnt/formula1dlsgen/{container_name}"
    source_url = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

    if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=source_url,
            mount_point=mount_point,
            extra_configs=configs 
        )
    else:
        print(f"Already mounted: {mount_point}")


# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")


# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlsgen/raw")

# COMMAND ----------

dbutils.fs.mounts()



# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlsgen/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dlsgen/presentation")
