# Databricks notebook source
# MAGIC %md
# MAGIC ### mount Azure Data Lake container for the project
# MAGIC
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key='client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key='client-secret')

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    

## unmount the mount if any already exists
    for mount in dbutils.fs.mounts():
        if mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}":
            dbutils.fs.unmount(mount.mountPoint)
    

        # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net//",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

    display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls('formularacedl1', 'demo')

# COMMAND ----------

mount_adls('formularacedl1', 'raw')

# COMMAND ----------

mount_adls('formularacedl1', 'processed')

# COMMAND ----------

mount_adls('formularacedl1', 'presentation')

# COMMAND ----------

# display(dbutils.fs.ls("/mnt/formularacedl1/demo"))
dbutils.fs.ls("/mnt/formularacedl1/demo")

# COMMAND ----------

ab = spark.read.csv("/mnt/formularacedl1/demo/circuits.csv")

# COMMAND ----------

display(ab)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#### umnount the mount 
dbutils.fs.unmount('/mnt/formularacedl1/demo')

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

