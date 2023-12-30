# Databricks notebook source
# MAGIC %md
# MAGIC ### mount Azure Data Lake using service principal
# MAGIC #### steps to follow
# MAGIC 1. get client_id, tenant_id and client_secret from the key vault
# MAGIC 2. set spark Config with App/Client id, Directory/Tenant id & Secret
# MAGIC 3. call file system utility mount to mount storage
# MAGIC 4. Explore other file system utlities  related to mount (list all mounts, unmounts)
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key='client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key='client-secret')


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@formularacedl1.dfs.core.windows.net/",
  mount_point = "/mnt/formularacedl1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formularacedl1/demo"))

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

for mount in dbutils.fs.mounts():
    if mount.mountPoint == f"/mnt/formularacedl1/demo":
        dbutils.fs.unmount(mount.mountPoint)


# COMMAND ----------

