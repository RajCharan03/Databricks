# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principal
# MAGIC 1. register azure AD application / service principal
# MAGIC 2. generate a secret/ password for the application
# MAGIC 3. set the spark config with App/ client id, directory / tenant id & secret
# MAGIC 4. assign role 'storage blob data contributor' to the data lake
# MAGIC
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key='client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key='tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key='client-secret')


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formularacedl1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formularacedl1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formularacedl1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formularacedl1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formularacedl1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formularacedl1.dfs.core.windows.net"))

# COMMAND ----------

ab = spark.read.csv("abfss://demo@formularacedl1.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(ab)

# COMMAND ----------

