# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS token
# MAGIC
# MAGIC 1. set the spark config for SAS token
# MAGIC 2. list the files from the demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

sas_token = dbutils.secrets.get(scope='formula1-scope', key='SAS-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formularacedl1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formularacedl1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formularacedl1.dfs.core.windows.net", sas_token)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formularacedl1.dfs.core.windows.net"))

# COMMAND ----------

ab = spark.read.csv("abfss://demo@formularacedl1.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(ab)

# COMMAND ----------

