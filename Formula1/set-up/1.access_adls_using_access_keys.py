# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 2. list the files from the demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formularacedl1.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formularacedl1.dfs.core.windows.net"))

# COMMAND ----------

ab = spark.read.csv("abfss://demo@formularacedl1.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(ab)

# COMMAND ----------

