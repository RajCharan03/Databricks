# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster_scope_cred
# MAGIC 1. set the spark config fs.azure.account.key in the cluster
# MAGIC 2. list the files from the demo container
# MAGIC 3. read data from circuits.csv file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formularacedl1.dfs.core.windows.net"))

# COMMAND ----------

ab = spark.read.csv("abfss://demo@formularacedl1.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(ab)

# COMMAND ----------

