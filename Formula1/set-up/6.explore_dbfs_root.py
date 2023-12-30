# Databricks notebook source
# MAGIC %md
# MAGIC #### explore_dbfs_root
# MAGIC 1. list all the folders in the DBFS root
# MAGIC 2. interact with DBFS file browser
# MAGIC 3. upload file to DBFS root
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

