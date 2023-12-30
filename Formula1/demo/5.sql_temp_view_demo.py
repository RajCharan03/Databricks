# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from global_temp.gv_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

