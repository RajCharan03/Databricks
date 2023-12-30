# Databricks notebook source
# MAGIC %md
# MAGIC #### Access dataframes using SQL
# MAGIC ##### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_year = 2019

# COMMAND ----------

race_result_df = spark.sql(f"select * from v_race_results where race_year = {race_year}")

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Global Temporary Views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Acesss the view from another notebook

# COMMAND ----------

race_df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from global_temp.gv_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_result_df_g = spark.sql(f"select * from global_temp.gv_race_results where race_year = {race_year}")

# COMMAND ----------

display(race_result_df_g)

# COMMAND ----------

