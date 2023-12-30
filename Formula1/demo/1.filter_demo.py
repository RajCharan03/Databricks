# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

##sql way
race_filter_df = race_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

##python way
race_filter1_df = race_df.filter((race_df['race_year'] == 2021) & (race_df['round'] <= 5))

# COMMAND ----------

display(race_filter_df)

# COMMAND ----------

display(race_filter1_df)

# COMMAND ----------

