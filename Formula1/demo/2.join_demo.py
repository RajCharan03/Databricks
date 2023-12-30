# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/races") \
                    .withColumnRenamed("name", "race_name") \
                    .filter('race_year = 2019' )

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .filter('circuit_id < 70') \
                        .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_df)
display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###inner join

# COMMAND ----------

race_circuit_df = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "inner").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, race_df.race_name, race_df.round)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##outer join

# COMMAND ----------

##left join 
race_circuit_df_left = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "left").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, race_df.race_name, race_df.round)

# COMMAND ----------

display(race_circuit_df_left)

# COMMAND ----------

##right join 
race_circuit_df_right = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "right").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, race_df.race_name, race_df.round)

# COMMAND ----------

display(race_circuit_df_right)

# COMMAND ----------

##fullouter join 
race_circuit_df_fullouter = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "full").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, race_df.race_name, race_df.round)

# COMMAND ----------

display(race_circuit_df_fullouter)

# COMMAND ----------

# MAGIC %md
# MAGIC ##semi joins

# COMMAND ----------

race_circuit_df_semi = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "semi")

# COMMAND ----------

display(race_circuit_df_semi)

# COMMAND ----------

# MAGIC %md
# MAGIC ##anti joins

# COMMAND ----------

race_circuit_df_anti = circuits_df.join(race_df, circuits_df.circuit_id == race_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuit_df_anti)

# COMMAND ----------

# MAGIC %md
# MAGIC ##cross joins

# COMMAND ----------

race_circuit_df_cross =race_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuit_df_cross)
race_circuit_df_cross.count()

# COMMAND ----------

int(race_df.count()) * int(circuits_df.count())

# COMMAND ----------

