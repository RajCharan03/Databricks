# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", StringType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True),

])

# COMMAND ----------

races_df = spark.read \
    .option("header", "True") \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

races_select_df = races_df.select(col("raceId"), col("year"),  col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

races_renamed_df = races_select_df \
                    .withColumnRenamed("raceId", "race_id") \
                    .withColumnRenamed("year", "race_year") \
                    .withColumnRenamed("circuitId", "circuit_id") \
                    .withColumn("data_source", lit(v_data_source)) \
                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat

# COMMAND ----------

races_semifinal_df = races_renamed_df.withColumn("ingestion_date", current_timestamp()).withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_final_df = races_semifinal_df.select(col("race_id"), col("race_year"),  col("round"), col("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"), col("data_source"))

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Succeeded")