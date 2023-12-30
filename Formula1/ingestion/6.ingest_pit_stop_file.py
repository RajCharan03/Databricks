# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('driverId', IntegerType(), True),
                                      StructField('stop', StringType(), True),
                                      StructField('lap', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('duration', StringType(), True),
                                      StructField('milliseconds', IntegerType(), True)
                                      ])

# COMMAND ----------

##since the pit_stop is a multiline json, need to add one option multiline as True
pit_stops_df = spark.read \
                    .schema(pit_stops_schema) \
                    .option('multiLine', True) \
                    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
                                 .withColumnRenamed('driverId', 'driver_id') \
                                 .withColumn('ingestion_date', current_timestamp() ) \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

# overwrite_partition(pit_stops_final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop  AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df,'f1_processed', 'pit_stops' , processed_folder_path, merge_condition,'race_id' )

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# pit_stops_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

dbutils.notebook.exit('Succeeded')