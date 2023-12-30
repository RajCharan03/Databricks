# Databricks notebook source
spark.read.json('/mnt/formularacedl1/raw/2021-03-21/results.json').createOrReplaceTempView('result_cutover')

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1)
# MAGIC from result_cutover
# MAGIC group by raceId
# MAGIC order by raceId desc;

# COMMAND ----------

spark.read.json('/mnt/formularacedl1/raw/2021-03-28/results.json').createOrReplaceTempView('result_w1')

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1)
# MAGIC from result_w1
# MAGIC group by raceId
# MAGIC order by raceId desc;

# COMMAND ----------

spark.read.json('/mnt/formularacedl1/raw/2021-04-18/results.json').createOrReplaceTempView('result_w2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1)
# MAGIC from result_w2
# MAGIC group by raceId
# MAGIC order by raceId desc;

# COMMAND ----------

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

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True),

                                    ])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

result_final_df = results_df.withColumnRenamed("resultId", "result_id") \
                              .withColumnRenamed("raceId", "race_id") \
                              .withColumnRenamed("driverId", "driver_id") \
                              .withColumnRenamed("constructorId", "constructor_id") \
                              .withColumnRenamed("positionText", "position_text") \
                              .withColumnRenamed("positionOrder", "position_order") \
                              .withColumnRenamed("fastestLap", "fastest_lap") \
                              .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                              .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                              .withColumn("ingestion_date", current_timestamp()) \
                              .withColumn("data_source", lit(v_data_source)) \
                              .withColumn("file_date", lit(v_file_date)) \
                              .drop("statusId") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##de-dupe the dataframe

# COMMAND ----------

result_dedupe_df = result_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 1 

# COMMAND ----------

# for race_id_list in result_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS  PARTITION (race_id = {race_id_list.race_id} )")

# COMMAND ----------

# result_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results;

# COMMAND ----------

#overwrite_partition(result_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(result_dedupe_df,'f1_processed', 'results' , processed_folder_path, merge_condition,'race_id' )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select count(*) from f1_processed.results
# MAGIC
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit('Succeeded')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)
# MAGIC from f1_processed.results
# MAGIC where file_date = '2021-03-21'

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC having count(1) >1 
# MAGIC order by race_id, driver_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results
# MAGIC where race_id = 540 and driver_id =229;

# COMMAND ----------

