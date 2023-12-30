# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formularacedl1/demo'

# COMMAND ----------

results_df = spark.read.option("inferSchema", True).json("/mnt/formularacedl1/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formularacedl1/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING  DELTA
# MAGIC LOCATION '/mnt/formularacedl1/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format('delta').load('/mnt/formularacedl1/demo/results_external')

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy('constructorId').saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed 
# MAGIC set points = 11 - position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formularacedl1/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed WHERE position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formularacedl1/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ###Upsert using merge
# MAGIC

# COMMAND ----------

driver_day1_df = spark.read.option("inferSchema", True) \
                            .json('/mnt/formularacedl1/raw/2021-03-28/drivers.json') \
                            .filter("driverId <= 10") \
                            .select("driverId", "dob", 'name.forename', 'name.surname')

# COMMAND ----------

display(driver_day1_df)

# COMMAND ----------

driver_day1_df.createOrReplaceTempView('driver_day1')

# COMMAND ----------

from pyspark.sql.functions import upper
driver_day2_df = spark.read.option("inferSchema", True) \
                            .json('/mnt/formularacedl1/raw/2021-03-28/drivers.json') \
                            .filter("driverId BETWEEN 6 AND 15") \
                            .select("driverId", "dob", upper('name.forename').alias("forename"), upper('name.surname').alias('surname'))

# COMMAND ----------

display(driver_day2_df)

# COMMAND ----------

driver_day2_df.createOrReplaceTempView('driver_day2')

# COMMAND ----------

from pyspark.sql.functions import upper
driver_day3_df = spark.read.option("inferSchema", True) \
                            .json('/mnt/formularacedl1/raw/2021-03-28/drivers.json') \
                            .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
                            .select("driverId", "dob", upper('name.forename').alias("forename"), upper('name.surname').alias('surname'))

# COMMAND ----------

display(driver_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ##day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ###Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, '/mnt/formularacedl1/demo/drivers_merge')


deltaTable.alias('tgt') \
  .merge(
    driver_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createDate": "current_timestamp()",
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History and versioning 
# MAGIC 2. time travel
# MAGIC 3. vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-09-23T15:57:35.000+0000'

# COMMAND ----------

df = spark.read.format('delta').option('timestampAsOf', '2023-09-23T15:57:35.000+0000').load('/mnt/formularacedl1/demo/drivers_merge')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-09-23T15:57:35.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-09-23T15:57:35.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge --version as of 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge version as of 3 src
# MAGIC       on(tgt.driverId = src.driverId)
# MAGIC when not matched then
# MAGIC   insert *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transaction Log

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn 
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn 
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 2;

# COMMAND ----------

for driver_id in range(3, 20):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn 
                  SELECT * FROM f1_demo.drivers_merge
                  WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn 
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_txn where driverId = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Convert parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta 
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demo.drivers_convert_to_delta 

# COMMAND ----------

df = spark.table('f1_demo.drivers_convert_to_delta ')

# COMMAND ----------

df.write.format('parquet').save('mnt/formularacedl1/demo/drivers_convert_to_delta_new')

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/formularacedl1/demo/drivers_convert_to_delta_new`

# COMMAND ----------

