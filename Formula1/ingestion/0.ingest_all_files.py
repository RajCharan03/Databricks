# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_Races_file", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.Ingest_constructors_file", 0,  {"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingestion_driver_file", 0,  {"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0,  {"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stop_file", 0,  {"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file", 0,  {"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------


v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0,  {"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})
v_result

# COMMAND ----------

