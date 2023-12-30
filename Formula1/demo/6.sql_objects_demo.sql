-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

CREATE DATABASE demo

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

describe database demo

-- COMMAND ----------

describe database extended demo 

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe table 

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df  = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe extended race_results_python

-- COMMAND ----------

select * from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

create table demo.race_results_sql
as
select * from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

describe extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping an external table
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').option('path', f"{presentation_folder_path}/race_results_ext_py").saveAsTable('demo.race_results_ext_py')

-- COMMAND ----------

describe extended demo.race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
using parquet
location '/mnt/formularacedl1/presentation/race_results_ext_sql'

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

select count(*) from demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

create or replace temp view v_race_results
as 
select * 
from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as 
select * 
from demo.race_results_python
where race_year = 2012

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

create or replace view demo.pv_race_results
as 
select * 
from demo.race_results_python
where race_year = 2000;

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from demo.pv_race_results

-- COMMAND ----------

