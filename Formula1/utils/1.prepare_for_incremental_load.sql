-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

create database if not exists f1_processed
location '/mnt/formularacedl1/processed';

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE

-- COMMAND ----------

create database if not exists f1_presentation
location '/mnt/formularacedl1/presentation'

-- COMMAND ----------

describe database extended f1_presentation

-- COMMAND ----------

