-- Databricks notebook source
-- MAGIC %run /F1_Practice_Project/setup/4.Connection_to_raw_Container

-- COMMAND ----------

-- MAGIC %run "/F1_Practice_Project/Includes/1. Storage path"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(f"{raw_folder_path}")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Creating Circuits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE Table f1_raw.circuits(circuitId Int,
circuitRef String,
name String,
location String,
lat Double,
lng Double,
alt Integer,
url String)
using csv
options(path "abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/circuits.csv", header True)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating Race Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.Races;
CREATE TABLE IF NOT EXISTS f1_raw.Races(race_id Integer,
race_year Integer,
round Integer,
circuit_id Integer,
name String,
date Date,
time String,
url String)
using CSV
options(path "abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/races.csv",header True)

-- COMMAND ----------

select * from f1_raw.Races;

-- COMMAND ----------


