-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Setting up the Environment

-- COMMAND ----------

-- MAGIC %run "/Repos/Raj/Spark_DataEngineering_Project1/setup/Setup Script"
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net"))
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{raw_folder_path}"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Constructor table from simple json file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_json=spark.read.json("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/results.json")
-- MAGIC df_json.printSchema()

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE Table IF NOT EXISTS f1_raw.constructors(constructorId  long,
constructorRef  string,
name  string,
nationality string)
using json
options(path "abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/constructors.json", header True)

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

desc extended f1_raw.Races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reading the file in python API to demonstrate the difference in SQL API

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType,StructField,StringType
-- MAGIC name_schema=StructType(fields=[StructField("forename",StringType(),True),
-- MAGIC                                StructField("Surname",StringType(),True)])
-- MAGIC drivers_schema=StructType(fields=[StructField("code",StringType(),True),
-- MAGIC                                  StructField("dob",StringType(),True),
-- MAGIC                                  StructField("driverId",StringType(),True),
-- MAGIC                                  StructField("driverRef",StringType(),True),
-- MAGIC                                  StructField("name",name_schema,True),
-- MAGIC                                  StructField("nationality",StringType(),True),
-- MAGIC                                  StructField("number",StringType(),True),
-- MAGIC                                  StructField("url",StringType(),True),
-- MAGIC                                  ])
-- MAGIC df_jason=spark.read.json("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/drivers.json")
-- MAGIC df_jason.display()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df_jason.printSchema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drivers table creation using multiline Json file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
create table if not exists f1_raw.drivers(
code string,
dob string,
driverId string,
driverRef string,
name struct<forename string,surname string>,
nationality string,
number string,
url string)
using json
options(path "abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
create table if not exists f1_raw.drivers
using json
options(path "abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating table for pitstop using multiline json

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df_pit_stops=spark.read.csv("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops.json")
-- MAGIC df_pit_stops.display()

-- COMMAND ----------

"raceId":841
"driverId":20
 "stop":1
 "lap":14
 time":"17:26:03"
"duration":24.863
"milliseconds":24863

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType,StructField,StringType
-- MAGIC pit_stops_schema=StructType(fields=[StructField("raceId",StringType(),True),
-- MAGIC                                     StructField("driverId",StringType(),True),
-- MAGIC                                     StructField("stop",StringType(),True),
-- MAGIC                                     StructField("lap",StringType(),True),
-- MAGIC                                     StructField("time",StringType(),True),
-- MAGIC                                     StructField("duration",StringType(),True),
-- MAGIC                                     StructField("milliseconds",StringType(),True)])
-- MAGIC
-- MAGIC
-- MAGIC ##df_pit_stops=spark.read.options(schema=pit_stops_schema).option("multinline","true").json("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops.json")
-- MAGIC
-- MAGIC df_pit_stops=spark.read.option("schema","pit_stops_schema").option("multiline","true").json("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops.json")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df_pit_stops.display()

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverID int,
  duration DOUBLE,
  lap int,
  millisecinds int,
  raceid int,
  stop int,
  time TIMESTAMP
)
using JSON
options(path "abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops.json",multiline "true")

-- COMMAND ----------

SELECT * from f1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
using JSON
options(path "abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops.json",multiline "true")

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------


