# Databricks notebook source
# MAGIC %md
# MAGIC ### Setting up the Environment

# COMMAND ----------

# MAGIC %run "/Repos/Raj/Spark_DataEngineering_Project1/setup/Setup Script"

# COMMAND ----------

pit_stop_df=spark.read.csv("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops.json")
pit_stop_df.display()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,FloatType,StringType
pit_stop_schema=StructType(fields=[StructField("raceId",IntegerType(),True),
                                   StructField("driverId",IntegerType(),False),
                                   StructField("stop",IntegerType(),False),
                                   StructField("lap",IntegerType(),False),
                                   StructField("time",StringType(),False),
                                   StructField("duration",StringType(),False),
                                   StructField("milliseconds",StringType(),False)])

# COMMAND ----------

pit_stop_df=spark.read.schema(pit_stop_schema).json("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops.json",multiLine=True)

# COMMAND ----------

pit_stop_df.display()

# COMMAND ----------

from pyspark.sql.functions import current_date
pit_stop_final_df=pit_stop_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_date())

# COMMAND ----------

pit_stop_final_df.write.mode("overwrite").parquet("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/pit_stops"))

# COMMAND ----------


