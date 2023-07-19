# Databricks notebook source


# COMMAND ----------

# MAGIC %run /F1_Practice_Project/setup/4.Connection_to_raw_Container

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
lap_time_schema=StructType(fields=[StructField("raceid",IntegerType(),True),
                                   StructField("driverid",IntegerType(),True),
                                   StructField("lap",IntegerType(),True),
                                   StructField("position",IntegerType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("milliseconds",IntegerType(),True)
                                   ])

# COMMAND ----------

lap_time_df=spark.read.schema(lap_time_schema).csv("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/lap_times")
lap_time_df.display()

# COMMAND ----------

from pyspark.sql.functions import current_date
lap_time_final_df=lap_time_df.withColumnRenamed("raceid","race_id")\
                            .withColumnRenamed("driverid","driver_id")\
                            .withColumn("ingestion_date",current_date())

# COMMAND ----------

lap_time_final_df.write.mode("overwrite").parquet("abfss://processed@adlsstreamingpocgn2.dfs.core.windows.net/lap_times")

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@adlsstreamingpocgn2.dfs.core.windows.net/lap_times"))

# COMMAND ----------


