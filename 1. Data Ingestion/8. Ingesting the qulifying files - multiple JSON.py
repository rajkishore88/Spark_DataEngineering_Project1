# Databricks notebook source
# MAGIC %run /F1_Practice_Project/setup/4.Connection_to_raw_Container

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType
qualifying_schema=StructType(fields=[StructField("qualifyId",IntegerType(),True),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("q1",StringType(),True),
                                     StructField("q2",StringType(),True),
                                     StructField("q3",StringType(),True)
                                     ])

# COMMAND ----------


qualifying_df=spark.read.schema(qualifying_schema).json("abfss://raw@adlsstreamingpocgn2.dfs.core.windows.net/qualifying", multiLine=True)
qualifying_df.display()

# COMMAND ----------

from pyspark.sql.functions import current_date
qualifying_final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumn("Ingestion_time",current_date())

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("abfss://processed@adlsstreamingpocgn2.dfs.core.windows.net/qualifying")

# COMMAND ----------


