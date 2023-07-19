# Databricks notebook source
# MAGIC %md
# MAGIC ####1. Create the Connection to the RAW Container

# COMMAND ----------

# MAGIC %run ../setup/4.Connection_to_raw_Container

# COMMAND ----------

# MAGIC %run "../Includes/1. Storage path"

# COMMAND ----------

# MAGIC %md 
# MAGIC ####2. Reading the Results.json file into the dataframe using spark reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
Results_Schema=StructType(fields = [StructField("constructorId",IntegerType(),True),
                                   StructField("driverId",IntegerType(),True),
                                   StructField("fastestLap",IntegerType(),True),
                                   StructField("fastestLapSpeed",StringType(),True),
                                   StructField("fastestLapTime",StringType(),True),
                                   StructField("grid",IntegerType(),True),
                                   StructField("laps",IntegerType(),True),
                                   StructField("milliseconds",IntegerType(),True),
                                   StructField("number",IntegerType(),True),
                                   StructField("points",DoubleType(),True),
                                   StructField("position",IntegerType(),True),
                                   StructField("positionOrder",IntegerType(),True),
                                   StructField("positionText",StringType(),True),
                                   StructField("raceId",IntegerType(),True),
                                   StructField("rank",IntegerType(),True),
                                   StructField("resultId",IntegerType(),True),
                                   StructField("statusId",IntegerType(),True),
                                   StructField("time",StringType(),True)
                                   ])

# COMMAND ----------

Results_df=spark.read.schema(Results_Schema).json(f"{raw_folder_path}/results.json")
Results_df.display()


# COMMAND ----------

from pyspark.sql.functions import current_date
Results_renamed_df=Results_df.withColumnRenamed("resultId","result_id")\
    .drop("statusId")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("positionOrder","position_order")\
    .withColumnRenamed("positionText","position_text")\
    .withColumnRenamed("fastestLap","fastest_lap")\
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
    .withColumn("Ingestion_date",current_date())

# COMMAND ----------

Results_renamed_df.display()

# COMMAND ----------


Results_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------


