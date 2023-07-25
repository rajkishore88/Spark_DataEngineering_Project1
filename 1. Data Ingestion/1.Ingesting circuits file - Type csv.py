# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingesting CSV File
# MAGIC 1. Read the csv file with reader api
# MAGIC 2. Apply the schema
# MAGIC 3. Remove the unwanted fields
# MAGIC 4. Add time stamp field for logging purpose
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Setting the environment

# COMMAND ----------

# MAGIC %run "/Repos/Raj/Spark_DataEngineering_Project1/setup/Setup Script"

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_folder_path}/circuits.csv"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
Circuits_Schema=StructType(fields=[StructField("circuitId",IntegerType(),True),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)])

# COMMAND ----------

circuits_df=spark.read.options(header=True,Schema=Circuits_Schema).csv(f"{raw_folder_path}/circuits.csv")
circuits_df.display()

# COMMAND ----------

circuits_df_final=circuits_df.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_ref")\
    .withColumnRenamed("lat","Lattitude")\
    .withColumnRenamed("lng","Longitude")\
    .withColumnRenamed("alt","altitude")\
    .drop("url")

# COMMAND ----------

circuits_df_final.display()

# COMMAND ----------

circuits_df_final.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}"))

# COMMAND ----------


