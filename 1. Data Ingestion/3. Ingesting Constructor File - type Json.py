# Databricks notebook source
# MAGIC %run /F1_Practice_Project/setup/4.Connection_to_raw_Container

# COMMAND ----------

# MAGIC %run "../Includes/1. Storage path"

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("environment","test","My_env")

# COMMAND ----------

dbutils.widgets.get("environment")

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
Constructor_schema=StructType([StructField("constructorId",IntegerType(),False),
                              StructField("constructorRef",StringType(),False),
                              StructField("name",StringType(),False),
                              StructField("nationality",StringType(),False),
                              StructField("url",StringType(),False)])


###constructor_schema=(constructorId INT,"constructorRef" STRING,"name" STRING,"nationality" STRING,"url" STRING)

# COMMAND ----------


constructor_df=spark.read.json(f"{raw_folder_path}/constructors.json",schema=Constructor_schema)
constructor_df.display()
constructor_df.printSchema()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import current_timestamp
constructor_final_df=constructor_df.drop("url")\
    .withColumnRenamed("constructorId","constructor_Id")\
    .withColumnRenamed("constructorRef","constructor_Ref")\
    .withColumn("Ingestion_time",current_timestamp())

# COMMAND ----------

constructor_final_df.display()
constructor_final_df.printSchema()

# COMMAND ----------


constructor_final_df.write.parquet(f"{processed_folder_path}/constructor",mode="overwrite")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/constructor"))

# COMMAND ----------


